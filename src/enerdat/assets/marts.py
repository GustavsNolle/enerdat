"""The point-in-time mart: one row per market interval, no future information."""

from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext
from dagster_duckdb import DuckDBResource

from enerdat.config import (
    TARGET_TECHNOLOGY,
    WEATHER_LEAD_DAYS,
    WEATHER_VARIABLES,
    ZONE,
)
from enerdat.resources import LakeResource

# Capacity-weighted mean per variable, plus the issue time of the run each
# feature came from. Written out explicitly rather than with PIVOT: PIVOT has
# to enumerate its columns at bind time, which DuckDB cannot do in a statement
# that also carries parameters -- and an explicit list gives a stable schema
# that does not shift when a variable stops being reported.
_WEATHER_COLUMNS = ",\n        ".join(
    f"""sum(CASE WHEN w.variable = '{v}' THEN w.value * w.weight END)
            / nullif(sum(CASE WHEN w.variable = '{v}' THEN w.weight END), 0)
                                                             AS {v}_v,
        max(CASE WHEN w.variable = '{v}' THEN w.issue_time_utc END)
                                                             AS {v}_issued"""
    for v in WEATHER_VARIABLES
)

# Deduplication is the whole bitemporal story in one clause: the raw layer holds
# every retrieval of every interval, and the mart takes the most recent one.
# Swap DESC for a `WHERE retrieved_at_utc <= <as_of>` to reconstruct what was
# known on any past date.
_LATEST = (
    "QUALIFY row_number() OVER "
    "(PARTITION BY valid_time_utc ORDER BY retrieved_at_utc DESC) = 1"
)

MART_SQL = f"""
CREATE OR REPLACE TABLE forecast_error AS
WITH tso_forecast AS (
    SELECT valid_time_utc, value AS tso_forecast_mw
    FROM read_parquet($forecast_glob)
    WHERE variable = $technology
    {_LATEST}
),
actual AS (
    SELECT valid_time_utc, value AS actual_mw
    FROM read_parquet($actual_glob)
    WHERE variable = $technology
    {_LATEST}
),
prices AS (
    SELECT
        valid_time_utc,
        max(CASE WHEN variable = 'Long'  THEN value END) AS price_long,
        max(CASE WHEN variable = 'Short' THEN value END) AS price_short
    FROM (
        SELECT valid_time_utc, variable, value
        FROM read_parquet($imbalance_glob)
        WHERE variable IN ('Long', 'Short')
        QUALIFY row_number() OVER (
            PARTITION BY valid_time_utc, variable ORDER BY retrieved_at_utc DESC
        ) = 1
    )
    GROUP BY valid_time_utc
),
day_ahead AS (
    SELECT valid_time_utc, value AS price_day_ahead
    FROM read_parquet($dayahead_glob)
    WHERE variable = 'day_ahead_price'
    {_LATEST}
),
weather AS (
    SELECT
        w.valid_time_utc,
        {_WEATHER_COLUMNS}
    FROM read_parquet($weather_glob) AS w
    GROUP BY w.valid_time_utc
)
SELECT
    f.valid_time_utc,
    timezone('Europe/Brussels', f.valid_time_utc)::DATE      AS delivery_date,
    -- gate closure: 12:00 market time on D-1, expressed back in UTC
    timezone(
        'Europe/Brussels',
        (timezone('Europe/Brussels', f.valid_time_utc)::DATE
         - INTERVAL 1 DAY) + INTERVAL 12 HOUR
    )                                                        AS gate_closure_utc,
    f.tso_forecast_mw,
    a.actual_mw,
    a.actual_mw - f.tso_forecast_mw                          AS tso_error_mw,
    -- Prices are carried for settlement and for estimating the cost-optimal
    -- objective from history. They are deliberately NOT features: the
    -- imbalance price for delivery day D is unknown at its gate closure.
    da.price_day_ahead,
    pr.price_long,
    pr.price_short,
    w.* EXCLUDE (valid_time_utc),
    $lead_days                                               AS weather_lead_days
FROM tso_forecast AS f
LEFT JOIN actual    AS a  USING (valid_time_utc)
LEFT JOIN prices    AS pr USING (valid_time_utc)
LEFT JOIN day_ahead AS da USING (valid_time_utc)
LEFT JOIN weather   AS w
       ON w.valid_time_utc = date_trunc('hour', f.valid_time_utc)
ORDER BY f.valid_time_utc
"""


def _has_files(glob: str) -> bool:
    root = Path(glob.split("**")[0])
    return root.exists() and any(root.rglob("*.parquet"))


@dg.asset(
    deps=[
        "entsoe_day_ahead_forecast",
        "entsoe_actual_generation",
        "openmeteo_archived_forecast",
        "entsoe_imbalance_price",
        "entsoe_day_ahead_price",
    ],
    group_name="marts",
    kinds={"duckdb", "sql"},
    description=(
        "One row per market interval for the target technology: the TSO's "
        "day-ahead forecast, what actually happened, and capacity-weighted "
        "weather features from a model run that predates gate closure. "
        "Weather is hourly and ENTSO-E is 15-minute, so features are held "
        "constant within the hour -- an explicit choice, not an accident."
    ),
)
def forecast_error_mart(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    lake: LakeResource,
) -> dg.MaterializeResult:
    globs = {
        "forecast_glob": lake.glob("entsoe_day_ahead_forecast"),
        "actual_glob": lake.glob("entsoe_actual_generation"),
        "weather_glob": lake.glob("openmeteo_archived_forecast"),
        "imbalance_glob": lake.glob("entsoe_imbalance_price"),
        "dayahead_glob": lake.glob("entsoe_day_ahead_price"),
    }

    # Forecast, actual and weather are the mart. Prices are carried for
    # settlement, and a zone can legitimately not publish them -- DE_LU has no
    # bidding-zone imbalance price at all -- so their absence degrades the mart
    # rather than failing it. The cost columns downstream become NULL, and the
    # model reports that it fell back to the megawatt objective.
    required = ["forecast_glob", "actual_glob", "weather_glob"]
    optional = ["imbalance_glob", "dayahead_glob"]

    missing_required = [n for n in required if not _has_files(globs[n])]
    if missing_required:
        raise dg.Failure(
            description=(
                f"No landed parquet for: {', '.join(missing_required)}. "
                "Materialise the raw assets for at least one partition first."
            )
        )

    missing_optional = [n for n in optional if not _has_files(globs[n])]
    for name in missing_optional:
        # An unmatched glob would raise; point it at the forecast files and
        # filter every row out, so the CTE exists with the right shape.
        globs[name] = globs["forecast_glob"]
    if missing_optional:
        context.log.warning(
            f"No price data for {', '.join(missing_optional)} -- settlement "
            "costs will be NULL and the model cannot use the euro objective."
        )

    with duckdb.get_connection() as connection:
        connection.execute(
            MART_SQL,
            {
                **globs,
                "technology": TARGET_TECHNOLOGY,
                "lead_days": WEATHER_LEAD_DAYS,
            },
        )
        rows, first, last, matched = connection.execute(
            """
            SELECT count(*),
                   min(valid_time_utc),
                   max(valid_time_utc),
                   count(actual_mw)
            FROM forecast_error
            """
        ).fetchone()

    context.log.info(f"forecast_error: {rows} intervals, {matched} with an actual")

    return dg.MaterializeResult(
        metadata={
            "zone": ZONE,
            "technology": TARGET_TECHNOLOGY,
            "rows": rows,
            "intervals_with_actual": matched,
            "first_interval_utc": str(first),
            "last_interval_utc": str(last),
            "weather_lead_days": WEATHER_LEAD_DAYS,
        }
    )
