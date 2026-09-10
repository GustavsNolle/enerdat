"""The point-in-time mart: one row per market interval, no future information."""

from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext
from dagster_duckdb import DuckDBResource

from enerdat.config import (
    DECISION_TIME_LOCAL,
    GRID_POINTS,
    TARGET_TECHNOLOGY,
    WEATHER_LEAD_DAYS,
    WEATHER_VARIABLES,
    ZONE,
)
from enerdat.resources import LakeResource

# Hour of DECISION_TIME_LOCAL, so the SQL deadline follows the config rather
# than a literal that drifts out of step with it.
_DECISION_HOUR = DECISION_TIME_LOCAL.hour

# Weather is summarised three ways, because averaging alone destroys the two
# things that matter most.
#
#   {var}_v         capacity-weighted mean across sites, then averaged over
#                   models -- the central estimate.
#   {var}_model_sd  disagreement between ICON, GFS and ECMWF at that hour, and
#                   the uncertainty feature the quantile objective wants: it is
#                   estimating a distribution, so spread speaks to its width.
#   ws_site_N       per-site wind speed, averaged over models. The Belgian
#                   fleet spans ~30 km, so the gradient across it says
#                   something about a front's timing that one number cannot.
#   ws_site_sd      spread of wind speed across sites.
#
# The two spreads must be computed at different levels of aggregation. Taking a
# standard deviation over the flat (site x model) rows produces the SAME number
# for both -- it measures the two sources of variation mixed together and
# attributes it to whichever column you happened to name. So models are
# collapsed across sites first and sites across models first, and only then is
# each spread taken.
#
# Written out explicitly rather than with PIVOT, which cannot enumerate its
# columns in a statement that also carries parameters.
_PER_MODEL = ",\n            ".join(
    f"""sum(CASE WHEN variable = '{v}' THEN value * weight END)
                / nullif(sum(CASE WHEN variable = '{v}' THEN weight END), 0) AS {v}"""
    for v in WEATHER_VARIABLES
)

_ACROSS_MODELS = ",\n        ".join(
    f"""avg({v})                                             AS {v}_v,
        stddev_samp({v})                                     AS {v}_model_sd"""
    for v in WEATHER_VARIABLES
)

_ISSUED = ",\n            ".join(
    f"""max(CASE WHEN variable = '{v}' THEN issue_time_utc END) AS {v}_issued"""
    for v in WEATHER_VARIABLES
)

_SITE_PIVOT = ",\n            ".join(
    f"""avg(CASE WHEN site = '{point['name']}' THEN ws END)   AS ws_site_{i}"""
    for i, point in enumerate(GRID_POINTS)
)

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
per_model AS (
    -- One row per (interval, model): the capacity-weighted fleet mean that
    -- each forecasting centre implies.
    SELECT
        valid_time_utc,
        model,
        {_PER_MODEL}
    FROM read_parquet($weather_glob)
    GROUP BY valid_time_utc, model
),
weather AS (
    SELECT
        valid_time_utc,
        {_ACROSS_MODELS}
    FROM per_model
    GROUP BY valid_time_utc
),
per_site AS (
    -- One row per (interval, site): wind speed averaged over models.
    SELECT
        valid_time_utc,
        site,
        avg(CASE WHEN variable = 'wind_speed_100m' THEN value END) AS ws
    FROM read_parquet($weather_glob)
    GROUP BY valid_time_utc, site
),
sites AS (
    SELECT
        valid_time_utc,
        {_SITE_PIVOT},
        stddev_samp(ws)                                      AS ws_site_sd
    FROM per_site
    GROUP BY valid_time_utc
),
issued AS (
    SELECT
        valid_time_utc,
        {_ISSUED}
    FROM read_parquet($weather_glob)
    GROUP BY valid_time_utc
)
SELECT
    f.valid_time_utc,
    timezone('Europe/Brussels', f.valid_time_utc)::DATE      AS delivery_date,
    -- the decision deadline for this delivery day, back in UTC
    timezone(
        'Europe/Brussels',
        (timezone('Europe/Brussels', f.valid_time_utc)::DATE
         - INTERVAL 1 DAY) + INTERVAL {_DECISION_HOUR} HOUR
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
    s.* EXCLUDE (valid_time_utc),
    i.* EXCLUDE (valid_time_utc),
    $lead_days                                               AS weather_lead_days
FROM tso_forecast AS f
LEFT JOIN actual    AS a  USING (valid_time_utc)
LEFT JOIN prices    AS pr USING (valid_time_utc)
LEFT JOIN day_ahead AS da USING (valid_time_utc)
LEFT JOIN weather   AS w ON w.valid_time_utc = date_trunc('hour', f.valid_time_utc)
LEFT JOIN sites     AS s ON s.valid_time_utc = date_trunc('hour', f.valid_time_utc)
LEFT JOIN issued    AS i ON i.valid_time_utc = date_trunc('hour', f.valid_time_utc)
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
