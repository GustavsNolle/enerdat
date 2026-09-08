"""Turn forecast error into money.

Sign conventions, stated once because every bug in imbalance settlement is a
sign bug:

    imbalance_mwh = (actual - scheduled) * interval_hours

    imbalance_mwh > 0   the BRP is LONG  -- delivered more than scheduled,
                        and is settled at the Long price.
    imbalance_mwh < 0   the BRP is SHORT -- delivered less than scheduled,
                        and is settled at the Short price.

    settlement_eur = imbalance_mwh * price      (revenue: positive = received)
    cost_eur       = -settlement_eur            (positive = out of pocket)

Both directions can cost money. Being long into a negative price means paying
to deliver, which is exactly the regime a renewables portfolio lands in when
everyone's wind over-produces at once -- so cost is not a function of |error|.
Under a single-price regime the two price columns carry the same value and the
arithmetic is unchanged.
"""

from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext
from dagster_duckdb import DuckDBResource

from enerdat.config import TARGET_TECHNOLOGY, ZONE
from enerdat.resources import LakeResource

SETTLEMENT_SQL = """
CREATE OR REPLACE TABLE imbalance_settlement AS
WITH prices AS (
    SELECT
        valid_time_utc,
        max(CASE WHEN variable = 'Long'  THEN value END) AS price_long,
        max(CASE WHEN variable = 'Short' THEN value END) AS price_short
    FROM (
        SELECT valid_time_utc, variable, value
        FROM read_parquet($imbalance_glob)
        QUALIFY row_number() OVER (
            PARTITION BY valid_time_utc, variable ORDER BY retrieved_at_utc DESC
        ) = 1
    )
    GROUP BY valid_time_utc
),
paced AS (
    SELECT
        *,
        -- Derive the settlement period from the data rather than assuming 15
        -- or 60 minutes: the market moved to quarter-hourly MTU mid-history,
        -- and a DST day contains an interval of a different length.
        coalesce(
            date_diff(
                'second',
                valid_time_utc,
                lead(valid_time_utc) OVER (ORDER BY valid_time_utc)
            ) / 3600.0,
            0.25
        ) AS interval_hours
    FROM forecast_error
)
SELECT
    p.valid_time_utc,
    p.delivery_date,
    p.interval_hours,
    p.actual_mw,
    p.tso_forecast_mw,
    pr.price_long,
    pr.price_short,
    (p.actual_mw - p.tso_forecast_mw) * p.interval_hours   AS tso_imbalance_mwh,
    CASE
        WHEN p.actual_mw IS NULL OR p.tso_forecast_mw IS NULL THEN NULL
        WHEN (p.actual_mw - p.tso_forecast_mw) >= 0
            THEN (p.actual_mw - p.tso_forecast_mw) * p.interval_hours * pr.price_long
        ELSE (p.actual_mw - p.tso_forecast_mw) * p.interval_hours * pr.price_short
    END                                                    AS tso_settlement_eur,
    CASE
        WHEN p.actual_mw IS NULL OR p.tso_forecast_mw IS NULL THEN NULL
        WHEN (p.actual_mw - p.tso_forecast_mw) >= 0
            THEN -((p.actual_mw - p.tso_forecast_mw) * p.interval_hours * pr.price_long)
        ELSE -((p.actual_mw - p.tso_forecast_mw) * p.interval_hours * pr.price_short)
    END                                                    AS tso_cost_eur
FROM paced AS p
LEFT JOIN prices AS pr USING (valid_time_utc)
ORDER BY p.valid_time_utc
"""


def _has_files(glob: str) -> bool:
    root = Path(glob.split("**")[0])
    return root.exists() and any(root.rglob("*.parquet"))


@dg.asset(
    deps=["forecast_error_mart", "entsoe_imbalance_price"],
    group_name="marts",
    kinds={"duckdb", "sql"},
    description=(
        "Settles the TSO's own day-ahead forecast against the imbalance price, "
        "interval by interval. This is the baseline cost any candidate model "
        "has to beat: the headline number is the difference between this and "
        "the same settlement run on your own schedule."
    ),
)
def imbalance_settlement_mart(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    lake: LakeResource,
) -> dg.MaterializeResult:
    imbalance_glob = lake.glob("entsoe_imbalance_price")
    if not _has_files(imbalance_glob):
        raise dg.Failure(
            description=(
                f"No landed imbalance prices. {ZONE} must publish article 17 "
                "data at bidding-zone level -- DE_LU does not, which is why "
                "the default zone is NL."
            )
        )

    with duckdb.get_connection() as connection:
        connection.execute(SETTLEMENT_SQL, {"imbalance_glob": imbalance_glob})
        (
            rows,
            priced,
            cost,
            mwh_short,
            mwh_long,
            days,
        ) = connection.execute(
            """
            SELECT
                count(*),
                count(tso_cost_eur),
                sum(tso_cost_eur),
                sum(tso_imbalance_mwh) FILTER (WHERE tso_imbalance_mwh < 0),
                sum(tso_imbalance_mwh) FILTER (WHERE tso_imbalance_mwh > 0),
                count(DISTINCT delivery_date)
            FROM imbalance_settlement
            """
        ).fetchone()

    cost = cost or 0.0
    per_day = cost / days if days else 0.0
    context.log.info(
        f"baseline imbalance cost {cost:,.0f} EUR over {days} day(s) "
        f"({per_day:,.0f} EUR/day)"
    )

    return dg.MaterializeResult(
        metadata={
            "zone": ZONE,
            "technology": TARGET_TECHNOLOGY,
            "intervals": rows,
            "intervals_priced": priced,
            "baseline_cost_eur": round(cost, 2),
            "baseline_cost_eur_per_day": round(per_day, 2),
            "mwh_short": round(mwh_short or 0.0, 1),
            "mwh_long": round(mwh_long or 0.0, 1),
            "days": days,
        }
    )
