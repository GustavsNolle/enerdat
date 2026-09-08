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


def _settle(schedule: str, prefix: str) -> str:
    """Settlement columns for one schedule. Same arithmetic for every schedule,
    so the baseline and the candidate can never drift apart."""
    dev = f"(p.actual_mw - p.{schedule})"
    guard = f"p.actual_mw IS NULL OR p.{schedule} IS NULL"
    return f"""
    {dev} * p.interval_hours                                 AS {prefix}_imbalance_mwh,
    CASE WHEN {guard} THEN NULL
         WHEN {dev} >= 0 THEN {dev} * p.interval_hours * pr.price_long
         ELSE                 {dev} * p.interval_hours * pr.price_short
    END                                                      AS {prefix}_settlement_eur,
    CASE WHEN {guard} THEN NULL
         WHEN {dev} >= 0 THEN -({dev} * p.interval_hours * pr.price_long)
         ELSE                 -({dev} * p.interval_hours * pr.price_short)
    END                                                      AS {prefix}_cost_eur"""


SETTLEMENT_SQL = f"""
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
    FROM candidate_forecast
)
SELECT
    p.valid_time_utc,
    p.delivery_date,
    p.interval_hours,
    p.actual_mw,
    p.tso_forecast_mw,
    p.candidate_mw,
    pr.price_long,
    pr.price_short,
    {_settle("tso_forecast_mw", "tso")},
    {_settle("candidate_mw", "candidate")},
    -- The headline. Positive means the candidate schedule cost less to settle
    -- than the TSO's own forecast would have.
    CASE
        WHEN p.candidate_mw IS NULL THEN NULL
        ELSE (
            CASE WHEN (p.actual_mw - p.tso_forecast_mw) >= 0
                 THEN -((p.actual_mw - p.tso_forecast_mw) * p.interval_hours * pr.price_long)
                 ELSE -((p.actual_mw - p.tso_forecast_mw) * p.interval_hours * pr.price_short)
            END
            -
            CASE WHEN (p.actual_mw - p.candidate_mw) >= 0
                 THEN -((p.actual_mw - p.candidate_mw) * p.interval_hours * pr.price_long)
                 ELSE -((p.actual_mw - p.candidate_mw) * p.interval_hours * pr.price_short)
            END
        )
    END                                                      AS savings_eur
FROM paced AS p
LEFT JOIN prices AS pr USING (valid_time_utc)
ORDER BY p.valid_time_utc
"""


def _has_files(glob: str) -> bool:
    root = Path(glob.split("**")[0])
    return root.exists() and any(root.rglob("*.parquet"))


@dg.asset(
    deps=["candidate_forecast", "entsoe_imbalance_price"],
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
            candidate_cost,
            savings,
            scored,
            days,
        ) = connection.execute(
            """
            SELECT
                count(*),
                count(tso_cost_eur),
                sum(tso_cost_eur),
                sum(candidate_cost_eur),
                sum(savings_eur),
                count(savings_eur),
                count(DISTINCT delivery_date)
            FROM imbalance_settlement
            """
        ).fetchone()

    cost = cost or 0.0
    savings = savings or 0.0
    scored_days = max(scored / 96, 1) if scored else 1
    context.log.info(
        f"baseline {cost:,.0f} EUR over {days} day(s); "
        f"candidate saves {savings:,.0f} EUR on {scored} scored intervals"
    )

    return dg.MaterializeResult(
        metadata={
            "zone": ZONE,
            "technology": TARGET_TECHNOLOGY,
            "intervals": rows,
            "intervals_priced": priced,
            "intervals_scored": scored,
            "days": days,
            "baseline_cost_eur": round(cost, 2),
            "candidate_cost_eur": round(candidate_cost or 0.0, 2),
            "savings_eur": round(savings, 2),
            # Annualised, so it is comparable across backfill lengths. Still
            # divide by installed MW before quoting it as EUR/MW/year.
            "savings_eur_per_year": round(savings / scored_days * 365, 2),
            "candidate_beats_tso": savings > 0,
        }
    )
