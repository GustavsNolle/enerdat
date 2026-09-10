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

import dagster as dg
from dagster import AssetExecutionContext
from dagster_duckdb import DuckDBResource

from enerdat.config import TARGET_TECHNOLOGY, ZONE


def _settle(schedule: str, prefix: str) -> str:
    """Settlement columns for one schedule.

    Two measures, because they answer different questions:

    *_settlement_eur is the raw cash exchanged on the imbalance market.

    *_cost_eur is the opportunity cost, and it is the one to optimise. The
    schedule was already sold at the day-ahead price, so deviating costs the
    spread between that price and the imbalance price:

        cost = (actual - scheduled) * hours * (P_dayahead - P_imbalance)

    Long into a price below day-ahead costs money; short into a price above it
    costs money; both are non-negative in the normal regime. The raw settlement
    figure is not a well-posed objective -- minimising it would push the
    schedule to zero whenever the long price is positive.
    """
    dev = f"(p.actual_mw - p.{schedule})"
    guard = f"p.actual_mw IS NULL OR p.{schedule} IS NULL"
    imbalance_price = (
        f"CASE WHEN {dev} >= 0 THEN p.price_long ELSE p.price_short END"
    )
    return f"""
    {dev} * p.interval_hours                                 AS {prefix}_imbalance_mwh,
    CASE WHEN {guard} THEN NULL
         ELSE {dev} * p.interval_hours * ({imbalance_price})
    END                                                      AS {prefix}_settlement_eur,
    CASE WHEN {guard} OR p.price_day_ahead IS NULL THEN NULL
         ELSE {dev} * p.interval_hours
              * (p.price_day_ahead - ({imbalance_price}))
    END                                                      AS {prefix}_cost_eur"""


SETTLEMENT_SQL = f"""
CREATE OR REPLACE TABLE imbalance_settlement AS
WITH paced AS (
    SELECT
        *,
        -- Derive the settlement period from the data rather than assuming 15
        -- or 60 minutes: resolution differs by zone, changed mid-history, and
        -- a DST day contains an interval of a different length.
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
    p.price_day_ahead,
    p.price_long,
    p.price_short,
    {_settle("tso_forecast_mw", "tso")},
    {_settle("candidate_mw", "candidate")},
    -- The headline. Positive means the candidate schedule cost less to settle
    -- than the TSO's own forecast would have.
    CASE
        WHEN p.candidate_mw IS NULL OR p.price_day_ahead IS NULL THEN NULL
        ELSE (
            (p.actual_mw - p.tso_forecast_mw) * p.interval_hours
              * (p.price_day_ahead - CASE WHEN (p.actual_mw - p.tso_forecast_mw) >= 0
                                          THEN p.price_long ELSE p.price_short END)
            -
            (p.actual_mw - p.candidate_mw) * p.interval_hours
              * (p.price_day_ahead - CASE WHEN (p.actual_mw - p.candidate_mw) >= 0
                                          THEN p.price_long ELSE p.price_short END)
        )
    END                                                      AS savings_eur
FROM paced AS p
ORDER BY p.valid_time_utc
"""


@dg.asset(
    deps=["candidate_forecast"],
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
) -> dg.MaterializeResult:
    with duckdb.get_connection() as connection:
        connection.execute(SETTLEMENT_SQL)
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
