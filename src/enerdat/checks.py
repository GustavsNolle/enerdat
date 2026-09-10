"""Asset checks. The first one is the reason this project is credible."""

from __future__ import annotations

import dagster as dg
from dagster_duckdb import DuckDBResource


@dg.asset_check(
    asset="forecast_error_mart",
    blocking=True,
    description=(
        "Proves no feature post-dates gate closure. Every weather column "
        "carries the issue time of the model run it came from; each must be "
        "at or before 12:00 market time on D-1."
    ),
)
def leakage_free_features(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    with duckdb.get_connection() as connection:
        issued_columns = [
            row[0]
            for row in connection.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'forecast_error'
                  AND column_name LIKE '%_issued'
                ORDER BY column_name
                """
            ).fetchall()
        ]

        if not issued_columns:
            return dg.AssetCheckResult(
                passed=False,
                severity=dg.AssetCheckSeverity.ERROR,
                description=(
                    "No *_issued columns found -- the mart cannot prove its "
                    "features predate gate closure."
                ),
            )

        # A single row violating the bound fails the check. Reported per column
        # so a regression points at the feature that caused it.
        violations = {}
        worst_headroom = None
        for column in issued_columns:
            count, headroom = connection.execute(
                f"""
                SELECT
                    count(*) FILTER (WHERE "{column}" > gate_closure_utc),
                    min(date_diff('minute', "{column}", gate_closure_utc))
                FROM forecast_error
                WHERE "{column}" IS NOT NULL
                """
            ).fetchone()
            if count:
                violations[column] = count
            if headroom is not None:
                worst_headroom = (
                    headroom if worst_headroom is None else min(worst_headroom, headroom)
                )

    passed = not violations
    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.ERROR,
        description=(
            "Every feature predates gate closure."
            if passed
            else f"Leaking columns: {violations}"
        ),
        metadata={
            "columns_checked": len(issued_columns),
            "leaking_columns": len(violations),
            # How close the tightest feature runs to the deadline. Small and
            # positive is fine; negative would mean a leak.
            "min_headroom_minutes": worst_headroom if worst_headroom is not None else -1,
        },
    )


@dg.asset_check(
    asset="forecast_error_mart",
    description=(
        "Market days have 23, 24 or 25 hours. Any other length means the "
        "delivery window was built with fixed 24-hour arithmetic across a DST "
        "transition, silently dropping or duplicating an hour."
    ),
)
def market_day_length(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    with duckdb.get_connection() as connection:
        rows = connection.execute(
            """
            WITH per_day AS (
                SELECT delivery_date,
                       count(*)                                  AS intervals,
                       count(DISTINCT valid_time_utc)            AS distinct_intervals,
                       date_diff('hour', min(valid_time_utc), max(valid_time_utc)) + 1
                                                                 AS span_hours
                FROM forecast_error
                GROUP BY delivery_date
            )
            SELECT delivery_date, intervals, span_hours
            FROM per_day
            WHERE span_hours NOT IN (23, 24, 25)
            ORDER BY delivery_date
            LIMIT 10
            """
        ).fetchall()

        complete, total = connection.execute(
            """
            WITH per_day AS (
                SELECT delivery_date,
                       date_diff('hour', min(valid_time_utc), max(valid_time_utc)) + 1
                           AS span_hours
                FROM forecast_error GROUP BY delivery_date
            )
            SELECT count(*) FILTER (WHERE span_hours IN (23, 24, 25)), count(*)
            FROM per_day
            """
        ).fetchone()

    passed = not rows
    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.WARN,
        description=(
            f"All {total} delivery days span a legal market-day length."
            if passed
            else f"Irregular days: {rows}"
        ),
        metadata={"days_checked": total, "days_ok": complete},
    )


@dg.asset_check(
    asset="forecast_error_mart",
    description="One row per market interval -- deduplication actually worked.",
)
def unique_intervals(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    with duckdb.get_connection() as connection:
        total, distinct = connection.execute(
            "SELECT count(*), count(DISTINCT valid_time_utc) FROM forecast_error"
        ).fetchone()

    return dg.AssetCheckResult(
        passed=total == distinct,
        severity=dg.AssetCheckSeverity.ERROR,
        description=(
            "No duplicate intervals."
            if total == distinct
            else f"{total - distinct} duplicate intervals survived deduplication."
        ),
        metadata={"rows": total, "distinct_intervals": distinct},
    )


@dg.asset_check(
    asset="forecast_error_mart",
    blocking=True,
    description=(
        "Forecast and actual must describe the same fleet. TSOs vary in what "
        "their metered actuals cover: where distributed generation is excluded "
        "from article 16.1 but included in the 14.1.D forecast, subtracting one "
        "from the other measures scope, not error."
    ),
)
def forecast_actual_comparable(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    """Catches a scope mismatch before it becomes a fictional euro figure.

    In NL this fires hard: metered solar is 3.6% of the forecast and metered
    offshore wind is 283% of it, because Dutch distributed generation never
    reaches TenneT's aggregate. The resulting "forecast error" would have been
    almost entirely definitional.
    """
    with duckdb.get_connection() as connection:
        row = connection.execute(
            """
            SELECT
                count(*),
                avg(tso_forecast_mw),
                avg(actual_mw),
                corr(tso_forecast_mw, actual_mw)
            FROM forecast_error
            WHERE actual_mw IS NOT NULL AND tso_forecast_mw IS NOT NULL
            """
        ).fetchone()

    n, mean_forecast, mean_actual, correlation = row
    if not n or not mean_forecast:
        return dg.AssetCheckResult(
            passed=False,
            severity=dg.AssetCheckSeverity.ERROR,
            description="No overlapping forecast/actual intervals to compare.",
            metadata={"intervals": n or 0},
        )

    ratio = mean_actual / mean_forecast
    # Bands are deliberately wide: a real forecast bias of +/-30% is plausible,
    # a factor of three is a different definition.
    ratio_ok = 0.7 <= ratio <= 1.4
    corr_ok = correlation is not None and correlation >= 0.85
    passed = ratio_ok and corr_ok

    problems = []
    if not ratio_ok:
        problems.append(f"mean actual / mean forecast = {ratio:.3f}, outside 0.70-1.40")
    if not corr_ok:
        problems.append(f"correlation {correlation:.3f} below 0.85")

    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.ERROR,
        description=(
            f"Forecast and actual are on the same scale (ratio {ratio:.3f}, "
            f"corr {correlation:.3f})."
            if passed
            else "Scope mismatch: " + "; ".join(problems)
        ),
        metadata={
            "intervals": n,
            "mean_forecast_mw": round(mean_forecast, 1),
            "mean_actual_mw": round(mean_actual, 1),
            "ratio": round(ratio, 4),
            "correlation": round(correlation, 4) if correlation is not None else -1,
        },
    )
