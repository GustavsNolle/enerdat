"""Raw ENTSO-E assets: one partition per delivery day, appended immutably.

Partitions are daily because that is the unit of incremental operation, but a
backfill does NOT issue one request per day. ENTSO-E serves an arbitrary date
range in a single response, so these assets carry a single-run backfill policy:
70 days is one request taking about a minute, not 70 requests taking twenty.
The response is then split back out to one Parquet file per delivery day, so
the storage layout is identical either way.
"""

import dagster as dg
import pandas as pd
from dagster import AssetExecutionContext

from enerdat.config import MARKET_TZ, ZONE
from enerdat.partitions import daily_partitions, delivery_window
from enerdat.resources import EntsoeResource, LakeResource


def _range_window(context: AssetExecutionContext):
    """Market-time window spanning every partition in this run."""
    keys = sorted(context.partition_keys)
    start, _ = delivery_window(keys[0])
    _, end = delivery_window(keys[-1])
    return keys, start, end


def _net_generation(frame: pd.DataFrame) -> pd.DataFrame:
    """Flatten entsoe-py's (technology, direction) columns to net generation.

    Zones with pumped storage or grid batteries report both directions, so a
    storage technology can legitimately net negative over an interval.
    """
    if not isinstance(frame.columns, pd.MultiIndex):
        return frame

    levels = frame.columns.get_level_values(1)
    aggregated = frame.xs("Actual Aggregated", axis=1, level=1)
    if "Actual Consumption" not in levels:
        return aggregated

    consumption = frame.xs("Actual Consumption", axis=1, level=1)
    return aggregated.sub(consumption, fill_value=0)


def _to_long(frame, retrieved_at: pd.Timestamp) -> pd.DataFrame:
    """Wide, market-time-indexed entsoe-py output -> long UTC records.

    delivery_date is derived from each timestamp's market-local date rather
    than passed in, so a multi-day response splits correctly and DST days land
    on the right side of midnight.
    """
    if isinstance(frame, pd.Series):
        # Keep the series' own name as the variable label, so single-series
        # endpoints (day-ahead price, net position) are not all labelled the
        # same thing in the lake.
        frame = frame.to_frame(name=frame.name or "value")

    frame = frame.copy()
    frame.index = pd.to_datetime(frame.index, utc=True)
    frame.index.name = "valid_time_utc"

    # melt refuses a value_name that matches an existing column, and a column
    # legitimately called "value" is exactly what an unnamed series produces.
    # Melt into a private name and rename after, so no input can trip it.
    long = (
        frame.reset_index()
        .melt(id_vars="valid_time_utc", var_name="variable", value_name="__value")
        .rename(columns={"__value": "value"})
        .dropna(subset=["value"])
    )
    long["value"] = pd.to_numeric(long["value"], errors="coerce")
    long = long.dropna(subset=["value"])
    long["zone"] = ZONE
    long["delivery_date"] = (
        long["valid_time_utc"].dt.tz_convert(MARKET_TZ).dt.date.astype(str)
    )
    long["retrieved_at_utc"] = retrieved_at
    return long


def _materialise(
    context: AssetExecutionContext,
    lake: LakeResource,
    dataset: str,
    frame,
    keys: list[str],
) -> dg.MaterializeResult:
    retrieved_at = pd.Timestamp.now(tz="UTC")

    if frame is None or len(frame) == 0:
        context.log.warning(
            f"{dataset}: nothing published for {ZONE} across {len(keys)} day(s)"
        )
        return dg.MaterializeResult(
            metadata={"rows": 0, "published": False, "zone": ZONE,
                      "partitions_requested": len(keys), "partitions_written": 0}
        )

    long = _to_long(frame, retrieved_at)
    long = long[long["delivery_date"].isin(set(keys))]

    written = 0
    for delivery_date, part in long.groupby("delivery_date"):
        lake.write(dataset, delivery_date, part, retrieved_at)
        written += 1

    return dg.MaterializeResult(
        metadata={
            "rows": len(long),
            "published": True,
            "zone": ZONE,
            "partitions_requested": len(keys),
            "partitions_written": written,
            "variables": dg.MetadataValue.json(sorted(long["variable"].unique().tolist())),
            "intervals": int(long["valid_time_utc"].nunique()),
            "retrieved_at_utc": retrieved_at.isoformat(),
        }
    )


@dg.asset(
    partitions_def=daily_partitions,
    backfill_policy=dg.BackfillPolicy.single_run(),
    group_name="raw_entsoe",
    kinds={"python", "parquet"},
    description=(
        "TSO day-ahead wind and solar forecast (article 14.1.D). This is the "
        "baseline the project exists to beat, not a feature."
    ),
)
def entsoe_day_ahead_forecast(
    context: AssetExecutionContext,
    entsoe: EntsoeResource,
    lake: LakeResource,
) -> dg.MaterializeResult:
    keys, start, end = _range_window(context)
    frame = entsoe.fetch_range(
        "query_wind_and_solar_forecast",
        ZONE,
        start=start,
        end=end,
        psr_type=None,
        process_type="A01",
    )
    return _materialise(context, lake, "entsoe_day_ahead_forecast", frame, keys)


@dg.asset(
    partitions_def=daily_partitions,
    backfill_policy=dg.BackfillPolicy.single_run(),
    group_name="raw_entsoe",
    kinds={"python", "parquet"},
    description=(
        "Realised generation per production type (article 16.1.B/C) -- the "
        "prediction target. Values are restated after publication, so every "
        "retrieval is kept rather than overwritten."
    ),
)
def entsoe_actual_generation(
    context: AssetExecutionContext,
    entsoe: EntsoeResource,
    lake: LakeResource,
) -> dg.MaterializeResult:
    keys, start, end = _range_window(context)
    frame = entsoe.fetch_range("query_generation", ZONE, start=start, end=end, psr_type=None)

    if frame is not None:
        frame = _net_generation(frame)

    return _materialise(context, lake, "entsoe_actual_generation", frame, keys)


@dg.asset(
    partitions_def=daily_partitions,
    backfill_policy=dg.BackfillPolicy.single_run(),
    group_name="raw_entsoe",
    kinds={"python", "parquet"},
    description=(
        "Imbalance settlement price (article 17.1.F/G) -- the exchange rate "
        "that turns forecast error in MWh into euros. Not published at "
        "bidding-zone level by every TSO; empty partitions are expected."
    ),
)
def entsoe_imbalance_price(
    context: AssetExecutionContext,
    entsoe: EntsoeResource,
    lake: LakeResource,
) -> dg.MaterializeResult:
    keys, start, end = _range_window(context)
    frame = entsoe.fetch_range("query_imbalance_prices", ZONE, start=start, end=end)
    return _materialise(context, lake, "entsoe_imbalance_price", frame, keys)


@dg.asset(
    partitions_def=daily_partitions,
    backfill_policy=dg.BackfillPolicy.single_run(),
    group_name="raw_entsoe",
    kinds={"python", "parquet"},
    description=(
        "Day-ahead auction clearing price (article 12.1.D). Needed because "
        "imbalance cost is only well defined relative to it: the schedule was "
        "already sold at this price, so the cost of deviating is the spread "
        "between it and the imbalance price, not the imbalance price itself."
    ),
)
def entsoe_day_ahead_price(
    context: AssetExecutionContext,
    entsoe: EntsoeResource,
    lake: LakeResource,
) -> dg.MaterializeResult:
    keys, start, end = _range_window(context)
    series = entsoe.fetch_range("query_day_ahead_prices", ZONE, start=start, end=end)
    if series is not None:
        series = series.rename("day_ahead_price")
    return _materialise(context, lake, "entsoe_day_ahead_price", series, keys)
