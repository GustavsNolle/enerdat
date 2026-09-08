"""Raw ENTSO-E assets: one partition per delivery day, appended immutably."""

import dagster as dg
from dagster import AssetExecutionContext
import pandas as pd

from enerdat.config import MARKET_TZ, ZONE
from enerdat.partitions import daily_partitions, delivery_window
from enerdat.resources import EntsoeResource, LakeResource


def _to_long(
    frame: pd.DataFrame | pd.Series,
    partition_key: str,
    retrieved_at: pd.Timestamp,
) -> pd.DataFrame:
    """Wide, market-time-indexed entsoe-py output -> long UTC records."""
    if isinstance(frame, pd.Series):
        frame = frame.to_frame(name="value")

    frame = frame.copy()
    frame.index = pd.to_datetime(frame.index, utc=True)
    frame.index.name = "valid_time_utc"

    long = (
        frame.reset_index()
        .melt(id_vars="valid_time_utc", var_name="variable", value_name="value")
        .dropna(subset=["value"])
    )
    long["value"] = pd.to_numeric(long["value"], errors="coerce")
    long = long.dropna(subset=["value"])
    long["zone"] = ZONE
    long["delivery_date"] = partition_key
    long["retrieved_at_utc"] = retrieved_at
    return long


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


def _materialise(
    context: AssetExecutionContext,
    lake: LakeResource,
    dataset: str,
    frame,
) -> dg.MaterializeResult:
    partition_key = context.partition_key
    retrieved_at = pd.Timestamp.now(tz="UTC")

    if frame is None or len(frame) == 0:
        context.log.warning(
            f"{dataset}: nothing published for {ZONE} on {partition_key}"
        )
        return dg.MaterializeResult(
            metadata={"rows": 0, "published": False, "zone": ZONE}
        )

    long = _to_long(frame, partition_key, retrieved_at)
    path = lake.write(dataset, partition_key, long, retrieved_at)

    return dg.MaterializeResult(
        metadata={
            "rows": len(long),
            "published": True,
            "zone": ZONE,
            "variables": dg.MetadataValue.json(sorted(long["variable"].unique().tolist())),
            "intervals": int(long["valid_time_utc"].nunique()),
            "retrieved_at_utc": retrieved_at.isoformat(),
            "path": dg.MetadataValue.path(str(path)),
        }
    )


@dg.asset(
    partitions_def=daily_partitions,
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
    start, end = delivery_window(context.partition_key)
    frame = entsoe.fetch(
        "query_wind_and_solar_forecast",
        ZONE,
        start=start,
        end=end,
        psr_type=None,
        process_type="A01",
    )
    return _materialise(context, lake, "entsoe_day_ahead_forecast", frame)


@dg.asset(
    partitions_def=daily_partitions,
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
    start, end = delivery_window(context.partition_key)
    frame = entsoe.fetch("query_generation", ZONE, start=start, end=end, psr_type=None)

    if frame is not None:
        frame = _net_generation(frame)

    return _materialise(context, lake, "entsoe_actual_generation", frame)


@dg.asset(
    partitions_def=daily_partitions,
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
    start, end = delivery_window(context.partition_key)
    frame = entsoe.fetch("query_imbalance_prices", ZONE, start=start, end=end)
    return _materialise(context, lake, "entsoe_imbalance_price", frame)
