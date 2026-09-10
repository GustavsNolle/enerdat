"""Archived weather forecasts, pinned to a lead time that predates gate closure."""

import dagster as dg
import pandas as pd
from dagster import AssetExecutionContext

from enerdat.config import (
    GRID_POINTS,
    MARKET_TZ,
    OPEN_METEO_MODEL,
    WEATHER_LEAD_DAYS,
    WEATHER_VARIABLES,
)
from enerdat.partitions import daily_partitions, delivery_window, gate_closure
from enerdat.resources import LakeResource, OpenMeteoResource


@dg.asset(
    partitions_def=daily_partitions,
    backfill_policy=dg.BackfillPolicy.single_run(),
    group_name="raw_weather",
    kinds={"python", "parquet"},
    description=(
        "Hourly archived forecasts at each offshore wind site, taken from the "
        "model run issued WEATHER_LEAD_DAYS before valid time. Sourced from "
        "Open-Meteo's historical-FORECAST API; the reanalysis archive would "
        "leak future information and is never used. Open-Meteo serves a date "
        "range per request, so a backfill costs one request per site, not one "
        "per site per day."
    ),
)
def openmeteo_archived_forecast(
    context: AssetExecutionContext,
    open_meteo: OpenMeteoResource,
    lake: LakeResource,
) -> dg.MaterializeResult:
    keys = sorted(context.partition_keys)
    start_local, _ = delivery_window(keys[0])
    _, end_local = delivery_window(keys[-1])
    start_utc = start_local.tz_convert("UTC")
    end_utc = end_local.tz_convert("UTC")

    # The market day straddles two UTC dates, so widen the request and clip.
    request_start = (start_utc - pd.Timedelta(hours=1)).date().isoformat()
    request_end = end_utc.date().isoformat()

    retrieved_at = pd.Timestamp.now(tz="UTC")

    frames = []
    for point in GRID_POINTS:
        long = open_meteo.fetch_point(
            lat=point["lat"],
            lon=point["lon"],
            start_date=request_start,
            end_date=request_end,
            variables=WEATHER_VARIABLES,
            lead_days=WEATHER_LEAD_DAYS,
        )
        long["site"] = point["name"]
        long["weight"] = point["weight"]
        frames.append(long)

    combined = pd.concat(frames, ignore_index=True).dropna(subset=["value"])
    combined = combined[
        (combined["valid_time_utc"] >= start_utc)
        & (combined["valid_time_utc"] < end_utc)
    ]
    combined["delivery_date"] = (
        combined["valid_time_utc"].dt.tz_convert(MARKET_TZ).dt.date.astype(str)
    )
    combined = combined[combined["delivery_date"].isin(set(keys))]

    # Conservative bound on when the source model run was issued. The real run
    # is at or before this instant, so proving this <= gate closure proves the
    # feature was knowable in time.
    combined["issue_time_utc"] = combined["valid_time_utc"] - pd.Timedelta(
        days=WEATHER_LEAD_DAYS
    )
    combined["model"] = OPEN_METEO_MODEL
    combined["retrieved_at_utc"] = retrieved_at

    # Checked per delivery day rather than against the range maximum: one
    # leaking day inside an otherwise clean backfill must still fail.
    closures = {key: gate_closure(key) for key in keys}
    deadline = combined["delivery_date"].map(closures)
    leaking = combined[combined["issue_time_utc"] > deadline]
    if len(leaking):
        worst = leaking["delivery_date"].unique()[:5]
        raise ValueError(
            f"Refusing to land leaking features: {len(leaking)} rows across "
            f"{leaking['delivery_date'].nunique()} day(s) have an implied issue "
            f"time after gate closure (e.g. {list(worst)}). Raise "
            "WEATHER_LEAD_DAYS."
        )

    written = 0
    for delivery_date, part in combined.groupby("delivery_date"):
        lake.write("openmeteo_archived_forecast", delivery_date, part, retrieved_at)
        written += 1

    headroom_hours = (
        ((deadline - combined["issue_time_utc"]).dt.total_seconds() / 3600).min()
        if len(combined)
        else 0
    )

    return dg.MaterializeResult(
        metadata={
            "rows": len(combined),
            "sites": len(GRID_POINTS),
            "requests": len(GRID_POINTS),
            "partitions_requested": len(keys),
            "partitions_written": written,
            "lead_days": WEATHER_LEAD_DAYS,
            "min_headroom_hours": round(float(headroom_hours), 1),
        }
    )
