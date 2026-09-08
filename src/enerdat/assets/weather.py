"""Archived weather forecasts, pinned to a lead time that predates gate closure."""

import dagster as dg
from dagster import AssetExecutionContext
import pandas as pd

from enerdat.config import (
    GRID_POINTS,
    OPEN_METEO_MODEL,
    WEATHER_LEAD_DAYS,
    WEATHER_VARIABLES,
)
from enerdat.partitions import daily_partitions, delivery_window, gate_closure
from enerdat.resources import LakeResource, OpenMeteoResource


@dg.asset(
    partitions_def=daily_partitions,
    group_name="raw_weather",
    kinds={"python", "parquet"},
    description=(
        "Hourly archived forecasts at each offshore wind site, taken from the "
        "model run issued WEATHER_LEAD_DAYS before valid time. Sourced from "
        "Open-Meteo's historical-FORECAST API; the reanalysis archive would "
        "leak future information and is never used."
    ),
)
def openmeteo_archived_forecast(
    context: AssetExecutionContext,
    open_meteo: OpenMeteoResource,
    lake: LakeResource,
) -> dg.MaterializeResult:
    partition_key = context.partition_key
    start_local, end_local = delivery_window(partition_key)
    start_utc = start_local.tz_convert("UTC")
    end_utc = end_local.tz_convert("UTC")

    # The market day straddles two UTC dates, so request both and clip back to
    # the delivery window.
    request_start = (start_utc - pd.Timedelta(hours=1)).date().isoformat()
    request_end = end_utc.date().isoformat()

    retrieved_at = pd.Timestamp.now(tz="UTC")
    closure = gate_closure(partition_key)

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

    combined = pd.concat(frames, ignore_index=True)
    combined = combined[
        (combined["valid_time_utc"] >= start_utc)
        & (combined["valid_time_utc"] < end_utc)
    ].dropna(subset=["value"])

    # Conservative bound on when the source model run was issued. The real run
    # is at or before this instant, so proving this <= gate closure proves the
    # feature was knowable in time.
    combined["issue_time_utc"] = combined["valid_time_utc"] - pd.Timedelta(
        days=WEATHER_LEAD_DAYS
    )
    combined["delivery_date"] = partition_key
    combined["model"] = OPEN_METEO_MODEL
    combined["retrieved_at_utc"] = retrieved_at

    latest_issue = combined["issue_time_utc"].max()
    if len(combined) and latest_issue > closure:
        raise ValueError(
            f"Refusing to land leaking features for {partition_key}: latest "
            f"implied issue time {latest_issue} is after gate closure {closure}. "
            f"Raise WEATHER_LEAD_DAYS."
        )

    path = lake.write("openmeteo_archived_forecast", partition_key, combined, retrieved_at)

    return dg.MaterializeResult(
        metadata={
            "rows": len(combined),
            "sites": len(GRID_POINTS),
            "lead_days": WEATHER_LEAD_DAYS,
            "gate_closure_utc": closure.isoformat(),
            "latest_issue_time_utc": (
                latest_issue.isoformat() if len(combined) else "n/a"
            ),
            "headroom_hours": (
                round((closure - latest_issue).total_seconds() / 3600, 1)
                if len(combined)
                else 0
            ),
            "path": dg.MetadataValue.path(str(path)),
        }
    )
