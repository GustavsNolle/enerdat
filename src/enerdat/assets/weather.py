"""Archived weather forecasts, pinned to a lead time that predates gate closure."""

import dagster as dg
import pandas as pd
from dagster import AssetExecutionContext

from enerdat.config import (
    GRID_POINTS,
    MARKET_TZ,
    OPEN_METEO_MODELS,
    WEATHER_LEAD_DAYS_OPTIONS,
    WEATHER_VARIABLES,
)
from enerdat.partitions import daily_partitions, decision_deadline, delivery_window
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
            leads=WEATHER_LEAD_DAYS_OPTIONS,
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

    # Conservative bound on when each run was issued. The real run is at or
    # before this instant, so proving this <= gate closure proves the feature
    # was knowable in time.
    combined["issue_time_utc"] = combined["valid_time_utc"] - pd.to_timedelta(
        combined["lead_days"], unit="D"
    )
    combined["retrieved_at_utc"] = retrieved_at

    # The deadline is per delivery day, so the legality of a given lead varies
    # across the day.
    closures = {key: decision_deadline(key) for key in keys}
    combined["gate_closure_utc"] = combined["delivery_date"].map(closures)

    # Drop everything illegal FIRST, then keep the freshest survivor. Selection
    # can only ever choose among rows that already predate the deadline, so no
    # ordering mistake here can produce a leak.
    legal = combined[combined["issue_time_utc"] <= combined["gate_closure_utc"]]
    dropped = len(combined) - len(legal)

    fresh = (
        legal.sort_values("lead_days")
        .drop_duplicates(
            subset=["site", "model", "variable", "valid_time_utc"], keep="first"
        )
        .sort_values(["valid_time_utc", "site", "variable"])
    )

    if fresh.empty:
        raise ValueError(
            "No legal weather rows survived the gate-closure filter. Every "
            "candidate lead in WEATHER_LEAD_DAYS_OPTIONS is issued too late."
        )

    # Belt and braces: the filter above should make this impossible.
    assert (fresh["issue_time_utc"] <= fresh["gate_closure_utc"]).all()

    combined = fresh
    written = 0
    for delivery_date, part in combined.groupby("delivery_date"):
        lake.write("openmeteo_archived_forecast", delivery_date, part, retrieved_at)
        written += 1

    headroom_hours = (
        (combined["gate_closure_utc"] - combined["issue_time_utc"])
        .dt.total_seconds()
        .div(3600)
        .min()
    )
    lead_mix = combined["lead_days"].value_counts().sort_index().to_dict()

    return dg.MaterializeResult(
        metadata={
            "rows": len(combined),
            "sites": len(GRID_POINTS),
            "models": dg.MetadataValue.json(list(OPEN_METEO_MODELS)),
            "requests": len(GRID_POINTS),
            "partitions_requested": len(keys),
            "partitions_written": written,
            "leads_offered": dg.MetadataValue.json(list(WEATHER_LEAD_DAYS_OPTIONS)),
            "rows_dropped_as_too_late": dropped,
            "lead_days_used": dg.MetadataValue.json(
                {str(k): int(v) for k, v in lead_mix.items()}
            ),
            "min_headroom_hours": round(float(headroom_hours), 1),
        }
    )
