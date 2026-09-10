"""Dagster entry point: `dagster dev` finds this via [tool.dagster] in pyproject."""

from __future__ import annotations

import os

import dagster as dg
from dagster_duckdb import DuckDBResource
from dotenv import load_dotenv

from enerdat import checks
from enerdat.assets import entsoe, marts, model, settlement, weather
from enerdat.config import DUCKDB_PATH, LAKE_ROOT, OPEN_METEO_MODELS
from enerdat.partitions import daily_partitions
from enerdat.resources import EntsoeResource, LakeResource, OpenMeteoResource

load_dotenv()

raw_assets = dg.load_assets_from_modules([entsoe, weather])
mart_assets = dg.load_assets_from_modules([marts, model, settlement])

# Raw ingestion is one API call per partition, so a backfill is a long queue of
# small requests. ENTSO-E allows 400/minute and throttles hard above it; the
# job-level concurrency limit is the thing standing between a two-year backfill
# and a temporary ban.
backfill_raw = dg.define_asset_job(
    name="backfill_raw",
    selection=dg.AssetSelection.groups("raw_entsoe", "raw_weather"),
    partitions_def=daily_partitions,
    config={"execution": {"config": {"multiprocess": {"max_concurrent": 4}}}},
)

build_mart = dg.define_asset_job(
    name="build_mart",
    selection=dg.AssetSelection.groups("marts"),
)

defs = dg.Definitions(
    assets=[*raw_assets, *mart_assets],
    asset_checks=[
        checks.leakage_free_features,
        checks.market_day_length,
        checks.unique_intervals,
        checks.forecast_actual_comparable,
    ],
    jobs=[backfill_raw, build_mart],
    schedules=[
        dg.ScheduleDefinition(
            name="daily_ingest",
            job=backfill_raw,
            # 06:00 market time: the previous delivery day is settled and the
            # next day's forecast is published.
            cron_schedule="0 6 * * *",
            execution_timezone="Europe/Brussels",
        )
    ],
    resources={
        "entsoe": EntsoeResource(
            api_key=dg.EnvVar("ENTSOE_API_KEY"),
            endpoint_url=os.getenv("ENTSOE_ENDPOINT_URL", ""),
        ),
        "open_meteo": OpenMeteoResource(models=list(OPEN_METEO_MODELS)),
        "lake": LakeResource(root=LAKE_ROOT),
        "duckdb": DuckDBResource(database=DUCKDB_PATH),
    },
)
