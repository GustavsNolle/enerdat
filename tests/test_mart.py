"""The mart's two load-bearing behaviours: dedup of revisions, and no leakage."""

import dagster as dg
import pandas as pd
import pytest
from dagster_duckdb import DuckDBResource

from enerdat.assets.marts import forecast_error_mart
from enerdat.checks import leakage_free_features, market_day_length, unique_intervals
from enerdat.config import TARGET_TECHNOLOGY
from enerdat.partitions import delivery_window, gate_closure
from enerdat.resources import LakeResource

DELIVERY_DATE = "2026-08-28"
SITES = [("Borssele", 1.0), ("Gemini", 3.0)]  # deliberately unequal weights


def _quarter_hours(partition_key):
    start, end = delivery_window(partition_key)
    return pd.date_range(
        start.tz_convert("UTC"), end.tz_convert("UTC"), freq="15min", inclusive="left"
    )


def _write_entsoe(lake, dataset, values, retrieved_at):
    index = _quarter_hours(DELIVERY_DATE)
    frame = pd.DataFrame(
        {
            "valid_time_utc": index,
            "variable": TARGET_TECHNOLOGY,
            "value": values(index),
            "zone": "NL",
            "delivery_date": DELIVERY_DATE,
            "retrieved_at_utc": retrieved_at,
        }
    )
    lake.write(dataset, DELIVERY_DATE, frame, retrieved_at)


def _write_weather(lake, issue_offset_days, retrieved_at):
    start, end = delivery_window(DELIVERY_DATE)
    hours = pd.date_range(
        start.tz_convert("UTC"), end.tz_convert("UTC"), freq="1h", inclusive="left"
    )
    rows = []
    for site, weight in SITES:
        for hour in hours:
            rows.append(
                {
                    "valid_time_utc": hour,
                    "variable": "wind_speed_100m",
                    "value": 10.0 if site == "Borssele" else 14.0,
                    "lead_days": issue_offset_days,
                    "site": site,
                    "weight": weight,
                    "issue_time_utc": hour - pd.Timedelta(days=issue_offset_days),
                    "delivery_date": DELIVERY_DATE,
                    "model": "test",
                    "retrieved_at_utc": retrieved_at,
                }
            )
    lake.write("openmeteo_archived_forecast", DELIVERY_DATE, pd.DataFrame(rows), retrieved_at)


def _build(tmp_path, issue_offset_days):
    lake = LakeResource(root=str(tmp_path / "raw"))
    duckdb = DuckDBResource(database=str(tmp_path / "test.duckdb"))
    t0 = pd.Timestamp("2026-08-29T06:00:00Z")

    _write_entsoe(lake, "entsoe_day_ahead_forecast", lambda i: 100.0, t0)
    # Two retrievals of the same intervals: ENTSO-E restated the actuals.
    # The mart must take the later one.
    _write_entsoe(lake, "entsoe_actual_generation", lambda i: 111.0, t0)
    _write_entsoe(
        lake,
        "entsoe_actual_generation",
        lambda i: 222.0,
        t0 + pd.Timedelta(days=1),
    )
    _write_weather(lake, issue_offset_days, t0)

    resources = {"lake": lake, "duckdb": duckdb}
    result = dg.materialize([forecast_error_mart], resources=resources)
    assert result.success
    return duckdb, resources


def test_mart_takes_the_latest_revision(tmp_path):
    duckdb, _ = _build(tmp_path, issue_offset_days=2)
    with duckdb.get_connection() as conn:
        rows, distinct, actuals = conn.execute(
            """
            SELECT count(*), count(DISTINCT valid_time_utc),
                   count(DISTINCT actual_mw)
            FROM forecast_error
            """
        ).fetchone()
        value = conn.execute("SELECT DISTINCT actual_mw FROM forecast_error").fetchone()[0]

    assert rows == 96, "a 24-hour day at 15-minute resolution is 96 intervals"
    assert rows == distinct, "deduplication left duplicate intervals"
    assert actuals == 1
    assert value == 222.0, "the mart kept the superseded first retrieval"


def test_weather_is_capacity_weighted(tmp_path):
    duckdb, _ = _build(tmp_path, issue_offset_days=2)
    with duckdb.get_connection() as conn:
        weighted = conn.execute(
            "SELECT DISTINCT wind_speed_100m_v FROM forecast_error"
        ).fetchone()[0]
    # (10*1 + 14*3) / 4 = 13.0 -- not the unweighted mean of 12.0
    assert weighted == pytest.approx(13.0)


def test_leakage_check_passes_at_safe_lead_time(tmp_path):
    duckdb, resources = _build(tmp_path, issue_offset_days=2)
    result = dg.materialize(
        [forecast_error_mart, leakage_free_features, market_day_length, unique_intervals],
        resources=resources,
        selection=dg.AssetSelection.checks_for_assets(forecast_error_mart),
    )
    assert result.success
    for evaluation in result.get_asset_check_evaluations():
        assert evaluation.passed, f"{evaluation.check_name} failed unexpectedly"


def test_leakage_check_catches_a_leak(tmp_path):
    """The check must fail when features come from a post-gate-closure run.

    Lead time 0 means the weather run was issued at valid time itself -- deep
    inside the delivery day, long after the schedule was submitted. If this
    test ever passes, the guarantee is worthless.
    """
    duckdb, resources = _build(tmp_path, issue_offset_days=0)

    closure = gate_closure(DELIVERY_DATE)
    with duckdb.get_connection() as conn:
        latest = conn.execute(
            "SELECT max(wind_speed_100m_issued) FROM forecast_error"
        ).fetchone()[0]
    assert pd.Timestamp(latest).tz_convert("UTC") > closure, "fixture is not leaking"

    result = dg.materialize(
        [forecast_error_mart, leakage_free_features],
        resources=resources,
        selection=dg.AssetSelection.checks_for_assets(forecast_error_mart),
        raise_on_error=False,
    )
    evaluations = list(result.get_asset_check_evaluations())
    assert evaluations, "the leakage check did not run"
    assert not evaluations[0].passed, "leaking features were not detected"
