"""The mart's two load-bearing behaviours: dedup of revisions, and no leakage."""

import dagster as dg
import numpy as np
import pandas as pd
import pytest
from dagster_duckdb import DuckDBResource

from enerdat.assets.marts import forecast_error_mart
from enerdat.checks import (
    forecast_actual_comparable,
    leakage_free_features,
    market_day_length,
    unique_intervals,
)
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


def _write_weather_models(lake, retrieved_at, per_model_site_speeds):
    """Weather for several models at several sites, with controlled values.

    `per_model_site_speeds` maps model -> {site: wind speed}, so a test can
    dictate exactly how much models disagree and how much sites disagree.
    """
    start, end = delivery_window(DELIVERY_DATE)
    hours = pd.date_range(
        start.tz_convert("UTC"), end.tz_convert("UTC"), freq="1h", inclusive="left"
    )
    weights = dict(SITES)
    rows = []
    for model, site_speeds in per_model_site_speeds.items():
        for site, speed in site_speeds.items():
            for hour in hours:
                rows.append({
                    "valid_time_utc": hour,
                    "variable": "wind_speed_100m",
                    "value": float(speed),
                    "lead_days": 2,
                    "site": site,
                    "weight": weights[site],
                    "model": model,
                    "issue_time_utc": hour - pd.Timedelta(days=2),
                    "delivery_date": DELIVERY_DATE,
                    "retrieved_at_utc": retrieved_at,
                })
    lake.write("openmeteo_archived_forecast", DELIVERY_DATE, pd.DataFrame(rows), retrieved_at)


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


def _write_prices(lake, retrieved_at):
    """Imbalance and day-ahead prices, which the mart now carries for settlement."""
    index = _quarter_hours(DELIVERY_DATE)
    for dataset, rows in {
        "entsoe_imbalance_price": [("Long", 40.0), ("Short", 160.0)],
        "entsoe_day_ahead_price": [("day_ahead_price", 100.0)],
    }.items():
        frame = pd.concat(
            [
                pd.DataFrame({
                    "valid_time_utc": index, "variable": name, "value": value,
                    "zone": "BE", "delivery_date": DELIVERY_DATE,
                    "retrieved_at_utc": retrieved_at,
                })
                for name, value in rows
            ],
            ignore_index=True,
        )
        lake.write(dataset, DELIVERY_DATE, frame, retrieved_at)


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
    _write_prices(lake, t0)

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


def _build_with_scale(tmp_path, actual_scale):
    """forecast_error where actual is a fixed multiple of the forecast."""
    lake = LakeResource(root=str(tmp_path / "raw"))
    duckdb = DuckDBResource(database=str(tmp_path / "scale.duckdb"))
    t0 = pd.Timestamp("2026-08-29T06:00:00Z")

    # Both series must vary: correlation against a constant is undefined, and
    # the check treats an undefined correlation as a failure.
    shape = lambda i: 100.0 + np.sin(np.arange(len(i)) / 6) * 40
    _write_entsoe(lake, "entsoe_day_ahead_forecast", shape, t0)
    _write_entsoe(
        lake, "entsoe_actual_generation",
        lambda i: shape(i) * actual_scale + np.cos(np.arange(len(i))) * 3, t0,
    )
    _write_weather(lake, 2, t0)
    _write_prices(lake, t0)

    resources = {"lake": lake, "duckdb": duckdb}
    assert dg.materialize([forecast_error_mart], resources=resources).success
    return resources


def test_scope_check_passes_when_scales_agree(tmp_path):
    resources = _build_with_scale(tmp_path, actual_scale=1.05)
    result = dg.materialize(
        [forecast_error_mart, forecast_actual_comparable],
        resources=resources,
        selection=dg.AssetSelection.checks_for_assets(forecast_error_mart),
        raise_on_error=False,
    )
    evaluation = next(iter(result.get_asset_check_evaluations()))
    assert evaluation.passed


def test_scope_check_catches_the_nl_style_mismatch(tmp_path):
    """NL metered offshore wind was 283% of the forecast. That must fail.

    Without this check a scope mismatch becomes a fictional euro figure, and
    nothing else in the pipeline would have noticed.
    """
    resources = _build_with_scale(tmp_path, actual_scale=2.83)
    result = dg.materialize(
        [forecast_error_mart, forecast_actual_comparable],
        resources=resources,
        selection=dg.AssetSelection.checks_for_assets(forecast_error_mart),
        raise_on_error=False,
    )
    evaluation = next(iter(result.get_asset_check_evaluations()))
    assert not evaluation.passed
    assert "ratio" in str(evaluation.metadata).lower()


def test_model_spread_and_site_spread_are_measured_separately(tmp_path):
    """They are different quantities and must not collapse to one number.

    A standard deviation taken over the flat (site x model) rows returns the
    SAME value for both, mixing the two sources of variation and attributing it
    to whichever column happens to be named. Here the models are made to
    disagree a lot and the sites barely at all, so a shared implementation
    cannot pass.
    """
    lake = LakeResource(root=str(tmp_path / "raw"))
    duckdb = DuckDBResource(database=str(tmp_path / "spread.duckdb"))
    t0 = pd.Timestamp("2026-08-29T06:00:00Z")

    shape = lambda i: 100.0 + np.sin(np.arange(len(i)) / 6) * 40
    _write_entsoe(lake, "entsoe_day_ahead_forecast", shape, t0)
    _write_entsoe(lake, "entsoe_actual_generation", shape, t0)
    _write_prices(lake, t0)

    # Models 5 m/s apart; sites 0.2 m/s apart.
    _write_weather_models(lake, t0, {
        "alpha": {"Borssele": 5.0, "Gemini": 5.2},
        "beta":  {"Borssele": 15.0, "Gemini": 15.2},
    })

    resources = {"lake": lake, "duckdb": duckdb}
    assert dg.materialize([forecast_error_mart], resources=resources).success

    with duckdb.get_connection() as conn:
        model_sd, site_sd = conn.execute(
            """SELECT DISTINCT round(wind_speed_100m_model_sd, 3),
                               round(ws_site_sd, 3)
               FROM forecast_error WHERE wind_speed_100m_model_sd IS NOT NULL"""
        ).fetchone()

    # Two models at ~5.1 and ~15.1 -> sample sd of about 7.07
    assert model_sd == pytest.approx(7.07, abs=0.05)
    # Two sites, each averaged across models -> 10.0 and 10.2 -> sd about 0.14
    assert site_sd == pytest.approx(0.141, abs=0.02)
    assert model_sd > site_sd * 10, "the two spreads collapsed into one number"
