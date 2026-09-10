"""Exercise the ENTSO-E transforms against real API payloads.

The fixtures in tests/fixtures/ are genuine responses captured from the
Transparency Platform on 2026-09-07, the day before the legacy API host began
returning 404 for every route. They are what stops that outage from also being
a hole in the test suite: the parsing paths stay covered whether or not the
upstream API is reachable.
"""

from pathlib import Path

import pandas as pd
import pytest

from enerdat.assets.entsoe import _net_generation, _to_long

FIXTURES = Path(__file__).parent / "fixtures"
RETRIEVED_AT = pd.Timestamp("2026-09-08T06:00:00Z")


def _load(name):
    path = FIXTURES / f"{name}.pkl"
    if not path.exists():
        pytest.skip(f"fixture {name} not captured")
    return pd.read_pickle(path)


@pytest.mark.parametrize("name", ["wind_solar_forecast", "imbalance"])
def test_flat_payloads_convert_to_long(name):
    frame = _load(name)
    long = _to_long(frame, RETRIEVED_AT)

    assert set(long.columns) == {
        "valid_time_utc", "variable", "value", "zone", "delivery_date",
        "retrieved_at_utc",
    }
    assert str(long["valid_time_utc"].dt.tz) == "UTC"
    assert long["variable"].nunique() == frame.shape[1]
    assert len(long) <= frame.shape[0] * frame.shape[1]
    assert pd.api.types.is_numeric_dtype(long["value"])
    assert long["value"].notna().all(), "nulls must be dropped, not carried"


def test_multiindex_generation_flattens_to_net():
    frame = _load("generation")
    assert isinstance(frame.columns, pd.MultiIndex), "fixture lost its MultiIndex"

    flat = _net_generation(frame)
    assert not isinstance(flat.columns, pd.MultiIndex)

    technologies = set(frame.columns.get_level_values(0))
    assert set(flat.columns) == technologies

    # Pumped storage nets consumption against generation, so it can go negative;
    # a technology that only generates cannot.
    long = _to_long(flat, RETRIEVED_AT)
    assert len(long) > 0
    assert str(long["valid_time_utc"].dt.tz) == "UTC"


def test_net_generation_subtracts_consumption():
    frame = _load("generation")
    levels = frame.columns.get_level_values(1)
    if "Actual Consumption" not in levels:
        pytest.skip("fixture has no consumption column")

    consuming = [
        c for c in frame.columns.get_level_values(0).unique()
        if (c, "Actual Consumption") in frame.columns
        and (c, "Actual Aggregated") in frame.columns
    ]
    assert consuming, "fixture should contain at least one storage technology"

    flat = _net_generation(frame)
    tech = consuming[0]
    expected = (
        frame[(tech, "Actual Aggregated")].fillna(0)
        - frame[(tech, "Actual Consumption")].fillna(0)
    )
    pd.testing.assert_series_equal(
        flat[tech].fillna(0), expected, check_names=False, check_freq=False
    )


def test_market_resolution_is_quarter_hourly():
    """Day-ahead moved to 15-minute MTU; a backfill crosses that boundary."""
    frame = _load("wind_solar_forecast")
    step = frame.index.to_series().diff().median()
    assert step == pd.Timedelta(minutes=15), (
        f"fixture resolution is {step}; the mart's hourly weather join assumes "
        "sub-hourly market data"
    )
