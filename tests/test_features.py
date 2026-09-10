"""Physics and lag features, and the rule that keeps the allow-list honest."""

import numpy as np
import pandas as pd
import pytest

from enerdat.assets.model import (
    LAGGED,
    add_lag_features,
    add_physics_features,
    feature_columns,
    power_fraction,
)
from enerdat.config import (
    USE_TSO_FORECAST_AS_FEATURE,
    TURBINE_CUT_IN_MS,
    TURBINE_CUT_OUT_MS,
    TURBINE_RATED_MS,
    WEATHER_LAG_STEPS,
)


def test_power_curve_has_the_right_shape():
    assert power_fraction([0.0])[0] == 0.0
    assert power_fraction([TURBINE_CUT_IN_MS - 0.1])[0] == 0.0
    assert power_fraction([TURBINE_RATED_MS])[0] == pytest.approx(1.0)
    assert power_fraction([TURBINE_CUT_OUT_MS - 0.1])[0] == pytest.approx(1.0)
    mid = power_fraction([(TURBINE_CUT_IN_MS + TURBINE_RATED_MS) / 2])[0]
    assert 0.0 < mid < 1.0


def test_power_curve_is_not_monotonic_at_cut_out():
    """The whole reason this feature exists.

    Above cut-out the fleet shuts down, so a storm and a dead calm produce the
    same output. No monotonic function of wind speed can say that, which is why
    handing the model raw speed alone makes it guess.
    """
    storm = power_fraction([TURBINE_CUT_OUT_MS + 1])[0]
    rated = power_fraction([TURBINE_RATED_MS])[0]
    assert storm == 0.0
    assert rated == 1.0
    assert power_fraction([TURBINE_CUT_OUT_MS + 5])[0] == power_fraction([0.0])[0]


def test_power_curve_rises_with_the_cube_between_cut_in_and_rated():
    speeds = np.linspace(TURBINE_CUT_IN_MS, TURBINE_RATED_MS, 40)
    values = power_fraction(speeds)
    assert np.all(np.diff(values) >= -1e-12), "must be non-decreasing on the ramp"
    # doubling speed in the cubic region multiplies output by far more than two
    low, high = power_fraction([5.0])[0], power_fraction([10.0])[0]
    assert high / low > 4


def test_power_curve_propagates_nan():
    assert np.isnan(power_fraction([np.nan])[0])


def _frame(n=24):
    return pd.DataFrame(
        {
            "valid_time_utc": pd.date_range("2026-07-01T00:00:00Z", periods=n, freq="1h"),
            "wind_speed_100m_v": np.linspace(2, 20, n),
            "ws_site_0": np.linspace(2, 20, n),
        }
    )


def test_lags_look_both_ways():
    """Later hours are legal because these are forecasts, not outturns."""
    out = add_lag_features(add_physics_features(_frame()))
    for step in WEATHER_LAG_STEPS:
        assert f"wind_speed_100m_v_t{step:+d}" in out.columns

    base = out["wind_speed_100m_v"].to_numpy()
    ahead = out["wind_speed_100m_v_t+1"].to_numpy()
    assert ahead[0] == pytest.approx(base[1]), "t+1 must be the next interval"
    behind = out["wind_speed_100m_v_t-1"].to_numpy()
    assert behind[1] == pytest.approx(base[0]), "t-1 must be the previous interval"


def test_delta_captures_the_ramp():
    out = add_lag_features(add_physics_features(_frame()))
    assert out["wind_speed_100m_v_delta"].iloc[1:].gt(0).all()
    assert pd.isna(out["wind_speed_100m_v_delta"].iloc[0])


def test_lags_follow_time_order_not_row_order():
    shuffled = _frame().sample(frac=1, random_state=1)
    out = add_lag_features(add_physics_features(shuffled))
    times = out["valid_time_utc"]
    assert times.is_monotonic_increasing, "must sort by time before shifting"


def test_feature_list_is_an_allow_list_not_a_blocklist():
    """Prices and the TSO forecast sit in the mart and must never be selected.

    A subtractive rule ("everything except the target") would be one careless
    column away from leaking; this asserts the additive one holds.
    """
    available = [
        "wind_speed_100m_v", "wind_speed_100m_model_sd", "power_fraction",
        "hour_sin", "hour_cos", "month",
        # none of these are knowable at gate closure
        "actual_mw", "price_day_ahead", "price_long", "price_short",
        "tso_forecast_mw", "savings_eur", "delivery_date",
    ]
    selected = feature_columns(available)
    # Never admissible: the outturn itself, and prices that are unknown for the
    # delivery day at the moment the schedule is fixed.
    for forbidden in (
        "actual_mw", "price_day_ahead", "price_long", "price_short",
        "savings_eur", "delivery_date",
    ):
        assert forbidden not in selected, f"{forbidden} must never be a feature"
    assert "wind_speed_100m_v" in selected
    assert "wind_speed_100m_model_sd" in selected


def test_model_spread_is_offered_as_a_feature():
    """Cross-model disagreement is what the quantile objective needs."""
    selected = feature_columns(["wind_speed_100m_v", "wind_speed_100m_model_sd"])
    assert "wind_speed_100m_model_sd" in selected


def test_tso_forecast_is_admitted_only_when_the_deadline_allows_it():
    """Article 14.1.D publishes at 18:00 on D-1.

    With DECISION_TIME_LOCAL at or after that hour the forecast is public
    before the schedule is fixed and may be corrected; with an earlier deadline
    it does not exist yet. The flag encodes that, and the feature list must
    follow it rather than deciding on its own.
    """
    selected = feature_columns(["tso_forecast_mw", "wind_speed_100m_v"])
    assert ("tso_forecast_mw" in selected) is USE_TSO_FORECAST_AS_FEATURE


def test_outturn_lags_are_all_publishable_before_the_deadline():
    """The intraday horizon's entire edge, and its entire risk.

    An actual at T-k is published at T-k+ACTUALS_PUBLICATION_LAG and the
    schedule for T is fixed at T-DECISION_LEAD, so k must be at least the sum.
    A shorter lag is a straightforward leak wearing a plausible name.
    """
    from enerdat.config import ACTUAL_LAG_HOURS, MIN_ACTUAL_LAG

    for hours in ACTUAL_LAG_HOURS:
        assert pd.Timedelta(hours=hours) >= pd.Timedelta(MIN_ACTUAL_LAG), (
            f"actual_lag{hours}h is not published by the time the schedule "
            f"is fixed (needs >= {MIN_ACTUAL_LAG})"
        )


def test_outturn_features_are_refused_at_the_day_ahead_horizon():
    """They do not exist yet at 18:00 on D-1, so the two horizons must not
    silently share a feature set."""
    import enerdat.assets.model as model

    frame = pd.DataFrame({
        "valid_time_utc": pd.date_range("2026-07-01T00:00:00Z", periods=48, freq="1h"),
        "actual_mw": np.linspace(100, 200, 48),
        "tso_forecast_mw": np.linspace(110, 190, 48),
    })

    original = model.HORIZON
    try:
        model.HORIZON = "day_ahead"
        out = model.add_outturn_features(frame)
        assert not [c for c in out.columns if c.startswith("actual_lag")]
        model.HORIZON = "intraday"
        out = model.add_outturn_features(frame)
        assert [c for c in out.columns if c.startswith("actual_lag")]
    finally:
        model.HORIZON = original


def test_a_too_short_lag_is_rejected_rather_than_built():
    import enerdat.assets.model as model

    frame = pd.DataFrame({
        "valid_time_utc": pd.date_range("2026-07-01T00:00:00Z", periods=48, freq="1h"),
        "actual_mw": np.linspace(100, 200, 48),
        "tso_forecast_mw": np.linspace(110, 190, 48),
    })
    original = model.ACTUAL_LAG_HOURS
    try:
        model.ACTUAL_LAG_HOURS = (1,)          # not yet published
        with pytest.raises(ValueError, match="not been published"):
            model.add_outturn_features(frame)
    finally:
        model.ACTUAL_LAG_HOURS = original
