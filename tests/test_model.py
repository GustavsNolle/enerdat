"""Walk-forward causality: no delivery day may be predicted by a model that saw it."""

import numpy as np
import pandas as pd
import pytest

from enerdat.assets.model import (
    add_calendar_features,
    feature_columns,
    walk_forward_predict,
)

LAG = pd.Timedelta(hours=1)


def _history(days=60, start="2026-01-01"):
    """Synthetic mart: wind speed drives output, with noise."""
    rng = np.random.default_rng(0)
    index = pd.date_range(f"{start}T00:00:00Z", periods=days * 96, freq="15min")
    wind = 8 + 5 * np.sin(np.arange(len(index)) / 180) + rng.normal(0, 1.2, len(index))

    frame = pd.DataFrame(
        {
            "valid_time_utc": index,
            "delivery_date": index.tz_convert("Europe/Brussels").date,
            "wind_speed_100m_v": wind,
            "actual_mw": np.clip(wind, 0, None) ** 3 * 0.9 + rng.normal(0, 20, len(index)),
            "tso_forecast_mw": np.clip(wind, 0, None) ** 3 * 0.9,
        }
    )
    local_midnight = pd.to_datetime(frame["delivery_date"]).dt.tz_localize("Europe/Brussels")
    frame["gate_closure_utc"] = (
        local_midnight - pd.Timedelta(days=1) + pd.Timedelta(hours=12)
    ).dt.tz_convert("UTC")
    return frame


def test_training_never_reaches_past_the_cutoff():
    """The audit trail is the proof, and this is the assertion that reads it."""
    frame = add_calendar_features(_history())
    features = feature_columns(list(frame.columns))
    _, audit = walk_forward_predict(frame, features, retrain_days=7, min_train_days=10, lag=LAG)

    trained = audit.dropna(subset=["max_train_valid_time_utc"])
    assert len(trained) > 0, "no day ever trained; fixture too small"

    for row in trained.itertuples():
        assert row.max_train_valid_time_utc <= row.cutoff_utc, (
            f"{row.delivery_date} trained on data up to "
            f"{row.max_train_valid_time_utc}, past its cutoff {row.cutoff_utc}"
        )


def test_cutoff_respects_the_publication_lag():
    frame = add_calendar_features(_history())
    features = feature_columns(list(frame.columns))
    _, audit = walk_forward_predict(frame, features, retrain_days=7, min_train_days=10, lag=LAG)

    for row in audit.itertuples():
        expected = pd.Timestamp(row.cutoff_utc) + LAG
        gate = frame.loc[
            frame["delivery_date"] == row.delivery_date, "gate_closure_utc"
        ].iloc[0]
        assert expected == gate, "cutoff must be gate closure minus the lag"


def test_model_abstains_until_history_is_long_enough():
    frame = add_calendar_features(_history(days=20))
    features = feature_columns(list(frame.columns))
    predictions, audit = walk_forward_predict(
        frame, features, retrain_days=7, min_train_days=10_000,
        train_window_days=None, lag=LAG,
    )
    assert predictions.isna().all(), "abstention must mean NULL, not a guess"
    assert not audit["predicted"].any()


def test_first_days_have_no_prediction():
    frame = add_calendar_features(_history())
    features = feature_columns(list(frame.columns))
    predictions, audit = walk_forward_predict(
        frame, features, retrain_days=7, min_train_days=21, lag=LAG
    )
    frame = frame.assign(pred=predictions)

    abstained = audit[~audit["predicted"]]["delivery_date"].tolist()
    assert abstained, "the earliest days cannot have enough history"
    assert frame[frame["delivery_date"].isin(abstained)]["pred"].isna().all()

    predicted = audit[audit["predicted"]]["delivery_date"].tolist()
    assert frame[frame["delivery_date"].isin(predicted)]["pred"].notna().any()


def test_predictions_track_the_signal():
    """Sanity: a causal model on a learnable signal should still be useful."""
    frame = add_calendar_features(_history(days=120))
    features = feature_columns(list(frame.columns))
    predictions, _ = walk_forward_predict(
        frame, features, retrain_days=30, min_train_days=21, lag=LAG
    )
    scored = frame.assign(pred=predictions).dropna(subset=["pred"])
    assert len(scored) > 0
    corr = scored["pred"].corr(scored["actual_mw"])
    assert corr > 0.8, f"causal model should still track the target (r={corr:.2f})"


def test_tso_forecast_is_not_a_feature_by_default():
    """Article 14.1.D lands at 18:00 D-1, after the 12:00 gate closure."""
    frame = add_calendar_features(_history(days=5))
    assert "tso_forecast_mw" not in feature_columns(list(frame.columns))


def test_training_window_actually_bounds_the_history():
    """A rolling window must forget old intervals, not just gate the start.

    MIN_TRAIN_DAYS alone cannot do this: it decides when prediction begins and
    leaves training expanding, so two different values produce byte-identical
    predictions on any day both can reach.
    """
    frame = add_calendar_features(_history(days=200))
    features = feature_columns(list(frame.columns))

    _, wide = walk_forward_predict(
        frame, features, retrain_days=30, min_train_days=30,
        train_window_days=180, lag=LAG,
    )
    _, narrow = walk_forward_predict(
        frame, features, retrain_days=30, min_train_days=30,
        train_window_days=45, lag=LAG,
    )

    wide_rows = wide[wide["predicted"]]["n_train"].max()
    narrow_rows = narrow[narrow["predicted"]]["n_train"].max()
    assert narrow_rows < wide_rows, "the narrow window did not bound anything"
    assert narrow_rows <= 45 * 96 + 96, "narrow window kept more than 45 days"


def test_expanding_window_keeps_growing():
    frame = add_calendar_features(_history(days=200))
    features = feature_columns(list(frame.columns))
    _, audit = walk_forward_predict(
        frame, features, retrain_days=30, min_train_days=30,
        train_window_days=None, lag=LAG,
    )
    trained = audit[audit["predicted"]]["n_train"]
    assert trained.iloc[-1] > trained.iloc[0], "expanding history must grow"


def test_a_window_shorter_than_the_gate_is_rejected():
    """Otherwise the model silently predicts nothing at all.

    The windowed history can never reach the gate, so every day abstains and
    the run looks successful while producing no forecast.
    """
    frame = add_calendar_features(_history(days=30))
    features = feature_columns(list(frame.columns))
    with pytest.raises(ValueError, match="shorter than"):
        walk_forward_predict(
            frame, features, min_train_days=90, train_window_days=60, lag=LAG
        )
