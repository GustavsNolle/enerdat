"""A candidate forecast, fitted strictly causally.

The modelling here is deliberately unremarkable -- gradient boosting on hub
height wind speed. The part that matters is *when* the model is allowed to
learn things.

A single fit over the whole history, scored on a random split, would leak:
predicting January with a model that saw July is not a forecast. So this walks
forward. For delivery day D the model may only have seen intervals whose
actuals were published before D's gate closure, and `walk_forward_predict`
returns an audit trail proving it for every day it predicted.
"""

import numpy as np
import pandas as pd

import dagster as dg
from dagster import AssetExecutionContext
from dagster_duckdb import DuckDBResource

from enerdat.config import (
    ACTUALS_PUBLICATION_LAG,
    GRID_POINTS,
    MIN_TRAIN_DAYS,
    MODEL_OBJECTIVE,
    MODEL_RETRAIN_DAYS,
    TARGET_TECHNOLOGY,
    TAU_BOUNDS,
    TRAIN_WINDOW_DAYS,
    TURBINE_CUT_IN_MS,
    TURBINE_CUT_OUT_MS,
    TURBINE_RATED_MS,
    WEATHER_LAG_STEPS,
    USE_TSO_FORECAST_AS_FEATURE,
    WEATHER_VARIABLES,
    ZONE,
)


LAGGED = ("wind_speed_100m_v", "power_fraction")


def power_fraction(speed_ms):
    """Turbine power curve, normalised to 1 at rated output.

    Zero below cut-in, cubic to rated, flat to cut-out, and zero again beyond
    it. That last step is why this cannot be replaced by any monotonic function
    of wind speed: in a storm the fleet shuts down, so a 30 m/s hour and a dead
    calm look identical from the grid's side.
    """
    speed = np.asarray(speed_ms, dtype="float64")
    out = np.zeros_like(speed)

    ramp = (speed >= TURBINE_CUT_IN_MS) & (speed < TURBINE_RATED_MS)
    out[ramp] = (speed[ramp] ** 3 - TURBINE_CUT_IN_MS**3) / (
        TURBINE_RATED_MS**3 - TURBINE_CUT_IN_MS**3
    )
    out[(speed >= TURBINE_RATED_MS) & (speed < TURBINE_CUT_OUT_MS)] = 1.0

    out[~np.isfinite(speed)] = np.nan
    return out


def add_physics_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Hand the model the turbine response instead of making it be inferred."""
    out = frame.copy()
    if "wind_speed_100m_v" in out.columns:
        out["power_fraction"] = power_fraction(out["wind_speed_100m_v"])
        for i in range(len(GRID_POINTS)):
            column = f"ws_site_{i}"
            if column in out.columns:
                out[f"pf_site_{i}"] = power_fraction(out[column])
    return out


def add_lag_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Neighbouring hours, a local average, and a rate of change.

    Forecast error concentrates on ramps, and a model shown one instant at a
    time cannot see one. Both directions are used: these are *forecast* values,
    all of which were on the table at gate closure, so a later hour is no more
    privileged than an earlier one. Lagging the target would be another matter
    entirely and is not done.
    """
    out = frame.sort_values("valid_time_utc").copy()
    for column in LAGGED:
        if column not in out.columns:
            continue
        for step in WEATHER_LAG_STEPS:
            out[f"{column}_t{step:+d}"] = out[column].shift(-step)
        out[f"{column}_roll3"] = (
            out[column].rolling(3, center=True, min_periods=1).mean()
        )
        out[f"{column}_delta"] = out[column].diff()
    return out


def feature_columns(available: list[str]) -> list[str]:
    """Everything the model is allowed to see, named explicitly.

    An allow-list rather than "all columns except the target": prices and the
    TSO forecast both sit in the mart and neither is knowable at gate closure,
    so a subtractive rule would be one careless column away from a leak.
    """
    columns = []

    for variable in WEATHER_VARIABLES:
        columns += [f"{variable}_v", f"{variable}_model_sd"]

    columns += [f"ws_site_{i}" for i in range(len(GRID_POINTS))]
    columns += [f"pf_site_{i}" for i in range(len(GRID_POINTS))]
    columns += ["ws_site_sd", "power_fraction"]

    for base in LAGGED:
        columns += [f"{base}_t{step:+d}" for step in WEATHER_LAG_STEPS]
        columns += [f"{base}_roll3", f"{base}_delta"]

    columns += ["hour_sin", "hour_cos", "month"]

    if USE_TSO_FORECAST_AS_FEATURE:
        columns.append("tso_forecast_mw")

    return [c for c in columns if c in available]


def add_calendar_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Hour-of-day as a circle, not an integer.

    Hour is cyclic: 23:00 and 00:00 are adjacent, but as a plain integer they
    sit at opposite ends of the range and a tree has to spend splits
    rediscovering that.
    """
    out = frame.copy()
    hour = out["valid_time_utc"].dt.hour + out["valid_time_utc"].dt.minute / 60
    out["hour_sin"] = np.sin(2 * np.pi * hour / 24)
    out["hour_cos"] = np.cos(2 * np.pi * hour / 24)
    out["month"] = out["valid_time_utc"].dt.month
    return out


def cost_optimal_tau(history: pd.DataFrame) -> float | None:
    """The quantile that minimises settlement cost, from realised prices.

    c_long  = day-ahead price - long price   (cost per MWh of over-delivering)
    c_short = short price - day-ahead price  (cost per MWh of under-delivering)

    Negative values mean that direction actually paid in that interval; they
    are floored at zero rather than allowed to offset, so one very profitable
    hour cannot argue the schedule into a reckless bias.

    Returns None when prices are missing, which the caller treats as "fall back
    to the megawatt objective" rather than guessing.
    """
    needed = {"price_day_ahead", "price_long", "price_short"}
    if not needed.issubset(history.columns):
        return None

    priced = history.dropna(subset=list(needed))
    if priced.empty:
        return None

    c_long = (priced["price_day_ahead"] - priced["price_long"]).clip(lower=0).mean()
    c_short = (priced["price_short"] - priced["price_day_ahead"]).clip(lower=0).mean()

    total = c_long + c_short
    if not total or not np.isfinite(total):
        return None

    low, high = TAU_BOUNDS
    return float(np.clip(c_long / total, low, high))


def _fit(history: pd.DataFrame, features: list[str], seed: int):
    """Fit the candidate, and report the objective actually used."""
    from sklearn.ensemble import HistGradientBoostingRegressor

    tau = cost_optimal_tau(history) if MODEL_OBJECTIVE == "euros" else None

    if tau is None:
        model = HistGradientBoostingRegressor(
            max_iter=300, learning_rate=0.06, random_state=seed
        )
    else:
        model = HistGradientBoostingRegressor(
            loss="quantile", quantile=tau,
            max_iter=300, learning_rate=0.06, random_state=seed,
        )

    model.fit(history[features], history["actual_mw"])
    return model, tau


def walk_forward_predict(
    frame: pd.DataFrame,
    features: list[str],
    retrain_days: int = MODEL_RETRAIN_DAYS,
    min_train_days: int = MIN_TRAIN_DAYS,
    train_window_days: int | None = TRAIN_WINDOW_DAYS,
    lag: pd.Timedelta | None = None,
    seed: int = 0,
) -> tuple[pd.Series, pd.DataFrame]:
    """Predict each delivery day using only data knowable at its gate closure.

    `min_train_days` gates when prediction starts; `train_window_days` bounds
    how far back each fit looks (None = expanding, i.e. all history).

    Returns (predictions aligned to `frame.index`, per-day audit trail).

    The audit trail is not diagnostics -- it is the evidence. Each row records
    the training cutoff for that day and the latest interval actually trained
    on, so a test can assert the second never exceeds the first.
    """
    if train_window_days is not None and train_window_days < min_train_days:
        raise ValueError(
            f"train_window_days={train_window_days} is shorter than "
            f"min_train_days={min_train_days}, so the windowed history can "
            "never satisfy the gate and the model would silently predict "
            "nothing. Widen the window or lower the gate."
        )

    lag = pd.Timedelta(ACTUALS_PUBLICATION_LAG) if lag is None else lag

    frame = frame.sort_values("valid_time_utc")
    predictions = pd.Series(np.nan, index=frame.index, dtype="float64")
    audit = []

    model = None
    tau = None
    last_fit_date = None

    for delivery_date, day in frame.groupby("delivery_date", sort=True):
        closure = day["gate_closure_utc"].iloc[0]
        cutoff = closure - lag

        # Everything whose actual had been published by this day's cutoff.
        history = frame[
            (frame["valid_time_utc"] <= cutoff) & frame["actual_mw"].notna()
        ]
        if train_window_days is not None:
            # Rolling rather than expanding: forget intervals older than the
            # window, so the fit describes the fleet as it is now.
            window_start = cutoff - pd.Timedelta(days=train_window_days)
            history = history[history["valid_time_utc"] > window_start]
        history = history.dropna(subset=features)

        due = (
            last_fit_date is None
            or (pd.Timestamp(delivery_date) - pd.Timestamp(last_fit_date)).days
            >= retrain_days
        )

        history_days = history["delivery_date"].nunique()
        if history_days < min_train_days:
            audit.append(
                {
                    "delivery_date": delivery_date,
                    "cutoff_utc": cutoff,
                    "n_train": len(history),
                    "train_days": history_days,
                    "max_train_valid_time_utc": history["valid_time_utc"].max()
                    if len(history)
                    else pd.NaT,
                    "refit": False,
                    "predicted": False,
                }
            )
            continue

        if due or model is None:
            model, tau = _fit(history, features, seed)
            last_fit_date = delivery_date

        usable = day.dropna(subset=features)
        if len(usable):
            predictions.loc[usable.index] = model.predict(usable[features])

        audit.append(
            {
                "delivery_date": delivery_date,
                "cutoff_utc": cutoff,
                "n_train": len(history),
                "train_days": history_days,
                "max_train_valid_time_utc": history["valid_time_utc"].max(),
                "refit": bool(due),
                "predicted": bool(len(usable)),
                "tau": tau,
            }
        )

    return predictions, pd.DataFrame(audit)


@dg.asset(
    deps=["forecast_error_mart"],
    group_name="marts",
    kinds={"python", "duckdb"},
    description=(
        "Candidate day-ahead forecast, fitted walk-forward so no delivery day "
        "is predicted by a model that saw it. Emits NULL rather than guessing "
        "while history is too short."
    ),
)
def candidate_forecast(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    with duckdb.get_connection() as connection:
        frame = connection.execute("SELECT * FROM forecast_error").df()

    if frame.empty:
        raise dg.Failure(description="forecast_error is empty; build the mart first.")

    frame = add_calendar_features(frame)
    frame = add_physics_features(frame)
    frame = add_lag_features(frame)
    features = feature_columns(list(frame.columns))
    if not features:
        raise dg.Failure(description="No usable feature columns in forecast_error.")

    predictions, audit = walk_forward_predict(frame, features)
    frame["candidate_mw"] = predictions

    scored = frame.dropna(subset=["candidate_mw", "actual_mw", "tso_forecast_mw"])
    mae_candidate = float((scored.candidate_mw - scored.actual_mw).abs().mean()) if len(scored) else float("nan")
    mae_tso = float((scored.tso_forecast_mw - scored.actual_mw).abs().mean()) if len(scored) else float("nan")

    derived = set(features) | {"hour_sin", "hour_cos", "month"}
    keep = [c for c in frame.columns if not c.endswith("_issued") and c not in derived]
    with duckdb.get_connection() as connection:
        connection.register("candidate_df", frame[keep])
        connection.execute(
            "CREATE OR REPLACE TABLE candidate_forecast AS SELECT * FROM candidate_df"
        )
        connection.unregister("candidate_df")

    context.log.info(
        f"candidate MAE {mae_candidate:.2f} MW vs TSO {mae_tso:.2f} MW "
        f"over {len(scored)} scored intervals"
    )

    return dg.MaterializeResult(
        metadata={
            "zone": ZONE,
            "technology": TARGET_TECHNOLOGY,
            "features": dg.MetadataValue.json(features),
            "rows": len(frame),
            "intervals_predicted": len(scored),
            "days_predicted": int(audit["predicted"].sum()) if len(audit) else 0,
            "days_abstained": int((~audit["predicted"]).sum()) if len(audit) else 0,
            "refits": int(audit["refit"].sum()) if len(audit) else 0,
            "mae_candidate_mw": round(mae_candidate, 3),
            "mae_tso_mw": round(mae_tso, 3),
            "mae_improvement_pct": (
                round((mae_tso - mae_candidate) / mae_tso * 100, 2)
                if mae_tso and mae_tso == mae_tso and mae_tso != 0
                else None
            ),
        }
    )
