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
    ACTUAL_LAG_HOURS,
    DECISION_LEAD,
    HORIZON,
    MIN_ACTUAL_LAG,
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


# What imbalance_settlement_mart reads off this table. Kept regardless of
# whether a column also happens to be a feature.
SETTLEMENT_COLUMNS = (
    "valid_time_utc",
    "delivery_date",
    "actual_mw",
    "tso_forecast_mw",
    "price_day_ahead",
    "price_long",
    "price_short",
)

LAGGED = ("wind_speed_100m_v", "power_fraction")

# The TSO forecast gets lagged too when it is admissible: its error is
# autocorrelated, so the neighbouring hours say something about this one.
TSO_LAGGED = ("tso_forecast_mw",)


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


def add_outturn_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Recent outturn, and the TSO's recent error. Intraday only.

    This is what the intraday horizon buys, and it is worth more than any
    weather feature. At T minus DECISION_LEAD the fleet's output several hours
    earlier has been published, and wind is strongly autocorrelated at short
    horizons -- persistence alone is a serious forecaster over a few hours.

    The TSO's recent error is knowable too, since both its forecast and the
    outturn are public for past intervals, and that error is autocorrelated:
    when the published forecast has been running high all afternoon it is
    usually still running high now.

    Nothing here is admissible at the day-ahead horizon, where none of these
    values exist yet. add_lag_features refuses to build them unless
    HORIZON == "intraday", so the two horizons cannot silently share features.
    """
    if HORIZON != "intraday":
        return frame

    out = frame.sort_values("valid_time_utc").copy()
    step = out["valid_time_utc"].diff().median()
    if pd.isna(step) or step <= pd.Timedelta(0):
        return out

    per_hour = pd.Timedelta(hours=1) / step
    tso_error = out["actual_mw"] - out["tso_forecast_mw"]

    for hours in ACTUAL_LAG_HOURS:
        # Guard rather than trust the constant list.
        if pd.Timedelta(hours=hours) < pd.Timedelta(MIN_ACTUAL_LAG):
            raise ValueError(
                f"ACTUAL_LAG_HOURS contains {hours}h, shorter than the "
                f"{MIN_ACTUAL_LAG} that is knowable when the schedule is "
                "fixed. That lag has not been published yet."
            )
        shift = int(round(hours * per_hour))
        out[f"actual_lag{hours}h"] = out["actual_mw"].shift(shift)
        out[f"tso_error_lag{hours}h"] = tso_error.shift(shift)

    # Persistence relative to the published forecast: how far the outturn has
    # been running from it, at the freshest lag available.
    freshest = min(ACTUAL_LAG_HOURS)
    out["persistence_gap"] = out[f"actual_lag{freshest}h"] - out["tso_forecast_mw"]
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
    lagged = LAGGED + (TSO_LAGGED if USE_TSO_FORECAST_AS_FEATURE else ())
    for column in lagged:
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

    bases = LAGGED + (TSO_LAGGED if USE_TSO_FORECAST_AS_FEATURE else ())
    for base in bases:
        columns += [f"{base}_t{step:+d}" for step in WEATHER_LAG_STEPS]
        columns += [f"{base}_roll3", f"{base}_delta"]

    if HORIZON == "intraday":
        for hours in ACTUAL_LAG_HOURS:
            columns += [f"actual_lag{hours}h", f"tso_error_lag{hours}h"]
        columns.append("persistence_gap")

    columns += ["hour_sin", "hour_cos", "month"]

    if USE_TSO_FORECAST_AS_FEATURE:
        # Admissible only because DECISION_TIME_LOCAL is at or after the
        # article 14.1.D publication hour. With an earlier deadline this
        # forecast does not exist yet and must stay out.
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

    # The quantile framework needs DUAL pricing. Its whole basis is that cost
    # is piecewise-linear in the schedule, with a kink at the outturn because
    # the two directions settle at different prices -- that kink is what makes
    # an interior optimum exist and puts it at a quantile.
    #
    # Under a SINGLE imbalance price the two arms share a slope, cost becomes
    # linear in the schedule, and the optimum runs off to an extreme: schedule
    # zero if the imbalance price sits above day-ahead on average, unbounded if
    # below. A model handed that objective does not find an edge, it finds the
    # degeneracy, and reports enormous fictional savings from systematically
    # under-nominating.
    #
    # Most of Europe has harmonised on single pricing, so this is the common
    # case rather than a data error -- and ENTSO-E still labels the columns
    # Long and Short, which makes it easy to miss. Fall back to the megawatt
    # objective and let the caller report why.
    if priced["price_long"].equals(priced["price_short"]):
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
    frame = add_outturn_features(frame)
    features = feature_columns(list(frame.columns))
    if not features:
        raise dg.Failure(description="No usable feature columns in forecast_error.")

    predictions, audit = walk_forward_predict(frame, features)
    frame["candidate_mw"] = predictions

    scored = frame.dropna(subset=["candidate_mw", "actual_mw", "tso_forecast_mw"])
    mae_candidate = float((scored.candidate_mw - scored.actual_mw).abs().mean()) if len(scored) else float("nan")
    mae_tso = float((scored.tso_forecast_mw - scored.actual_mw).abs().mean()) if len(scored) else float("nan")

    # Allow-list, for the same reason feature selection is one. The previous
    # rule dropped "anything that is a feature", which silently removed
    # tso_forecast_mw from this table the moment it became admissible -- and
    # settlement needs it. Name what downstream requires instead of guessing
    # what it does not.
    keep = [c for c in SETTLEMENT_COLUMNS if c in frame.columns] + ["candidate_mw"]
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
