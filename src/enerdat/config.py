"""Project constants.

Everything a reader needs to understand the *scope* of the pipeline lives here.
The two values that carry real analytical weight are GATE_CLOSURE_LOCAL and
WEATHER_LEAD_DAYS -- together they define the information set the model is
allowed to see. See docs in `checks.py` for why.
"""

from __future__ import annotations

import datetime as dt

# --- scope -------------------------------------------------------------------

# Bidding zone, as an entsoe-py alias.
#
# BE, chosen on evidence rather than preference. Two things have to hold at
# once, and few zones manage both:
#
#   1. The zone must publish an imbalance price at bidding-zone level, since
#      that is what converts MWh of error into euros. DE_LU does not.
#   2. The 14.1.D forecast and the 16.1 metered actual must describe the same
#      fleet. Measured over 1-7 Sep 2026, mean actual / mean forecast:
#
#          zone   solar   offshore   onshore
#          BE      1.05      1.00      0.95     <- consistent
#          DE_LU   1.04      0.90      1.00     (but no imbalance price)
#          DK_1    1.11      1.20      1.19     (systematic +19% bias)
#          NL      0.04      2.83      0.25     <- unusable
#
#      NL fails badly: Dutch distributed generation never reaches TenneT's
#      metered aggregate, so subtracting forecast from actual measures scope,
#      not error. `checks.forecast_actual_comparable` now fails the mart on
#      exactly this, so the trap cannot be re-entered silently.
ZONE = "BE"

# The series we are trying to predict, as ENTSO-E names it.
TARGET_TECHNOLOGY = "Wind Offshore"

# ENTSO-E market time. Note this is a *market* convention, not a display
# preference: delivery days are defined in it, so DST days have 23 or 25 hours.
MARKET_TZ = "Europe/Brussels"

PARTITION_START = "2024-01-01"

# Largest window sent to ENTSO-E in one request during a range backfill.
#
# entsoe-py chunks only at the year boundary, but the Transparency Platform
# does not reliably serve a year of 14.1.D forecast: a 364-day request was
# observed returning 133 KB and then going silent with the connection still
# open, which a read timeout cannot distinguish from a slow stream. Sixty days
# comes back in about a minute, and a failure costs one chunk instead of the
# whole backfill.
MAX_QUERY_DAYS = 60

# --- the information boundary ------------------------------------------------

# Day-ahead auction gate closure: 12:00 market time on D-1. A schedule for
# delivery day D must be submitted by then, so nothing published after this
# instant may be used as a feature for day D.
GATE_CLOSURE_LOCAL = dt.time(12, 0)

# Which archived weather run to use, expressed as Open-Meteo's `previous_dayN`
# lead-time offset.
#
#   previous_day1 -> run issued ~24h before valid time.
#   previous_day2 -> run issued ~48h before valid time.
#
# For delivery day D, valid times run from D 00:00 to D 23:59. With N=1, the
# run behind D 13:00 was issued ~D-1 13:00 -- an hour AFTER gate closure. So
# N=1 leaks for every afternoon and evening hour. N=2 is issued at the latest
# D-2 23:00, comfortably before D-1 12:00, and is safe for every hour of D.
#
# This is deliberately conservative: it costs forecast skill to buy a bound
# that holds for all 24 hours without special-casing. `leakage_free_features`
# in checks.py proves it rather than trusting this comment.
WEATHER_LEAD_DAYS = 2

# --- model ---

# Actuals are not knowable the instant they occur: ENTSO-E publishes realised
# generation with a lag. Training for delivery day D may therefore only use
# intervals whose actuals had been published by D's gate closure.
ACTUALS_PUBLICATION_LAG = dt.timedelta(hours=1)

# Walk-forward retraining cadence, in days. Refitting for every delivery day is
# the purest form but costs a fit per day over a multi-year backfill; refitting
# monthly stays strictly causal and runs in reasonable time.
MODEL_RETRAIN_DAYS = 30

# Below this much training history the model abstains and emits NULL rather
# than a prediction nobody should trust.
#
# Counted in DAYS, not intervals, because market resolution is not a constant:
# BE publishes the day-ahead forecast hourly (24 intervals/day) while NL
# publishes quarter-hourly (96). An interval threshold silently means four
# times as much history in one zone as the other -- and at 2000 intervals it
# meant BE could never train at all.
MIN_TRAIN_DAYS = 21

# Whether the TSO's own day-ahead forecast may be used as a model feature.
#
# False, deliberately. Article 14.1.D forecasts are published by 18:00 on D-1,
# which is AFTER the 12:00 day-ahead gate closure this project treats as the
# decision point. Using it would mean predicting with information the schedule
# could not have contained. The cost is real -- the TSO forecast is a strong
# feature and we are beating it with strictly less information than it had --
# but a win under this rule is unambiguous, and a loss is explicable.
USE_TSO_FORECAST_AS_FEATURE = False

# --- weather sampling --------------------------------------------------------

# The Belgian offshore wind zone, grouped into five sampling points. Weights
# are installed MW summed from query_installed_generation_capacity_per_unit,
# which lists ten BE offshore units totalling 2260.8 MW.
#
# The whole fleet sits inside roughly 30 km of North Sea, so these points are
# highly correlated -- that is a fact about Belgian offshore wind, not a
# sampling flaw, and it is exactly why the zone's output swings as one block.
GRID_POINTS = [
    {"name": "Norther",              "lat": 51.53, "lon": 3.00, "weight": 370.0},
    {"name": "Thorntonbank C-Power", "lat": 51.55, "lon": 2.93, "weight": 325.2},
    {"name": "Rentel + Northwind",   "lat": 51.60, "lon": 2.92, "weight": 523.0},
    {"name": "Seastar + Mermaid",    "lat": 51.65, "lon": 2.85, "weight": 487.5},
    {"name": "Belwind + NW2",        "lat": 51.67, "lon": 2.80, "weight": 555.1},
]

# Hourly variables pulled at each grid point. Wind power tracks the cube of
# wind speed, so speed at hub height (100 m) is the load-bearing feature;
# direction matters through wake effects, and temperature through air density.
WEATHER_VARIABLES = [
    "wind_speed_100m",
    "wind_direction_100m",
    "wind_gusts_10m",
    "temperature_2m",
    "surface_pressure",
]

OPEN_METEO_MODEL = "icon_seamless"

# --- storage -----------------------------------------------------------------

LAKE_ROOT = "data/raw"
DUCKDB_PATH = "data/enerdat.duckdb"
