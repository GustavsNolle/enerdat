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

# Which archived weather runs to fetch, as Open-Meteo `previous_dayN` offsets.
#
# previous_dayN is the forecast for a timestamp taken from the run issued
# roughly N days earlier. For delivery day D with gate closure at 12:00 local
# on D-1:
#
#   N=1  issued ~T-24h. Legal for T up to D 12:00, because that run predates
#        gate closure. For an afternoon or evening hour it does NOT: the run
#        behind D 13:00 was issued around 13:00 on D-1, an hour too late.
#   N=2  issued ~T-48h, at the latest D-2 23:00. Legal for every hour of D.
#
# Fetching both and taking the freshest legal one per interval recovers a full
# day of forecast skill for the morning half of every delivery day, at no cost
# to correctness: the asset drops any row whose implied issue time is after its
# own day's gate closure BEFORE choosing, so a leak cannot survive the choice.
# Using N=2 uniformly, as this did originally, threw that skill away.
WEATHER_LEAD_DAYS_OPTIONS = (1, 2)

# The lead used when nothing fresher is legal; also what the mart reports.
WEATHER_LEAD_DAYS = max(WEATHER_LEAD_DAYS_OPTIONS)

# --- model ---

# Actuals are not knowable the instant they occur: ENTSO-E publishes realised
# generation with a lag. Training for delivery day D may therefore only use
# intervals whose actuals had been published by D's gate closure.
ACTUALS_PUBLICATION_LAG = dt.timedelta(hours=1)

# Walk-forward retraining cadence, in days. Refitting for every delivery day is
# the purest form but costs a fit per day over a multi-year backfill; refitting
# monthly stays strictly causal and runs in reasonable time.
MODEL_RETRAIN_DAYS = 30

# How far back a fit is allowed to look, in days. None keeps every interval
# since the start of the backfill (expanding); an integer makes it a rolling
# window that forgets older data.
#
# The trade-off is real in both directions. A turbine fleet changes -- farms
# commission, blades degrade, curtailment rules shift -- so old data can
# describe a plant that no longer exists. Against that, wind is seasonal, and a
# window shorter than a year has never seen the season it is being asked to
# predict.
# Ninety, measured. Swept 60/90/180/270/expanding on a common 7292-interval
# evaluation set (candidate MAE / TSO MAE): 1.225, 1.211, 1.220, 1.237, 1.255.
#
# A shallow U with its minimum near a quarter, and the notable end is the far
# one: training on ALL history is the worst setting tested, 3.6% behind a
# 90-day window. More data actively hurts here, because old intervals describe
# a fleet and a curtailment regime that have since moved on. Anything from 60
# to 180 days is within about 1% of the best, so this is a real effect but not
# a sharp one -- do not over-tune it on a single year.
TRAIN_WINDOW_DAYS = 90

# Below this much training history the model abstains and emits NULL rather
# than a prediction nobody should trust.
#
# NOTE this is a *gate*, not a window length: it decides when the model has
# enough history to start predicting at all. Training history is expanding by
# default, so raising this shortens the evaluation period without changing what
# any surviving day is trained on -- 60 and 240 produce byte-identical
# predictions on the days both can reach. Use TRAIN_WINDOW_DAYS to bound how
# far back training actually looks.
#
# Ninety days rather than the original twenty-one: at BE's hourly resolution
# three weeks is only about 500 rows to learn a seasonal, nonlinear response
# from, and the feature set is now far wider than it was.
#
# Counted in DAYS, not intervals, because market resolution is not a constant:
# BE publishes the day-ahead forecast hourly (24 intervals/day) while NL
# publishes quarter-hourly (96). An interval threshold silently means four
# times as much history in one zone as the other -- and at 2000 intervals it
# meant BE could never train at all.
MIN_TRAIN_DAYS = 90

# Whether the TSO's own day-ahead forecast may be used as a model feature.
#
# False, deliberately. Article 14.1.D forecasts are published by 18:00 on D-1,
# which is AFTER the 12:00 day-ahead gate closure this project treats as the
# decision point. Using it would mean predicting with information the schedule
# could not have contained. The cost is real -- the TSO forecast is a strong
# feature and we are beating it with strictly less information than it had --
# but a win under this rule is unambiguous, and a loss is explicable.
USE_TSO_FORECAST_AS_FEATURE = False

# What the model minimises.
#
#   "mw"     squared error. Targets the middle of the distribution -- the
#            best estimate of megawatts, which is what the TSO publishes.
#   "euros"  the settlement cost itself.
#
# The second is not a tweak of the first, it is a different problem. Cost of
# scheduling S when the outturn is A is
#
#     c_long * (A - S)+   +   c_short * (S - A)+
#
# with c_long = P_dayahead - P_long and c_short = P_short - P_dayahead, both
# normally positive: the schedule was already sold at the day-ahead price, so
# deviating costs the spread between it and the imbalance price.
#
# That is pinball loss, so the cost-minimising schedule is not the mean but the
# tau-quantile of the outturn distribution, with
#
#     tau = c_long / (c_long + c_short)
#
# Being short is usually punished harder than being long is rewarded, which
# puts tau below 0.5 and deliberately biases the schedule low. That is the
# whole edge: it needs no better weather than the TSO has, only a different
# objective. tau is estimated from the training window alone, so it stays as
# causal as everything else.
MODEL_OBJECTIVE = "euros"

# Bounds on the estimated tau. A window where prices went one-sided can imply a
# degenerate quantile; clamping keeps the schedule sane.
TAU_BOUNDS = (0.05, 0.95)

# Turbine response, used to hand the model physics rather than make it
# rediscover a sigmoid from a few months of history. Power rises with the cube
# of wind speed between cut-in and rated, is flat to cut-out, and is zero
# beyond it -- a storm shutdown looks identical to a calm from the grid's side,
# which no monotonic function of wind speed can express.
TURBINE_CUT_IN_MS = 3.0
TURBINE_RATED_MS = 12.5
TURBINE_CUT_OUT_MS = 25.0

# Offsets, in intervals, used to give the model the shape of the weather around
# each hour rather than a single instant. Negative is earlier, positive later:
# both are legal, because these are forecast values that were all on the table
# at gate closure. Lagging the *target* would not be, and is not done.
WEATHER_LAG_STEPS = (-3, -1, 1, 3)

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

# Three independent forecasting centres rather than one. Their disagreement at
# a given hour is the uncertainty signal: when ICON, GFS and ECMWF part company
# the outturn is genuinely less predictable, and the quantile objective needs
# exactly that -- it is estimating a distribution, not a point.
#
# Open-Meteo's ensemble API would give members of a single model; cross-model
# spread captures structural disagreement instead, and comes from the same
# archived-forecast endpoint that already guarantees a fixed lead time.
OPEN_METEO_MODELS = (
    "icon_seamless",
    "gfs_seamless",
    "ecmwf_ifs025",
)

# --- storage -----------------------------------------------------------------

LAKE_ROOT = "data/raw"
DUCKDB_PATH = "data/enerdat.duckdb"
