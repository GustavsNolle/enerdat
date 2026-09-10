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
# BE. It is the only zone scanned whose 14.1.D forecast and 16.1 metered actual
# describe the same fleet (ratio 0.968, corr 0.953) AND whose settlement cost
# measure is well posed -- a schedule of zero costs MORE than the TSO forecast
# there, so no trivial strategy beats perfect foresight.
#
# PL was tried and abandoned. Its imbalance price sits above day-ahead 88.4% of
# the time under single pricing, which makes cost linear in the schedule and
# its optimum degenerate: bidding zero "saved" 5085m against perfect
# foresight's 198m. See checks.cost_measure_is_well_posed.
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

# Day-ahead auction gate closure: 12:00 market time on D-1. Recorded because it
# is a real market boundary, but it is NOT the deadline this project schedules
# against -- see DECISION_TIME_LOCAL.
GATE_CLOSURE_LOCAL = dt.time(12, 0)

# When our schedule is fixed, in market time on D-1. Everything downstream --
# which weather runs are legal, whether the TSO forecast may be a feature, and
# what the leakage check enforces -- keys off this one value.
#
# 18:00, which is a deliberate change from the 12:00 auction gate closure this
# project originally used, for two reasons.
#
# First, imbalance is settled against a BRP's FINAL nominated position, not its
# day-ahead position. Intraday markets stay open long past 12:00, so treating
# the day-ahead auction as the last decision point models a party that stops
# trading at noon and then watches its exposure accumulate. Nobody does that.
#
# Second, article 14.1.D obliges TSOs to publish the day-ahead wind and solar
# forecast by 18:00 on D-1. At 12:00 it does not exist yet, which is why it was
# excluded as a feature; at 18:00 it is public. Since that forecast is both the
# baseline and, measurably, biased -- it over-forecasts BE offshore wind by
# 37.6 MW on average and is short 63% of intervals -- correcting it is the
# single largest effect available.
#
# The comparison stays honest: the TSO forecast is public information at this
# hour, available to every market participant, and the question becomes the one
# a BRP actually faces -- given the published forecast, can you schedule better
# than it?
#
# NOTE this is an assumption about publication timing taken from the
# regulation, not something the data can confirm. If a TSO published late, this
# would be optimistic.
DECISION_TIME_LOCAL = dt.time(18, 0)

# Which horizon the schedule is fixed at.
#
#   "day_ahead"  DECISION_TIME_LOCAL on D-1: one deadline for the whole
#                delivery day, and the schedule for 23:00 is fixed at the same
#                instant as the one for 00:00.
#   "intraday"   DECISION_LEAD before each interval: a deadline PER INTERVAL.
#
# Intraday is the more faithful model. Imbalance settles against a BRP's final
# nominated position, and continuous intraday trading runs until roughly an
# hour before delivery -- so the day-ahead deadline describes a party that
# stops trading the evening before and then watches its exposure accumulate.
#
# The change is not mainly about fresher weather. Open-Meteo's archive is
# day-granular, so the freshest legal run is the same one either way. What
# intraday unlocks is RECENT OUTTURN: at T-1h the fleet's output at T-2h has
# been published, and short-horizon persistence is a far stronger predictor
# than any weather feature. Lagged actuals are inadmissible at day-ahead and
# admissible here, which is the whole point.
HORIZON = "intraday"

# How far before delivery the intraday schedule is fixed.
DECISION_LEAD = dt.timedelta(hours=1)

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

# Smallest lag of the outturn that is knowable when the schedule is fixed.
#
# An actual at T-k is published by T-k+ACTUALS_PUBLICATION_LAG, and the
# schedule for T is fixed at T-DECISION_LEAD, so the lag is admissible only
# when k >= DECISION_LEAD + ACTUALS_PUBLICATION_LAG. With both at one hour that
# is two hours: the outturn at T-2h is usable, T-1h is not.
MIN_ACTUAL_LAG = DECISION_LEAD + ACTUALS_PUBLICATION_LAG

# Offsets of the outturn offered to the model, in hours before valid time.
# Every one must be >= MIN_ACTUAL_LAG; feature selection asserts it rather than
# trusting the list.
ACTUAL_LAG_HOURS = (2, 3, 4, 6, 12, 24)

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
# True, and legal precisely because DECISION_TIME_LOCAL is 18:00: article
# 14.1.D requires publication by that hour, so the forecast is public before
# the schedule is fixed. Under the old 12:00 deadline it was not, and this was
# False.
#
# This makes the model a correction to the published forecast rather than an
# independent one, which is the point: the TSO's accuracy comes from telemetry
# we do not have, while its bias is visible in the public record.
USE_TSO_FORECAST_AS_FEATURE = True

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

# Weight training samples by how expensive an error at that interval was.
#
# The decomposition that motivates this: settlement cost splits into
#
#     mean(deviation) x mean(spread)   +   cov(deviation, spread)
#
# and on BE the first term is 0.8% of the total. A 37 MW standing bias costs
# about 150k against a 19.5m bill, because the mean spread is only 0.62 EUR/MWh.
# Essentially all cost is COVARIANCE -- whether errors land on expensive
# intervals -- which is a quantity neither squared error nor the quantile
# objective has ever targeted.
#
# Minimising cost directly is not available: under single pricing cost is
# linear in the schedule and its optimum is degenerate. Re-weighting a proper
# loss is, and it aims at the right thing -- be accurate where being wrong is
# expensive, and spend less effort where it is cheap.
#
# Weights come from realised spreads in the training window only, so this stays
# as causal as everything else. The spread for the interval being predicted is
# unknown at decision time and is never used.
COST_WEIGHTED_TRAINING = True

# Spreads are heavy-tailed, so a handful of intervals would otherwise dominate
# the fit entirely. Winsorise before normalising.
COST_WEIGHT_CLIP_QUANTILE = 0.99

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

# The Belgian offshore wind zone, grouped into five sampling points. Weights are
# installed MW summed from query_installed_generation_capacity_per_unit, which
# lists ten BE offshore units totalling 2260.8 MW.
#
# These MUST match ZONE. They did not once: after re-pointing from PL back to
# BE, ZONE and TARGET_TECHNOLOGY were changed and these were left as Polish
# voivodeships, so a Belgian fleet was modelled on weather from 1,000 km away.
# Nothing failed, because the intraday feature set leans on lagged outturn and
# the TSO forecast, which carried the model while the weather columns were
# noise. checks.grid_points_match_zone now fails on that.
GRID_POINTS = [
    {"name": "Norther",              "lat": 51.53, "lon": 3.00, "weight": 370.0},
    {"name": "Thorntonbank C-Power", "lat": 51.55, "lon": 2.93, "weight": 325.2},
    {"name": "Rentel + Northwind",   "lat": 51.60, "lon": 2.92, "weight": 523.0},
    {"name": "Seastar + Mermaid",    "lat": 51.65, "lon": 2.85, "weight": 487.5},
    {"name": "Belwind + NW2",        "lat": 51.67, "lon": 2.80, "weight": 555.1},
]

# Rough centre of each bidding zone, used only to assert GRID_POINTS are
# plausibly inside the zone they claim to model.
ZONE_CENTROIDS = {
    "BE":    (50.6, 4.4),
    "NL":    (52.2, 5.3),
    "PL":    (52.1, 19.4),
    "DE_LU": (51.1, 10.4),
    "FR":    (46.6, 2.5),
    "DK_1":  (56.2, 9.5),
    "DK_2":  (55.5, 11.8),
    "ES":    (40.2, -3.6),
    "AT":    (47.6, 14.1),
}

# Farthest a sampling point may sit from its zone's centroid, in degrees.
# Generous -- offshore sites are outside the landmass and zones are large --
# but nowhere near wide enough to let another country through.
MAX_SITE_DISTANCE_DEG = 6.0

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
