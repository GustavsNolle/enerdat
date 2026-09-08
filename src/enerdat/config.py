"""Project constants.

Everything a reader needs to understand the *scope* of the pipeline lives here.
The two values that carry real analytical weight are GATE_CLOSURE_LOCAL and
WEATHER_LEAD_DAYS -- together they define the information set the model is
allowed to see. See docs in `checks.py` for why.
"""

from __future__ import annotations

import datetime as dt

# --- scope -------------------------------------------------------------------

# Bidding zone, as an entsoe-py alias. NL is the default rather than DE_LU
# because Germany publishes no imbalance price at bidding-zone level, and the
# imbalance price is what converts forecast error into euros.
ZONE = "NL"

# The series we are trying to predict, as ENTSO-E names it.
TARGET_TECHNOLOGY = "Wind Offshore"

# ENTSO-E market time. Note this is a *market* convention, not a display
# preference: delivery days are defined in it, so DST days have 23 or 25 hours.
MARKET_TZ = "Europe/Brussels"

PARTITION_START = "2024-01-01"

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

# --- weather sampling --------------------------------------------------------

# Dutch offshore wind sites. A handful of real locations beats a national
# average, because offshore wind is spatially concentrated. Capacity weights
# are approximate and should be replaced with values derived from
# `query_installed_generation_capacity_per_unit`.
GRID_POINTS = [
    {"name": "Borssele",             "lat": 51.70, "lon": 3.05, "weight": 1.5},
    {"name": "Hollandse Kust Zuid",  "lat": 52.30, "lon": 4.02, "weight": 1.5},
    {"name": "Hollandse Kust Noord", "lat": 52.69, "lon": 4.24, "weight": 0.7},
    {"name": "Luchterduinen",        "lat": 52.40, "lon": 4.18, "weight": 0.1},
    {"name": "Gemini",               "lat": 54.04, "lon": 5.96, "weight": 0.6},
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
