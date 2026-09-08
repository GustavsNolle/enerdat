# Imbalance Alpha

Can you beat the TSO's own published day-ahead renewables forecast — and what is
the improvement worth in euros?

ENTSO-E publishes both the forecast a TSO made and what actually happened. That
makes it a free, continuously-updating supervised-learning dataset with an expert
baseline already inside it, priced by a market that settles every error at the
imbalance price.

This repo is the data pipeline behind that question: Dagster for orchestration,
DuckDB for the warehouse, an append-only Parquet lake in between.

## The one rule everything else serves

To honestly claim you beat a day-ahead forecast, every feature must contain only
what was knowable at **gate closure — 12:00 market time on D−1**.

The obvious way to get historical weather violates this. Reanalysis products
(ERA5, and Open-Meteo's `archive-api`) are reconstructed *using the observations
that came afterwards*. Train on them and the model looks superb and is worthless,
and nothing errors — the metrics just quietly become fiction.

So this pipeline uses **archived forecasts**, not archived weather:

| Endpoint | Verdict |
| --- | --- |
| `archive-api.open-meteo.com` | ERA5 reanalysis. **Leaks. Never used here.** |
| `historical-forecast-api.open-meteo.com` | Archived model runs. What was predicted, at the time. **This one.** |

Lead time is pinned via Open-Meteo's `previous_dayN` variables, which select the
run issued roughly N days before each valid time. `WEATHER_LEAD_DAYS = 2`, because
`N=1` is issued ~24h before valid time — which for a 13:00 delivery hour means a
run from 13:00 on D−1, an hour *after* gate closure. `N=2` clears the deadline for
every hour of the day. It costs forecast skill to buy a bound that holds without
special-casing.

This is not left to a comment. `checks.leakage_free_features` recomputes the
implied issue time of every feature and fails the asset if any of them post-dates
gate closure, and `tests/test_mart.py::test_leakage_check_catches_a_leak` proves
the check actually fires by feeding it deliberately leaking data.

## Revisions

ENTSO-E restates "actual" values after publication. The raw layer is therefore
**append-only**: re-materialising a partition writes a new file stamped with its
retrieval time and never overwrites. The mart takes the latest retrieval per
interval via `QUALIFY row_number()`. Change that clause to
`WHERE retrieved_at_utc <= <as_of>` and you can reconstruct what was known on any
past date.

## Setup

```bash
uv sync
echo "ENTSOE_API_KEY=..." > .env      # free: register, then email transparency@entsoe.eu
export DAGSTER_HOME="$PWD/.dagster_home"
```

## Run

```bash
dagster dev                                        # UI at localhost:3000

# one delivery day, end to end
dagster asset materialize --select openmeteo_archived_forecast \
    --partition 2026-08-28 -m enerdat.definitions

# the mart plus its checks
dagster asset materialize --select forecast_error_mart -m enerdat.definitions

pytest
```

Backfills run through the `backfill_raw` job, which caps concurrency at 4 —
ENTSO-E allows 400 requests/minute and throttles hard above it, and a two-year
backfill across three datasets is several thousand calls.

## Layout

```
src/enerdat/
  config.py         scope, gate closure, lead time, grid points
  partitions.py     daily partitions; DST-correct delivery windows
  resources.py      ENTSO-E + Open-Meteo clients, append-only lake
  assets/
    entsoe.py       day-ahead forecast, actual generation, imbalance price
    weather.py      archived forecasts at each wind site
    marts.py        the point-in-time join
  checks.py         leakage, market-day length, interval uniqueness
tests/
  test_partitions.py  23- and 25-hour days
  test_mart.py        revision dedup, capacity weighting, leakage detection
```

## Notes on the data

- **Zone is NL, not DE_LU.** Germany publishes no imbalance price at bidding-zone
  level, and the imbalance price is what converts MWh of error into euros.
- **Market days have 23, 24 or 25 hours.** Delivery windows use calendar
  arithmetic, never `Timedelta(days=1)`. `market_day_length` asserts it.
- **Day-ahead is 15-minute now**, weather is hourly. Features are held constant
  within the hour — an explicit choice, visible in the join.
