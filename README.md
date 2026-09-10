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
run issued roughly N days before each valid time. Whether a given lead is legal
**varies across the delivery day**: `N=1` is issued ~24h before valid time, which
is fine for a morning hour but for 13:00 means a run from 13:00 on D−1, an hour
*after* gate closure.

So both leads are fetched in one request, every row whose implied issue time
post-dates its own day's gate closure is **dropped first**, and the freshest
survivor is kept per interval. Selection can only ever choose among legal rows,
so no ordering mistake can produce a leak. In practice ~54% of intervals get the
fresher 1-day lead and the rest fall back to 2 days; minimum headroom measured
0.0h and 12.0h respectively, with zero rows past the deadline.

Using `N=2` uniformly, as this originally did, threw that skill away — it cost
13.6% of candidate MAE.

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

## If ENTSO-E returns 404

The Transparency Platform has moved its API host before. On 2026-09-08 the
legacy `web-api.tp.entsoe.eu` began returning `404 page not found` for every
route — including unauthenticated ones, which is a routing failure rather than
an auth one — while the portal stayed up. `entsoe-py` still defaults to that
host, so the client breaks with it.

Nothing here needs a code change for that. Repoint it:

```bash
export ENTSOE_ENDPOINT_URL=https://<current-host>/api
```

`EntsoeResource` fails fast on 404 with that instruction rather than burning
four retries on a route that will never resolve, and distinguishes it from
401/403, which means the key rather than the endpoint.

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
    model.py        walk-forward candidate forecast
    settlement.py   both schedules priced at the imbalance price
  checks.py         leakage, market-day length, interval uniqueness
tests/
  fixtures/            real API payloads captured 2026-09-07
  test_partitions.py   23- and 25-hour days
  test_mart.py         revision dedup, capacity weighting, leakage detection
  test_entsoe_payloads.py  parsing, against real responses
  test_settlement.py   settlement sign conventions
  test_model.py        walk-forward causality
```

## Features

Roughly 40, all derived from archived forecasts and the clock — never from the
outturn, never from prices, never from the TSO's own forecast.

| group | what |
| --- | --- |
| central estimate | five weather variables, capacity-weighted across the five Belgian farm groups then averaged over models |
| **model spread** | disagreement between ICON, GFS and ECMWF per variable — the uncertainty signal the quantile objective needs, since it is estimating a distribution |
| **site structure** | per-site wind speed plus cross-site spread; the fleet spans ~30 km and the gradient across it carries a front's timing |
| **physics** | turbine power curve — zero below cut-in, cubic to rated, flat to cut-out, zero beyond. That last step is why raw wind speed is not enough: in a storm the fleet shuts down, so 30 m/s and a dead calm look identical from the grid's side |
| **shape** | ±1 and ±3 intervals, a centred 3-interval mean, and a first difference, on wind speed and power fraction. Forecast error concentrates on ramps and a model shown one instant cannot see one |
| calendar | hour-of-day on a circle, month |

The two spreads are computed at *different levels of aggregation*. A standard
deviation over the flat (site × model) rows gives the same number for both and
silently attributes mixed variance to whichever column is named — measured
1.02 m/s across models against 0.42 m/s across sites once separated.

Feature selection is an **allow-list**. Prices and the TSO forecast both sit in
the mart and neither is knowable at gate closure, so a subtractive rule would be
one careless column away from a leak.

## The candidate model

Gradient boosting (`HistGradientBoostingRegressor`, 300 iterations). What
matters is *when* the model is allowed to learn things.

A single fit scored on a random split would leak: predicting January with a
model that saw July is not a forecast. So `walk_forward_predict` walks forward.
For delivery day D the model may only have seen intervals whose actuals were
**published** before D's gate closure — gate closure minus
`ACTUALS_PUBLICATION_LAG`, because realised generation is not knowable the
instant it occurs.

It returns an audit trail, not just predictions: one row per delivery day
recording that day's cutoff and the latest interval actually trained on.
`test_training_never_reaches_past_the_cutoff` asserts the second never exceeds
the first. Where history is too short the model **abstains and emits NULL**
rather than a prediction nobody should trust.

`USE_TSO_FORECAST_AS_FEATURE` is `False` on purpose. Article 14.1.D forecasts
are published by 18:00 on D−1, *after* the 12:00 gate closure this project
treats as the decision point. We therefore beat the TSO using strictly less
information than it had — which costs accuracy, but makes a win unambiguous.

## Training window

Two separate knobs, and conflating them is easy:

- `MIN_TRAIN_DAYS` — a **gate**. When does the model have enough history to
  start predicting at all?
- `TRAIN_WINDOW_DAYS` — a **window**. How far back may each fit look?

Only the second changes what a model learns. With expanding history, raising
the gate from 60 to 240 days produced *byte-identical* predictions on every day
both settings could reach — it only shortened the evaluation period.

Swept on a common 7,292-interval evaluation set, as candidate MAE ÷ TSO MAE:

| window | 60d | **90d** | 180d | 270d | expanding |
| --- | --- | --- | --- | --- | --- |
| ratio | 1.225 | **1.211** | 1.220 | 1.237 | 1.255 |

A shallow U with its minimum near a quarter. The notable end is the far one:
training on **all** history is the worst setting tested, 3.6% behind a 90-day
window. More data actively hurts, because old intervals describe a fleet and a
curtailment regime that have since moved on. 60–180 days all sit within ~1% of
the best, so the effect is real but not sharp — don't over-tune it on one year.

A window shorter than the gate is rejected outright: the windowed history could
never satisfy the gate, so every day would abstain and the run would look
successful while forecasting nothing.

## Settlement

`imbalance_settlement_mart` prices the TSO's own forecast error interval by
interval — the baseline any candidate model has to beat. The headline number is
the difference between this and the same settlement run on your schedule.

    imbalance_mwh = (actual - scheduled) * interval_hours
    long  (> 0) -> settled at the Long price
    short (< 0) -> settled at the Short price

Cost is not a function of `|error|`, which is why MAE is the wrong objective
here. Being long into a *negative* price means paying to deliver. And a
schedule can be wrong by four times as much as the TSO's yet settle cheaper, by
being wrong in the long direction while the TSO is short — `test_settlement.py`
pins both cases.

`savings_eur` is the headline: baseline cost minus candidate cost, per interval.
Negative means the candidate lost, and it is reported as a loss rather than an
absolute value.

The settlement period is derived from the data rather than assumed, because the
market moved to quarter-hourly MTU mid-history and DST days contain an interval
of a different length.

## Notes on the data

- **Zone is BE**, chosen on evidence. Two things must hold at once: the zone
  publishes a bidding-zone imbalance price (DE_LU does not), *and* its 14.1.D
  forecast describes the same fleet as its 16.1 metered actual. Measured mean
  actual / mean forecast: BE 1.05/1.00/0.95 for solar/offshore/onshore, versus
  NL's 0.04/2.83/0.25 — Dutch distributed generation never reaches TenneT's
  aggregate, so subtracting one from the other there measures scope, not error.
  `forecast_actual_comparable` now fails the mart on exactly that.
- **Backfills use range requests, not one per day.** ENTSO-E serves an arbitrary
  window in a single response, so the raw assets carry
  `BackfillPolicy.single_run()`: 70 days is one request of about a minute rather
  than 70 requests of twenty minutes. Requests are capped at `MAX_QUERY_DAYS`
  because the platform does not reliably serve a full year — a 364-day request
  returned 133 KB and then went silent with the connection still open.
- **`entsoe-py` sets no request timeout by default**, so that silence blocks
  forever; `EntsoeResource` sets one. Note it is a *read* timeout, not a
  total-duration cap, which is why chunking rather than the timeout is the real
  protection.
- **Market resolution is not a constant.** BE publishes the day-ahead forecast
  hourly, NL quarter-hourly. Any threshold expressed in intervals silently means
  different amounts of history per zone, which is why `MIN_TRAIN_DAYS` counts
  days.
- **Market days have 23, 24 or 25 hours.** Delivery windows use calendar
  arithmetic, never `Timedelta(days=1)`. `market_day_length` asserts it.
- **Day-ahead is 15-minute now**, weather is hourly. Features are held constant
  within the hour — an explicit choice, visible in the join.
