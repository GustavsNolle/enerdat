---
title: Method
description: How the numbers are produced and what to distrust
---

# Method

## The decomposition

Settlement cost splits into two terms:

```
cost = mean(deviation) × mean(spread)  +  cov(deviation, spread)
        └── bias term ──┘                 └── covariance term ──┘
```

On Belgium the bias term is **0.8% of the total**. The mean spread is €0.62/MWh, so
even the TSO's 37 MW standing bias is worth about €0.2m against a €26m bill.
Everything else is covariance — whether errors land on expensive intervals.

Neither squared error nor a quantile objective targets that. Minimising cost
directly is unavailable, because under single pricing it is linear in the schedule
and its optimum is degenerate. **Re-weighting a proper loss** is what remains:
each training sample is weighted by |P<sub>day-ahead</sub> − P<sub>imbalance</sub>|,
winsorised at the 99th percentile and normalised to mean 1, drawn from the training
window only.

The result is a model that is **less accurate and cheaper** — MAE rises 137.3 → 141.6
while cost falls €1.06m, entirely in the covariance term.

## Point-in-time correctness

The schedule is fixed one hour before delivery, so nothing published after that may
inform it.

- **Weather** comes from Open-Meteo's archived *forecast* runs, never reanalysis.
  The reanalysis archive is reconstructed using observations that came afterwards;
  training on it makes a model look superb and be worthless, and nothing errors.
- **Lead time** is chosen per interval: illegal runs are dropped first, then the
  freshest survivor is kept, so no ordering mistake can leak.
- **Lagged outturn** is bounded by `DECISION_LEAD + ACTUALS_PUBLICATION_LAG` = 2h.
  An actual at T−2h is published by T−1h; anything fresher has not been published
  when the schedule is fixed.
- **Training walks forward** on a rolling 90-day window and returns an audit trail
  recording each day's cutoff and the latest interval actually trained on. A test
  asserts the second never exceeds the first.

## The checks

Seven asset checks run on every materialisation. Two have killed results that
looked good.

| Check | What it does |
| --- | --- |
| `cost_measure_is_well_posed` | No schedule may cost less than perfect foresight. **Fired on Poland**, where bidding zero "saved" €5,085m against perfect foresight's €198m. |
| `forecast_actual_comparable` | Forecast and actual must describe the same fleet. **Fired on the Netherlands**, where metered solar is 3.6% of the forecast and offshore wind 283%. |
| `leakage_free_features` | Every feature's implied issue time predates the decision deadline. |
| `market_day_length` | Delivery days span 23, 24 or 25 hours. Verified on real DST days. |
| `unique_intervals` | One row per interval after deduplicating ENTSO-E's restatements. |
| `imbalance_pricing_is_dual` | Reports the settlement regime. Belgium is single-priced, so the quantile objective is degenerate here and falls back to megawatts. |
| `no_trivial_schedule_wins` | A zero schedule costs more than every real schedule, so the ranking is not an artefact of being systematically long. |

## What to distrust

**The honest baseline is persistence, not the TSO.** No BRP schedules Belgian wind
by copying the published day-ahead forecast, so the headline against the TSO
overstates the real opportunity. Against persistence — one window function — the
margin is about €0.16/MWh, which intraday spread and market impact would erode.

**One lag has zero headroom.** The two-hour outturn lag is published exactly at the
decision deadline. Legal under `≤`, but a minute of publication slippage would leak.
Widening `ACTUAL_LAG_HOURS` to start at 3h buys margin at some cost in skill.

**Weather is day-granular.** Open-Meteo's archive means the freshest legal run is
the same at intraday as at day-ahead. The intraday gain is recent outturn, not
better weather.

**Single pricing limits the objective.** ENTSO-E labels the two price columns
`Long`/`Short` regardless of regime; in every zone scanned they are byte-identical.
The cost-optimal quantile framework needs genuine dual pricing and has nowhere to
bite here.

## Pipeline

Dagster daily partitions over ENTSO-E and Open-Meteo, an append-only Parquet lake
partitioned by zone, and DuckDB marts. Raw assets use single-run backfills — ENTSO-E
serves a date range in one response, so a year is a handful of requests rather than
365. Re-materialising never overwrites: each run writes a file stamped with its
retrieval time, which is what makes ENTSO-E's restatements of published actuals
observable rather than silently applied.
