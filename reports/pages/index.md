---
title: Imbalance Ledger
description: A year of Belgian offshore wind imbalance settlement
---

```sql cover
select * from enerdat.coverage
```

A schedule that forecasts **less accurately** settles **cheaper**. Imbalance cost
is almost entirely covariance — whether errors land on expensive intervals — not
error size, so optimising accuracy optimises the wrong thing.

Belgian offshore wind, <Value data={cover} column=days/> delivery days to
<Value data={cover} column=last_day/>, <Value data={cover} column=intervals fmt='#,##0'/>
scored intervals. Schedule fixed one hour before delivery.

## The result

```sql schedules
select * from enerdat.schedules
```

```sql headline
select
  round(max(case when schedule = 'TSO forecast' then cost_eur_m end)
      - max(case when schedule = 'Model' then cost_eur_m end), 2) as vs_tso,
  round(max(case when schedule = 'Persistence (T-2h)' then cost_eur_m end)
      - max(case when schedule = 'Model' then cost_eur_m end), 2) as vs_persistence,
  round(max(case when schedule = 'Model' then mae_mw end)
      - max(case when schedule = 'Persistence (T-2h)' then mae_mw end), 1) as mae_gap
from enerdat.schedules
```

<BigValue data={headline} value=vs_tso title="Saved vs TSO forecast (€m/yr)" fmt='0.00'/>
<BigValue data={headline} value=vs_persistence title="Saved vs persistence (€m/yr)" fmt='0.00'/>
<BigValue data={headline} value=mae_gap title="MAE vs persistence (MW)" fmt='+0.0'/>

Cost is the opportunity cost of deviating: `(actual − scheduled) × hours ×
(P_dayahead − P_imbalance)`. Perfect foresight is zero by construction and nothing
may beat it — a blocking asset check enforces exactly that, and it is what caught
a €1bn artefact in Poland.

<DataTable data={schedules} rows=4>
  <Column id=schedule title="Schedule"/>
  <Column id=mae_mw title="MAE (MW)" fmt='#,##0.0'/>
  <Column id=bias_mw title="Bias (MW)" fmt='+#,##0.0'/>
  <Column id=cost_eur_m title="Cost (€m/yr)" fmt='#,##0.00'/>
  <Column id=bias_term_eur_m title="of which bias" fmt='#,##0.00'/>
</DataTable>

The **bias column is the argument**. Belgium's mean spread is
<Value data={cover} column=mean_spread_eur_mwh fmt='€0.00'/>/MWh, so even the TSO's
37 MW standing bias is worth about €0.2m against a €26m bill. Everything else is
covariance — which neither squared error nor a quantile objective targets.

## Cumulative saving against the TSO forecast

```sql cumulative
select * from enerdat.cumulative
```

<LineChart data={cumulative} x=day y=cumulative_eur_m yAxisTitle="€m cumulative" 
  title="Running total of avoided settlement cost"/>

The line is not monotonic. The model loses in stretches, and those stretches are
left in rather than smoothed away.

## Monthly settlement cost

```sql monthly
select * from enerdat.monthly
```

<BarChart data={monthly} x=month y=cost_eur_k series=schedule type=grouped
  yAxisTitle="€k per month" title="Cost by schedule and month"/>

The model beats the TSO in most months and loses in two. Negative bars are months
where a schedule earned rather than cost.

## What guards these numbers

```sql zero
select zero_schedule_cost_eur_m from enerdat.coverage
```

Seven asset checks run on every materialisation. Two have already killed results
that looked good:

- **`cost_measure_is_well_posed`** — no schedule may cost less than perfect
  foresight. Fired on Poland, where bidding zero "saved" €5,085m against perfect
  foresight's €198m. Here a zero schedule costs
  <Value data={zero} column=zero_schedule_cost_eur_m fmt='€0.00'/>m, more than every
  real schedule, so the ranking is not an artefact of being systematically long.
- **`forecast_actual_comparable`** — fired on the Netherlands, where metered solar
  is 3.6% of the forecast and offshore wind 283%. Subtracting one from the other
  measures scope, not error.
- **`leakage_free_features`** — every weather feature carries the issue time of the
  run it came from, and all predate the decision deadline. Lagged outturn is bounded
  by `DECISION_LEAD + ACTUALS_PUBLICATION_LAG` = 2h.
- **`market_day_length`** — all 364 delivery days span 23, 24 or 25 hours. Verified
  on real DST days: 2025-10-26 has 25, 2026-03-29 has 23.
- **`imbalance_pricing_is_dual`** — reports that Belgium settles both directions at
  one price, so the cost-optimal quantile objective is degenerate here and falls
  back to megawatts. ENTSO-E labels the columns `Long`/`Short` regardless; they are
  byte-identical.

## What to distrust

**The honest baseline is persistence, not the TSO.** No BRP schedules Belgian wind
by copying the TSO's published forecast, so the headline overstates the real
opportunity. Against persistence — one line of SQL — the margin is
<Value data={headline} column=vs_persistence fmt='€0.00'/>m/yr, roughly €0.16/MWh,
which intraday spread and market impact would erode.

**One lag has zero headroom.** The two-hour outturn lag is published exactly at the
decision deadline. Legal under `≤`, but a minute of publication slippage would leak.
Widening `ACTUAL_LAG_HOURS` to start at 3h buys margin at some cost in skill.

**Weather is day-granular.** Open-Meteo's archive means the freshest legal run is the
same at intraday as at day-ahead. The intraday gain is recent outturn, not better
weather.
