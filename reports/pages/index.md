---
title: Imbalance Ledger
description: Settlement cost of Belgian offshore wind schedules
---

```sql cover
select * from enerdat.coverage
```

<DateRange
  name=range
  title="Delivery period"
  start={cover[0].first_day}
  end={cover[0].last_day}
/>

<ButtonGroup name=grain title="Compare against" defaultValue="tso">
  <ButtonGroupItem valueLabel="TSO forecast" value="tso" default/>
  <ButtonGroupItem valueLabel="Persistence" value="pers"/>
</ButtonGroup>

```sql filtered
select
    *,
    (actual_mw - tso_forecast_mw) * interval_hours * spread_eur_mwh as tso_cost,
    (actual_mw - persistence_mw)  * interval_hours * spread_eur_mwh as pers_cost,
    (actual_mw - candidate_mw)    * interval_hours * spread_eur_mwh as model_cost,
    '${inputs.grain}'                                               as baseline_key
from enerdat.settlement
where persistence_mw is not null
  and delivery_date between '${inputs.range.start}' and '${inputs.range.end}'
```

```sql kpi
select
    round(sum(case when baseline_key = 'tso' then tso_cost else pers_cost end)
        - sum(model_cost), 0)                                       as saved_eur,
    round(sum(model_cost), 0)                                       as model_cost_eur,
    round(avg(abs(actual_mw - candidate_mw)), 1)                    as model_mae,
    round(avg(abs(actual_mw - case when baseline_key = 'tso'
                then tso_forecast_mw else persistence_mw end)), 1)  as base_mae,
    round(avg(actual_mw - candidate_mw), 1)                         as model_bias,
    count(*)                                                        as intervals
from ${filtered}
```

<Grid cols=4>
  <BigValue data={kpi} value=saved_eur title="Saved vs baseline" fmt='€#,##0' 
    comparison=model_bias comparisonTitle="model bias MW" comparisonFmt='+#,##0.0'/>
  <BigValue data={kpi} value=model_cost_eur title="Model settlement cost" fmt='€#,##0'/>
  <BigValue data={kpi} value=model_mae title="Model MAE (MW)" fmt='#,##0.0'
    comparison=base_mae comparisonTitle="baseline MAE" comparisonFmt='#,##0.0'/>
  <BigValue data={kpi} value=intervals title="Intervals in range" fmt='#,##0'/>
</Grid>

<Alert status=info>
The model is usually <strong>less accurate</strong> than the baseline and still settles
cheaper. Cost is driven by whether errors land on expensive intervals, not by their size.
</Alert>

## Cost over time

```sql daily
select
    delivery_date,
    round(sum(case when baseline_key = 'tso' then tso_cost else pers_cost end) / 1e3, 1) as baseline_k,
    round(sum(model_cost) / 1e3, 1)                                                           as model_k,
    round(sum(sum(case when baseline_key = 'tso' then tso_cost else pers_cost end)
            - sum(model_cost)) over (order by delivery_date) / 1e6, 3)                        as cumulative_saved_m
from ${filtered}
group by delivery_date
order by delivery_date
```

<Grid cols=2>
  <LineChart data={daily} x=delivery_date y=cumulative_saved_m
    title="Cumulative saving (€m)" yAxisTitle="€m" />
  <LineChart data={daily} x=delivery_date y={["baseline_k","model_k"]}
    title="Daily cost (€k)" yAxisTitle="€k" />
</Grid>

## Where the cost concentrates

```sql by_hour
select
    hour_utc,
    round(sum(model_cost) / 1e3, 1)     as model_k,
    round(avg(spread_eur_mwh), 1)       as mean_spread,
    round(avg(abs(actual_mw - candidate_mw)), 0) as mae
from ${filtered} group by hour_utc order by hour_utc
```

```sql by_month
select
    month,
    round(sum(case when baseline_key = 'tso' then tso_cost else pers_cost end) / 1e3, 0) as baseline_k,
    round(sum(model_cost) / 1e3, 0)                                                          as model_k
from ${filtered} group by month order by month
```

<Grid cols=2>
  <BarChart data={by_month} x=month y={["baseline_k","model_k"]} type=grouped
    title="Cost by month (€k)" yAxisTitle="€k"/>
  <BarChart data={by_hour} x=hour_utc y=model_k
    title="Model cost by hour of day (€k)" yAxisTitle="€k" xAxisTitle="hour UTC"/>
</Grid>

## Most expensive intervals

The twenty intervals that cost the model most. Sort or filter to find what they
have in common — they are overwhelmingly high-spread hours, not high-error ones.

```sql worst
select
    valid_time_utc,
    round(actual_mw, 0)                        as actual_mw,
    round(candidate_mw, 0)                     as model_mw,
    round(actual_mw - candidate_mw, 0)         as error_mw,
    round(spread_eur_mwh, 1)                   as spread,
    round(model_cost, 0)                       as cost_eur
from ${filtered} order by model_cost desc limit 20
```

<DataTable data={worst} rows=10 search=true>
  <Column id=valid_time_utc title="Interval"/>
  <Column id=actual_mw title="Actual" fmt='#,##0'/>
  <Column id=model_mw title="Scheduled" fmt='#,##0'/>
  <Column id=error_mw title="Error" fmt='+#,##0'/>
  <Column id=spread title="Spread €/MWh" fmt='#,##0.0'/>
  <Column id=cost_eur title="Cost €" fmt='€#,##0'/>
</DataTable>

<Details title="Provenance and caveats">

Belgian offshore wind, <Value data={cover} column=days/> delivery days,
<Value data={cover} column=intervals fmt='#,##0'/> settled intervals. Schedule fixed
one hour before delivery; mean day-ahead-to-imbalance spread
<Value data={cover} column=mean_spread_eur_mwh fmt='€0.00'/>/MWh.

Every figure is computed by SQL against the DuckDB marts the pipeline builds — the
same tables seven asset checks run against. **The honest baseline is persistence,
not the TSO**: no BRP schedules Belgian wind by copying the published forecast.
Against persistence the margin is roughly €0.16/MWh, which intraday spread and market
impact would erode. Full method, checks and caveats on the [Method](/method) page.

</Details>
