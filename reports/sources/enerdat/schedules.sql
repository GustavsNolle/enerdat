-- Four schedules settled against the same year, on identical intervals.
-- persistence = the outturn two hours earlier, the freshest lag that is
-- published before the intraday decision deadline.
with base as (
    select *,
           lag(actual_mw, 2) over (order by valid_time_utc) as persistence_mw,
           price_day_ahead - price_long                     as spread
    from imbalance_settlement
    where price_day_ahead is not null and actual_mw is not null
),
scored as (select * from base where candidate_mw is not null and persistence_mw is not null),
scale as (select 365.0 / count(distinct delivery_date) as f from scored),
unpivoted as (
    select 'TSO forecast' as schedule, 1 as ord, tso_forecast_mw as sched, s.* from scored s
    union all select 'Persistence (T-2h)', 2, persistence_mw, s.* from scored s
    union all select 'Model',              3, candidate_mw,   s.* from scored s
    union all select 'Perfect foresight',  4, actual_mw,      s.* from scored s
)
select
    schedule,
    ord,
    round(avg(abs(sched - actual_mw)), 1)                              as mae_mw,
    round(avg(actual_mw - sched), 1)                                   as bias_mw,
    round(sum((actual_mw - sched) * interval_hours * spread)
          * max(f) / 1e6, 2)                                           as cost_eur_m,
    round(avg((actual_mw - sched) * interval_hours) * avg(spread)
          * count(*) * max(f) / 1e6, 2)                                as bias_term_eur_m
from unpivoted, scale
group by schedule, ord
order by ord
