-- Monthly settlement cost per schedule. Shown to make the losing months visible.
with base as (
    select *,
           lag(actual_mw, 2) over (order by valid_time_utc) as persistence_mw,
           price_day_ahead - price_long                     as spread
    from imbalance_settlement
    where price_day_ahead is not null and actual_mw is not null
),
scored as (select * from base where candidate_mw is not null and persistence_mw is not null)
select strftime(delivery_date, '%Y-%m') as month, 'TSO forecast' as schedule,
       round(sum((actual_mw - tso_forecast_mw) * interval_hours * spread) / 1e3, 0) as cost_eur_k
from scored group by 1
union all
select strftime(delivery_date, '%Y-%m'), 'Persistence (T-2h)',
       round(sum((actual_mw - persistence_mw) * interval_hours * spread) / 1e3, 0) from scored group by 1
union all
select strftime(delivery_date, '%Y-%m'), 'Model',
       round(sum((actual_mw - candidate_mw) * interval_hours * spread) / 1e3, 0) from scored group by 1
order by month, schedule
