-- Running total of settlement cost avoided against the TSO forecast.
select
    valid_time_utc::date                                        as day,
    round(sum(sum(saving)) over (order by valid_time_utc::date) / 1e6, 3) as cumulative_eur_m
from (
    select valid_time_utc,
           (actual_mw - tso_forecast_mw) * interval_hours * (price_day_ahead - price_long)
         - (actual_mw - candidate_mw)    * interval_hours * (price_day_ahead - price_long)
           as saving
    from imbalance_settlement
    where price_day_ahead is not null and candidate_mw is not null and actual_mw is not null
)
group by valid_time_utc::date
order by day
