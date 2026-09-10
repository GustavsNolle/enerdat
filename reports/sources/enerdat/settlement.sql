-- One row per settled market interval. Deliberately NOT pre-aggregated: the
-- dashboard slices this at query time, so filters actually change the numbers
-- rather than re-labelling a fixed summary.
select
    valid_time_utc,
    delivery_date,
    strftime(delivery_date, '%Y-%m')                     as month,
    hour(valid_time_utc)                                 as hour_utc,
    interval_hours,
    actual_mw,
    tso_forecast_mw,
    candidate_mw,
    lag(actual_mw, 2) over (order by valid_time_utc)     as persistence_mw,
    price_day_ahead,
    price_long                                           as price_imbalance,
    price_day_ahead - price_long                         as spread_eur_mwh
from imbalance_settlement
where price_day_ahead is not null
  and actual_mw is not null
  and candidate_mw is not null
order by valid_time_utc
