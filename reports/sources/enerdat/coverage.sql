-- Fixed facts about the run, used for the filter bounds and the provenance line.
with base as (
    select *, lag(actual_mw, 2) over (order by valid_time_utc) as persistence_mw
    from imbalance_settlement
    where price_day_ahead is not null and actual_mw is not null and candidate_mw is not null
),
scored as (select * from base where persistence_mw is not null)
select
    min(valid_time_utc)::date                   as first_day,
    max(valid_time_utc)::date                   as last_day,
    count(*)                                    as intervals,
    count(distinct delivery_date)               as days,
    round(avg(price_day_ahead - price_long), 2) as mean_spread_eur_mwh
from scored
