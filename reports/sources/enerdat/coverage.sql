-- Scope of the run, so the page states its own provenance rather than
-- relying on numbers typed into prose.
with base as (
    select * from imbalance_settlement
    where price_day_ahead is not null
      and actual_mw is not null
      and candidate_mw is not null
)
select
    count(*)                                    as intervals,
    count(distinct delivery_date)               as days,
    min(valid_time_utc)::date                   as first_day,
    max(valid_time_utc)::date                   as last_day,
    round(avg(actual_mw), 0)                    as mean_output_mw,
    round(avg(price_day_ahead - price_long), 2) as mean_spread_eur_mwh,
    -- A schedule of zero, as the yardstick every "saving" must be read against.
    round(sum(actual_mw * interval_hours * (price_day_ahead - price_long))
          * 365.0 / count(distinct delivery_date) / 1e6, 2) as zero_schedule_cost_eur_m
from base
