-- Custom test: returns any rows that break the rules below. 0 rows = pass.
-- 1. percentiles are in order (p10 <= p25 <= median <= p75 <= p90)
-- 2. every % is between 0 and 100
-- 3. no graduate is lost or double-counted: grad counts add up to the intermediate row count
--    (once per dimension in mart_roi_by_band, once in each of the other two marts)
with bands as (

    select * from {{ ref('mart_roi_by_band') }}

),

spread as (

    select * from {{ ref('mart_outcome_spread_by_major') }}

),

what_if as (

    select * from {{ ref('mart_what_if') }}

),

source_grads as (

    select count(*) as n from {{ ref('int_graduates_enriched') }}

)

select band_id as row_id, 'bands: percentiles out of order or bad %' as problem
from bands
where not (p25_net_roi_usd <= median_net_roi_usd and median_net_roi_usd <= p75_net_roi_usd)
   or pct_roi_negative not between 0 and 100

union all

select dimension, 'bands: grad count mismatch'
from bands
group by dimension
having sum(grad_count) <> (select n from source_grads)

union all

select major, 'spread: percentiles out of order'
from spread
where not (p10_net_roi_usd <= p25_net_roi_usd and p25_net_roi_usd <= median_net_roi_usd
           and median_net_roi_usd <= p75_net_roi_usd and p75_net_roi_usd <= p90_net_roi_usd)

union all

select 'ALL MAJORS', 'spread: grad count mismatch'
where (select sum(grad_count) from spread) <> (select n from source_grads)

union all

select what_if_id, 'what_if: percentiles out of order or bad %'
from what_if
where not (p25_net_roi_usd <= median_net_roi_usd and median_net_roi_usd <= p75_net_roi_usd)
   or pct_roi_negative not between 0 and 100

union all

select 'ALL GROUPS', 'what_if: grad count mismatch'
where (select sum(grad_count) from what_if) <> (select n from source_grads)
