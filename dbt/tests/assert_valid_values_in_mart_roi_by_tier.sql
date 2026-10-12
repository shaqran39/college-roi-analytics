-- Custom test: returns any rows that break the rules below. 0 rows = pass.
-- 1. the four ROI band %s add up to 100 (allow 0.2 for rounding each to 1 decimal)
-- 2. every % is between 0 and 100
-- 3. no graduate is lost or double-counted: grad counts add up to the intermediate row count
with mart as (

    select * from {{ ref('mart_roi_by_tier') }}

),

totals as (

    select
        (select sum(grad_count) from mart)                          as mart_grads,
        (select count(*) from {{ ref('int_graduates_enriched') }})  as source_grads

)

select institution_tier, 'bad percentages' as problem
from mart
where abs(pct_roi_negative + pct_roi_low + pct_roi_medium + pct_roi_high - 100) > 0.2
   or least(pct_roi_negative, pct_roi_low, pct_roi_medium, pct_roi_high, pct_not_paid_back_10yr) < 0
   or greatest(pct_roi_negative, pct_roi_low, pct_roi_medium, pct_roi_high, pct_not_paid_back_10yr) > 100

union all

select 'ALL TIERS' as institution_tier, 'grad count mismatch' as problem
from totals
where mart_grads <> source_grads
