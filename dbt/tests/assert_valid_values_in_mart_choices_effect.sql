-- Custom test: returns any rows that break the rules below. 0 rows = pass.
-- 1. exactly 32 rows (7 categories + 'all', x 2 choices, x yes/no)
-- 2. for each category and choice, yes + no grad counts equal that category's grad count
-- 3. % negative is between 0 and 100
-- 4. net_roi_lift_usd is filled on every yes row and empty on every no row
with mart as (

    select * from {{ ref('mart_choices_effect') }}

),

graduates as (

    select major_category from {{ ref('int_graduates_enriched') }}

),

category_counts as (

    select major_category, count(*) as grads from graduates group by major_category
    union all
    select 'all' as major_category, count(*) as grads from graduates

),

mart_counts as (

    select major_category, choice, sum(grad_count) as grads
    from mart
    group by major_category, choice

)

select 'row count' as problem, cast(count(*) as string) as detail
from mart
having count(*) <> 32

union all

select 'grad count mismatch' as problem, concat(m.major_category, '-', m.choice) as detail
from mart_counts m
left join category_counts c on m.major_category = c.major_category
where c.grads is null or m.grads <> c.grads

union all

select 'bad values' as problem, choice_group_id as detail
from mart
where pct_roi_negative < 0
   or pct_roi_negative > 100
   or (made_choice and net_roi_lift_usd is null)
   or (not made_choice and net_roi_lift_usd is not null)
