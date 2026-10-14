-- Custom test: every major has exactly one report card. 0 rows = pass.
-- Returns majors that are missing a report card or have more than one.
-- Prod only, like the model it checks.

{{ config(enabled = target.name == 'prod') }}

with majors as (

    select major from {{ ref('mart_roi_by_major') }}

),

report_cards as (

    select major, count(*) as card_count
    from {{ ref('mart_major_report_cards') }}
    group by major

)

select
    majors.major,
    coalesce(report_cards.card_count, 0) as card_count
from majors
left join report_cards on majors.major = report_cards.major
where coalesce(report_cards.card_count, 0) <> 1
