-- One row per major, with the category it belongs to.
-- Each major maps to exactly one category in the data.

with graduates as (

    select * from {{ ref('int_graduates_enriched') }}

),

majors as (

    select distinct
        major,
        major_category
    from graduates

)

select * from majors
