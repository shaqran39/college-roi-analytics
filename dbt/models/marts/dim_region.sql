-- One row per region.

with graduates as (

    select * from {{ ref('int_graduates_enriched') }}

),

regions as (

    select distinct region
    from graduates

)

select * from regions
