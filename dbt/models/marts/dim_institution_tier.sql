-- One row per institution tier, with a sort order so charts list tiers
-- from most to least selective.

with graduates as (

    select * from {{ ref('int_graduates_enriched') }}

),

tiers as (

    select distinct institution_tier
    from graduates

),

final as (

    select
        institution_tier,
        case institution_tier
            when 'ivy_elite'  then 1
            when 'top_public' then 2
            when 'mid_tier'   then 3
            when 'regional'   then 4
            when 'for_profit' then 5
        end as tier_sort_order
    from tiers

)

select * from final
