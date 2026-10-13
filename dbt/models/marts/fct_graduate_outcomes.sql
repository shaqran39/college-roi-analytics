-- One row per graduate: the centre of the star schema.
-- Holds the measures plus keys to dim_major, dim_institution_tier and dim_region.
-- Descriptive details (like major_category) live in the dimensions, not here.

with graduates as (

    select * from {{ ref('int_graduates_enriched') }}

),

final as (

    select
        -- keys
        grad_id,
        major,
        institution_tier,
        region,

        -- about the graduate
        gpa,
        selectivity_pctile,
        is_selectivity_capped,
        has_internship,
        is_completed_on_time,

        -- money (USD)
        net_cost_usd,
        debt_usd,
        hs_baseline_10yr_usd,
        added_earnings_10yr_usd,
        earnings_10yr_usd,
        net_roi_usd,
        roi_pct,

        -- measures from int_graduates_enriched
        avg_annual_earnings_usd,
        has_debt,
        debt_to_annual_earnings_ratio,
        payback_years,
        roi_band,
        is_positive_roi,
        is_high_roi

    from graduates

)

select * from final
