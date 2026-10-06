-- One row per graduate. Rename columns, turn 0/1 flags into true/false,
-- and fix the known data issue: selectivity percentiles above 100.

with source as (

    select * from {{ source('college', 'college_major_roi') }}

),

cleaned as (

    select
        -- ids
        grad_id,

        -- education
        major,
        major_category,
        institution_tier,
        least(institution_selectivity_pctile, 100)   as selectivity_pctile,
        institution_selectivity_pctile > 100         as is_selectivity_capped,
        cast(gpa as decimal(3, 2))                    as gpa,
        had_internship = 1                            as has_internship,
        completed_on_time = 1                         as is_completed_on_time,
        region,

        -- money (USD)
        net_cost_usd,
        debt_usd,
        hs_baseline_10yr_usd,
        added_earnings_10yr_usd,
        earnings_10yr_usd,
        net_roi_usd,
        roi_pct,
        positive_roi = 1                              as is_positive_roi,
        high_roi = 1                                  as is_high_roi

    from source

)

select * from cleaned
