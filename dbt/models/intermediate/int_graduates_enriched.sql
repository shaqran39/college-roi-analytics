-- One row per graduate. Keeps every stg_graduates column and adds
-- easy-to-read measures: yearly earnings, debt vs salary, payback time and an ROI band.

with graduates as (

    select * from {{ ref('stg_graduates') }}

),

enriched as (

    select
        *,

        -- yearly view of the 10-year totals (USD)
        round(earnings_10yr_usd / 10.0, 0)                                 as avg_annual_earnings_usd,
        round(added_earnings_10yr_usd / 10.0, 0)                           as avg_annual_added_earnings_usd,

        -- debt
        debt_usd > 0                                                       as has_debt,
        round(debt_usd / nullif(earnings_10yr_usd / 10.0, 0), 2)           as debt_to_annual_earnings_ratio,

        -- years of the yearly earnings premium needed to earn back the net cost
        round(net_cost_usd / nullif(added_earnings_10yr_usd / 10.0, 0), 1) as payback_years,

        -- built on the dataset's own flags so the band always agrees with them
        case
            when is_high_roi          then 'high'
            when not is_positive_roi  then 'negative'
            when net_roi_usd < 200000 then 'low'
            else 'medium'
        end                                                                as roi_band

    from graduates

)

select * from enriched
