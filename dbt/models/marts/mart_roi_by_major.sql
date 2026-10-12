-- One row per major. Summarises whether each major pays off over 10 years:
-- typical ROI, how grads spread across ROI bands, payback time and debt load.

with graduates as (

    select * from {{ ref('int_graduates_enriched') }}

),

by_major as (

    select
        major,

        count(*)                                                                       as grad_count,

        -- typical 10-year gain after paying for college (median so outliers can't skew it)
        round(median(net_roi_usd), 0)                                                  as median_net_roi_usd,

        -- % of grads in each ROI band (the four add up to 100)
        round(100.0 * avg(case when roi_band = 'negative' then 1 else 0 end), 1)       as pct_roi_negative,
        round(100.0 * avg(case when roi_band = 'low'      then 1 else 0 end), 1)       as pct_roi_low,
        round(100.0 * avg(case when roi_band = 'medium'   then 1 else 0 end), 1)       as pct_roi_medium,
        round(100.0 * avg(case when roi_band = 'high'     then 1 else 0 end), 1)       as pct_roi_high,

        -- payback
        round(median(payback_years), 1)                                                as median_payback_years,
        round(100.0 * avg(case when payback_years > 10 then 1 else 0 end), 1)          as pct_not_paid_back_10yr,

        -- debt
        round(median(debt_to_annual_earnings_ratio), 2)                                as median_debt_to_earnings_ratio

    from graduates
    group by major

)

select * from by_major
