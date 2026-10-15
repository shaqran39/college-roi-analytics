-- One row per major x institution tier x internship x on-time completion.
-- Powers the dashboard's "What if" tool: pick a combination, see the typical outcome
-- for graduates like that. Small groups are kept; the dashboard warns when grad_count is low.
-- Note: this compares groups, it doesn't prove the choices caused the outcome.

with graduates as (

    select * from {{ ref('int_graduates_enriched') }}

),

combos as (

    select
        concat_ws('-',
            major,
            institution_tier,
            case when has_internship       then 'intern' else 'no_intern' end,
            case when is_completed_on_time then 'on_time' else 'late' end
        )                                                                              as what_if_id,
        major,
        institution_tier,
        has_internship,
        is_completed_on_time,
        count(*)                                                                       as grad_count,
        round(percentile_cont(0.25) within group (order by net_roi_usd), 0)            as p25_net_roi_usd,
        round(median(net_roi_usd), 0)                                                  as median_net_roi_usd,
        round(percentile_cont(0.75) within group (order by net_roi_usd), 0)            as p75_net_roi_usd,
        round(100.0 * avg(case when roi_band = 'negative' then 1 else 0 end), 1)       as pct_roi_negative,
        round(median(payback_years), 1)                                                as median_payback_years,
        round(median(net_cost_usd), 0)                                                 as median_net_cost_usd,
        round(median(debt_usd), 0)                                                     as median_debt_usd
    from graduates
    group by major, institution_tier, has_internship, is_completed_on_time

)

select * from combos
