-- One row per major. How spread out the 10-year net ROI is: worst case (10th percentile),
-- typical (median) and best case (90th percentile), plus the middle half (25th to 75th).

with graduates as (

    select * from {{ ref('int_graduates_enriched') }}

),

spread as (

    select
        major,
        count(*)                                                                       as grad_count,
        round(percentile_cont(0.10) within group (order by net_roi_usd), 0)            as p10_net_roi_usd,
        round(percentile_cont(0.25) within group (order by net_roi_usd), 0)            as p25_net_roi_usd,
        round(median(net_roi_usd), 0)                                                  as median_net_roi_usd,
        round(percentile_cont(0.75) within group (order by net_roi_usd), 0)            as p75_net_roi_usd,
        round(percentile_cont(0.90) within group (order by net_roi_usd), 0)            as p90_net_roi_usd
    from graduates
    group by major

)

select * from spread
