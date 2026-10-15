-- One row per GPA band and per debt band. Shows how net ROI moves as GPA or debt goes up,
-- with the middle half of outcomes (25th to 75th percentile) around the median.
-- Note: this compares groups, it doesn't prove GPA or debt caused the difference.

with graduates as (

    select * from {{ ref('int_graduates_enriched') }}

),

-- each grad appears once per dimension; band_order keeps the bands in the right order on a chart
banded as (

    select
        'gpa' as dimension,
        case
            when gpa < 2.25 then 1
            when gpa < 2.50 then 2
            when gpa < 2.75 then 3
            when gpa < 3.00 then 4
            when gpa < 3.25 then 5
            when gpa < 3.50 then 6
            when gpa < 3.75 then 7
            else                 8
        end as band_order,
        case
            when gpa < 2.25 then 'Under 2.25'
            when gpa < 2.50 then '2.25–2.49'
            when gpa < 2.75 then '2.50–2.74'
            when gpa < 3.00 then '2.75–2.99'
            when gpa < 3.25 then '3.00–3.24'
            when gpa < 3.50 then '3.25–3.49'
            when gpa < 3.75 then '3.50–3.74'
            else                 '3.75–4.00'
        end as band_label,
        net_roi_usd,
        roi_band
    from graduates

    union all

    select
        'debt' as dimension,
        case
            when debt_usd = 0      then 1
            when debt_usd < 25000  then 2
            when debt_usd < 50000  then 3
            when debt_usd < 75000  then 4
            when debt_usd < 100000 then 5
            when debt_usd < 150000 then 6
            else                        7
        end as band_order,
        case
            when debt_usd = 0      then 'No debt'
            when debt_usd < 25000  then 'Under US$25K'
            when debt_usd < 50000  then 'US$25K–50K'
            when debt_usd < 75000  then 'US$50K–75K'
            when debt_usd < 100000 then 'US$75K–100K'
            when debt_usd < 150000 then 'US$100K–150K'
            else                        'US$150K+'
        end as band_label,
        net_roi_usd,
        roi_band
    from graduates

),

by_band as (

    select
        concat(dimension, '-', cast(band_order as string))                             as band_id,
        dimension,
        band_order,
        band_label,
        count(*)                                                                       as grad_count,
        round(percentile_cont(0.25) within group (order by net_roi_usd), 0)            as p25_net_roi_usd,
        round(median(net_roi_usd), 0)                                                  as median_net_roi_usd,
        round(percentile_cont(0.75) within group (order by net_roi_usd), 0)            as p75_net_roi_usd,
        round(100.0 * avg(case when roi_band = 'negative' then 1 else 0 end), 1)       as pct_roi_negative
    from banded
    group by dimension, band_order, band_label

)

select * from by_band
