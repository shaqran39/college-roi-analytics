-- One row per major category x choice x yes/no. Compares grads who did an internship
-- vs didn't, and who finished on time vs late. major_category = 'all' covers everyone.
-- Note: this compares groups, it doesn't prove the choice caused the difference.

with graduates as (

    select * from {{ ref('int_graduates_enriched') }}

),

-- each grad appears once per choice
choices as (

    select major_category, 'internship' as choice, has_internship as made_choice,
           net_roi_usd, roi_band, payback_years
    from graduates

    union all

    select major_category, 'on_time_completion' as choice, is_completed_on_time as made_choice,
           net_roi_usd, roi_band, payback_years
    from graduates

),

-- add an 'all' copy of every row for the overall comparison
with_all as (

    select * from choices

    union all

    select 'all' as major_category, choice, made_choice, net_roi_usd, roi_band, payback_years
    from choices

),

grouped as (

    select
        major_category,
        choice,
        made_choice,
        count(*)                                                                       as grad_count,
        round(median(net_roi_usd), 0)                                                  as median_net_roi_usd,
        round(100.0 * avg(case when roi_band = 'negative' then 1 else 0 end), 1)       as pct_roi_negative,
        round(median(payback_years), 1)                                                as median_payback_years
    from with_all
    group by major_category, choice, made_choice

),

final as (

    select
        concat(major_category, '-', choice, '-', case when made_choice then 'yes' else 'no' end)
                                                                                       as choice_group_id,
        major_category,
        choice,
        made_choice,
        grad_count,
        median_net_roi_usd,
        pct_roi_negative,
        median_payback_years,

        -- on the 'yes' rows: how much higher the median net ROI is than the matching 'no' row
        case
            when made_choice then median_net_roi_usd - max(case when not made_choice then median_net_roi_usd end)
                                                           over (partition by major_category, choice)
        end                                                                            as net_roi_lift_usd

    from grouped

)

select * from final
