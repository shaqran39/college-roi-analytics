-- One row. An AI-written summary paragraph of the whole project, built from a short
-- fact sheet of the marts. input_facts keeps the exact facts sent, so the paragraph
-- can be checked for made-up numbers.
-- Databricks only (any target, including the scheduled job): ai_query doesn't exist on DuckDB.

{{ config(enabled = target.type == 'databricks') }}

with roi_by_major as (

    select * from {{ ref('mart_roi_by_major') }}

),

roi_by_tier as (

    select * from {{ ref('mart_roi_by_tier') }}

),

choices_effect as (

    select * from {{ ref('mart_choices_effect') }}

),

-- rank majors best to worst by median net ROI
ranked_majors as (

    select
        *,
        row_number() over (order by median_net_roi_usd desc) as best_rank,
        row_number() over (order by median_net_roi_usd asc)  as worst_rank
    from roi_by_major

),

-- the zero-padded rank at the front makes array_sort keep them in order
major_facts as (

    select
        concat(
            'Top 3 majors by median 10-year net return: ',
            array_join(array_sort(collect_list(case when best_rank <= 3 then concat(
                lpad(cast(best_rank as string), 2, '0'), '. ', replace(major, '_', ' '),
                case when median_net_roi_usd < 0 then concat(': typical graduate was US$', format_number(-median_net_roi_usd, 0), ' worse off') else concat(': typical graduate came out US$', format_number(median_net_roi_usd, 0), ' ahead') end,
                ' (', cast(pct_roi_negative as string), '% worse off than not going to college)'
            ) end)), '; '),
            '\nBottom 3 majors by median 10-year net return: ',
            array_join(array_sort(collect_list(case when worst_rank <= 3 then concat(
                lpad(cast(worst_rank as string), 2, '0'), '. ', replace(major, '_', ' '),
                case when median_net_roi_usd < 0 then concat(': typical graduate was US$', format_number(-median_net_roi_usd, 0), ' worse off') else concat(': typical graduate came out US$', format_number(median_net_roi_usd, 0), ' ahead') end,
                ' (', cast(pct_roi_negative as string), '% worse off than not going to college)'
            ) end)), '; ')
        ) as facts
    from ranked_majors

),

tier_facts as (

    select
        concat(
            'By institution tier: ',
            array_join(array_sort(collect_list(concat(
                replace(institution_tier, '_', ' '), ': typical graduate',
                case when median_net_roi_usd < 0 then concat(' was US$', format_number(-median_net_roi_usd, 0), ' worse off')
                     else concat(' came out US$', format_number(median_net_roi_usd, 0), ' ahead') end,
                ', ', cast(pct_roi_negative as string), '% of graduates worse off than not going to college',
                ', median debt US$', format_number(median_debt_usd, 0)
            ))), '; ')
        ) as facts
    from roi_by_tier

),

choice_facts as (

    select
        concat(
            'Choices, all graduates (median net return with vs without, and the difference): ',
            array_join(array_sort(collect_list(concat(
                replace(choice, '_', ' '),
                case when yes_roi < 0 then concat(' US$', format_number(-yes_roi, 0), ' worse off') else concat(' US$', format_number(yes_roi, 0)) end,
                ' vs', case when no_roi < 0 then concat(' US$', format_number(-no_roi, 0), ' worse off') else concat(' US$', format_number(no_roi, 0)) end,
                ', difference US$', format_number(yes_roi - no_roi, 0)
            ))), '; ')
        ) as facts
    from (
        select
            choice,
            max(case when made_choice     then median_net_roi_usd end) as yes_roi,
            max(case when not made_choice then median_net_roi_usd end) as no_roi
        from choices_effect
        where major_category = 'all'
        group by choice
    )

),

fact_sheet as (

    select concat_ws('\n',
        concat('Graduates analysed: ', format_number((select sum(grad_count) from roi_by_major), 0),
               ' across ', cast((select count(*) from roi_by_major) as string), ' majors'),
        m.facts,
        t.facts,
        c.facts
    ) as input_facts
    from major_facts m
    cross join tier_facts t
    cross join choice_facts c

),

key_findings as (

    select
        1                                          as finding_id,
        -- join any stray lines back into one paragraph, adding a missing full stop
        regexp_replace(regexp_replace(trim(ai_query(
            '{{ var("ai_model_endpoint") }}',
            concat(
                'You are summarising a data project for a general audience. The question is: ',
                'which college majors and college types pay off over 10 years, and how much do ',
                'internships and finishing on time change the answer? ',
                'Write ONE paragraph of 4 to 6 sentences in plain English. ',
                'Cover all three parts of the facts: the best and worst majors, the institution tiers ',
                '(including which tier had the most graduates worse off), and the internship and on-time differences. ',
                'Rules: use ONLY the facts and numbers below. Do not add any other numbers, reasons or causes. ',
                'Say that the data is synthetic (simulated graduates, not real people).',
                'Say that the internship and on-time differences compare groups and do not prove cause. ',
                'These are past results from this data, not predictions: write in the past tense and never use the word will. ',
                'Medians describe the typical graduate: never say most or the majority for a median. ',
                'Describe money outcomes only as came out ahead or worse off. Never use the words loss, losses, lost, gain or gains. ',
                'Use Australian English spelling (for example analyse, favourable, labour). ',
                'Money is in US dollars, so write amounts as US$. ',
                'Reply with plain text only: no heading, no bullet points, no quote marks.\n\n',
                input_facts
            ),
            modelParameters => named_struct('temperature', 0.2, 'max_tokens', 400)
        )), '([^.!?\\s])\\s*\\n+\\s*', '$1. '), '\\s*\\n+\\s*', ' ')
                                                   as summary_paragraph,
        input_facts,
        '{{ var("ai_model_endpoint") }}'           as model_endpoint,
        current_timestamp()                        as generated_at
    from fact_sheet

)

select * from key_findings
