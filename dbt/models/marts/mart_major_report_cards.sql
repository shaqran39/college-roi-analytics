-- One row per major. A rule-based takeaway plus 2 AI-written sentences on whether the major pays off,
-- built only from that major's numbers in mart_roi_by_major.
-- Prod only: ai_query exists on Databricks, not DuckDB.

{{ config(enabled = target.name == 'prod') }}

with roi_by_major as (

    select * from {{ ref('mart_roi_by_major') }}

),

-- optional test filter: --vars "{report_card_majors: ['a', 'b']}"
selected_majors as (

    select * from roi_by_major
    {% if var('report_card_majors') | length > 0 %}
    where major in (
        {%- for m in var('report_card_majors') -%}
            '{{ m }}'{{ ',' if not loop.last }}
        {%- endfor -%}
    )
    {% endif %}

),

-- the takeaway label comes from fixed rules, not the AI, so it is consistent and checkable
with_takeaway as (

    select
        *,
        case
            when median_net_roi_usd < 0      then 'Financially risky.'
            when pct_roi_negative >= 25      then 'A mixed bet.'
            when median_net_roi_usd >= 300000 then 'A strong financial bet.'
            when median_net_roi_usd >= 100000 then 'A solid choice.'
            else                                  'A modest payoff.'
        end as takeaway
    from selected_majors

),

prompts as (

    select
        *,
        concat(
            'You are writing a short verdict for a student deciding whether to study a college major. ',
            'The verdict starts with this takeaway, which is already written: ', takeaway, ' ',
            'Write the next 2 sentences only, in plain English, backing up the takeaway with the numbers. ',
            'Do not repeat the takeaway. ',
            'Rules: use ONLY the numbers given below. Do not add any other numbers, salaries, job titles, ',
            'reasons, causes or comparisons with other majors. ',
            'These are past results from this data, not predictions: write in the past tense. Never use the word will. ',
            'Never say most or the majority. Leave out any share that is 0.0%. ',
            'Do not explain how the return is calculated. ',
            'Write 2 natural, flowing sentences of about 45 words in total, using the 4 or 5 most telling numbers. ',
            'Use Australian English spelling (for example analyse, favourable, labour). ',
            'Money is in US dollars, so write amounts as US$. ',
            'Where a line below is a full sentence, keep its meaning exactly. ',
            'Reply with plain text only: no heading, no bullet points, no quote marks.\n\n',
            'Major: ', replace(major, '_', ' '), '\n',
            'Graduates in the data: ', cast(grad_count as string), '\n',
            'After 10 years, counting extra earnings vs high school minus net cost, the typical graduate ',
                case
                    when median_net_roi_usd < 0 then concat('lost US$', format_number(-median_net_roi_usd, 0))
                    else concat('came out US$', format_number(median_net_roi_usd, 0), ' ahead')
                end, '.\n',
            'Share of graduates worse off than not going to college: ', cast(pct_roi_negative as string), '%\n',
            'Share with a low positive return (under US$200,000): ', cast(pct_roi_low as string), '%\n',
            'Share with a medium return: ', cast(pct_roi_medium as string), '%\n',
            'Share in the top quarter of returns across all graduates: ', cast(pct_roi_high as string), '%\n',
                case
                    when median_payback_years <= 10
                        then concat('Half of graduates earned back the net cost within ', cast(median_payback_years as string), ' years.')
                    else concat('Half of graduates would need ', cast(median_payback_years as string),
                                ' years or more to earn back the net cost, longer than the 10 years in the data.')
                end, '\n',
            'Share who had not earned back the net cost within 10 years: ', cast(pct_not_paid_back_10yr as string), '%\n',
            'Median debt as a share of yearly earnings: ', cast(median_debt_to_earnings_ratio as string),
                ' (1.0 = debt equals one year of pay)'
        ) as prompt
    from with_takeaway

),

report_cards as (

    select
        major,
        grad_count,
        median_net_roi_usd,
        pct_roi_negative,
        pct_roi_high,
        median_payback_years,
        pct_not_paid_back_10yr,
        median_debt_to_earnings_ratio,
        takeaway,
        -- takeaway + the AI's 2 sentences; any stray line breaks are joined back into one line,
        -- adding a full stop where one is missing, and doubled full stops are removed
        concat(takeaway, ' ', regexp_replace(regexp_replace(
            regexp_replace(
                trim(ai_query(
                    '{{ var("ai_model_endpoint") }}',
                    prompt,
                    modelParameters => named_struct('temperature', 0.2, 'max_tokens', 200)
                )),
                '([^.!?\\s])\\s*\\n+\\s*', '$1. '
            ),
            '\\s*\\n+\\s*', ' '
        ), '\\.(\\s*\\.)+', '.'))                   as verdict,
        '{{ var("ai_model_endpoint") }}'           as model_endpoint,
        current_timestamp()                        as generated_at
    from prompts

)

select * from report_cards
