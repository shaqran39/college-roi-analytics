-- Custom test: returns a row if the fact table doesn't hold exactly the same graduates
-- and money as int_graduates_enriched. 0 rows = pass.
with fct as (

    select count(*) as grads, sum(net_roi_usd) as total_net_roi
    from {{ ref('fct_graduate_outcomes') }}

),

source as (

    select count(*) as grads, sum(net_roi_usd) as total_net_roi
    from {{ ref('int_graduates_enriched') }}

)

select fct.grads, source.grads as source_grads, fct.total_net_roi, source.total_net_roi as source_total_net_roi
from fct
cross join source
where fct.grads <> source.grads
   or fct.total_net_roi <> source.total_net_roi
