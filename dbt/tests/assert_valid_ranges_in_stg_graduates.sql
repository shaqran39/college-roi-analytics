-- Custom test: returns any rows with impossible values. 0 rows = pass.
select *
from {{ ref('stg_graduates') }}
where selectivity_pctile not between 0 and 100
   or gpa not between 0 and 4
   or net_cost_usd < 0
   or debt_usd < 0
