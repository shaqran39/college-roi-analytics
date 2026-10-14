-- Custom test: mart_key_findings has exactly one row. 0 rows = pass.
-- Prod only, like the model it checks.

{{ config(enabled = target.name == 'prod') }}

select count(*) as row_count
from {{ ref('mart_key_findings') }}
having count(*) <> 1
