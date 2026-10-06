# CLAUDE.md: project rules for Claude Code

Claude Code reads this file at the start of every session. Keep it up to date.

## What this project is
An end-to-end analytics project on `college_major_roi.csv`: 30,000 graduates (synthetic data), one row each, with major, institution tier, region, GPA, internship, cost, debt and 10-year earnings vs a high-school baseline.
Business question: **Which majors and college types actually pay off over 10 years, and how much do internships, finishing on time and debt change the answer?**

Known data issues: 288 rows have institution_selectivity_pctile > 100 (capped in stg_graduates). Log any new issue in the README "Data quality" table.

## About the owner
Shaqran is learning AI-assisted data engineering. When you make changes:
- Explain what you did and why in plain, simple language
- Work in small steps and say what the next step is
- Never make big changes across many files without asking first

## Stack
- **Production: Databricks Free Edition.** Catalog `workspace`, raw table `workspace.raw.college_major_roi`, dbt builds into `workspace.staging`, `workspace.intermediate` and `workspace.marts`
- Local practice: DuckDB at `warehouse/college.duckdb` (dbt target `dev`, loaded by `ingestion/load_raw.py`)
- Ingestion (cloud): CSVs uploaded to the volume `/Volumes/workspace/raw/landing`, tables created by `ingestion/databricks_load_raw.sql`
- Transformation: dbt Core in `dbt/` (run all dbt commands from inside the `dbt/` folder)
- AI in pipeline: Databricks `ai_query` (per-major report cards, findings summary). Only run these on the `prod` target, and guard them with `{% if target.name == 'prod' %}`
- Dashboard: Streamlit app in `dashboards/`, deployed to Streamlit Community Cloud (public)
- OS: Windows. Use PowerShell-friendly commands. The venv lives in `.venv`.
- Databricks credentials come from env vars DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN

## Common commands
```
cd dbt
dbt debug --target prod                 # check Databricks connection
dbt build --target prod                 # run models + tests in Databricks
dbt build --target prod --select stg_graduates+   # one model and everything after it
dbt build                               # same thing on local DuckDB (practice)
dbt docs generate; dbt docs serve
```

## Free Edition limits to respect
- One small SQL warehouse, serverless only, max 5 concurrent job tasks
- Keep AI function calls cheap: test on `limit 100` before running on the full table
- Non-commercial use only

## dbt layer rules
| Layer | Folder | Prefix | Materialisation | Purpose |
|---|---|---|---|---|
| Staging | models/staging | `stg_` | view | 1:1 with a raw table. Rename, cast types, light cleaning. No joins. |
| Intermediate | models/intermediate | `int_` | view | Joins and business logic |
| Marts | models/marts | `fct_` / `dim_` / `mart_` | table | Star schema + summary tables for the dashboard |

## Conventions
- snake_case for everything. Timestamps end in `_at`, dates in `_date`, booleans start with `is_` / `has_`
- Every model gets an entry in a `.yml` file with a description
- Every primary key gets `unique` + `not_null` tests. Foreign keys get `relationships` tests
- Always reference other models with `ref()` and raw tables with `source()`, never hard-coded table names
- Use CTEs: import CTEs at the top, then logic, then a final `select`
- After changing a model, run `dbt build --select <model>+` and fix failures before saying you're done

## Do not
- Commit anything in `data/raw/` or `warehouse/` (they're gitignored, the data is too big)
- Delete or overwrite raw data
- Commit secrets or credentials (e.g. Databricks tokens). Use environment variables.
