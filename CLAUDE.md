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
- **We only build on prod now.** Always use `--target prod`. Skip local DuckDB builds (`warehouse/college.duckdb`, target `dev`); they're kept only as an old practice setup
- Ingestion (cloud): CSVs uploaded to the volume `/Volumes/workspace/raw/landing`, tables created by `ingestion/databricks_load_raw.sql`
- Transformation: dbt Core in `dbt/` (run all dbt commands from inside the `dbt/` folder)
- AI in pipeline: Databricks `ai_query` (per-major report cards, findings summary). Only run these on Databricks, and guard them with `{{ config(enabled = target.type == 'databricks') }}` (not `target.name`: the scheduled Databricks Job's dbt task uses its own target name)
- Auto insights (separate from the college pipeline): `auto_insights/profile_upload.py` profiles the newest CSV in the volume `/Volumes/workspace/raw/auto_landing/` into the schema `workspace.auto_insights` (Job `auto_insights_refresh`, dashboard page `views/auto_insights.py`, which also has a password-protected uploader in `dashboards/uploader.py` that writes to the volume with its own upload-only token). No dbt. Don't mix it with `raw.landing` or the college schemas. Rule: "private per upload". Each upload's file name gets a random part (`secrets.token_urlsafe(16)`), so its `upload_id` can't be guessed. The page shows exactly one upload: a valid `?report=<upload_id>` link (format-checked, then looked up with a parameterised query, under 24h old), else this browser session's own upload, else the pinned **Example** (`upload_id = 'example'`, made-up café data, created once with `--as-example`, CSV kept in the volume `workspace.auto_insights.example_data`, not `auto_landing`, so it doesn't start the Job). Never add a list, picker or "latest upload" query. Retention after each run: delete the CSV just profiled, any other CSV in `auto_landing` older than 1 hour, and rows in all `workspace.auto_insights` tables for uploads older than 24 hours, never `'example'`. Test with `--cleanup-dry-run`. The analysis adapts to the columns (text numbers / yes-no / dd-mm dates are converted into the scratch table `typed_upload`; extra tables `column_details`, `time_series`, `time_patterns`), and each analysis only runs when it applies. The Job runs the code on **main**, so never ALTER an existing auto_insights table: add new tables instead. Test from a laptop with `--test-file <csv>` (uses the volume `workspace.auto_insights.test_data`, not `auto_landing`, so it doesn't start the Job). After profiler changes, `--refresh-example` re-profiles the Example
- Dashboard design: one theme in `dashboards/.streamlit/config.toml` (light + dark), one chart style in `dashboards/charts.py` (`style()`, and `show()` to draw a chart), and `page_header()` in `db.py` on every page. Horizontal charts with long names switch to a phone layout with `is_phone()` + `names_above_rows()` (names above the rows); desktop keeps the normal layout. Check new chart colours with the dataviz validator and never use colour as the only signal
- Dashboard: Streamlit app in `dashboards/`, deployed to Streamlit Community Cloud (public)
- OS: Windows. Use PowerShell-friendly commands. The venv lives in `.venv`.
- Databricks credentials come from env vars DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN

## Common commands
```
cd dbt
dbt debug --target prod                 # check Databricks connection
dbt build --target prod                 # run models + tests in Databricks
dbt build --target prod --select stg_graduates+   # one model and everything after it
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
- After changing a model, run `dbt build --target prod --select <model>+` and fix failures before saying you're done

## Do not
- Commit anything in `data/raw/` or `warehouse/` (they're gitignored, the data is too big)
- Delete or overwrite raw data. The ONLY exception is the retention clean-up in `auto_insights/profile_upload.py` (`cleanup()`): it may delete CSVs ONLY in `/Volumes/workspace/raw/auto_landing/` (the one just profiled, and others older than 1 hour) and rows older than 24 hours ONLY in `workspace.auto_insights`, never the Example (`upload_id = 'example'`). One more exception in the same file: `refresh_example()` (`--refresh-example`, run by hand) may delete and rewrite ONLY the rows where `upload_id = 'example'`. Never add deleting code anywhere else, and never point it at `raw.landing` or the college schemas
- Commit secrets or credentials (e.g. Databricks tokens). Use environment variables.
