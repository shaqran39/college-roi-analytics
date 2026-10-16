# Archive

Old files the pipeline no longer uses, kept for reference.

| File | What it was | Why it's here |
|---|---|---|
| `load_raw.py` | Loaded `data/raw/*.csv` into a local DuckDB file (`warehouse/college.duckdb`) for practice | The project now loads data only on Databricks (`ingestion/databricks_load_raw.sql`). The `dev` (DuckDB) target in `dbt/profiles.yml` is kept but not used |
