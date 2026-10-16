# Does College Pay Off? AI-Powered ROI Analytics on Databricks

🔗 **Live dashboard: [https://college-roi-analytics-wdsk3fzehd7jx4ncmyr3sr.streamlit.app/](https://college-roi-analytics-wdsk3fzehd7jx4ncmyr3sr.streamlit.app/)** (synthetic data, first load can take ~30 seconds while the warehouse wakes up)

**What it is:** a cloud data pipeline and public dashboard that analyses 30,000 graduates' costs, debt and 10-year earnings. AI is used to *build* it (Claude Code) and *inside* it (AI-written insights and major "report cards").

**Business question:** Which majors and college types actually pay off over 10 years, and how much do internships, finishing on time and debt change the answer?

## How it works
```
college_major_roi.csv
   │  uploaded to a Databricks Volume
   ▼
RAW            workspace.raw.college_major_roi
   │  dbt staging       clean, rename, fix bad values (e.g. selectivity > 100)
   ▼
   │  dbt intermediate  ROI bands, debt-to-earnings ratio, cohort flags
   ▼
MARTS          star schema + summary tables
   │            fct_graduate_outcomes, dim_major, dim_institution_tier, dim_region
   │            mart_roi_by_major, mart_roi_by_tier, mart_choices_effect
   │
   ├── AI: plain-English "report card" per major      mart_major_report_cards  (Databricks ai_query)
   ├── AI: summary of key findings on every run       mart_key_findings        (Databricks ai_query)
   ▼
Public Streamlit dashboard   (charts + AI insights, anyone can open it)

Scheduled with Databricks Jobs. Built with Claude Code as AI pair-engineer.
```

## How the automation works
A Databricks Job, `college_roi_refresh` ([definition](orchestration/college_roi_refresh_job.yml)), keeps everything up to date with no manual steps:

1. **Trigger:** a new CSV lands in `/Volumes/workspace/raw/landing/`. The job waits 60 seconds after the last change, so a slow upload finishes first.
2. **`load_raw`:** runs [`ingestion/databricks_load_raw.sql`](ingestion/databricks_load_raw.sql). It loads the **newest** CSV in the folder into `workspace.raw.college_major_roi` and records which file in `_source_file`. If the load comes out empty, it stops with an error.
3. **`dbt_build`:** runs only if step 2 succeeded. Runs `dbt build` on all models and tests, including the AI report cards and key findings.
4. **Fail loudly:** any error or failed test stops the job and sends an email. Only one run at a time.

Both tasks run the code on `main` straight from GitHub, so a merged PR is live on the next run. The dashboard picks up new numbers within 6 hours (its cache time).

**To refresh the data, upload a new CSV with a new name**, e.g. `college_major_roi_2026-11.csv`. The trigger only fires for new file names, not re-uploads of the same name. The dashboard shows the new numbers within an hour, or straight away with its **Refresh data** button.

**The CSV must have the same 18 columns as `college_major_roi.csv`** (names and meaning, see [data_dictionary.csv](data_dictionary.csv)). More or fewer rows are fine. If the columns change:
1. **The load still works.** `load_raw` copies whatever columns the file has into the raw table.
2. **dbt fails.** A missing or renamed column breaks `stg_graduates`; bad values fail its tests. Everything after it is skipped.
3. **The job emails a failure.**
4. **The dashboard keeps the last good data.** The marts are only rebuilt when everything before them passes. (A failed test on a mart itself is the exception: that one table has already been rebuilt.)

Extra columns are ignored, because staging only picks the columns it knows.

## Where the AI is
| | What | Tool |
|---|---|---|
| AI as builder | Writes, runs, tests and fixes the SQL/Python | Claude Code |
| AI in the pipeline | Writes a short verdict per major ("high cost, but pays back fast if you intern") | Databricks `ai_query` |
| AI in the product | Summarises the key findings, shown on the dashboard | Databricks `ai_query` |

### How the AI text is kept honest
- Model: `databricks-meta-llama-3-3-70b-instruct`, set once in `dbt_project.yml` (`ai_model_endpoint`)
- The prompt only gets numbers from the marts and is told not to add any others. It writes in the past tense ("in this data…"), in Australian English
- Each report card's opening takeaway ("A strong financial bet." … "Financially risky.") comes from fixed SQL rules, not the AI, so it's consistent and checkable
- `mart_key_findings.input_facts` stores the exact fact sheet the AI was given, so every number in the summary can be checked
- AI models run only on Databricks (DuckDB has no `ai_query`)

Column definitions: [data_dictionary.csv](data_dictionary.csv)

## Data quality
| Issue found | Fix |
|---|---|
| 288 rows have selectivity percentile above 100 | Capped at 100 in `stg_graduates`, flagged with `is_selectivity_capped` |
| Money columns are each rounded to the nearest $100, so the dictionary formulas (baseline + added = earnings, added − cost = net ROI) are off by exactly $100 in ~7,500 rows each | Expected, not an error. Custom test allows a $100 difference |

## Stack
| Layer | Tool |
|---|---|
| Cloud warehouse | Databricks Free Edition (Unity Catalog, SQL warehouse) |
| Transformation | dbt Core (`dbt-databricks`), plus DuckDB for local practice |
| Orchestration | Databricks Jobs |
| Dashboard | Streamlit Community Cloud (public link) |
| Code | GitHub |

## Roadmap
- [x] Phase 0: Project scaffold
- [x] Phase 1: Load raw data into Databricks
- [x] Phase 2: Staging model + tests (`stg_graduates`)
- [x] Phase 3: Intermediate models
- [x] Phase 4: Marts (star schema + summary tables)
- [x] Phase 5: AI report cards + insights
- [x] Phase 6: Public Streamlit dashboard
- [x] Phase 7a: Automate with a Databricks Job (file arrival trigger → load → dbt build)
- [ ] Phase 7b: Write-up + LinkedIn post

Getting started: [SETUP_GUIDE.md](SETUP_GUIDE.md)
