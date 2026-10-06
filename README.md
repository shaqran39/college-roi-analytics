# Does College Pay Off? AI-Powered ROI Analytics on Databricks

**What it is:** a cloud data pipeline and public dashboard that analyses 30,000 graduates' costs, debt and 10-year earnings. AI is used to *build* it (Claude Code) and *inside* it (AI-written insights and major "report cards").

**Business question:** Which majors and college types actually pay off over 10 years, and how much do internships, finishing on time and debt change the answer?

🔗 **Live dashboard:** _coming soon_

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
   │            mart_roi_by_major, mart_roi_by_tier, mart_internship_effect
   │
   ├── AI: plain-English "report card" per major      (Databricks ai_query)
   ├── AI: summary of key findings on every run       (Databricks ai_query)
   ▼
Public Streamlit dashboard   (charts + AI insights, anyone can open it)

Scheduled with Databricks Jobs. Built with Claude Code as AI pair-engineer.
```

## Where the AI is
| | What | Tool |
|---|---|---|
| AI as builder | Writes, runs, tests and fixes the SQL/Python | Claude Code |
| AI in the pipeline | Writes a short verdict per major ("high cost, but pays back fast if you intern") | Databricks `ai_query` |
| AI in the product | Summarises the key findings, plus an ask-a-question box on the dashboard | Databricks `ai_query` |

## Data quality
| Issue found | Fix |
|---|---|
| 288 rows have selectivity percentile above 100 | Capped at 100 in `stg_graduates`, flagged with `is_selectivity_capped` |

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
- [ ] Phase 1: Load raw data into Databricks
- [x] Phase 2: Staging model + tests (`stg_graduates`)
- [ ] Phase 3: Intermediate models
- [ ] Phase 4: Marts (star schema + summary tables)
- [ ] Phase 5: AI report cards + insights
- [ ] Phase 6: Public Streamlit dashboard
- [ ] Phase 7: Schedule with Databricks Jobs, write-up + LinkedIn post

Getting started: [SETUP_GUIDE.md](SETUP_GUIDE.md)
