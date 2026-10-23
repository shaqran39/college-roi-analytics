# Does College Pay Off? AI-Powered ROI Analytics on Databricks

✅ **Project complete** (built 30 Sep to 16 Oct 2026)

🔗 **Live dashboard: [https://college-roi-analytics-wdsk3fzehd7jx4ncmyr3sr.streamlit.app/](https://college-roi-analytics-wdsk3fzehd7jx4ncmyr3sr.streamlit.app/)** (synthetic data, first load can take ~30 seconds while the warehouse wakes up)

📘 **[Full handover & rebuild guide](docs/College_ROI_Project_Handover.pdf)** (PDF: architecture, decision log, step-by-step rebuild)

## The problem
Students take on years of cost and debt for a degree, but "does it pay off?" depends a lot on *what* and *where* you study.
This project answers: **Which majors and college types actually pay off over 10 years, and how much do internships, finishing on time and debt change the answer?**

The data is 30,000 simulated graduates (synthetic, not real people): major, institution tier, region, GPA, internship, cost, debt and 10-year earnings vs a high-school baseline.
"Net ROI" means extra 10-year earnings vs high school, minus the net cost of college.

## Architecture
```
college_major_roi_<date>.csv
   │  uploaded to a Databricks Volume  (/Volumes/workspace/raw/landing/)
   ▼
RAW            workspace.raw.college_major_roi        newest CSV, loaded by the job
   │  dbt staging       clean, rename, fix bad values (e.g. selectivity > 100)
   ▼
   │  dbt intermediate  ROI bands, debt-to-earnings ratio, cohort flags
   ▼
MARTS          star schema + summary tables
   │            fct_graduate_outcomes, dim_major, dim_institution_tier, dim_region
   │            mart_roi_by_major, mart_roi_by_tier, mart_choices_effect,
   │            mart_roi_by_band, mart_outcome_spread_by_major, mart_what_if
   │
   ├── AI: plain-English "report card" per major      mart_major_report_cards  (Databricks ai_query)
   ├── AI: summary of key findings on every run       mart_key_findings        (Databricks ai_query)
   ▼
Public Streamlit dashboard   (4 pages: Overview, Majors, Colleges & choices, Deep dive with a What-if tool,
                              plus an "Auto insights" page for any CSV, see below)

All of it runs automatically from a Databricks Job when a new CSV arrives.
Built with Claude Code as AI pair-engineer. Every step is tested with dbt (models + data tests).
```

## Key findings
Medians over 10 years, in this synthetic data. These compare groups of graduates; they don't prove a choice *caused* the outcome.

- **Overall:** the typical graduate came out **US$198.1K ahead** of skipping college; **16.5%** were worse off.
- **Major matters most.** Electrical engineering topped the list (**US$525.8K**), followed by business analytics (US$479.5K) and chemical engineering (US$466.7K). Fine arts was at the bottom (**−US$35.5K**, 88.9% worse off), then humanities (−US$11.2K, 62.4% worse off). Psychology roughly broke even (US$1.8K, 48.3% worse off).
- **College type:** ivy/elite graduates came out furthest ahead (US$304.4K), but **for-profit** colleges did worst by far (**US$68.8K**, 36.8% worse off, the highest debt-to-earnings ratio). Regional colleges (US$195.4K) were close to mid-tier (US$216.9K).
- **Internships:** graduates with one came out **US$116.5K further ahead** (US$264.5K vs US$148.0K). 12.5% were worse off, vs 19.6% without.
- **Finishing on time:** **US$55.2K further ahead** (US$216.0K vs US$160.8K). 12.9% were worse off, vs 24.1% for late finishers.
- **Debt:** more debt went with a lower return: from US$240.6K (under US$25K owed) down to US$125.6K (US$150K+), with the worse-off share rising from 5.7% to 33.6%. Graduates with no debt (US$196.7K) did slightly worse than those with a small debt.
- **GPA barely mattered:** every GPA band's median sat between US$170K and US$220K.

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
- Money sentences are pre-written in SQL ("came out US$X ahead" / "was US$X worse off"), and the AI is told never to say "most" or "the majority" for a median
- `mart_key_findings.input_facts` stores the exact fact sheet the AI was given, so every number in the summary can be checked
- AI models run only on Databricks, guarded with `target.type == 'databricks'`

## How the automation works
A Databricks Job, `college_roi_refresh` ([definition](orchestration/college_roi_refresh_job.yml)), keeps everything up to date with no manual steps:

1. **Trigger:** a new CSV lands in `/Volumes/workspace/raw/landing/`. The job waits 60 seconds after the last change, so a slow upload finishes first.
2. **`load_raw`:** runs [`ingestion/databricks_load_raw.sql`](ingestion/databricks_load_raw.sql). It loads the **newest** CSV in the folder into `workspace.raw.college_major_roi` and records which file in `_source_file`. If the load comes out empty, it stops with an error.
3. **`dbt_build`:** runs only if step 2 succeeded. Runs `dbt build` on all models and tests, including the AI report cards and key findings.
4. **Fail loudly:** any error or failed test stops the job and sends an email. Only one run at a time.

Both tasks run the code on `main` straight from GitHub, so a merged PR is live on the next run.

**To refresh the data, upload a new CSV with a new name**, e.g. `college_major_roi_2026-11.csv`. The trigger only fires for new file names, not re-uploads of the same name. The dashboard shows the new numbers within an hour, or straight away with its **Refresh data** button.

**The CSV must have the same 18 columns as `college_major_roi.csv`** (names and meaning, see [data_dictionary.csv](data_dictionary.csv)). More or fewer rows are fine. If the columns change:
1. **The load still works.** `load_raw` copies whatever columns the file has into the raw table.
2. **dbt fails.** A missing or renamed column breaks `stg_graduates`; bad values fail its tests. Everything after it is skipped.
3. **The job emails a failure.**
4. **The dashboard keeps the last good data.** The marts are only rebuilt when everything before them passes. (A failed test on a mart itself is the exception: that one table has already been rebuilt.)

Extra columns are ignored, because staging only picks the columns it knows.

## Auto insights (any CSV)
A second, separate path for **any CSV with any columns**. It never touches the college pipeline above.

1. **Drop a CSV** into `/Volumes/workspace/raw/auto_landing/` (new file name each time).
2. **The Job `auto_insights_refresh`** ([definition](orchestration/auto_insights_job.yml)) runs [`auto_insights/profile_upload.py`](auto_insights/profile_upload.py). It loads the newest CSV into `workspace.auto_insights.latest_upload`, cleans the column names (snake_case) and profiles the first 50 columns (a random 1M-row sample for bigger files).
3. **Each column gets a type**, and the analysis adapts to it:
   - **Numeric**, including numbers stored as text like `$1,200`, `1,200` or `45%` (the unit is kept: money or %). Plus skew (only for columns with 10+ different values), outliers (1.5 × IQR rule), and the share of zeros and negatives.
   - **Yes / no**: yes/no, true/false or 0/1 columns, analysed like a category.
   - **Category**: top 10 values plus an "Other" share, and a flag when one value is over 80%.
   - **Date**: common formats, including Australian dd/mm/yyyy. If the first part is ever over 12 it's dd/mm, if the second is it's mm/dd, and if it can't tell it reads dd/mm and says so. Columns Databricks guesses are dates are re-read as text first, so these rules always apply.
   - **ID / free text**: nearly every value unique, left out of the charts and the AI. Whole numbers only count as an ID if the name says so (`id`, `code`, `number`...) or they run 1, 2, 3..., so amounts like incomes aren't skipped.

   The converted columns go into the scratch table `typed_upload`. **Each analysis only runs when it applies**: correlations need 2+ numeric columns, group differences a category and a numeric column, trends a date column.
4. **Results are written to `workspace.auto_insights`**, tagged with an `upload_id` (time + file name, which includes a random part for uploads from the app):

   | Table | What's in it |
   |---|---|
   | `uploads` | One row per finished upload: file, rows, columns, sampled or not |
   | `column_profile` | Type, % missing, distinct count, min / quartiles / median / mean / max, date range |
   | `column_details` | Unit, date format (and a note if day/month was ambiguous), skew, outliers, % zeros / negatives, top value share |
   | `top_values` | Top 10 values of each category and yes/no column, plus an "Other" row for the rest |
   | `correlations` | Top 10 Pearson correlations between numeric columns |
   | `group_insights` | Biggest differences in a numeric column's average across a category's groups (groups of 30+ rows) |
   | `histograms` | 20 bins for up to 6 numeric columns, for the charts |
   | `time_series` | For up to 2 date columns: rows per day, week or month (chosen from the span, at most ~200 points), plus up to 3 numeric columns: money totals like revenue first, then the most complete ones. Each one is either summed (amounts that add up: revenue, sales, amount, cost, qty, count) or averaged (prices, rates, %, scores, ages, and anything else), never both. Empty periods are kept, and partial first / last periods are marked |
   | `time_patterns` | Rows (and means) by weekday, by month of the year (if the data spans 12+ months) and by hour (if there are real times) |
   | `ai_report` | ONE `ai_query` report per upload (about 250 words) and the exact `input_facts` it was given |

   The AI report adapts too. Its sections are What the data is, Data quality, Key insights, **Trends over time** (only when there's a date column) and Worth checking next, and the facts only include what the file has.
5. **The dashboard's "Auto insights" page is private per upload, and only shows what applies.** Tabs appear only when they have content: **Overview** (the AI report, then a Data quality table with % missing bars and units), **Columns** (histograms and top-value charts), **Relationships** (correlation and group-difference charts, each with a table view) and **Trends over time** (a line per date column with partial periods marked, a Rows / sum / mean selector, and weekday / month / hour charts). Everyone sees an **Example** (made-up café data) by default. After you upload a CSV (password needed), that browser session shows only *your* results, plus a private link (`?report=<id>`) you can share. The id contains 22 random characters, so it can't be guessed, and the link works for 24 hours. There is no list of uploads, so nobody can browse anyone else's. A wrong or expired link shows a friendly message and the Example.
6. **24-hour retention.** After each successful run, the clean-up deletes the CSV it just profiled, any other CSV in `auto_landing` older than 1 hour (an upload that never got profiled), and every upload's rows in `workspace.auto_insights` once they're more than 24 hours old. The Example (`upload_id = 'example'`) is never deleted. A clean-up problem only logs a warning; the new profile is already saved. To see what it would delete without deleting anything: `python auto_insights/profile_upload.py --cleanup-dry-run`.
7. **The Example** was created once with `python auto_insights/profile_upload.py --as-example data/raw/cafe_sales_test.csv`. Its CSV is kept in the volume `workspace.auto_insights.example_data` (not `auto_landing`, so it doesn't start the Job). Running it again does nothing if the Example already exists. After the profiler changes, `python auto_insights/profile_upload.py --refresh-example` re-profiles it: it deletes **only** the rows where `upload_id = 'example'` (after checking its CSV can still be read) and writes them again.

The AI follows the same rules as above: only the numbers it's given, no invented causes, no forecasts, correlation isn't causation, Australian spelling.
To test from a laptop without starting the Job: `python auto_insights/profile_upload.py --test-file data/raw/my_file.csv` (uses the same env vars as dbt). It copies the CSV to the volume `workspace.auto_insights.test_data` under an upload-style name, profiles it, and prints its private link. Its results are removed by the normal 24-hour clean-up.

⚠️ **Uploads are private, but anyone you give the link to can see them for 24 hours, and the data is processed in Databricks.** Still only use public or synthetic data, never anything personal or sensitive.

## Tech stack
| Layer | Tool |
|---|---|
| Cloud warehouse | Databricks Free Edition (Unity Catalog, serverless SQL warehouse, Volumes) |
| Transformation + tests | dbt Core (`dbt-databricks`) |
| AI in the pipeline | Databricks `ai_query` with Llama 3.3 70B |
| Orchestration | Databricks Jobs (file arrival trigger) |
| Dashboard | Streamlit + Plotly on Streamlit Community Cloud (public link). One theme with light and dark colours (`dashboards/.streamlit/config.toml`), one shared chart style (`dashboards/charts.py`, colour-blind-checked colours, never colour alone), and the same header on every page |
| AI pair-engineer | Claude Code |
| Code | GitHub |

## Lessons learned
- **Same-name uploads don't fire the trigger.** The file arrival trigger only reacts to *new* file names. So new data gets a new name (e.g. a date), and the load picks the newest CSV in the folder instead of a fixed file name.
- **The SQL file task splits on semicolons.** A `begin … end` block worked in the SQL Editor but broke in the job, because the task cut it apart at every `;`. The fix: one plain `create or replace table` statement, using `read_files` and `_metadata.file_modification_time` to keep only the newest file's rows.
- **AI text needs guard rails, not trust.**
  - The model called a median "most graduates", but a median describes the typical graduate, not the majority. The prompts now forbid "most" and "the majority".
  - It also wrote awkward phrases like "losses… a gain", so money sentences are now pre-written in SQL.
  - The headline label on each report card comes from fixed SQL rules, so the AI can't overstate it.
  - The fact sheet the AI saw is stored next to its answer, so every number can be checked.
- **Don't let the database guess date formats.** Databricks read `07/01/2026` as a date on its own, which happens to work when some days are over 12, but would silently pick a format when every day is 12 or less. Auto insights now reads guessed dates as text and applies its own dd/mm rule, with a note when it can't tell.
- **"Nearly every value is unique" isn't enough to call a column an ID.** An income column (whole dollars, almost all different) was skipped as an ID. It now also needs an ID-like name or values that run 1, 2, 3...
- **Keep secrets out of Git from day one.**
  - The Databricks token lives in environment variables (for dbt) and in Streamlit secrets (for the dashboard).
  - `secrets.toml` is gitignored before it's ever filled in, and only a blank `secrets.toml.example` is committed.
  - The exported job definition uses variables for the warehouse ID and the alert email.

## Limitations
- **Same 18 columns only.** The pipeline expects the exact columns of `college_major_roi.csv`. A different CSV (other columns or a different topic) needs the dbt models adapted. (Auto insights takes any CSV, but only gives a general profile, not a tailored analysis.)
- **New data needs a new file name.** Re-uploading a file with the same name won't start the job.
- **The dashboard can lag up to 1 hour** behind a job run, because it caches query results for an hour (the **Refresh data** button skips the wait).
- The data is synthetic, and the findings describe groups, not causes.

## Data quality
| Issue found | Fix |
|---|---|
| 288 rows have selectivity percentile above 100 | Capped at 100 in `stg_graduates`, flagged with `is_selectivity_capped` |
| Money columns are each rounded to the nearest $100, so the dictionary formulas (baseline + added = earnings, added − cost = net ROI) are off by exactly $100 in ~7,500 rows each | Expected, not an error. Custom test allows a $100 difference |

## Roadmap
- [x] Phase 0: Project scaffold
- [x] Phase 1: Load raw data into Databricks
- [x] Phase 2: Staging model + tests (`stg_graduates`)
- [x] Phase 3: Intermediate models
- [x] Phase 4: Marts (star schema + summary tables)
- [x] Phase 5: AI report cards + insights
- [x] Phase 6: Public Streamlit dashboard
- [x] Phase 7a: Automate with a Databricks Job (file arrival trigger → load → dbt build)
- [x] Phase 7b: Write-up + LinkedIn post

**Status: complete.** 🎉

## Repo map
| Folder | What's in it |
|---|---|
| `ingestion/` | `databricks_load_raw.sql`: newest CSV → raw table (job task 1) |
| `auto_insights/` | `profile_upload.py`: profiles any CSV + AI report (Auto insights job) |
| `dbt/` | Models (staging → intermediate → marts, including AI marts), tests, macros |
| `orchestration/` | The Databricks Job definition (YAML) |
| `dashboards/` | The Streamlit app (`app.py`, `db.py`, `charts.py`, `views/`) |
| `docs/` | Handover & rebuild guide (PDF) |
| `archive/` | Old files no longer used (see its README) |

Getting started: [SETUP_GUIDE.md](SETUP_GUIDE.md) · Column definitions: [data_dictionary.csv](data_dictionary.csv)
