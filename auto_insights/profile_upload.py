"""Auto insights: profile the newest CSV in the auto_landing volume and write an AI report.

Works on ANY CSV (any columns). Runs as the "Python script" task of the Databricks Job
auto_insights_refresh when a new file lands in /Volumes/workspace/raw/auto_landing/,
or from a laptop for testing:

    python auto_insights/profile_upload.py               # profile the newest CSV
    python auto_insights/profile_upload.py --setup-only  # just create the schema, volume and tables
    python auto_insights/profile_upload.py --cleanup-dry-run  # show what the clean-up WOULD delete, delete nothing
    python auto_insights/profile_upload.py --as-example data/raw/cafe_sales_test.csv  # create the pinned Example (once)
    python auto_insights/profile_upload.py --test-file data/raw/test_mixed.csv  # profile a local CSV without the Job
    python auto_insights/profile_upload.py --refresh-example  # re-profile the Example with the current code

The analysis adapts to the columns: text like "$1,200", "45%", "yes"/"no" or dd/mm/yyyy is read as a
number, boolean or date first (into typed_upload), and each analysis only runs when it applies
(correlations need 2+ numeric columns, group differences a category and a numeric column, trends a date).

All work is SQL sent through run_sql(). In the Job it uses spark.sql; on a laptop it uses
databricks-sql-connector with the env vars DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN.
Values from the CSV (file names, category labels, the AI facts) always go in as query parameters.
Column names can't be parameters, so they are cleaned to snake_case and backtick-quoted.

Separate from the college ROI pipeline: it only touches workspace.raw.auto_landing
and the workspace.auto_insights schema.

Each upload is private: the dashboard shows it only to the person who uploaded it (or anyone with its
private link), for 24 hours. After each successful run it cleans up (see cleanup()): the CSV it just
profiled, any CSV left in auto_landing for over an hour, and every upload's rows in workspace.auto_insights
once they're over 24 hours old. The Example (upload_id 'example') is never deleted by it.
This is the ONLY code in the project allowed to delete: cleanup(), plus refresh_example(), which
replaces ONLY the Example's own rows when run by hand.
"""

import argparse
import os
import re
import secrets
from datetime import datetime, timedelta, timezone

LANDING_PATH = "/Volumes/workspace/raw/auto_landing/"
SCHEMA = "workspace.auto_insights"
AI_MODEL = "databricks-meta-llama-3-3-70b-instruct"

# clean-up after each successful run (see cleanup())
RESULTS_KEEP_HOURS = 24     # an upload's results (and its private link) last this long
LEFTOVER_CSV_HOURS = 1      # a CSV still in auto_landing after this long was never profiled: delete it
EXAMPLE_ID = "example"      # the pinned Example everyone sees by default; the clean-up never deletes it
EXAMPLE_VOLUME = "workspace.auto_insights.example_data"     # NOT auto_landing, so it doesn't start the Job
EXAMPLE_PATH = "/Volumes/workspace/auto_insights/example_data/"
TEST_VOLUME = "workspace.auto_insights.test_data"           # --test-file: also NOT auto_landing
TEST_PATH = "/Volumes/workspace/auto_insights/test_data/"

MAX_COLUMNS = 50            # profile at most the first 50 columns
MAX_ROWS = 1_000_000        # bigger files are profiled on a random sample of this many rows
TOP_VALUES_MAX_DISTINCT = 50
TOP_VALUES_SHOWN = 10       # top 10 values per category column, plus an "Other" row for the rest
DOMINANT_PCT = 80           # one value over 80% of a column "dominates" it
MIN_GROUP_ROWS = 30         # groups smaller than this are ignored in group_insights
ID_DISTINCT_SHARE = 0.95    # "nearly every value is unique" -> id / free text
PARSE_SHARE = 0.90          # a text column where 90%+ of values read as numbers (or dates) is numeric (or a date)
HISTOGRAM_COLUMNS = 6
HISTOGRAM_BINS = 20
SKEW_THRESHOLD = 0.5        # skewness beyond +/-0.5 = a long tail on one side
OUTLIER_IQR = 1.5           # outlier = more than 1.5 x IQR below p25 or above p75

TIME_COLUMNS = 2            # analyse at most 2 date columns (the most complete ones)
TIME_MEASURES = 3           # per period: sum and mean of the 3 most complete numeric columns
MAX_PERIODS = 200
FLAT_PCT = 5                # a change under 5% either way is "flat"

NUMERIC_TYPES = ("tinyint", "smallint", "int", "bigint", "float", "double", "decimal")
WHOLE_NUMBER_TYPES = ("tinyint", "smallint", "int", "bigint")

# text numbers: strip these before reading the value ("$1,200" -> 1200, "45%" -> 45)
NUMBER_STRIP = "[$€£,% ]"
CURRENCY_SIGNS = ("$", "€", "£")
# yes/no style columns become boolean (analysed like a category)
BOOLEAN_PAIRS = [{"yes", "no"}, {"y", "n"}, {"true", "false"}, {"t", "f"}, {"0", "1"}]
# date formats tried on text columns, as families. dmy and mdy both read "05/03/2026": when both read
# every value, dd/mm wins (Australian default) and the column gets a note.
DATE_FAMILIES = {
    "iso": [],  # try_to_timestamp / try_to_date without a format (yyyy-mm-dd, with or without a time)
    "dd/mm/yyyy": [f"d{s}M{s}yyyy{t}" for s in "/-." for t in ("", " H:mm", " H:mm:ss")],
    "mm/dd/yyyy": [f"M{s}d{s}yyyy{t}" for s in "/-." for t in ("", " H:mm", " H:mm:ss")],
    "yyyy/mm/dd": [f"yyyy/M/d{t}" for t in ("", " H:mm", " H:mm:ss")],
    "d mmm yyyy": ["d MMM yyyy", "d MMMM yyyy", "MMM d, yyyy", "MMMM d, yyyy"],
}
# time grains, smallest first: (name for date_trunc, step for sequence(), roughly how many days per step)
GRAINS = [("day", "1 day", 1), ("week", "1 week", 7), ("month", "1 month", 30.44),
          ("quarter", "3 month", 91.31), ("year", "1 year", 365.25)]
# how units and column types are named in the AI facts
UNIT_LABELS = {"currency": "money", "percent": "%"}
TYPE_NAMES = {"numeric": "numeric", "categorical": "category", "boolean": "yes/no", "date": "date", "id_text": "ID / free text"}
WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

# every append table, with explicit column types so each run's rows line up
TABLES = {
    "uploads": {
        "upload_id": "string", "file_name": "string", "file_path": "string",
        "row_count": "bigint", "column_count": "int", "profiled_column_count": "int",
        "profiled_row_count": "bigint", "is_sampled": "boolean", "loaded_at": "timestamp",
    },
    "column_profile": {
        "upload_id": "string", "column_position": "int", "column_name": "string",
        "original_name": "string", "source_type": "string", "column_type": "string",
        "non_null_count": "bigint", "null_pct": "double", "distinct_count": "bigint",
        "min_value": "double", "max_value": "double", "mean_value": "double",
        "median_value": "double", "p25_value": "double", "p75_value": "double",
        "min_date": "string", "max_date": "string",
    },
    "top_values": {
        "upload_id": "string", "column_name": "string", "value_rank": "int",
        "value": "string", "row_count": "bigint", "pct_of_rows": "double",
    },
    "correlations": {
        "upload_id": "string", "correlation_rank": "int", "column_a": "string",
        "column_b": "string", "pearson_r": "double",
    },
    "group_insights": {
        "upload_id": "string", "insight_rank": "int", "category_column": "string",
        "numeric_column": "string", "strength_eta_sq": "double", "group_count": "int",
        "overall_mean": "double", "highest_group": "string", "highest_mean": "double",
        "highest_rows": "bigint", "lowest_group": "string", "lowest_mean": "double",
        "lowest_rows": "bigint",
    },
    "histograms": {
        "upload_id": "string", "column_name": "string", "bin_index": "int",
        "bin_start": "double", "bin_end": "double", "row_count": "bigint",
    },
    "ai_report": {
        "upload_id": "string", "input_facts": "string", "report_text": "string",
        "model_endpoint": "string", "generated_at": "timestamp",
    },
    # extra per-column detail, in its own table so column_profile keeps its old shape
    "column_details": {
        "upload_id": "string", "column_name": "string", "unit": "string", "date_format": "string",
        "date_note": "string", "skewness": "double", "skew_direction": "string",
        "outlier_count": "bigint", "outlier_pct": "double", "zero_pct": "double", "negative_pct": "double",
        "top_value_pct": "double", "is_dominated": "boolean",
    },
    "time_series": {
        "upload_id": "string", "date_column": "string", "grain": "string", "period_start": "timestamp",
        "is_partial": "boolean", "row_count": "bigint", "measure_name": "string", "measure_value": "double",
    },
    "time_patterns": {
        "upload_id": "string", "date_column": "string", "pattern_type": "string", "bucket": "string",
        "bucket_order": "int", "row_count": "bigint", "share_pct": "double",
        "measure_name": "string", "measure_value": "double",
    },
}

# the sections depend on the file: "Trends over time" only when a date column was analysed (see build_prompt)
AI_PROMPT = """You are a careful data analyst. Below is an automatic profile of a CSV file someone uploaded.
Write a short report for a general audience. Rules:
- Use ONLY the numbers in the facts. Do not add, estimate or round to new numbers.
- Never invent causes or reasons. A correlation, a group difference or a change over time is not proof that one thing causes another; say "went with" or "was linked to", not "caused" or "led to".
- Never forecast or predict what happens next.
- If a group difference, correlation or change is weak or flat, say so.
- Only write about what the facts contain. If a kind of column is missing, say so at most once, in one short sentence under Data quality.
- Plain English, Australian spelling (e.g. analyse, colour, organisation). About 250 words.
- Use exactly these sections, in this order, each as a markdown heading (###): {sections}.
- Don't mention these rules.

Facts:
"""


# ---------- SQL helpers ----------

_connection = None


def run_sql(statement: str, params: dict | None = None) -> list[dict]:
    """Run one SQL statement and return its rows as dicts (empty list if it returns nothing)."""
    global _connection
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        # inside the Databricks Job: use the Spark session the task already has
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        frame = spark.sql(statement, args=params) if params else spark.sql(statement)
        return [row.asDict() for row in frame.collect()]

    # on a laptop: databricks-sql-connector with the env vars
    if _connection is None:
        from databricks import sql

        missing = [v for v in ("DATABRICKS_HOST", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN") if not os.environ.get(v)]
        if missing:
            raise RuntimeError("Missing environment variables: " + ", ".join(missing))
        _connection = sql.connect(
            server_hostname=os.environ["DATABRICKS_HOST"].removeprefix("https://").rstrip("/"),
            http_path=os.environ["DATABRICKS_HTTP_PATH"],
            access_token=os.environ["DATABRICKS_TOKEN"],
        )
    with _connection.cursor() as cursor:
        cursor.execute(statement, parameters=params)
        if cursor.description is None:
            return []
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def quote_ident(name: str) -> str:
    """`column name` with any backtick doubled, so a column name can't break out of the SQL."""
    return "`" + name.replace("`", "``") + "`"


def sql_literal(text: str) -> str:
    """'text' as a SQL string literal, for the few places a parameter isn't allowed (read_files path)."""
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def insert_rows(table: str, rows: list[dict], chunk_size: int = 200) -> None:
    """Append rows to one of TABLES, every value passed as a typed query parameter."""
    columns = TABLES[table]
    for start in range(0, len(rows), chunk_size):
        chunk = rows[start:start + chunk_size]
        params, value_lists = {}, []
        for i, row in enumerate(chunk):
            placeholders = []
            for j, (column, column_type) in enumerate(columns.items()):
                key = f"p{i}_{j}"
                params[key] = row.get(column)
                placeholders.append(f"cast(:{key} as {column_type})")
            value_lists.append("(" + ", ".join(placeholders) + ")")
        # columns named explicitly, so the insert still lines up if a table ever gains a column
        run_sql(f"insert into {SCHEMA}.{table} ({', '.join(columns)}) values " + ",\n".join(value_lists), params)


def to_float(value) -> float | None:
    return None if value is None else float(value)


# ---------- setup ----------

def setup() -> None:
    """Create the volume, schema and tables if they don't exist yet. Never drops anything."""
    run_sql("create volume if not exists workspace.raw.auto_landing")
    run_sql(f"create schema if not exists {SCHEMA}")
    for table, columns in TABLES.items():
        column_sql = ", ".join(f"{name} {column_type}" for name, column_type in columns.items())
        run_sql(f"create table if not exists {SCHEMA}.{table} ({column_sql})")


# ---------- load ----------

def as_utc(modified) -> datetime:
    """LIST gives milliseconds since 1970, read_files gives a timestamp; both become a UTC datetime."""
    if isinstance(modified, datetime):
        return modified if modified.tzinfo else modified.replace(tzinfo=timezone.utc)
    return datetime.fromtimestamp(int(modified) / 1000, tz=timezone.utc)


def list_csvs() -> list[tuple[str, str, datetime]]:
    """(file_path, file_name, modified_at) of every *.csv in the landing volume, newest first."""
    try:
        listing = [
            (row["path"], row["name"], row["modification_time"])
            for row in run_sql(f"list {sql_literal(LANDING_PATH)}")
        ]
    except Exception:
        # fallback if LIST isn't allowed: read the file metadata with read_files
        listing = [
            (row["path"], row["path"].rsplit("/", 1)[-1], row["modificationTime"])
            for row in run_sql(
                f"select path, modificationTime from read_files({sql_literal(LANDING_PATH)}, "
                "format => 'binaryFile', pathGlobFilter => '*.csv')"
            )
        ]
    csvs = [
        (path.removeprefix("dbfs:"), name, as_utc(modified))
        for path, name, modified in listing
        if name.lower().endswith(".csv")
    ]
    return sorted(csvs, key=lambda item: item[2], reverse=True)


def newest_csv() -> tuple[str, str]:
    """(file_path, file_name) of the most recently modified *.csv in the landing volume."""
    csvs = list_csvs()
    if not csvs:
        raise RuntimeError(f"No CSV files found in {LANDING_PATH}. Upload one first.")
    path, name, _ = csvs[0]
    return path, name


def clean_names(names: list[str]) -> list[str]:
    """'Unit Price ($)' -> 'unit_price', '2024 sales' -> 'col_2024_sales'; duplicates get _2, _3."""
    cleaned, seen = [], {}
    for name in names:
        clean = re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_") or "column"
        if clean[0].isdigit():
            clean = "col_" + clean
        if clean in seen:
            seen[clean] += 1
            clean = f"{clean}_{seen[clean]}"
        else:
            seen[clean] = 1
        cleaned.append(clean)
    return cleaned


def load_latest(file_path: str) -> list[dict]:
    """Load one CSV (types inferred) into latest_upload with clean column names. Returns its columns."""
    source = f"read_files({sql_literal(file_path)}, format => 'csv', header => true)"
    try:
        described = run_sql(f"describe query select * from {source}")
    except Exception as error:
        raise RuntimeError(f"Could not read {file_path} as a CSV: {error}") from error
    raw_columns = [row for row in described if row["col_name"] != "_rescued_data"]
    if not raw_columns:
        raise RuntimeError(f"{file_path} has no columns. Is it empty?")

    # columns Databricks guessed are dates are read as text instead, so detect_types reads them with our
    # rules (dd/mm unless the data says mm/dd, and a note when it can't tell) rather than a silent guess
    guessed_dates = [row["col_name"] for row in raw_columns if row["data_type"].lower().startswith(("date", "timestamp"))]
    if guessed_dates:
        hints = ", ".join(f"{quote_ident(name)} STRING" for name in guessed_dates)
        source = f"read_files({sql_literal(file_path)}, format => 'csv', header => true, schemaHints => {sql_literal(hints)})"
        raw_columns = [row for row in run_sql(f"describe query select * from {source}") if row["col_name"] != "_rescued_data"]

    names = clean_names([row["col_name"] for row in raw_columns])
    select_list = ", ".join(f"{quote_ident(row['col_name'])} as {quote_ident(name)}" for row, name in zip(raw_columns, names))
    run_sql(f"create or replace table {SCHEMA}.latest_upload as select {select_list} from {source}")
    return [
        {"original_name": row["col_name"], "column_name": name, "source_type": row["data_type"].lower()}
        for row, name in zip(raw_columns, names)
    ]


# ---------- detect types ----------

def is_numeric(source_type: str) -> bool:
    return source_type.startswith(NUMERIC_TYPES)


def name_unit(original_name: str) -> str | None:
    """A unit from the column name: 'Unit Price ($)' -> 'currency', 'Discount %' -> 'percent'."""
    lower = original_name.lower()
    if any(sign in lower for sign in CURRENCY_SIGNS) or re.search(r"(^|[^a-z])(usd|aud|eur|gbp)([^a-z]|$)", lower):
        return "currency"
    if "%" in lower or re.search(r"(^|[^a-z])(pct|percent|percentage)([^a-z]|$)", lower):
        return "percent"
    return None


def date_family_expr(col: str, family: str) -> str:
    """A timestamp expression that reads col with any format of one DATE_FAMILIES family (null if none fits)."""
    if family == "iso":
        return f"coalesce(try_to_timestamp({col}), cast(try_to_date({col}) as timestamp))"
    return "coalesce(" + ", ".join(f"try_to_timestamp({col}, '{fmt}')" for fmt in DATE_FAMILIES[family]) + ")"


def detect_types(table: str, columns: list[dict]) -> None:
    """Decide how each column should be read, in ONE aggregate query. Sets on each dict:

    stat_type   the type it's analysed as: 'double', 'timestamp', 'boolean' or the type read from the CSV
    typed_sql   the expression that converts it ("$1,200" -> 1200.0, "25/12/2026" -> a timestamp, "Yes" -> 'yes')
    unit, date_format, date_note   for the page and the AI facts
    """
    expressions = []
    for i, column in enumerate(columns):
        col, kind = quote_ident(column["column_name"]), column["source_type"]
        if kind == "string":
            text = f"nullif(lower(trim({col})), '')"
            expressions += [
                f"count({text}) as nn_{i}",
                f"case when count(distinct {text}) <= 2 then array_sort(collect_set({text})) end as set_{i}",
                f"count(try_cast(regexp_replace({col}, '{NUMBER_STRIP}', '') as double)) as num_{i}",
                f"count_if({col} rlike '[$€£]') as cur_{i}",
                f"count_if(contains({col}, '%')) as pct_{i}",
            ]
            expressions += [f"count({date_family_expr(col, family)}) as d{f}_{i}" for f, family in enumerate(DATE_FAMILIES)]
        elif kind.startswith(WHOLE_NUMBER_TYPES):
            expressions.append(
                f"case when count(distinct {col}) <= 2 then array_sort(collect_set(cast({col} as string))) end as set_{i}"
            )
    found = run_sql(f"select {', '.join(expressions)} from {table}")[0] if expressions else {}

    for i, column in enumerate(columns):
        col, kind = quote_ident(column["column_name"]), column["source_type"]
        column.update(stat_type=kind, typed_sql=col, unit=None, date_format=None, date_note=None)
        found_set = found.get(f"set_{i}")  # a list from Spark, a numpy array from the connector
        values = set() if found_set is None else set(found_set)
        if kind == "boolean" or (len(values) == 2 and any(values <= pair for pair in BOOLEAN_PAIRS)):
            column.update(stat_type="boolean", typed_sql=f"nullif(lower(trim(cast({col} as string))), '')")
        elif kind == "string" and found[f"nn_{i}"]:
            non_empty = found[f"nn_{i}"]
            date_counts = {family: found[f"d{f}_{i}"] for f, family in enumerate(DATE_FAMILIES)}
            best_family = max(date_counts, key=date_counts.get)  # ties go to the first family listed (dd/mm before mm/dd)
            if found[f"num_{i}"] / non_empty >= PARSE_SHARE:
                unit = ("currency" if found[f"cur_{i}"] > non_empty / 2 else
                        "percent" if found[f"pct_{i}"] > non_empty / 2 else name_unit(column["original_name"]))
                column.update(
                    stat_type="double", unit=unit,
                    typed_sql=f"try_cast(regexp_replace({col}, '{NUMBER_STRIP}', '') as double)",
                )
            elif date_counts[best_family] / non_empty >= PARSE_SHARE:
                column.update(
                    stat_type="timestamp", typed_sql=date_family_expr(col, best_family),
                    date_format="yyyy-mm-dd" if best_family == "iso" else best_family,
                )
                if best_family == "dd/mm/yyyy" and date_counts["mm/dd/yyyy"] == date_counts["dd/mm/yyyy"]:
                    column["date_note"] = "ambiguous day/month: read as dd/mm"
        elif is_numeric(kind):
            column["unit"] = name_unit(column["original_name"])
        elif kind.startswith(("date", "timestamp")):
            column.update(stat_type="timestamp", typed_sql=f"cast({col} as timestamp)")


def build_typed(columns: list[dict]) -> None:
    """typed_upload = latest_upload with every column converted as detect_types decided (scratch table, no upload_id)."""
    select_list = ", ".join(f"{c['typed_sql']} as {quote_ident(c['column_name'])}" for c in columns)
    run_sql(f"create or replace table {SCHEMA}.typed_upload as select {select_list} from {SCHEMA}.latest_upload")


# ---------- profile ----------

def profile_columns(table: str, columns: list[dict], row_count: int) -> None:
    """One aggregate query for all profiled columns (plus one for outliers); fills in stats and column_type."""
    expressions = []
    for i, column in enumerate(columns):
        col, kind = quote_ident(column["column_name"]), column["stat_type"]
        expressions += [f"count({col}) as nn_{i}", f"count(distinct {col}) as nd_{i}"]
        if is_numeric(kind):
            value = f"cast({col} as double)"
            expressions += [
                f"min({value}) as min_{i}", f"max({value}) as max_{i}", f"avg({value}) as mean_{i}",
                f"percentile_approx({value}, 0.25) as p25_{i}", f"percentile_approx({value}, 0.5) as med_{i}",
                f"percentile_approx({value}, 0.75) as p75_{i}", f"skewness({value}) as sk_{i}",
                f"count_if({value} = 0) as zero_{i}", f"count_if({value} < 0) as neg_{i}",
            ]
        elif kind == "timestamp":
            expressions += [f"cast(min({col}) as string) as dmin_{i}", f"cast(max({col}) as string) as dmax_{i}"]
    stats = run_sql(f"select {', '.join(expressions)} from {table}")[0]

    for i, column in enumerate(columns):
        kind = column["stat_type"]
        non_null, distinct = int(stats[f"nn_{i}"]), int(stats[f"nd_{i}"])
        column.update(
            non_null_count=non_null,
            null_pct=round(100 * (row_count - non_null) / row_count, 2),
            distinct_count=distinct,
        )
        nearly_unique = non_null > 0 and distinct > TOP_VALUES_MAX_DISTINCT and distinct / non_null >= ID_DISTINCT_SHARE

        if kind == "boolean":
            column["column_type"] = "boolean"
        elif is_numeric(kind):
            skewness = to_float(stats[f"sk_{i}"])
            column.update(
                min_value=to_float(stats[f"min_{i}"]), max_value=to_float(stats[f"max_{i}"]),
                mean_value=to_float(stats[f"mean_{i}"]), median_value=to_float(stats[f"med_{i}"]),
                p25_value=to_float(stats[f"p25_{i}"]), p75_value=to_float(stats[f"p75_{i}"]),
                skewness=None if skewness is None else round(skewness, 4),
                skew_direction=(None if skewness is None else "right (long tail of high values)" if skewness > SKEW_THRESHOLD
                                else "left (long tail of low values)" if skewness < -SKEW_THRESHOLD else "roughly symmetric"),
                zero_pct=round(100 * stats[f"zero_{i}"] / non_null, 2) if non_null else None,
                negative_pct=round(100 * stats[f"neg_{i}"] / non_null, 2) if non_null else None,
            )
            # whole numbers that are nearly all different are an ID only if the name says so or they run
            # 1, 2, 3...; otherwise it's a real amount (an income or a price is nearly unique too)
            looks_like_id = nearly_unique and kind.startswith(WHOLE_NUMBER_TYPES) and (
                re.search(r"(^|_)(id|key|code|no|num|number|ref)($|_)", column["column_name"]) is not None
                or column["max_value"] - column["min_value"] + 1 <= 2 * distinct
            )
            column["column_type"] = "id_text" if looks_like_id else "numeric"
        elif kind == "timestamp":
            # "2026-05-08 00:00:00" -> "2026-05-08" when there's no time of day
            column.update(
                min_date=(stats[f"dmin_{i}"] or "").removesuffix(" 00:00:00") or None,
                max_date=(stats[f"dmax_{i}"] or "").removesuffix(" 00:00:00") or None,
                column_type="date",
            )
        elif nearly_unique:
            column["column_type"] = "id_text"
        else:
            column["column_type"] = "categorical"

    # outliers need p25 and p75 first: values more than 1.5 x IQR outside them
    numerics = [(i, c) for i, c in enumerate(columns) if c["column_type"] == "numeric" and c["p25_value"] is not None]
    if numerics:
        expressions = []
        for i, c in numerics:
            iqr = c["p75_value"] - c["p25_value"]
            low, high = c["p25_value"] - OUTLIER_IQR * iqr, c["p75_value"] + OUTLIER_IQR * iqr
            value = f"cast({quote_ident(c['column_name'])} as double)"
            expressions.append(f"count_if({value} < {low!r} or {value} > {high!r}) as out_{i}")
        outliers = run_sql(f"select {', '.join(expressions)} from {table}")[0]
        for i, c in numerics:
            c["outlier_count"] = int(outliers[f"out_{i}"])
            c["outlier_pct"] = round(100 * c["outlier_count"] / c["non_null_count"], 2)


def top_values(table: str, categoricals: list[dict], row_count: int) -> list[dict]:
    """Top 10 values (with count and % of rows) of each category / boolean column, plus an "Other" row
    for the rest when there are more than 10. Also sets top_value_pct and is_dominated on each column."""
    chosen = [c for c in categoricals if c["non_null_count"]]
    if not chosen:
        return []
    parts = [
        f"select {i} as col_index, cast({quote_ident(c['column_name'])} as string) as value, count(*) as n "
        f"from {table} where {quote_ident(c['column_name'])} is not null group by 2"
        for i, c in enumerate(chosen)
    ]
    rows = run_sql(
        "select col_index, value, n, row_number() over (partition by col_index order by n desc, value) as value_rank "
        f"from ({' union all '.join(parts)}) qualify value_rank <= {TOP_VALUES_SHOWN}"
    )
    result = [
        {
            "column_name": chosen[row["col_index"]]["column_name"], "value_rank": int(row["value_rank"]),
            "value": row["value"], "row_count": int(row["n"]), "pct_of_rows": round(100 * row["n"] / row_count, 2),
        }
        for row in rows
    ]
    for c in chosen:
        shown = [r for r in result if r["column_name"] == c["column_name"]]
        top = max(shown, key=lambda r: r["row_count"])
        c["top_value_pct"] = round(100 * top["row_count"] / c["non_null_count"], 2)
        c["is_dominated"] = c["top_value_pct"] > DOMINANT_PCT
        other = c["non_null_count"] - sum(r["row_count"] for r in shown)
        if c["distinct_count"] > TOP_VALUES_SHOWN and other > 0:
            result.append({
                "column_name": c["column_name"], "value_rank": TOP_VALUES_SHOWN + 1,
                "value": f"Other ({c['distinct_count'] - TOP_VALUES_SHOWN:,} more values)",
                "row_count": other, "pct_of_rows": round(100 * other / row_count, 2),
            })
    return result


def correlations(table: str, numerics: list[dict]) -> list[dict]:
    """Pearson r for every pair of numeric columns, top 10 by absolute value."""
    pairs = [(a, b) for i, a in enumerate(numerics) for b in numerics[i + 1:]]
    if not pairs:
        return []
    expressions = [
        f"corr(cast({quote_ident(a['column_name'])} as double), cast({quote_ident(b['column_name'])} as double)) as r_{i}"
        for i, (a, b) in enumerate(pairs)
    ]
    result = run_sql(f"select {', '.join(expressions)} from {table}")[0]
    scored = [
        (a["column_name"], b["column_name"], float(result[f"r_{i}"]))
        for i, (a, b) in enumerate(pairs)
        if result[f"r_{i}"] is not None
    ]
    scored.sort(key=lambda item: abs(item[2]), reverse=True)
    return [
        {"correlation_rank": rank, "column_a": a, "column_b": b, "pearson_r": round(r, 4)}
        for rank, (a, b, r) in enumerate(scored[:10], start=1)
    ]


def group_insights(table: str, categoricals: list[dict], numerics: list[dict]) -> list[dict]:
    """For the strongest category x numeric pairs: highest and lowest group means (groups of 30+ rows).

    Strength = eta squared: the share of the numeric column's spread explained by the groups (0 to 1).
    """
    candidates = []
    for category in [c for c in categoricals if 2 <= c["distinct_count"] <= TOP_VALUES_MAX_DISTINCT]:
        expressions = []
        for j, numeric in enumerate(numerics):
            value = f"cast({quote_ident(numeric['column_name'])} as double)"
            expressions += [f"count({value}) as k_{j}", f"avg({value}) as m_{j}", f"var_pop({value}) as v_{j}"]
        cat = quote_ident(category["column_name"])
        groups = run_sql(
            f"select cast({cat} as string) as grp, {', '.join(expressions)} from {table} where {cat} is not null group by 1"
        )
        for j, numeric in enumerate(numerics):
            kept = [
                (g["grp"], int(g[f"k_{j}"]), float(g[f"m_{j}"]), float(g[f"v_{j}"]))
                for g in groups
                if g[f"k_{j}"] >= MIN_GROUP_ROWS
            ]
            if len(kept) < 2:
                continue
            total = sum(k for _, k, _, _ in kept)
            grand_mean = sum(k * m for _, k, m, _ in kept) / total
            between = sum(k * (m - grand_mean) ** 2 for _, k, m, _ in kept)
            within = sum(k * v for _, k, _, v in kept)
            if between + within == 0:
                continue
            highest, lowest = max(kept, key=lambda g: g[2]), min(kept, key=lambda g: g[2])
            candidates.append({
                "category_column": category["column_name"], "numeric_column": numeric["column_name"],
                "strength_eta_sq": round(between / (between + within), 4), "group_count": len(kept),
                "overall_mean": round(grand_mean, 4),
                "highest_group": highest[0], "highest_mean": round(highest[2], 4), "highest_rows": highest[1],
                "lowest_group": lowest[0], "lowest_mean": round(lowest[2], 4), "lowest_rows": lowest[1],
            })
    candidates.sort(key=lambda c: c["strength_eta_sq"], reverse=True)
    return [{"insight_rank": rank, **c} for rank, c in enumerate(candidates[:5], start=1)]


def histograms(table: str, numerics: list[dict]) -> list[dict]:
    """20 equal-width bins for the numeric columns with the fewest missing values (top 6)."""
    chosen = [
        n for n in sorted(numerics, key=lambda n: -n["non_null_count"])
        if n["min_value"] is not None and n["max_value"] > n["min_value"]
    ][:HISTOGRAM_COLUMNS]
    if not chosen:
        return []
    parts = []
    for i, n in enumerate(chosen):
        value = f"cast({quote_ident(n['column_name'])} as double)"
        bucket = f"least(width_bucket({value}, {n['min_value']!r}, {n['max_value']!r}, {HISTOGRAM_BINS}), {HISTOGRAM_BINS})"
        parts.append(f"select {i} as col_index, {bucket} as bin_index, count(*) as n from {table} where {value} is not null group by 2")
    rows = run_sql(" union all ".join(parts))
    result = []
    for row in rows:
        n = chosen[row["col_index"]]
        width = (n["max_value"] - n["min_value"]) / HISTOGRAM_BINS
        b = int(row["bin_index"])
        result.append({
            "column_name": n["column_name"], "bin_index": b, "row_count": int(row["n"]),
            "bin_start": n["min_value"] + (b - 1) * width, "bin_end": n["min_value"] + b * width,
        })
    return result


# ---------- time analysis ----------

def choose_grain(span_days: float) -> tuple[str, str]:
    """(grain, sequence step): day up to ~90 days, week up to ~2 years, else month; coarser if still over 200 points."""
    start = 0 if span_days <= 90 else 1 if span_days <= 730 else 2
    for grain, step, days_per_step in GRAINS[start:]:
        if span_days / days_per_step + 1 <= MAX_PERIODS:
            return grain, step
    return GRAINS[-1][0], GRAINS[-1][1]


def period_label(start: datetime, grain: str) -> str:
    """How a period is named in the facts: 'week of 2026-03-02', 'Mar 2026', 'Q1 2026'."""
    if grain == "day":
        return f"{start:%Y-%m-%d}"
    if grain == "week":
        return f"week of {start:%Y-%m-%d}"
    if grain == "month":
        return f"{start:%b %Y}"
    if grain == "quarter":
        return f"Q{(start.month - 1) // 3 + 1} {start:%Y}"
    return f"{start:%Y}"


def change(first: float | None, last: float | None) -> dict:
    """% change from first to last; 'flat' if under FLAT_PCT either way. No % if first is 0 or missing."""
    if first is None or last is None or first == 0:
        return {"first": first, "last": last, "pct": None, "direction": None}
    pct = 100 * (last - first) / abs(first)
    return {"first": first, "last": last, "pct": round(pct, 1),
            "direction": "flat" if abs(pct) < FLAT_PCT else "up" if pct > 0 else "down"}


def busiest_and_quietest(buckets: list[tuple[str, int]]) -> dict | None:
    """{'busiest': (label, rows), 'quietest': (label, rows)} over buckets that aren't all equal."""
    if len(buckets) < 2:
        return None
    return {"busiest": max(buckets, key=lambda b: b[1]), "quietest": min(buckets, key=lambda b: b[1])}


def time_analysis(table: str, date_column: dict, measures: list[dict]) -> tuple[list[dict], list[dict], dict]:
    """Rows per period (plus sum and mean of up to 3 measures) and day / month / hour patterns for one date column.

    Returns (time_series rows, time_patterns rows, facts for the AI). Empty periods are kept (0 rows) and
    the first / last period is marked partial when the data doesn't cover all of it.
    """
    name = date_column["column_name"]
    col = quote_ident(name)
    values = [quote_ident(m["column_name"]) for m in measures]
    bounds = run_sql(
        f"select min({col}) as lo, max({col}) as hi, count({col}) as n, "
        f"count_if(hour({col}) <> 0 or minute({col}) <> 0 or second({col}) <> 0) as timed from {table}"
    )[0]
    lo, hi = bounds["lo"], bounds["hi"]
    span_days = (hi - lo).total_seconds() / 86400
    grain, step = choose_grain(span_days)

    measure_sql = "".join(f", sum(cast({v} as double)) as s_{j}, avg(cast({v} as double)) as a_{j}" for j, v in enumerate(values))
    measure_out = "".join(f", d.s_{j}, d.a_{j}" for j in range(len(values)))
    periods = run_sql(
        f"with bounds as (select min({col}) as lo, max({col}) as hi from {table}), "
        f"periods as (select explode(sequence(date_trunc('{grain}', lo), date_trunc('{grain}', hi), interval {step})) "
        f"as period_start from bounds), "
        f"data as (select date_trunc('{grain}', {col}) as period_start, count(*) as n{measure_sql} "
        f"from {table} where {col} is not null group by 1) "
        f"select p.period_start, p.period_start + interval {step} as period_end, coalesce(d.n, 0) as n{measure_out} "
        f"from periods p left join data d on p.period_start = d.period_start order by 1"
    )
    for i, p in enumerate(periods):
        # a day is never partial; a week / month / quarter / year is if the data starts after it begins
        # or ends before its last day
        first_cut = i == 0 and lo.date() > p["period_start"].date()
        last_cut = i == len(periods) - 1 and hi.date() < (p["period_end"] - timedelta(days=1)).date()
        p["is_partial"] = grain != "day" and (first_cut or last_cut)

    series = []
    for p in periods:
        base = {"date_column": name, "grain": grain, "period_start": f"{p['period_start']:%Y-%m-%d %H:%M:%S}",
                "is_partial": p["is_partial"], "row_count": int(p["n"])}
        if not measures:
            series.append({**base, "measure_name": None, "measure_value": None})
        for j, m in enumerate(measures):
            total = to_float(p[f"s_{j}"])
            series.append({**base, "measure_name": f"sum of {m['column_name']}", "measure_value": 0.0 if total is None else total})
            series.append({**base, "measure_name": f"mean of {m['column_name']}", "measure_value": to_float(p[f"a_{j}"])})

    # patterns: weekday always, month of year if the span covers 12+ months, hour of day if real times exist
    months_spanned = (hi.year - lo.year) * 12 + hi.month - lo.month
    patterns_wanted = {"day_of_week": (f"((dayofweek({col}) + 5) % 7) + 1", WEEKDAYS)}
    if months_spanned >= 12:
        patterns_wanted["month_of_year"] = (f"month({col})", MONTHS)
    if bounds["timed"]:
        patterns_wanted["hour_of_day"] = (f"hour({col}) + 1", [f"{h:02d}:00" for h in range(24)])
    mean_sql = "".join(f", avg(cast({v} as double)) as a_{j}" for j, v in enumerate(values))
    found = run_sql(" union all ".join(
        f"select '{pattern}' as pattern_type, {bucket_sql} as bucket_order, count(*) as n{mean_sql} "
        f"from {table} where {col} is not null group by 2"
        for pattern, (bucket_sql, _) in patterns_wanted.items()
    ))
    total_rows = int(bounds["n"])
    patterns, pattern_facts = [], {}
    for pattern, (_, labels) in patterns_wanted.items():
        by_order = {int(r["bucket_order"]): r for r in found if r["pattern_type"] == pattern}
        buckets = []
        for order, label in enumerate(labels, start=1):
            r = by_order.get(order, {})
            n = int(r.get("n", 0))
            buckets.append((label, n))
            base = {"date_column": name, "pattern_type": pattern, "bucket": label, "bucket_order": order,
                    "row_count": n, "share_pct": round(100 * n / total_rows, 2)}
            if not measures:
                patterns.append({**base, "measure_name": None, "measure_value": None})
            for j, m in enumerate(measures):
                patterns.append({**base, "measure_name": f"mean of {m['column_name']}", "measure_value": to_float(r.get(f"a_{j}"))})
        # hours with no rows at all are left out of busiest / quietest (a shop closed at 3am isn't "quiet")
        pattern_facts[pattern] = busiest_and_quietest([b for b in buckets if b[1] > 0] if pattern == "hour_of_day" else buckets)

    full = [p for p in periods if not p["is_partial"]]
    facts = {
        "column": name, "grain": grain, "start": lo, "end": hi, "span_days": round(span_days),
        "date_note": date_column.get("date_note"), "period_count": len(periods), "months_covered": months_spanned + 1,
        "partial_periods": sum(p["is_partial"] for p in periods),
        "empty_periods": sum(p["n"] == 0 for p in periods), "full_periods": len(full),
        "busiest_period": busiest_and_quietest([(period_label(p["period_start"], grain), int(p["n"])) for p in full]),
        "patterns": pattern_facts, "changes": {},
    }
    if len(full) >= 2:
        first, last = full[0], full[-1]
        facts["change_from"], facts["change_to"] = period_label(first["period_start"], grain), period_label(last["period_start"], grain)
        facts["changes"]["rows"] = change(first["n"], last["n"])
        for j, m in enumerate(measures):
            facts["changes"][f"sum of {m['column_name']}"] = change(to_float(first[f"s_{j}"]) or 0.0, to_float(last[f"s_{j}"]) or 0.0)
    return series, patterns, facts


# ---------- AI report ----------

def fmt(value: float | None) -> str:
    """Short, readable number for the fact sheet: 1234.5678 -> '1,234.57'."""
    if value is None:
        return "n/a"
    return f"{value:,.0f}" if abs(value) >= 1000 or float(value).is_integer() else f"{value:,.2f}"


def display_name(file_name: str) -> str:
    """'upload_20261020T051324Z_<random part>_my_sales.csv' -> 'my_sales.csv' (other names are unchanged).

    The random part is what keeps a private link unguessable, so it never goes into the AI report.
    """
    return re.sub(r"^upload_[0-9]{8}T[0-9]{6}Z_[A-Za-z0-9_-]{22}_", "", file_name)


def describe_change(label: str, c: dict) -> str:
    """'rows 67 -> 59 (-11.9%, down)'; without a % when the first value was 0."""
    if c["pct"] is None:
        return f"{label} {fmt(c['first'])} -> {fmt(c['last'])}"
    return f"{label} {fmt(c['first'])} -> {fmt(c['last'])} ({c['pct']:+.1f}%, {c['direction']})"


def time_lines(t: dict) -> list[str]:
    """The facts for one analysed date column."""
    grain = t["grain"]
    lines = [
        f"- {t['column']}: {t['start']:%Y-%m-%d} to {t['end']:%Y-%m-%d} ({t['span_days']:,} days), counted per {grain}: "
        f"{t['period_count']} {grain}s"
        + (f", {t['partial_periods']} of them partial (only partly covered, left out of the comparisons)" if t["partial_periods"] else "")
        + (f", {t['empty_periods']} with no rows at all" if t["empty_periods"] else ", none empty") + "."
    ]
    if t["date_note"]:
        lines.append(f"  Note: {t['date_note']}.")
    if t["changes"]:
        lines.append(
            f"  First to last full {grain} ({t['change_from']} to {t['change_to']}): "
            + "; ".join(describe_change(label, c) for label, c in t["changes"].items())
            + f". Changes under {FLAT_PCT}% either way count as flat."
        )
    else:
        lines.append(f"  Fewer than 2 full {grain}s, so no change over time is given.")
    if t["busiest_period"]:
        (b_label, b_rows), (q_label, q_rows) = t["busiest_period"]["busiest"], t["busiest_period"]["quietest"]
        lines.append(f"  Busiest full {grain}: {b_label} ({b_rows:,} rows). Quietest: {q_label} ({q_rows:,} rows).")
    names = {"day_of_week": "weekday", "month_of_year": "month of the year", "hour_of_day": "hour of the day"}
    for pattern, found in t["patterns"].items():
        if found:
            (b_label, b_rows), (q_label, q_rows) = found["busiest"], found["quietest"]
            lines.append(f"  Busiest {names[pattern]}: {b_label} ({b_rows:,} rows). Quietest: {q_label} ({q_rows:,} rows)."
                         + (f" The data covers {t['months_covered']} calendar months, so some months appear in it more "
                            "often than others." if pattern == "month_of_year" and t["months_covered"] % 12 else ""))
    return lines


def build_facts(upload: dict, columns: list[dict], top: list[dict], corr: list[dict], groups: list[dict],
                time_facts: list[dict]) -> str:
    """Compact text of facts from the profile, in labelled sections. A section is only written when it has
    something in it. id / free-text columns are only named, never described."""
    by_type = {}
    for c in columns:
        by_type.setdefault(c["column_type"], []).append(c)

    def named(c: dict) -> str:
        return c["column_name"] + (f" ({UNIT_LABELS[c['unit']]})" if c.get("unit") else "")

    lines = [
        "## Overview",
        f"File: {display_name(upload['file_name'])}",
        f"Rows: {upload['row_count']:,}. Columns: {upload['column_count']}"
        + (f" (first {upload['profiled_column_count']} profiled)" if upload["profiled_column_count"] < upload["column_count"] else "")
        + ".",
    ]
    if upload["is_sampled"]:
        lines.append(f"Stats are from a random sample of {upload['profiled_row_count']:,} rows.")
    lines.append("Column types: " + "; ".join(
        f"{TYPE_NAMES[kind]}: {', '.join(named(c) for c in cols)}" for kind, cols in by_type.items()
    ))
    if by_type.get("id_text"):
        lines.append("ID / free-text columns (nearly every value unique) are left out of the analysis.")

    lines.append("\n## Data quality")
    missing = [c for c in columns if c["null_pct"] > 0]
    lines.append("Missing values: " + (", ".join(f"{c['column_name']} {c['null_pct']}%" for c in missing) if missing else "none"))
    absent = [TYPE_NAMES[kind] for kind in ("numeric", "categorical", "date") if not by_type.get(kind)]
    if absent:
        lines.append("Not in this file: " + ", ".join(absent) + " columns.")
    lines += [f"Date note: {c['column_name']}: {c['date_note']}." for c in columns if c.get("date_note")]
    for c in columns:
        if c.get("is_dominated"):
            first = next(t for t in top if t["column_name"] == c["column_name"] and t["value_rank"] == 1)
            lines.append(f"One value dominates {c['column_name']}: '{first['value']}' is {c['top_value_pct']}% of its values.")

    if by_type.get("numeric"):
        lines.append("\n## Numeric columns (min / p25 / median / mean / p75 / max)")
        for c in by_type["numeric"]:
            extras = [f"skew: {c['skew_direction']}"] if c.get("skew_direction") else []
            if c.get("outlier_count"):
                extras.append(f"{c['outlier_count']:,} outliers ({c['outlier_pct']}%, outside 1.5 x IQR)")
            if c.get("zero_pct"):
                extras.append(f"{c['zero_pct']}% zeros")
            if c.get("negative_pct"):
                extras.append(f"{c['negative_pct']}% negative")
            lines.append(
                f"- {named(c)}: {fmt(c['min_value'])} / {fmt(c['p25_value'])} / {fmt(c['median_value'])} / "
                f"{fmt(c['mean_value'])} / {fmt(c['p75_value'])} / {fmt(c['max_value'])}"
                + (f"; {'; '.join(extras)}" if extras else "")
            )

    category_columns = by_type.get("categorical", []) + by_type.get("boolean", [])
    if category_columns:
        lines.append("\n## Category and yes/no columns (top values, % of rows)")
        for c in category_columns:
            values = [t for t in top if t["column_name"] == c["column_name"]]
            shown = [f"{t['value']} {t['pct_of_rows']}%" for t in values if t["value_rank"] <= 5]
            other = [t for t in values if t["value_rank"] > TOP_VALUES_SHOWN]
            if other:
                shown.append(f"all values outside the top {TOP_VALUES_SHOWN}: {other[0]['pct_of_rows']}%")
            lines.append(f"- {c['column_name']} ({c['distinct_count']} distinct): {', '.join(shown)}")

    if corr or groups:
        lines.append("\n## Relationships")
    if corr:
        lines.append("Strongest correlations (Pearson r, -1 to 1):")
        lines += [f"- {c['column_a']} and {c['column_b']}: {c['pearson_r']:.2f}" for c in corr[:5]]
    if groups:
        lines.append("Biggest group differences (mean of numeric by category, groups of 30+ rows; strength 0 to 1):")
        lines += [
            f"- {g['numeric_column']} by {g['category_column']} (strength {g['strength_eta_sq']:.2f}): "
            f"highest {g['highest_group']} {fmt(g['highest_mean'])}, lowest {g['lowest_group']} {fmt(g['lowest_mean'])}, "
            f"overall {fmt(g['overall_mean'])}"
            for g in groups
        ]

    if time_facts:
        lines.append("\n## Trends over time")
        for t in time_facts:
            lines += time_lines(t)
    return "\n".join(lines)


def build_prompt(has_time: bool) -> str:
    """The prompt, with "Trends over time" only when a date column was analysed."""
    sections = ["What the data is", "Data quality", "Key insights (3 to 5 bullet points)"]
    if has_time:
        sections.append("Trends over time (2 to 4 bullet points)")
    sections.append("Worth checking next")
    return AI_PROMPT.format(sections=", ".join(sections))


def write_ai_report(upload_id: str, prompt: str, facts: str) -> str:
    """ONE ai_query call per upload. The facts are stored next to the answer so it can be checked."""
    run_sql(
        f"insert into {SCHEMA}.ai_report "
        f"select :upload_id, :facts, ai_query('{AI_MODEL}', concat(:prompt, :facts), "
        "modelParameters => named_struct('temperature', 0.2)), "
        f"'{AI_MODEL}', current_timestamp()",
        {"upload_id": upload_id, "facts": facts, "prompt": prompt},
    )
    return run_sql(f"select report_text from {SCHEMA}.ai_report where upload_id = :upload_id", {"upload_id": upload_id})[0]["report_text"]


# ---------- clean-up (retention) ----------

def delete_file(path: str) -> None:
    """Delete one file, but only inside the auto_landing volume."""
    if not path.startswith(LANDING_PATH):
        raise ValueError(f"Refusing to delete {path}: it's outside {LANDING_PATH}")
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        os.remove(path)  # in the Job, volumes can be used like a normal folder: no extra library needed
    else:
        from databricks.sdk import WorkspaceClient

        WorkspaceClient(
            host="https://" + os.environ["DATABRICKS_HOST"].removeprefix("https://").rstrip("/"),
            token=os.environ["DATABRICKS_TOKEN"],
        ).files.delete(path)


def cleanup(processed_path: str | None = None, dry_run: bool = False) -> None:
    """Retention. With dry_run=True, only print what WOULD be deleted.

    1. processed_path: the CSV this run just profiled (its results are saved, so the file isn't needed).
    2. Any other CSV in auto_landing older than LEFTOVER_CSV_HOURS (never the one being processed).
    3. Rows in every workspace.auto_insights table (the ones with an upload_id) for uploads older than
       RESULTS_KEEP_HOURS. Never the Example. latest_upload, typed_upload and profile_sample hold no
       upload_id (they're replaced every run), so they're left alone.
    Each step has its own try/except: a problem only prints a warning, and the next step still runs.
    """
    verb = "Would delete" if dry_run else "Deleted"
    print("\nClean-up" + (" (DRY RUN: nothing is deleted)" if dry_run else ""))
    now = datetime.now(timezone.utc)

    # 1. the CSV just profiled
    if processed_path:
        try:
            if not dry_run:
                delete_file(processed_path)
            print(f"  {verb} the profiled file {processed_path.rsplit('/', 1)[-1]}")
        except Exception as error:
            print(f"  WARNING: couldn't delete the profiled file: {error}")

    # 2. leftover CSVs (e.g. an upload that never got profiled)
    try:
        cutoff = now - timedelta(hours=LEFTOVER_CSV_HOURS)
        old_files = [
            (path, name, modified) for path, name, modified in list_csvs()
            if modified < cutoff and path != processed_path
        ]
        for path, name, modified in old_files:
            if not dry_run:
                delete_file(path)
            print(f"  {verb} leftover file {name} (last changed {modified:%Y-%m-%d %H:%M} UTC)")
        if not old_files:
            print(f"  No CSV files older than {LEFTOVER_CSV_HOURS} hour(s) in auto_landing.")
    except Exception as error:
        print(f"  WARNING: couldn't clean up leftover files: {error}")

    # 3. results older than RESULTS_KEEP_HOURS. upload_id starts with the run time (20260920_101500_<file name>),
    # so "<" the cutoff means older. That also catches leftover rows from a run that failed before it wrote
    # its uploads row. 'example' doesn't start with a time, and is excluded by name as well.
    try:
        params = {"cutoff": f"{now - timedelta(hours=RESULTS_KEEP_HOURS):%Y%m%d_%H%M%S}", "example": EXAMPLE_ID}
        where = "upload_id <> :example and upload_id rlike '^[0-9]{8}_[0-9]{6}_' and upload_id < :cutoff"
        # uploads first, so a private link stops working before the details go
        total = 0
        for table in ["uploads"] + [t for t in TABLES if t != "uploads"]:
            found = run_sql(
                f"select count(*) as n, count(distinct upload_id) as uploads, "
                f"array_join(array_sort(collect_set(upload_id)), ', ') as ids from {SCHEMA}.{table} where {where}",
                params,
            )[0]
            if not found["n"]:
                continue
            if not dry_run:
                run_sql(f"delete from {SCHEMA}.{table} where {where}", params)
            total += found["n"]
            print(f"  {verb} {found['n']:,} rows from {table} ({found['uploads']} uploads: {found['ids']})")
        if not total:
            print(f"  No results older than {RESULTS_KEEP_HOURS} hours (the Example is always kept).")
    except Exception as error:
        print(f"  WARNING: couldn't clean up old results: {error}")


# ---------- main ----------

def profile(file_path: str, file_name: str, upload_id: str, loaded_at: datetime) -> None:
    """Profile one CSV, write the AI report, and record it in uploads (last, so only finished uploads show)."""
    all_columns = load_latest(file_path)
    row_count = int(run_sql(f"select count(*) as n from {SCHEMA}.latest_upload")[0]["n"])
    if row_count == 0:
        raise RuntimeError(f"{file_name} has no data rows.")

    # read "$1,200", "45%", "yes"/"no" and dd/mm/yyyy text as what they are (into typed_upload)
    columns = all_columns[:MAX_COLUMNS]
    for position, column in enumerate(columns, start=1):
        column["column_position"] = position
    detect_types(f"{SCHEMA}.latest_upload", columns)
    build_typed(columns)

    # big files: profile a random sample, and say so
    table, profiled_rows, is_sampled = f"{SCHEMA}.typed_upload", row_count, row_count > MAX_ROWS
    if is_sampled:
        fraction = min(1.0, 1.1 * MAX_ROWS / row_count)
        run_sql(
            f"create or replace table {SCHEMA}.profile_sample as "
            f"select * from {SCHEMA}.typed_upload where rand(42) < {fraction} limit {MAX_ROWS}"
        )
        table = f"{SCHEMA}.profile_sample"
        profiled_rows = int(run_sql(f"select count(*) as n from {table}")[0]["n"])

    profile_columns(table, columns, profiled_rows)
    numerics = [c for c in columns if c["column_type"] == "numeric"]
    categoricals = [c for c in columns if c["column_type"] in ("categorical", "boolean")]
    # the most complete date columns first; the most complete numeric columns are the measures over time
    dates = sorted([c for c in columns if c["column_type"] == "date" and c["non_null_count"]],
                   key=lambda c: -c["non_null_count"])[:TIME_COLUMNS]
    measures = sorted(numerics, key=lambda c: -c["non_null_count"])[:TIME_MEASURES]
    print(f"{row_count:,} rows, {len(all_columns)} columns: " + ", ".join(
        f"{c['column_name']} ({c['column_type']}"
        + "".join(f", {c[key]}" for key in ("unit", "date_format", "date_note") if c.get(key)) + ")"
        for c in columns
    ))

    # only the analyses that apply to these columns
    top = top_values(table, categoricals, profiled_rows) if categoricals else []
    corr = correlations(table, numerics) if len(numerics) >= 2 else []
    groups = group_insights(table, categoricals, numerics) if categoricals and numerics else []
    bins = histograms(table, numerics) if numerics else []
    series, patterns, time_facts = [], [], []
    for date_column in dates:
        column_series, column_patterns, facts = time_analysis(table, date_column, measures)
        series += column_series
        patterns += column_patterns
        time_facts.append(facts)

    def keyed(rows: list[dict]) -> list[dict]:
        return [{"upload_id": upload_id, **row} for row in rows]

    insert_rows("column_profile", keyed(columns))
    insert_rows("column_details", keyed(columns))
    insert_rows("top_values", keyed(top))
    insert_rows("correlations", keyed(corr))
    insert_rows("group_insights", keyed(groups))
    insert_rows("histograms", keyed(bins))
    insert_rows("time_series", keyed(series))
    insert_rows("time_patterns", keyed(patterns))

    upload = {
        "upload_id": upload_id, "file_name": file_name, "file_path": file_path, "row_count": row_count,
        "column_count": len(all_columns), "profiled_column_count": len(columns),
        "profiled_row_count": profiled_rows, "is_sampled": is_sampled,
        "loaded_at": loaded_at.strftime("%Y-%m-%d %H:%M:%S"),
    }
    facts = build_facts(upload, columns, top, corr, groups, time_facts)
    report = write_ai_report(upload_id, build_prompt(has_time=bool(time_facts)), facts)

    # written last: the dashboard only lists uploads that finished every step
    insert_rows("uploads", [upload])
    print(f"Done: {upload_id}\n\n{report}")


def create_example(local_csv: str) -> None:
    """Create the pinned Example (upload_id 'example') from a local CSV, once. Run from a laptop.

    The CSV is copied to its own volume, not auto_landing, so it doesn't start the Job.
    If the Example already exists, nothing happens (this never deletes or replaces it).
    """
    if run_sql(f"select 1 from {SCHEMA}.uploads where upload_id = :example", {"example": EXAMPLE_ID}):
        print("The Example already exists, so nothing was changed.")
        return
    from databricks.sdk import WorkspaceClient

    run_sql(f"create volume if not exists {EXAMPLE_VOLUME}")
    file_name = os.path.basename(local_csv)
    file_path = EXAMPLE_PATH + file_name
    with open(local_csv, "rb") as file:
        WorkspaceClient(
            host="https://" + os.environ["DATABRICKS_HOST"].removeprefix("https://").rstrip("/"),
            token=os.environ["DATABRICKS_TOKEN"],
        ).files.upload(file_path, file, overwrite=True)
    print(f"Copied {local_csv} to {file_path}")
    profile(file_path, file_name, EXAMPLE_ID, datetime.now(timezone.utc))


def refresh_example() -> None:
    """Re-profile the pinned Example with the current code. Run from a laptop, by hand.

    The one exception to "never delete" outside cleanup(): it deletes ONLY the rows where
    upload_id = 'example', then profiles the Example's CSV (in its own volume) again. The CSV is read
    first, so if it's missing or broken nothing is deleted and the old Example stays.
    """
    found = run_sql(f"select file_name, file_path from {SCHEMA}.uploads where upload_id = :example", {"example": EXAMPLE_ID})
    if not found:
        print("There's no Example yet. Create it once with --as-example.")
        return
    file_name, file_path = found[0]["file_name"], found[0]["file_path"]
    if not file_path.startswith(EXAMPLE_PATH):
        raise RuntimeError(f"The Example's CSV should be in {EXAMPLE_PATH}, not {file_path}. Nothing was changed.")
    load_latest(file_path)  # raises if the CSV can't be read, before anything is deleted
    for table in ["uploads"] + [t for t in TABLES if t != "uploads"]:
        run_sql(f"delete from {SCHEMA}.{table} where upload_id = :example", {"example": EXAMPLE_ID})
    print(f"Deleted the old Example rows; profiling {file_name} again")
    profile(file_path, file_name, EXAMPLE_ID, datetime.now(timezone.utc))


def profile_test_file(local_csv: str) -> None:
    """Profile a local CSV as if it was uploaded on the page, WITHOUT starting the Job. Run from a laptop.

    The CSV goes to its own test volume (not auto_landing, so the Job doesn't notice it), under an
    upload-style name with a random part, so the result opens with a private ?report= link and is
    removed by the normal 24-hour clean-up. Nothing is deleted here; the test CSV stays in test_data.
    """
    from databricks.sdk import WorkspaceClient

    run_sql(f"create volume if not exists {TEST_VOLUME}")
    now = datetime.now(timezone.utc)
    # same format as landing_name() in dashboards/uploader.py
    clean = re.sub(r"[^a-z0-9]+", "_", os.path.splitext(os.path.basename(local_csv))[0].lower()).strip("_")[:60].strip("_") or "file"
    file_name = f"upload_{now:%Y%m%dT%H%M%SZ}_{secrets.token_urlsafe(16)}_{clean}.csv"
    file_path = TEST_PATH + file_name
    with open(local_csv, "rb") as file:
        WorkspaceClient(
            host="https://" + os.environ["DATABRICKS_HOST"].removeprefix("https://").rstrip("/"),
            token=os.environ["DATABRICKS_TOKEN"],
        ).files.upload(file_path, file, overwrite=False)
    upload_id = f"{now:%Y%m%d_%H%M%S}_{file_name}"
    profile(file_path, file_name, upload_id, now)
    print(f"\nPrivate link (24 hours): ?report={upload_id}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--setup-only", action="store_true", help="only create the volume, schema and tables")
    parser.add_argument("--cleanup-dry-run", action="store_true", help="only show what the clean-up would delete")
    parser.add_argument("--as-example", metavar="CSV", help="create the pinned Example from this local CSV (once)")
    parser.add_argument("--test-file", metavar="CSV", help="profile this local CSV via the test volume (doesn't start the Job)")
    parser.add_argument("--refresh-example", action="store_true", help="re-profile the pinned Example with the current code")
    args = parser.parse_args()

    if args.cleanup_dry_run:
        cleanup(dry_run=True)
        return

    setup()
    if args.setup_only:
        print(f"Set up {SCHEMA} and the volume {LANDING_PATH}")
        return
    if args.as_example:
        create_example(args.as_example)
        return
    if args.test_file:
        profile_test_file(args.test_file)
        return
    if args.refresh_example:
        refresh_example()
        return

    file_path, file_name = newest_csv()
    print(f"Newest CSV: {file_name}")
    loaded_at = datetime.now(timezone.utc)
    profile(file_path, file_name, f"{loaded_at:%Y%m%d_%H%M%S}_{file_name}", loaded_at)

    # the profile is saved, so a clean-up problem must not fail the run (cleanup() only prints warnings)
    cleanup(processed_path=file_path)


if __name__ == "__main__":
    main()
