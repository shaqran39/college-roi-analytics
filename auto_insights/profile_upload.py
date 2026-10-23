"""Auto insights: profile the newest CSV in the auto_landing volume and write an AI report.

Works on ANY CSV (any columns). Runs as the "Python script" task of the Databricks Job
auto_insights_refresh when a new file lands in /Volumes/workspace/raw/auto_landing/,
or from a laptop for testing:

    python auto_insights/profile_upload.py               # profile the newest CSV
    python auto_insights/profile_upload.py --setup-only  # just create the schema, volume and tables
    python auto_insights/profile_upload.py --cleanup-dry-run  # show what the clean-up WOULD delete, delete nothing
    python auto_insights/profile_upload.py --as-example data/raw/cafe_sales_test.csv  # create the pinned Example (once)

All work is SQL sent through run_sql(). In the Job it uses spark.sql; on a laptop it uses
databricks-sql-connector with the env vars DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN.
Values from the CSV (file names, category labels, the AI facts) always go in as query parameters.
Column names can't be parameters, so they are cleaned to snake_case and backtick-quoted.

Separate from the college ROI pipeline: it only touches workspace.raw.auto_landing
and the workspace.auto_insights schema.

Each upload is private: the dashboard shows it only to the person who uploaded it (or anyone with its
private link), for 24 hours. After each successful run it cleans up (see cleanup()): the CSV it just
profiled, any CSV left in auto_landing for over an hour, and every upload's rows in workspace.auto_insights
once they're over 24 hours old. The Example (upload_id 'example') is never deleted.
This is the ONLY code in the project allowed to delete.
"""

import argparse
import os
import re
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

MAX_COLUMNS = 50            # profile at most the first 50 columns
MAX_ROWS = 1_000_000        # bigger files are profiled on a random sample of this many rows
TOP_VALUES_MAX_DISTINCT = 50
MIN_GROUP_ROWS = 30         # groups smaller than this are ignored in group_insights
ID_DISTINCT_SHARE = 0.95    # "nearly every value is unique" -> id / free text
DATE_PARSE_SHARE = 0.90     # a text column where 90%+ of values read as dates is a date
HISTOGRAM_COLUMNS = 6
HISTOGRAM_BINS = 20

NUMERIC_TYPES = ("tinyint", "smallint", "int", "bigint", "float", "double", "decimal")
WHOLE_NUMBER_TYPES = ("tinyint", "smallint", "int", "bigint")

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
}

AI_PROMPT = """You are a careful data analyst. Below is an automatic profile of a CSV file someone uploaded.
Write a short report for a general audience. Rules:
- Use ONLY the numbers in the facts. Do not add, estimate or round to new numbers.
- Never invent causes or reasons. A correlation or a group difference is not proof that one thing causes another; say "went with" or "was linked to", not "caused" or "led to".
- If a group difference or correlation is weak, say so.
- Plain English, Australian spelling (e.g. analyse, colour, organisation). About 200 words.
- Use exactly these four sections, each as a markdown heading (###): What the data is, Data quality, Key insights (3 to 5 bullet points), Worth checking next.
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
        run_sql(f"insert into {SCHEMA}.{table} values " + ",\n".join(value_lists), params)


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

    names = clean_names([row["col_name"] for row in raw_columns])
    select_list = ", ".join(f"{quote_ident(row['col_name'])} as {quote_ident(name)}" for row, name in zip(raw_columns, names))
    run_sql(f"create or replace table {SCHEMA}.latest_upload as select {select_list} from {source}")
    return [
        {"original_name": row["col_name"], "column_name": name, "source_type": row["data_type"].lower()}
        for row, name in zip(raw_columns, names)
    ]


# ---------- profile ----------

def is_numeric(source_type: str) -> bool:
    return source_type.startswith(NUMERIC_TYPES)


def profile_columns(table: str, columns: list[dict], row_count: int) -> None:
    """One aggregate query for all profiled columns; fills in stats and column_type on each dict."""
    expressions = []
    for i, column in enumerate(columns):
        col, kind = quote_ident(column["column_name"]), column["source_type"]
        expressions += [f"count({col}) as nn_{i}", f"count(distinct {col}) as nd_{i}"]
        if is_numeric(kind):
            value = f"cast({col} as double)"
            expressions += [
                f"min({value}) as min_{i}", f"max({value}) as max_{i}", f"avg({value}) as mean_{i}",
                f"percentile_approx({value}, 0.25) as p25_{i}", f"percentile_approx({value}, 0.5) as med_{i}",
                f"percentile_approx({value}, 0.75) as p75_{i}",
            ]
        elif kind.startswith(("date", "timestamp")):
            expressions += [f"cast(min({col}) as string) as dmin_{i}", f"cast(max({col}) as string) as dmax_{i}"]
        elif kind == "string":
            as_date = f"coalesce(try_to_timestamp({col}), cast(try_to_date({col}) as timestamp))"
            expressions += [
                f"count({as_date}) as dates_{i}",
                f"cast(min({as_date}) as string) as dmin_{i}", f"cast(max({as_date}) as string) as dmax_{i}",
            ]
    stats = run_sql(f"select {', '.join(expressions)} from {table}")[0]

    for i, column in enumerate(columns):
        kind = column["source_type"]
        non_null, distinct = int(stats[f"nn_{i}"]), int(stats[f"nd_{i}"])
        column.update(
            non_null_count=non_null,
            null_pct=round(100 * (row_count - non_null) / row_count, 2),
            distinct_count=distinct,
        )
        nearly_unique = non_null > 0 and distinct > TOP_VALUES_MAX_DISTINCT and distinct / non_null >= ID_DISTINCT_SHARE

        if is_numeric(kind):
            column.update(
                min_value=to_float(stats[f"min_{i}"]), max_value=to_float(stats[f"max_{i}"]),
                mean_value=to_float(stats[f"mean_{i}"]), median_value=to_float(stats[f"med_{i}"]),
                p25_value=to_float(stats[f"p25_{i}"]), p75_value=to_float(stats[f"p75_{i}"]),
            )
            column["column_type"] = "id_text" if nearly_unique and kind.startswith(WHOLE_NUMBER_TYPES) else "numeric"
        elif kind.startswith(("date", "timestamp")):
            column.update(min_date=stats[f"dmin_{i}"], max_date=stats[f"dmax_{i}"], column_type="date")
        elif kind == "string" and non_null and int(stats[f"dates_{i}"]) / non_null >= DATE_PARSE_SHARE:
            column.update(min_date=stats[f"dmin_{i}"], max_date=stats[f"dmax_{i}"], column_type="date")
        elif nearly_unique:
            column["column_type"] = "id_text"
        else:
            column["column_type"] = "categorical"


def top_values(table: str, categoricals: list[dict], row_count: int) -> list[dict]:
    """Top 10 values (with count and % of rows) of each categorical with 50 or fewer distinct values."""
    chosen = [c for c in categoricals if c["distinct_count"] <= TOP_VALUES_MAX_DISTINCT]
    if not chosen:
        return []
    parts = [
        f"select {i} as col_index, cast({quote_ident(c['column_name'])} as string) as value, count(*) as n "
        f"from {table} where {quote_ident(c['column_name'])} is not null group by 2"
        for i, c in enumerate(chosen)
    ]
    rows = run_sql(
        "select col_index, value, n, row_number() over (partition by col_index order by n desc, value) as value_rank "
        f"from ({' union all '.join(parts)}) qualify value_rank <= 10"
    )
    return [
        {
            "column_name": chosen[row["col_index"]]["column_name"], "value_rank": int(row["value_rank"]),
            "value": row["value"], "row_count": int(row["n"]), "pct_of_rows": round(100 * row["n"] / row_count, 2),
        }
        for row in rows
    ]


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


def build_facts(upload: dict, columns: list[dict], top: list[dict], corr: list[dict], groups: list[dict]) -> str:
    """Compact text of facts from the profile. id / free-text columns are only named, never described."""
    lines = [
        f"File: {display_name(upload['file_name'])}",
        f"Rows: {upload['row_count']:,}. Columns: {upload['column_count']}"
        + (f" (first {upload['profiled_column_count']} profiled)" if upload["profiled_column_count"] < upload["column_count"] else "")
        + ".",
    ]
    if upload["is_sampled"]:
        lines.append(f"Stats are from a random sample of {upload['profiled_row_count']:,} rows.")

    by_type = {}
    for c in columns:
        by_type.setdefault(c["column_type"], []).append(c["column_name"])
    lines.append("Column types: " + "; ".join(f"{kind}: {', '.join(names)}" for kind, names in by_type.items()))
    if by_type.get("id_text"):
        lines.append("id_text columns (nearly every value unique) are left out of the analysis.")

    missing = [c for c in columns if c["null_pct"] > 0]
    lines.append(
        "Missing values: " + (", ".join(f"{c['column_name']} {c['null_pct']}%" for c in missing) if missing else "none")
    )

    lines.append("Numeric columns (min / p25 / median / mean / p75 / max):")
    for c in [c for c in columns if c["column_type"] == "numeric"]:
        lines.append(
            f"- {c['column_name']}: {fmt(c['min_value'])} / {fmt(c['p25_value'])} / {fmt(c['median_value'])} / "
            f"{fmt(c['mean_value'])} / {fmt(c['p75_value'])} / {fmt(c['max_value'])}"
        )
    for c in [c for c in columns if c["column_type"] == "date"]:
        lines.append(f"Date column {c['column_name']}: {c['min_date']} to {c['max_date']}")

    lines.append("Categorical columns (top values, % of rows):")
    for c in [c for c in columns if c["column_type"] == "categorical"]:
        values = [t for t in top if t["column_name"] == c["column_name"]][:5]
        shown = ", ".join(f"{t['value']} {t['pct_of_rows']}%" for t in values) if values else "too many to list"
        lines.append(f"- {c['column_name']} ({c['distinct_count']} distinct): {shown}")

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
    return "\n".join(lines)


def write_ai_report(upload_id: str, facts: str) -> str:
    """ONE ai_query call per upload. The facts are stored next to the answer so it can be checked."""
    run_sql(
        f"insert into {SCHEMA}.ai_report "
        f"select :upload_id, :facts, ai_query('{AI_MODEL}', concat(:prompt, :facts), "
        "modelParameters => named_struct('temperature', 0.2)), "
        f"'{AI_MODEL}', current_timestamp()",
        {"upload_id": upload_id, "facts": facts, "prompt": AI_PROMPT},
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
       RESULTS_KEEP_HOURS. Never the Example. latest_upload and profile_sample hold no upload_id
       (they're replaced every run), so they're left alone.
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

    # big files: profile a random sample, and say so
    table, profiled_rows, is_sampled = f"{SCHEMA}.latest_upload", row_count, row_count > MAX_ROWS
    if is_sampled:
        fraction = min(1.0, 1.1 * MAX_ROWS / row_count)
        run_sql(
            f"create or replace table {SCHEMA}.profile_sample as "
            f"select * from {SCHEMA}.latest_upload where rand(42) < {fraction} limit {MAX_ROWS}"
        )
        table = f"{SCHEMA}.profile_sample"
        profiled_rows = int(run_sql(f"select count(*) as n from {table}")[0]["n"])

    columns = all_columns[:MAX_COLUMNS]
    for position, column in enumerate(columns, start=1):
        column["column_position"] = position
    profile_columns(table, columns, profiled_rows)
    numerics = [c for c in columns if c["column_type"] == "numeric"]
    categoricals = [c for c in columns if c["column_type"] == "categorical"]
    print(f"{row_count:,} rows, {len(all_columns)} columns: "
          + ", ".join(f"{c['column_name']} ({c['column_type']})" for c in columns))

    top = top_values(table, categoricals, profiled_rows)
    corr = correlations(table, numerics)
    groups = group_insights(table, categoricals, numerics)
    bins = histograms(table, numerics)

    def keyed(rows: list[dict]) -> list[dict]:
        return [{"upload_id": upload_id, **row} for row in rows]

    insert_rows("column_profile", keyed(columns))
    insert_rows("top_values", keyed(top))
    insert_rows("correlations", keyed(corr))
    insert_rows("group_insights", keyed(groups))
    insert_rows("histograms", keyed(bins))

    upload = {
        "upload_id": upload_id, "file_name": file_name, "file_path": file_path, "row_count": row_count,
        "column_count": len(all_columns), "profiled_column_count": len(columns),
        "profiled_row_count": profiled_rows, "is_sampled": is_sampled,
        "loaded_at": loaded_at.strftime("%Y-%m-%d %H:%M:%S"),
    }
    report = write_ai_report(upload_id, build_facts(upload, columns, top, corr, groups))

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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--setup-only", action="store_true", help="only create the volume, schema and tables")
    parser.add_argument("--cleanup-dry-run", action="store_true", help="only show what the clean-up would delete")
    parser.add_argument("--as-example", metavar="CSV", help="create the pinned Example from this local CSV (once)")
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

    file_path, file_name = newest_csv()
    print(f"Newest CSV: {file_name}")
    loaded_at = datetime.now(timezone.utc)
    profile(file_path, file_name, f"{loaded_at:%Y%m%d_%H%M%S}_{file_name}", loaded_at)

    # the profile is saved, so a clean-up problem must not fail the run (cleanup() only prints warnings)
    cleanup(processed_path=file_path)


if __name__ == "__main__":
    main()
