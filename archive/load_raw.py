"""
Load every CSV in data/raw/ into the `raw` schema of the DuckDB warehouse.

Example: data/raw/college_major_roi.csv  ->  raw.college_major_roi

Run from the project root:
    python ingestion/load_raw.py
"""
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"
DB_PATH = ROOT / "warehouse" / "college.duckdb"


def table_name_for(csv_path: Path) -> str:
    """college_major_roi.csv -> college_major_roi"""
    return csv_path.stem.lower()


def main() -> None:
    csv_files = sorted(RAW_DIR.glob("*.csv"))
    if not csv_files:
        print(f"No CSV files found in {RAW_DIR}. Put college_major_roi.csv there first (see SETUP_GUIDE.md).")
        return

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute("create schema if not exists raw")

    print(f"Loading {len(csv_files)} files into {DB_PATH.name}\n")
    for csv_path in csv_files:
        table = table_name_for(csv_path)
        # Full refresh each run: simple and safe for a static dataset
        con.execute(
            f"""
            create or replace table raw.{table} as
            select *, current_timestamp as _loaded_at
            from read_csv_auto(?, header = true)
            """,
            [str(csv_path)],
        )
        rows = con.execute(f"select count(*) from raw.{table}").fetchone()[0]
        print(f"  raw.{table:<35} {rows:>10,} rows")

    con.close()
    print("\nDone. Next: cd dbt, then dbt build")


if __name__ == "__main__":
    main()
