"""Read-only access to the Databricks marts, with caching.

Credentials come from st.secrets["databricks"] (dashboards/.streamlit/secrets.toml locally,
the app's Secrets settings on Streamlit Cloud). Never hard-code them.
"""

import pandas as pd
import streamlit as st
from databricks import sql

MARTS_SCHEMA = "workspace.marts"

# results are kept for 1 hour, so clicking around the dashboard doesn't wake the warehouse,
# but new data from the Databricks Job shows up within the hour (or straight away with "Refresh data")
CACHE_SECONDS = 60 * 60


def _credentials() -> dict:
    """Return the Databricks credentials, or stop the page with a friendly message if they're missing."""
    creds = st.secrets.get("databricks", {})
    missing = [key for key in ("host", "http_path", "token") if not creds.get(key)]
    if missing:
        st.error(
            "Databricks credentials are missing: " + ", ".join(missing) + ". "
            "Fill them in `dashboards/.streamlit/secrets.toml` (locally) or the app's Secrets settings "
            "(Streamlit Cloud), then refresh."
        )
        st.stop()
    return creds


@st.cache_data(
    ttl=CACHE_SECONDS,
    show_spinner="Loading from Databricks. The first load can take up to 30 seconds while the warehouse wakes up...",
)
def run_query(query: str, params: dict | None = None) -> pd.DataFrame:
    """Run a SQL query and return the result as a DataFrame. Same query (and params) = cached result.

    Values that change, like a picked upload, go in params as named markers (`where id = :id`),
    never into the query text.
    """
    return query_now(query, params)


def query_now(query: str, params: dict | None = None) -> pd.DataFrame:
    """Like run_query, but never cached: always asks Databricks. Only for checks that must be live."""
    creds = _credentials()
    # a fresh connection per (rare, cached) query, so an idle warehouse can't leave us a dead connection
    with sql.connect(
        server_hostname=creds["host"].removeprefix("https://").rstrip("/"),
        http_path=creds["http_path"],
        access_token=creds["token"],
    ) as connection, connection.cursor() as cursor:
        cursor.execute(query, parameters=params)
        return cursor.fetchall_arrow().to_pandas()


def refresh_button() -> None:
    """Sidebar button that forgets the cached results and reloads the page with fresh data."""
    if st.sidebar.button(
        "Refresh data",
        icon=":material/refresh:",
        help="Fetch the latest numbers from Databricks (otherwise they update every hour).",
    ):
        run_query.clear()
        st.rerun()


def load_mart(table_name: str) -> pd.DataFrame:
    """Load a whole mart table, e.g. load_mart("mart_roi_by_major")."""
    return run_query(f"select * from {MARTS_SCHEMA}.{table_name}")


def money(value: float) -> str:
    """Compact US$ amount for headline numbers: 525800 -> 'US$525.8K', -35500 -> '−US$35.5K'."""
    sign = "−" if value < 0 else ""
    amount = abs(value)
    if amount >= 1_000_000:
        return f"{sign}US${amount / 1_000_000:.1f}M"
    if amount >= 1_000:
        return f"{sign}US${amount / 1_000:.1f}K"
    return f"{sign}US${amount:,.0f}"


def md_safe(text: str) -> str:
    """Escape $ so Streamlit markdown shows 'US$1,800' instead of reading '$...$' as LaTeX maths."""
    return text.replace("$", r"\$")


def pretty(name: str) -> str:
    """'electrical_engineering' -> 'Electrical engineering'."""
    return name.replace("_", " ").capitalize()
