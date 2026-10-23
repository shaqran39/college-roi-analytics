"""AppTest: the Auto insights page shows exactly the tabs and sections that apply to each upload.

For each upload it reads which auto_insights tables have rows, works out what the page SHOULD show,
then runs the page and checks it shows exactly that (and no errors). The Example is always checked.
Test uploads only last 24 hours, so pass their ids in an env var (comma-separated), e.g. the
"Private link" ids printed by `python auto_insights/profile_upload.py --test-file <csv>`:

    $env:AUTO_INSIGHTS_TEST_IDS = "<id 1>,<id 2>"
    pytest dashboards/tests

Needs the Databricks credentials in dashboards/.streamlit/secrets.toml (skipped without them).
"""

import os
import sys
import tomllib
from pathlib import Path

import pytest
from databricks import sql
from streamlit.testing.v1 import AppTest

DASHBOARDS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DASHBOARDS))  # the page imports charts, db and uploader from dashboards/
SECRETS = DASHBOARDS / ".streamlit" / "secrets.toml"
SCHEMA = "workspace.auto_insights"
UPLOAD_IDS = ["example"] + [i.strip() for i in os.environ.get("AUTO_INSIGHTS_TEST_IDS", "").split(",") if i.strip()]

pytestmark = pytest.mark.skipif(not SECRETS.exists(), reason="needs dashboards/.streamlit/secrets.toml")


@pytest.fixture(scope="module")
def secrets() -> dict:
    with open(SECRETS, "rb") as file:
        return tomllib.load(file)


def row_counts(secrets: dict, upload_id: str) -> dict:
    """Rows per table for one upload, plus how many numeric columns it has."""
    creds = secrets["databricks"]
    tables = ["column_profile", "top_values", "correlations", "group_insights", "time_series"]
    query = " union all ".join(
        f"select '{t}' as name, count(*) as n from {SCHEMA}.{t} where upload_id = :id" for t in tables
    ) + f" union all select 'numeric' as name, count(*) as n from {SCHEMA}.column_profile where upload_id = :id and column_type = 'numeric'"
    with sql.connect(server_hostname=creds["host"].removeprefix("https://").rstrip("/"), http_path=creds["http_path"],
                     access_token=creds["token"]) as connection, connection.cursor() as cursor:
        cursor.execute(query, parameters={"id": upload_id})
        return {name: int(n) for name, n in cursor.fetchall()}


@pytest.mark.parametrize("upload_id", UPLOAD_IDS)
def test_page_shows_only_what_applies(secrets: dict, upload_id: str) -> None:
    counts = row_counts(secrets, upload_id)
    assert counts["column_profile"], f"{upload_id} has no results (more than 24 hours old?)"
    expected_tabs = ["Overview"]
    expected_sections = ["AI report", "Data quality"]
    if counts["numeric"] or counts["top_values"]:
        expected_tabs.append("Columns")
        expected_sections += ["Numeric columns"] * bool(counts["numeric"]) + ["Category columns"] * bool(counts["top_values"])
    if counts["correlations"] or counts["group_insights"]:
        expected_tabs.append("Relationships")
        expected_sections += (["Strongest correlations"] * bool(counts["correlations"])
                              + ["Biggest group differences"] * bool(counts["group_insights"]))
    if counts["time_series"]:
        expected_tabs.append("Trends over time")

    at = AppTest.from_file(str(DASHBOARDS / "views" / "auto_insights.py"), default_timeout=300)
    for key, value in secrets.items():
        at.secrets[key] = value
    if upload_id != "example":
        at.query_params["report"] = upload_id
    at.run()

    assert not at.exception, [e.value for e in at.exception]
    assert not at.error, [e.value for e in at.error]
    assert not at.warning, [w.value for w in at.warning]  # e.g. "That report link doesn't work"
    assert [t.label for t in at.tabs] == expected_tabs
    assert [s.value for s in at.subheader] == expected_sections
