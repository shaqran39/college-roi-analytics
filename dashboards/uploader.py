"""Password-protected CSV upload for the Analyse your CSV page (views/auto_insights.py), plus a live progress tracker.

Viewing the page is public; uploading needs st.secrets["upload_password"].
The file is saved to the auto_landing volume with its own, write-only token
(st.secrets["databricks_upload_token"]), never the dashboard's read token.
The Databricks Job auto_insights_refresh notices the new file and profiles it.

Each upload is private: its file name gets a random part, so its upload_id (and the ?report= link
made from it) can't be guessed. Only this browser session (st.session_state.my_upload_id) and
anyone given the link can see it, for 24 hours.
"""

import hmac
import io
import re
import secrets
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

from db import query_now

LANDING_PATH = "/Volumes/workspace/raw/auto_landing/"
MAX_MB = 10                 # keep in step with server.maxUploadSize in .streamlit/config.toml
COOLDOWN_SECONDS = 5 * 60   # one upload per browser session every 5 minutes
POLL_SECONDS = 15           # how often the tracker checks whether the profile is ready
START_SECONDS = 2 * 60      # the Job waits ~1 min for the file to settle, then starts
GIVE_UP_SECONDS = 15 * 60   # after this, stop checking and say "check back later"


# ---------- checks and helpers (no Streamlit, easy to test) ----------

def check_csv(data: bytes) -> str | None:
    """None if the file is a usable CSV, otherwise a friendly message saying what's wrong."""
    if len(data) > MAX_MB * 1024 * 1024:
        return f"That file is bigger than {MAX_MB} MB. Please upload a smaller one."
    try:
        data.decode("utf-8")  # Databricks reads it as UTF-8; anything else would show garbled letters
    except UnicodeDecodeError:
        return "That file isn't saved as UTF-8 text. In Excel, use Save As → **CSV UTF-8**."
    try:
        frame = pd.read_csv(io.BytesIO(data))
    except pd.errors.EmptyDataError:
        return "That file is empty."
    except pd.errors.ParserError:
        return "That file couldn't be read as a CSV: some rows have a different number of columns than the first row."
    except Exception:
        return "That file couldn't be read as a CSV."

    names = [str(name).strip() for name in frame.columns]
    if any(name == "" or name.startswith("Unnamed:") for name in names):
        return "Every column needs a name in the first row (the header row). At least one is blank."
    if all(re.fullmatch(r"[-+]?[\d.,]+", name) for name in names):
        return "The first row looks like data, not column names. Add a header row with a name for each column."
    if frame.empty:
        return "That file has column names but no data rows."
    return None


def landing_name(original: str, now: datetime) -> str:
    """'My Sales (Q3).CSV' -> 'upload_20261020T051324Z_<22 random characters>_my_sales_q3.csv'."""
    clean = re.sub(r"[^a-z0-9]+", "_", Path(original).stem.lower()).strip("_")[:60].strip("_") or "file"
    return f"upload_{now:%Y%m%dT%H%M%SZ}_{secrets.token_urlsafe(16)}_{clean}.csv"


def display_name(file_name: str) -> str:
    """'upload_20261020T051324Z_<random part>_my_sales.csv' -> 'my_sales.csv', so the random part stays out of sight."""
    return re.sub(r"^upload_[0-9]{8}T[0-9]{6}Z_[A-Za-z0-9_-]{22}_", "", file_name)


def save_to_volume(file_name: str, data: bytes) -> None:
    """Write the file to the landing volume with the upload-only token. Never overwrites."""
    from databricks.sdk import WorkspaceClient

    host = st.secrets["databricks"]["host"].removeprefix("https://").rstrip("/")
    client = WorkspaceClient(host=f"https://{host}", token=st.secrets["databricks_upload_token"], auth_type="pat")
    client.files.upload(LANDING_PATH + file_name, io.BytesIO(data), overwrite=False)


def minutes(seconds: float) -> str:
    """135 -> '2:15'."""
    return f"{int(seconds) // 60}:{int(seconds) % 60:02d}"


# ---------- the parts of the page ----------

def upload_box() -> None:
    """The 'Upload your own CSV' section: password first, then the upload box."""
    state = st.session_state
    password = st.secrets.get("upload_password", "")
    with st.expander("Upload your own CSV", icon=":material/upload_file:"):
        if not password or not st.secrets.get("databricks_upload_token"):
            st.caption("Uploading is switched off on this copy of the app.")
            return

        if not state.get("upload_unlocked"):
            with st.form("unlock_upload", border=False):
                entered = st.text_input("Upload password", type="password")
                if st.form_submit_button("Unlock"):
                    if hmac.compare_digest(entered.encode(), password.encode()):
                        state.upload_unlocked = True
                        st.rerun()
                    st.error("Wrong password.")
            return

        if state.get("pending_upload"):
            st.info("Your upload is being processed (see the progress above). You can upload another when it's done.")
            return
        wait = COOLDOWN_SECONDS - (time.time() - state.get("last_upload_at", 0))
        if wait > 0:
            st.info(f"You can upload another file in {minutes(wait)} minutes.")
            return

        st.info(
            "Only you (and anyone you share its private link with) can see your results. "
            "They're deleted after 24 hours. Still, use only public or made-up data.",
            icon=":material/lock:",
        )
        file = st.file_uploader(f"CSV file (max {MAX_MB} MB)", type="csv", max_upload_size=MAX_MB)
        st.caption("Uploads are processed one at a time, so if another one is running, yours waits in the queue.")
        if file is None or not st.button("Upload and profile", type="primary"):
            return

        data = file.getvalue()
        problem = check_csv(data)
        if problem:
            st.error(problem)
            return
        now = datetime.now(timezone.utc)
        file_name = landing_name(file.name, now)
        try:
            save_to_volume(file_name, data)
        except Exception as error:
            st.error(f"The upload didn't work ({type(error).__name__}). Please try again in a few minutes.")
            return
        state.pending_upload = {"file_name": file_name, "started": time.time()}
        state.last_upload_at = time.time()
        state.late_upload = None
        state.pop("upload_message", None)
        st.rerun()


def upload_status() -> None:
    """While an upload is processing, a live tracker; afterwards, a one-line result."""
    state = st.session_state
    if state.get("pending_upload"):
        _tracker()
    message = state.get("upload_message")
    if message:
        kind, text = message
        (st.success if kind == "done" else st.warning)(text)
    # the tracker gave up, but the upload may still finish: one check per click
    late = state.get("late_upload")
    if late and st.button("Check again", icon=":material/refresh:"):
        if not _found(late):
            st.info("Not ready yet. Try again in a few minutes.")


def _found(file_name: str) -> bool:
    """If this session's upload has finished, remember it, make the URL its private link and rerun."""
    state = st.session_state
    try:
        found = query_now(
            "select upload_id from workspace.auto_insights.uploads where file_name = :file_name "
            "order by loaded_at desc limit 1",
            params={"file_name": file_name},
        )
    except Exception:
        return False  # a failed check just waits for the next one
    if found.empty:
        return False
    state.my_upload_id = found.iloc[0]["upload_id"]
    st.query_params["report"] = state.my_upload_id
    state.upload_message = ("done", f"**{display_name(file_name)}** is ready. It's shown below.")
    state.pending_upload = state.late_upload = None
    st.rerun()


@st.fragment(run_every=POLL_SECONDS)
def _tracker() -> None:
    """Re-runs every POLL_SECONDS on its own (not the whole page) until the profile appears or we give up."""
    state = st.session_state
    pending = state.get("pending_upload")
    if not pending:
        return
    elapsed = time.time() - pending["started"]
    name = display_name(pending["file_name"])

    _found(pending["file_name"])
    if elapsed > GIVE_UP_SECONDS:
        state.upload_message = ("slow", f"**{name}**: this is taking longer than usual. Keep this tab open and check again later.")
        state.late_upload = pending["file_name"]
        state.pending_upload = None
        st.rerun()

    # the steps are estimated from the time since upload (the app can't see inside the Job)
    starting = elapsed < START_SECONDS
    with st.status(f"Profiling **{name}** · {minutes(elapsed)} elapsed", state="running"):
        st.markdown(
            ":material/check_circle: File received\n\n"
            + (":material/pending: **Waiting for Databricks to start**\n\n:material/radio_button_unchecked: Processing"
               if starting else
               ":material/check_circle: Waiting for Databricks to start\n\n:material/pending: **Processing** (usually 3 to 5 minutes)")
        )
        st.caption(f"Checks every {POLL_SECONDS} seconds. Keep this tab open; the result is shown automatically when it's ready.")
