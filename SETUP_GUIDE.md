# Setup guide (Windows)

There are 3 parts. Do them in order. Stuck? Copy the error and paste it to Claude.

- **Part A:** tools on your laptop (about 30 min)
- **Part B:** the data in Databricks (about 30 min)
- **Part C:** connect dbt + Claude Code to Databricks (about 15 min)

---

# Part A: Laptop tools

## A1. Open a terminal in the project folder
1. In File Explorer, go to `Documents\ai-data-warehouse`
2. Click the address bar, type `powershell` and press Enter. This window is your **terminal**.

## A2. Check Python and Git
```
python --version
git --version
```
- Python should be **3.10 to 3.12**. If not, install 3.12 from python.org and tick **"Add python.exe to PATH"**.
- If Git is missing, install it from git-scm.com with the default options.

## A3. Make a virtual environment and install packages
```
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```
You should see `(.venv)` at the start of the line. Run the `activate` line every time you open a new terminal for this project.
> If you see "running scripts is disabled": type `cmd` in the address bar instead of `powershell`, then use `.venv\Scripts\activate.bat`

## A4. Install Claude Code
```
irm https://claude.ai/install.ps1 | iex
```
Close and reopen the terminal (A1), activate the venv, then type `claude` and log in.
Paste this as your first message:
```
Move the two files in _setup/claude-commands/ into .claude/commands/ and delete the _setup folder. Then read CLAUDE.md and README.md and explain this project back to me simply.
```
Type `/exit` to leave. Next time you run `claude`, you'll have `/next-step` and `/explain` shortcuts.

## A5. The data
You already have `college_major_roi.csv`. For local practice, also copy it into the `data\raw\` folder.

---

# Part B: Databricks

## B1. Sign up
Go to **databricks.com/learn/free-edition** and sign up. You'll land in your workspace.

## B2. Create the raw schema and an upload folder
1. In the left menu, click **SQL Editor**
2. Paste in **only these 2 lines** and click **Run**:
```
create schema if not exists workspace.raw;
create volume if not exists workspace.raw.landing;
```

## B3. Upload the CSVs
1. In the left menu, click **Catalog**, then go to **workspace → raw → Volumes → landing**
2. Click **Upload to this volume** and drag in `college_major_roi.csv`

## B4. Create the raw table
1. Back in the **SQL Editor**, open `ingestion\databricks_load_raw.sql` from the project folder in Notepad or VS Code
2. Copy all of it, paste it in and click **Run all**
3. The last result should show **30000** rows

✅ Your data is now in the cloud.

---

# Part C: Connect dbt and Claude Code to Databricks

## C1. Get your connection details
1. Left menu: **SQL Warehouses**, then click your warehouse, then the **Connection details** tab
2. Copy the **Server hostname** (looks like `dbc-xxxx.cloud.databricks.com`) and the **HTTP path**

## C2. Create an access token (like a password for dbt)
1. Click your profile icon (top right) → **Settings** → **Developer** → **Access tokens** → **Generate new token**
2. Copy it. **Never share it or put it in a file that goes to GitHub.**

## C3. Save them on your laptop
In the terminal, replace the parts in quotes with your values:
```
setx DATABRICKS_HOST "dbc-xxxx.cloud.databricks.com"
setx DATABRICKS_HTTP_PATH "/sql/1.0/warehouses/xxxx"
setx DATABRICKS_TOKEN "dapixxxx"
```
Then **close and reopen the terminal** and activate the venv again.

## C4. Test it
```
cd dbt
dbt debug --target prod
dbt build --target prod
```
`dbt debug` should end with **All checks passed!** and `dbt build` should show green **PASS** lines.
In Databricks, under Catalog → workspace → staging, you'll now see `stg_graduates`. **dbt built that.** 🎉

## C5. Hand it over to Claude Code
```
cd ..
claude
```
Then say:
```
/next-step
```
Claude will tell you where we are and what to build next.

---

# Part D (later): Put it on GitHub
Ask Claude Code: `help me push this project to a new public GitHub repo called college-roi-analytics`
