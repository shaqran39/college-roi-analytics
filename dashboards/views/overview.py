"""Page 1: the landing page. What the project is, how it works, four headline numbers and the AI key findings."""

import streamlit as st

from db import data_updated, load_mart, md_safe, money, page_header, pretty

STEPS = [  # the "How it works" strip: (icon, step, what happens)
    (":material/description:", "CSV", "30,000 synthetic graduates, one row each"),
    (":material/database:", "Databricks", "Loaded into a raw table in the lakehouse"),
    (":material/account_tree:", "dbt", "Cleaned, joined and summarised, with tests"),
    (":material/auto_awesome:", "AI", "ai_query writes report cards from the numbers"),
    (":material/dashboard:", "Dashboard", "This app, reading the finished tables"),
]

roi_by_major = load_mart("mart_roi_by_major")
choices = load_mart("mart_choices_effect")
findings = load_mart("mart_key_findings").iloc[0]

best = roi_by_major.loc[roi_by_major["median_net_roi_usd"].idxmax()]
worst = roi_by_major.loc[roi_by_major["median_net_roi_usd"].idxmin()]
internship_lift = choices.loc[
    (choices["major_category"] == "all") & (choices["choice"] == "internship") & choices["made_choice"],
    "net_roi_lift_usd",
].iloc[0]

page_header(
    "Does college pay off?",
    "Which majors and college types paid off over 10 years, and how much did internships, "
    "finishing on time and debt change the answer? "
    "**Net return** = extra earnings vs a high-school graduate over 10 years, minus the net cost of college.",
    data_updated(),
)
st.markdown(
    "An end-to-end analytics project: a CSV of **30,000 synthetic graduates** goes through Databricks, dbt and AI "
    "into this dashboard. Explore the results below, or try **Analyse your CSV** on a file of your own."
)

# how it works
st.subheader("How it works")
for column, (icon, step, detail) in zip(st.columns(len(STEPS)), STEPS):
    with column.container(border=True):
        st.markdown(f"{icon} **{step}**")
        st.caption(detail)

# headline numbers
st.subheader("Headline numbers")
col1, col2, col3, col4 = st.columns(4)
col1.metric(
    "Graduates analysed",
    f"{roi_by_major['grad_count'].sum():,}",
    help=f"Across {len(roi_by_major)} majors. Synthetic data.",
    border=True,
)
col2.metric(
    "Best major",
    money(best["median_net_roi_usd"]),
    delta=pretty(best["major"]), delta_color="off", delta_arrow="off",
    help="Median 10-year net return for the major with the highest median.",
    border=True,
)
col3.metric(
    "Worst major",
    money(worst["median_net_roi_usd"]),
    delta=pretty(worst["major"]), delta_color="off", delta_arrow="off",
    help="Median 10-year net return for the major with the lowest median. Negative = worse off than not going to college.",
    border=True,
)
col4.metric(
    "Internship difference",
    f"+{money(internship_lift)}" if internship_lift >= 0 else money(internship_lift),
    help="Median net return of graduates with an internship minus those without. Compares groups, doesn't prove cause.",
    border=True,
)

button, _ = st.columns([1, 3])
if button.button("Analyse your CSV", type="primary", icon=":material/upload_file:", width="stretch"):
    st.switch_page("views/auto_insights.py")

# AI summary
st.subheader("Key findings")
with st.container(border=True):
    st.markdown(md_safe(findings["summary_paragraph"]))
    st.caption(f"Written by AI (`{findings['model_endpoint']}`) on {findings['generated_at']:%d %b %Y}, using only the facts below.")
    with st.expander("Check the numbers: the exact facts the AI was given"):
        st.text(findings["input_facts"])
