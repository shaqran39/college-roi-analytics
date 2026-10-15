"""Page 1: the AI-written key findings plus four headline numbers."""

import streamlit as st

from db import load_mart, md_safe, money, pretty

roi_by_major = load_mart("mart_roi_by_major")
choices = load_mart("mart_choices_effect")
findings = load_mart("mart_key_findings").iloc[0]

best = roi_by_major.loc[roi_by_major["median_net_roi_usd"].idxmax()]
worst = roi_by_major.loc[roi_by_major["median_net_roi_usd"].idxmin()]
internship_lift = choices.loc[
    (choices["major_category"] == "all") & (choices["choice"] == "internship") & choices["made_choice"],
    "net_roi_lift_usd",
].iloc[0]

st.title("Does college pay off?")
st.markdown(
    "Which majors and college types paid off over 10 years, and how much did internships, "
    "finishing on time and debt change the answer? "
    "**Net return** = extra earnings vs a high-school graduate over 10 years, minus the net cost of college."
)

# headline numbers
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

# AI summary
st.subheader("Key findings")
st.markdown(f"> {md_safe(findings['summary_paragraph'])}")
st.caption(f"Written by AI (`{findings['model_endpoint']}`) on {findings['generated_at']:%d %b %Y}, using only the facts below.")

with st.expander("Check the numbers: the exact facts the AI was given"):
    st.text(findings["input_facts"])
