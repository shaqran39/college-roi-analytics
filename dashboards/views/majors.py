"""Page 2: typical 10-year net return by major, plus each major's AI report card."""

import plotly.graph_objects as go
import streamlit as st

from charts import BLUE, RED, style, zero_line
from db import load_mart, md_safe, money, pretty

roi_by_major = load_mart("mart_roi_by_major").sort_values("median_net_roi_usd")
report_cards = load_mart("mart_major_report_cards").set_index("major")
roi_by_major["label"] = roi_by_major["major"].map(pretty)

st.title("Majors")
st.markdown(
    "Typical (median) **10-year net return** for each major: extra earnings vs a high-school graduate, "
    "minus the net cost of college. **Red** = the typical graduate was worse off than not going to college."
)

# ROI by major
fig = go.Figure(go.Bar(
    x=roi_by_major["median_net_roi_usd"],
    y=roi_by_major["label"],
    orientation="h",
    marker=dict(color=[RED if v < 0 else BLUE for v in roi_by_major["median_net_roi_usd"]], cornerradius=4),
    # negative bars get their label from the annotations below instead
    text=[money(v) if v >= 0 else "" for v in roi_by_major["median_net_roi_usd"]],
    textposition="outside",
    cliponaxis=False,
    customdata=roi_by_major[["grad_count", "pct_roi_negative", "median_payback_years"]],
    hovertemplate=(
        "<b>%{y}</b><br>Typical net return: US$%{x:,.0f}<br>"
        "Worse off than no college: %{customdata[1]}%<br>"
        "Median payback: %{customdata[2]} years<br>"
        "Graduates: %{customdata[0]:,}<extra></extra>"
    ),
))
# negative values are written just right of the zero line, so on a narrow phone screen
# they can't collide with the major names on the left
for _, row in roi_by_major[roi_by_major["median_net_roi_usd"] < 0].iterrows():
    fig.add_annotation(x=0, y=row["label"], text=money(row["median_net_roi_usd"]),
                       showarrow=False, xanchor="left", xshift=4)
# room on the right so the longest bar's label isn't cut off
low, high = roi_by_major["median_net_roi_usd"].min(), roi_by_major["median_net_roi_usd"].max()
fig.update_xaxes(title_text="Median 10-year net return (US$)", range=[min(low, 0) * 1.1, high * 1.05])
style(fig, height=32 * len(roi_by_major) + 100)
fig.update_layout(margin_r=90)  # fixed space for the longest label, even on a phone
st.plotly_chart(zero_line(fig), width="stretch")

with st.expander("Table view"):
    st.dataframe(
        roi_by_major.sort_values("median_net_roi_usd", ascending=False)[[
            "label", "grad_count", "median_net_roi_usd", "pct_roi_negative",
            "median_payback_years", "pct_not_paid_back_10yr", "median_debt_to_earnings_ratio",
        ]],
        hide_index=True,
        width="stretch",
        column_config={
            "label": "Major",
            "grad_count": st.column_config.NumberColumn("Graduates", format="localized"),
            "median_net_roi_usd": st.column_config.NumberColumn("Typical net return (US$)", format="localized"),
            "pct_roi_negative": st.column_config.NumberColumn("% worse off", format="%.1f%%"),
            "median_payback_years": st.column_config.NumberColumn("Median payback (years)", format="%.1f"),
            "pct_not_paid_back_10yr": st.column_config.NumberColumn("% not paid back in 10 yrs", format="%.1f%%"),
            "median_debt_to_earnings_ratio": st.column_config.NumberColumn("Debt ÷ yearly pay", format="%.2f"),
        },
    )

# report card for one major
st.subheader("Report card")
majors_best_first = roi_by_major.sort_values("median_net_roi_usd", ascending=False)["major"].tolist()
major = st.selectbox("Pick a major", majors_best_first, format_func=pretty)
card = report_cards.loc[major]
stats = roi_by_major.set_index("major").loc[major]

with st.container(border=True):
    st.markdown(f"#### {pretty(major)}")
    st.markdown(md_safe(card["verdict"]))
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Typical net return", money(stats["median_net_roi_usd"]))
    col2.metric("Worse off than no college", f"{stats['pct_roi_negative']}%")
    col3.metric("Median payback", f"{stats['median_payback_years']} yrs")
    col4.metric("Debt ÷ yearly pay", f"{stats['median_debt_to_earnings_ratio']:.2f}")
    st.caption(f"Opening takeaway from fixed rules; the rest written by AI (`{card['model_endpoint']}`) from these numbers only.")

with st.expander("All report cards"):
    for m in majors_best_first:
        st.markdown(f"**{pretty(m)}**: {md_safe(report_cards.loc[m, 'verdict'])}")
