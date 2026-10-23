"""Page 4: GPA vs ROI, debt vs ROI, the spread of outcomes per major, and a "What if" tool."""

import plotly.graph_objects as go
import streamlit as st

from charts import BLUE, BLUE_LIGHT, is_phone, names_above_rows, show, style, zero_line
from db import data_updated, load_mart, md_safe, money, page_header, pretty

TIER_NAMES = {
    "ivy_elite": "Ivy / elite",
    "top_public": "Top public",
    "mid_tier": "Mid-tier",
    "regional": "Regional",
    "for_profit": "For-profit",
}
SMALL_GROUP = 30   # below this, warn that the result is shaky
TINY_GROUP = 10    # below this, don't show a result at all

bands = load_mart("mart_roi_by_band").sort_values(["dimension", "band_order"])
spread = load_mart("mart_outcome_spread_by_major").sort_values("median_net_roi_usd")
what_if = load_mart("mart_what_if")
roi_by_major = load_mart("mart_roi_by_major").set_index("major")

page_header(
    "Deep dive",
    "How do GPA and debt go with the result, how spread out are the outcomes, and how did graduates like you do?",
    data_updated(),
)

# GPA and debt vs ROI
st.subheader("GPA and debt vs 10-year net return")
st.caption(
    "The line is the typical (median) graduate in each group; the shaded band is the middle half "
    "of graduates (25th to 75th percentile). These compare groups and don't prove cause."
)

# both charts share one y range so they can be compared side by side
y_range = [bands["p25_net_roi_usd"].min() * 1.15 if bands["p25_net_roi_usd"].min() < 0 else 0,
           bands["p75_net_roi_usd"].max() * 1.08]


def band_chart(dimension: str, x_title: str) -> go.Figure:
    d = bands[bands["dimension"] == dimension]
    fig = go.Figure()
    # middle-half band: upper edge, then lower edge filled up to it
    fig.add_trace(go.Scatter(x=d["band_label"], y=d["p75_net_roi_usd"], mode="lines",
                             line=dict(width=0), hoverinfo="skip"))
    fig.add_trace(go.Scatter(x=d["band_label"], y=d["p25_net_roi_usd"], mode="lines", line=dict(width=0),
                             fill="tonexty", fillcolor="rgba(42,120,214,0.12)", hoverinfo="skip"))
    fig.add_trace(go.Scatter(
        x=d["band_label"], y=d["median_net_roi_usd"], mode="lines+markers",
        line=dict(color=BLUE, width=2), marker=dict(size=9, color=BLUE, line=dict(color="white", width=2)),
        customdata=d[["p25_net_roi_usd", "p75_net_roi_usd", "grad_count", "pct_roi_negative"]],
        hovertemplate=(
            "<b>%{x}</b><br>Typical: US$%{y:,.0f}<br>"
            "Middle half: US$%{customdata[0]:,.0f} to US$%{customdata[1]:,.0f}<br>"
            "Worse off than no college: %{customdata[3]}%<br>Graduates: %{customdata[2]:,}<extra></extra>"
        ),
    ))
    fig.update_xaxes(title_text=x_title, type="category")
    fig.update_yaxes(title_text="10-year net return (US$)", range=y_range)
    return zero_line(style(fig, height=380, money_axis="y"), axis="y")


col1, col2 = st.columns(2)
show(band_chart("gpa", "GPA"), col1.container(border=True))
show(band_chart("debt", "Debt at graduation (US$)"), col2.container(border=True))

# no-debt grads vs small-debt grads: worth calling out because it surprises people
debt = bands[bands["dimension"] == "debt"].set_index("band_order")
no_debt, small_debt = debt.loc[1], debt.loc[2]
if no_debt["median_net_roi_usd"] < small_debt["median_net_roi_usd"]:
    col2.caption(md_safe(
        f"Graduates with no debt did slightly worse (typical {money(no_debt['median_net_roi_usd'])}) than those "
        f"with small debts under US$25K ({money(small_debt['median_net_roi_usd'])}). This is a difference between "
        "groups, not proof that a little debt helps: the groups may differ in other ways, like major or college type."
    ))

with st.expander("Table view"):
    st.dataframe(
        bands[["dimension", "band_label", "grad_count", "p25_net_roi_usd", "median_net_roi_usd",
               "p75_net_roi_usd", "pct_roi_negative"]],
        hide_index=True,
        width="stretch",
        column_config={
            "dimension": "Split by",
            "band_label": "Group",
            "grad_count": st.column_config.NumberColumn("Graduates", format="localized"),
            "p25_net_roi_usd": st.column_config.NumberColumn("25th pct (US$)", format="localized"),
            "median_net_roi_usd": st.column_config.NumberColumn("Typical (US$)", format="localized"),
            "p75_net_roi_usd": st.column_config.NumberColumn("75th pct (US$)", format="localized"),
            "pct_roi_negative": st.column_config.NumberColumn("% worse off", format="%.1f%%"),
        },
    )

# spread of outcomes per major
st.subheader("How spread out are the outcomes?")
st.caption(
    "For each major: the thin line runs from the **worst case** (1 in 10 graduates did this badly or worse) "
    "to the **best case** (1 in 10 did this well or better). The thick bar is the middle half; the dot is the typical graduate."
)
spread["label"] = spread["major"].map(pretty)
fig = go.Figure()
for _, r in spread.iterrows():
    fig.add_trace(go.Scatter(x=[r["p10_net_roi_usd"], r["p90_net_roi_usd"]], y=[r["label"]] * 2, mode="lines",
                             line=dict(color=BLUE_LIGHT, width=2), hoverinfo="skip"))
    fig.add_trace(go.Scatter(x=[r["p25_net_roi_usd"], r["p75_net_roi_usd"]], y=[r["label"]] * 2, mode="lines",
                             line=dict(color=BLUE_LIGHT, width=10), hoverinfo="skip"))
fig.add_trace(go.Scatter(
    x=spread["median_net_roi_usd"], y=spread["label"], mode="markers",
    marker=dict(color=BLUE, size=10, line=dict(color="white", width=2)),
    customdata=spread[["p10_net_roi_usd", "p25_net_roi_usd", "p75_net_roi_usd", "p90_net_roi_usd"]],
    hovertemplate=(
        "<b>%{y}</b><br>Best case (top 10%): US$%{customdata[3]:,.0f}<br>"
        "Middle half: US$%{customdata[1]:,.0f} to US$%{customdata[2]:,.0f}<br>"
        "Typical: US$%{x:,.0f}<br>Worst case (bottom 10%): US$%{customdata[0]:,.0f}<extra></extra>"
    ),
))
fig.update_xaxes(title_text="10-year net return (US$)")
phone = is_phone()
style(fig, height=(48 if phone else 32) * len(spread) + 100)
if phone:  # names above the lines, so the lines get the full width
    names_above_rows(fig, spread["label"], gap=8)
show(zero_line(fig), st.container(border=True))

with st.expander("Table view"):
    st.dataframe(
        spread.sort_values("median_net_roi_usd", ascending=False)[[
            "label", "grad_count", "p10_net_roi_usd", "p25_net_roi_usd", "median_net_roi_usd",
            "p75_net_roi_usd", "p90_net_roi_usd"]],
        hide_index=True,
        width="stretch",
        column_config={
            "label": "Major",
            "grad_count": st.column_config.NumberColumn("Graduates", format="localized"),
            "p10_net_roi_usd": st.column_config.NumberColumn("Worst case (US$)", format="localized"),
            "p25_net_roi_usd": st.column_config.NumberColumn("25th pct (US$)", format="localized"),
            "median_net_roi_usd": st.column_config.NumberColumn("Typical (US$)", format="localized"),
            "p75_net_roi_usd": st.column_config.NumberColumn("75th pct (US$)", format="localized"),
            "p90_net_roi_usd": st.column_config.NumberColumn("Best case (US$)", format="localized"),
        },
    )

# What if
st.subheader("What if…?")
st.caption("Pick a combination to see how graduates like that did in this data.")

c1, c2, c3, c4 = st.columns([3, 2, 2, 2], vertical_alignment="bottom")
majors_best_first = roi_by_major.sort_values("median_net_roi_usd", ascending=False).index.tolist()
major = c1.selectbox("Major", majors_best_first, format_func=pretty)
tier = c2.selectbox("Type of college", list(TIER_NAMES), format_func=TIER_NAMES.get, index=2)
has_internship = c3.toggle("Did an internship", value=True)
on_time = c4.toggle("Finished on time", value=True)

match = what_if[
    (what_if["major"] == major) & (what_if["institution_tier"] == tier)
    & (what_if["has_internship"] == has_internship) & (what_if["is_completed_on_time"] == on_time)
]
who = (f"{pretty(major)} graduates from {TIER_NAMES[tier].lower()} colleges who "
       f"{'did' if has_internship else 'did not do'} an internship and finished "
       f"{'on time' if on_time else 'late'}")

with st.container(border=True):
    if match.empty or match.iloc[0]["grad_count"] < TINY_GROUP:
        n = 0 if match.empty else int(match.iloc[0]["grad_count"])
        st.warning(f"Only {n} {who.lower()} in the data: too few for a reliable answer. Try changing one choice.")
    else:
        r = match.iloc[0]
        major_typical = roi_by_major.loc[major, "median_net_roi_usd"]
        st.markdown(f"**{int(r['grad_count']):,}** {who}:")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric(
            "Typical 10-year net return", money(r["median_net_roi_usd"]),
            delta=f"{money(r['median_net_roi_usd'] - major_typical)} vs whole major",
            help="Median for this group, and the difference from the median of every graduate in the major.",
            border=True,
        )
        m2.metric("Worse off than no college", f"{r['pct_roi_negative']}%", border=True,
                  help="Share of this group whose net return was below zero.")
        m3.metric("Median payback", f"{r['median_payback_years']} yrs", border=True,
                  help="Typical years until extra earnings covered the net cost of college.")
        m4.metric("Typical debt", money(r["median_debt_usd"]), border=True,
                  help=f"Typical net cost of college for this group: {money(r['median_net_cost_usd'])}.")
        st.markdown(md_safe(
            f"Half of this group landed between **{money(r['p25_net_roi_usd'])}** and "
            f"**{money(r['p75_net_roi_usd'])}** (25th to 75th percentile)."
        ))
        if r["grad_count"] < SMALL_GROUP:
            st.caption(f"⚠️ Small group ({int(r['grad_count'])} graduates): treat these numbers as rough.")
    st.caption("Past results in synthetic data, not a prediction for any real student.")
