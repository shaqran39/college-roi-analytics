"""Page 3: institution tier comparison, plus the internship / on-time difference by major category."""

import plotly.graph_objects as go
import streamlit as st

from charts import BLUE, BLUE_DARK, BLUE_LIGHT, GREY, RED, style, zero_line
from db import load_mart, money

TIER_NAMES = {
    "ivy_elite": "Ivy / elite",
    "top_public": "Top public",
    "mid_tier": "Mid-tier",
    "regional": "Regional",
    "for_profit": "For-profit",
}
CHOICE_NAMES = {"internship": "Internship", "on_time_completion": "Finishing on time"}

tiers = load_mart("mart_roi_by_tier")
tiers["label"] = tiers["institution_tier"].map(TIER_NAMES)
tiers = tiers.set_index("institution_tier").loc[list(TIER_NAMES)].reset_index()
choices = load_mart("mart_choices_effect")

st.title("Colleges & choices")

# tier comparison: three small charts side by side instead of one chart with mixed scales
st.subheader("By type of college")
st.markdown("The typical (median) result for graduates of each type of college.")


def tier_bar(column: str, title: str, text, hover: str, color: str = BLUE) -> go.Figure:
    fig = go.Figure(go.Bar(
        x=tiers[column], y=tiers["label"], orientation="h",
        marker=dict(color=color, cornerradius=4),
        text=text, textposition="outside", cliponaxis=False,
        hovertemplate=f"<b>%{{y}}</b><br>{hover}<extra></extra>",
    ))
    fig.update_yaxes(autorange="reversed")
    # room on the right for the value label of the longest bar
    fig.update_xaxes(title_text=title, showticklabels=False, range=[min(tiers[column].min(), 0), tiers[column].max() * 1.55])
    return style(fig, height=270, money_axis=None)


col1, col2, col3 = st.columns(3)
col1.plotly_chart(
    zero_line(tier_bar("median_net_roi_usd", "Typical 10-year net return",
                       tiers["median_net_roi_usd"].map(money), "Typical net return: US$%{x:,.0f}")),
    width="stretch",
)
col2.plotly_chart(
    tier_bar("pct_roi_negative", "% worse off than no college",
             tiers["pct_roi_negative"].map(lambda v: f"{v}%"), "Worse off: %{x}%", color=RED),
    width="stretch",
)
col3.plotly_chart(
    tier_bar("median_debt_usd", "Typical debt at graduation",
             tiers["median_debt_usd"].map(money), "Median debt: US$%{x:,.0f}"),
    width="stretch",
)

with st.expander("Table view"):
    st.dataframe(
        tiers[["label", "grad_count", "median_net_roi_usd", "pct_roi_negative", "median_payback_years",
               "median_net_cost_usd", "median_debt_usd", "median_debt_to_earnings_ratio"]],
        hide_index=True,
        width="stretch",
        column_config={
            "label": "Type of college",
            "grad_count": st.column_config.NumberColumn("Graduates", format="localized"),
            "median_net_roi_usd": st.column_config.NumberColumn("Typical net return (US$)", format="localized"),
            "pct_roi_negative": st.column_config.NumberColumn("% worse off", format="%.1f%%"),
            "median_payback_years": st.column_config.NumberColumn("Median payback (years)", format="%.1f"),
            "median_net_cost_usd": st.column_config.NumberColumn("Typical net cost (US$)", format="localized"),
            "median_debt_usd": st.column_config.NumberColumn("Typical debt (US$)", format="localized"),
            "median_debt_to_earnings_ratio": st.column_config.NumberColumn("Debt ÷ yearly pay", format="%.2f"),
        },
    )

# internship / on-time difference by major category
st.subheader("Internships and finishing on time")
choice = st.segmented_control(
    "Compare graduates by", list(CHOICE_NAMES), format_func=CHOICE_NAMES.get, default="internship",
) or "internship"
st.caption(
    "These compare groups of graduates. They don't prove the internship or finishing on time *caused* "
    "the difference: the same students may differ in other ways too."
)

picked = choices[choices["choice"] == choice]
wide = picked.pivot(index="major_category", columns="made_choice", values="median_net_roi_usd")
wide = wide.rename(columns={False: "without", True: "with"})
wide.columns.name = None
wide["difference"] = wide["with"] - wide["without"]
counts = picked.pivot(index="major_category", columns="made_choice", values="grad_count")
wide["n_without"], wide["n_with"] = counts[False], counts[True]
# 'All graduates' on top, then categories by size of the difference
wide = wide.reset_index()
wide["label"] = wide["major_category"].map(lambda c: "All graduates" if c == "all" else c if c == "STEM" else c.replace("_", " ").capitalize())
wide = wide.assign(is_all=wide["major_category"] == "all").sort_values(["is_all", "difference"], ascending=[False, False])

fig = go.Figure()
for _, row in wide.iterrows():  # grey connector between the two dots
    fig.add_trace(go.Scatter(x=[row["without"], row["with"]], y=[row["label"]] * 2, mode="lines",
                             line=dict(color=GREY, width=2), hoverinfo="skip", showlegend=False))
for side, color, name in [("without", BLUE_LIGHT, f"Without {CHOICE_NAMES[choice].lower()}"),
                          ("with", BLUE_DARK, f"With {CHOICE_NAMES[choice].lower()}")]:
    fig.add_trace(go.Scatter(
        x=wide[side], y=wide["label"], mode="markers", name=name,
        marker=dict(color=color, size=12, line=dict(color="white", width=2)),
        customdata=wide[[f"n_{side}", "difference"]],
        hovertemplate=f"<b>%{{y}}</b><br>{name}: US$%{{x:,.0f}}<br>Graduates: %{{customdata[0]:,}}<extra></extra>",
    ))
# the difference, written at the right end of each row
for _, row in wide.iterrows():
    fig.add_annotation(x=max(row["with"], row["without"]), y=row["label"], text=f"{'+' if row['difference'] >= 0 else ''}{money(row['difference'])}",
                       showarrow=False, xanchor="left", xshift=12, font=dict(size=12))
fig.update_yaxes(autorange="reversed")
fig.update_xaxes(title_text="Median 10-year net return (US$)")
style(fig, height=60 * len(wide) + 80)
fig.update_layout(showlegend=True, legend=dict(orientation="h", y=1.02, yanchor="bottom", x=0), margin=dict(r=100, t=44))
st.plotly_chart(fig, width="stretch")

with st.expander("Table view"):
    st.dataframe(
        wide[["label", "without", "with", "difference", "n_without", "n_with"]],
        hide_index=True,
        width="stretch",
        column_config={
            "label": "Major category",
            "without": st.column_config.NumberColumn("Without (US$)", format="localized"),
            "with": st.column_config.NumberColumn("With (US$)", format="localized"),
            "difference": st.column_config.NumberColumn("Difference (US$)", format="localized"),
            "n_without": st.column_config.NumberColumn("Graduates without", format="localized"),
            "n_with": st.column_config.NumberColumn("Graduates with", format="localized"),
        },
    )
