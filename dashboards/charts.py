"""Shared chart styling so every chart on every page looks the same.

style() is the one shared template: same font and sizes, light gridlines, left-aligned titles,
clean hover labels, axis titles with units. show() draws a chart with the Plotly toolbar hidden.
Streamlit's own theme still sets the text and background colours, so charts follow light / dark mode.

Colours follow one rule each, and are never the only signal (labels, +/- signs or a legend go with them):
blue = the one series being shown, red = worse off than not going to college (below zero),
orange = the other side of a comparison or a negative value, grey = context.
Blue / red and blue / orange were checked for colour-blind separation in light and dark mode.
"""

import plotly.graph_objects as go
import streamlit as st

BLUE = "#2a78d6"        # main series
BLUE_LIGHT = "#86b6ef"  # ranges around a blue series (e.g. the middle half), never a second category
RED = "#e34948"         # below zero: worse off than not going to college
ORANGE = "#eb6834"      # the other side of a with / without comparison, or a negative correlation
GREY = "#898781"        # context marks, connectors, zero line
GRID = "rgba(137,135,129,0.22)"
FONT_SIZE = 15          # all chart text: big enough for a laptop and a phone (the font itself comes from Streamlit's theme)

# colour-blind-safe order for charts with several categories (blue, orange, aqua: checked all-pairs)
CATEGORICAL = [BLUE, ORANGE, "#1baf7a"]


def style(fig: go.Figure, height: int, money_axis: str | None = "x") -> go.Figure:
    """Apply the shared look. money_axis = which axis shows US$ amounts ('x', 'y' or None)."""
    fig.update_layout(
        height=height,
        # top margin keeps the first bar's value label clear of the title (less when there's no title)
        margin=dict(l=8, r=24, t=48 if fig.layout.title.text else 24, b=8),
        showlegend=False,
        font=dict(size=FONT_SIZE),
        title=dict(x=0, xanchor="left", xref="paper", font=dict(size=FONT_SIZE + 1)),
        legend=dict(font_size=FONT_SIZE),
        hoverlabel=dict(font_size=FONT_SIZE, align="left"),
        bargap=0.35,
        colorway=CATEGORICAL,
    )
    for axis in (fig.update_xaxes, fig.update_yaxes):
        axis(showgrid=False, zeroline=False, linecolor=GRID, tickfont_size=FONT_SIZE,
             title_font_size=FONT_SIZE, title_standoff=10)
    fig.update_traces(textfont_size=FONT_SIZE, selector=dict(type="bar"))
    fig.update_annotations(font_size=FONT_SIZE)
    if money_axis:
        update = fig.update_xaxes if money_axis == "x" else fig.update_yaxes
        # '~s' gives 200k / 1.2M; the axis title says it's US$. Few, flat ticks so they fit on a phone
        update(tickformat="~s", showgrid=True, gridcolor=GRID, gridwidth=1, nticks=5, tickangle=0)
    return fig


def show(fig: go.Figure, where=st) -> None:
    """Draw a chart full width with the Plotly toolbar hidden. where = st, a column or a container."""
    where.plotly_chart(fig, width="stretch", config={"displayModeBar": False})


def zero_line(fig: go.Figure, axis: str = "x") -> go.Figure:
    """A solid hairline at 0: left/below it = worse off than not going to college."""
    if axis == "x":
        fig.add_vline(x=0, line_width=1, line_color=GREY)
    else:
        fig.add_hline(y=0, line_width=1, line_color=GREY)
    return fig
