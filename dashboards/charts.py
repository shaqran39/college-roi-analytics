"""Shared chart styling so every chart on every page looks the same.

Colours follow one rule each: blue = the one series being shown, red = worse off than not going
to college (below zero), grey = context. Thin marks, hairline grid, value labels only where useful.
"""

import plotly.graph_objects as go

BLUE = "#2a78d6"        # main series
BLUE_LIGHT = "#86b6ef"  # "without" side of a with/without comparison
BLUE_DARK = "#184f95"   # "with" side of a with/without comparison
RED = "#e34948"         # below zero: worse off than not going to college
ORANGE = "#eb6834"      # the negative side of a +/- measure (e.g. a negative correlation); always with a +/- label
GREY = "#898781"        # context marks, connectors, zero line
GRID = "rgba(137,135,129,0.25)"
FONT_SIZE = 15          # all chart text: big enough for a laptop and a phone


def style(fig: go.Figure, height: int, money_axis: str | None = "x") -> go.Figure:
    """Apply the shared look. money_axis = which axis shows US$ amounts ('x', 'y' or None)."""
    fig.update_layout(
        height=height,
        # top margin keeps the hover toolbar clear of the first bar's value label
        margin=dict(l=8, r=24, t=44, b=8),
        showlegend=False,
        font=dict(size=FONT_SIZE),
        legend=dict(font_size=FONT_SIZE),
        hoverlabel=dict(font_size=FONT_SIZE),
        bargap=0.35,
    )
    for axis in (fig.update_xaxes, fig.update_yaxes):
        axis(showgrid=False, zeroline=False, linecolor=GRID, tickfont_size=FONT_SIZE, title_font_size=FONT_SIZE + 1)
    fig.update_traces(textfont_size=FONT_SIZE, selector=dict(type="bar"))
    fig.update_annotations(font_size=FONT_SIZE)
    if money_axis:
        update = fig.update_xaxes if money_axis == "x" else fig.update_yaxes
        # '~s' gives 200k / 1.2M; the axis title says it's US$
        update(tickformat="~s", showgrid=True, gridcolor=GRID, gridwidth=1)
    return fig


def zero_line(fig: go.Figure, axis: str = "x") -> go.Figure:
    """A solid hairline at 0: left/below it = worse off than not going to college."""
    if axis == "x":
        fig.add_vline(x=0, line_width=1, line_color=GREY)
    else:
        fig.add_hline(y=0, line_width=1, line_color=GREY)
    return fig
