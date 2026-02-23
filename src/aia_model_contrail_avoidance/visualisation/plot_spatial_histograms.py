"""Plotly visualisation of spatial histograms from flight statistics data."""

from __future__ import annotations

from typing import Any

import plotly.graph_objects as go


def plot_distance_flown_by_flight_level_histogram(
    flight_statistics: dict[str, Any],
    output_file: str = "distance_flown_by_flight_level_histogram",
) -> None:
    """Plot a histogram of distance flown by flight level from statistics data.

    Args:
        flight_statistics: Dictionary containing flight statistics data.
        output_file: name with which to save the output file.
    """
    # Extract histogram data
    histogram = flight_statistics["distance_flown_by_flight_level_histogram"]
    energy_forcing_histogram = flight_statistics["energy_forcing_by_flight_level_histogram"]

    fig = go.Figure()
    # make both traces visible by shifgting the bars slightly to the left and right
    fig.add_trace(
        go.Bar(
            x=list(histogram.keys()),
            y=list(histogram.values()),
            marker_color="#85B09A",  # green
            marker_line_color="#85B09A",
            marker_line_width=0.5,
            name="Distance Flown",
            offsetgroup=0,
            yaxis="y",
        ),
    )
    fig.add_trace(
        go.Bar(
            x=list(energy_forcing_histogram.keys()),
            y=list(energy_forcing_histogram.values()),
            marker_color="#FF6F61",  # cherry red
            marker_line_color="#FF6F61",
            marker_line_width=0.5,
            name="Energy Forcing",
            offsetgroup=1,
            yaxis="y2",
        ),
    )

    fig.update_layout(
        bargap=0.1,
        barmode="group",
        modebar_remove=[
            "zoom",
            "pan",
            "select",
            "lasso",
            "zoomIn",
            "zoomOut",
            "autoScale",
            "resetScale",
        ],
        plot_bgcolor="white",
        paper_bgcolor="white",
        title_text="Distance Flown and Energy Forcing by Flight Level",
        height=800,
        yaxis={
            "title": "Distance Flown (meters)",
            "showline": True,
            "linecolor": "black",
            "gridcolor": "lightgray",
            "mirror": True,
        },
        yaxis2={
            "title": "Energy Forcing (W/m²)",
            "overlaying": "y",
            "side": "right",
            "showline": True,
            "linecolor": "black",
            "gridcolor": "lightgray",
            "mirror": True,
        },
    )

    fig.update_xaxes(showline=True, linecolor="black", gridcolor="lightgray", mirror=True)
    fig.add_annotation(
        text=f"Total Distance Flown: {sum(histogram.values()):,.2f} meters",
        xref="paper",
        yref="paper",
        x=0.05,
        y=0.98,
        showarrow=False,
        bgcolor="white",
        bordercolor="black",
        borderwidth=1,
    )
    fig.add_annotation(
        text=f"Total Energy Forcing: {sum(energy_forcing_histogram.values()):,.2f} W/m^2",
        xref="paper",
        yref="paper",
        x=0.05,
        y=0.94,
        showarrow=False,
        bgcolor="white",
        bordercolor="black",
        borderwidth=1,
    )
    fig.write_html(
        f"results/plots/{output_file}.html",
        config={"displaylogo": False},
        full_html=False,
        include_plotlyjs="cdn",
    )
