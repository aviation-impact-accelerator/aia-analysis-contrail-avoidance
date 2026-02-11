"""Plotly visualisation of spatial histograms from flight statistics data."""

from __future__ import annotations

from typing import Any

import plotly.express as px  # type: ignore[import-untyped]


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

    fig = px.bar(
        x=list(histogram.keys()),
        y=list(histogram.values()),
        labels={"x": "Flight Level", "y": "Distance Flown (meters)"},
        title="Distance Flown by Flight Level Histogram",
    )

    fig.update_layout(
        bargap=0.1,
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
    )

    fig.update_traces(
        marker_color="rgba(133, 176, 154, 1)", marker_line_color="black", marker_line_width=1
    )
    fig.update_xaxes(showline=True, linecolor="black", gridcolor="lightgray", mirror=True)
    fig.update_yaxes(showline=True, linecolor="black", gridcolor="lightgray", mirror=True)

    fig.add_annotation(
        text=f"Total Distance Flown: {sum(histogram.values()):,.2f} meters",
        xref="paper",
        yref="paper",  # Use relative coordinates (0-1)
        x=0.05,
        y=0.95,  # Top left corner
        showarrow=False,
        bgcolor="white",
        bordercolor="black",
        borderwidth=1,
    )
    fig.update_traces(marker_line_color="black", marker_line_width=1)
    fig.write_html(
        f"results/plots/{output_file}.html",
        config={"displaylogo": False},
        full_html=False,
        include_plotlyjs="cdn",
    )
