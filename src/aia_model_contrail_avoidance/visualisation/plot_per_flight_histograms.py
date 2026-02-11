"""Plotly functions to visualise histograms of energy forcing per flight and cumulative forcing analysis."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import matplotlib.pyplot as plt
import numpy as np
import plotly.express as px

if TYPE_CHECKING:
    from pathlib import Path


def plot_energy_forcing_histogram(
    energy_forcing_statistics: dict[str, Any],
    output_file_cumulative: str | Path,
) -> None:
    """Plot histogram of energy forcing per flight with cumulative forcing analysis.

    Args:
        energy_forcing_statistics: Dictionary containing energy forcing statistics
        output_file_cumulative: Path to save the output cumulative plot image
    """
    # Extract histogram data for plotting
    histogram = [
        energy_forcing_statistics["cumulative_energy_forcing_per_flight"]["histogram"].get(
            str(i), 0
        )
        for i in range(100 + 1)
    ]

    # read each value in histogram and split into two lists: bin_edges and counts

    flights_for_80_percent = energy_forcing_statistics["cumulative_energy_forcing_per_flight"][
        "number_of_flights_for_80_percent_ef"
    ]
    flights_for_50_percent = energy_forcing_statistics["cumulative_energy_forcing_per_flight"][
        "number_of_flights_for_50_percent_ef"
    ]
    flights_for_20_percent = energy_forcing_statistics["cumulative_energy_forcing_per_flight"][
        "number_of_flights_for_20_percent_ef"
    ]
    # Create figure with two subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))

    # Cumulative energy forcing plot (Plotly)
    cumulative_ef_percentage = np.array(histogram)

    fig2 = px.line(
        x=np.arange(len(histogram)),
        y=cumulative_ef_percentage,
        labels={
            "x": "Percentage of Flights (sorted by energy forcing contribution)",
            "y": "Cumulative Energy Forcing (%)",
        },
        title="Cumulative Energy Forcing Contribution",
    )
    fig2.update_traces(marker_line_color="black", marker_line_width=1)
    fig2.update_xaxes(showline=True, linecolor="black", gridcolor="lightgray", mirror=True)
    fig2.update_yaxes(showline=True, linecolor="black", gridcolor="lightgray", mirror=True)

    fig2.add_hline(
        80,
        line_dash="dash",
        line_color="green",
        annotation_text=f"80% of forcing ({flights_for_80_percent} flights)",
        annotation_position="bottom right",
    )

    fig2.add_hline(
        50,
        line_dash="dash",
        line_color="orange",
        annotation_text=f"50% of forcing ({flights_for_50_percent} flights)",
        annotation_position="bottom right",
    )

    fig2.add_hline(
        20,
        line_dash="dash",
        line_color="red",
        annotation_text=f"20% of forcing ({flights_for_20_percent} flights)",
        annotation_position="bottom right",
    )

    fig2.update_layout(
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
        xaxis={"range": [0, 100]},
        yaxis={"range": [0, 105]},
        plot_bgcolor="white",
        paper_bgcolor="white",
    )

    fig2.add_vline(flights_for_80_percent, line_dash="dot", line_color="green", opacity=0.7)
    fig2.add_vline(flights_for_50_percent, line_dash="dot", line_color="orange", opacity=0.7)
    fig2.add_vline(flights_for_20_percent, line_dash="dot", line_color="red", opacity=0.7)

    fig2.write_html(
        f"results/plots/{output_file_cumulative}.html",
        config={"displaylogo": False},
        full_html=False,
        include_plotlyjs="cdn",
    )
