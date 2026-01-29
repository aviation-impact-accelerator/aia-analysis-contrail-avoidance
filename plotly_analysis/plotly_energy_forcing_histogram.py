"""Plot histogram of energy forcing per flight with cumulative forcing analysis."""  # noqa: INP001

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import matplotlib.pyplot as plt
import numpy as np
import plotly.express as px  # type: ignore[import-untyped]
import polars as pl

if TYPE_CHECKING:
    from pathlib import Path


def plot_energy_forcing_histogram(json_file: str | Path, output_file: str | Path) -> None:
    """Plot histogram of energy forcing per flight with cumulative forcing analysis.

    Args:
        json_file: Path to the JSON file containing energy forcing statistics
        output_file: Path to save the output plot image
    """
    # Load the JSON file
    with open(f"results/{json_file}.json") as f:  # noqa: PTH123
        stats = json.load(f)

    # Load the full flight data with energy forcing
    flight_data = pl.read_parquet(
        "data/contrails_model_data/2024_01_01_sample_processed_with_interpolation_with_ef.parquet"
    )

    # Calculate total energy forcing per flight
    flight_ef_summary = (
        flight_data.group_by("flight_id")
        .agg(pl.col("ef").sum().alias("total_ef"))
        .sort("total_ef", descending=True)
    )

    # Calculate cumulative energy forcing
    flight_ef_summary = flight_ef_summary.with_columns(
        pl.col("total_ef").cum_sum().alias("cumulative_ef")
    )
    total_energy_forcing = flight_ef_summary["total_ef"].sum()

    # Find how many flights contribute to 80%, 50%, and 20% of total forcing
    ef_80_percent = total_energy_forcing * 0.8
    ef_50_percent = total_energy_forcing * 0.5
    ef_20_percent = total_energy_forcing * 0.2

    flights_for_80_percent = (flight_ef_summary["cumulative_ef"] <= ef_80_percent).sum()
    flights_for_50_percent = (flight_ef_summary["cumulative_ef"] <= ef_50_percent).sum()
    flights_for_20_percent = (flight_ef_summary["cumulative_ef"] <= ef_20_percent).sum()

    # Extract histogram data for plotting
    histogram = stats["energy_forcing_per_flight"]["histogram"]
    bin_edges = np.array(histogram["bin_edges"])
    counts = np.array(histogram["counts"])

    total_flights = len(flight_ef_summary)

    # Create figure with two subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))

    # --- Top subplot: Regular histogram ---
    fig1 = px.bar(
        x=(bin_edges[:-1] + bin_edges[1:]) / 2,
        y=counts,
        labels={"x": "Total Energy Forcing per Flight (J)", "y": "Number of Flights"},
        title="Distribution of Energy Forcing per Flight",
    )

    fig1.update_layout(
        bargap=0.05,
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
        xaxis={"range": [-bin_edges[2], bin_edges[-1] + -bin_edges[2]]},
        plot_bgcolor="white",
        paper_bgcolor="white",
    )

    fig1.update_traces(marker_line_color="black", marker_line_width=1)
    fig1.update_xaxes(showline=True, linecolor="black", gridcolor="lightgray")
    fig1.update_yaxes(showline=True, linecolor="black", gridcolor="lightgray")

    fig1.add_annotation(
        text=(
            f"Total flights: {total_flights:,}<br>"
            f"Total EF: {total_energy_forcing:.2e} J<br><br>"
            f"20% of forcing: {flights_for_20_percent:,} flights ({flights_for_20_percent / total_flights * 100:.1f}%)<br>"
            f"50% of forcing: {flights_for_50_percent:,} flights ({flights_for_50_percent / total_flights * 100:.1f}%)<br>"
            f"80% of forcing: {flights_for_80_percent:,} flights ({flights_for_80_percent / total_flights * 100:.1f}%)"
        ),
        xref="paper",
        yref="paper",  # Use relative coordinates (0-1)
        x=0.95,
        y=0.95,  # Top left corner
        showarrow=False,
        bgcolor="wheat",
        bordercolor="black",
        borderwidth=1,
    )

    # --- Bottom subplot: Cumulative energy forcing ---
    flight_indices = np.arange(1, len(flight_ef_summary) + 1)
    cumulative_ef_percentage = (flight_ef_summary["cumulative_ef"] / total_energy_forcing) * 100

    ax2.plot(flight_indices, cumulative_ef_percentage, linewidth=2, color="blue")
    ax2.axhline(
        80,
        color="green",
        linestyle="--",
        linewidth=2,
        label=f"80% of forcing ({flights_for_80_percent} flights)",
    )
    ax2.axhline(
        50,
        color="orange",
        linestyle="--",
        linewidth=2,
        label=f"50% of forcing ({flights_for_50_percent} flights)",
    )
    ax2.axhline(
        20,
        color="red",
        linestyle="--",
        linewidth=2,
        label=f"20% of forcing ({flights_for_20_percent} flights)",
    )

    ax2.axvline(flights_for_80_percent, color="green", linestyle=":", linewidth=1.5, alpha=0.7)
    ax2.axvline(flights_for_50_percent, color="orange", linestyle=":", linewidth=1.5, alpha=0.7)
    ax2.axvline(flights_for_20_percent, color="red", linestyle=":", linewidth=1.5, alpha=0.7)

    ax2.set_xlabel("Number of Flights (sorted by EF, highest first)", fontsize=12)
    ax2.set_ylabel("Cumulative Energy Forcing (%)", fontsize=12)
    ax2.set_title("Cumulative Energy Forcing Contribution", fontsize=14, fontweight="bold")
    ax2.legend(fontsize=10, loc="lower right")
    ax2.grid(alpha=0.3)
    ax2.set_xlim(0, total_flights)
    ax2.set_ylim(0, 105)

    fig.write_html(
        f"plotly_analysis/plotly_plots/{output_file}.html", full_html=False, include_plotlyjs="cdn"
    )


if __name__ == "__main__":
    input_json = "energy_forcing_statistics"
    output_file = "energy_forcing_per_flight_histogram"
    plot_energy_forcing_histogram(input_json, output_file)
