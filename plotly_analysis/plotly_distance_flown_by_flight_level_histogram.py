"""Plot histogram of distance traveled in segment from JSON statistics file."""

from __future__ import annotations

import json
from pathlib import Path

from aia_model_contrail_avoidance.visualisation.plotly_spatial_histograms import (
    plot_distance_flown_by_flight_level_histogram,
)


def plot_distance_flown_by_flight_level_histogram_from_json(
    stats_file_path: Path,
    output_file: str = "distance_flown_by_flight_level_histogram",
) -> None:
    """Plot a histogram of distance flown by flight level from a JSON statistics file.

    Args:
        stats_file_path: Path to the JSON statistics file.
        output_file: name with which to save the output file.
    """
    with stats_file_path.open() as f:
        flight_statistics = json.load(f)
        plot_distance_flown_by_flight_level_histogram(flight_statistics, output_file)


if __name__ == "__main__":
    plot_distance_flown_by_flight_level_histogram_from_json(
        stats_file_path=Path("results/energy_forcing_statistics_week_1_2024.json"),
        output_file="distance_flown_by_flight_level_histogram",
    )
