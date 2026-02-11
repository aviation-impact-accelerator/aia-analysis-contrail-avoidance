"""Plot histogram of energy forcing per flight with cumulative forcing analysis."""

from __future__ import annotations

import json
from pathlib import Path

from aia_model_contrail_avoidance.visualisation.plotly_per_flight_histograms import (
    plot_energy_forcing_histogram,
)


def plot_energy_forcing_histogram_from_json(
    json_file: str | Path, output_file_cumulative: str | Path
) -> None:
    """Plot histogram of energy forcing per flight with cumulative forcing analysis.

    Args:
        json_file: Path to the JSON file containing energy forcing statistics
        output_file_cumulative: Path to save the output cumulative plot image
    """
    # Load the JSON file
    with Path(json_file).open() as f:
        energy_forcing_stats = json.load(f)

    plot_energy_forcing_histogram(
        energy_forcing_statistics=energy_forcing_stats,
        output_file_cumulative=output_file_cumulative,
    )


if __name__ == "__main__":
    input_json = "results/energy_forcing_statistics_week_1_2024.json"
    output_file_cumulative = "energy_forcing_cumulative_week_1_2024"
    plot_energy_forcing_histogram_from_json(input_json, output_file_cumulative)
