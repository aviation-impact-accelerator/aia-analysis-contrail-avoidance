"""Plot pie chart of contrail formation as distance traveled and number of flights."""

from __future__ import annotations

import json
from pathlib import Path

from aia_model_contrail_avoidance.visualisation.plot_pie_charts import (
    plot_pie_chart_distance_forming_contrails,
    plot_pie_chart_number_of_flights_forming_contrails,
)


def plot_pie_chart_contrail_formation_from_json(
    json_file_path: Path,
) -> None:
    """Plot pie chart of contrail formation as distance traveled and number of flights.

    Args:
        json_file_path: Path to the JSON file containing energy forcing statistics
    """
    # Load the JSON file
    with Path(json_file_path).open("r") as f:
        flight_statistics = json.load(f)
    plot_pie_chart_distance_forming_contrails(
        flight_statistics=flight_statistics,
        output_file_name="pie_chart_distance_forming_contrails",
    )
    plot_pie_chart_number_of_flights_forming_contrails(
        flight_statistics=flight_statistics,
        output_file_name="pie_chart_number_of_flights_forming_contrails",
    )


if __name__ == "__main__":
    input_json_path = Path("results/energy_forcing_statistics_week_1_2024.json")
    plot_pie_chart_contrail_formation_from_json(
        input_json_path,
    )
