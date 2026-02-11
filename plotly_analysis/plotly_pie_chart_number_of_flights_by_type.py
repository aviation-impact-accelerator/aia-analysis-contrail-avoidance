"""Plot pie chart of number of domestic vs international flights."""

from __future__ import annotations

import json
from pathlib import Path

from src.aia_model_contrail_avoidance.visualisation.plotly_pie_charts import (
    plot_pie_chart_number_of_flights_domestic_and_international,
)


def plot_pie_chart_number_of_flights_by_type(
    json_file_path: Path,
    output_file_name: str,
) -> None:
    """Plot pie chart of number of domestic vs international flights.

    Args:
        json_file_path: Path to the JSON file containing energy forcing statistics
        output_file_name: Name of the output file to save the plot (without extension)
    """
    # Load the JSON file
    with Path(json_file_path).open("r") as f:
        flight_statistics = json.load(f)
    plot_pie_chart_number_of_flights_domestic_and_international(
        flight_statistics=flight_statistics,
        output_file_name=output_file_name,
    )


if __name__ == "__main__":
    input_json_path = Path("results/energy_forcing_statistics_week_1_2024.json")
    output_file_name = "pie_chart_number_of_domestic_and_international_flights"
    plot_pie_chart_number_of_flights_by_type(
        input_json_path,
        output_file_name,
    )
