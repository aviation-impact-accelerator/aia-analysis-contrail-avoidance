"""Plot pie chart of domestic vs international flights."""  # noqa: INP001

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import plotly.express as px  # type: ignore[import-untyped]


def plot_domestic_international_flights_pie_chart_from_json(
    json_file_path: Path,
    output_file_name: str,
) -> None:
    """Plot histogram of energy forcing per flight with cumulative forcing analysis.

    Args:
        json_file_path: Path to the JSON file containing energy forcing statistics
        output_file_name: Name of the output file to save the plot (without extension)
    """
    # Load the JSON file
    with Path(json_file_path).open("r") as f:
        flight_statistics = json.load(f)
    plot_domestic_international_flights_pie_chart(
        flight_statistics=flight_statistics,
        output_file_name=output_file_name,
    )


def plot_domestic_international_flights_pie_chart(
    flight_statistics: dict[str, Any],
    output_file_name: str,
) -> None:
    """Plot pie chart of domestic vs international flights.

    Args:
        flight_statistics: Dictionary containing flight statistics
        output_file_name: Name of the output file to save the plot (without extension)
    """
    domestic_flights = flight_statistics["number_of_flights"]["regional"]
    international_flights = flight_statistics["number_of_flights"]["international"]

    fig = px.pie(
        names=["Domestic Flights", "International Flights"],
        values=[domestic_flights, international_flights],
        title="Domestic vs International Flights",
        color_discrete_sequence=["#6a9179", "#FF6F61"],
    )

    fig.write_html(
        f"results/plots/{output_file_name}.html",
        config={"displaylogo": False},
        full_html=False,
        include_plotlyjs="cdn",
    )


if __name__ == "__main__":
    input_json_path = Path("results/energy_forcing_statistics_week_1_2024.json")
    output_file_name = "domestic_international_flights_pie_chart"
    plot_domestic_international_flights_pie_chart_from_json(
        input_json_path,
        output_file_name,
    )
