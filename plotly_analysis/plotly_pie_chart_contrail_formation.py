"""Plot pie chart of contrail formation as distance traveled and number of flights."""  # noqa: INP001

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import plotly.express as px  # type: ignore[import-untyped]


def plot_pie_chart_contrail_formation(
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


def plot_pie_chart_distance_forming_contrails(
    flight_statistics: dict[str, Any],
    output_file_name: str,
) -> None:
    """Plot pie chart of distance traveled for domestic vs international flights.

    Args:
        flight_statistics: Dictionary containing flight statistics
        output_file_name: Name of the output file to save the plot (without extension)
    """
    distance_traveled = flight_statistics["flight_distance_by_airspace"]["total_nm"]
    distance_forming_contrails = flight_statistics["contrail_formation"][
        "distance_forming_contrails_nm"
    ]
    distance_not_forming_contrails = distance_traveled - distance_forming_contrails
    fig = px.pie(
        names=["Distance Forming Contrails", "Distance Not Forming Contrails"],
        values=[distance_forming_contrails, distance_not_forming_contrails],
        title="Distance Traveled: Forming vs Not Forming Contrails",
        color_discrete_sequence=["#6a9179", "#FF6F61"],
    )

    fig.write_html(
        f"results/plots/{output_file_name}.html",
        config={"displaylogo": False},
        full_html=False,
        include_plotlyjs="cdn",
    )


def plot_pie_chart_number_of_flights_forming_contrails(
    flight_statistics: dict[str, Any],
    output_file_name: str,
) -> None:
    """Plot pie chart of number of flights forming contrails for domestic vs international flights.

    Args:
        flight_statistics: Dictionary containing flight statistics
        output_file_name: Name of the output file to save the plot (without extension)
    """
    number_of_flights = flight_statistics["number_of_flights"]["regional"]
    number_of_flights_forming_contrails = flight_statistics["number_of_flights"]["international"]

    fig = px.pie(
        names=["Flights Forming Contrails", "Flights Not Forming Contrails"],
        values=[
            number_of_flights_forming_contrails,
            number_of_flights - number_of_flights_forming_contrails,
        ],
        title="Number of Flights Forming Contrails: Forming vs Not Forming Contrails",
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
    plot_pie_chart_contrail_formation(
        input_json_path,
    )
