"""Plot pie chart of total distance traveled for domestic vs international flights."""  # noqa : INP001

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import plotly.express as px  # type: ignore[import-untyped]


def plot_pie_chart_distance_traveled_by_type(
    json_file_path: Path,
    output_file_name: str,
) -> None:
    """Plot pie chart of total distance traveled for domestic vs international flights.

    Args:
        json_file_path: Path to the JSON file containing energy forcing statistics
        output_file_name: Name of the output file to save the plot (without extension)
    """
    # Load the JSON file
    with Path(json_file_path).open("r") as f:
        flight_statistics = json.load(f)
    plot_pie_chart_distance_traveled_by_domestic_and_international_flights(
        flight_statistics=flight_statistics,
        output_file_name=output_file_name,
    )


def plot_pie_chart_distance_traveled_by_domestic_and_international_flights(
    flight_statistics: dict[str, Any],
    output_file_name: str,
) -> None:
    """Plot pie chart of distance traveled for domestic vs international flights.

    Args:
        flight_statistics: Dictionary containing flight statistics
        output_file_name: Name of the output file to save the plot (without extension)
    """
    domestic_flights = flight_statistics["flight_distance_by_airspace"]["uk_airspace_nm"]
    international_flights = flight_statistics["flight_distance_by_airspace"][
        "international_airspace_nm"
    ]

    fig = px.pie(
        names=["Domestic Flights", "International Flights"],
        values=[domestic_flights, international_flights],
        title="Distance Traveled: <br> Domestic vs International Flights",
        color_discrete_sequence=["#6a9179", "#FF6F61"],
    )
    fig.update_layout(
        legend={"yanchor": "bottom", "y": -1.5, "xanchor": "center", "x": 0.5, "orientation": "h"},
        title={
            "x": 0.5,
            "xanchor": "center",
            # decrease text size of the title
            "font": {"size": 13},
        },
    )

    fig.write_html(
        f"results/plots/{output_file_name}.html",
        config={"displaylogo": False},
        full_html=False,
        include_plotlyjs="cdn",
    )


if __name__ == "__main__":
    input_json_path = Path("results/energy_forcing_statistics_week_1_2024.json")
    output_file_name = "pie_chart_distance_traveled_by_domestic_and_international_flights"
    plot_pie_chart_distance_traveled_by_type(
        input_json_path,
        output_file_name,
    )
