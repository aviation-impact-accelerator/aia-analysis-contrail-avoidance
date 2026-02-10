"""Plot pie chart of domestic vs international flights."""  # noqa: INP001

from __future__ import annotations

import json
from pathlib import Path

import plotly.express as px  # type: ignore[import-untyped]


def plot_domestic_international_flights_pie_chart(
    json_file: str | Path,
    output_file: str | Path,
) -> None:
    """Plot histogram of energy forcing per flight with cumulative forcing analysis.

    Args:
        json_file: Path to the JSON file containing energy forcing statistics
        output_file: Path to save the output histogram plot image
    """
    # Load the JSON file
    with Path(json_file).open("r") as f:
        stats = json.load(f)

    flight_data = stats["flight_data"]
    domestic_flights = flight_data["number_of_regional_flights"]
    international_flights = flight_data["number_of_international_flights"]

    fig = px.pie(
        names=["Domestic Flights", "International Flights"],
        values=[domestic_flights, international_flights],
        title="Domestic vs International Flights",
        color_discrete_sequence=["#6a9179", "#FF6F61"],
    )

    fig.write_html(
        f"results/plots/{output_file}.html",
        config={"displaylogo": False},
        full_html=False,
        include_plotlyjs="cdn",
    )


if __name__ == "__main__":
    input_json = "2024_01_01_sample_stats_processed"
    output_file = "domestic_international_flights_pie_chart"
    plot_domestic_international_flights_pie_chart(
        input_json,
        output_file,
    )
