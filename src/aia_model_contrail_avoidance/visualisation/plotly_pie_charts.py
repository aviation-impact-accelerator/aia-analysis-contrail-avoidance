"""Module for visualisation of modelling results using Plotly pie charts."""

from __future__ import annotations

from typing import Any

import plotly.express as px  # type: ignore[import-untyped]


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


def plot_pie_chart_number_of_flights_domestic_and_international(
    flight_statistics: dict[str, Any],
    output_file_name: str,
) -> None:
    """Plot pie chart of number of domestic vs international flights.

    Args:
        flight_statistics: Dictionary containing flight statistics
        output_file_name: Name of the output file to save the plot (without extension)
    """
    domestic_flights = flight_statistics["number_of_flights"]["regional"]
    international_flights = flight_statistics["number_of_flights"]["international"]

    fig = px.pie(
        names=["Domestic Flights", "International Flights"],
        values=[domestic_flights, international_flights],
        title="Number of Domestic <br> vs International Flights",
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
        # break into two lines to avoid long title
        title="Distance Traveled: <br> Forming vs Not Forming Contrails",
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


def plot_pie_chart_number_of_flights_forming_contrails(
    flight_statistics: dict[str, Any],
    output_file_name: str,
) -> None:
    """Plot pie chart of number of flights forming contrails for domestic vs international flights.

    Args:
        flight_statistics: Dictionary containing flight statistics
        output_file_name: Name of the output file to save the plot (without extension)
    """
    number_of_flights = flight_statistics["number_of_flights"]["total"]
    number_of_flights_forming_contrails = flight_statistics["contrail_formation"][
        "flights_forming_contrails"
    ]

    fig = px.pie(
        names=["Flights Forming Contrails", "Flights Not Forming Contrails"],
        values=[
            number_of_flights_forming_contrails,
            number_of_flights - number_of_flights_forming_contrails,
        ],
        title="Number of Flights Forming Contrails: <br> Forming vs Not Forming Contrails",
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
