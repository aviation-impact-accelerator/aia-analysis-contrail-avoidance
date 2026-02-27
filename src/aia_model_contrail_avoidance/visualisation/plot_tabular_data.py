"""Module for visualisation of modelling results in tabular format."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import plotly.graph_objects as go
import polars as pl

from aia_model_contrail_avoidance.core_model.airports import airport_name_from_icao_code

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def plot_top_ten_warming_flights(
    flights_info_with_ef_dir: Path,
    *,
    sort_by_total_energy_forcing: bool = True,
    output_file: str = "top_warming_flights",
) -> None:
    """Identify the top ten flights contributing to warming.

    Args:
        flight_data (pl.DataFrame): DataFrame containing flight data with energy forcing information.
        flights_info_with_ef_dir (Path): Directory containing flight info data with energy forcing values.
        sort_by_total_energy_forcing (bool): Whether to sort by total energy forcing or average
            energy forcing.
        output_file (str): Name of the output HTML file to save the plot.

    Returns:
        None
    """
    # load all flight data with energy forcing information
    parquet_files = sorted(flights_info_with_ef_dir.glob("*.parquet"))
    all_flight_data = pl.DataFrame()
    logger.info("Calculating Top 10 Warming Flights")
    for file_path in parquet_files:
        logger.info("Loading flight data from file: %s", file_path.name)
        flight_data = pl.read_parquet(file_path)
        all_flight_data = all_flight_data.vstack(flight_data)

    flight_data = all_flight_data

    flight_data = flight_data.filter(pl.col("total_energy_forcing").is_not_nan())
    # removed
    flight_data_per_pair = flight_data.group_by(
        ["departure_airport_icao", "arrival_airport_icao"]
    ).agg(
        pl.col("total_energy_forcing").sum().round(2).alias("total_energy_forcing_sum"),
        pl.col("total_energy_forcing").mean().round(2).alias("average_energy_forcing"),
        pl.len().alias("number_of_flights"),
        pl.col("total_energy_forcing")
        .filter(pl.col("total_energy_forcing") > 0)
        .count()
        .alias("number_of_flights_forming_contrails"),
    )
    if sort_by_total_energy_forcing:
        top_flights = flight_data_per_pair.sort("total_energy_forcing_sum", descending=True).head(
            10
        )
    else:
        top_flights = flight_data_per_pair.sort("average_energy_forcing", descending=True).head(10)

    # Map ICAO codes to airport names using to_series() and assign as new columns
    top_flights = top_flights.with_columns(
        pl.Series(
            "departure_airport_name",
            airport_name_from_icao_code(top_flights["departure_airport_icao"].to_list()),
        ),
        pl.Series(
            "arrival_airport_name",
            airport_name_from_icao_code(top_flights["arrival_airport_icao"].to_list()),
        ),
    )

    # Sort by average energy forcing and select the top ten
    row_colors = ["#e1eae5" if i % 2 == 0 else "#b7cdc2" for i in range(top_flights.height)]
    header_height = 42
    cell_height = 30

    fig = go.Figure(
        data=[
            go.Table(
                header={
                    "values": [
                        "<b>Departure Airport</b>",
                        "<b>Arrival Airport</b>",
                        "<b>Average Energy Forcing (W/m²)</b>",
                        "<b>Total Energy Forcing (W/m²)</b>",
                        "<b>Number of Flights</b>",
                        "<b>&#37; Forming Contrails</b>",
                    ],
                    "fill_color": "#6a9179",
                    "font_color": "Black",
                    "align": "center",
                    "height": header_height,
                },
                cells={
                    "values": [
                        top_flights["departure_airport_name"].to_list(),
                        top_flights["arrival_airport_name"].to_list(),
                        [f"{num:.2e}" for num in top_flights["average_energy_forcing"].to_list()],
                        [f"{num:.2e}" for num in top_flights["total_energy_forcing_sum"].to_list()],
                        top_flights["number_of_flights"].to_list(),
                        [
                            f"{num:.0f}%"
                            for num in (
                                top_flights["number_of_flights_forming_contrails"]
                                / top_flights["number_of_flights"]
                                * 100
                            ).to_list()
                        ],
                    ],
                    "fill_color": [row_colors] * 6,
                    "font_color": "Black",
                    "align": "center",
                    "height": cell_height,
                },
            )
        ]
    )
    fig.update_layout(margin={"l": 10, "r": 10, "t": 10, "b": 10})

    fig.write_html(
        f"results/plots/{output_file}.html",
        config={
            "displaylogo": False,
        },
        full_html=False,
        include_plotlyjs="cdn",
    )


def plot_co2e_gauge_chart(
    energy_forcing_statistics: dict[str, Any],
    output_file: str = "contrails_co2_comparison",
) -> None:
    """Plot a gauge chart showing the percentage of flights forming contrails.

    Args:
        energy_forcing_statistics: Dictionary containing energy forcing statistics.
        output_file: Name of the output HTML file to save the plot.

    """
    total_co2_emissions = energy_forcing_statistics["emissions"][
        "total_co2_emissions_from_fuel_burn"
    ]
    total_co2_equivalent_emissions_from_contrails = energy_forcing_statistics["emissions"][
        "total_co2_equivalent_emissions_from_contrails"
    ]
    gauge_max = max(total_co2_emissions, total_co2_equivalent_emissions_from_contrails)

    fig = go.Figure(
        go.Indicator(
            mode="gauge",
            title={"text": "CO2 Equivalent Emissions Comparison"},
            gauge={
                "axis": {"range": [0, gauge_max], "tickformat": ".1e"},
                "bar": {"color": "#FF6F61"},
                "steps": [
                    {"range": [0, total_co2_emissions], "color": "#6a9179"},
                    {
                        "range": [
                            total_co2_emissions,
                            total_co2_equivalent_emissions_from_contrails,
                        ],
                        "color": "#FF6F61",
                    },  # Coral for contrails
                ],
                "threshold": {
                    "line": {"color": "#2E7D32", "width": 4},
                    "thickness": 1,
                    "value": total_co2_emissions,
                },
            },
        )
    )
    fig.add_annotation(
        x=0.5,
        y=0.15,
        xref="paper",
        yref="paper",
        showarrow=False,
        text=f"<span style='color:#6a9179'>■</span> CO2 from fuel: {total_co2_emissions:.2e} &nbsp; "
        f"<span style='color:#FF6F61'>■</span> CO2e from contrails: {total_co2_equivalent_emissions_from_contrails:.2e}",
        font={"size": 15},
    )
    fig.update_layout(margin={"l": 60, "r": 60, "t": 20, "b": 20})
    fig.write_html(
        f"results/plots/{output_file}.html",
        config={
            "displaylogo": False,
        },
        full_html=False,
        include_plotlyjs="cdn",
    )


def plot_percentage_warming_flights(
    energy_forcing_statistics: dict[str, Any],
    output_file: str = "percentage_warming_flights_table",
) -> None:
    """Plot a table showing the percentage of flights contributing to warming.

    Args:
        energy_forcing_statistics: Dictionary containing energy forcing statistics.
        output_file: Name of the output HTML file to save the plot.

    """
    total_number_of_flights = energy_forcing_statistics["number_of_flights"]["total"]
    flights_for_80_percent = energy_forcing_statistics["cumulative_energy_forcing_per_flight"][
        "number_of_flights_for_80_percent_ef"
    ]
    flights_for_50_percent = energy_forcing_statistics["cumulative_energy_forcing_per_flight"][
        "number_of_flights_for_50_percent_ef"
    ]
    flights_for_20_percent = energy_forcing_statistics["cumulative_energy_forcing_per_flight"][
        "number_of_flights_for_20_percent_ef"
    ]

    percent_flights_for_80_percent = (
        f"{(flights_for_80_percent / total_number_of_flights) * 100:.2f}%"
    )
    percent_flights_for_50_percent = (
        f"{(flights_for_50_percent / total_number_of_flights) * 100:.2f}%"
    )
    percent_flights_for_20_percent = (
        f"{(flights_for_20_percent / total_number_of_flights) * 100:.2f}%"
    )
    row_colors = ["#e1eae5" if i % 2 == 0 else "#b7cdc2" for i in range(4)]
    header_height = 42
    cell_height = 30
    fig = go.Figure(
        data=[
            go.Table(
                header={
                    "values": [
                        "<b>X&#37; of Flights</b>",
                        "<b>cause X&#37; of Energy Forcing</b>",
                    ],
                    "fill_color": "#6a9179",
                    "font_color": "Black",
                    "align": "center",
                    "height": header_height,
                },
                cells={
                    "values": [
                        [
                            percent_flights_for_20_percent,
                            percent_flights_for_50_percent,
                            percent_flights_for_80_percent,
                        ],
                        ["20%", "50%", "80%"],
                    ],
                    "fill_color": [row_colors] * 6,
                    "font_color": "Black",
                    "align": "center",
                    "height": cell_height,
                },
            )
        ]
    )
    fig.update_layout(margin={"l": 10, "r": 10, "t": 200, "b": 50})

    fig.write_html(
        f"results/plots/{output_file}.html",
        config={
            "displaylogo": False,
        },
        full_html=False,
        include_plotlyjs="cdn",
    )


if __name__ == "__main__":
    ADS_B_ANALYSIS_DIR = Path("~/ads_b_analysis").expanduser()
    FLIGHTS_INFO_WITH_EF_DIR = ADS_B_ANALYSIS_DIR / "ads_b_flights_info_with_ef"

    # load all flight data with energy forcing information
    parquet_files = sorted(FLIGHTS_INFO_WITH_EF_DIR.glob("*.parquet"))
    all_flight_data = pl.DataFrame()
    for file_path in parquet_files:
        logger.info("Loading flight data from file: %s", file_path.name)
        flight_data = pl.read_parquet(file_path)
        all_flight_data = all_flight_data.vstack(flight_data)

    output_file = "top_warming_flights"
    plot_top_ten_warming_flights(
        FLIGHTS_INFO_WITH_EF_DIR,
        sort_by_total_energy_forcing=True,
        output_file=output_file,
    )
    output_file = "contrails_co2_comparison"
    json_file_name = "results/energy_forcing_statistics_month_01_2024.json"
    with Path(json_file_name).open() as f:
        energy_forcing_statistics = json.load(f)
    plot_co2e_gauge_chart(
        energy_forcing_statistics=energy_forcing_statistics, output_file=output_file
    )

    plot_percentage_warming_flights(
        energy_forcing_statistics=energy_forcing_statistics,
        output_file="percentage_warming_flights_table",
    )
