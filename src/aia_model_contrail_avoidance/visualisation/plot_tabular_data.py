"""Module for visualisation of modelling results in tabular format."""

from __future__ import annotations

import logging
from pathlib import Path

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
