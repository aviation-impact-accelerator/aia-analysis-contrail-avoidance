"""Identify flights contributing to warming."""  # noqa: INP001

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from aia_model_contrail_avoidance.core_model.airports import airport_name_from_icao_code


def top_ten_warming_flights(
    flight_data: pl.DataFrame, *, sort_by_total_energy_forcing: bool = False
) -> pl.DataFrame:
    """Identify the top ten flights contributing to warming.

    Args:
        flight_data (pl.DataFrame): DataFrame containing flight data with energy forcing information.
        sort_by_total_energy_forcing (bool): Whether to sort by total energy forcing or average
        energy forcing.

    Returns:
        pl.DataFrame: DataFrame containing the top ten flights contributing to warming.
    """
    # Calculate the total warming contribution for each flight
    # group by departure arrival pair and average the energy forcing contributions
    print("columns in flight data:", flight_data.columns)
    # ensure there sre no nan values in the total_energy_forcing column
    print(
        "removed %d rows with NaN values in total_energy_forcing",
        flight_data.height - flight_data.filter(pl.col("total_energy_forcing").is_not_nan()).height,
    )

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
    return top_flights.select(
        [
            "departure_airport_name",
            "arrival_airport_name",
            "average_energy_forcing",
            "total_energy_forcing_sum",
            "number_of_flights",
            "number_of_flights_forming_contrails",
        ]
    )


ADS_B_ANALYSIS_DIR = Path("~/ads_b_analysis").expanduser()
FLIGHTS_INFO_WITH_EF_DIR = ADS_B_ANALYSIS_DIR / "ads_b_flights_info_with_ef"

if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # load all flight data with energy forcing information
    parquet_files = sorted(FLIGHTS_INFO_WITH_EF_DIR.glob("*.parquet"))
    # pick first 7 days
    parquet_files = parquet_files[:7]
    all_flight_data = pl.DataFrame()
    for file_path in parquet_files:
        logger.info("Loading flight data from file: %s", file_path.name)
        flight_data = pl.read_parquet(file_path)
        all_flight_data = all_flight_data.vstack(flight_data)

    top_flights = top_ten_warming_flights(all_flight_data, sort_by_total_energy_forcing=True)
    print(top_flights)
    # save the top flights to a csv file
    output_file = "results/top_warming_flights.csv"
    top_flights.write_csv(output_file)
