"""Calculate energy forcing for flight data using the UK ADS-B January environment."""  # noqa: INP001

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from aia_model_contrail_avoidance.core_model.airspace import (
    find_uk_airspace_of_flight_segment,
)
from aia_model_contrail_avoidance.core_model.environment import (
    calculate_total_energy_forcing,
    create_grid_environment,
    run_flight_data_through_environment,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BOOL_REMOVE_DATAPOINTS_OUTSIDE_UK_ENVIRONMENT = False


def add_energy_forcing_to_flight_info_database(
    flight_dataframe_with_ef: pl.DataFrame,
    flight_info_with_ef_file_path: str,
) -> None:
    """Add energy forcing information to the flight information database.

    Args:
        flight_dataframe_with_ef: Polars DataFrame containing flight data with energy forcing
            information.
        flight_info_with_ef_file_path: Path to save the flight information with energy forcing as a
            parquet file.
    """
    # Calculate total energy forcing for each unique flight
    unique_flight_ids = flight_dataframe_with_ef["flight_id"].unique().to_list()
    total_ef_list = calculate_total_energy_forcing(unique_flight_ids, flight_dataframe_with_ef)

    # Create a summary dataframe with flight information and total energy forcing
    energy_forcing_per_flight = pl.DataFrame(
        {"flight_id": unique_flight_ids, "total_energy_forcing": total_ef_list}
    )
    # Load the existing flight information database (assuming it's a parquet file)
    flight_info_df = pl.read_parquet(flight_info_with_ef_file_path)
    # Join the two dataframes on flight_id
    joined_df = flight_info_df.join(energy_forcing_per_flight, on="flight_id", how="inner")
    # Save the joined dataframe to a new parquet file
    joined_df.write_parquet(file=flight_info_with_ef_file_path, mkdir=True)


def calculate_energy_forcing_for_flights(
    flight_dataframe_path: str,
    parquet_file_with_ef: str,
    flight_info_with_ef_file_path: str,
) -> None:
    """Calculate energy forcing for flight data using the UK ADS-B January environment.

    Args:
        flight_dataframe_path: Path to the flight data parquet file.
        parquet_file_with_ef: Path to save the flight timestamps with energy forcing as a parquet
            file.
        flight_info_with_ef_file_path: Path to save the flight information with energy forcing as a
            parquet file.
    """
    # Load the processed flight data from parquet file
    flight_dataframe = pl.read_parquet(flight_dataframe_path)

    logger.info("Loading environment data")
    environment = create_grid_environment("cocip_grid_global_week_1_2024")

    if BOOL_REMOVE_DATAPOINTS_OUTSIDE_UK_ENVIRONMENT:
        # environmental bounds for UK ADS-B January environment
        environmental_bounds = {
            "lat_min": 49.0,
            "lat_max": 62.0,
            "lon_min": -8.0,
            "lon_max": 3.0,
        }
        # Remove datapoints that are outside the environment (latitude and longitude bounds)
        flight_dataframe = flight_dataframe.filter(
            (pl.col("latitude") >= environmental_bounds["lat_min"])
            & (pl.col("latitude") <= environmental_bounds["lat_max"])
            & (pl.col("longitude") >= environmental_bounds["lon_min"])
            & (pl.col("longitude") <= environmental_bounds["lon_max"])
        )
    logger.info("Running flight data through environment")
    flight_data_with_ef = run_flight_data_through_environment(flight_dataframe, environment)
    logger.info("Processed %s data points", len(flight_data_with_ef))

    # adding airspace information to dataframe
    flight_data_with_ef = find_uk_airspace_of_flight_segment(flight_data_with_ef)
    logger.info("Added airspace information to flight data.")

    # Save the flight data with energy forcing to parquet
    flight_data_with_ef.write_parquet(file=parquet_file_with_ef, mkdir=True)

    # Add energy forcing information to the flight information database
    add_energy_forcing_to_flight_info_database(flight_data_with_ef, flight_info_with_ef_file_path)


if __name__ == "__main__":
    FLIGHTS_WITH_IDS_DIR = Path("~/ads_b_processed_flights").expanduser()
    SAVE_FLIGHTS_WITH_EF_DIR = Path("~/ads_b_flights_with_ef").expanduser()
    SAVE_FLIGHTS_INFO_WITH_EF_DIR = Path("~/ads_b_processed_flights_info").expanduser()

    if not SAVE_FLIGHTS_WITH_EF_DIR.exists():
        SAVE_FLIGHTS_WITH_EF_DIR.mkdir(parents=True, exist_ok=True)
    if not SAVE_FLIGHTS_INFO_WITH_EF_DIR.exists():
        SAVE_FLIGHTS_INFO_WITH_EF_DIR.mkdir(parents=True, exist_ok=True)

    parquet_file_paths = sorted(FLIGHTS_WITH_IDS_DIR.glob("UK_flights_day_00*.parquet"))
    logger.info("Found %s files in directory.", len(parquet_file_paths))

    for file_path in parquet_file_paths:
        output_file_name = str(file_path.stem + "_with_ef")
        logger.info("Processing file: %s", output_file_name)

        calculate_energy_forcing_for_flights(
            flight_dataframe_path=str(file_path),
            parquet_file_with_ef=str(SAVE_FLIGHTS_WITH_EF_DIR / f"{output_file_name}.parquet"),
            flight_info_with_ef_file_path=str(
                SAVE_FLIGHTS_INFO_WITH_EF_DIR / f"{file_path.stem}_flight_info.parquet"
            ),
        )
