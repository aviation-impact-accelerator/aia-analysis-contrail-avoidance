"""Calculate energy forcing for flight data using the UK ADS-B January environment."""  # noqa: INP001

from __future__ import annotations

import logging
import time
from pathlib import Path

import polars as pl

from aia_model_contrail_avoidance.core_model.airspace import (
    ENVIRONMENTAL_BOUNDS_UK_AIRSPACE,
    find_uk_airspace_of_flight_segment,
)
from aia_model_contrail_avoidance.core_model.environment import (
    calculate_total_energy_forcing,
    create_grid_environment,
    run_flight_data_through_environment,
)
from aia_model_contrail_avoidance.flight_data_processing import TemporalFlightSubset

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BOOL_REMOVE_DATAPOINTS_OUTSIDE_UK_ENVIRONMENT = False


def add_energy_forcing_to_flight_info_database(
    flight_dataframe_with_ef: pl.DataFrame,
    flight_info_file_path: str,
    save_flights_info_with_ef_dir: str,
) -> None:
    """Add energy forcing information to the flight information database.

    Args:
        flight_dataframe_with_ef: Polars DataFrame containing flight data with energy forcing
            information.
        flight_info_file_path: Path to the existing flight information parquet file.
        save_flights_info_with_ef_dir: Directory to save the flight information with energy forcing
            as a parquet file.
    """
    # Calculate total energy forcing for each unique flight
    unique_flight_ids = flight_dataframe_with_ef["flight_id"].unique().to_list()
    total_ef_list = calculate_total_energy_forcing(unique_flight_ids, flight_dataframe_with_ef)

    # Create a summary dataframe with flight information and total energy forcing
    energy_forcing_per_flight = pl.DataFrame(
        {"flight_id": unique_flight_ids, "total_energy_forcing": total_ef_list}
    )
    # Load the existing flight information database (assuming it's a parquet file)
    flight_info_df = pl.read_parquet(flight_info_file_path)
    # Join the two dataframes on flight_id
    joined_df = flight_info_df.join(energy_forcing_per_flight, on="flight_id", how="inner")
    # Save the joined dataframe to a new parquet file
    joined_df.write_parquet(file=save_flights_info_with_ef_dir, mkdir=True)
    logger.info("flight info saved to path: %s", save_flights_info_with_ef_dir)


def calculate_energy_forcing_for_flights(
    flight_dataframe_path: str,
    flight_info_file_path: str,
    enviornment_filename: str,
    parquet_file_with_ef: str,
    save_flights_info_with_ef_dir: str,
) -> None:
    """Calculate energy forcing for flight data using the UK ADS-B January environment.

    Args:
        flight_dataframe_path: Path to the flight data parquet file.
        parquet_file_with_ef: Path to save the flight timestamps with energy forcing as a parquet
            file.
        enviornment_filename: Filename of the saved CocipGrid environment dataset to use for energy
            forcing calculation.
        flight_info_file_path: Path to the existing flight information parquet file.
        save_flights_info_with_ef_dir: Directory to save the flight information with energy forcing
            as a parquet file.
    """
    # Load the processed flight data from parquet file
    flight_dataframe = pl.read_parquet(flight_dataframe_path)

    logger.info("Loading environment data")
    environment = create_grid_environment(enviornment_filename)

    if BOOL_REMOVE_DATAPOINTS_OUTSIDE_UK_ENVIRONMENT:
        # Remove datapoints that are outside the environment (latitude and longitude bounds)
        flight_dataframe = flight_dataframe.filter(
            (pl.col("latitude") >= ENVIRONMENTAL_BOUNDS_UK_AIRSPACE["lat_min"])
            & (pl.col("latitude") <= ENVIRONMENTAL_BOUNDS_UK_AIRSPACE["lat_max"])
            & (pl.col("longitude") >= ENVIRONMENTAL_BOUNDS_UK_AIRSPACE["lon_min"])
            & (pl.col("longitude") <= ENVIRONMENTAL_BOUNDS_UK_AIRSPACE["lon_max"])
        )
    logger.info("Running flight data through environment")
    flight_data_with_ef = run_flight_data_through_environment(flight_dataframe, environment)
    logger.info("Processed %s data points", len(flight_data_with_ef))

    # adding airspace information to dataframe
    flight_data_with_ef = find_uk_airspace_of_flight_segment(flight_data_with_ef)
    logger.info("Added airspace information to flight data.")

    # Save the flight data with energy forcing to parquet
    flight_data_with_ef.write_parquet(file=parquet_file_with_ef, mkdir=True)
    logger.info("Flight data saved to path: %s", parquet_file_with_ef)

    # Add energy forcing information to the flight information database
    add_energy_forcing_to_flight_info_database(
        flight_data_with_ef, flight_info_file_path, save_flights_info_with_ef_dir
    )


def calculate_energy_forcing_from_filepath(  # noqa: PLR0913
    processed_flights_with_ids_dir: Path,
    processed_flights_info_dir: Path,
    save_flights_with_ef_dir: Path,
    save_flights_info_with_ef_dir: Path,
    temporal_flight_subset: TemporalFlightSubset,
    enviornment_filename: str,
) -> None:
    """Calculate energy forcing for processed ADS-B flight data.

    Args:
        processed_flights_with_ids_dir: Directory containing processed parquet files with flight data.
        processed_flights_info_dir: Directory containing processed parquet files with flight information.
        save_flights_with_ef_dir: Directory to save flights with energy forcing data.
        save_flights_info_with_ef_dir: Directory to save flight information with energy forcing.
        temporal_flight_subset: TemporalFlightSubset, the temporal subset of flights to process.
        enviornment_filename: Filename of the saved CocipGrid environment dataset to use for energy
            forcing calculations.
    """
    start = time.time()

    first_day = temporal_flight_subset.value[4]
    final_day = temporal_flight_subset.value[5]

    processed_paraquet_files = sorted(processed_flights_with_ids_dir.glob("*.parquet"))
    logger.info("Found %s files to process.", len(processed_paraquet_files))
    if len(processed_paraquet_files) < final_day:
        logger.info(
            "Generating Statistics from files %s to %s.",
            first_day,
            len(processed_paraquet_files),
        )
    else:
        logger.info("Generating Statistics from files %s to %s.", first_day, final_day)
    for file_path in processed_paraquet_files[first_day - 1 : final_day]:
        output_file_name = str(file_path.stem + "_with_ef")
        logger.info("Processing file: %s", output_file_name)
        # Find the matching info file with the same stem (day number)
        info_file_path = processed_flights_info_dir / f"{file_path.stem}_flight_info.parquet"
        if not info_file_path.exists():
            logger.warning("No matching info file found for %s, skipping.", file_path.name)
            continue
        info_output_file_name = str(info_file_path.stem + "_with_ef")
        logger.info("Processing info file: %s", info_output_file_name)
        calculate_energy_forcing_for_flights(
            flight_dataframe_path=str(file_path),
            flight_info_file_path=str(info_file_path),
            parquet_file_with_ef=str(save_flights_with_ef_dir / f"{output_file_name}.parquet"),
            save_flights_info_with_ef_dir=str(
                save_flights_info_with_ef_dir / f"{info_output_file_name}.parquet"
            ),
            enviornment_filename=enviornment_filename,
        )

    end = time.time()
    length = end - start
    logger.info("Energy forcing calculation completed in %.1f minutes.", round(length / 60, 1))


if __name__ == "__main__":
    ADS_B_ANALYSIS_DIR = Path("~/ads_b_analysis").expanduser()
    PROCESSED_FLIGHTS_WITH_IDS_DIR = ADS_B_ANALYSIS_DIR / "ads_b_processed_flights"
    PROCESSED_FLIGHTS_INFO_DIR = ADS_B_ANALYSIS_DIR / "ads_b_processed_flights_info"
    SAVE_FLIGHTS_WITH_EF_DIR = ADS_B_ANALYSIS_DIR / "ads_b_flights_with_ef"
    SAVE_FLIGHTS_INFO_WITH_EF_DIR = ADS_B_ANALYSIS_DIR / "ads_b_flights_info_with_ef"

    if not SAVE_FLIGHTS_WITH_EF_DIR.exists():
        SAVE_FLIGHTS_WITH_EF_DIR.mkdir(parents=True, exist_ok=True)
    if not SAVE_FLIGHTS_INFO_WITH_EF_DIR.exists():
        SAVE_FLIGHTS_INFO_WITH_EF_DIR.mkdir(parents=True, exist_ok=True)

    calculate_energy_forcing_from_filepath(
        PROCESSED_FLIGHTS_WITH_IDS_DIR,
        PROCESSED_FLIGHTS_INFO_DIR,
        SAVE_FLIGHTS_WITH_EF_DIR,
        SAVE_FLIGHTS_INFO_WITH_EF_DIR,
        temporal_flight_subset=TemporalFlightSubset.JANUARY,
        enviornment_filename="cocip_grid_global_week_1_2024",
    )
