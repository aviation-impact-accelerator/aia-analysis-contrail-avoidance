"""Process ADS-B flight data from parquet file."""  # noqa: INP001

from __future__ import annotations

import logging
import time
from pathlib import Path

from aia_model_contrail_avoidance.flight_data_processing import (
    FlightDepartureAndArrivalSubset,
    TemporalFlightSubset,
    process_ads_b_flight_data,
)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def process_ads_b_flight_data_from_filepath(
    temporal_flight_subset: TemporalFlightSubset,
    flight_departure_and_arrival_subset: FlightDepartureAndArrivalSubset,
    unprocessed_paraquet_files: list[Path],
    processed_flights_with_ids_dir: Path,
    processed_flights_info_dir: Path,
) -> None:
    """Run the processing of ADS-B flight data.

    Args:
        temporal_flight_subset: TemporalFlightSubset, the temporal subset of flights to process.
        flight_departure_and_arrival_subset: FlightDepartureAndArrivalSubset,
        the subset of flights based on departure and arrival criteria.
        unprocessed_paraquet_files: list[Path], list of paths to unprocessed parquet files.
        processed_flights_with_ids_dir: Path, directory to save processed flights with IDs.
        processed_flights_info_dir: Path, directory to save processed flights info.
    """
    start = time.time()

    logger.info("Processing with Temporal Subset: %s", temporal_flight_subset.name)
    logger.info("Available Temporal Subsets: %s", list(TemporalFlightSubset.__members__.keys()))
    logger.info(
        "Processing with Flight Departure and Arrival Subset: %s",
        flight_departure_and_arrival_subset.name,
    )
    logger.info(
        "Available Flight Departure and Arrival Subsets: %s",
        list(FlightDepartureAndArrivalSubset.__members__.keys()),
    )
    for input_file in unprocessed_paraquet_files:
        logger.info("Processing file: %s", input_file.name)
        parquet_file_path = str(input_file)
        save_filename = input_file.stem
        full_save_path = processed_flights_with_ids_dir / f"{save_filename}.parquet"
        info_save_path = processed_flights_info_dir / f"{save_filename}.parquet"

        process_ads_b_flight_data(
            parquet_file_path,
            str(full_save_path),
            str(info_save_path),
            flight_departure_and_arrival_subset,
            temporal_flight_subset,
        )
    end = time.time()
    length = end - start
    logger.info("Data processing completed in %.1f minutes.", round(length / 60, 1))


if __name__ == "__main__":
    # input directory with ADS-B data files with flight ids added
    FLIGHTS_WITH_IDS_DIR = Path("~/ads_b_with_flight_ids").expanduser()
    unprocessed_paraquet_files = sorted(FLIGHTS_WITH_IDS_DIR.glob("*.parquet"))
    PROCESSED_FLIGHTS_WITH_IDS_DIR = Path("~/ads_b_processed_flights").expanduser()
    PROCESSED_FLIGHTS_INFO_DIR = Path("~/ads_b_processed_flights_info").expanduser()
    temporal_flight_subset = TemporalFlightSubset.FIRST_MONTH
    flight_departure_and_arrival_subset = FlightDepartureAndArrivalSubset.ALL
    process_ads_b_flight_data_from_filepath(
        temporal_flight_subset,
        flight_departure_and_arrival_subset,
        unprocessed_paraquet_files,
        PROCESSED_FLIGHTS_WITH_IDS_DIR,
        PROCESSED_FLIGHTS_INFO_DIR,
    )
