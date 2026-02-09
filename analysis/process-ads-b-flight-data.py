"""Process ADS-B flight data from parquet file."""  # noqa: INP001

from __future__ import annotations

import logging
import sys
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


# input directory with ADS-B data files with flight ids added
FLIGHTS_WITH_IDS_DIR = Path("~/ads_b_with_flight_ids").expanduser()

PROCESSED_FLIGHTS_WITH_IDS_DIR = Path("~/ads_b_processed_flights").expanduser()
PROCESSED_FLIGHTS_INFO_DIR = Path("~/ads_b_processed_flights_info").expanduser()

if __name__ == "__main__":
    start = time.time()
    temporal_flight_subset = TemporalFlightSubset.FIRST_MONTH
    flight_departure_and_arrival = FlightDepartureAndArrivalSubset.ALL

    # if directoy does not exist, throw error
    if FLIGHTS_WITH_IDS_DIR.exists() and FLIGHTS_WITH_IDS_DIR.is_dir():
        parquet_file_paths = sorted(FLIGHTS_WITH_IDS_DIR.glob("*.parquet"))

    else:
        logger.error(
            "\n Input directory not found: %s This directory will"
            " be created. \n Add the parquet files before running the script again.",
            FLIGHTS_WITH_IDS_DIR,
        )
        FLIGHTS_WITH_IDS_DIR.mkdir(parents=True, exist_ok=True)
        sys.exit(1)

    # if directoy does not exist, create it
    if not PROCESSED_FLIGHTS_WITH_IDS_DIR.exists() and not PROCESSED_FLIGHTS_WITH_IDS_DIR.is_dir():
        logger.info(
            "Output directory created at: %s.",
            PROCESSED_FLIGHTS_WITH_IDS_DIR,
        )
        PROCESSED_FLIGHTS_WITH_IDS_DIR.mkdir(parents=True, exist_ok=True)
        PROCESSED_FLIGHTS_INFO_DIR.mkdir(parents=True, exist_ok=True)

    for input_file in parquet_file_paths:
        logger.info("Processing file: %s", input_file.name)
        parquet_file_path = str(input_file)
        save_filename = input_file.stem
        full_save_path = PROCESSED_FLIGHTS_WITH_IDS_DIR / f"{save_filename}.parquet"
        info_save_path = PROCESSED_FLIGHTS_INFO_DIR / f"{save_filename}_info.parquet"

        process_ads_b_flight_data(
            parquet_file_path,
            str(full_save_path),
            str(info_save_path),
            flight_departure_and_arrival,
            temporal_flight_subset,
        )
    end = time.time()
    length = end - start
    logger.info("Data processing completed in %.1f minutes.", round(length / 60, 1))
