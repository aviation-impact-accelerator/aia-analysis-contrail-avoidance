"""Process ADS-B flight data from parquet file."""  # noqa: INP001

from __future__ import annotations

import time
from pathlib import Path

from aia_model_contrail_avoidance.flight_data_processing import (
    FlightDepartureAndArrivalSubset,
    TemporalFlightSubset,
    process_ads_b_flight_data,
)

# input directory with ADS-B data files with flight ids added
FLIGHTS_WITH_IDS_DIR = Path("/home/as3091/ads_b_with_flight_ids")

PROCESSED_FLIGHTS_WITH_IDS_DIR = Path("/home/as3091/ads_b_processed_flights")


if __name__ == "__main__":
    start = time.time()
    temporal_flight_subset = TemporalFlightSubset.FIRST_MONTH
    flight_departure_and_arrival = FlightDepartureAndArrivalSubset.ALL

    parquet_file_paths = list(FLIGHTS_WITH_IDS_DIR.glob("*.parquet"))
    for input_file in parquet_file_paths:
        print("Processing file:", input_file.name)
        parquet_file_path = str(input_file)
        save_filename = input_file.stem

        process_ads_b_flight_data(
            parquet_file_path,
            save_filename,
            flight_departure_and_arrival,
            temporal_flight_subset,
        )
    end = time.time()
    length = end - start
    print("Data processing completed in", round(length / 60, 1), "minutes.")
