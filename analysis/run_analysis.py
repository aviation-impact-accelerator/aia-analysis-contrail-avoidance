"""Run the analysis for contrail avoidance."""  # noqa: INP001

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path
from typing import Any, cast

import inquirer  # type: ignore[import-untyped]
from calculate_energy_forcing_from_filepath import calculate_energy_forcing_from_filepath
from generate_energy_forcing_statistics_from_filepath import (
    generate_energy_forcing_statistics_from_filepath,
)
from process_ads_b_flight_data_from_filepath import process_ads_b_flight_data_from_filepath

from aia_model_contrail_avoidance.flight_data_processing import (
    FlightDepartureAndArrivalSubset,
    TemporalFlightSubset,
)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


# process flight data & calculate energy forcing input
FLIGHTS_WITH_IDS_DIR = Path("~/ads_b_with_flight_ids").expanduser()
unprocessed_paraquet_files = sorted(FLIGHTS_WITH_IDS_DIR.glob("*.parquet"))
temporal_flight_subset = TemporalFlightSubset.FIRST_MONTH
flight_departure_and_arrival_subset = FlightDepartureAndArrivalSubset.ALL
# process flight data output
PROCESSED_FLIGHTS_WITH_IDS_DIR = Path("~/ads_b_processed_flights").expanduser()
processed_paraquet_files = sorted(PROCESSED_FLIGHTS_WITH_IDS_DIR.glob("*.parquet"))
PROCESSED_FLIGHTS_INFO_DIR = Path("~/ads_b_processed_flights_info").expanduser()
# calculate energy forcing outputs
SAVE_FLIGHTS_WITH_EF_DIR = Path("~/ads_b_flights_with_ef").expanduser()
energy_forcing_paraquet_files = sorted(
    SAVE_FLIGHTS_WITH_EF_DIR.glob("UK_flights_day_00*_with_ef.parquet")
)
SAVE_FLIGHTS_INFO_WITH_EF_DIR = Path("~/ads_b_processed_flights_info").expanduser()
# energy forcing statistics output
energy_forcing_statistics_json = "energy_forcing_statistics_week_1_2024"
first_day = 1
final_day = 7


def make_analysis_directories() -> list[Path]:
    """Create necessary directories for the analysis."""
    # if directory does not exist, throw error
    if FLIGHTS_WITH_IDS_DIR.exists() and FLIGHTS_WITH_IDS_DIR.is_dir():
        unprocessed_file_paths = sorted(FLIGHTS_WITH_IDS_DIR.glob("*.parquet"))
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

    if not SAVE_FLIGHTS_WITH_EF_DIR.exists():
        SAVE_FLIGHTS_WITH_EF_DIR.mkdir(parents=True, exist_ok=True)
    if not SAVE_FLIGHTS_INFO_WITH_EF_DIR.exists():
        SAVE_FLIGHTS_INFO_WITH_EF_DIR.mkdir(parents=True, exist_ok=True)

    return unprocessed_file_paths


def processing_user_selction() -> dict[str, Any]:
    """Prompt user for analysis options."""
    questions = [
        inquirer.Checkbox(
            "processing steps",
            message="Which Analysis Steps would you like to run (space to select)?",
            choices=[
                "All Steps",
                "ADS-B Processing",
                "Calculate Energy Forcing",
                "Generate Energy Forcing Statistics",
                "Plots",
            ],
        ),
    ]
    result = inquirer.prompt(questions)
    if result is None:
        return {"processing_steps": []}
    return cast("dict[str, Any]", result)


def run_analysis() -> None:
    """Run the analysis for contrail avoidance."""
    total_start = time.time()

    answers = processing_user_selction()
    print(answers)
    make_analysis_directories()
    if "All Steps" in answers["processing steps"]:
        process_ads_b_flight_data_from_filepath(
            temporal_flight_subset,
            flight_departure_and_arrival_subset,
            unprocessed_paraquet_files,
            PROCESSED_FLIGHTS_WITH_IDS_DIR,
            PROCESSED_FLIGHTS_INFO_DIR,
        )
        calculate_energy_forcing_from_filepath(
            processed_paraquet_files,
            SAVE_FLIGHTS_WITH_EF_DIR,
            SAVE_FLIGHTS_INFO_WITH_EF_DIR,
        )
        generate_energy_forcing_statistics_from_filepath(
            energy_forcing_paraquet_files, energy_forcing_statistics_json, first_day, final_day
        )

    if "ADS-B Processing" in answers["processing steps"]:
        process_ads_b_flight_data_from_filepath(
            temporal_flight_subset,
            flight_departure_and_arrival_subset,
            unprocessed_paraquet_files,
            PROCESSED_FLIGHTS_WITH_IDS_DIR,
            PROCESSED_FLIGHTS_INFO_DIR,
        )

    if "Calculate Energy Forcing" in answers["processing steps"]:
        calculate_energy_forcing_from_filepath(
            processed_paraquet_files,
            SAVE_FLIGHTS_WITH_EF_DIR,
            SAVE_FLIGHTS_INFO_WITH_EF_DIR,
        )
    if "Generate Energy Forcing Statistics" in answers["processing steps"]:
        generate_energy_forcing_statistics_from_filepath(
            energy_forcing_paraquet_files, energy_forcing_statistics_json, first_day, final_day
        )
    if "Plots" in answers["processing steps"]:
        # fix later
        print("plots")
    if answers["processing steps"] == []:
        logger.info("No analysis steps selected. Exiting.")
        sys.exit(0)

    total_end = time.time()
    total_length = total_end - total_start
    logger.info("Total analysis completed in %.1f minutes.", round(total_length / 60, 1))


if __name__ == "__main__":
    run_analysis()
