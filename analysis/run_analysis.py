"""Run the analysis for contrail avoidance."""  # noqa: INP001

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path
from typing import Any, cast

import inquirer
from calculate_energy_forcing_from_filepath import calculate_energy_forcing_from_filepath
from generate_energy_forcing_statistics_from_filepath import (
    generate_energy_forcing_statistics_from_filepath,
)
from process_ads_b_flight_data_from_filepath import process_ads_b_flight_data_from_filepath

from aia_model_contrail_avoidance.core_model.airspace import ENVIRONMENTAL_BOUNDS_UK_AIRSPACE
from aia_model_contrail_avoidance.core_model.dimensions import (
    SpatialGranularity,
)
from aia_model_contrail_avoidance.flight_data_processing import (
    FlightDepartureAndArrivalSubset,
    TemporalFlightSubset,
)
from aia_model_contrail_avoidance.visualisation.generate_all_plots import generate_all_plots

# USER SELECTIONS FOR ANALYSIS
# ------------------------------------------------------------------------------
temporal_flight_subset = TemporalFlightSubset.JANUARY
flight_departure_and_arrival_subset = FlightDepartureAndArrivalSubset.ALL
enviornmental_bounds = ENVIRONMENTAL_BOUNDS_UK_AIRSPACE
spatial_granularity = SpatialGranularity.ONE_DEGREE
# ------------------------------------------------------------------------------

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Parent directory for all analysis inputs and outputs
ADS_B_ANALYSIS_DIR = Path("~/ads_b_analysis").expanduser()
# Process flight data input directory
FLIGHTS_WITH_IDS_DIR = ADS_B_ANALYSIS_DIR / "ads_b_with_flight_ids"
# Process flight data output directories
PROCESSED_FLIGHTS_WITH_IDS_DIR = ADS_B_ANALYSIS_DIR / "ads_b_processed_flights"
PROCESSED_FLIGHTS_INFO_DIR = ADS_B_ANALYSIS_DIR / "ads_b_processed_flights_info"
# Calculate energy forcing output directories
FLIGHTS_WITH_EF_DIR = ADS_B_ANALYSIS_DIR / "ads_b_flights_with_ef"
FLIGHTS_INFO_WITH_EF_DIR = ADS_B_ANALYSIS_DIR / "ads_b_flights_info_with_ef"

# Filenames for environment and statistics
name, month_num, month_padded, days_in_month, *rest = temporal_flight_subset.value
enviornment_filename = f"cocip_grid_global_month_{month_padded}_2024"
energy_forcing_statistics_json = f"energy_forcing_statistics_month_{month_padded}_2024"
plot_energy_forcing_statistics_json = f"results/{energy_forcing_statistics_json}.json"


def make_analysis_directories() -> None:
    """Create necessary directories for the analysis."""
    # if directory does not exist, throw error
    if not FLIGHTS_WITH_IDS_DIR.exists() or not FLIGHTS_WITH_IDS_DIR.is_dir():
        logger.error(
            "\n Input directory not found: %s This directory will"
            " be created. \n Add the parquet files before running the script again.",
            FLIGHTS_WITH_IDS_DIR,
        )
        FLIGHTS_WITH_IDS_DIR.mkdir(parents=True, exist_ok=True)
        sys.exit(1)

    # if directory does not exist, create it
    if not PROCESSED_FLIGHTS_WITH_IDS_DIR.exists():
        PROCESSED_FLIGHTS_WITH_IDS_DIR.mkdir(parents=True, exist_ok=True)
        logger.info(
            "Output directory created at: %s.",
            PROCESSED_FLIGHTS_WITH_IDS_DIR,
        )

    if not PROCESSED_FLIGHTS_INFO_DIR.exists():
        PROCESSED_FLIGHTS_INFO_DIR.mkdir(parents=True, exist_ok=True)
        logger.info(
            "Output directory created at: %s.",
            PROCESSED_FLIGHTS_INFO_DIR,
        )
    if not FLIGHTS_WITH_EF_DIR.exists():
        FLIGHTS_WITH_EF_DIR.mkdir(parents=True, exist_ok=True)
        logger.info(
            "Output directory created at: %s.",
            FLIGHTS_WITH_EF_DIR,
        )
    if not FLIGHTS_INFO_WITH_EF_DIR.exists():
        FLIGHTS_INFO_WITH_EF_DIR.mkdir(parents=True, exist_ok=True)
        logger.info(
            "Output directory created at: %s.",
            FLIGHTS_INFO_WITH_EF_DIR,
        )


def remove_zone_identifier_files() -> None:
    """Remove zone identifier files from the input directory."""
    zone_identifier_files = sorted(FLIGHTS_WITH_IDS_DIR.glob("*:Zone.Identifier"))
    if zone_identifier_files:
        logger.info("Found %d zone identifier files to remove.", len(zone_identifier_files))
        zone_identifier_user_selections = [
            inquirer.List(
                "processing steps",
                message="Would you like to remove the zone identifier files?",
                choices=[
                    "Yes, remove the zone identifier files",
                    "No, keep the zone identifier files",
                ],
            ),
        ]
        result = inquirer.prompt(zone_identifier_user_selections)
        if result is None or result["processing steps"] == "Yes, remove the zone identifier files":
            for file in zone_identifier_files:
                try:
                    logger.info("Removing zone identifier file: %s", file.name)
                    file.unlink()
                except OSError as e:
                    logger.warning("Failed to remove %s: %s", file.name, e)
        elif result["processing steps"] == "No, keep the zone identifier files":
            logger.info(
                "Keeping zone identifier files. Please remove them manually "
                "before running the analysis."
            )
            sys.exit(0)
    else:
        logger.info("Ready to process...")


def process_user_selection() -> dict[str, Any]:
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
    remove_zone_identifier_files()
    answers = process_user_selection()
    total_start = time.time()
    make_analysis_directories()
    if "All Steps" in answers["processing steps"]:
        process_ads_b_flight_data_from_filepath(
            temporal_flight_subset,
            flight_departure_and_arrival_subset,
            FLIGHTS_WITH_IDS_DIR,
            PROCESSED_FLIGHTS_WITH_IDS_DIR,
            PROCESSED_FLIGHTS_INFO_DIR,
        )

        calculate_energy_forcing_from_filepath(
            PROCESSED_FLIGHTS_WITH_IDS_DIR,
            PROCESSED_FLIGHTS_INFO_DIR,
            FLIGHTS_WITH_EF_DIR,
            FLIGHTS_INFO_WITH_EF_DIR,
            temporal_flight_subset=temporal_flight_subset,
            enviornment_filename=enviornment_filename,
        )

        generate_energy_forcing_statistics_from_filepath(
            FLIGHTS_WITH_EF_DIR, energy_forcing_statistics_json, temporal_flight_subset
        )
        generate_all_plots(
            json_file_name=plot_energy_forcing_statistics_json,
            flights_with_ef_dir=FLIGHTS_WITH_EF_DIR,
            environmental_bounds=enviornmental_bounds,
            spatial_granularity=spatial_granularity,
        )

    if "ADS-B Processing" in answers["processing steps"]:
        process_ads_b_flight_data_from_filepath(
            temporal_flight_subset,
            flight_departure_and_arrival_subset,
            FLIGHTS_WITH_IDS_DIR,
            PROCESSED_FLIGHTS_WITH_IDS_DIR,
            PROCESSED_FLIGHTS_INFO_DIR,
        )

    if "Calculate Energy Forcing" in answers["processing steps"]:
        calculate_energy_forcing_from_filepath(
            PROCESSED_FLIGHTS_WITH_IDS_DIR,
            PROCESSED_FLIGHTS_INFO_DIR,
            FLIGHTS_WITH_EF_DIR,
            FLIGHTS_INFO_WITH_EF_DIR,
            temporal_flight_subset=temporal_flight_subset,
            enviornment_filename=enviornment_filename,
        )
    if "Generate Energy Forcing Statistics" in answers["processing steps"]:
        generate_energy_forcing_statistics_from_filepath(
            FLIGHTS_WITH_EF_DIR, energy_forcing_statistics_json, temporal_flight_subset
        )
    if "Plots" in answers["processing steps"]:
        generate_all_plots(
            json_file_name=plot_energy_forcing_statistics_json,
            flights_with_ef_dir=FLIGHTS_WITH_EF_DIR,
            environmental_bounds=enviornmental_bounds,
            spatial_granularity=spatial_granularity,
        )
    if answers["processing steps"] == []:
        logger.info(
            "No analysis steps selected. Please use spacebar to select steps and enter to run."
        )
        sys.exit(0)

    total_end = time.time()
    total_length = total_end - total_start
    logger.info("Total analysis completed in %.1f minutes.", round(total_length / 60, 1))


if __name__ == "__main__":
    run_analysis()
