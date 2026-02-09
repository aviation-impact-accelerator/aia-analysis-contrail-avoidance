"""Run the analysis for contrail avoidance."""  # noqa: INP001

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path
from typing import Any, cast

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import inquirer  # type: ignore[import-untyped]
import polars as pl
from calculate_energy_forcing import (
    calculate_energy_forcing_for_flights,
)
from generate_energy_forcing_statistics import generate_energy_forcing_statistics

from aia_model_contrail_avoidance.core_model.dimensions import SpatialGranularity
from aia_model_contrail_avoidance.flight_data_processing import (
    FlightDepartureAndArrivalSubset,
    TemporalFlightSubset,
    process_ads_b_flight_data,
)
from plotly_analysis.better_plotly_air_traffic_density import (
    plot_air_traffic_density_map,  # type: ignore[import-not-found]
)
from plotly_analysis.plotly_contrails_formed_per_time import plot_contrails_formed
from plotly_analysis.plotly_distance_flown_by_altitude_histogram import (
    plot_distance_flown_by_altitude_histogram,
)
from plotly_analysis.plotly_domestic_international_flights import (
    plot_domestic_international_flights_pie_chart,
)
from plotly_analysis.plotly_energy_forcing_histogram import plot_energy_forcing_histogram
from plotly_analysis.plotly_uk_airspace import plot_airspace_polygons

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


# process flight data & calculate energy forcing input
FLIGHTS_WITH_IDS_DIR = Path("~/ads_b_with_flight_ids").expanduser()
# process flight data output
PROCESSED_FLIGHTS_WITH_IDS_DIR = Path("~/ads_b_processed_flights").expanduser()
processed_flight_file_paths = sorted(PROCESSED_FLIGHTS_WITH_IDS_DIR.glob("*.parquet"))
PROCESSED_FLIGHTS_INFO_DIR = Path("~/ads_b_processed_flights_info").expanduser()
# calculate energy forcing outputs
SAVE_FLIGHTS_WITH_EF_DIR = Path("~/ads_b_flights_with_ef").expanduser()
energy_forcing_file_paths = sorted(
    SAVE_FLIGHTS_WITH_EF_DIR.glob("UK_flights_day_00*_with_ef.parquet")
)
SAVE_FLIGHTS_INFO_WITH_EF_DIR = Path("~/ads_b_processed_flights_info").expanduser()
# energy forcing statistics output
energy_forcing_statistics_json = "energy_forcing_statistics_week_1_2024"


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


def run_process_ads_b_data() -> None:
    """Run the processing of ADS-B flight data."""
    start = time.time()
    questions = [
        inquirer.List(
            "temporal_subset",
            message="Which Temporal Subset would you like to process?",
            choices=[e.name for e in TemporalFlightSubset],
        ),
    ]
    questions2 = [
        inquirer.List(
            "flight_subset",
            message="Which Flight Subset would you like to process?",
            choices=[e.name for e in FlightDepartureAndArrivalSubset],
        ),
    ]
    temporal_subset = inquirer.prompt(questions)
    flight_subset = inquirer.prompt(questions2)
    temporal_flight_subset = TemporalFlightSubset[temporal_subset["temporal_subset"]]
    flight_departure_and_arrival = FlightDepartureAndArrivalSubset[flight_subset["flight_subset"]]
    for input_file in unprocessed_file_paths:
        logger.info("Processing file: %s", input_file.name)
        parquet_file_path = str(input_file)
        save_filename = input_file.stem
        full_save_path = PROCESSED_FLIGHTS_WITH_IDS_DIR / f"{save_filename}.parquet"
        info_save_path = PROCESSED_FLIGHTS_INFO_DIR / f"{save_filename}.parquet"

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


def run_calculate_energy_forcing() -> None:
    """Calculate energy forcing for processed ADS-B flight data."""
    start = time.time()
    for file_path in processed_flight_file_paths:
        output_file_name = str(file_path.stem + "_with_ef")
        logger.info("Processing file: %s", output_file_name)

        calculate_energy_forcing_for_flights(
            flight_dataframe_path=str(file_path),
            parquet_file_with_ef=str(SAVE_FLIGHTS_WITH_EF_DIR / f"{output_file_name}.parquet"),
            flight_info_with_ef_file_path=str(
                SAVE_FLIGHTS_INFO_WITH_EF_DIR / f"{file_path.stem}_flight_info.parquet"
            ),
        )

    end = time.time()
    length = end - start
    logger.info("Energy forcing calculation completed in %.1f minutes.", round(length / 60, 1))


def run_generate_energy_forcing_statistics() -> None:
    """Generate energy forcing statistics from calculated energy forcing data."""
    start = time.time()
    # choose 7 days
    complete_flight_dataframe: pl.DataFrame = pl.DataFrame()
    for parquet_file in energy_forcing_file_paths[:7]:
        # read and append all dataframes together
        daily_dataframe = pl.read_parquet(parquet_file)
        if complete_flight_dataframe.is_empty():
            complete_flight_dataframe = pl.concat([complete_flight_dataframe, daily_dataframe])
        else:
            complete_flight_dataframe = daily_dataframe

    logger.info(
        "Generating energy forcing statistics from %s to %s",
        complete_flight_dataframe["timestamp"].min(),
        complete_flight_dataframe["timestamp"].max(),
    )

    generate_energy_forcing_statistics(complete_flight_dataframe, energy_forcing_statistics_json)

    end = time.time()
    length = end - start
    logger.info(
        "Energy forcing statistics generation completed in %.1f minutes.", round(length / 60, 1)
    )


def generate_all_plotly() -> None:
    """Generate all Plotly graphs."""
    plot_contrails_formed(
        name_of_forcing_stats_file=energy_forcing_statistics_json,
        output_plot_name="contrails_formed",
    )
    plot_distance_flown_by_altitude_histogram(
        stats_file=energy_forcing_statistics_json,
        output_file="distance_flown_by_altitude_histogram",
    )
    plot_energy_forcing_histogram(
        json_file=energy_forcing_statistics_json,
        output_file_histogram="energy_forcing_per_flight_histogram",
        output_file_cumulative="energy_forcing_cumulative",
    )
    plot_airspace_polygons(
        output_file="uk_airspace_map",
    )

    environmental_bounds = {
        "lat_min": 45.0,
        "lat_max": 61.0,
        "lon_min": -30.0,
        "lon_max": 5.0,
    }

    plot_air_traffic_density_map(
        # Will Fix This!!!
        parquet_file_name="2024_01_01_sample_processed_with_interpolation",
        environmental_bounds=environmental_bounds,
        spatial_granularity=SpatialGranularity.ONE_DEGREE,
        output_file="air_traffic_density_map_uk_airspace",
    )

    plot_domestic_international_flights_pie_chart(
        json_file=energy_forcing_statistics_json,
        output_file="domestic_international_flights_pie_chart",
    )


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


if __name__ == "__main__":
    total_start = time.time()
    answers = processing_user_selction()
    print(answers)
    if "All Steps" in answers["processing steps"]:
        unprocessed_file_paths = make_analysis_directories()
        run_process_ads_b_data()
        run_calculate_energy_forcing()
        run_generate_energy_forcing_statistics()
        generate_all_plotly()
    elif "ADS-B Processing" in answers["processing steps"]:
        unprocessed_file_paths = make_analysis_directories()
        run_process_ads_b_data()
    elif "Calculate Energy Forcing" in answers["processing steps"]:
        run_calculate_energy_forcing()
    elif "Generate Energy Forcing Statistics" in answers["processing steps"]:
        run_generate_energy_forcing_statistics()
    elif "Plots" in answers["processing steps"]:
        generate_all_plotly()
    else:
        logger.info("No analysis steps selected. Exiting.")
        sys.exit(0)

    total_end = time.time()
    total_length = total_end - total_start
    logger.info("Total analysis completed in %.1f minutes.", round(total_length / 60, 1))
