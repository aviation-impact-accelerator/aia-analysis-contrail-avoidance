"""Generate energy forcing summary statistics including contrail formation analysis."""  # noqa: INP001

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

import numpy as np
import polars as pl

from aia_model_contrail_avoidance.core_model.airports import list_of_uk_airports
from aia_model_contrail_avoidance.core_model.dimensions import (
    TemporalGranularity,
    _get_temporal_grouping_field,
    _get_temporal_range_and_labels,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def create_histogram_distance_flown_by_flight_level(
    complete_flights_dataframe: pl.DataFrame,
) -> dict[str, float]:
    """Calculate distance flown by altitude histogram.

    Args:
        complete_flights_dataframe: DataFrame containing flight data with energy forcing.
            required columns: "distance_flown_in_segment", "flight_level"

    Returns:
        Dictionary mapping altitude ranges (in flight levels) to distance flown.
    """
    # Altitude bins are each 10 flight levels, from 0 to 450 (i.e. FL0-10, ..., FL440-450)
    flight_level_bins = [(i, i + 10) for i in range(0, 450, 10)]
    flight_level_bin_labels = [
        f"FL{flight_level_bin[0]}-{flight_level_bin[1]}" for flight_level_bin in flight_level_bins
    ]
    complete_flights_dataframe = complete_flights_dataframe.with_columns(
        pl.col("flight_level").cast(pl.Int64)
    )
    distance_flown_by_flight_level_histogram = {}
    for (lower_bound, upper_bound), label in zip(
        flight_level_bins, flight_level_bin_labels, strict=False
    ):
        distance_in_bin = complete_flights_dataframe.filter(
            pl.col("flight_level").is_between(lower_bound, upper_bound, closed="left")
        )["distance_flown_in_segment"].sum()
        distance_flown_by_flight_level_histogram[label] = distance_in_bin
    return distance_flown_by_flight_level_histogram


def create_histogram_distance_flown_over_time(
    temporal_granularity: TemporalGranularity, flight_dataframe: pl.DataFrame
) -> dict[str, float]:
    """Calculate distance flown per temporal unit histogram.

    Args:
        temporal_granularity: Temporal granularity for aggregation.
        flight_dataframe: DataFrame containing flight data.

    Returns:
        Dictionary mapping temporal units to distance flown.
    """
    temporal_field = _get_temporal_grouping_field(temporal_granularity)
    temporal_range, _labels = _get_temporal_range_and_labels(temporal_granularity)

    flight_dataframe_with_temporal = flight_dataframe.with_columns(
        pl.col("timestamp").dt.__getattribute__(temporal_field)().alias("temporal_unit")
    )
    distance_per_temporal = (
        flight_dataframe_with_temporal.group_by("temporal_unit")
        .agg(pl.col("distance_flown_in_segment").sum())
        .to_dict(as_series=False)
    )
    # if hourly, divide by number of days in the dataframe to get average distance flown per hour across the week
    if temporal_granularity == TemporalGranularity.HOURLY:
        number_of_days = flight_dataframe["timestamp"].dt.date().n_unique()
        distance_per_temporal["distance_flown_in_segment"] = [
            distance / number_of_days
            for distance in distance_per_temporal["distance_flown_in_segment"]
        ]
    temporal_to_distance = dict(
        zip(
            distance_per_temporal["temporal_unit"],
            distance_per_temporal["distance_flown_in_segment"],
            strict=True,
        )
    )
    return {str(unit): temporal_to_distance.get(unit, 0) for unit in temporal_range}


def create_histogram_distance_forming_contrails_over_time(
    temporal_granularity: TemporalGranularity, flight_dataframe_of_contrails: pl.DataFrame
) -> dict[str, float]:
    """Calculate distance forming contrails per temporal unit histogram.

    Args:
        temporal_granularity: Temporal granularity for aggregation.
        flight_dataframe_of_contrails: DataFrame containing flight data forming contrails.

    Returns:
        Dictionary mapping temporal units to distance forming contrails.
    """
    temporal_field = _get_temporal_grouping_field(temporal_granularity)
    temporal_range, _labels = _get_temporal_range_and_labels(temporal_granularity)

    flight_dataframe_with_temporal = flight_dataframe_of_contrails.with_columns(
        pl.col("timestamp").dt.__getattribute__(temporal_field)().alias("temporal_unit")
    )
    distance_per_temporal = (
        flight_dataframe_with_temporal.group_by("temporal_unit")
        .agg(pl.col("distance_flown_in_segment").sum())
        .to_dict(as_series=False)
    )
    # if hourly, divide by number of days in the dataframe to get average distance flown per hour across the week
    if temporal_granularity == TemporalGranularity.HOURLY:
        number_of_days = flight_dataframe_of_contrails["timestamp"].dt.date().n_unique()
        distance_per_temporal["distance_flown_in_segment"] = [
            distance / number_of_days
            for distance in distance_per_temporal["distance_flown_in_segment"]
        ]
    temporal_to_distance = dict(
        zip(
            distance_per_temporal["temporal_unit"],
            distance_per_temporal["distance_flown_in_segment"],
            strict=True,
        )
    )
    return {str(unit): temporal_to_distance.get(unit, 0) for unit in temporal_range}


def create_plot_cumulative_energy_forcing_flight(
    flight_dataframe: pl.DataFrame,
) -> dict[str, float]:
    """Calculate cumulative energy forcing per flight plot.

    Args:
        flight_dataframe: DataFrame containing flight data.

    Returns:
        Dictionary mapping percentage of flights to cumulative energy forcing staring with most warming.
    """
    # Per-flight energy forcing statistics
    flight_ef_summary = flight_dataframe.group_by("flight_id").agg(
        pl.col("ef").sum().alias("total_ef")
    )
    number_of_flights = flight_ef_summary["flight_id"].n_unique()
    total_energy_forcing = flight_ef_summary["total_ef"].sum()
    # Sort flights by energy forcing in descending order
    flight_ef_summary = flight_ef_summary.sort("total_ef", descending=True)
    # Calculate cumulative energy forcing and corresponding percentage of flights
    flight_ef_summary = flight_ef_summary.with_columns(
        pl.col("total_ef").cum_sum().alias("cumulative_ef"),
        (pl.arange(1, number_of_flights + 1) / number_of_flights * 100).alias(
            "percentage_of_flights"
        ),
    )

    cumulative_ef_percentage = (flight_ef_summary["cumulative_ef"] / total_energy_forcing) * 100
    flight_ef_summary = flight_ef_summary.with_columns(
        cumulative_ef_percentage.alias("cumulative_ef_percentage")
    )
    # Sample the cumulative energy forcing at each percentage_of_flights
    flight_ef_summary = flight_ef_summary.select(
        "percentage_of_flights", "cumulative_ef_percentage"
    )
    # for each percent from 1 0 to 100 find the cumulative energy forcing percentage at that point and return as a dictionary
    percentage_of_flights = np.arange(1, 101)
    cumulative_ef_percentage_at_percentage_of_flights = np.interp(
        percentage_of_flights,
        flight_ef_summary["percentage_of_flights"].to_numpy(),
        flight_ef_summary["cumulative_ef_percentage"].to_numpy(),
    )
    histogram = dict(
        zip(
            percentage_of_flights.astype(int),
            cumulative_ef_percentage_at_percentage_of_flights,
            strict=False,
        )
    )

    return {
        str(percent): histogram.get(percent, 0) for percent in percentage_of_flights.astype(int)
    }


def create_histogram_air_traffic_density_over_time(
    temporal_granularity: TemporalGranularity, flight_dataframe: pl.DataFrame
) -> dict[str, int]:
    """Calculate air traffic density per temporal unit histogram.

    Args:
        temporal_granularity: Temporal granularity for aggregation.
        flight_dataframe: DataFrame containing flight data.

    Returns:
        Dictionary mapping temporal units to air traffic density.
    """
    temporal_field = _get_temporal_grouping_field(temporal_granularity)
    temporal_range, _labels = _get_temporal_range_and_labels(temporal_granularity)

    flight_dataframe_with_temporal = flight_dataframe.with_columns(
        pl.col("timestamp").dt.__getattribute__(temporal_field)().alias("temporal_unit")
    )
    planes_per_temporal = (
        flight_dataframe_with_temporal.group_by("temporal_unit")
        .agg(pl.col("flight_id").n_unique())
        .to_dict(as_series=False)
    )
    # if hourly, divide by number of days in the dataframe to get average distance flown per hour across the week
    if temporal_granularity == TemporalGranularity.HOURLY:
        number_of_days = flight_dataframe["timestamp"].dt.date().n_unique()
        planes_per_temporal["flight_id"] = [
            count / number_of_days for count in planes_per_temporal["flight_id"]
        ]
    temporal_to_planes = dict(
        zip(planes_per_temporal["temporal_unit"], planes_per_temporal["flight_id"], strict=True)
    )
    return {str(unit): temporal_to_planes.get(unit, 0) for unit in temporal_range}


def generate_energy_forcing_statistics(
    complete_flight_dataframe: pl.DataFrame,
    output_filename: str,
) -> None:
    """Generate energy forcing summary statistics including contrail formation analysis.

    Args:
        complete_flight_dataframe: DataFrame containing flight data with energy forcing.
        output_filename: Path to save the statistics as JSON.

    """
    # -- Data Quality Checks ---
    # Check for missing values in critical columns
    critical_columns = ["flight_id", "timestamp", "distance_flown_in_segment", "ef"]
    missing_values_report = {
        column: int(complete_flight_dataframe[column].is_null().sum())
        for column in critical_columns
    }
    logger.info("Missing Values Report: %s", missing_values_report)

    # remove not a number values from distance_flown_in_segment and ef columns
    complete_flight_dataframe = complete_flight_dataframe.filter(
        pl.col("distance_flown_in_segment").is_finite() & pl.col("ef").is_finite()
    )
    # --- General Statistics ---
    total_number_of_datapoints = len(complete_flight_dataframe)
    total_number_of_flights = complete_flight_dataframe["flight_id"].n_unique()
    total_distance_flown = complete_flight_dataframe["distance_flown_in_segment"].sum()

    # -- Airspace Specific Statistics ---
    uk_airspace_flights_dataframe = complete_flight_dataframe.filter(
        pl.col("airspace").is_not_null()
    )

    international_airspace_flights_dataframe = complete_flight_dataframe.filter(
        pl.col("airspace").is_null()
    )
    # change datapoints in airspace as "international"
    international_airspace_flights_dataframe = (
        international_airspace_flights_dataframe.with_columns(
            pl.lit("international").alias("airspace")
        )
    )

    total_energy_forcing_in_uk_airspace = uk_airspace_flights_dataframe["ef"].sum()
    total_energy_forcing_in_international_airspace = international_airspace_flights_dataframe[
        "ef"
    ].sum()

    # -- Regional vs International Flights ---
    uk_airports = list_of_uk_airports()
    regional_flights_dataframe = complete_flight_dataframe.filter(
        pl.col("arrival_airport_icao").is_in(uk_airports)
        & pl.col("departure_airport_icao").is_in(uk_airports)
    )

    number_of_regional_flights = regional_flights_dataframe["flight_id"].n_unique()
    number_of_international_flights = total_number_of_flights - number_of_regional_flights

    # --- Contrail Formation Analysis ---

    contrail_forming_flight_segments_dataframe = complete_flight_dataframe.filter(
        pl.col("ef") > 0.0
    )  # Segments with positive energy forcing form contrails

    total_distance_forming_contrails = contrail_forming_flight_segments_dataframe[
        "distance_flown_in_segment"
    ].sum()
    percentage_distance_forming_contrails = (
        (total_distance_forming_contrails / total_distance_flown) * 100
        if total_distance_flown > 0.0
        else 0
    )

    number_of_flights_forming_contrails = contrail_forming_flight_segments_dataframe[
        "flight_id"
    ].n_unique()
    percentage_of_flights_forming_contrails = (
        number_of_flights_forming_contrails / total_number_of_flights
    ) * 100

    # --- Energy Forcing Statistics ---
    total_energy_forcing = complete_flight_dataframe["ef"].sum()

    # Per-flight energy forcing statistics
    flight_ef_summary = complete_flight_dataframe.group_by("flight_id").agg(
        pl.col("ef").sum().alias("total_ef")
    )
    number_of_flights = flight_ef_summary["flight_id"].n_unique()
    total_energy_forcing = flight_ef_summary["total_ef"].sum()
    # Sort flights by energy forcing in descending order
    flight_ef_summary = flight_ef_summary.sort("total_ef", descending=True)
    # Calculate cumulative energy forcing and corresponding percentage of flights
    flight_ef_summary = flight_ef_summary.with_columns(
        pl.col("total_ef").cum_sum().alias("cumulative_ef"),
        (pl.arange(1, number_of_flights + 1)).alias("number_of_flights"),
    )
    top_20_percent_ef = total_energy_forcing * 0.2
    top_50_percent_ef = total_energy_forcing * 0.5
    top_80_percent_ef = total_energy_forcing * 0.8

    number_of_flights_for_20_percent_ef = (
        flight_ef_summary.filter(pl.col("cumulative_ef") >= top_20_percent_ef)
        .select(pl.col("number_of_flights").min())
        .item()
    )
    number_of_flights_for_50_percent_ef = (
        flight_ef_summary.filter(pl.col("cumulative_ef") >= top_50_percent_ef)
        .select(pl.col("number_of_flights").min())
        .item()
    )
    number_of_flights_for_80_percent_ef = (
        flight_ef_summary.filter(pl.col("cumulative_ef") >= top_80_percent_ef)
        .select(pl.col("number_of_flights").min())
        .item()
    )

    # --- Build Summary ---
    stats = {
        "overview": {
            "total_datapoints": total_number_of_datapoints,
        },
        "contrail_formation": {
            "flights_forming_contrails": int(number_of_flights_forming_contrails),
            "percentage_flights_forming_contrails": round(
                percentage_of_flights_forming_contrails, 2
            ),
            "distance_forming_contrails_nm": float(total_distance_forming_contrails),
            "percentage_distance_forming_contrails": round(
                percentage_distance_forming_contrails, 2
            ),
        },
        "number_of_flights": {
            "total": total_number_of_flights,
            "regional": number_of_regional_flights,
            "international": number_of_international_flights,
        },
        "flight_distance_by_airspace": {
            "total_nm": float(total_distance_flown),
            "uk_airspace_nm": float(
                uk_airspace_flights_dataframe["distance_flown_in_segment"].sum()
            ),
            "international_airspace_nm": float(
                international_airspace_flights_dataframe["distance_flown_in_segment"].sum()
            ),
        },
        "energy_forcing": {
            "total": float(total_energy_forcing),
            "uk_airspace": float(total_energy_forcing_in_uk_airspace),
            "international_airspace": float(total_energy_forcing_in_international_airspace),
        },
        "cumulative_energy_forcing_per_flight": {
            "histogram": create_plot_cumulative_energy_forcing_flight(complete_flight_dataframe),
            "number_of_flights_for_80_percent_ef": number_of_flights_for_80_percent_ef,
            "number_of_flights_for_50_percent_ef": number_of_flights_for_50_percent_ef,
            "number_of_flights_for_20_percent_ef": number_of_flights_for_20_percent_ef,
        },
        "distance_flown_over_time_histogram": {
            "hourly": create_histogram_distance_flown_over_time(
                TemporalGranularity.HOURLY, complete_flight_dataframe
            ),
            "daily": create_histogram_distance_flown_over_time(
                TemporalGranularity.DAILY, complete_flight_dataframe
            ),
            "monthly": create_histogram_distance_flown_over_time(
                TemporalGranularity.MONTHLY, complete_flight_dataframe
            ),
            "seasonally": create_histogram_distance_flown_over_time(
                TemporalGranularity.SEASONALLY, complete_flight_dataframe
            ),
            "annually": create_histogram_distance_flown_over_time(
                TemporalGranularity.ANNUALLY, complete_flight_dataframe
            ),
        },
        "distance_forming_contrails_over_time_histogram": {
            "hourly": create_histogram_distance_forming_contrails_over_time(
                TemporalGranularity.HOURLY, contrail_forming_flight_segments_dataframe
            ),
            "daily": create_histogram_distance_forming_contrails_over_time(
                TemporalGranularity.DAILY, contrail_forming_flight_segments_dataframe
            ),
            "monthly": create_histogram_distance_forming_contrails_over_time(
                TemporalGranularity.MONTHLY, contrail_forming_flight_segments_dataframe
            ),
            "seasonally": create_histogram_distance_forming_contrails_over_time(
                TemporalGranularity.SEASONALLY, contrail_forming_flight_segments_dataframe
            ),
            "annually": create_histogram_distance_forming_contrails_over_time(
                TemporalGranularity.ANNUALLY, contrail_forming_flight_segments_dataframe
            ),
        },
        "air_traffic_density_over_time_histogram": {
            "hourly": create_histogram_air_traffic_density_over_time(
                TemporalGranularity.HOURLY, complete_flight_dataframe
            ),
            "daily": create_histogram_air_traffic_density_over_time(
                TemporalGranularity.DAILY, complete_flight_dataframe
            ),
            "monthly": create_histogram_air_traffic_density_over_time(
                TemporalGranularity.MONTHLY, complete_flight_dataframe
            ),
            "seasonally": create_histogram_air_traffic_density_over_time(
                TemporalGranularity.SEASONALLY, complete_flight_dataframe
            ),
            "annually": create_histogram_air_traffic_density_over_time(
                TemporalGranularity.ANNUALLY, complete_flight_dataframe
            ),
        },
        "distance_flown_by_flight_level_histogram": create_histogram_distance_flown_by_flight_level(
            complete_flight_dataframe
        ),
    }

    # --- Write Output ---
    logger.info("Saving statistics to results/%s.json", output_filename)
    with Path("results/" + output_filename + ".json").open("w") as f:
        json.dump(stats, f, indent=4)


def generate_energy_forcing_statistics_from_filepath(
    flights_with_ef_dir: Path,
    output_filename_json: str,
    first_day: int,
    final_day: int,
) -> None:
    """Generate energy forcing statistics from calculated energy forcing data.

    Args:
        flights_with_ef_dir: Directory containing parquet files with flight data
          with energy forcing calculated.
        output_filename_json: Name of the output JSON file to save the statistics,
        (without extension).
        first_day: The first day to include in the statistics.
        final_day: The final day to include in the statistics.
    """
    start = time.time()
    energy_forcing_paraquet_files = sorted(
        flights_with_ef_dir.glob("UK_flights_day_00*_with_ef.parquet")
    )
    complete_flight_dataframe: pl.DataFrame = pl.DataFrame()
    logger.info("Found %s files in directory.", len(energy_forcing_paraquet_files))
    logger.info("Generating Statistics from %s files.", final_day - first_day + 1)
    for parquet_file in energy_forcing_paraquet_files[first_day - 1 : final_day]:
        # read and append all dataframes together
        daily_dataframe = pl.read_parquet(parquet_file)
        if complete_flight_dataframe.is_empty():
            complete_flight_dataframe = daily_dataframe
        else:
            complete_flight_dataframe = pl.concat([complete_flight_dataframe, daily_dataframe])

    logger.info(
        "Generating energy forcing statistics from %s to %s",
        complete_flight_dataframe["timestamp"].min(),
        complete_flight_dataframe["timestamp"].max(),
    )
    logger.info(
        "Total number of flights in the dataframe: %d",
        complete_flight_dataframe["flight_id"].n_unique(),
    )

    generate_energy_forcing_statistics(complete_flight_dataframe, output_filename_json)

    end = time.time()
    length = end - start
    logger.info(
        "Energy forcing statistics generation completed in %.1f minutes.", round(length / 60, 1)
    )


if __name__ == "__main__":
    ADS_B_ANALYSIS_DIR = Path("~/ads_b_analysis").expanduser()
    SAVE_FLIGHTS_WITH_EF_DIR = ADS_B_ANALYSIS_DIR / "ads_b_flights_with_ef"
    energy_forcing_statistics_json = "energy_forcing_statistics_week_1_2024"
    first_day = 1
    final_day = 7
    generate_energy_forcing_statistics_from_filepath(
        SAVE_FLIGHTS_WITH_EF_DIR, energy_forcing_statistics_json, first_day, final_day
    )
