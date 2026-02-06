"""Processing flight data from ADS-B sources into dataframes suitable for contrail modeling."""

from __future__ import annotations

import datetime
import enum
import logging
import math

import numpy as np
import polars as pl

from aia_model_contrail_avoidance.config import ADS_B_SCHEMA_CLEANED
from aia_model_contrail_avoidance.core_model.airports import list_of_uk_airports
from aia_model_contrail_avoidance.core_model.flights import flight_distance_from_location_vectorized

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# Use a named logger
logger = logging.getLogger(__name__)


# Global constants and enums for flight data processing
MAX_DISTANCE_BETWEEN_FLIGHT_TIMESTAMPS = 3.0  # nautical miles
LOW_FLIGHT_LEVEL_THRESHOLD = 20.0  # flight level 20 = 2000 feet

# Set to True to merge datapoints that are very close together in space
BOOL_MERGE_CLOSE_POINTS = False
# Set to True to interpolate new datapoints for flights with large distance flown in segment
BOOL_INTERPOLATE_LARGE_DISTANCE_FLIGHTS = False


class FlightDepartureAndArrivalSubset(enum.Enum):
    """Enum for selecting subsets of flight data based on departure and arrival airports."""

    ALL = "all"
    UK = "flights to and from the UK"
    REGIONAL = "regional"


class TemporalFlightSubset(enum.Enum):
    """Enum for selecting subsets of flight data based on time periods."""

    ALL = "all"
    FIRST_MONTH = "first_month"


def process_ads_b_flight_data(
    parquet_file_path: str,
    path_to_save_file: str,
    departure_and_arrival_subset: FlightDepartureAndArrivalSubset,
    temporal_subset: TemporalFlightSubset,
) -> None:
    """Processes ADS-B flight data from a parquet file and saves the cleaned DataFrame.

    Args:
        parquet_file_path: Path to the parquet file containing ADS-B flight data.
        path_to_save_file: Path to save the processed parquet file.
        departure_and_arrival_subset: Enum specifying the departure and arrival airport subset.
        temporal_subset: Enum specifying the temporal subset of the data.
    """
    dataframe = generate_flight_dataframe_from_ads_b_data(parquet_file_path)

    selected_dataframe = select_subset_of_ads_b_flight_data(
        dataframe, departure_and_arrival_subset, temporal_subset
    )
    cleaned_dataframe = clean_ads_b_flight_dataframe(selected_dataframe)

    process_ads_b_flight_data_for_environment(cleaned_dataframe, path_to_save_file)

    flight_info_database_save_path = str(path_to_save_file).replace(
        ".parquet", "_flight_info.parquet"
    )

    generate_flight_info_database(path_to_save_file, flight_info_database_save_path)


def generate_flight_dataframe_from_ads_b_data(parquet_file_path: str) -> pl.DataFrame:
    """Reads ADS-B flight data into a DataFrame and removes unnecessary columns.

    Args:
        parquet_file_path: Path to the parquet file containing ADS-B flight data.

    Returns:
        DataFrame containing ADS-B flight data.
    """
    flight_dataframe = pl.read_parquet(parquet_file_path)
    needed_columns = [
        "timestamp",
        "latitude",
        "longitude",
        "altitude_baro",
        "flight_id",
        "icao_address",
        "departure_airport_icao",
        "arrival_airport_icao",
    ]
    logger.info("Loaded flight dataframe with %d rows.", len(flight_dataframe))

    return flight_dataframe.select(needed_columns)


def select_subset_of_ads_b_flight_data(
    flight_dataframe: pl.DataFrame,
    departure_and_arrival_subset: FlightDepartureAndArrivalSubset,
    temporal_subset: TemporalFlightSubset,
) -> pl.DataFrame:
    """Selects a subset of columns from the ADS-B flight data DataFrame.

    Args:
        flight_dataframe: DataFrame containing ADS-B flight data.
        departure_and_arrival_subset: Enum specifying the departure and arrival airport subset.
        temporal_subset: Enum specifying the temporal subset of the data.

    Returns:
        DataFrame containing a subset of the original ADS-B flight data.
    """
    if temporal_subset == TemporalFlightSubset.FIRST_MONTH:
        flight_dataframe = flight_dataframe.filter(
            (pl.col("timestamp") >= pl.datetime(2024, 1, 1))
            & (pl.col("timestamp") < pl.datetime(2024, 2, 1))
        )

    if departure_and_arrival_subset == FlightDepartureAndArrivalSubset.UK:
        uk_airport_icaos = list_of_uk_airports()
        flight_dataframe = flight_dataframe.filter(
            pl.col("arrival_airport_icao").is_in(uk_airport_icaos)
            | pl.col("departure_airport_icao").is_in(uk_airport_icaos)
        )

    elif departure_and_arrival_subset == FlightDepartureAndArrivalSubset.REGIONAL:
        uk_airport_icaos = list_of_uk_airports()
        flight_dataframe = flight_dataframe.filter(
            pl.col("arrival_airport_icao").is_in(uk_airport_icaos)
            & pl.col("departure_airport_icao").is_in(uk_airport_icaos)
        )

    logger.info("After selecting subsets, the flight dataframe has %d rows.", len(flight_dataframe))
    return flight_dataframe


def clean_ads_b_flight_dataframe(flight_dataframe: pl.DataFrame) -> pl.DataFrame:
    """Cleans the flight DataFrame by adding necessary columns and removing unnecessary ones.

    New columns added:
    - flight_level: Altitude of aircraft in term of flight levels (altitude_baro divided by 100)
    - distance_flown_in_segment: Distance traveled in meters between consecutive datapoints for the
        same flight

    Augmented rows:
    - For any row where distance_flown_in_segment exceeds a threshold (e.g. 50 nautical miles), new
        rows are generated with interpolated values for latitude, longitude, and timestamp to ensure
        no segment exceeds the threshold.

    Args:
        flight_dataframe: DataFrame containing ADS-B flight data.

    Returns:
        Cleaned DataFrame with added columns.
    """
    # Divide altitude_baro by 100 to convert from pha to flight level
    flight_dataframe = flight_dataframe.with_columns(
        (pl.col("altitude_baro") // 100.0).alias("flight_level")
    )

    # Drop the original altitude_baro column
    flight_dataframe = flight_dataframe.drop("altitude_baro")

    # order by flight id and timestamp
    flight_dataframe = flight_dataframe.sort(["flight_id", "timestamp"])

    # Calculate distance_flown_in_segment using window functions
    flight_dataframe = flight_dataframe.with_columns(
        [
            pl.col("latitude").shift(1).over("flight_id").alias("prev_lat"),
            pl.col("longitude").shift(1).over("flight_id").alias("prev_lon"),
            pl.col("timestamp").shift(1).over("flight_id").alias("prev_timestamp"),
        ]
    )

    # Calculate distances traveled in each segment using datafames
    flight_dataframe = flight_dataframe.with_columns(
        pl.Series(
            "distance_flown_in_segment",
            flight_distance_from_location_vectorized(
                pl.Series(flight_dataframe["latitude"]),
                pl.Series(flight_dataframe["longitude"]),
                pl.Series(flight_dataframe["prev_lat"]),
                pl.Series(flight_dataframe["prev_lon"]),
            ),
        )
    )

    # remove columns where distance_flown_in_segment is zero
    flight_dataframe = flight_dataframe.filter(pl.col("distance_flown_in_segment") > 0)

    # fill altitude nulls with previous value for that flight
    flight_dataframe = flight_dataframe.with_columns(
        pl.col("flight_level").fill_null(strategy="forward").over("flight_id")
    )
    flight_dataframe = flight_dataframe.select(
        [
            "timestamp",
            "latitude",
            "longitude",
            "flight_level",
            "flight_id",
            "icao_address",
            "departure_airport_icao",
            "arrival_airport_icao",
            "distance_flown_in_segment",
            "prev_lat",
            "prev_lon",
            "prev_timestamp",
        ]
    )

    if BOOL_INTERPOLATE_LARGE_DISTANCE_FLIGHTS:
        # for large distance_flown_in_segment, create new rows with interpolated values
        flight_dataframe = generate_interpolated_rows_of_large_distance_flights(
            flight_dataframe, max_distance=MAX_DISTANCE_BETWEEN_FLIGHT_TIMESTAMPS
        )

    length_after_cleaning = len(flight_dataframe)
    logger.info("After cleaning, the flight dataframe has %d rows.", length_after_cleaning)

    # reorganise columns
    flight_dataframe = flight_dataframe.select(
        [
            "timestamp",
            "latitude",
            "longitude",
            "flight_level",
            "flight_id",
            "icao_address",
            "departure_airport_icao",
            "arrival_airport_icao",
            "distance_flown_in_segment",
        ]
    )
    if BOOL_MERGE_CLOSE_POINTS:
        # Merge datapoints that are very close together in space
        # (the sum of their distances to previous and next points is less than the threshold)

        flight_dataframe = merge_close_datapoints_of_flight(
            flight_dataframe, MAX_DISTANCE_BETWEEN_FLIGHT_TIMESTAMPS
        )

        length_after_merging = len(flight_dataframe)
        logger.info(
            "After merging very close points, the flight dataframe has %d rows.",
            length_after_merging,
        )
        logger.info(
            "Total of %d rows removed by merging very close points.",
            length_after_cleaning - length_after_merging,
        )

    return flight_dataframe


def generate_interpolated_rows_of_large_distance_flights(
    flight_dataframe: pl.DataFrame, max_distance: float = 15.0
) -> pl.DataFrame:
    """Generates interpolated rows for flights with large distance flown in segment.

    Args:
        flight_dataframe: DataFrame containing ADS-B flight data.
        max_distance: Maximum distance in nautical miles before interpolation is needed.

    Returns:
        DataFrame with interpolated rows added.
    """
    flight_dataframe = flight_dataframe.sort(["flight_id", "timestamp"])

    # filter out rows where distance_flown_in_segment exceeds max_distance
    flight_dataframe_with_large_distances = flight_dataframe.filter(
        (pl.col("distance_flown_in_segment") > max_distance)
        & (pl.col("prev_lat").is_not_null())
        & (pl.col("prev_lon").is_not_null())
    )
    flight_dataframe = flight_dataframe.filter(
        (pl.col("distance_flown_in_segment") < max_distance)
        | pl.col("prev_lat").is_null()
        | pl.col("prev_lon").is_null()
    ).drop(["prev_lat", "prev_lon", "prev_timestamp"])

    for row in flight_dataframe_with_large_distances.iter_rows(named=True):
        # calculate intervals for each row
        num_new_rows = math.ceil(row["distance_flown_in_segment"] / max_distance)
        time_step = (row["timestamp"] - row["prev_timestamp"]).total_seconds() / (num_new_rows + 1)
        distance_flown_in_segment_step = row["distance_flown_in_segment"] / (num_new_rows + 1)
        # create new rows
        latitudes = np.linspace((row["prev_lat"]), (row["latitude"]), num_new_rows)
        longitudes = np.linspace((row["prev_lon"]), (row["longitude"]), num_new_rows)
        timestamps = [
            row["prev_timestamp"] + datetime.timedelta(seconds=i * time_step)
            for i in range(num_new_rows)
        ]
        rows_to_add = pl.DataFrame(
            {
                "timestamp": timestamps,
                "latitude": latitudes,
                "longitude": longitudes,
                "flight_level": [row["flight_level"]] * num_new_rows,
                "flight_id": [row["flight_id"]] * num_new_rows,
                "icao_address": [row["icao_address"]] * num_new_rows,
                "departure_airport_icao": [row["departure_airport_icao"]] * num_new_rows,
                "arrival_airport_icao": [row["arrival_airport_icao"]] * num_new_rows,
                "distance_flown_in_segment": [distance_flown_in_segment_step] * num_new_rows,
            },
            schema=ADS_B_SCHEMA_CLEANED,
        )

        # add new rows to dataframe
        flight_dataframe = pl.concat([flight_dataframe, rows_to_add], how="vertical")

    # sort by flight_id and timestamp
    return flight_dataframe.sort(["flight_id", "timestamp"])


def process_ads_b_flight_data_for_environment(
    generated_dataframe: pl.DataFrame, save_path: str
) -> None:
    """Process ADS-B flight data and save cleaned DataFrame to parquet.

    Removes datapoints with low flight levels (near or on ground)

    Args:
        generated_dataframe: DataFrame containing raw ADS-B flight data.
        save_path: Path to save the processed parquet file.
    """
    # Remove datapoints where flight level is none or negative
    dataframe_processed = generated_dataframe.filter(
        pl.col("flight_level").is_not_null()
        & (pl.col("flight_level") >= LOW_FLIGHT_LEVEL_THRESHOLD)
    )

    # percentage of datapoints removed
    percentage_removed = 100 * (1 - len(dataframe_processed) / len(generated_dataframe))
    logger.info("Removed %.2f%% of datapoints due to low flight level", percentage_removed)
    # Save processed dataframe to parquet
    dataframe_processed.write_parquet(save_path)


def generate_flight_info_database(processed_parquet_path: str, save_path: str) -> None:
    """Generates a flight information database from processed ADS-B data."""
    flight_dataframe = pl.read_parquet(processed_parquet_path)

    # Extract unique flight information
    flight_info_dataframe = flight_dataframe.select(
        [
            "flight_id",
            "icao_address",
            "departure_airport_icao",
            "arrival_airport_icao",
        ]
    ).unique()

    # add new colums for first message timestamp and last message timestamp
    first_timestamps = flight_dataframe.group_by("flight_id").agg(
        pl.col("timestamp").min().alias("first_message_timestamp")
    )
    last_timestamps = flight_dataframe.group_by("flight_id").agg(
        pl.col("timestamp").max().alias("last_message_timestamp")
    )
    number_of_messages = flight_dataframe.group_by("flight_id").agg(
        pl.count().alias("number_of_messages")
    )
    flight_info_dataframe = (
        flight_info_dataframe.join(first_timestamps, on="flight_id")
        .join(last_timestamps, on="flight_id")
        .join(number_of_messages, on="flight_id")
    )

    # Save flight information database to parquet
    flight_info_dataframe.write_parquet(save_path)


def merge_close_datapoints_of_flight(
    flight_dataframe: pl.DataFrame,
    distance_threshold: float,
) -> pl.DataFrame:
    """Merges close datapoints of a flight based on distance threshold.

    Args:
        flight_dataframe: DataFrame containing ADS-B flight data.
        distance_threshold: Distance threshold in nautical miles for merging datapoints.

    Returns:
        DataFrame with merged datapoints.
    """
    flight_dataframe = flight_dataframe.sort(["flight_id", "timestamp"])
    flight_dataframe = flight_dataframe.with_columns(
        pl.col("distance_flown_in_segment")
        .shift(1)
        .over("flight_id")
        .alias("prev_distance_flown_in_segment")
    )
    flight_dataframe = flight_dataframe.with_columns(
        pl.col("distance_flown_in_segment")
        + pl.col("prev_distance_flown_in_segment")
        .fill_null(0.0)
        .alias("total_distance_of_current_and_previous")
    )
    # if total_distance is over threshold set to none-- these rows will not be merged
    flight_dataframe = flight_dataframe.with_columns(
        pl.when(pl.col("total_distance_of_current_and_previous") > distance_threshold)
        .then(None)
        .otherwise(pl.col("total_distance_of_current_and_previous"))
        .alias("merged_distance_with_previous")
    )
    # flag previous row for merging
    flight_dataframe = flight_dataframe.with_columns(
        pl.col("merged_distance_with_previous")
        .shift(-1)
        .over("flight_id")
        .alias("merge_flag_to_delete_row")
    )
    # if both merge flag and merged_distance_with_previous is not null, make both merged_distance_with_previous and merge_flag_to_delete_row null.
    # this means that if many consecutive rows can be merged, the overlapping merges are avoided.
    flight_dataframe = flight_dataframe.with_columns(
        pl.when(
            pl.col("merge_flag_to_delete_row").is_not_null()
            & pl.col("merged_distance_with_previous").is_not_null()
        )
        .then(pl.lit(None))
        .otherwise(pl.col("merge_flag_to_delete_row"))
        .alias("final_flag_to_delete_row")
    )
    # remove merged_distance_with_previous if final_flag_to_delete_row is not null
    flight_dataframe = flight_dataframe.with_columns(
        pl.when(pl.col("final_flag_to_delete_row").is_not_null())
        .then(None)
        .otherwise(pl.col("merged_distance_with_previous"))
        .alias("merged_distance_with_previous")
    )
    # update distance_flown_in_segment to merged_distance_with_previous if not null
    flight_dataframe = flight_dataframe.with_columns(
        pl.when(pl.col("merged_distance_with_previous").is_not_null())
        .then(pl.col("merged_distance_with_previous"))
        .otherwise(pl.col("distance_flown_in_segment"))
        .alias("distance_flown_in_segment")
    )
    # drop rows where final_flag_to_delete_row is not null
    flight_dataframe = flight_dataframe.filter(pl.col("final_flag_to_delete_row").is_null())

    return flight_dataframe.drop(
        "prev_distance_flown_in_segment",
        "total_distance_of_current_and_previous",
        "merged_distance_with_previous",
        "merge_flag_to_delete_row",
        "final_flag_to_delete_row",
    )
