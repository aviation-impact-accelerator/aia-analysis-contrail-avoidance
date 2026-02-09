"""Add unique flight id to dataframe using polars."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from timeit import default_timer
from typing import Literal

import polars as pl

from aia_model_contrail_avoidance.config import (
    ADS_B_PARQUET_INPUT_SCHEMA,
    ADS_B_PARQUET_OUTPUT_SCHEMA_WITH_FLIGHT_ID,
)
from aia_model_contrail_avoidance.core_model.airports import list_of_uk_airports

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# Input: flight daraframe parquet files for the week without flight IDs
FLIGHT_DATAFRAME_DIR = Path("~/ads_b").expanduser()

# Output: clean flights with flight IDs
FLIGHTS_WITH_IDS_DIR = Path("~/ads_b_with_flight_ids").expanduser()


# TAKEN FROM PETERS CODE AND KEPT FOR REFERENCE
@dataclass
class FlightSegmentationConfig:
    """Configuration parameters for flight segmentation logic."""

    # "Soft" in-air gap where we need consistency checks
    soft_gap_minutes: float = 45.0
    # Long ground gap between flights
    long_ground_gap_minutes: float = 50.0
    # Hard "always new flight" gap (currently used)
    hard_gap_hours: float = 6.0
    # Big spatial jump threshold (km)
    max_jump_km: float = 500.0
    # "Same heading" threshold (deg) for in-air continuity
    same_heading_deg: float = 90.0


def filter_and_fill_origin_destination_pair(
    flight_dataframe: pl.DataFrame,
) -> pl.DataFrame:
    """Filter out rows with missing origin and destination airports, and fill forward/backward.

    Args:
        flight_dataframe: polars DataFrame sorted by timestamp.
            required columns: departure_airport_icao, arrival_airport_icao, icao_address.

    Returns: dataframe with filled origin and destination airport columns.
    """
    # remove rows where both departure and arrival airports are missing
    flight_dataframe_filtered = flight_dataframe.filter(
        pl.col("departure_airport_icao").is_not_null().over("icao_address")
        & pl.col("arrival_airport_icao").is_not_null().over("icao_address")
    )

    logger.debug(
        "After filtering out rows with missing origin and destination airports, there are %d unique aircraft",
        len(flight_dataframe_filtered["icao_address"].unique()),
    )

    # fill forward and backward the missing departure and arrival airports within each icao_address group
    return flight_dataframe_filtered.with_columns(
        [
            pl.col("departure_airport_icao")
            .fill_null(strategy="forward")
            .fill_null(strategy="backward")
            .over("icao_address"),
            pl.col("arrival_airport_icao")
            .fill_null(strategy="forward")
            .fill_null(strategy="backward")
            .over("icao_address"),
        ]
    )


def add_unique_flight_identifier(
    flight_dataframe: pl.DataFrame,
) -> pl.DataFrame:
    """Add unique_flight_identifier column using: aircraft id, departure and arrival airports.

    Args:
        flight_dataframe: polars DataFrame sorted by timestamp.
            required columns: icao_address, departure_airport_icao, arrival_airport_icao.

    Returns: dataframe with new "unique_flight_identifier" column.
    """
    return flight_dataframe.with_columns(
        (
            pl.col("icao_address").cast(pl.Utf8)
            + "_"
            + pl.col("departure_airport_icao").cast(pl.Utf8)
            + "_"
            + pl.col("arrival_airport_icao").cast(pl.Utf8)
        ).alias("unique_flight_identifier")
    )


def create_flight_info_dataframe_for_latest_flights(
    output_dataframe: pl.DataFrame,
) -> pl.DataFrame:
    """Create flight info dataframe with last timestamps per flight_id within the last 6 hours.

    Args:
        output_dataframe: polars DataFrame with flight_id column.
            required columns: icao_address, departure_airport_icao, arrival_airport_icao, flight_id,
            timestamp.

    Returns: information dataframe with last timestamps per flight_id within the last 6 hours.
    """
    if output_dataframe.is_empty():
        return pl.DataFrame()

    required_columns = {
        "icao_address",
        "departure_airport_icao",
        "arrival_airport_icao",
        "flight_id",
        "timestamp",
    }
    missing = required_columns - set(output_dataframe.columns)
    if missing:
        msg = f"Missing required columns in output_dataframe: {missing}"
        raise ValueError(msg)

    max_timestamp = output_dataframe.select(pl.col("timestamp").max()).to_series()[0]
    threshold = max_timestamp - pl.duration(hours=6)
    latest_flights_info_dataframe = (
        output_dataframe.group_by(
            ["flight_id", "icao_address", "departure_airport_icao", "arrival_airport_icao"]
        )
        .agg(pl.col("timestamp").max().alias("last_timestamp"))
        .filter(pl.col("last_timestamp") >= threshold)
    )
    # create unique flight identifier in latest_flights_info_dataframe
    return add_unique_flight_identifier(latest_flights_info_dataframe)


def remove_non_uk_flights(
    output_dataframe: pl.DataFrame,
) -> pl.DataFrame:
    """Remove non-UK flights (when both departure and arrival airports are not in UK).

    Args:
        output_dataframe: polars DataFrame with flight data.
            required columns: departure_airport_icao, arrival_airport_icao.

    Returns: dataframe with only UK flights.
    """
    uk_airport_icaos = list_of_uk_airports()
    return output_dataframe.filter(
        pl.col("departure_airport_icao").is_in(uk_airport_icaos)
        | pl.col("arrival_airport_icao").is_in(uk_airport_icaos)
    )


def remove_erroneous_single_point_flights(
    flight_dataframe_with_flight_id: pl.DataFrame,
) -> pl.DataFrame:
    """Remove flights that consist of only a few data points.

    Args:
        flight_dataframe_with_flight_id: polars DataFrame of flight data.
            required columns: icao_address, timestamp,first_flight_id, unique_flight_identifier.

    Returns: dataframe with single-point flights removed and first_flight_id refactored.
    """
    # order by icao_address and timestamp
    flight_dataframe_with_flight_id = flight_dataframe_with_flight_id.sort(
        ["icao_address", "timestamp"]
    )
    # get flight IDs that occur less than the minimum number of consecutive datapoints threshold
    min_number_of_consecutive_datapoints = 3
    unique_flights_with_high_counts = (
        flight_dataframe_with_flight_id.group_by("first_flight_id")
        .agg(pl.len().alias("count"))
        .filter(pl.col("count") > min_number_of_consecutive_datapoints)
        .select("first_flight_id")
        .to_series()
        .to_list()
    )
    logger.debug(
        "number of unique flights before removing erroneous single-point flights: %d",
        len(flight_dataframe_with_flight_id["first_flight_id"].unique()),
    )
    # remove timestamps with these flight IDs
    flight_dataframe_with_flight_id = flight_dataframe_with_flight_id.filter(
        pl.col("first_flight_id").is_in(unique_flights_with_high_counts)
    )
    logger.debug(
        "number of unique flights after removing erroneous single-point flights: %d",
        len(flight_dataframe_with_flight_id["first_flight_id"].unique()),
    )

    # refactor first_flight_id to be consecutive after removing single-point flights
    flight_dataframe_with_flight_id = flight_dataframe_with_flight_id.with_columns(
        pl.struct(["unique_flight_identifier"]).rle_id().alias("first_flight_id")
    )
    logger.debug(
        "After removing erroneous flights, there are %d unique flights",
        len(flight_dataframe_with_flight_id["first_flight_id"].unique()),
    )
    return flight_dataframe_with_flight_id


def segment_dataframe_into_new_and_continued_flights(
    previous_flight_info_dataframe: pl.DataFrame,
    flight_dataframe_with_unique_flight_identifier: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Segment flight dataframe into new flights and continued flights using data from previous run.

    Args:
        previous_flight_info_dataframe: polars DataFrame with last timestamps per flight_id from
            previous flight info.
            required columns: icao_address, unique_flight_identifier, flight_id.
        flight_dataframe_with_unique_flight_identifier: polars DataFrame with unique flight identifier column.
            required columns: icao_address, unique_flight_identifier, timestamp.

    Returns: tuple of dataframes (continued_flights_dataframe, new_flights_dataframe_with_unique_flight_identifier).
    """
    # get first timestamp per icao_address in the flight dataframe within the next 6 hours
    next_flight_per_aircraft_from_flight_dataframe = (
        flight_dataframe_with_unique_flight_identifier.group_by(
            "icao_address", "unique_flight_identifier"
        ).agg(pl.col("timestamp").min().alias("first_timestamp"))
    ).filter(pl.col("first_timestamp") <= (pl.col("first_timestamp").min() + pl.duration(hours=6)))

    # the unique_flight_identifier has to match the flight_info_dataframe
    continued_flights_info_dataframe = next_flight_per_aircraft_from_flight_dataframe.join(
        previous_flight_info_dataframe,
        on=["icao_address", "unique_flight_identifier"],
        how="inner",
    )
    # keep only necessary columns
    continued_flights_info_dataframe = continued_flights_info_dataframe.select(
        ["icao_address", "unique_flight_identifier", "first_timestamp", "flight_id"]
    )
    # change first_timestamp column name to timestamp for joining
    continued_flights_info_dataframe = continued_flights_info_dataframe.rename(
        {"first_timestamp": "timestamp"}
    )

    # segment data into continued flights from previous flight info and new flights based on unique identifier
    flight_dataframe_with_previous_flight_id = flight_dataframe_with_unique_flight_identifier.join(
        continued_flights_info_dataframe,
        on=["icao_address", "unique_flight_identifier", "timestamp"],
        how="left",
    )
    # fill in flight_id for continued flights
    flight_dataframe_with_previous_flight_id = (
        flight_dataframe_with_previous_flight_id.with_columns(
            pl.col("flight_id").fill_null(strategy="forward").over("icao_address")
        )
    )
    # remove rows where there is no flight_id (these are new flights)
    continued_flights_dataframe = flight_dataframe_with_previous_flight_id.filter(
        pl.col("flight_id").is_not_null()
    )
    new_flights_dataframe_with_unique_flight_identifier = (
        flight_dataframe_with_previous_flight_id.filter(pl.col("flight_id").is_null()).drop(
            "flight_id"
        )
    )
    return continued_flights_dataframe, new_flights_dataframe_with_unique_flight_identifier


def seperate_flight_id_for_large_time_gaps(
    flight_dataframe_with_flight_id: pl.DataFrame,
    config: FlightSegmentationConfig,
) -> pl.DataFrame:
    """Separate flight IDs for large time gaps within the same flight.

    Args:
        flight_dataframe_with_flight_id: polars DataFrame with flight data.
            required columns: first_flight_id, timestamp.
        config: FlightSegmentationConfig with parameters for segmentation logic.

    Returns: dataframe with updated "flight_id" column.
    """
    # order by icao_address and timestamp
    flight_dataframe_with_flight_id = flight_dataframe_with_flight_id.sort(
        ["icao_address", "timestamp"]
    )
    # for each flight, check if there are any time gaps larger than the threshold
    # gt() returns boolean series where True indicates a gap larger than threshold
    flight_dataframe_with_flight_id = flight_dataframe_with_flight_id.with_columns(
        pl.col("timestamp")
        .diff()
        .gt(config.hard_gap_hours * pl.duration(hours=1))
        .over("first_flight_id")
        .alias("time_gap_flight_increment")
    )
    # cum_sum() over the whole dataframe to increment for each time gap found
    flight_dataframe_with_flight_id = flight_dataframe_with_flight_id.with_columns(
        pl.col("time_gap_flight_increment").cum_sum().alias("time_gap_flight_increment")
    )
    # this is added to first_flight_id to create final flight_id
    flight_dataframe_with_flight_id = flight_dataframe_with_flight_id.with_columns(
        (
            pl.col("first_flight_id").cast(pl.Int32)
            + pl.col("time_gap_flight_increment").cast(pl.Int32)
        ).alias("flight_id")
    )
    logger.debug(
        "After separating flight IDs for large time gaps, there are %d unique flights",
        len(flight_dataframe_with_flight_id["flight_id"].unique()),
    )
    logger.debug("smallest flight_id: %d", flight_dataframe_with_flight_id["flight_id"].min())
    logger.debug("largest flight_id: %d", flight_dataframe_with_flight_id["flight_id"].max())

    return flight_dataframe_with_flight_id.drop(
        "unique_flight_identifier", "first_flight_id", "time_gap_flight_increment"
    )


def assign_flight_id_to_unique_flights(
    flight_dataframe: pl.DataFrame,
    config: FlightSegmentationConfig,
    previous_flight_info_dataframe: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Segment flights based on message data in the dataframe.

    Args:
        flight_dataframe: polars DataFrame sorted by timestamp.
        previous_flight_info_dataframe: polars DataFrame with last timestamps per flight_id from
            previous flight.
        config: FlightSegmentationConfig with parameters for segmentation logic.

    Returns: dataframe with new "flight_id" column.
    """
    if flight_dataframe.is_empty():
        return flight_dataframe.with_columns(pl.lit(None, pl.Int32).alias("flight_id"))

    # Ensure chronological order and per-aircraft processing and encure correct types
    flight_dataframe = flight_dataframe.sort(["icao_address", "timestamp"])

    # remove datapoints where departure and arrival airports are missing for the whole aircraft
    flight_dataframe_filled_origin_destination_pair = filter_and_fill_origin_destination_pair(
        flight_dataframe
    )

    # create origin-destination pair column with calsign to identify unique flights
    flight_dataframe_with_unique_flight_identifier = add_unique_flight_identifier(
        flight_dataframe_filled_origin_destination_pair
    )

    # if flight info from previous flight is provided, use it to continue flight IDs
    if previous_flight_info_dataframe is not None and not previous_flight_info_dataframe.is_empty():
        # next flight_id to assign
        next_flight_id = (
            previous_flight_info_dataframe.select(pl.col("flight_id").max()).item()
        ) + 1
        logger.info(
            "Continuing flight IDs from previous chunk, starting with flight_id %d",
            next_flight_id,
        )
        # create unique flight identifier in previous flight info dataframe
        continued_flights_dataframe, new_flights_dataframe_with_unique_flight_identifier = (
            segment_dataframe_into_new_and_continued_flights(
                previous_flight_info_dataframe, flight_dataframe_with_unique_flight_identifier
            )
        )
        continued_flights_dataframe = continued_flights_dataframe.drop(
            "unique_flight_identifier",
        )
    else:
        # start flight_id from 0
        next_flight_id = 0
        continued_flights_dataframe = pl.DataFrame(
            [], schema=flight_dataframe_with_unique_flight_identifier.schema
        )
        # add flight id column
        continued_flights_dataframe = continued_flights_dataframe.with_columns(
            pl.lit(None, pl.Int32).alias("flight_id")
        ).drop("unique_flight_identifier")
        new_flights_dataframe_with_unique_flight_identifier = (
            flight_dataframe_with_unique_flight_identifier
        )

    # assign flight_id based on unique icao_addresses, O-D pairs
    flight_dataframe_with_flight_id = new_flights_dataframe_with_unique_flight_identifier.sort(
        ["icao_address", "timestamp"]
    ).with_columns(
        pl.struct(["icao_address", "unique_flight_identifier"]).rle_id().alias("first_flight_id")
    )

    # if first_flight_id occurs only once, remove it to avoid erroneous metadata switches
    flight_dataframe_with_flight_id = remove_erroneous_single_point_flights(
        flight_dataframe_with_flight_id
    )
    # add next_flight_id to first_flight_id to ensure unique flight IDs across flights
    flight_dataframe_with_flight_id = flight_dataframe_with_flight_id.with_columns(
        (pl.col("first_flight_id") + int(next_flight_id)).alias("first_flight_id")
    )
    logger.debug(
        "After adding next_flight_id, largest first_flight_id: %d",
        flight_dataframe_with_flight_id["first_flight_id"].max(),
    )

    # for each flight, check if there are any time gaps larger than the threshold, and increment flight_id accordingly
    flight_dataframe_with_flight_id = seperate_flight_id_for_large_time_gaps(
        flight_dataframe_with_flight_id, config
    )

    # combine continued flights and new flights with flight IDs
    return pl.concat(
        [continued_flights_dataframe, flight_dataframe_with_flight_id], how="vertical"
    ).sort(["icao_address", "timestamp"])


def identify_uk_flights(
    input_files: list[Path],
    output_dir: Path,
    *,
    config: FlightSegmentationConfig,
    compression: Literal["lz4", "uncompressed", "snappy", "gzip", "brotli", "zstd"] = "zstd",
) -> None:
    """For each parquet file in directory, identify flights and assign flight IDs."""
    if config is None:
        config = FlightSegmentationConfig()

    overall_start = default_timer()
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        "Running flight identification for %d flight data files",
        len(input_files),
    )
    logger.info("Chunking 5 files at a time")
    logger.info("Output dir: %s", output_dir)

    previous_chunk_flight_info_dataframe = pl.DataFrame()

    # create file chunks of 5 files each
    file_chunks = [input_files[i : i + 5] for i in range(0, len(input_files), 5)]
    for file_chunk in file_chunks:
        logger.info("Processing file chunk starting with %s", file_chunk[0].name)
        # open all files in directory and add them to one dataframe
        flight_dataframe = pl.concat(
            [pl.read_parquet(file, schema=ADS_B_PARQUET_INPUT_SCHEMA) for file in file_chunk],
            how="vertical",
        )
        # update dataframe so that timestamp is datetime type
        flight_dataframe = flight_dataframe.with_columns(
            pl.col("timestamp").str.to_datetime(format="%Y-%m-%d %H:%M:%S%.f %Z")
        )

        # assign flight IDs
        output_dataframe = assign_flight_id_to_unique_flights(
            flight_dataframe=flight_dataframe,
            config=FlightSegmentationConfig(),
            previous_flight_info_dataframe=previous_chunk_flight_info_dataframe,
        )

        # extract last timestamp per flight_id within the last 6 hours of data
        previous_chunk_flight_info_dataframe = create_flight_info_dataframe_for_latest_flights(
            output_dataframe=output_dataframe
        )
        # remove non UK flights (both departure and arrival airports not in UK)
        output_dataframe_uk_flights = remove_non_uk_flights(output_dataframe)

        # Save output dataframe to parquet files
        # Partition output by day (calculated from timestamp)
        output_dataframe_uk_flights = output_dataframe_uk_flights.with_columns(
            pl.col("timestamp").dt.ordinal_day().alias("flight_day")
        )
        for day, day_df in output_dataframe_uk_flights.partition_by(
            "flight_day", as_dict=True
        ).items():
            day_str = str(day[0]).zfill(3)
            day_file = output_dir / f"UK_flights_day_{day_str}.parquet"
            # if file exists, append to it
            if day_file.exists():
                existing_day_df = pl.read_parquet(
                    day_file, schema=ADS_B_PARQUET_OUTPUT_SCHEMA_WITH_FLIGHT_ID
                )
                day_df = pl.concat([existing_day_df, day_df], how="vertical")  # noqa: PLW2901
            day_df.write_parquet(day_file, compression=compression)

    elapsed = default_timer() - overall_start
    logger.info(
        "Flight identification complete in %dm %.1fs",
        int(elapsed // 60),
        elapsed % 60,
    )


if __name__ == "__main__":
    logging.basicConfig(
        # logging options: DEBUG, INFO, WARNING, ERROR, CRITICAL
        level=logging.DEBUG,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
    )

    input_files = sorted(FLIGHT_DATAFRAME_DIR.glob("*.parquet"))
    output_dir = FLIGHTS_WITH_IDS_DIR

    first_input_file = [input_files[0]]  # For testing, only process first file
    # Run flight identification
    identify_uk_flights(
        input_files=input_files,
        output_dir=output_dir,
        config=FlightSegmentationConfig(),
    )

    logger.info("Done! Output written to %s", output_dir)
