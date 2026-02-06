"""Generate synthetic flights."""

from __future__ import annotations

__all__ = (
    "flight_distance_from_location",
    "generate_synthetic_flight",
    "most_common_cruise_flight_level",
)

import datetime

import numpy as np
import polars as pl

from aia_model_contrail_avoidance.config import FLIGHT_TIMESTAMPS_SCHEMA


def to_float_numpy(series: pl.Series) -> np.ndarray:
    """Convert a Polars Series to a NumPy array of floats.

    Args:
        series: A Polars Series to convert.

    Returns: A NumPy array of floats.
    """
    if isinstance(series, pl.Series):
        return series.cast(float).to_numpy()
    return np.asarray(series, dtype=float)


def generate_synthetic_flight(  # noqa: PLR0913
    flight_id: int,
    departure_location: tuple[float, float],
    arrival_location: tuple[float, float],
    departure_time: datetime.datetime,
    length_of_flight: float,
    flight_level: int,
) -> pl.DataFrame:
    """Generates synthetic flight from departure to arrival location as a series of timestamps.

    Args:
        flight_id: Unique identifier for the flight.
        departure_location: Tuple of (latitude, longitude) for departure.
        arrival_location: Tuple of (latitude, longitude) for arrival.
        departure_time: Departure time as a datetime object.
        length_of_flight: Length of flight in seconds.
        flight_level: Flight level as the standard pHa.
    """
    distance_traveled_in_nautical_miles = flight_distance_from_location(
        departure_location, arrival_location
    )
    number_of_timestamps = int(distance_traveled_in_nautical_miles)  # 1 nautical mile per timestamp
    latitudes = np.linspace(departure_location[0], arrival_location[0], number_of_timestamps)
    longitudes = np.linspace(departure_location[1], arrival_location[1], number_of_timestamps)
    timestamps = [
        departure_time + datetime.timedelta(seconds=i * (length_of_flight / number_of_timestamps))
        for i in range(number_of_timestamps)
    ]

    return pl.DataFrame(
        {
            "flight_id": np.full(number_of_timestamps, flight_id, dtype=int),
            "departure_location": [list(departure_location)] * number_of_timestamps,
            "arrival_location": [list(arrival_location)] * number_of_timestamps,
            "departure_time": [departure_time] * number_of_timestamps,
            "timestamp": timestamps,
            "latitude": latitudes,
            "longitude": longitudes,
            "flight_level": np.full(number_of_timestamps, flight_level, dtype=int),
            "distance_flown_in_segment": np.full(number_of_timestamps, 1.0, dtype=float),
        },
        schema=FLIGHT_TIMESTAMPS_SCHEMA,
    )


def flight_distance_from_location_vectorized(
    departure_lat: np.ndarray,
    departure_long: np.ndarray,
    arrival_lat: np.ndarray,
    arrival_long: np.ndarray,
) -> np.ndarray:
    """Calculates the distance between two arrays of locations using the Haversine formula.

    This is the same as the great circle distance.

    Args:
        departure_lat: Array of departure latitudes in degrees.
        departure_long: Array of departure longitudes in degrees.
        arrival_lat: Array of arrival latitudes in degrees.
        arrival_long: Array of arrival longitudes in degrees.

    Returns:
        Array of distances in nautical miles.
    """
    earth_radius = 3443.92  # Radius of the Earth in nautical miles

    # Ensure input is np.ndarray of float for compatibility with np.radians

    # Only convert if input is a pl.Series, else assume np.ndarray
    if isinstance(departure_lat, pl.Series):
        departure_lat = to_float_numpy(departure_lat)
    if isinstance(departure_long, pl.Series):
        departure_long = to_float_numpy(departure_long)
    if isinstance(arrival_lat, pl.Series):
        arrival_lat = to_float_numpy(arrival_lat)
    if isinstance(arrival_long, pl.Series):
        arrival_long = to_float_numpy(arrival_long)

    # Check for empty arrays to avoid ShapeError
    if (
        departure_lat.size == 0
        or departure_long.size == 0
        or arrival_lat.size == 0
        or arrival_long.size == 0
    ):
        return np.array([])

    departure_lat = np.radians(departure_lat)
    departure_long = np.radians(departure_long)
    arrival_lat = np.radians(arrival_lat)
    arrival_long = np.radians(arrival_long)

    dlat = arrival_lat - departure_lat
    dlon = arrival_long - departure_long
    a = np.sin(dlat / 2) ** 2 + np.cos(departure_lat) * np.cos(arrival_lat) * np.sin(dlon / 2) ** 2
    c = 2 * np.arcsin(np.sqrt(a))

    return c * earth_radius  # type: ignore[no-any-return]


def flight_distance_from_location(
    departure_location: tuple[float, float] | np.ndarray,
    arrival_location: tuple[float, float] | np.ndarray,
) -> float | np.ndarray:
    """Calculates the distance between two locations using the Haversine formula.

    Args:
        departure_location: Tuple of (latitude, longitude) for departure or array of such tuples.
        arrival_location: Tuple of (latitude, longitude) for arrival or array of such tuples.

    Returns:
        Distance in nautical miles as a float if input is a single tuple, or an array of distances
            if input is arrays of tuples.
    """
    earth_radius = 3443.92  # Radius of the Earth in nautical miles
    _tuple_length = 2

    departure_location = np.atleast_1d(departure_location)
    arrival_location = np.atleast_1d(arrival_location)

    # Handle scalar or tuple inputs
    if departure_location.ndim == 1 and len(departure_location) == _tuple_length:
        departure_location = departure_location.reshape(1, -1)
    if arrival_location.ndim == 1 and len(arrival_location) == _tuple_length:
        arrival_location = arrival_location.reshape(1, -1)

    departure_latitude, departure_longitude = np.radians(departure_location).T
    arrival_latitude, arrival_longitude = np.radians(arrival_location).T

    dlat = arrival_latitude - departure_latitude
    dlon = arrival_longitude - departure_longitude
    a = (
        np.sin(dlat / 2) ** 2
        + np.cos(departure_latitude) * np.cos(arrival_latitude) * np.sin(dlon / 2) ** 2
    )
    c = 2 * np.arcsin(np.sqrt(a))

    result = earth_radius * c
    return float(result[0]) if result.size == 1 else result


def most_common_cruise_flight_level() -> int:
    """Most common cruise flight level for an aircraft in UK airspace.

    Currently coinsides with sample grid data but will be updated.
    """
    return 300


def read_ads_b_flight_dataframe() -> pl.DataFrame:
    """Read the pre-processed ADS-B flight data from a parquet file."""
    parquet_file = "data/contrails_model_data/2024_01_01_sample_processed.parquet"
    return pl.read_parquet(parquet_file)
