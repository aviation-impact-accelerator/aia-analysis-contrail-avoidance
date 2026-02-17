"""Generate synthetic flights."""

from __future__ import annotations

__all__ = ("flight_distance_from_location", "flight_distance_from_location_vectorized")

import numpy as np
import polars as pl


def to_float_numpy(series: pl.Series) -> np.ndarray:
    """Convert a Polars Series to a NumPy array of floats.

    Args:
        series: A Polars Series to convert.

    Returns: A NumPy array of floats.
    """
    if isinstance(series, pl.Series):
        return series.cast(float).to_numpy()
    return np.asarray(series, dtype=float)


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


def read_ads_b_flight_dataframe() -> pl.DataFrame:
    """Read the pre-processed ADS-B flight data from a parquet file."""
    parquet_file = "data/contrails_model_data/2024_01_01_sample_processed.parquet"
    return pl.read_parquet(parquet_file)
