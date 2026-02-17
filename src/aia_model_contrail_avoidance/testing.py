"""Functions used for testing the core model and grid environment."""

from __future__ import annotations

__all__ = [
    "create_synthetic_grid_environment",
    "generate_synthetic_flight",
]

import datetime

import numpy as np
import polars as pl
import xarray as xr

from aia_model_contrail_avoidance.config import FLIGHT_TIMESTAMPS_SCHEMA
from aia_model_contrail_avoidance.core_model.flights import flight_distance_from_location


def create_synthetic_grid_environment() -> xr.DataArray:
    """Creates a synthetic grid environment for testing."""
    longitudes = xr.DataArray(
        list(range(-8, 3)),  # linear increase from -8 to 2 degrees (UK airspace)
        dims=("longitude"),
    )
    latitudes = xr.DataArray(
        list(range(49, 62)),  # linear increase from 49 to 61 degrees (UK airspace)
        dims=("latitude"),
    )
    levels = xr.DataArray(
        [250, 300, 350],  # flight levels in hPa
        dims=("level"),
    )
    times = xr.DataArray(
        pl.datetime_range(
            start=pl.datetime(2024, 1, 1),
            end=pl.datetime(2024, 1, 1, 23),
            interval="1h",
            eager=True,
        ).to_list(),
        dims=("time"),
    )

    ef_per_m = xr.DataArray(
        np.zeros((len(longitudes), len(latitudes), len(levels), len(times))),
        dims=("longitude", "latitude", "level", "time"),
        coords={
            "longitude": longitudes,
            "latitude": latitudes,
            "level": levels,
            "time": times,
        },
    )

    # Fill with synthetic data
    ef_per_m.loc[{"level": 300}] = 0.5
    ef_per_m.loc[{"level": 350}] = 1.0

    return ef_per_m


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
