"""Functions used for testing the core model and grid environment."""

from __future__ import annotations

__all__ = [
    "create_synthetic_grid_environment",
    "generate_synthetic_flight",
]

import datetime
from typing import Any

import numpy as np
import polars as pl
import xarray as xr

from aia_model_contrail_avoidance.config import FLIGHT_TIMESTAMPS_SCHEMA
from aia_model_contrail_avoidance.core_model.airports import airport_icao_code_to_location
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


def generate_synthetic_flight_database(
    flight_info_list: list[dict[str, Any]], database_name: str
) -> None:
    """Generate a synthetic flight database for testing purposes."""
    flight_dataframe = pl.DataFrame()

    for flight_info in flight_info_list:
        new_flight = generate_synthetic_flight(
            flight_info["flight_id"],
            airport_icao_code_to_location(flight_info["departure_airport"]),
            airport_icao_code_to_location(flight_info["arrival_airport"]),
            flight_info["departure_time"],
            flight_info["length_of_flight"],
            flight_info["flight_level"],
        )

        flight_dataframe = pl.concat([flight_dataframe, new_flight], how="vertical")

    flight_dataframe.write_parquet(f"data/contrails_model_data/{database_name}.parquet")


def create_flight_info_list_with_time_offset(  # noqa: PLR0913
    number_of_flights: int,
    time_offset: float,
    departure_airport: str,
    arrival_airport: str,
    departure_time: datetime.datetime,
    length_of_flight: float,
    flight_level: float,
) -> list[dict[str, Any]]:
    """Generate a flight info list for the same flight offset by a constant time.

    Args:
        number_of_flights: Number of flights in the list.
        time_offset: Time offset in hours between each flight.
        departure_airport: ICAO code for departure airport.
        arrival_airport: ICAO code for arrival airport.
        departure_time: Departure time for the first flight as a datetime object.
        length_of_flight: Length of flight in hours.
        flight_level: Flight level for all flights.
    """
    flight_info_list = []
    for i in range(number_of_flights):
        new_flight = {
            "flight_id": i + 1,
            "departure_airport": departure_airport,
            "arrival_airport": arrival_airport,
            "departure_time": departure_time + datetime.timedelta(hours=i * time_offset),
            "length_of_flight": length_of_flight,
            "flight_level": flight_level,
        }
        flight_info_list.append(new_flight)
    return flight_info_list
