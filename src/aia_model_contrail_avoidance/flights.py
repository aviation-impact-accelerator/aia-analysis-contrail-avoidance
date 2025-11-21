"""Generate synthetic flights."""

from __future__ import annotations

__all__ = (
    "flight_distance_from_location",
    "generate_synthetic_flight",
    "most_common_cruise_flight_level",
)

import datetime

import numpy as np
import pandas as pd


def generate_synthetic_flight(  # noqa: PLR0913
    flight_id: int,
    departure_location: tuple[float, float],
    arrival_location: tuple[float, float],
    departure_time: datetime.datetime,
    length_of_flight: float,
    flight_level: int,
) -> dict[str, object]:
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
    number_of_timesteps = int(distance_traveled_in_nautical_miles)  # 1 nautical mile per timestep
    latitudes = np.linspace(departure_location[0], arrival_location[0], number_of_timesteps)
    longitudes = np.linspace(departure_location[1], arrival_location[1], number_of_timesteps)

    return {
        "flight_id": flight_id,
        "timestamps": pd.date_range(
            departure_time,
            departure_time + datetime.timedelta(seconds=length_of_flight),
            periods=number_of_timesteps,
        ),
        "latitudes": latitudes,
        "longitudes": longitudes,
        "flight_level": flight_level,
    }


def flight_distance_from_location(
    departure_location: tuple[float, float],
    arrival_location: tuple[float, float],
) -> float:
    """Calculates the distance between two locations using the Haversine formula.

    This is the same as the great circle distance.

    Args:
        departure_location: Tuple of (latitude, longitude) for departure.
        arrival_location: Tuple of (latitude, longitude) for arrival.

    Returns:
        Distance in nautical miles.
    """
    earth_radius = 3443.92  # Radius of the Earth in nautical miles
    departure_latitude, departure_longitude = np.radians(departure_location)
    arrival_latitude, arrival_longitude = np.radians(arrival_location)

    dlat = arrival_latitude - departure_latitude
    dlon = arrival_longitude - departure_longitude
    a = (
        np.sin(dlat / 2) ** 2
        + np.cos(departure_latitude) * np.cos(arrival_latitude) * np.sin(dlon / 2) ** 2
    )
    c = 2 * np.arcsin(np.sqrt(a))

    return earth_radius * c


def most_common_cruise_flight_level() -> int:
    """Most common cruise flight level for an aircraft in UK airspace.

    Currently coinsides with sample grid data but will be updated.
    """
    return 300
