"""Docstring for analysis.geenerate_synthetic_flight_database."""  # noqa: INP001

from __future__ import annotations

import datetime

from aia_model_contrail_avoidance.testing import (
    create_flight_info_list_with_time_offset,
    generate_synthetic_flight_database,
)

if __name__ == "__main__":
    flight_info_list = create_flight_info_list_with_time_offset(
        number_of_flights=12,
        time_offset=1.0,
        departure_airport="EGLL",
        arrival_airport="EGPH",
        departure_time=datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.UTC),
        length_of_flight=1.0,
        flight_level=350,
    )
    generate_synthetic_flight_database(flight_info_list, "test_flights_database")
