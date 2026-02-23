"""Calculate energy forcing from a synthetic flight database and real weather data."""  # noqa: INP001

from __future__ import annotations

import datetime

import plotly.express as px

from aia_model_contrail_avoidance.core_model.environment import (
    calculate_total_energy_forcing,
    create_grid_environment,
    run_flight_data_through_environment,
)
from aia_model_contrail_avoidance.testing import (
    create_flight_info_list_with_time_offset,
    generate_synthetic_flight_database,
)

if __name__ == "__main__":
    flight_info_list_flight_level_300 = create_flight_info_list_with_time_offset(
        number_of_flights=24 * 7,  # one flight per hour for a week
        time_offset=1.0,
        departure_airport="EGLL",
        arrival_airport="EGPH",
        departure_time=datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.UTC),
        length_of_flight=1.0,
        flight_level=300,
    )
    flight_info_list_flight_level_350 = create_flight_info_list_with_time_offset(
        number_of_flights=24 * 7,  # one flight per hour for a week
        time_offset=1.0,
        departure_airport="EGLL",
        arrival_airport="EGPH",
        departure_time=datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.UTC),
        length_of_flight=1.0,
        flight_level=350,
    )
    flight_info_list_flight_level_250 = create_flight_info_list_with_time_offset(
        number_of_flights=24 * 7,  # one flight per hour for a week
        time_offset=1.0,
        departure_airport="EGLL",
        arrival_airport="EGPH",
        departure_time=datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.UTC),
        length_of_flight=1.0,
        flight_level=250,
    )
    flight_dataframe_flight_level_300 = generate_synthetic_flight_database(
        flight_info_list_flight_level_300, "test_flights_database"
    )
    flight_dataframe_flight_level_350 = generate_synthetic_flight_database(
        flight_info_list_flight_level_350, "test_flights_database"
    )
    flight_dataframe_flight_level_250 = generate_synthetic_flight_database(
        flight_info_list_flight_level_250, "test_flights_database"
    )

    environment = create_grid_environment("cocip_grid_global_week_1_fine_pha_2024")

    flight_data_with_ef_300 = run_flight_data_through_environment(
        flight_dataframe_flight_level_300, environment
    )
    flight_data_with_ef_350 = run_flight_data_through_environment(
        flight_dataframe_flight_level_350, environment
    )
    flight_data_with_ef_250 = run_flight_data_through_environment(
        flight_dataframe_flight_level_250, environment
    )
    # calculate total energy forcing for each flight and save to a new parquet file
    flight_ids = flight_data_with_ef_300["flight_id"].unique().to_list()
    forcing_per_flight_300 = calculate_total_energy_forcing(flight_ids, flight_data_with_ef_300)
    forcing_per_flight_350 = calculate_total_energy_forcing(flight_ids, flight_data_with_ef_350)
    forcing_per_flight_250 = calculate_total_energy_forcing(flight_ids, flight_data_with_ef_250)
    # plot total energy forcing for each flight
    departure_time_of_flights = flight_data_with_ef_300.group_by("flight_id").first()[
        "departure_time"
    ]
    fig = px.bar(
        x=departure_time_of_flights,
        y=forcing_per_flight_350,
        labels={"x": "Departure Time", "y": "Total Energy Forcing (W/m^2)"},
        title="Total Energy Forcing per Flight for Different Flight Levels",
        barmode="group",
    )
    fig.add_trace(
        px.bar(x=departure_time_of_flights, y=forcing_per_flight_300).data[0],
    )
    fig.add_trace(
        px.bar(x=departure_time_of_flights, y=forcing_per_flight_250).data[0],
    )
    # color bars by flight level
    fig.data[0].marker.color = "blue"
    fig.data[1].marker.color = "orange"
    fig.data[2].marker.color = "green"
    fig.data[0].name = "Flight Level 350"
    fig.data[1].name = "Flight Level 300"
    fig.data[2].name = "Flight Level 250"
    fig.update_layout(legend_title_text="Flight Level")
    # show legend at top right
    fig.update_layout(legend={"x": 0.9, "y": 0.9})

    # save
    fig.write_html("results/plots/total_energy_forcing_per_fake_flight.html")
