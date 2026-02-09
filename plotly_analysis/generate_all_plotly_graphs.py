"""This module generates all Plotly graphs in one run."""

from __future__ import annotations

from aia_model_contrail_avoidance.core_model.dimensions import SpatialGranularity

from .better_plotly_air_traffic_density import plot_air_traffic_density_map
from .plotly_contrails_formed_per_time import plot_contrails_formed
from .plotly_distance_flown_by_altitude_histogram import plot_distance_flown_by_altitude_histogram
from .plotly_domestic_international_flights import plot_domestic_international_flights_pie_chart
from .plotly_energy_forcing_histogram import plot_energy_forcing_histogram
from .plotly_uk_airspace import plot_airspace_polygons


def generate_all_plotly() -> None:
    """Generate all Plotly graphs."""
    plot_contrails_formed(
        name_of_forcing_stats_file="energy_forcing_statistics",
        output_plot_name="contrails_formed",
    )
    plot_distance_flown_by_altitude_histogram(
        stats_file="2024_01_01_sample_stats_processed",
        output_file="distance_flown_by_altitude_histogram",
    )
    plot_energy_forcing_histogram(
        json_file="energy_forcing_statistics",
        output_file_histogram="energy_forcing_per_flight_histogram",
        output_file_cumulative="energy_forcing_cumulative",
    )
    plot_airspace_polygons(
        output_file="uk_airspace_map",
    )

    environmental_bounds = {
        "lat_min": 45.0,
        "lat_max": 61.0,
        "lon_min": -30.0,
        "lon_max": 5.0,
    }

    plot_air_traffic_density_map(
        parquet_file_name="2024_01_01_sample_processed_with_interpolation",
        environmental_bounds=environmental_bounds,
        spatial_granularity=SpatialGranularity.ONE_DEGREE,
        output_file="air_traffic_density_map_uk_airspace",
    )

    plot_domestic_international_flights_pie_chart(
        json_file="2024_01_01_sample_stats_processed",
        output_file="domestic_international_flights_pie_chart",
    )


if __name__ == "__main__":
    generate_all_plotly()
