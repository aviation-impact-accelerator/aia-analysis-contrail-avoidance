"""This module generates all Plotly graphs in one run."""  # noqa: INP001

from __future__ import annotations

import json
import logging
from pathlib import Path

from better_plotly_air_traffic_density import plot_air_traffic_density_map
from plotly_contrails_formed_per_time import plot_contrails_formed_over_time
from plotly_distance_flown_by_flight_level_histogram import (
    plot_distance_flown_by_flight_level_histogram,
)
from plotly_domestic_international_flights import plot_domestic_international_flights_pie_chart
from plotly_energy_forcing_histogram import plot_energy_forcing_histogram
from plotly_uk_airspace import plot_airspace_polygons

from aia_model_contrail_avoidance.core_model.dimensions import (
    SpatialGranularity,
    TemporalGranularity,
)


def generate_all_plotly() -> None:
    """Generate all Plotly graphs."""
    # read json file
    file_name = "results/energy_forcing_statistics_week_1_2024.json"
    with Path(file_name).open() as f:
        energy_forcing_statistics = json.load(f)
    logger.info("Loaded data from %s", file_name)

    # temporal options available in the data
    available_temporal_granularities = list(
        energy_forcing_statistics.get("distance_forming_contrails_over_time_histogram", {}).keys()
    )
    logger.info(
        "Available temporal granularities in the data: %s", available_temporal_granularities
    )

    # plot data that varies by temporal granularity

    for temporal_granularity_key in available_temporal_granularities:
        temporal_granularity = TemporalGranularity.from_histogram_key(temporal_granularity_key)
        output_plot_name = f"contrails_formed_{temporal_granularity_key}"
        plot_contrails_formed_over_time(
            forcing_stats_data=energy_forcing_statistics,
            output_plot_name=output_plot_name,
            temporal_granularity=temporal_granularity,
        )

    plot_energy_forcing_histogram(
        energy_forcing_statistics=energy_forcing_statistics,
        output_file_cumulative="energy_forcing_cumulative",
    )

    plot_distance_flown_by_flight_level_histogram(
        flight_statistics=energy_forcing_statistics,
        output_file="distance_flown_by_flight_level_histogram",
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
        json_file="energy_forcing_statistics_week_1_2024",
        output_file="domestic_international_flights_pie_chart",
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.info("Generating all Plotly graphs.")
    generate_all_plotly()
    logger.info("Finished generating all Plotly graphs.")
