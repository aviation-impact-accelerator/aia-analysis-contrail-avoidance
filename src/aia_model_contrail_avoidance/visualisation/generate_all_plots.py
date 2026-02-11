"""Generate all plots for the analysis."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from aia_model_contrail_avoidance.core_model.airspace import ENVIRONMENTAL_BOUNDS_UK_AIRSPACE
from aia_model_contrail_avoidance.core_model.dimensions import (
    SpatialGranularity,
    TemporalGranularity,
)
from aia_model_contrail_avoidance.visualisation.plot_per_flight_histograms import (
    plot_energy_forcing_histogram,
)
from aia_model_contrail_avoidance.visualisation.plot_pie_charts import (
    plot_pie_chart_distance_forming_contrails,
    plot_pie_chart_distance_traveled_by_domestic_and_international_flights,
    plot_pie_chart_number_of_flights_domestic_and_international,
    plot_pie_chart_number_of_flights_forming_contrails,
)
from aia_model_contrail_avoidance.visualisation.plot_spatial_histograms import (
    plot_distance_flown_by_flight_level_histogram,
)
from aia_model_contrail_avoidance.visualisation.plot_spatial_maps import (
    plot_air_traffic_density_map,
    plot_uk_airspace_map,
)
from aia_model_contrail_avoidance.visualisation.plot_temporal_histograms import (
    plot_contrails_formed_over_time,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def generate_all_plots(
    json_file_name: str,
    flights_with_ef_dir: Path,
    environmental_bounds: tuple,
    spatial_granularity: SpatialGranularity,
) -> None:
    """Generate all Plotly graphs."""
    # read json file
    with Path(json_file_name).open() as f:
        energy_forcing_statistics = json.load(f)
    logger.info("Loaded data from %s", json_file_name)

    # temporal options available in the data
    available_temporal_granularities = list(
        energy_forcing_statistics.get("distance_forming_contrails_over_time_histogram", {}).keys()
    )
    logger.info(
        "Available temporal granularities in the data: %s", available_temporal_granularities
    )
    logger.info("Generating all Plotly graphs.")
    # plot data that varies by temporal granularity

    for temporal_granularity_key in available_temporal_granularities:
        temporal_granularity = TemporalGranularity.from_histogram_key(temporal_granularity_key)
        output_plot_name = f"contrails_formed_{temporal_granularity_key}"
        plot_contrails_formed_over_time(
            forcing_stats_data=energy_forcing_statistics,
            output_plot_name=output_plot_name,
            temporal_granularity=temporal_granularity,
        )

    # plot data that does not vary by temporal granularity

    plot_energy_forcing_histogram(
        energy_forcing_statistics=energy_forcing_statistics,
        output_file_cumulative="energy_forcing_cumulative",
    )

    plot_distance_flown_by_flight_level_histogram(
        flight_statistics=energy_forcing_statistics,
        output_file="distance_flown_by_flight_level_histogram",
    )

    plot_uk_airspace_map(
        output_file="uk_airspace_map",
    )

    plot_pie_chart_number_of_flights_domestic_and_international(
        flight_statistics=energy_forcing_statistics,
        output_file_name="pie_chart_number_of_flights_domestic_and_international_flights",
    )
    plot_pie_chart_distance_traveled_by_domestic_and_international_flights(
        flight_statistics=energy_forcing_statistics,
        output_file_name="pie_chart_distance_traveled_by_domestic_and_international_flights",
    )
    plot_pie_chart_number_of_flights_forming_contrails(
        flight_statistics=energy_forcing_statistics,
        output_file_name="pie_chart_number_of_flights_forming_contrails",
    )
    plot_pie_chart_distance_forming_contrails(
        flight_statistics=energy_forcing_statistics,
        output_file_name="pie_chart_distance_forming_contrails",
    )

    # plot data that requires the full dataframe (e.g. for spatial plotting)
    # use first day of data as sample for plotting
    parquet_files = sorted(flights_with_ef_dir.glob("*.parquet"))
    parquet_file_path = parquet_files[0]

    plot_air_traffic_density_map(
        parquet_file_path=parquet_file_path,
        environmental_bounds=environmental_bounds,
        spatial_granularity=spatial_granularity,
        output_file="air_traffic_density_map_uk_airspace",
    )
    logger.info("Finished generating all Plotly graphs.")


if __name__ == "__main__":
    generate_all_plots(
        json_file_name="results/energy_forcing_statistics_week_1_2024.json",
        flights_with_ef_dir=Path("~/ads_b_analysis/ads_b_flights_with_ef").expanduser(),
        environmental_bounds=ENVIRONMENTAL_BOUNDS_UK_AIRSPACE,
        spatial_granularity=SpatialGranularity.ONE_DEGREE,
    )
