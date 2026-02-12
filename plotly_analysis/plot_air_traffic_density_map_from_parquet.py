"""Generate a heatmap of air traffic density in UK airspace using Plotly."""

from __future__ import annotations

from pathlib import Path

from aia_model_contrail_avoidance.core_model.airspace import (
    ENVIRONMENTAL_BOUNDS_UK_AIRSPACE,
)
from aia_model_contrail_avoidance.core_model.dimensions import SpatialGranularity
from aia_model_contrail_avoidance.visualisation.plot_spatial_maps import (
    plot_air_traffic_density_map,
)


def plot_air_traffic_density_map_from_parquet(
    parquet_file_path: Path,
    output_file: str,
    environmental_bounds: dict[str, float],
    spatial_granularity: SpatialGranularity,
) -> None:
    """Generate a heatmap of air traffic density in UK airspace using Plotly.

    Args:
        parquet_file_path: Path to the Parquet file containing flight data with energy forcing.
        output_file: Name of the output file to save the plot (without extension).
        environmental_bounds: Dictionary defining the bounds of the map with keys 'min_latitude',
        'max_latitude', 'min_longitude', 'max_longitude'.
        spatial_granularity: SpatialGranularity defining the granularity of the heatmap.
    """
    plot_air_traffic_density_map(
        parquet_file_path=parquet_file_path,
        environmental_bounds=environmental_bounds,
        spatial_granularity=spatial_granularity,
        output_file=output_file,
    )


if __name__ == "__main__":
    # get first parquet file in ads_b_flights_with_ef folder
    parquet_folder = Path("~/ads_b_analysis/ads_b_flights_with_ef").expanduser()
    print(f"Looking for parquet files in {parquet_folder}")
    parquet_files = sorted(parquet_folder.glob("*.parquet"))
    parquet_file_path = parquet_files[0]

    plot_air_traffic_density_map_from_parquet(
        parquet_file_path=parquet_file_path,
        environmental_bounds=ENVIRONMENTAL_BOUNDS_UK_AIRSPACE,
        spatial_granularity=SpatialGranularity.ONE_DEGREE,
        output_file="air_traffic_density_map_uk_airspace",
    )
