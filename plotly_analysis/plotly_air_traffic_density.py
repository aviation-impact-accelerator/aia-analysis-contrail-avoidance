"""Generate a heatmap of air traffic density in UK airspace using Plotly."""

from __future__ import annotations

from pathlib import Path

from aia_model_contrail_avoidance.core_model.airspace import (
    ENVIRONMENTAL_BOUNDS_UK_AIRSPACE,
)
from aia_model_contrail_avoidance.core_model.dimensions import SpatialGranularity
from aia_model_contrail_avoidance.visualisation.plotly_spatial_maps import (
    plot_air_traffic_density_map,
)

if __name__ == "__main__":
    # get first parquet file in ads_b_flights_with_ef folder
    parquet_folder = Path("~/ads_b_flights_with_ef").expanduser()
    print(f"Looking for parquet files in {parquet_folder}")
    parquet_files = sorted(parquet_folder.glob("*.parquet"))
    parquet_file_path = parquet_files[0]

    plot_air_traffic_density_map(
        parquet_file_path=parquet_file_path,
        environmental_bounds=ENVIRONMENTAL_BOUNDS_UK_AIRSPACE,
        spatial_granularity=SpatialGranularity.ONE_DEGREE,
        output_file="better_air_traffic_density_map_uk_airspace",
    )
