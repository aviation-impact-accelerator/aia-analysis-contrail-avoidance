from __future__ import annotations  # noqa: D100, INP001

from pathlib import Path

import numpy as np
import plotly.graph_objects as go  # type: ignore[import-untyped]
import polars as pl
from shapely.geometry import box

from aia_model_contrail_avoidance.core_model.airspace import (
    get_gb_airspaces,
)
from aia_model_contrail_avoidance.core_model.dimensions import SpatialGranularity

# Define the boundary
SOUTH, NORTH = 39.0, 70.0
WEST, EAST = -12.0, 3.0

# Define the boundary for the graticule grid lines
grid_north = 61
grid_south = 45
grid_west = -30
grid_east = 5


# Calculate center from bounds
center_lat = (SOUTH + NORTH) / 2
center_lon = (WEST + EAST) / 2 - 5  # Shift west

# Create a simple map centered on the UK using Maplibre
fig = go.Figure()


def plot_air_traffic_density_map(  # noqa: C901, PLR0915
    parquet_file_name: str,
    environmental_bounds: dict[str, float] | None = None,
    spatial_granularity: SpatialGranularity = SpatialGranularity.ONE_DEGREE,
    output_file: str = "air_traffic_density_map",
) -> None:
    """Plot air traffic density as a heatmap matrix for each degree.

    Args:
        parquet_file_name: Name of the parquet file containing flight data (without extension).
        spatial_granularity: Spatial granularity for binning (default: ONE_DEGREE).
        environmental_bounds: Optional dict with lat_min, lat_max, lon_min, lon_max.
        output_file: Name of the output plot file (without extension).

    Raises:
        FileNotFoundError: If the parquet file is not found.
        NotImplementedError: If the chosen spatial granularity is not supported.
    """
    parquet_file_path = Path("data/contrails_model_data") / f"{parquet_file_name}.parquet"
    if not parquet_file_path.exists():
        msg = f"Parquet file not found: {parquet_file_path}"
        raise FileNotFoundError(msg)

    flight_dataframe = pl.read_parquet(parquet_file_path)

    # Create spatial bins and compute air traffic density
    if spatial_granularity == SpatialGranularity.ONE_DEGREE:
        # Create degree bins for latitude and longitude
        flight_dataframe_with_bins = flight_dataframe.with_columns(
            [
                pl.col("latitude").floor().alias("lat_bin"),
                pl.col("longitude").floor().alias("lon_bin"),
            ]
        )

        # Count unique flights per degree bin (air traffic density)
        density_data = (
            flight_dataframe_with_bins.group_by(["lat_bin", "lon_bin"])
            .agg(pl.col("flight_id").n_unique().alias("flight_count"))
            .sort(["lon_bin", "lat_bin"])
        )

        # Set environmental bounds
        if environmental_bounds is None:
            min_lat = int(flight_dataframe_with_bins["lat_bin"].min())  # type: ignore[arg-type]
            max_lat = int(flight_dataframe_with_bins["lat_bin"].max())  # type: ignore[arg-type]
            min_lon = int(flight_dataframe_with_bins["lon_bin"].min())  # type: ignore[arg-type]
            max_lon = int(flight_dataframe_with_bins["lon_bin"].max())  # type: ignore[arg-type]
            environmental_bounds = {
                "lat_min": min_lat,
                "lat_max": max_lat,
                "lon_min": min_lon,
                "lon_max": max_lon,
            }
        else:
            min_lat = int(environmental_bounds["lat_min"])
            max_lat = int(environmental_bounds["lat_max"])
            min_lon = int(environmental_bounds["lon_min"])
            max_lon = int(environmental_bounds["lon_max"])

        # Create matrix with zeros
        lat_range = max_lat - min_lat + 1
        lon_range = max_lon - min_lon + 1
        density_matrix = np.zeros((lat_range, lon_range))

        # Populate matrix and hande out-of-bounds gracefully
        for row in density_data.iter_rows():
            lat_bin = int(row[0])
            lon_bin = int(row[1])
            flight_count = row[2]

            if min_lat <= lat_bin <= max_lat and min_lon <= lon_bin <= max_lon:
                lat_index = lat_bin - min_lat
                lon_index = lon_bin - min_lon
                density_matrix[lat_index, lon_index] = flight_count

        # Prepare lat, lon, z arrays for Densitymap
        uk_airspaces = get_gb_airspaces()
        lats = []
        lons = []
        zs = []
        for i in range(density_matrix.shape[0]):
            for j in range(density_matrix.shape[1]):
                value = density_matrix[i, j]
                lat0 = min_lat + i
                lat1 = lat0 + 1
                lon0 = min_lon + j
                lon1 = lon0 + 1
                cell_poly = box(lon0, lat0, lon1, lat1)
                for airspace in uk_airspaces:
                    if (
                        cell_poly.intersects(airspace.shape)
                        and lon1 <= max_lon
                        and lat1 <= max_lat
                        and value > 0
                    ):
                        lats.append(lat0 + 0.5)  # Center of the bin
                        lons.append(lon0 + 0.5)
                        zs.append(value)
                        break  # Only count each cell once if it intersects any airspace

        fig = go.Figure(
            go.Densitymap(
                lat=lats,
                lon=lons,
                z=zs,
                zmin=0,
                zmax=np.max(density_matrix),
                colorscale="Viridis",
                colorbar={"title": "Flight Count"},
                radius=2000,
                showlegend=False,
                hovertemplate="Lat: %{lat}<br>Lon: %{lon}<br>Flights: %{z}<extra></extra>",
            )
        )

    uk_airspaces = get_gb_airspaces()

    for i, airspace in enumerate(uk_airspaces):  # noqa: B007
        # Get the exterior coordinates from the shapely geometry
        coords = np.array(airspace.shape.exterior.coords)
        lons = coords[:, 0]  # type: ignore [assignment]
        lats = coords[:, 1]  # type: ignore [assignment]

        fig.add_trace(
            go.Scattermap(
                lat=lats,
                lon=lons,
                mode="lines",
                line={"color": "red", "width": 5},
                showlegend=False,
                hoverinfo="skip",
            )
        )

        fig.update_layout(
            title="Flight Density Map in UK Airspace",
            map_center_lon=center_lon,
            map_center_lat=center_lat,
            map_zoom=3.5,
            margin={"l": 0, "r": 0, "t": 40, "b": 0},
            showlegend=False,
        )

    fig.write_html(
        f"results/plots/{output_file}.html",
        config={
            "displaylogo": False,
        },
        full_html=False,
        include_plotlyjs="cdn",
    )


if __name__ == "__main__":
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
        output_file="better_air_traffic_density_map_uk_airspace",
    )
