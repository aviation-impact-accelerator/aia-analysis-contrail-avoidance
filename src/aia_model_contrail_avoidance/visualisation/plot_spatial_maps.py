"""Plot UK airspace polygons on a map using Plotly."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
import plotly.graph_objects as go
import polars as pl
from shapely.geometry import box

from aia_model_contrail_avoidance.core_model.airspace import (
    get_gb_airspaces,
)
from aia_model_contrail_avoidance.core_model.dimensions import SpatialGranularity

if TYPE_CHECKING:
    from pathlib import Path

# --- Constants for UK Airspace Map ---

# Define the boundary
SOUTH, NORTH = 39.0, 70.0
WEST, EAST = -12.0, 3.0

# Define the boundary for the graticule grid lines
grid_north = 62.5
grid_south = 45
grid_west = -30
grid_east = 5

# Calculate center from bounds
center_lat = (SOUTH + NORTH) / 2
center_lon = (WEST + EAST) / 2 - 5  # Shift west


def plot_uk_airspace_map(
    output_file: str | Path,
) -> None:
    """Plot UK airspace polygons on a map.

    Args:
        output_file: Path to save the output plot image.
    """
    # Create a simple map centered on the UK using Maplibre
    fig = go.Figure()

    fig.update_layout(
        title="UK Airspace",
        map={
            "style": "carto-voyager",  # Clean, dark style (free, no API key)
            "center": {"lat": center_lat, "lon": center_lon},
            "zoom": 3.5,  # Manually tuned to fit the bounds
        },
        margin={"l": 0, "r": 0, "t": 40, "b": 0},
        showlegend=False,  # Remove external legend
    )

    # Add latitude lines (graticule)
    for lat in np.arange(grid_south, grid_north + 0.1, 2.5):  # Every 2.5 degrees
        fig.add_trace(
            go.Scattermap(
                lat=[lat] * 100,
                lon=np.linspace(grid_west, grid_east, 100).tolist(),
                mode="lines",
                line={"color": "gray", "width": 0.5},
                showlegend=False,
                hoverinfo="skip",
            )
        )

    # Add longitude lines (graticule)
    for lon in np.arange(grid_west, grid_east + 0.1, 2.5):  # Every 2.5 degrees
        fig.add_trace(
            go.Scattermap(
                lat=np.linspace(grid_south, grid_north, 100).tolist(),
                lon=[lon] * 100,
                mode="lines",
                line={"color": "gray", "width": 0.5},
                showlegend=False,
                hoverinfo="skip",
            )
        )

    # Add longitude labels at the bottom (westings/eastings)
    for lon in np.arange(
        grid_west, grid_east + 0.1, 2.5
    ):  # Labels every 5 degrees within visible area
        label = f"{abs(lon)}°W" if lon < 0 else f"{lon}°E" if lon > 0 else "0°"
        fig.add_trace(
            go.Scattermap(
                lat=[grid_south - 0.6],  # Slightly above bottom edge
                lon=[lon],
                mode="text",
                text=[label],
                textfont={"size": 10, "color": "black"},
                showlegend=False,
                hoverinfo="skip",
            )
        )

        fig.update_layout(
            modebar_remove=[
                "zoom",
                "pan",
                "select",
                "lasso",
                "zoomIn",
                "zoomOut",
                "autoScale",
                "resetScale",
            ],
        )
    # Add latitude labels on the left (northings)
    for lat in np.arange(
        grid_south, grid_north + 0.1, 2.5
    ):  # Labels every 5 degrees within visible area
        label = f"{lat}°N" if lat >= 0 else f"{abs(lat)}°S"
        fig.add_trace(
            go.Scattermap(
                lat=[lat],
                lon=[grid_west - 1.0],  # Slightly right of left edge
                mode="text",
                text=[label],
                textfont={"size": 10, "color": "black"},
                showlegend=False,
                hoverinfo="skip",
            )
        )

    uk_airspaces = get_gb_airspaces()

    # Add each airspace as a polygon
    colors = ["rgba(255, 0, 0, 0.2)", "rgba(0, 255, 0, 0.2)", "rgba(0, 0, 255, 0.2)"]
    for i, airspace in enumerate(uk_airspaces):
        # Get the exterior coordinates from the shapely geometry
        coords = np.array(airspace.shape.exterior.coords)
        lons = coords[:, 0]
        lats = coords[:, 1]

        fig.add_trace(
            go.Scattermap(
                lat=lats,
                lon=lons,
                mode="lines",
                fill="toself",
                fillcolor=colors[i % len(colors)],
                line={"color": "black", "width": 1},
                name=getattr(airspace, "name", f"Airspace {i}"),
            )
        )

        # Add label at centroid
        centroid = airspace.shape.centroid
        fig.add_trace(
            go.Scattermap(
                lat=[centroid.y],
                lon=[centroid.x],
                mode="text",
                text=[getattr(airspace, "name", f"Airspace {i}")],
                textfont={"size": 12, "color": "black"},
                showlegend=False,
            )
        )

    fig.write_html(
        f"results/plots/{output_file}.html",
        config={
            "displaylogo": False,
        },
        full_html=False,
        include_plotlyjs="cdn",
    )


def plot_air_traffic_density_map(
    parquet_file_path: Path,
    environmental_bounds: dict[str, float] | None = None,
    spatial_granularity: SpatialGranularity = SpatialGranularity.ONE_DEGREE,
    output_file: str = "air_traffic_density_map",
) -> None:
    """Plot air traffic density as a heatmap matrix for each degree.

    Args:
        parquet_file_path: Path to the parquet file containing flight data.
        spatial_granularity: Spatial granularity for binning (default: ONE_DEGREE).
        environmental_bounds: Optional dict with lat_min, lat_max, lon_min, lon_max.
        output_file: Name of the output plot file (without extension).

    Raises:
        NotImplementedError: If the chosen spatial granularity is not supported.
    """
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
                radius=50,
                showlegend=False,
                hovertemplate="Lat: %{lat}<br>Lon: %{lon}<br>Flights: %{z}<extra></extra>",
            )
        )

    uk_airspaces = get_gb_airspaces()

    for _i, airspace in enumerate(uk_airspaces):
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


def plot_cocip_grid_environment(
    selected_time_index: int,
    selected_flight_level_index: int,
    environment_filename: str,
    save_filename: str,
) -> None:
    """Plot the CocipGrid environment data.

    Args:
        selected_time_index: Index of the time to plot.
        selected_flight_level_index: Index of the flight level to plot.
        environment_filename: Filename of the saved CocipGrid environment dataset.
        save_filename: Filename to save the plot.
    """
    file_path = PROJECT_ROOT / "data" / "energy_forcing_data" / f"{environment_filename}.nc"
    grid_data = xr.open_dataset(str(file_path), engine="netcdf4")
    plt.figure(figsize=(12, 8))
    ef_per_m = grid_data["ef_per_m"].isel(
        time=selected_time_index, level=selected_flight_level_index
    )
    ef_per_m.plot(x="longitude", y="latitude", vmin=-1e8, vmax=1e8, cmap="coolwarm")  # type: ignore[call-arg]

    plt.title("CocipGrid Energy Forcing")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")

    if save_filename:
        output_path = PROJECT_ROOT / "results" / "plots" / f"{save_filename}.png"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(str(output_path))
    else:
        plt.show()
