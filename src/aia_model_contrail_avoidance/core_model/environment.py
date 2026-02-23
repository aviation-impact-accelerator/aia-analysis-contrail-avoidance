"""Environment module for AIA Model Contrail Avoidance."""

from __future__ import annotations

__all__ = (
    "calculate_total_energy_forcing",
    "create_grid_environment",
    "run_flight_data_through_environment",
)
import polars as pl
import xarray as xr

# Conversion factor from nautical miles to meters
NAUTICAL_MILES_TO_METERS = 1852.0


def calculate_total_energy_forcing(
    flight_id: int | list[int], flight_dataset_with_energy_forcing: pl.DataFrame
) -> float | list[float]:
    """Calculates total energy forcing for a flight or list of flights."""
    if isinstance(flight_id, int):
        return float(
            flight_dataset_with_energy_forcing.filter(pl.col("flight_id") == flight_id)["ef"].sum()
        )

    total_energy_forcing_list = []
    for fid in flight_id:
        total_energy_forcing = flight_dataset_with_energy_forcing.filter(
            pl.col("flight_id") == fid
        )["ef"].sum()
        total_energy_forcing_list.append(total_energy_forcing)
    return total_energy_forcing_list


def create_grid_environment(environment_file_name: str) -> xr.DataArray:
    """Creates grid environment from COSIP grid data."""
    environment_dataset = xr.open_dataset(
        "data/energy_forcing_data/" + environment_file_name + ".nc",
        decode_timedelta=True,
        drop_variables=("air_pressure", "contrail_age"),
        engine="netcdf4",
    )
    return xr.DataArray(
        environment_dataset["ef_per_m"], dims=("longitude", "latitude", "level", "time")
    )


def run_flight_data_through_environment(
    flight_dataset: pl.DataFrame, environment: xr.DataArray
) -> pl.DataFrame:
    """Runs flight data through environment to assign effective radiative forcing values.

    Args:
        flight_dataset: DataFrame containing flight data with latitude, longitude, timestamp, and
            flight level.
        environment: xarray DataArray containing environmental data with energy forcing per meter
            values.

    """
    flight_dataset = flight_dataset.clone()

    longitude_vector = xr.DataArray(flight_dataset["longitude"].to_numpy(), dims=["points"])
    latitude_vector = xr.DataArray(flight_dataset["latitude"].to_numpy(), dims=["points"])

    # Convert flight level (in hundreds of feet) to pressure altitude (hPa) using standard atmosphere
    # Flight Level 250 = 25,000 feet
    flight_level_feet = (
        flight_dataset["flight_level"].to_numpy() * 100
    )  # Convert from FL units to feet
    flight_level_meters = flight_level_feet * 0.3048  # Convert feet to meters
    # Barometric formula: P = P0 * (1 - L*h/T0)^(g*M/R/L)
    flight_level_hpa = 1013.25 * (1 - 0.0065 * flight_level_meters / 288.15) ** 5.255
    flight_level_vector = xr.DataArray(flight_level_hpa, dims=["points"])

    time_vector = xr.DataArray(flight_dataset["timestamp"].to_numpy(), dims=["points"])

    distance_flown_in_segment_vector = (
        xr.DataArray(flight_dataset["distance_flown_in_segment"].to_numpy(), dims=["points"])
        * NAUTICAL_MILES_TO_METERS
    )

    nearest_environment = environment.sel(
        longitude=longitude_vector,
        latitude=latitude_vector,
        level=flight_level_vector,
        time=time_vector,
        method="nearest",
    )

    ef_values = nearest_environment.astype(float) * distance_flown_in_segment_vector

    return flight_dataset.with_columns(pl.Series("ef", ef_values.values))
