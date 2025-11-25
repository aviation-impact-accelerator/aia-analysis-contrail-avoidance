"""Environment module for AIA Model Contrail Avoidance."""

from __future__ import annotations

__all__ = (
    "calculate_effective_radiative_forcing",
    "create_grid_environment",
    "run_flight_data_through_environment",
)

import pandas as pd
import xarray as xr


def calculate_effective_radiative_forcing(
    flight_id: int | list[int], flight_dataset_with_erf: pd.DataFrame
) -> float | list[float]:
    """Calculates total effective radiative forcing for a flight or list of flights."""
    if isinstance(flight_id, int):
        return float(
            flight_dataset_with_erf.loc[
                flight_dataset_with_erf["flight_id"] == flight_id, "erf"
            ].sum()
        )

    total_erf_list = []
    for fid in flight_id:
        total_erf = flight_dataset_with_erf.loc[
            flight_dataset_with_erf["flight_id"] == fid, "erf"
        ].sum()
        total_erf_list.append(total_erf)
    return total_erf_list


def create_grid_environment() -> xr.Dataset:
    """Creates grid environment from COSIP grid data."""
    ds = xr.open_dataset("./cosip_grid/cocipgrid_sample_result.nc", decode_timedelta=True)

    ds["time"] = pd.to_datetime(ds["time"].values)

    return ds[["longitude", "latitude", "level", "time", "ef_per_m"]]


def run_flight_data_through_environment(
    flight_dataset: pd.DataFrame, environment: xr.Dataset
) -> pd.DataFrame:
    """Runs flight data through environment to assign effective radiative forcing values.

    Args:
        flight_dataset: DataFrame containing flight data with latitude, longitude, timestamp, and
            flight level.
        environment: xarray DataArray containing environmental data with effective radiative forcing
            values.

    """
    flight_level_vector = xr.DataArray(
        flight_dataset["flight_level"].values,
    )
    time_vector = xr.DataArray(flight_dataset["timestamp"].values)
    latitude_vector = xr.DataArray(flight_dataset["latitude"].values)
    longitude_vector = xr.DataArray(flight_dataset["longitude"].values)

    nearest_environment = environment.sel(
        level=flight_level_vector,
        time=time_vector,
        latitude=latitude_vector,
        longitude=longitude_vector,
        method="nearest",
    )
    erf_values = nearest_environment["ef_per_m"].astype(float)

    flight_dataset["erf"] = erf_values

    return flight_dataset
