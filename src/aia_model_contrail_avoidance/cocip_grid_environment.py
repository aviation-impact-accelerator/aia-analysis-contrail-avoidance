"""Create and visualize a CocipGrid environment for contrail modeling."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr
from pycontrails.core import MetDataset
from pycontrails.datalib.ecmwf import ERA5
from pycontrails.models.cocipgrid import CocipGrid
from pycontrails.models.humidity_scaling import HistogramMatching
from pycontrails.models.ps_model import PSGrid

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent


def generate_cocip_grid_environment(
    time_bounds: tuple[str, str],
    lon_bounds: tuple[float, float],
    lat_bounds: tuple[float, float],
    pressure_levels: tuple[int, ...],
    save_filename: str,
) -> None:
    """Create a CocipGrid environment for contrail modeling.

    Args:
        time_bounds: Start and end times for the model run.
        lon_bounds: Longitude bounds for the model domain.
        lat_bounds: Latitude bounds for the model domain.
        pressure_levels: Pressure levels to be used in the model.
        save_filename: Filename to save the resulting dataset.
    """
    # Download meteorological data
    era5 = ERA5(time_bounds, pressure_levels=pressure_levels, variables=CocipGrid.met_variables)
    met = era5.open_metdataset()
    era5_rad = ERA5(time_bounds, variables=CocipGrid.rad_variables)
    rad = era5_rad.open_metdataset()

    # Model parameters
    params = {
        "dt_integration": np.timedelta64(5, "m"),
        "max_age": np.timedelta64(10, "h"),
        # The humidity_scaling parameter is only used for ECMWF ERA5 data
        # See https://py.contrails.org/api/pycontrails.models.humidity_scaling.html#module-pycontrails.models.humidity_scaling
        "humidity_scaling": HistogramMatching(),
        # Use Poll-Schumann aircraft performance model adapted for grid calculations
        # See https://py.contrails.org/api/pycontrails.models.ps_model.PSGrid.html#pycontrails.models.ps_model.PSGrid
        "aircraft_performance": PSGrid(),
    }

    # Initialize CocipGrid model
    cocip_grid = CocipGrid(met=met, rad=rad, params=params)

    # Create a grid source
    coords = {
        "level": pressure_levels,
        "time": pd.date_range(time_bounds[0], time_bounds[1], freq="1h")[
            0:4
        ],  # run for first 4 hours of domain
        "longitude": np.arange(lon_bounds[0], lon_bounds[1], 1.0),
        "latitude": np.arange(lat_bounds[0], lat_bounds[1], 1.0),
    }
    grid_source = MetDataset.from_coords(**coords)  # type: ignore[arg-type]

    # Run CocipGrid model
    result = cocip_grid.eval(source=grid_source)
    # save dataset to netcdf
    output_path = PROJECT_ROOT / "data" / "energy_forcing_data" / f"{save_filename}.nc"
    result.data.to_netcdf(str(output_path))


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
    grid_data = xr.open_dataset(str(file_path))
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
