"""Generate a CocipGrid environment for contrail modeling over a specified region and time period."""  # noqa: INP001

from __future__ import annotations

from aia_model_contrail_avoidance.cocip_grid_environment import (
    generate_cocip_grid_environment,
)

if __name__ == "__main__":
    # Set up time and spatial bounds for the model run
    modelling_time_bounds = ("2024-03-01 00:00:00", "2024-03-31 23:00:00")
    lon_bounds = (-120, 150)
    lat_bounds = (-60, 65)
    pressure_levels = tuple(range(150, 900, 10))
    # Generate CocipGrid environment
    generate_cocip_grid_environment(
        modelling_time_bounds=modelling_time_bounds,
        lon_bounds=lon_bounds,
        lat_bounds=lat_bounds,
        pressure_levels=pressure_levels,
        save_filename="cocip_grid_global_month_03_2024",
    )
