"""Generate a CocipGrid environment for contrail modeling over a specified region and time period."""  # noqa: INP001

from __future__ import annotations

from aia_model_contrail_avoidance.cosip_grid_environment import (
    generate_cosip_grid_environment,
)

if __name__ == "__main__":
    # Set up time and spatial bounds for the model run
    time_bounds = ("2024-01-01 00:00:00", "2024-01-01 23:00:00")
    lon_bounds = (-8, 3)
    lat_bounds = (49, 62)
    # ideally would have  200 to 450 in 10 pha increments but not all levels available in ERA5
    pressure_levels = (200, 225, 250, 300, 350, 400, 450)

    # Generate CocipGrid environment
    generate_cosip_grid_environment(
        time_bounds=time_bounds,
        lon_bounds=lon_bounds,
        lat_bounds=lat_bounds,
        pressure_levels=pressure_levels,
        save_filename="cosip_grid_uk_airspace_jan01_2024",
    )
