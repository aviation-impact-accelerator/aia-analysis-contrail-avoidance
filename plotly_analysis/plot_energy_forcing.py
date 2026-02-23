"""Plot energy foricng from cocip grid."""

from __future__ import annotations

import logging

from aia_model_contrail_avoidance.visualisation.plot_spatial_maps import (
    plot_cocip_grid_environment,
    plot_warming_zones_from_cocip_grid,
)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    plot_cocip_grid_environment(
        selected_time="2024-01-01T00:00",
        selected_flight_level=350,
        environment_filename="cocip_grid_global_week_1_fine_pha_2024",
        save_filename="cocip_grid_energy_forcing_map",
        show_uk_airspace=True,
    )
    plot_warming_zones_from_cocip_grid(
        selected_time_slice=("2024-01-01T00:00", "2024-01-02T00:00"),
        selected_flight_level_slice=(430, 300),
        environment_filename="cocip_grid_global_week_1_fine_pha_2024",
        save_filename="cocip_grid_energy_forcing_warming_zones_map",
        show_uk_airspace=True,
    )
