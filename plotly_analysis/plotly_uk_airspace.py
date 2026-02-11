"""Plot UK airspace polygons on a map using Plotly."""

from __future__ import annotations

from aia_model_contrail_avoidance.visualisation.plotly_spatial_data import (
    plot_airspace_polygons,
)

if __name__ == "__main__":
    plot_airspace_polygons(
        output_file="uk_airspace_map",
    )
