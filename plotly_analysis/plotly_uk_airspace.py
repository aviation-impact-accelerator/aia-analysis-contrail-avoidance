"""Plot UK airspace polygons on a map using Plotly."""

from __future__ import annotations

from aia_model_contrail_avoidance.visualisation.plot_spatial_maps import (
    plot_uk_airspace_map,
)

if __name__ == "__main__":
    plot_uk_airspace_map(
        output_file="uk_airspace_map",
    )
