"""Defines a function to plot contrails formed per temporal unit."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from aia_model_contrail_avoidance.core_model.dimensions import (
    TemporalGranularity,
)
from aia_model_contrail_avoidance.visualisation.plotly_temporal_hisograms import (
    plot_contrails_formed_over_time,
)


def plot_contrails_formed_over_time_from_json(
    name_of_forcing_stats_file: str,
    output_plot_name: str,
    temporal_granularity: TemporalGranularity | None = None,
) -> None:
    """Plots the number of contrails formed per temporal unit from the given JSON file.

    Args:
        name_of_forcing_stats_file: The name of the JSON file (without extension)
        output_plot_name: The name of the output HTML file (without extension) where the plot will be saved.
            The plot will be saved in the "results/plots" directory.
        temporal_granularity: The temporal granularity to use for the plot. If None, it will plot for all available
    """
    # Load the data from the specified stats file
    with Path(f"results/{name_of_forcing_stats_file}.json").open("r") as f:
        forcing_stats_data = json.load(f)
    # if temporal_granularity is not provided, get all available temporal granularities
    if temporal_granularity is None:
        # get list of keys in distance_forming_contrails_over_time_histogram
        keys = list(
            forcing_stats_data.get("distance_forming_contrails_over_time_histogram", {}).keys()
        )
        # for each key in keys, find the corresponding temporal granularity
        logger.info("Plotting for all available temporal granularities: %s", keys)
        for key in keys:
            temporal_granularity_for_key = TemporalGranularity.from_histogram_key(key)
            output_plot_name_over_time = f"{output_plot_name}_{key}"

            plot_contrails_formed_over_time(
                forcing_stats_data=forcing_stats_data,
                output_plot_name=output_plot_name_over_time,
                temporal_granularity=temporal_granularity_for_key,
            )
    else:
        plot_contrails_formed_over_time(
            forcing_stats_data=forcing_stats_data,
            output_plot_name=output_plot_name,
            temporal_granularity=temporal_granularity,
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.info("Starting to plot contrails formed over time from JSON data.")
    plot_contrails_formed_over_time_from_json(
        name_of_forcing_stats_file="energy_forcing_statistics_week_1_2024",
        output_plot_name="contrails_formed",
    )
