"""Defines a function to plot contrails formed per temporal unit."""

from __future__ import annotations

import json
import logging

import numpy as np
import plotly.graph_objects as go  # type: ignore[import-untyped]
from plotly.subplots import make_subplots  # type: ignore[import-untyped]

from aia_model_contrail_avoidance.core_model.dimensions import (
    TemporalGranularity,
    _get_temporal_range_and_labels,
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
    with open(f"results/{name_of_forcing_stats_file}.json") as f:  # noqa: PTH123
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


def plot_contrails_formed_over_time(
    forcing_stats_data: dict,  # type: ignore[type-arg]
    output_plot_name: str,
    temporal_granularity: TemporalGranularity,
) -> None:
    """Plots the number of contrails formed per temporal unit from the given dataframe.

    Args:
        forcing_stats_data: A dictionary containing the forcing statistics data, including
            the temporal granularity and histograms for distance forming contrails, distance flown,
            and air traffic density.
        output_plot_name: The name of the output HTML file (without extension) where the plot will be saved.
            The plot will be saved in the "results/plots" directory.
        temporal_granularity: The temporal granularity to use for the plot.
    """
    temporal_range, labels = _get_temporal_range_and_labels(temporal_granularity)
    time_label = "Time" if temporal_granularity == TemporalGranularity.HOURLY else "Day"
    temporal_granularity_key = str(TemporalGranularity.to_histogram_key(temporal_granularity))

    # Extract values from dictionaries
    distance_forming_contrails_per_temporal_histogram = np.array(
        [
            forcing_stats_data["distance_forming_contrails_over_time_histogram"][
                temporal_granularity_key
            ].get(str(i), 0)
            for i in temporal_range
        ]
    )
    # change to float type to avoid issues with division later on
    distance_forming_contrails_per_temporal_histogram = (
        distance_forming_contrails_per_temporal_histogram.astype(float)
    )

    distance_flown_per_temporal_histogram = np.array(
        [
            forcing_stats_data["distance_flown_over_time_histogram"][temporal_granularity_key].get(
                str(i), 0
            )
            for i in temporal_range
        ]
    )
    distance_flown_per_temporal_histogram = distance_flown_per_temporal_histogram.astype(float)

    air_traffic_density_per_temporal_histogram = np.array(
        [
            forcing_stats_data["air_traffic_density_over_time_histogram"][
                temporal_granularity_key
            ].get(str(i), 0)
            for i in temporal_range
        ]
    )

    percentage_of_distance_forming_contrails = (
        np.divide(
            distance_forming_contrails_per_temporal_histogram,
            distance_flown_per_temporal_histogram,
            out=np.zeros_like(distance_forming_contrails_per_temporal_histogram, dtype=float),
            where=distance_flown_per_temporal_histogram != 0,
        )
        * 100
    )

    # Plot contrails distance on primary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Scatter(
            x=np.arange(len(temporal_range)),
            y=percentage_of_distance_forming_contrails,
            mode="lines+markers",
            name="Percentage of Distance Forming Contrails",
            line={"color": "blue"},
            marker={"color": "blue"},
            customdata=labels,
            hovertemplate=(
                f"{time_label}: %{{customdata}}<br>"
                "Percent of distance forming contrails: %{y:.2f}%"
            ),
        ),
        secondary_y=False,
    )

    # Plot air traffic density on secondary y-axis
    fig.add_trace(
        go.Scatter(
            x=np.arange(len(temporal_range)),
            y=air_traffic_density_per_temporal_histogram,
            mode="lines+markers",
            name="Air Traffic Density",
            line={"color": "red"},
            marker={"color": "red"},
            customdata=labels,
            hovertemplate=(f"{time_label}: %{{customdata}}<br>Aircraft: %{{y:.0f}}<extra></extra>"),
        ),
        secondary_y=True,
    )

    fig.update_traces(
        marker={"size": 8},
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
        plot_bgcolor="white",
        paper_bgcolor="white",
        title="Distance Forming Contrails and Air Traffic Density --"
        f" {temporal_granularity.value.capitalize()}",
        legend={
            "x": 0.90,
            "y": 0.99,
            "xanchor": "right",
            "yanchor": "top",
            "font": {"size": 12},
            "itemsizing": "constant",
            "itemwidth": 30,
            "bgcolor": "rgba(255, 255, 255, 0.7)",
            "bordercolor": "lightgray",
            "borderwidth": 1,
        },
    )

    fig.update_xaxes(
        showgrid=True,
        showline=True,
        linecolor="black",
        gridcolor="lightgray",
        zeroline=True,
        zerolinecolor="lightgray",
        zerolinewidth=1,
        mirror=True,
    )
    fig.update_yaxes(
        title_text="Percentage of Distance\\ Forming Contrails (%)",
        title_font={"color": "blue"},
        rangemode="tozero",
        showline=True,
        linecolor="black",
        gridcolor="lightgray",
        mirror=True,
        secondary_y=False,
    )
    fig.update_yaxes(
        rangemode="tozero",
        title_text="Number of Aircraft",
        title_font={"color": "red"},
        showgrid=False,
        mirror=True,
        secondary_y=True,
    )

    if temporal_granularity == TemporalGranularity.HOURLY:
        fig.update_xaxes(
            title_text=f"Time--{temporal_granularity.value.capitalize()}",
            tickmode="array",
            tickvals=list(range(len(temporal_range))),
            ticktext=labels,
        )
    elif temporal_granularity == TemporalGranularity.DAILY:
        daily_tick_indices = [0, *list(range(29, len(temporal_range), 30))]
        fig.update_xaxes(
            title_text="Day of Year",
            tickmode="array",
            tickvals=daily_tick_indices,
            ticktext=[labels[i] for i in daily_tick_indices],
        )

    # Save the plot to the specified output path
    fig.write_html(
        f"results/plots/{output_plot_name}.html",
        config={"displaylogo": False},
        full_html=False,
        include_plotlyjs="cdn",
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.info("Starting to plot contrails formed over time from JSON data.")
    plot_contrails_formed_over_time_from_json(
        name_of_forcing_stats_file="energy_forcing_statistics_week_1_2024",
        output_plot_name="contrails_formed",
    )
