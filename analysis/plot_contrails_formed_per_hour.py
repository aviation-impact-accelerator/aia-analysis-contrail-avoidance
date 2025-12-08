"""Defines a function to plot the number of contrails formed per hour."""  # noqa: INP001

import json

import matplotlib.pyplot as plt
import numpy as np


def plot_contrails_formed_per_hour(
    name_of_flights_stats_file: str, name_of_forcing_stats_file: str, output_plot_name: str
) -> None:
    """Plots the number of contrails formed per hour from the given dataframe.

    Args:
        name_of_flights_stats_file (str): The name of the flights stats file to load data from.
        name_of_forcing_stats_file (str): The name of the forcing stats file to load data from.
        output_plot_name (str): The name of the output plot file.
    """
    # Load the data from the specified stats file
    with open(f"results/{name_of_flights_stats_file}.json") as f:  # noqa: PTH123
        flights_stats_data = json.load(f)
    with open(f"results/{name_of_forcing_stats_file}.json") as f:  # noqa: PTH123
        forcing_stats_data = json.load(f)

    # Extract values from dictionaries in hour order (0-23)
    distance_forming_contrails_per_hour_histogram = np.array(
        [
            forcing_stats_data["distance_forming_contrails_per_hour_histogram"][str(i)]
            for i in range(24)
        ]
    )
    distance_flown_per_hour_histogram = np.array(
        [forcing_stats_data["distance_flown_per_hour_histogram"][str(i)] for i in range(24)]
    )
    planes_per_hour_histogram = np.array(
        [flights_stats_data["planes_per_hour_histogram"][str(i)] for i in range(24)]
    )

    percentage_of_distance_forming_contrails = (
        np.divide(
            distance_forming_contrails_per_hour_histogram,
            distance_flown_per_hour_histogram,
            out=np.zeros_like(distance_forming_contrails_per_hour_histogram, dtype=float),
            where=distance_flown_per_hour_histogram != 0,
        )
        * 100
    )

    # Create the plot
    fig, ax1 = plt.subplots(figsize=(10, 6))

    # Plot contrails distance on primary y-axis
    ax1.plot(
        range(24),
        percentage_of_distance_forming_contrails,
        color="skyblue",
        marker="o",
        label="Distance forming contrails (nm)",
    )
    ax1.set_xlabel("Hour of Day")
    ax1.set_ylabel("Percentage of Distance Forming Contrails (%)", color="skyblue")
    ax1.tick_params(axis="y", labelcolor="skyblue")
    ax1.set_xticks(np.arange(0, 24))
    ax1.set_xticklabels([f"{i:02d}:00" for i in range(24)], rotation=20)
    ax1.grid(axis="y", alpha=0.3)

    # Plot aircraft count on secondary y-axis
    ax2 = ax1.twinx()
    ax2.plot(
        range(24), planes_per_hour_histogram, color="orange", marker="s", label="Number of aircraft"
    )
    ax2.set_ylabel("Number of Aircraft", color="orange")
    ax2.tick_params(axis="y", labelcolor="orange")

    # Add title and legend
    plt.title("Distance Forming Contrails and Aircraft Density Per Hour")

    # Combine legends
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

    # Save the plot to the specified output path
    plt.savefig(f"results/plots/{output_plot_name}.png")
    plt.close()


if __name__ == "__main__":
    plot_contrails_formed_per_hour(
        name_of_flights_stats_file="2024_01_01_sample_stats_processed",
        name_of_forcing_stats_file="energy_forcing_statistics",
        output_plot_name="contrails_formed_per_hour_plot",
    )
