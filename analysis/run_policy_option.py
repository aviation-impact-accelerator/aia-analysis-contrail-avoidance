"""Run policy option analysis functions."""  # noqa: INP001

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

import polars as pl
from generate_energy_forcing_statistics_from_filepath import (
    generate_energy_forcing_statistics,
)

from aia_model_contrail_avoidance.policy.mechanisms import (
    ContrailAvoidancePolicy,
    apply_contrail_avoidance_policy,
)
from aia_model_contrail_avoidance.policy.policy_results import calculate_policy_effect

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# save results to Json Statistics
if __name__ == "__main__":
    ADS_B_ANALYSIS_DIR = Path("~/ads_b_analysis").expanduser()
    FLIGHTS_WITH_EF_DIR = ADS_B_ANALYSIS_DIR / "ads_b_flights_with_ef"
    first_day = 1
    final_day = 7

    policy_statistics_json = "policy_01_week_01"
    save_path = "policy_data/" + policy_statistics_json
    # load data for first week of January
    start = time.time()
    energy_forcing_paraquet_files = sorted(FLIGHTS_WITH_EF_DIR.glob("UK_flights_day*"))
    complete_flight_dataframe: pl.DataFrame = pl.DataFrame()
    logger.info("Found %s files in directory.", len(energy_forcing_paraquet_files))
    if len(energy_forcing_paraquet_files) < final_day:
        logger.info(
            "Generating Statistics from files %s to %s.",
            first_day,
            len(energy_forcing_paraquet_files),
        )
    else:
        logger.info("Generating Statistics from files %s to %s.", first_day, final_day)
    for parquet_file in energy_forcing_paraquet_files[first_day - 1 : final_day]:
        # read and append all dataframes together
        daily_dataframe = pl.read_parquet(parquet_file)
        if complete_flight_dataframe.is_empty():
            complete_flight_dataframe = daily_dataframe
        else:
            complete_flight_dataframe = pl.concat([complete_flight_dataframe, daily_dataframe])

    # for each file in directory apply policy
    selected_dataframe = apply_contrail_avoidance_policy(
        ContrailAvoidancePolicy.AVOID_ALL_CONTRAILS_AT_NIGHT_IN_UK_AIRSPACE,
        complete_flight_dataframe,
    )

    generate_energy_forcing_statistics(selected_dataframe, save_path)

    # read json files and save results
    with Path("results/" + save_path + ".json").open() as f:
        contrail_policy_stats = json.load(f)
    logger.info("Loaded data from %s", "results/" + save_path + ".json")

    with Path("results/energy_forcing_statistics_week_1_coarse_pha_2024.json").open() as f:
        energy_forcing_stats = json.load(f)
    logger.info(
        "Loaded data from %s", "results/energy_forcing_statistics_week_1_coarse_pha_2024.json"
    )

    results_file = calculate_policy_effect(
        energy_forcing_results=energy_forcing_stats,
        policy_results=contrail_policy_stats,
    )

    with Path("results/policy_data/policy_effect_week_1_2024.json").open("w") as f:
        json.dump(results_file, f, indent=4)

    logger.info("Calculated policy effect and updated results dictionary.")
