"Policy mechanisms for contrail avoidance model."

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl


class ContrailAvoidancePolicy(str, Enum):
    """Enumeration of contrail avoidance policies."""

    AVOID_ALL_CONTRAILS_AT_NIGHT_IN_UK_AIRSPACE = "avoid all contrails at night in UK airspace"
    AVOID_WINTER_CONTRAILS_IN_UK_AIRSPACE = "avoid winter contrails in UK airspace"
    AVOID_ALL_CONTRAILS_IN_UK_AIRSPACE = "avoid all contrails in UK airspace"
    NO_AVOIDANCE = "no avoidance"


ALL_POLICIES = [policy.value for policy in ContrailAvoidancePolicy]


def apply_contrail_avoidance_policy(
    policy: ContrailAvoidancePolicy, flight_dataframe: pl.DataFrame
) -> pl.DataFrame:
    """Apply the specified contrail avoidance policy to the flight data.

    Args:
        policy (ContrailAvoidancePolicy): The contrail avoidance policy to apply.
        flight_dataframe (pl.DataFrame): DataFrame containing flight data.

    Returns:
        pl.DataFrame: DataFrame with all datapoints in the scope of the policy.
    """
    match policy:
        case ContrailAvoidancePolicy.AVOID_ALL_CONTRAILS_AT_NIGHT_IN_UK_AIRSPACE:
            return run_policy_avoid_contrails_at_night_in_uk_airspace(flight_dataframe)
        case ContrailAvoidancePolicy.AVOID_WINTER_CONTRAILS_IN_UK_AIRSPACE:
            return run_policy_avoid_winter_contrails_in_uk_airspace(flight_dataframe)
        case ContrailAvoidancePolicy.AVOID_ALL_CONTRAILS_IN_UK_AIRSPACE:
            return run_policy_avoid_contrails_in_uk_airspace(flight_dataframe)

        case _:
            msg = f"Contrail avoidance policy not currently supported: {policy}"
            raise ValueError(msg)


def run_policy_avoid_contrails_at_night_in_uk_airspace(
    flight_dataframe: pl.DataFrame,
) -> pl.DataFrame:
    """Run the policy to avoid all contrails at night in UK airspace.

    Args:
        flight_dataframe (pl.DataFrame): DataFrame containing flight data.

    Returns:
        pl.DataFrame: DataFrame with all datapoints in the scope of the policy.
    """
    start_of_night_hour = 18
    end_of_night_hour = 4

    uk_airspace_flight_dataframe = flight_dataframe.filter(
        flight_dataframe["airspace"].is_not_null()
    )
    return uk_airspace_flight_dataframe.filter(
        (uk_airspace_flight_dataframe["timestamp"].dt.hour() < end_of_night_hour)
        | (uk_airspace_flight_dataframe["timestamp"].dt.hour() >= start_of_night_hour)
    )


def run_policy_avoid_winter_contrails_in_uk_airspace(
    flight_dataframe: pl.DataFrame,
) -> pl.DataFrame:
    """Run the policy to avoid winter contrails in UK airspace.

    Args:
        flight_dataframe (pl.DataFrame): DataFrame containing flight data.

    Returns:
        pl.DataFrame: DataFrame with all datapoints in the scope of the policy.
    """
    winter_start_month = 11  # November
    winter_end_month = 3  # March

    uk_airspace_flight_dataframe = flight_dataframe.filter(
        flight_dataframe["airspace"].is_not_null()
    )
    return uk_airspace_flight_dataframe.filter(
        (uk_airspace_flight_dataframe["timestamp"].dt.month() >= winter_start_month)
        | (uk_airspace_flight_dataframe["timestamp"].dt.month() <= winter_end_month)
    )


def run_policy_avoid_contrails_in_uk_airspace(
    flight_dataframe: pl.DataFrame,
) -> pl.DataFrame:
    """Run the policy to avoid all contrails in UK airspace.

    Args:
        flight_dataframe (pl.DataFrame): DataFrame containing flight data.

    Returns:
        pl.DataFrame: DataFrame with all datapoints in the scope of the policy.
    """
    return flight_dataframe.filter(flight_dataframe["airspace"].is_not_null())
