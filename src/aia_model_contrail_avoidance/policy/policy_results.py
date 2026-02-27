"""Policy results calculation for contrail avoidance."""

from __future__ import annotations

from typing import Any


def calculate_policy_effect(
    energy_forcing_results: dict[str, Any], policy_results: dict[str, Any]
) -> dict[str, Any]:
    """Calculate the effect of the policy and update the results dictionary.

    Args:
        energy_forcing_results (dict): Dictionary containing energy forcing results for the selected dataframe.
        policy_results (dict): Dictionary to be updated with calculated effects of the policy.

    Returns:
        dict: Results dictionary with calculated effects.
    """
    return {
        "number_of_flights_in_scope": {
            "total": policy_results["number_of_flights"]["total"],
            "regional": policy_results["number_of_flights"]["regional"],
            "international": policy_results["number_of_flights"]["international"],
        },
        "number_of_flights_deviated": {
            "total": policy_results["contrail_formation"]["flights_forming_contrails"],
        },
        "energy_forcing_before": {
            "total": energy_forcing_results["energy_forcing"]["total"],
            "uk_airspace": energy_forcing_results["energy_forcing"]["uk_airspace"],
            "international_airspace": energy_forcing_results["energy_forcing"][
                "international_airspace"
            ],
        },
        "energy_forcing_after": {
            "total": energy_forcing_results["energy_forcing"]["total"]
            - policy_results["energy_forcing"]["total"],
            "uk_airspace": energy_forcing_results["energy_forcing"]["uk_airspace"]
            - policy_results["energy_forcing"]["uk_airspace"],
            "international_airspace": energy_forcing_results["energy_forcing"][
                "international_airspace"
            ]
            - policy_results["energy_forcing"]["international_airspace"],
        },
        "emissions": {
            "total_co2_emissions_from_fuel_burn": energy_forcing_results["emissions"][
                "total_co2_emissions_from_fuel_burn"
            ],
            "total_co2_equivalent_emissions_from_contrails_before_policy": energy_forcing_results[
                "emissions"
            ]["total_co2_equivalent_emissions_from_contrails"],
            "total_co2_equivalent_emissions_from_contrails_after_policy": energy_forcing_results[
                "emissions"
            ]["total_co2_equivalent_emissions_from_contrails"]
            - policy_results["emissions"]["total_co2_equivalent_emissions_from_contrails"],
        },
    }
