"""Module to calculate equivalent metrics for climate warming from the model output."""

from __future__ import annotations

__all__ = [
    "calculate_co2_mass_burned_from_flight_distance",
    "calculate_co2_mass_equivalent_from_energy_forcing",
    "calculate_energy_forcing_from_flight_distance",
]

# Global constants taken from literature introducing the CoCiPGrid model by Engberg et al. 2025

CO2_PER_FUEL_MASS_BURNED = 3.16  # kg CO2 per kg fuel
"""Conversion factor from fuel mass burned to CO2 mass emitted,
Units: kg of CO2 emitted per kg of fuel burned."""

AVERAGE_FUEL_BURN_PER_FLIGHT_DISTANCE = 11.0  # kg fuel/ nautical mile
"""ICAO estimate of average fuel burn per flight distance for a typical commercial flight.
Units: kg of fuel burned per nautical mile traveled by a flight."""

AVERAGE_CO2_ENERGY_FORCING_PER_FLIGHT_DISTANCE_FROM_FUEL = 3.41e7  # J/m traveled
"""Average energy forcing from CO2 emissions per meter traveled by a flight,
based on calculation in Teoh et al 2020 and a 100 year time horizon for global warming potential.
Units: Joules of energy forcing per meter traveled by a flight."""

RATIO_ERF_TO_EF = 0.42  # Dimensionless
"""Ratio of effective radiative forcing (ERF) to energy forcing (EF) for contrails,
based on Lee et al 2021.
Units: Dimensionless."""

ABSOLUTE_GLOBAL_WARMING_POTENTIAL_OVER_100_YEARS = 7.54e-7  # J/m^2 kg C02
"""Absolute global warming potential over 100 years for CO2,
based on calculation in Gaillot et al 2023.
Units: Joules of energy forcing per square meter per kg of CO2 emitted over 100 years."""

SURFACE_AREA_OF_EARTH = 5.1e14  # m^2
"""Surface area of the Earth.
Units: square meters."""

NAUTICAL_MILES_TO_METERS = 1852  # m/ nautical mile
"""Conversion factor from nautical miles to meters.
Units: meters per nautical mile."""

# Unused constants kept for reference and potential future use:

ABSOLUTE_GLOBAL_WARMING_POTENTIAL_OVER_20_YEARS = 2.78e-6  # J/m^2 kg C02
"""Absolute global warming potential over 20 years for CO2,
based on calculation in Gaillot et al 2023.
Units: Joules of energy forcing per square meter per kg of CO2 emitted over 20 years."""

CO2_EQUIVALENT_ENERGY_PER_FUEL_MASS = 4.70e9  # J/kg fuel
"""Conversion factor from fuel mass to CO2 equivalent energy forcing,
based on calculation in Teoh et al 2020 and a 100 year time horizon for global warming potential.
Units: Joules of energy forcing per kg of fuel burned."""


def calculate_co2_mass_equivalent_from_energy_forcing(energy_forcing: float) -> float:
    """Calculate the global warming potential for a given energy forcing.

    Args:
        energy_forcing: The energy forcing value to convert to global warming potential.

    Returns:
        The global warming potential corresponding to the given energy forcing in Joules
    """
    numerator = energy_forcing * RATIO_ERF_TO_EF
    denominator = ABSOLUTE_GLOBAL_WARMING_POTENTIAL_OVER_100_YEARS * SURFACE_AREA_OF_EARTH
    return numerator / denominator


def calculate_co2_mass_burned_from_flight_distance(distance_nm: float) -> float:
    """Calculate the CO2 mass equivalent for a given flight distance.

    Args:
        distance_nm: The distance traveled by the flight in nautical miles.

    Returns:
        The CO2 mass equivalent corresponding to the given flight distance in kilograms.
    """
    total_mass_of_fuel_burned = distance_nm * AVERAGE_FUEL_BURN_PER_FLIGHT_DISTANCE
    return total_mass_of_fuel_burned * CO2_PER_FUEL_MASS_BURNED


def calculate_energy_forcing_from_flight_distance(distance_nm: float) -> float:
    """Calculate the CO2 energy forcing for a given flight distance.

    Args:
        distance_nm: The distance traveled by the flight in nautical miles.

    Returns:
        The CO2 energy forcing corresponding to the given flight distance.
    """
    return (
        distance_nm
        * NAUTICAL_MILES_TO_METERS
        * AVERAGE_CO2_ENERGY_FORCING_PER_FLIGHT_DISTANCE_FROM_FUEL
    )
