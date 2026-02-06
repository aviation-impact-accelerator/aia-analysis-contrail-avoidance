"""Module for airspace-related operations."""

from __future__ import annotations

import polars as pl
import shapely
from traffic.data import eurofirs


def get_gb_airspaces() -> list:  # type: ignore[type-arg]
    """Retrieve airspace data for Great Britain FIRs."""
    # Select FIRs by their 'designator' attribute
    selected = []
    for fir in eurofirs:
        if getattr(fir, "designator", None) in ["EGTT", "EGPX", "EGGX"]:
            selected.append(fir)  # noqa: PERF401
    return selected


def find_uk_airspace_of_flight_segment(
    flight_dataframe: pl.DataFrame,
) -> pl.DataFrame:
    """Check if a given flight segment is within any of the UK airspaces."""
    gb_airspaces = get_gb_airspaces()
    # remove datapoints outside of UK environment
    flight_dataframe_within_uk = flight_dataframe.filter(
        pl.col("longitude").is_between(-8, 3) & pl.col("latitude").is_between(49, 62)
    )
    # add airspace information to dataframe
    datapoints_within_uk = find_airspace_of_flight_segment(flight_dataframe_within_uk, gb_airspaces)

    flight_dataframe_outside_uk = flight_dataframe.filter(
        ~(pl.col("longitude").is_between(-8, 3) & pl.col("latitude").is_between(49, 62))
    )
    # add airspace column with None for datapoints outside of UK environment
    flight_dataframe_outside_uk = flight_dataframe_outside_uk.with_columns(
        pl.lit(None).alias("airspace")
    )

    # add airspace information to original dataframe, filling in None for datapoints outside of UK environment
    return pl.concat(
        [
            datapoints_within_uk,
            flight_dataframe_outside_uk,
        ]
    )


def find_airspace_of_flight_segment(
    flight_dataframe: pl.DataFrame,
    airspaces: list,  # type: ignore[type-arg]
) -> pl.DataFrame:
    """Check if a given flight segment is within any of the provided airspaces.

    Args:
        flight_dataframe: Flight dataframe
        airspaces: List of airspace objects with 'shape' attribute.

    Returns:
        Polars DataFrame with an additional column 'airspace' indicating the name of the airspace
        if the point is within any airspace, otherwise None.
    """
    points = shapely.points(flight_dataframe["longitude"], flight_dataframe["latitude"])

    # add new column indicating the name of the airspace if the point is within any airspace
    return flight_dataframe.with_columns(
        pl.Series("airspace", name_of_airspace_of_point(points, airspaces))  # type: ignore[arg-type]
    )


def name_of_airspace_of_point(points: list[shapely.Point], airspaces: list) -> list[str | None]:  # type: ignore[type-arg]
    """Check if points are within any of the provided airspaces.

    Args:
        points: List of shapely Points to check.
        airspaces: List of airspace objects with 'shape' attribute.

    Returns:
        List of airspace names or None if the point is not in any airspace.
    """
    names = []
    for point in points:
        found_name = None
        for airspace in airspaces:
            if shapely.contains(airspace.shape, point):
                found_name = getattr(airspace, "name", "?")
                break
        names.append(found_name)
    return names
