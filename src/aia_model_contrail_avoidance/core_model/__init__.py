"""Core model for contrail avoidance analysis."""

from __future__ import annotations

from aia_model_contrail_avoidance.core_model.dimensions import (
    SpatialGranularity,
    TemporalGranularity,
    _get_temporal_grouping_field,
    _get_temporal_range_and_labels,
)

__all__ = (
    "SpatialGranularity",
    "TemporalGranularity",
    "_get_temporal_grouping_field",
    "_get_temporal_range_and_labels",
)
