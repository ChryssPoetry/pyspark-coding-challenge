"""
Ergonomic re-exports for the pyspark‑coding‑challenge project.

This package defines the core schemas, transformations and utilities
required to build training inputs from raw clickstream datasets.  By
exposing the most commonly used functions at the package level we
reduce the need for deep import paths in downstream modules and tests.
"""

from . import schema, config, constants  # noqa: F401
from .utils import get_spark, read_csv_with_schema, coerce_impressions_from_json  # noqa: F401
from .transformations import (
    explode_impressions,
    normalize_actions,
    build_training_inputs,
)  # noqa: F401

# Expose IO and metrics helpers at the package level
from .io import save_training_inputs  # noqa: F401
from .metrics import calculate_dq_metrics  # noqa: F401

# Day 3 analysis helpers
from .analysis import (
    sequence_length_distribution,
    action_type_frequency_by_position,
)  # noqa: F401