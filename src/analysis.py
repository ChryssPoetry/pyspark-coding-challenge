"""
Analysis helpers for Day 3 of the pyspark‑coding‑challenge project.

This module introduces additional analytics beyond the simple data quality
metrics provided in :mod:`src.metrics`.  In particular, it exposes
functions to compute the distribution of historical sequence lengths
and to calculate per‑position frequencies of each action type.  These
functions can be used to better understand how sparse or dense the
historical sequences are and whether certain actions dominate the
impression history.

The primary entry points are:

* :func:`sequence_length_distribution` – produce a mapping from
  the count of non‑padded historical actions to the number of rows
  exhibiting that length.
* :func:`action_type_frequency_by_position` – compute the frequency
  of each action type at each position in the sequences.

These functions operate on the output of
``build_training_inputs`` (see :mod:`src.transformations`) and are
designed to return native Python structures that can easily be
serialised or consumed by plotting libraries.
"""

from typing import Dict, List, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def sequence_length_distribution(df: DataFrame) -> Dict[int, int]:
    """Compute the distribution of non‑padded historical sequence lengths.

    For each row in the provided DataFrame, the length of the
    non‑zero portion of the ``actions`` array is determined (i.e. the
    number of action IDs that are not equal to zero).  The function
    then counts how many rows have each distinct length and returns
    these counts as a dictionary keyed by the sequence length.

    Parameters
    ----------
    df: DataFrame
        The DataFrame produced by ``build_training_inputs``.  It must
        contain an ``actions`` column of type ``array<int>``.

    Returns
    -------
    dict
        A mapping from integer sequence length to the number of rows
        exhibiting that length.
    """
    # Compute the non‑zero length for each row
    lengths_df = df.select(
        F.expr(
            "aggregate(actions, 0, (acc, x) -> acc + CASE WHEN x != 0 THEN 1 ELSE 0 END)"
        ).alias("hist_len")
    )
    # Count the frequency of each length
    counts = (
        lengths_df.groupBy("hist_len").count().collect()
    )
    return {int(row["hist_len"]): int(row["count"]) for row in counts}


def action_type_frequency_by_position(df: DataFrame, max_positions: int = 10) -> List[Dict[str, float]]:
    """Compute per‑position action type frequencies for the first ``max_positions``.

    This function analyses the ``action_types`` array column of the
    training DataFrame and, for each position up to
    ``max_positions``, determines the relative frequency of each
    action type code.  This can be used to identify biases in the
    historical sequences – for example, whether clicks (1) tend to
    dominate the most recent positions while orders (3) appear
    further back in the history.

    Parameters
    ----------
    df: DataFrame
        The DataFrame returned by ``build_training_inputs``.  It must
        contain an ``action_types`` column of type ``array<byte>``.
    max_positions: int, optional
        The number of positions from the start of the sequence to
        analyse.  Defaults to 10.  If ``max_positions`` is greater
        than the length of ``action_types``, the function will only
        analyse up to the available positions.

    Returns
    -------
    list of dict
        A list where each element corresponds to a position (0‑based
        index) and contains a mapping from action type code (as a
        string) to its relative frequency at that position.  The list
        length will be equal to ``max_positions`` or the actual
        sequence length if shorter.
    """
    frequencies: List[Dict[str, float]] = []
    for pos in range(max_positions):
        # Extract the action type at the specified position for all rows
        # Use element_at which is 1‑based; pos + 1 selects the pos
        position_df = df.select(F.element_at("action_types", pos + 1).alias("atype"))
        # Count occurrences of each type
        counts = position_df.groupBy("atype").count().collect()
        total = sum(row["count"] for row in counts)
        # Normalise counts to frequencies
        freq: Dict[str, float] = {}
        for row in counts:
            key = str(row["atype"])  # Convert ByteType to string key
            freq[key] = row["count"] / total if total > 0 else 0.0
        frequencies.append(freq)
    return frequencies


__all__ = ["sequence_length_distribution", "action_type_frequency_by_position"]