"""
Core transformation logic for the pyspark‑coding‑challenge project.

These functions convert raw impression, click, add‑to‑cart and order
datasets into fixed‑length training inputs suitable for feeding a
transformer model.  The transformation ensures that no future data
leaks into the historical sequences, and enforces a one‑year
lookback window.  Sequences are padded to a fixed length with
zeros to simplify tensorization on the downstream side.
"""

from typing import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F, Window as W

from .constants import (
    ACTION_CLICK,
    ACTION_ADD_TO_CART,
    ACTION_PREVIOUS_ORDER,
    ACTION_PADDING,
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_MAX_SEQ,
    COL_DT,
    COL_RANKING_ID,
    COL_CUSTOMER_ID,
    COL_IMPRESSIONS,
    COL_ITEM_ID,
    COL_IS_ORDER,
    COL_IMPRESSION_ITEM_ID,
    COL_ACTIONS,
    COL_ACTION_TYPES,
)


def _assert_has_columns(df: DataFrame, cols: Iterable[str], name: str) -> None:
    """Internal helper to validate DataFrame columns at runtime."""
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{name} is missing columns: {missing}")


def explode_impressions(impressions_df: DataFrame) -> DataFrame:
    """
    Flatten the impressions array into one row per impression.

    The output contains the original dt, ranking_id, customer_id and
    extracted impression item_id and is_order flag.  If the
    impressions column is empty or null the row is preserved with
    nulls for impression_item_id and is_order.
    """
    _assert_has_columns(
        impressions_df,
        [COL_DT, COL_RANKING_ID, COL_CUSTOMER_ID, COL_IMPRESSIONS],
        "impressions_df",
    )
    return (
        impressions_df
        .withColumn("impression", F.explode_outer(COL_IMPRESSIONS))
        .select(
            COL_DT,
            COL_RANKING_ID,
            COL_CUSTOMER_ID,
            F.col("impression.item_id").alias(COL_IMPRESSION_ITEM_ID),
            F.col("impression.is_order").alias(COL_IS_ORDER),
        )
    )


def normalize_actions(clicks_df: DataFrame, atc_df: DataFrame, orders_df: DataFrame) -> DataFrame:
    """
    Normalize disparate action tables into a unified schema.

    Each row of the returned DataFrame contains customer_id, item_id,
    timestamp (ts) and a byte-encoded action_type.  Missing columns
    raise ValueError to aid debugging.
    """
    _assert_has_columns(clicks_df, [COL_CUSTOMER_ID, COL_ITEM_ID, "click_time"], "clicks_df")
    _assert_has_columns(atc_df, [COL_CUSTOMER_ID, "config_id", "occurred_at"], "atc_df")
    _assert_has_columns(orders_df, [COL_CUSTOMER_ID, "config_id", "order_date"], "orders_df")

    clicks = clicks_df.select(
        COL_CUSTOMER_ID,
        F.col("item_id").alias(COL_ITEM_ID),
        F.col("click_time").alias("ts"),
        F.lit(ACTION_CLICK).cast("byte").alias("action_type"),
    )
    atc = atc_df.select(
        COL_CUSTOMER_ID,
        F.col("config_id").alias(COL_ITEM_ID),
        F.col("occurred_at").alias("ts"),
        F.lit(ACTION_ADD_TO_CART).cast("byte").alias("action_type"),
    )
    orders = orders_df.select(
        COL_CUSTOMER_ID,
        F.col("config_id").alias(COL_ITEM_ID),
        F.to_timestamp("order_date").alias("ts"),
        F.lit(ACTION_PREVIOUS_ORDER).cast("byte").alias("action_type"),
    )
    return clicks.unionByName(atc).unionByName(orders)


def build_training_inputs(
    impressions_df: DataFrame,
    clicks_df: DataFrame,
    atc_df: DataFrame,
    orders_df: DataFrame,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    max_seq: int = DEFAULT_MAX_SEQ,
) -> DataFrame:
    """
    Produce fixed‑length historical sequences for each exploded impression.

    Parameters
    ----------
    impressions_df:
        Raw impressions DataFrame with nested impressions array.
    clicks_df:
        DataFrame of click events with timestamp column `click_time`.
    atc_df:
        DataFrame of add‑to‑cart events with timestamp column `occurred_at`.
    orders_df:
        DataFrame of previous order events with date column `order_date`.
    lookback_days:
        Number of days to include when collecting historical actions.  Defaults
        to 365 (one year).
    max_seq:
        Maximum length of the historical sequence.  Sequences are padded
        or truncated to this length.  Defaults to 1000.

    Returns
    -------
    DataFrame
        Contains one row per impression with the following columns:
        - dt, ranking_id, customer_id, impression_item_id, is_order
        - actions: array<int> of length `max_seq`, most recent first
        - action_types: array<byte> of length `max_seq`, most recent first
    """
    # 1) Flatten impressions to one row per impression
    imp = explode_impressions(impressions_df).alias("i")

    # 2) Normalize actions and apply temporal filters
    actions = normalize_actions(clicks_df, atc_df, orders_df).alias("a")
    joined = (
        imp.join(actions, on=(F.col("i.customer_id") == F.col("a.customer_id")), how="left")
        .where(F.col("a.ts") < F.to_timestamp(F.col("i.dt")))
        .where(F.col("a.ts") >= F.date_sub(F.to_timestamp(F.col("i.dt")), int(lookback_days)))
    )

    # 3) Assign reverse chronological row numbers within each impression key
    w = (
        W.partitionBy("i.dt", "i.ranking_id", "i.customer_id", "i.impression_item_id")
        .orderBy(F.col("a.ts").desc())
    )
    ranked = joined.withColumn("rn", F.row_number().over(w)).where(F.col("rn") <= int(max_seq))

    # 4) Aggregate to arrays while preserving order by sorting on rn
    agg = (
        ranked
        .groupBy("i.dt", "i.ranking_id", "i.customer_id", "i.impression_item_id", "i.is_order")
        .agg(
            F.sort_array(F.collect_list(F.struct("rn", F.col("a.item_id"))), asc=False).alias("_acts"),
            F.sort_array(F.collect_list(F.struct("rn", F.col("a.action_type"))), asc=False).alias("_types"),
        )
        .select(
            F.col("dt"),
            F.col("ranking_id"),
            F.col("customer_id"),
            F.col("impression_item_id"),
            F.col("is_order"),
            F.transform("_acts", lambda x: x[1]).alias("actions_raw"),
            F.transform("_types", lambda x: x[1]).alias("action_types_raw"),
        )
    )

    # 5) Right‑pad or truncate arrays to max_seq length
    actions_expr = (
        f"slice(array_concat(actions_raw, array_repeat(0, GREATEST(0, {int(max_seq)} - size(actions_raw)))), "
        f"1, {int(max_seq)})"
    )
    action_types_expr = (
        f"slice(array_concat(action_types_raw, array_repeat({int(ACTION_PADDING)}, GREATEST(0, {int(max_seq)} - size(action_types_raw)))), "
        f"1, {int(max_seq)})"
    )
    out = (
        agg.withColumn(COL_ACTIONS, F.expr(actions_expr))
        .withColumn(COL_ACTION_TYPES, F.expr(action_types_expr))
        .drop("actions_raw", "action_types_raw")
    )
    return out