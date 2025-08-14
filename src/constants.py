"""
Constants and default parameters used throughout the pyspark‑coding‑challenge project.

Keeping these values in a dedicated module makes it easy to update
action type encodings, sequence lengths, or other configuration
options from a single place.  Downstream modules import from this
module rather than hard‑coding literal values.
"""

# Encoded action types expected by the downstream model
ACTION_CLICK: int = 1
ACTION_ADD_TO_CART: int = 2
ACTION_PREVIOUS_ORDER: int = 3
ACTION_PADDING: int = 0

# Temporal and sequence defaults
DEFAULT_LOOKBACK_DAYS: int = 365  # one year of history
DEFAULT_MAX_SEQ: int = 1000  # maximum length of the historical sequence

# Common column names used across schemas and transformations
COL_DT = "dt"
COL_RANKING_ID = "ranking_id"
COL_CUSTOMER_ID = "customer_id"
COL_IMPRESSIONS = "impressions"
COL_ITEM_ID = "item_id"
COL_IS_ORDER = "is_order"

# Normalized actions columns
COL_TS = "ts"
COL_ACTION_TYPE = "action_type"

# Output columns
COL_IMPRESSION_ITEM_ID = "impression_item_id"
COL_ACTIONS = "actions"
COL_ACTION_TYPES = "action_types"