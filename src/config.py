"""
Central configuration for the project.

All paths are resolved relative to the project root (the parent of ``src/``),
so every script works no matter which directory it is launched from.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
CLEANED_DIR = DATA_DIR / "cleaned"
ANALYTICS_DIR = DATA_DIR / "analytics"

OUTPUTS_DIR = PROJECT_ROOT / "outputs"
VISUALIZATIONS_DIR = OUTPUTS_DIR / "visualizations"
REPORTS_DIR = OUTPUTS_DIR / "reports"

LOG_DIR = PROJECT_ROOT / "logs"

ALL_DIRS = [
    RAW_DIR, CLEANED_DIR, ANALYTICS_DIR,
    VISUALIZATIONS_DIR, REPORTS_DIR, LOG_DIR,
]

# ---------------------------------------------------------------------------
# Data generation (spec section 3)
# ---------------------------------------------------------------------------
DEFAULT_TOTAL_RECORDS = 10_000_000
DEFAULT_BATCH_SIZE = 500_000
RANDOM_SEED = 42

START_DATE = datetime(2022, 1, 1)
END_DATE = datetime(2024, 12, 31, 23, 59, 59)

PRODUCT_CATEGORIES = [
    "Electronics", "Fashion", "Home & Garden", "Sports",
    "Books", "Beauty", "Toys", "Food",
]

PAYMENT_METHODS = ["Credit Card", "Debit Card", "Mobile Money", "Bank Transfer"]
PAYMENT_WEIGHTS = [0.40, 0.30, 0.20, 0.10]

STATUSES = ["SUCCESS", "FAILED"]
STATUS_WEIGHTS = [0.85, 0.15]

# Intentional data-quality issues (fractions of all records)
NULL_RATE = 0.04            # missing city / email (3-5%)
DUPLICATE_RATE = 0.015      # duplicate transaction_ids (1-2%)
NEGATIVE_RATE = 0.005       # negative price or quantity (0.5%)
MISMATCH_RATE = 0.02        # total_amount != price * quantity (2%)

# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------
SPARK_MASTER = "local[*]"
SPARK_DRIVER_MEMORY = "4g"
SPARK_SHUFFLE_PARTITIONS = "200"

HIGH_VALUE_THRESHOLD = 500.0
