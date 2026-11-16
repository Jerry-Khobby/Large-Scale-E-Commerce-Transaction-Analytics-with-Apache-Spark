"""
Stage 1 - Synthetic e-commerce transaction generator.

Generates millions of transactions that follow the schema and distributions in
the project specification (section 3) and writes them to ``data/raw/`` as
Snappy-compressed Parquet, partitioned by ``year_month``.

Speed: calling Faker for every field of every row is far too slow for tens of
millions of records. Instead, Faker builds pools of customers and products
once, and each batch is assembled with vectorised numpy sampling.

Intentional data-quality problems (spec section 3.3) are injected per batch:
missing city/email, duplicate transactions, negative price/quantity and
mismatched ``total_amount``.

Usage:
    python src/generate_data.py --records 20000000 --batch-size 500000 \
        --overwrite
"""

from __future__ import annotations

import argparse
import shutil
import sys
import time
import uuid

import numpy as np
import pandas as pd
import psutil
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker

import config
from utils import dir_size_bytes, ensure_dirs, get_logger

logger = get_logger("DataGeneration", "data_generation.log")

CUSTOMER_POOL_SIZE = 250_000
PRODUCTS_PER_CATEGORY = 60

# Explicit Arrow schema so every Parquet file is identical (spec section 3.1)
SCHEMA = pa.schema(
    [
        ("transaction_id", pa.string()),
        ("customer_id", pa.string()),
        ("customer_name", pa.string()),
        ("email", pa.string()),
        ("country", pa.string()),
        ("city", pa.string()),
        ("product_category", pa.string()),
        ("product_name", pa.string()),
        ("price", pa.float64()),
        ("quantity", pa.int32()),
        ("total_amount", pa.float64()),
        ("payment_method", pa.string()),
        ("status", pa.string()),
        ("transaction_timestamp", pa.timestamp("us")),
        ("year_month", pa.string()),
    ]
)


def build_customer_pool(
    fake: Faker, rng: np.random.Generator, size: int
) -> pd.DataFrame:
    """
    Create a reusable pool of customers (id, name, email, country, city).

    Calling Faker once per customer field is slow, so Faker supplies a few
    thousand realistic first names, last names, cities, countries and email
    domains, and numpy combines them into ``size`` customers.
    """
    logger.info(f"Building customer pool of {size:,} customers")
    first = np.array([fake.first_name() for _ in range(3_000)])
    last = np.array([fake.last_name() for _ in range(3_000)])
    cities = np.array([fake.city() for _ in range(5_000)])
    countries = np.array([fake.country_code() for _ in range(300)])
    domains = np.array([fake.free_email_domain() for _ in range(50)])

    first_s = pd.Series(rng.choice(first, size))
    last_s = pd.Series(rng.choice(last, size))
    ids = np.arange(1, size + 1)

    return pd.DataFrame(
        {
            "customer_id": [f"CUST-{i:07d}" for i in ids],
            "customer_name": first_s + " " + last_s,
            # suffix with the id so every customer email is unique
            "email": (
                first_s.str.lower()
                + "."
                + last_s.str.lower()
                + pd.Series(ids).astype(str)
                + "@"
                + pd.Series(rng.choice(domains, size))
            ),
            "country": rng.choice(countries, size),
            "city": rng.choice(cities, size),
        }
    )


def build_product_pool(fake: Faker, rng: np.random.Generator) -> pd.DataFrame:
    """Create a catalogue of products per category, each with a base price."""
    # Rough price ranges per category so analytics look realistic
    price_ranges = {
        "Electronics": (50, 1500),
        "Fashion": (10, 300),
        "Home & Garden": (15, 800),
        "Sports": (10, 600),
        "Books": (5, 80),
        "Beauty": (5, 150),
        "Toys": (5, 200),
        "Food": (2, 60),
    }
    rows = []
    for category in config.PRODUCT_CATEGORIES:
        low, high = price_ranges[category]
        names = set()
        while len(names) < PRODUCTS_PER_CATEGORY:
            names.add(
                f"{fake.color_name()} {fake.word().title()} "
                f"{category.split()[0]}"
            )
        for name in sorted(names):
            rows.append(
                (category, name, round(float(rng.uniform(low, high)), 2))
            )
    logger.info(f"Built product catalogue of {len(rows):,} products")
    return pd.DataFrame(
        rows, columns=["product_category", "product_name", "base_price"]
    )


# Relative shopping activity per hour of day (quiet overnight, evening peak)
HOUR_WEIGHTS = np.array(
    [
        2,
        1,
        1,
        1,
        1,
        2,
        3,
        5,
        6,
        7,
        8,
        8,
        9,
        8,
        7,
        7,
        8,
        9,
        10,
        11,
        11,
        9,
        6,
        4,
    ],
    dtype=float,
)
HOUR_WEIGHTS /= HOUR_WEIGHTS.sum()


def _day_weights() -> tuple[pd.DatetimeIndex, np.ndarray]:
    """Per-day sampling weights: weekend, Nov/Dec peak, yearly growth."""
    days = pd.date_range(
        config.START_DATE.date(), config.END_DATE.date(), freq="D"
    )
    dow, months, years = (
        np.asarray(x) for x in (days.dayofweek, days.month, days.year)
    )
    weekday = np.where(dow >= 5, 1.25, 1.0)
    month = np.select(
        [months == 11, months == 12, np.isin(months, [1, 2])],
        [1.35, 1.6, 0.85],
        1.0,
    )
    growth = 1.0 + 0.15 * (years - years.min())
    weights = weekday * month * growth
    return days, weights / weights.sum()


DAYS, DAY_WEIGHTS = _day_weights()


def sample_timestamps(n: int, rng: np.random.Generator) -> pd.Series:
    """Draw timestamps with realistic day-of-year and hour-of-day patterns."""
    day = DAYS[rng.choice(len(DAYS), n, p=DAY_WEIGHTS)]
    hour = rng.choice(24, n, p=HOUR_WEIGHTS)
    second = rng.integers(0, 3600, n)
    return pd.Series(day + pd.to_timedelta(hour * 3600 + second, unit="s"))


def generate_batch(
    n: int,
    customers: pd.DataFrame,
    products: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Generate ``n`` transactions, with intentional data-quality issues."""
    cust = customers.iloc[rng.integers(0, len(customers), n)].reset_index(
        drop=True
    )
    prod = products.iloc[rng.integers(0, len(products), n)].reset_index(
        drop=True
    )

    price = np.round(
        prod["base_price"].to_numpy() * rng.uniform(0.9, 1.1, n), 2
    )
    quantity = rng.integers(1, 6, n).astype(np.int32)

    timestamps = sample_timestamps(n, rng)

    df = pd.DataFrame(
        {
            "transaction_id": [str(uuid.uuid4()) for _ in range(n)],
            "customer_id": cust["customer_id"],
            "customer_name": cust["customer_name"],
            "email": cust["email"].astype(object),
            "country": cust["country"],
            "city": cust["city"].astype(object),
            "product_category": prod["product_category"],
            "product_name": prod["product_name"],
            "price": price,
            "quantity": quantity,
            "payment_method": rng.choice(
                config.PAYMENT_METHODS, n, p=config.PAYMENT_WEIGHTS
            ),
            "status": rng.choice(config.STATUSES, n, p=config.STATUS_WEIGHTS),
            "transaction_timestamp": timestamps,
        }
    )

    # --- Intentional data-quality issues (spec 3.3) -------------------------
    df.loc[rng.random(n) < config.NULL_RATE, "city"] = None
    df.loc[rng.random(n) < config.NULL_RATE, "email"] = None

    negative = rng.random(n) < config.NEGATIVE_RATE
    neg_price = negative & (rng.random(n) < 0.5)
    df.loc[neg_price, "price"] *= -1
    df.loc[negative & ~neg_price, "quantity"] *= -1

    df["total_amount"] = np.round(df["price"] * df["quantity"], 2)

    mismatch = rng.random(n) < config.MISMATCH_RATE
    df.loc[mismatch, "total_amount"] = np.round(
        df.loc[mismatch, "total_amount"]
        * rng.uniform(1.1, 1.5, mismatch.sum()),
        2,
    )

    # Duplicates: replace some rows with exact copies of other rows
    # (row count unchanged)
    n_dupes = int(n * config.DUPLICATE_RATE)
    if n_dupes:
        drop_idx = rng.choice(n, n_dupes, replace=False)
        keep = df.drop(index=drop_idx)
        copies = keep.iloc[rng.integers(0, len(keep), n_dupes)]
        df = pd.concat([keep, copies], ignore_index=True)

    df["year_month"] = df["transaction_timestamp"].dt.strftime("%Y-%m")
    return df[SCHEMA.names]


def write_batch(df: pd.DataFrame, batch_no: int) -> None:
    """Append a batch to the partitioned Parquet dataset in ``data/raw/``."""
    table = pa.Table.from_pandas(df, schema=SCHEMA, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=str(config.RAW_DIR),
        partition_cols=["year_month"],
        basename_template=f"batch-{batch_no:04d}-{{i}}.parquet",
        compression="snappy",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument(
        "--records",
        type=int,
        default=config.DEFAULT_TOTAL_RECORDS,
        help="total number of transactions to generate",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=config.DEFAULT_BATCH_SIZE,
        help="rows generated and written per batch",
    )
    parser.add_argument("--seed", type=int, default=config.RANDOM_SEED)
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="delete existing data/raw contents first",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensure_dirs()

    existing = [
        p for p in config.RAW_DIR.iterdir() if not p.name.startswith(".")
    ]
    if existing:
        if not args.overwrite:
            logger.error(
                f"{config.RAW_DIR} is not empty. "
                "Re-run with --overwrite to replace it."
            )
            sys.exit(1)
        logger.info(f"Removing existing data in {config.RAW_DIR}")
        for path in existing:  # keeps .gitkeep
            shutil.rmtree(path) if path.is_dir() else path.unlink()

    fake = Faker()
    Faker.seed(args.seed)
    rng = np.random.default_rng(args.seed)
    process = psutil.Process()
    peak_mem_mb = 0.0

    logger.info("Data generation started")
    logger.info(
        f"Target records: {args.records:,} | Batch size: {args.batch_size:,}"
    )
    start = time.time()

    try:
        customers = build_customer_pool(fake, rng, CUSTOMER_POOL_SIZE)
        products = build_product_pool(fake, rng)

        written = 0
        batch_no = 0
        while written < args.records:
            n = min(args.batch_size, args.records - written)
            write_batch(generate_batch(n, customers, products, rng), batch_no)
            written += n
            batch_no += 1

            mem_mb = process.memory_info().rss / 1024**2
            peak_mem_mb = max(peak_mem_mb, mem_mb)
            rate = written / (time.time() - start)
            logger.info(
                f"Batch {batch_no} written | "
                f"{written:,}/{args.records:,} records "
                f"| {rate:,.0f} rec/s | memory {mem_mb:,.0f} MB"
            )

        duration = time.time() - start
        size_gb = dir_size_bytes(config.RAW_DIR) / 1024**3
        n_files = sum(1 for _ in config.RAW_DIR.rglob("*.parquet"))

        logger.info("Data generation completed successfully")
        logger.info(f"Total records generated: {written:,}")
        logger.info(f"Total files: {n_files:,} | Total size: {size_gb:.2f} GB")
        logger.info(f"Total time taken: {duration:,.2f} s")
        logger.info(f"Peak memory usage: {peak_mem_mb:,.0f} MB")

    except Exception:
        logger.error("Data generation failed", exc_info=True)
        raise


if __name__ == "__main__":
    main()
