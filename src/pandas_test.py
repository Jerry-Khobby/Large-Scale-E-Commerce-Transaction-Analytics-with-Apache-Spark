"""
Stage 2 - Demonstrate the limits of Pandas on the full dataset.

1. Load a 1 million-row sample with Pandas (expected to succeed).
2. Attempt to load the *entire* raw dataset with Pandas, recording load time,
   memory growth and any ``MemoryError``.

Results are logged to ``logs/pandas_test.log`` and saved to
``outputs/reports/pandas_test_results.csv`` for the README / report.

Take Task Manager screenshots while step 2 runs (spec Task 2.2) and save them in
``outputs/reports/``.

Usage:
    python src/pandas_test.py --sample-rows 1000000
"""

from __future__ import annotations

import argparse
import csv
import time

import pandas as pd
import psutil
import pyarrow.dataset as ds

import config
from utils import dir_size_bytes, ensure_dirs, get_logger

logger = get_logger("PandasTest", "pandas_test.log")
process = psutil.Process()


def rss_mb() -> float:
    """Current resident memory of this process in MB."""
    return process.memory_info().rss / 1024 ** 2


def load_sample(n_rows: int) -> dict:
    """Load the first ``n_rows`` rows of the raw dataset into Pandas."""
    logger.info(f"[sample] Loading {n_rows:,} rows with Pandas")
    mem_before = rss_mb()
    start = time.perf_counter()

    dataset = ds.dataset(str(config.RAW_DIR), format="parquet", partitioning="hive")
    df = dataset.head(n_rows).to_pandas()

    elapsed = time.perf_counter() - start
    df_mb = df.memory_usage(deep=True).sum() / 1024 ** 2
    logger.info(f"[sample] Loaded {len(df):,} rows in {elapsed:.2f}s | "
                f"DataFrame {df_mb:,.0f} MB | process +{rss_mb() - mem_before:,.0f} MB")
    result = {"test": f"sample_{n_rows}", "rows": len(df), "status": "SUCCESS",
              "load_seconds": round(elapsed, 2), "dataframe_mb": round(df_mb, 1),
              "peak_process_mb": round(rss_mb(), 1)}
    del df
    return result


def load_full() -> dict:
    """Try to load the entire raw dataset into a single Pandas DataFrame."""
    on_disk_gb = dir_size_bytes(config.RAW_DIR) / 1024 ** 3
    total_ram_gb = psutil.virtual_memory().total / 1024 ** 3
    logger.info(f"[full] Dataset on disk: {on_disk_gb:.2f} GB (compressed) | "
                f"machine RAM: {total_ram_gb:.1f} GB")
    logger.info("[full] Attempting pd.read_parquet on the full dataset...")

    start = time.perf_counter()
    try:
        df = pd.read_parquet(config.RAW_DIR, engine="pyarrow")
        elapsed = time.perf_counter() - start
        df_mb = df.memory_usage(deep=True).sum() / 1024 ** 2
        logger.info(f"[full] Loaded {len(df):,} rows in {elapsed:.2f}s | "
                    f"DataFrame {df_mb:,.0f} MB | process {rss_mb():,.0f} MB")
        return {"test": "full_dataset", "rows": len(df), "status": "SUCCESS",
                "load_seconds": round(elapsed, 2), "dataframe_mb": round(df_mb, 1),
                "peak_process_mb": round(rss_mb(), 1)}
    except MemoryError:
        elapsed = time.perf_counter() - start
        logger.error(f"[full] MemoryError after {elapsed:.2f}s - dataset does not fit in RAM")
        return {"test": "full_dataset", "rows": None, "status": "MemoryError",
                "load_seconds": round(elapsed, 2), "dataframe_mb": None,
                "peak_process_mb": round(rss_mb(), 1)}


def save_results(rows: list[dict]) -> None:
    out = config.REPORTS_DIR / "pandas_test_results.csv"
    with open(out, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"Results saved to {out}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Pandas limitation demonstration")
    parser.add_argument("--sample-rows", type=int, default=1_000_000)
    parser.add_argument("--skip-full", action="store_true",
                        help="only run the sample load")
    args = parser.parse_args()

    ensure_dirs()
    if not any(config.RAW_DIR.rglob("*.parquet")):
        logger.error(f"No Parquet files in {config.RAW_DIR}. Run generate_data.py first.")
        return

    results = [load_sample(args.sample_rows)]
    if not args.skip_full:
        results.append(load_full())
    save_results(results)


if __name__ == "__main__":
    main()
