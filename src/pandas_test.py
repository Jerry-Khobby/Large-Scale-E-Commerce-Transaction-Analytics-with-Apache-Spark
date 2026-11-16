"""
Stage 2 - Demonstrate the limits of Pandas on the full dataset.

1. Load a 1 million-row sample with Pandas (expected to succeed).
2. Attempt to load the *entire* raw dataset with Pandas, recording load time,
   memory growth and any ``MemoryError``.

The full load runs in a child process watched by a memory guard: if system
memory usage passes ``--max-system-mem-pct`` the child is killed, so the
demonstration cannot freeze the machine by pushing it into heavy swapping.
Memory is sampled every 0.5 s to ``outputs/reports/pandas_memory_trace.csv``.

Results are logged to ``logs/pandas_test.log`` and saved to
``outputs/reports/pandas_test_results.csv`` for the README / report.

Take Task Manager screenshots while step 2 runs (spec Task 2.2) and save
them in
``outputs/reports/``.

Usage:
    python src/pandas_test.py --sample-rows 1000000
"""

from __future__ import annotations

import argparse
import csv
import multiprocessing as mp
import time

import pandas as pd
import psutil
import pyarrow.dataset as ds

import config
from utils import dir_size_bytes, ensure_dirs, get_logger

logger = get_logger("PandasTest", "pandas_test.log")


def rss_mb(process: psutil.Process | None = None) -> float:
    """Resident memory of ``process`` (default: this one) in MB."""
    return (process or psutil.Process()).memory_info().rss / 1024**2


def load_sample(n_rows: int) -> list[dict]:
    """
    Load the first ``n_rows`` rows into Pandas, then extrapolate the measured
    memory footprint and load time to the full dataset.
    """
    logger.info(f"[sample] Loading {n_rows:,} rows with Pandas")
    mem_before = rss_mb()
    start = time.perf_counter()

    dataset = ds.dataset(
        str(config.RAW_DIR), format="parquet", partitioning="hive"
    )
    df = dataset.head(n_rows).to_pandas()

    elapsed = time.perf_counter() - start
    df_mb = df.memory_usage(deep=True).sum() / 1024**2
    logger.info(
        f"[sample] Loaded {len(df):,} rows in {elapsed:.2f}s | "
        f"DataFrame {df_mb:,.0f} MB | process +{rss_mb() - mem_before:,.0f} MB"
    )
    results = [
        {
            "test": f"pandas_sample_{n_rows}",
            "rows": len(df),
            "status": "SUCCESS",
            "load_seconds": round(elapsed, 2),
            "dataframe_mb": round(df_mb, 1),
            "peak_process_mb": round(rss_mb(), 1),
        }
    ]

    # Project the measured per-row footprint onto the full dataset
    total_rows = dataset.count_rows()
    est_mb = df_mb / len(df) * total_rows
    est_seconds = elapsed / len(df) * total_rows
    total_ram_mb = psutil.virtual_memory().total / 1024**2
    logger.info(
        f"[estimate] Full dataset: {total_rows:,} rows -> "
        f"~{est_mb / 1024:,.1f} GB as a "
        f"Pandas DataFrame ({100 * est_mb / total_ram_mb:.0f}% of total "
        f"machine RAM), "
        f"~{est_seconds:,.0f}s to load"
    )
    results.append(
        {
            "test": "pandas_full_dataset_estimate",
            "rows": total_rows,
            "status": "EXCEEDS_RAM" if est_mb > total_ram_mb else "FITS_RAM",
            "load_seconds": round(est_seconds, 2),
            "dataframe_mb": round(est_mb, 1),
            "peak_process_mb": None,
        }
    )
    del df
    return results


def _full_load_worker(raw_dir: str, queue: mp.Queue) -> None:
    """Child process: read the whole dataset with Pandas and report back."""
    start = time.perf_counter()
    try:
        df = pd.read_parquet(raw_dir, engine="pyarrow")
        queue.put(
            {
                "status": "SUCCESS",
                "rows": len(df),
                "load_seconds": time.perf_counter() - start,
                "dataframe_mb": df.memory_usage(deep=True).sum() / 1024**2,
            }
        )
    except MemoryError:
        queue.put(
            {
                "status": "MemoryError",
                "rows": None,
                "load_seconds": time.perf_counter() - start,
                "dataframe_mb": None,
            }
        )


def load_full(max_system_mem_pct: float) -> dict:
    """Load the whole raw dataset into one Pandas DataFrame, memory-guarded."""
    on_disk_gb = dir_size_bytes(config.RAW_DIR) / 1024**3
    mem = psutil.virtual_memory()
    logger.info(
        f"[full] Dataset on disk: {on_disk_gb:.2f} GB (compressed) | "
        f"machine RAM: {mem.total / 1024 ** 3:.1f} GB, "
        f"{mem.available / 1024 ** 3:.1f} GB available | "
        f"guard at {max_system_mem_pct}% system memory"
    )
    logger.info("[full] Attempting pd.read_parquet on the full dataset...")

    queue: mp.Queue = mp.Queue()
    child = mp.Process(
        target=_full_load_worker, args=(str(config.RAW_DIR), queue)
    )
    start = time.perf_counter()
    child.start()
    proc = psutil.Process(child.pid)

    trace, peak_mb, outcome = [], 0.0, None
    while child.is_alive():
        try:
            child_mb = rss_mb(proc)
        except psutil.NoSuchProcess:
            break
        sys_pct = psutil.virtual_memory().percent
        elapsed = time.perf_counter() - start
        peak_mb = max(peak_mb, child_mb)
        trace.append(
            {
                "seconds": round(elapsed, 1),
                "pandas_process_mb": round(child_mb),
                "system_memory_pct": sys_pct,
                "machine_ram_mb": round(mem.total / 1024**2),
            }
        )

        if sys_pct >= max_system_mem_pct:
            child.kill()
            logger.error(
                f"[full] ABORTED after {elapsed:.1f}s: "
                f"system memory at {sys_pct}% "
                f"(Pandas process {child_mb:,.0f} MB) - "
                "dataset does not fit in RAM"
            )
            outcome = {
                "status": f"ABORTED_AT_{max_system_mem_pct:.0f}PCT_RAM",
                "rows": None,
                "load_seconds": elapsed,
                "dataframe_mb": None,
            }
            break
        if not queue.empty():
            break
        time.sleep(0.5)

    child.join(timeout=30)
    if outcome is None:
        outcome = (
            queue.get()
            if not queue.empty()
            else {
                "status": f"CRASHED (exit code {child.exitcode})",
                "rows": None,
                "load_seconds": time.perf_counter() - start,
                "dataframe_mb": None,
            }
        )

    with open(
        config.REPORTS_DIR / "pandas_memory_trace.csv",
        "w",
        newline="",
        encoding="utf-8",
    ) as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "seconds",
                "pandas_process_mb",
                "system_memory_pct",
                "machine_ram_mb",
            ],
        )
        writer.writeheader()
        writer.writerows(trace)

    logger.info(
        f"[full] Result: {outcome['status']} | "
        f"{outcome['load_seconds']:.1f}s | "
        f"peak Pandas process memory {peak_mb:,.0f} MB"
    )
    return {
        "test": "pandas_full_dataset",
        "rows": outcome["rows"],
        "status": outcome["status"],
        "load_seconds": round(outcome["load_seconds"], 2),
        "dataframe_mb": (
            round(outcome["dataframe_mb"], 1)
            if outcome["dataframe_mb"]
            else None
        ),
        "peak_process_mb": round(peak_mb, 1),
    }


def save_results(rows: list[dict]) -> None:
    out = config.REPORTS_DIR / "pandas_test_results.csv"
    with open(out, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"Results saved to {out}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pandas limitation demonstration"
    )
    parser.add_argument("--sample-rows", type=int, default=1_000_000)
    parser.add_argument(
        "--max-system-mem-pct",
        type=float,
        default=90.0,
        help="kill the full load when system memory use reaches this %%",
    )
    parser.add_argument(
        "--skip-full", action="store_true", help="only run the sample load"
    )
    args = parser.parse_args()

    ensure_dirs()
    if not any(config.RAW_DIR.rglob("*.parquet")):
        logger.error(
            f"No Parquet files in {config.RAW_DIR}. "
            "Run generate_data.py first."
        )
        return

    results = load_sample(args.sample_rows)
    if not args.skip_full:
        results.append(load_full(args.max_system_mem_pct))
    save_results(results)


if __name__ == "__main__":
    main()
