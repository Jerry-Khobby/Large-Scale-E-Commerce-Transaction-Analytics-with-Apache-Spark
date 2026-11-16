"""
Shared helpers: logging, Spark session creation, timing and result export.
"""

from __future__ import annotations

import io
import logging
import time
from contextlib import contextmanager, redirect_stdout
from pathlib import Path
from typing import Iterator

import config


def ensure_dirs() -> None:
    """Create every project output directory if it does not exist yet."""
    for directory in config.ALL_DIRS:
        directory.mkdir(parents=True, exist_ok=True)


def get_logger(name: str, log_file: str) -> logging.Logger:
    """
    Return a logger that writes to both the console and ``logs/<log_file>``.

    Calling it twice with the same name does not duplicate handlers.
    """
    config.LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

        file_handler = logging.FileHandler(config.LOG_DIR / log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        logger.propagate = False

    return logger


def get_spark_session(app_name: str, master: str = config.SPARK_MASTER, **extra_conf):
    """
    Build (or reuse) a SparkSession with the project's default configuration.

    Extra Spark settings can be passed as keyword arguments, using underscores
    instead of dots, e.g. ``spark_sql_adaptive_enabled="false"``.
    """
    from pyspark.sql import SparkSession  # imported lazily: not every script needs Spark

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", config.SPARK_SHUFFLE_PARTITIONS)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
    )
    for key, value in extra_conf.items():
        builder = builder.config(key.replace("_", "."), value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


@contextmanager
def timer(label: str, logger: logging.Logger, results: dict | None = None) -> Iterator[None]:
    """Log how long the wrapped block took; optionally store it in ``results[label]``."""
    start = time.perf_counter()
    yield
    elapsed = time.perf_counter() - start
    logger.info(f"{label} finished in {elapsed:.2f}s")
    if results is not None:
        results[label] = round(elapsed, 2)


def save_csv(df, name: str, base_dir: Path = config.ANALYTICS_DIR) -> Path:
    """Write a (small, aggregated) Spark DataFrame to ``base_dir/<name>/`` as one CSV file."""
    out_path = base_dir / name
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(str(out_path))
    )
    return out_path


def save_explain(df, name: str) -> Path:
    """Write ``df.explain('formatted')`` to ``outputs/reports/explain_<name>.txt``."""
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        df.explain(mode="formatted")
    out = config.REPORTS_DIR / f"explain_{name}.txt"
    out.write_text(buffer.getvalue(), encoding="utf-8")
    return out


def dir_size_bytes(path: Path) -> int:
    """Total size of all files under ``path``."""
    return sum(f.stat().st_size for f in Path(path).rglob("*") if f.is_file())
