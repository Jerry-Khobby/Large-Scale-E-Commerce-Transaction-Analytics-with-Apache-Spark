"""
Stage 7 - Performance optimisation experiments.

7.1 Baseline : a window-function + join workload with caching, broadcast joins
               and Adaptive Query Execution (AQE) disabled.
7.2 Optimised: same workload with column pruning, ``cache()``, a broadcast join
               hint, AQE and fewer shuffle partitions.
7.3 Compare  : improvement percentage; physical plans saved to
               ``outputs/reports/explain_*.txt``.
7.4 Parallelism: the baseline query on ``local[1]`` vs ``local[4]``.

Results go to ``outputs/reports/performance_results.csv``.

Usage:
    python src/spark_optimization.py [--skip-parallelism]
"""

from __future__ import annotations

import argparse
import csv
import time

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

import config
from utils import ensure_dirs, get_logger, get_spark_session
from utils import save_explain as write_explain

logger = get_logger("SparkOptimization", "spark_optimization.log")

NEEDED_COLUMNS = ["product_category", "product_name", "country",
                  "status", "total_amount", "transaction_timestamp"]


def build_query(df: DataFrame, broadcast: bool) -> DataFrame:
    """
    Products earning more than their category average, ranked within category.

    Involves two aggregations, a join and a window function - i.e. several
    shuffles, which makes optimisation effects visible.
    """
    product_rev = (
        df.filter(F.col("status") == "SUCCESS")
        .groupBy("product_category", "product_name")
        .agg(F.sum("total_amount").alias("revenue"))
    )
    category_avg = product_rev.groupBy("product_category").agg(F.avg("revenue").alias("cat_avg"))
    if broadcast:
        category_avg = F.broadcast(category_avg)

    rank_window = Window.partitionBy("product_category").orderBy(F.desc("revenue"))
    return (
        product_rev.join(category_avg, "product_category")
        .filter(F.col("revenue") > F.col("cat_avg"))
        .withColumn("rank", F.dense_rank().over(rank_window))
    )


def run_workload(df: DataFrame, broadcast: bool) -> float:
    """Run several actions that reuse ``df``; return elapsed seconds."""
    start = time.perf_counter()
    query = build_query(df, broadcast)
    query.count()
    query.orderBy(F.desc("revenue")).limit(20).collect()
    df.groupBy("country").agg(F.sum("total_amount")).collect()
    df.groupBy(F.month("transaction_timestamp")).count().collect()
    return time.perf_counter() - start


def save_explain(df: DataFrame, name: str) -> None:
    """Save the formatted execution plan of ``df`` to outputs/reports/."""
    logger.info(f"Execution plan saved to {write_explain(df, name)}")


def baseline_vs_optimized(spark: SparkSession) -> list[dict]:
    path = str(config.CLEANED_DIR)

    # --- 7.1 Baseline ---------------------------------------------------------
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.shuffle.partitions", config.SPARK_SHUFFLE_PARTITIONS)

    base_df = spark.read.parquet(path)
    save_explain(build_query(base_df, broadcast=False), "baseline")
    baseline = run_workload(base_df, broadcast=False)
    logger.info(f"Baseline workload: {baseline:.2f}s")

    # --- 7.2 Optimised --------------------------------------------------------
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
    spark.conf.set("spark.sql.shuffle.partitions", str(spark.sparkContext.defaultParallelism * 2))

    opt_df = spark.read.parquet(path).select(*NEEDED_COLUMNS).cache()
    cache_start = time.perf_counter()
    opt_df.count()  # materialise the cache
    cache_seconds = time.perf_counter() - cache_start
    logger.info(f"Cache materialised in {cache_seconds:.2f}s")

    save_explain(build_query(opt_df, broadcast=True), "optimized")
    optimized = run_workload(opt_df, broadcast=True)
    logger.info(f"Optimised workload: {optimized:.2f}s (+{cache_seconds:.2f}s one-off caching)")
    opt_df.unpersist()

    improvement = 100 * (baseline - optimized) / baseline
    logger.info(f"Improvement: {improvement:.1f}%")

    return [
        {"experiment": "baseline (no cache, no broadcast, AQE off)", "seconds": round(baseline, 2)},
        {"experiment": "cache materialisation (one-off)", "seconds": round(cache_seconds, 2)},
        {"experiment": "optimized (cache + broadcast + AQE + pruning)",
         "seconds": round(optimized, 2)},
        {"experiment": "improvement_pct", "seconds": round(improvement, 1)},
    ]


def parallelism_experiment(masters: list[str]) -> list[dict]:
    """7.4 - run the same query with different numbers of local threads."""
    rows = []
    for master in masters:
        spark = get_spark_session(f"Parallelism-{master}", master=master)
        try:
            df = spark.read.parquet(str(config.CLEANED_DIR))
            start = time.perf_counter()
            build_query(df, broadcast=False).count()
            elapsed = time.perf_counter() - start
            logger.info(f"{master}: {elapsed:.2f}s")
            rows.append({"experiment": f"parallelism {master}", "seconds": round(elapsed, 2)})
        finally:
            spark.stop()
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Spark performance experiments")
    parser.add_argument("--skip-parallelism", action="store_true")
    args = parser.parse_args()

    ensure_dirs()
    results: list[dict] = []

    spark = get_spark_session("TransactionAnalytics-Optimization")
    try:
        results += baseline_vs_optimized(spark)
    except Exception:
        logger.error("Optimisation experiment failed", exc_info=True)
        raise
    finally:
        spark.stop()

    if not args.skip_parallelism:
        results += parallelism_experiment(["local[1]", "local[4]"])

    out = config.REPORTS_DIR / "performance_results.csv"
    with open(out, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["experiment", "seconds"])
        writer.writeheader()
        writer.writerows(results)
    logger.info(f"Performance results written to {out}")


if __name__ == "__main__":
    main()
