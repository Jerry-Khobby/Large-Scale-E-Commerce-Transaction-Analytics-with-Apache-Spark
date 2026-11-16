"""
Stage 6 - Business analytics on the cleaned dataset.

Runs five query groups (revenue, product, customer, time-based and window
functions), demonstrates Spark SQL alongside the DataFrame API, writes every
result to ``data/analytics/<query_name>/`` as CSV and records the execution
time of each query group in ``outputs/reports/analytics_timings.csv``.

Usage:
    python src/spark_analytics.py
"""

from __future__ import annotations

import csv

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

import config
from utils import ensure_dirs, get_logger, get_spark_session, save_csv, timer

logger = get_logger("SparkAnalytics", "spark_analytics.log")


def publish(df: DataFrame, name: str, n: int = 20) -> DataFrame:
    """Show a result, save it to data/analytics/<name>/ and return it."""
    df.show(n, truncate=False)
    save_csv(df, name)
    logger.info(f"Saved {name}")
    return df


def successful(df: DataFrame) -> DataFrame:
    return df.filter(F.col("status") == "SUCCESS")


# ---------------------------------------------------------------------------
# Query group 1 - Revenue
# ---------------------------------------------------------------------------
def revenue_analysis(df: DataFrame) -> None:
    ok = successful(df)

    publish(ok.groupBy("country")
            .agg(F.sum("total_amount").alias("total_revenue"),
                 F.count("transaction_id").alias("transaction_count"),
                 F.avg("total_amount").alias("avg_order_value"))
            .orderBy(F.desc("total_revenue")).limit(20),
            "revenue_by_country")

    publish(ok.withColumn("year_month", F.date_format("transaction_timestamp", "yyyy-MM"))
            .groupBy("year_month")
            .agg(F.sum("total_amount").alias("monthly_revenue"),
                 F.count("transaction_id").alias("transaction_count"),
                 F.avg("total_amount").alias("avg_order_value"))
            .orderBy("year_month"),
            "monthly_revenue_trend", n=36)

    # Percentage of total computed with a window over the grouped result
    # (avoids a separate collect() inside the aggregation).
    publish(ok.groupBy("payment_method")
            .agg(F.sum("total_amount").alias("total_revenue"),
                 F.count("transaction_id").alias("transaction_count"),
                 F.avg("total_amount").alias("avg_order_value"))
            .withColumn("revenue_percentage",
                        F.round(100 * F.col("total_revenue")
                                / F.sum("total_revenue").over(Window.partitionBy()), 2))
            .orderBy(F.desc("total_revenue")),
            "revenue_by_payment")

    publish(ok.groupBy("product_category")
            .agg(F.sum("total_amount").alias("total_revenue"),
                 F.count("transaction_id").alias("transactions"),
                 F.sum("quantity").alias("units_sold"),
                 F.avg("price").alias("avg_price"),
                 F.avg("total_amount").alias("avg_order_value"))
            .orderBy(F.desc("total_revenue")),
            "revenue_by_category")


# ---------------------------------------------------------------------------
# Query group 2 - Products
# ---------------------------------------------------------------------------
def product_analysis(df: DataFrame) -> None:
    product_stats = (
        successful(df).groupBy("product_name", "product_category")
        .agg(F.sum("total_amount").alias("total_revenue"),
             F.sum("quantity").alias("units_sold"),
             F.count("transaction_id").alias("order_count"),
             F.avg("price").alias("avg_price"))
    )

    publish(product_stats.orderBy(F.desc("total_revenue")).limit(20), "top_products_revenue")
    publish(product_stats.orderBy(F.desc("units_sold")).limit(20), "top_products_quantity")

    publish(df.groupBy("product_category")
            .agg(F.avg("price").alias("avg_price"),
                 F.min("price").alias("min_price"),
                 F.max("price").alias("max_price"),
                 F.countDistinct("product_name").alias("distinct_products"))
            .orderBy(F.desc("avg_price")),
            "avg_price_by_category")

    publish(df.groupBy("product_name", "product_category")
            .agg(F.count("transaction_id").alias("total_transactions"),
                 F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_count"))
            .withColumn("failure_rate_pct",
                        F.round(100 * F.col("failed_count") / F.col("total_transactions"), 2))
            .filter(F.col("total_transactions") >= 100)
            .orderBy(F.desc("failure_rate_pct")).limit(20),
            "product_failure_rates")


# ---------------------------------------------------------------------------
# Query group 3 - Customers
# ---------------------------------------------------------------------------
def customer_analysis(df: DataFrame) -> None:
    customer_spend = (
        successful(df).groupBy("customer_id", "customer_name", "country")
        .agg(F.sum("total_amount").alias("total_spent"),
             F.count("transaction_id").alias("order_count"),
             F.avg("total_amount").alias("avg_order_value"),
             F.sum("quantity").alias("total_items"))
    )

    publish(customer_spend.orderBy(F.desc("total_spent")).limit(50), "top_customers", n=50)

    publish(customer_spend.select(
                F.count("customer_id").alias("customers"),
                F.avg("avg_order_value").alias("mean_avg_order_value"),
                F.expr("percentile_approx(avg_order_value, 0.5)").alias("median_avg_order_value"),
                F.min("avg_order_value").alias("min_avg_order_value"),
                F.max("avg_order_value").alias("max_avg_order_value")),
            "avg_order_value_per_customer")

    publish(df.groupBy("customer_id")
            .agg(F.count("transaction_id").alias("purchase_count"))
            .groupBy("purchase_count")
            .agg(F.count("customer_id").alias("customer_count"))
            .orderBy("purchase_count"),
            "purchase_frequency_distribution", n=50)

    publish(df.groupBy("customer_id", "customer_name")
            .agg(F.count("*").alias("transactions"),
                 F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("successes"))
            .filter(F.col("successes") == 0)
            .drop("successes")
            .orderBy(F.desc("transactions")),
            "customers_only_failed")


# ---------------------------------------------------------------------------
# Query group 4 - Time-based (Spark SQL)
# ---------------------------------------------------------------------------
def time_analysis(spark: SparkSession) -> None:
    """Time-based queries written in Spark SQL against the `transactions` view."""
    publish(spark.sql("""
        SELECT transaction_hour, COUNT(*) AS transactions, SUM(revenue) AS revenue
        FROM transactions
        GROUP BY transaction_hour
        ORDER BY transaction_hour
    """), "transactions_by_hour", n=24)

    publish(spark.sql("""
        SELECT day_of_week_num, day_of_week, COUNT(*) AS transactions, SUM(revenue) AS revenue
        FROM transactions
        GROUP BY day_of_week_num, day_of_week
        ORDER BY day_of_week_num
    """), "transactions_by_day_of_week")

    publish(spark.sql("""
        SELECT year, month, COUNT(*) AS transactions, SUM(revenue) AS revenue
        FROM transactions
        GROUP BY year, month
        ORDER BY transactions DESC
        LIMIT 10
    """), "peak_months")

    publish(spark.sql("""
        SELECT year, quarter,
               COUNT(*) AS transactions,
               ROUND(100 * AVG(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END), 2)
                   AS success_rate_pct
        FROM transactions
        GROUP BY year, quarter
        ORDER BY year, quarter
    """), "success_rate_by_quarter")

    # Inputs for the heatmap and order-value histogram visualisations
    publish(spark.sql("""
        SELECT day_of_week_num, day_of_week, transaction_hour, COUNT(*) AS transactions
        FROM transactions
        GROUP BY day_of_week_num, day_of_week, transaction_hour
    """), "hour_day_heatmap", n=5)

    publish(spark.sql("""
        SELECT FLOOR(total_amount / 50) * 50 AS order_value_bucket, COUNT(*) AS orders
        FROM transactions
        WHERE status = 'SUCCESS'
        GROUP BY FLOOR(total_amount / 50) * 50
        ORDER BY order_value_bucket
    """), "order_value_distribution", n=50)


# ---------------------------------------------------------------------------
# Query group 5 - Window functions (DataFrame API + SQL comparison)
# ---------------------------------------------------------------------------
def window_analysis(spark: SparkSession, df: DataFrame) -> None:
    product_revenue = (
        successful(df).groupBy("product_category", "product_name")
        .agg(F.sum("total_amount").alias("total_revenue"))
    )
    rank_window = Window.partitionBy("product_category").orderBy(F.desc("total_revenue"))
    publish(product_revenue.withColumn("rank_in_category", F.dense_rank().over(rank_window))
            .filter(F.col("rank_in_category") <= 10)
            .orderBy("product_category", "rank_in_category"),
            "product_rank_in_category", n=30)

    # Running total - DataFrame API
    monthly = (
        successful(df)
        .withColumn("year_month", F.date_format("transaction_timestamp", "yyyy-MM"))
        .groupBy("year_month").agg(F.sum("total_amount").alias("monthly_revenue"))
    )
    running = Window.orderBy("year_month").rowsBetween(Window.unboundedPreceding, 0)
    publish(monthly.withColumn("running_total", F.sum("monthly_revenue").over(running))
            .orderBy("year_month"),
            "running_revenue_by_month", n=36)

    # Same running total in Spark SQL, to compare the two approaches
    spark.sql("""
        WITH monthly AS (
            SELECT date_format(transaction_timestamp, 'yyyy-MM') AS year_month,
                   SUM(total_amount) AS monthly_revenue
            FROM transactions WHERE status = 'SUCCESS'
            GROUP BY 1
        )
        SELECT year_month, monthly_revenue,
               SUM(monthly_revenue) OVER (ORDER BY year_month) AS running_total
        FROM monthly ORDER BY year_month
    """).show(5)

    # Customers spending above the average customer
    publish(spark.sql("""
        WITH spend AS (
            SELECT customer_id, customer_name, SUM(revenue) AS total_spent
            FROM transactions
            GROUP BY customer_id, customer_name
        ),
        with_avg AS (
            SELECT *, ROUND(AVG(total_spent) OVER (), 2) AS avg_customer_spend
            FROM spend
        )
        SELECT customer_id, customer_name, total_spent, avg_customer_spend
        FROM with_avg
        WHERE total_spent > avg_customer_spend
        ORDER BY total_spent DESC
    """), "above_average_customers")


def write_timings(timings: dict[str, float]) -> None:
    out = config.REPORTS_DIR / "analytics_timings.csv"
    with open(out, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["query_group", "seconds"])
        writer.writerows(timings.items())
    logger.info(f"Timings written to {out}")


def main() -> None:
    ensure_dirs()
    spark = get_spark_session("TransactionAnalytics-Analytics")
    timings: dict[str, float] = {}

    try:
        df = spark.read.parquet(str(config.CLEANED_DIR))
        df.createOrReplaceTempView("transactions")
        logger.info(f"Loaded cleaned data: {df.count():,} rows")

        with timer("1_revenue", logger, timings):
            revenue_analysis(df)
        with timer("2_product", logger, timings):
            product_analysis(df)
        with timer("3_customer", logger, timings):
            customer_analysis(df)
        with timer("4_time_sql", logger, timings):
            time_analysis(spark)
        with timer("5_window", logger, timings):
            window_analysis(spark, df)

        write_timings(timings)
    except Exception:
        logger.error("Spark analytics failed", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
