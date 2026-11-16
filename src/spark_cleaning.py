"""
Stages 3-5 - Spark loading, data-quality assessment and cleaning.

Stage 3: create the SparkSession, load ``data/raw/`` and explore it
         (schema, sample rows, record count, partitions, describe()).
Stage 4: quantify data-quality issues and write
         ``outputs/reports/data_quality_report.csv``.
Stage 5: remove invalid rows, fill missing values, standardise strings, fix
         ``total_amount``, add engineered features and write
         ``data/cleaned/`` partitioned by year and month.

Usage:
    python src/spark_cleaning.py
"""

from __future__ import annotations

import csv
import io
from contextlib import redirect_stdout

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

import config
from utils import ensure_dirs, get_logger, get_spark_session, timer

logger = get_logger("SparkCleaning", "spark_cleaning.log")

EMAIL_REGEX = r"^[\w.+-]+@[\w-]+\.[\w.-]+$"
CRITICAL_COLUMNS = ["transaction_id", "customer_id"]
EXPECTED_COLUMNS = [
    "transaction_id",
    "customer_id",
    "customer_name",
    "email",
    "country",
    "city",
    "product_category",
    "product_name",
    "price",
    "quantity",
    "total_amount",
    "payment_method",
    "status",
    "transaction_timestamp",
]


# ---------------------------------------------------------------------------
# Stage 3 - Load & explore
# ---------------------------------------------------------------------------
def load_raw(spark: SparkSession) -> DataFrame:
    """Read the raw Parquet data and drop the generator's partition column."""
    df = spark.read.parquet(str(config.RAW_DIR))
    if "year_month" in df.columns:
        df = df.drop("year_month")

    missing = set(EXPECTED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(
            f"Raw data is missing expected columns: {sorted(missing)}"
        )
    return df


def explore(df: DataFrame) -> int:
    """
    Print schema, a sample, partition count and summary stats, and save the
    same output to ``outputs/reports/exploration_output.txt``. Returns the
    row count.
    """
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        df.printSchema()
        df.show(20, truncate=False)
        total = df.count()
        print(f"Total records: {total:,}")
        print(f"Number of partitions: {df.rdd.getNumPartitions()}")
        print()
        df.describe("price", "quantity", "total_amount").show()

    output = buffer.getvalue()
    print(output)
    out_file = config.REPORTS_DIR / "exploration_output.txt"
    out_file.write_text(output, encoding="utf-8")
    logger.info(f"Total records: {total:,}")
    logger.info(f"Exploration output written to {out_file}")
    return total


# ---------------------------------------------------------------------------
# Stage 4 - Data-quality assessment
# ---------------------------------------------------------------------------
def assess_quality(df: DataFrame, total: int) -> list[dict]:
    """
    Count every data-quality issue in a single pass over the data.

    Returns a list of ``{issue, count, percentage}`` rows.
    """
    start, end = config.START_DATE, config.END_DATE
    checks = {
        # Nulls in every column
        **{f"null_{c}": F.col(c).isNull() for c in EXPECTED_COLUMNS},
        "negative_or_zero_price": F.col("price") <= 0,
        "negative_or_zero_quantity": F.col("quantity") <= 0,
        "total_amount_mismatch": F.abs(
            F.col("total_amount") - F.col("price") * F.col("quantity")
        )
        > 0.01,
        "timestamp_out_of_range": (
            F.col("transaction_timestamp") < F.lit(start)
        )
        | (F.col("transaction_timestamp") > F.lit(end)),
        "invalid_email_format": F.col("email").isNotNull()
        & ~F.col("email").rlike(EMAIL_REGEX),
        "invalid_status": ~F.col("status").isin(*config.STATUSES),
    }

    any_issue = None
    for cond in checks.values():
        any_issue = cond if any_issue is None else (any_issue | cond)

    aggs = [
        F.sum(F.when(cond, 1).otherwise(0)).alias(name)
        for name, cond in checks.items()
    ]
    aggs.append(
        F.sum(F.when(any_issue, 1).otherwise(0)).alias("rows_with_any_issue")
    )
    aggs.append(
        F.countDistinct("transaction_id").alias("distinct_transaction_ids")
    )

    counts = df.agg(*aggs).first().asDict()

    duplicate_ids = total - counts.pop("distinct_transaction_ids")
    rows_with_issue = counts.pop("rows_with_any_issue")

    report = [
        {
            "issue": name,
            "count": int(cnt or 0),
            "percentage": round(100 * (cnt or 0) / total, 3),
        }
        for name, cnt in counts.items()
    ]
    report.append(
        {
            "issue": "duplicate_transaction_id",
            "count": duplicate_ids,
            "percentage": round(100 * duplicate_ids / total, 3),
        }
    )

    # Price outliers via z-score (from the original notebook exploration)
    stats = df.select(
        F.mean("price").alias("mean"), F.stddev("price").alias("std")
    ).first()
    outliers = df.filter(
        F.abs((F.col("price") - stats["mean"]) / stats["std"]) > 3
    ).count()
    report.append(
        {
            "issue": "price_outlier_z_gt_3",
            "count": outliers,
            "percentage": round(100 * outliers / total, 3),
        }
    )

    # Quality score: share of rows with no row-level issue and a unique id
    bad_rows = rows_with_issue + duplicate_ids
    quality_score = round(100 * (1 - bad_rows / total), 2)
    report.append(
        {
            "issue": "QUALITY_SCORE_PCT",
            "count": total - bad_rows,
            "percentage": quality_score,
        }
    )

    for row in report:
        if row["count"]:
            logger.info(
                f"  {row['issue']:<30} {row['count']:>12,}  "
                f"({row['percentage']}%)"
            )
    logger.info(f"Data quality score: {quality_score}%")
    return report


def write_report(rows: list[dict], filename: str) -> None:
    out = config.REPORTS_DIR / filename
    with open(out, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"Report written to {out}")


# ---------------------------------------------------------------------------
# Stage 5 - Cleaning & feature engineering
# ---------------------------------------------------------------------------
def remove_invalid(df: DataFrame) -> DataFrame:
    """
    Drop rows with critical nulls, non-positive price/quantity, bad dates
    and duplicate ids.
    """
    return (
        df.dropna(subset=CRITICAL_COLUMNS + ["transaction_timestamp"])
        .filter((F.col("price") > 0) & (F.col("quantity") > 0))
        .filter(
            F.col("transaction_timestamp").between(
                F.lit(config.START_DATE), F.lit(config.END_DATE)
            )
        )
        .dropDuplicates(["transaction_id"])
    )


def handle_missing(df: DataFrame) -> DataFrame:
    """Fill non-critical missing values with explicit placeholders."""
    return df.fillna(
        {
            "city": "Unknown",
            "email": "unknown@example.com",
            "customer_name": "Unknown",
            "country": "UNKNOWN",
            "product_name": "Unknown",
        }
    )


def standardize(df: DataFrame) -> DataFrame:
    """Trim strings, normalise casing and recompute total_amount."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, F.trim(F.col(field.name)))

    return (
        df.withColumn(
            "payment_method",
            F.upper(F.regexp_replace("payment_method", r"\s+", "_")),
        )
        .withColumn("country", F.upper("country"))
        .withColumn("status", F.upper("status"))
        .withColumn("email", F.lower("email"))
        .withColumn(
            "total_amount", F.round(F.col("price") * F.col("quantity"), 2)
        )
    )


def add_features(df: DataFrame) -> DataFrame:
    """Add date parts, hour, high-value flag and revenue (successful only)."""
    ts = F.col("transaction_timestamp")
    return (
        df.withColumn("year", F.year(ts))
        .withColumn("month", F.month(ts))
        .withColumn("quarter", F.quarter(ts))
        .withColumn("day_of_week", F.date_format(ts, "EEEE"))
        .withColumn("day_of_week_num", F.dayofweek(ts))  # 1 = Sunday
        .withColumn("transaction_date", F.to_date(ts))
        .withColumn("transaction_hour", F.hour(ts))
        .withColumn(
            "is_high_value",
            F.col("total_amount") > config.HIGH_VALUE_THRESHOLD,
        )
        .withColumn(
            "revenue",
            F.when(
                F.col("status") == "SUCCESS", F.col("total_amount")
            ).otherwise(0.0),
        )
    )


def clean(df: DataFrame) -> DataFrame:
    """Full cleaning pipeline (Tasks 5.1 - 5.4)."""
    return add_features(standardize(handle_missing(remove_invalid(df))))


def save_cleaned(df: DataFrame) -> None:
    """Write cleaned data partitioned by year and month (Task 5.5)."""
    (
        df.repartition("year", "month")
        .write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(str(config.CLEANED_DIR))
    )
    logger.info(f"Cleaned data written to {config.CLEANED_DIR}")


def validate_cleaned(spark: SparkSession) -> int:
    """Re-read the cleaned data and assert the cleaning rules hold."""
    cleaned = spark.read.parquet(str(config.CLEANED_DIR))
    total = cleaned.count()
    problems = (
        cleaned.agg(
            F.sum(F.when(F.col("price") <= 0, 1).otherwise(0)).alias(
                "bad_price"
            ),
            F.sum(F.when(F.col("quantity") <= 0, 1).otherwise(0)).alias(
                "bad_quantity"
            ),
            F.sum(
                F.when(
                    F.col("city").isNull() | F.col("email").isNull(), 1
                ).otherwise(0)
            ).alias("nulls"),
            F.sum(
                F.when(
                    F.abs(
                        F.col("total_amount")
                        - F.col("price") * F.col("quantity")
                    )
                    > 0.01,
                    1,
                ).otherwise(0)
            ).alias("mismatch"),
            (F.count("*") - F.countDistinct("transaction_id")).alias(
                "duplicates"
            ),
        )
        .first()
        .asDict()
    )

    for name, value in problems.items():
        level = logger.info if not value else logger.warning
        level(f"Validation {name}: {value}")
    return total


def main() -> None:
    ensure_dirs()
    spark = get_spark_session("TransactionAnalytics-Cleaning")
    timings: dict[str, float] = {}

    try:
        with timer("Stage 3 - load & explore", logger, timings):
            raw = load_raw(spark)
            before = explore(raw)

        with timer("Stage 4 - quality assessment", logger, timings):
            write_report(
                assess_quality(raw, before), "data_quality_report.csv"
            )

        with timer("Stage 5 - clean & save", logger, timings):
            save_cleaned(clean(raw))
            after = validate_cleaned(spark)

        removed = before - after
        logger.info(
            f"Rows before: {before:,} | after: {after:,} | "
            f"removed: {removed:,} ({100 * removed / before:.2f}%)"
        )
        write_report(
            [
                {"metric": "rows_before", "value": before},
                {"metric": "rows_after", "value": after},
                {"metric": "rows_removed", "value": removed},
                *(
                    {"metric": f"seconds_{k}", "value": v}
                    for k, v in timings.items()
                ),
            ],
            "cleaning_summary.csv",
        )

    except Exception:
        logger.error("Spark cleaning failed", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
