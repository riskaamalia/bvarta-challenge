"""
Data Engineer Take-Home Pipeline
Medallion Architecture: Bronze → Silver → Gold
"""

import argparse
import logging
from datetime import datetime

import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType, DoubleType
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# Config
# ─────────────────────────────────────────────

def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


# Spark
# ─────────────────────────────────────────────

def get_spark(app_name: str = "de-pipeline") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        # Dynamic partition overwrite: only overwrite partitions that appear
        # in the new data — this is the key to idempotency without a full
        # table rewrite, and it lets late-arriving data update only the
        # affected event_date partitions.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


# Schemas
# ─────────────────────────────────────────────

RAW_EVENT_SCHEMA = StructType([
    StructField("event_id",    StringType(),    nullable=True),
    StructField("user_id",     StringType(),    nullable=True),
    StructField("event_type",  StringType(),    nullable=True),
    StructField("event_ts",    StringType(),    nullable=True),  # read as string; validate below
    StructField("value",       StringType(),    nullable=True),  # read as string; cast below
])

USER_SCHEMA = StructType([
    StructField("user_id",     StringType(),    nullable=False),
    StructField("country",     StringType(),    nullable=True),
    StructField("signup_date", StringType(),    nullable=True),  # validate below
])

VALID_EVENT_TYPES = {"CLICK", "VIEW", "PURCHASE"}


# ─────────────────────────────────────────────
# Step 1 – Ingestion
# ─────────────────────────────────────────────

def read_raw_events(spark: SparkSession, path: str) -> DataFrame:
    """Read all raw JSONL event files with an explicit schema.

    Using a strict schema means any field with an unexpected type will land
    as null (Spark's permissive mode), which we surface in the quarantine
    layer instead of crashing the job.
    """
    logger.info("Reading raw events from: %s", path)
    df = (
        spark.read
        .schema(RAW_EVENT_SCHEMA)
        .option("mode", "PERMISSIVE")          # bad rows → nulls, not exceptions
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(path)
    )
    logger.info("Raw event count: %d", df.count())
    return df


def read_users(spark: SparkSession, path: str) -> DataFrame:
    """Read user reference CSV with an explicit schema."""
    logger.info("Reading user reference from: %s", path)
    return spark.read.schema(USER_SCHEMA).option("header", "true").csv(path)


# ─────────────────────────────────────────────
# Step 2 – Bronze (clean + quarantine)
# ─────────────────────────────────────────────

def build_bronze(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Apply data-quality rules and split into clean + quarantine.

    Rules applied in order:
    1. Normalize event_type to UPPER (handles 'click' → 'CLICK')
    2. Cast value to double (handles '30' string change to 30.0, nulls → 0.0 default)
    3. Parse event_ts; null/invalid timestamps → quarantine
    4. Quarantine: null event_id, null/empty user_id, null/invalid event_type,
       null/invalid event_ts
    5. Deduplicate on (event_id, event_ts)
       idempotent because the same event_id + ts always deduplicates to one row.
    """
    df = (
        raw
        # 1. Normalize event_type
        .withColumn("event_type", F.upper(F.trim(F.col("event_type"))))
        # 2. Cast value safely; default null values to 0.0
        .withColumn("value", F.coalesce(F.col("value").cast(DoubleType()), F.lit(0.0)))
        # 3. Try parsing event_ts (ISO-8601); invalid strings → null
        .withColumn("event_ts_parsed", F.try_to_timestamp("event_ts", F.lit("yyyy-MM-dd'T'HH:mm:ss'Z'")))
    )

    # 4. Tag invalid rows
    df = df.withColumn(
        "_reject_reason",
        F.when(F.col("event_id").isNull(), F.lit("null_event_id"))
        .when(F.col("user_id").isNull() | (F.trim(F.col("user_id")) == ""), F.lit("null_or_empty_user_id"))
        .when(F.col("event_type").isNull() | ~F.col("event_type").isin(*VALID_EVENT_TYPES), F.lit("invalid_event_type"))
        .when(F.col("event_ts_parsed").isNull(), F.lit("invalid_or_null_event_ts"))
        .otherwise(F.lit(None).cast(StringType()))
    )

    quarantine = (
        df.filter(F.col("_reject_reason").isNotNull())
        .drop("event_ts_parsed")
        .withColumn("_quarantine_ts", F.current_timestamp())
    )

    clean = (
        df.filter(F.col("_reject_reason").isNull())
        .drop("_reject_reason", "event_ts")               # drop raw string ts
        .withColumnRenamed("event_ts_parsed", "event_ts") # promote parsed ts
    )

    # 5. Deduplicate: same event re-delivered across files should produce one row
    clean = clean.dropDuplicates(["event_id", "event_ts"])

    logger.info("Bronze clean: %d  |  quarantine: %d", clean.count(), quarantine.count())
    return clean, quarantine


# ─────────────────────────────────────────────
# Step 3 – Silver (enrich)
# ─────────────────────────────────────────────

def build_silver(bronze: DataFrame, users_raw: DataFrame) -> DataFrame:
    """Enrich events with user reference data and add derived fields.

    User dimension cleaning:
    - Users with null/empty country → 'UNKNOWN'
    - Users with invalid signup_date → null (no days_since_signup available)

    Join strategy: LEFT JOIN so events with no matching user are kept
    (user_id present in events but absent from reference → UNKNOWN dims).
    """
    users = (
        users_raw
        .withColumn("country", F.coalesce(F.nullif(F.trim(F.col("country")), F.lit("")), F.lit("UNKNOWN")))
        .withColumn("signup_date_parsed", F.try_to_date("signup_date", "yyyy-MM-dd"))
        .drop("signup_date")
        .withColumnRenamed("signup_date_parsed", "signup_date")
        .dropDuplicates(["user_id"])
    )

    silver = (
        bronze
        .join(users, on="user_id", how="left")
        # Fill missing dimension values for unmatched users
        .withColumn("country", F.coalesce(F.col("country"), F.lit("UNKNOWN")))
        # Derived fields
        .withColumn("event_date",       F.to_date("event_ts"))
        .withColumn("is_purchase",      F.col("event_type") == "PURCHASE")
        .withColumn(
            "days_since_signup",
            F.when(
                F.col("signup_date").isNotNull(),
                F.datediff(F.col("event_date"), F.col("signup_date"))
            ).otherwise(F.lit(None).cast(LongType()))
        )
    )

    logger.info("Silver row count: %d", silver.count())
    return silver


# ─────────────────────────────────────────────
# Step 4 – Gold (aggregate)
# ─────────────────────────────────────────────

def build_gold(silver: DataFrame) -> DataFrame:
    """Aggregate to daily x country metrics.

    Partitioned by event_date so that when late data arrives and Silver is
    rewritten for the affected date, re-running Gold overwrites only those
    partitions (dynamic overwrite mode).
    """
    gold = (
        silver
        .groupBy("event_date", "country")
        .agg(
            F.count("*").alias("total_events"),
            F.sum("value").alias("total_value"),
            F.sum(F.col("is_purchase").cast("int")).alias("total_purchases"),
            F.countDistinct("user_id").alias("unique_users"),
        )
        .orderBy("event_date", "country")
    )

    logger.info("Gold row count: %d", gold.count())
    return gold


# ─────────────────────────────────────────────
# Step 5 – Write helpers (idempotent)
# ─────────────────────────────────────────────

def write_parquet(df: DataFrame, path: str, partition_by: list[str] | None = None) -> None:
    """Write a DataFrame to Parquet.

    Using mode='overwrite' with spark.sql.sources.partitionOverwriteMode=dynamic
    means only partitions present in `df` are overwritten — all other existing
    partitions are untouched.  This makes every layer idempotent:

    - Reprocessing the same input produces the same output (dedup in Bronze).
    - Late events trigger a rerun of the affected event_date partition(s) only.
    """
    writer = df.write.mode("overwrite").format("parquet")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)
    logger.info("Written to: %s  (partitions: %s)", path, partition_by)


# Main
# ─────────────────────────────────────────────

def main(config_path: str) -> None:
    config = load_config(config_path)
    paths  = config["paths"]

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    output_root  = paths["output"]
    bronze_path  = f"{output_root}/bronze"
    silver_path  = f"{output_root}/silver"
    gold_path    = f"{output_root}/gold"
    quarantine_path = f"{output_root}/quarantine"

    # 1 ── Ingestion
    raw_events = read_raw_events(spark, paths["raw_events"])
    raw_users  = read_users(spark, paths["users"])

    # 2 ── Bronze
    bronze, quarantine = build_bronze(raw_events)
    bronze_with_date = bronze.withColumn("event_date", F.to_date("event_ts"))
    write_parquet(bronze_with_date, bronze_path, partition_by=["event_date"])
    write_parquet(quarantine, quarantine_path)

    # Re-derive event_date column for bronze partition (added in silver but
    # useful to partition bronze too for late-data rewrites)
    bronze_with_date = bronze.withColumn("event_date", F.to_date("event_ts"))
    write_parquet(bronze_with_date, bronze_path, partition_by=["event_date"])

    # 3 ── Silver
    silver = build_silver(bronze, raw_users)
    write_parquet(silver, silver_path, partition_by=["event_date"])

    # 4 ── Gold
    gold = build_gold(silver)
    write_parquet(gold, gold_path, partition_by=["event_date"])

    logger.info("Pipeline complete ✓")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Medallion pipeline")
    parser.add_argument("--config", required=True, help="Path to pipeline.yaml")
    args = parser.parse_args()
    main(args.config)
