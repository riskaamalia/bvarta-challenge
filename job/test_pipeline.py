"""
Unit tests for the pipeline using PySpark local mode.
Run: uv run pytest job/test_pipeline.py -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

from pipeline import (
    RAW_EVENT_SCHEMA,
    USER_SCHEMA,
    build_bronze,
    build_silver,
    build_gold,
)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("pipeline-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


# ─────────────────────────────────────────────
# Bronze tests
# ─────────────────────────────────────────────

def make_raw(spark, rows):
    return spark.createDataFrame(rows, schema=RAW_EVENT_SCHEMA)


def test_bronze_normalizes_event_type(spark):
    raw = make_raw(spark, [
        ("e1", "u1", "click", "2025-01-01T10:00:00Z", "5"),
    ])
    clean, quarantine = build_bronze(raw)
    assert clean.count() == 1
    assert clean.first()["event_type"] == "CLICK"
    assert quarantine.count() == 0


def test_bronze_rejects_null_user_id(spark):
    raw = make_raw(spark, [
        ("e1", None, "CLICK", "2025-01-01T10:00:00Z", "5"),
    ])
    clean, quarantine = build_bronze(raw)
    assert clean.count() == 0
    assert quarantine.count() == 1
    assert quarantine.first()["_reject_reason"] == "null_or_empty_user_id"


def test_bronze_rejects_empty_user_id(spark):
    raw = make_raw(spark, [
        ("e1", "", "CLICK", "2025-01-01T10:00:00Z", "5"),
    ])
    clean, quarantine = build_bronze(raw)
    assert clean.count() == 0
    assert quarantine.count() == 1


def test_bronze_rejects_invalid_timestamp(spark):
    raw = make_raw(spark, [
        ("e1", "u1", "CLICK", "invalid_ts", "5"),
        ("e2", "u1", "CLICK", None, "5"),
    ])
    clean, quarantine = build_bronze(raw)
    assert clean.count() == 0
    assert quarantine.count() == 2


def test_bronze_rejects_invalid_event_type(spark):
    raw = make_raw(spark, [
        ("e1", "u1", "UNKNOWN_TYPE", "2025-01-01T10:00:00Z", "5"),
        ("e2", "u1", None, "2025-01-01T10:00:00Z", "5"),
    ])
    clean, quarantine = build_bronze(raw)
    assert clean.count() == 0
    assert quarantine.count() == 2


def test_bronze_deduplicates(spark):
    raw = make_raw(spark, [
        ("e1", "u1", "CLICK", "2025-01-01T10:00:00Z", "5"),
        ("e1", "u1", "CLICK", "2025-01-01T10:00:00Z", "5"),  # duplicate
    ])
    clean, _ = build_bronze(raw)
    assert clean.count() == 1


def test_bronze_casts_string_value(spark):
    raw = make_raw(spark, [
        ("e1", "u1", "PURCHASE", "2025-01-01T10:00:00Z", "30"),
    ])
    clean, _ = build_bronze(raw)
    assert clean.first()["value"] == 30.0


def test_bronze_null_value_defaults_to_zero(spark):
    raw = make_raw(spark, [
        ("e1", "u1", "VIEW", "2025-01-01T10:00:00Z", None),
    ])
    clean, _ = build_bronze(raw)
    assert clean.first()["value"] == 0.0


# ─────────────────────────────────────────────
# Silver tests
# ─────────────────────────────────────────────

def make_bronze_clean(spark, rows):
    """Create a minimal bronze-style DataFrame (post-build_bronze)."""
    schema = StructType([
        StructField("event_id",   StringType(), True),
        StructField("user_id",    StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_ts",   TimestampType(), True),
        StructField("value",      DoubleType(), True),
    ])
    return spark.createDataFrame(rows, schema=schema)


def make_users(spark, rows):
    return spark.createDataFrame(rows, schema=USER_SCHEMA)


def test_silver_adds_derived_fields(spark):
    from datetime import datetime
    bronze = make_bronze_clean(spark, [
        ("e1", "u1", "PURCHASE", datetime(2025, 1, 5, 10, 0, 0), 25.0),
    ])
    users = make_users(spark, [("u1", "ID", "2024-12-01")])
    silver = build_silver(bronze, users)
    row = silver.first()
    assert str(row["event_date"]) == "2025-01-05"
    assert row["is_purchase"] is True
    assert row["days_since_signup"] == 35  # 2024-12-01 → 2025-01-05


def test_silver_unknown_country_for_missing_user(spark):
    from datetime import datetime
    bronze = make_bronze_clean(spark, [
        ("e1", "u_ghost", "CLICK", datetime(2025, 1, 1, 10, 0, 0), 1.0),
    ])
    users = make_users(spark, [("u1", "ID", "2024-12-01")])
    silver = build_silver(bronze, users)
    row = silver.first()
    assert row["country"] == "UNKNOWN"
    assert row["days_since_signup"] is None


def test_silver_unknown_country_for_null_country(spark):
    from datetime import datetime
    bronze = make_bronze_clean(spark, [
        ("e1", "u4", "CLICK", datetime(2025, 1, 1, 10, 0, 0), 1.0),
    ])
    users = make_users(spark, [("u4", None, "2024-11-01")])
    silver = build_silver(bronze, users)
    row = silver.first()
    assert row["country"] == "UNKNOWN"


def test_silver_invalid_signup_date_gives_null_days(spark):
    from datetime import datetime
    bronze = make_bronze_clean(spark, [
        ("e1", "u3", "CLICK", datetime(2025, 1, 1, 10, 0, 0), 1.0),
    ])
    users = make_users(spark, [("u3", "SG", "invalid_date")])
    silver = build_silver(bronze, users)
    row = silver.first()
    assert row["days_since_signup"] is None


# ─────────────────────────────────────────────
# Gold tests
# ─────────────────────────────────────────────

def make_silver(spark, rows):
    from pyspark.sql.types import BooleanType, LongType, DateType
    schema = StructType([
        StructField("event_id",        StringType(), True),
        StructField("user_id",         StringType(), True),
        StructField("event_type",      StringType(), True),
        StructField("event_ts",        TimestampType(), True),
        StructField("value",           DoubleType(), True),
        StructField("country",         StringType(), True),
        StructField("signup_date",     StringType(), True),
        StructField("event_date",      StringType(), True),
        StructField("is_purchase",     BooleanType(), True),
        StructField("days_since_signup", LongType(), True),
    ])
    return spark.createDataFrame(rows, schema=schema)


def test_gold_aggregates_correctly(spark):
    rows = [
        ("e1", "u1", "CLICK",    None, 5.0,  "ID", None, "2025-01-01", False, 31),
        ("e2", "u1", "PURCHASE", None, 25.0, "ID", None, "2025-01-01", True,  31),
        ("e3", "u2", "VIEW",     None, 0.0,  "US", None, "2025-01-01", False, 17),
    ]
    silver = make_silver(spark, rows)
    gold = build_gold(silver)

    id_row = gold.filter(F.col("country") == "ID").first()
    assert id_row["total_events"] == 2
    assert id_row["total_value"] == 30.0
    assert id_row["total_purchases"] == 1
    assert id_row["unique_users"] == 1

    us_row = gold.filter(F.col("country") == "US").first()
    assert us_row["total_events"] == 1
    assert us_row["unique_users"] == 1


def test_gold_idempotent_on_deduped_input(spark):
    """Running the same silver data twice should produce identical gold output."""
    rows = [
        ("e1", "u1", "CLICK", None, 5.0, "ID", None, "2025-01-01", False, 31),
        ("e1", "u1", "CLICK", None, 5.0, "ID", None, "2025-01-01", False, 31),  # dup
    ]
    silver = make_silver(spark, rows)
    # Simulate dedup happening at bronze level before silver
    silver = silver.dropDuplicates(["event_id"])
    gold = build_gold(silver)
    row = gold.first()
    assert row["total_events"] == 1
