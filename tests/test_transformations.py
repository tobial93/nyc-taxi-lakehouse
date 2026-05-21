"""
Unit tests for Silver transformation logic.
Run with: pytest tests/ -v
"""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType, TimestampType
)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("nyc-taxi-lakehouse-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


RAW_SCHEMA = StructType([
    StructField("VendorID", IntegerType()),
    StructField("tpep_pickup_datetime", TimestampType()),
    StructField("tpep_dropoff_datetime", TimestampType()),
    StructField("passenger_count", IntegerType()),
    StructField("trip_distance", DoubleType()),
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    StructField("RatecodeID", IntegerType()),
    StructField("fare_amount", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("tolls_amount", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("payment_type", IntegerType()),
])


def make_row(
    pickup="2024-01-10 08:00:00",
    dropoff="2024-01-10 08:30:00",
    fare=15.0,
    total=18.0,
    distance=3.5,
):
    return (
        1,
        datetime.fromisoformat(pickup),
        datetime.fromisoformat(dropoff),
        2, distance, 132, 236, 1,
        fare, 2.5, 0.0, total, 1,
    )


def apply_silver_filters(df):
    return (
        df
        .dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime", "total_amount"])
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("trip_distance") < 200)
        .filter(F.col("fare_amount") > 0)
        .filter(F.col("total_amount") > 0)
        .filter(F.col("total_amount") < 1000)
        .filter(F.col("tpep_pickup_datetime") < F.col("tpep_dropoff_datetime"))
        .withColumn(
            "trip_duration_minutes",
            (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60,
        )
        .filter(F.col("trip_duration_minutes") > 0)
        .filter(F.col("trip_duration_minutes") < 300)
    )


def test_valid_row_passes(spark):
    df = spark.createDataFrame([make_row()], schema=RAW_SCHEMA)
    result = apply_silver_filters(df)
    assert result.count() == 1


def test_negative_fare_dropped(spark):
    df = spark.createDataFrame([make_row(fare=-5.0, total=-2.0)], schema=RAW_SCHEMA)
    result = apply_silver_filters(df)
    assert result.count() == 0


def test_zero_distance_dropped(spark):
    df = spark.createDataFrame([make_row(distance=0.0)], schema=RAW_SCHEMA)
    result = apply_silver_filters(df)
    assert result.count() == 0


def test_pickup_after_dropoff_dropped(spark):
    df = spark.createDataFrame(
        [make_row(pickup="2024-01-10 09:00:00", dropoff="2024-01-10 08:00:00")],
        schema=RAW_SCHEMA,
    )
    result = apply_silver_filters(df)
    assert result.count() == 0


def test_extreme_fare_dropped(spark):
    df = spark.createDataFrame([make_row(total=5000.0)], schema=RAW_SCHEMA)
    result = apply_silver_filters(df)
    assert result.count() == 0


def test_trip_duration_calculated(spark):
    df = spark.createDataFrame(
        [make_row(pickup="2024-01-10 08:00:00", dropoff="2024-01-10 08:30:00")],
        schema=RAW_SCHEMA,
    )
    result = apply_silver_filters(df)
    duration = result.select("trip_duration_minutes").first()[0]
    assert duration == pytest.approx(30.0)
