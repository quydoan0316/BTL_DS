#!/usr/bin/env python3
"""
Spark Structured Streaming Consumer for Water Quality Data.

This job reads from Kafka, performs:
- Filtering & validation
- Window aggregation (hourly, by region)
- Writes to PostgreSQL table `water_quality_spark`

Runs in parallel with consumer.py (different consumer group).
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window,
    avg, min, max, count, sum as spark_sum,
    year, month, hour, dayofweek,
    when, lit, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

# -------------------------
# Configuration
# -------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "water-quality")
POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://localhost:5433/hpb_spark")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoint")

# Violation limits
LIMITS = {
    "BOD ATU": 20.0,
    "Ammonia (N)": 0.6,
    "Dissolved Oxygen": 5.0,  # minimum
    "pH": (6.0, 9.0),  # range
}

# -------------------------
# Spark Session
# -------------------------
spark = SparkSession.builder \
    .appName("WaterQualitySparkStreaming") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.postgresql:postgresql:42.7.1") \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------
# Kafka Message Schema
# -------------------------
kafka_schema = StructType([
    StructField("@id", StringType(), True),
    StructField("sample.sampleDateTime", StringType(), True),
    StructField("determinand.label", StringType(), True),
    StructField("result", StringType(), True),
    StructField("determinand.unit.label", StringType(), True),
    StructField("sample.samplingPoint.label", StringType(), True),
    StructField("sample.samplingPoint.easting", StringType(), True),
    StructField("sample.samplingPoint.northing", StringType(), True),
])

# -------------------------
# Read from Kafka
# -------------------------
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("kafka.group.id", "spark-batch-group") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON value
parsed_df = raw_df.select(
    from_json(col("value").cast("string"), kafka_schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select("data.*", "kafka_timestamp")

# -------------------------
# Data Preprocessing
# -------------------------
processed_df = (
    parsed_df
    # Filter out null values
    .filter(col("result").isNotNull())
    .filter(col("determinand.label").isNotNull())
    
    # Cast and transform
    .withColumn("id", col("@id"))
    .withColumn("sampling_point", col("sample.samplingPoint.label"))
    .withColumn("sample_time", to_timestamp(col("sample.sampleDateTime")))
    .withColumn("determinand", col("determinand.label"))
    .withColumn("value", col("result").cast(DoubleType()))
    .withColumn("unit", col("determinand.unit.label"))
    .withColumn("easting", col("sample.samplingPoint.easting").cast(DoubleType()))
    .withColumn("northing", col("sample.samplingPoint.northing").cast(DoubleType()))
    
    # Feature engineering
    .withColumn("hour", hour(col("sample_time")))
    .withColumn("day_of_week", dayofweek(col("sample_time")))
    .withColumn("month", month(col("sample_time")))
    
    # Violation detection
    .withColumn(
        "violation",
        when(
            (col("determinand") == "BOD ATU") & (col("value") > LIMITS["BOD ATU"]),
            True
        ).when(
            (col("determinand") == "Ammonia (N)") & (col("value") > LIMITS["Ammonia (N)"]),
            True
        ).when(
            (col("determinand") == "Dissolved Oxygen") & (col("value") < LIMITS["Dissolved Oxygen"]),
            True
        ).otherwise(False)
    )
    
    # Add processing timestamp
    .withColumn("processed_at", current_timestamp())
    
    # Select final columns
    .select(
        "id", "sampling_point", "sample_time", "determinand",
        "value", "unit", "easting", "northing",
        "hour", "day_of_week", "month", "violation", "processed_at"
    )
)

# -------------------------
# Window Aggregation (Hourly by Determinand)
# -------------------------
aggregated_df = (
    processed_df
    .withWatermark("sample_time", "1 hour")
    .groupBy(
        window(col("sample_time"), "1 hour"),
        col("determinand")
    )
    .agg(
        count("*").alias("record_count"),
        avg("value").alias("avg_value"),
        min("value").alias("min_value"),
        max("value").alias("max_value"),
        spark_sum(when(col("violation"), 1).otherwise(0)).alias("violation_count")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "determinand",
        "record_count",
        "avg_value",
        "min_value",
        "max_value",
        "violation_count"
    )
)

# -------------------------
# Write to PostgreSQL (Batch)
# -------------------------
def write_to_postgres(batch_df, batch_id):
    """Write each micro-batch to PostgreSQL."""
    if batch_df.count() == 0:
        return
    
    batch_df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", "water_quality_spark") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    
    print(f"[Spark] Batch {batch_id}: wrote {batch_df.count()} records")


def write_aggregations_to_postgres(batch_df, batch_id):
    """Write aggregated data to PostgreSQL."""
    if batch_df.count() == 0:
        return
    
    batch_df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", "water_quality_hourly_agg") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    
    print(f"[Spark] Aggregation Batch {batch_id}: wrote {batch_df.count()} records")


# -------------------------
# Start Streaming Queries
# -------------------------
# Query 1: Write processed records
query_records = processed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/records") \
    .start()

# Query 2: Write hourly aggregations
query_agg = aggregated_df.writeStream \
    .foreachBatch(write_aggregations_to_postgres) \
    .outputMode("update") \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/aggregations") \
    .start()

print("=" * 60)
print("Spark Structured Streaming started!")
print(f"  Kafka: {KAFKA_BOOTSTRAP} / topic: {KAFKA_TOPIC}")
print(f"  PostgreSQL: {POSTGRES_URL}")
print("  Tables: water_quality_spark, water_quality_hourly_agg")
print("=" * 60)

# Wait for termination
spark.streams.awaitAnyTermination()
