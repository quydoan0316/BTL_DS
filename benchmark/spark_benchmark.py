#!/usr/bin/env python3
"""
Spark Performance Benchmark Script.

Measures:
- Processing latency (time from Kafka to output)
- Throughput (records/second)
- Batch processing time
"""
import os
import time
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, unix_timestamp, lit
)
from pyspark.sql.types import StructType, StructField, StringType

# -------------------------
# Configuration
# -------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "water-quality")
BENCHMARK_DURATION_SEC = 60  # Run for 60 seconds
OUTPUT_FILE = "benchmark_results.json"

# -------------------------
# Spark Session
# -------------------------
spark = SparkSession.builder \
    .appName("SparkBenchmark") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------
# Schema
# -------------------------
schema = StructType([
    StructField("@id", StringType(), True),
    StructField("sample.sampleDateTime", StringType(), True),
    StructField("determinand.label", StringType(), True),
    StructField("result", StringType(), True),
])

# Metrics
metrics = {
    "start_time": None,
    "end_time": None,
    "total_records": 0,
    "total_batches": 0,
    "batch_times": [],
    "latencies": []
}


def process_batch(batch_df, batch_id):
    """Process each micro-batch and collect metrics."""
    global metrics
    
    batch_start = time.time()
    count = batch_df.count()
    batch_end = time.time()
    
    batch_time = batch_end - batch_start
    metrics["total_records"] += count
    metrics["total_batches"] += 1
    metrics["batch_times"].append(batch_time)
    
    if count > 0:
        print(f"[Batch {batch_id}] Records: {count}, Time: {batch_time:.3f}s")


def run_benchmark():
    """Run streaming benchmark for specified duration."""
    global metrics
    
    print("=" * 60)
    print("SPARK STREAMING BENCHMARK")
    print(f"Duration: {BENCHMARK_DURATION_SEC} seconds")
    print("=" * 60)
    
    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse
    parsed = df.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_ts")
    ).select("data.*", "kafka_ts")
    
    # Start streaming
    metrics["start_time"] = time.time()
    
    query = parsed.writeStream \
        .foreachBatch(process_batch) \
        .trigger(processingTime="2 seconds") \
        .start()
    
    # Run for specified duration
    time.sleep(BENCHMARK_DURATION_SEC)
    query.stop()
    
    metrics["end_time"] = time.time()
    
    # Calculate results
    duration = metrics["end_time"] - metrics["start_time"]
    throughput = metrics["total_records"] / duration if duration > 0 else 0
    avg_batch_time = sum(metrics["batch_times"]) / len(metrics["batch_times"]) if metrics["batch_times"] else 0
    
    results = {
        "duration_seconds": round(duration, 2),
        "total_records": metrics["total_records"],
        "total_batches": metrics["total_batches"],
        "throughput_records_per_sec": round(throughput, 2),
        "avg_batch_time_seconds": round(avg_batch_time, 4),
        "min_batch_time": round(min(metrics["batch_times"]), 4) if metrics["batch_times"] else 0,
        "max_batch_time": round(max(metrics["batch_times"]), 4) if metrics["batch_times"] else 0,
    }
    
    # Print results
    print("\n" + "=" * 60)
    print("BENCHMARK RESULTS")
    print("=" * 60)
    for key, value in results.items():
        print(f"  {key}: {value}")
    
    # Save to file
    with open(OUTPUT_FILE, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to: {OUTPUT_FILE}")
    
    return results


if __name__ == "__main__":
    run_benchmark()
