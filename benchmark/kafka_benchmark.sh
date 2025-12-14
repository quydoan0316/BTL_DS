#!/bin/bash
# =========================================
# Kafka Performance Benchmark Script
# =========================================
# This script runs Kafka built-in performance tests
# to measure throughput and latency.

set -e

KAFKA_CONTAINER="kafka"
TOPIC="water-quality-perf-test"
BOOTSTRAP="localhost:9092"
NUM_RECORDS=10000
RECORD_SIZE=500
THROUGHPUT=-1  # unlimited

echo "========================================"
echo "KAFKA PERFORMANCE BENCHMARK"
echo "========================================"
echo "Records: $NUM_RECORDS"
echo "Record size: $RECORD_SIZE bytes"
echo ""

# Create test topic
echo "[1/4] Creating test topic..."
docker exec $KAFKA_CONTAINER kafka-topics --create \
    --topic $TOPIC \
    --bootstrap-server $BOOTSTRAP \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists

# Producer performance test
echo ""
echo "[2/4] Running Producer Performance Test..."
echo "----------------------------------------"
docker exec $KAFKA_CONTAINER kafka-producer-perf-test \
    --topic $TOPIC \
    --num-records $NUM_RECORDS \
    --record-size $RECORD_SIZE \
    --throughput $THROUGHPUT \
    --producer-props bootstrap.servers=$BOOTSTRAP

# Consumer performance test
echo ""
echo "[3/4] Running Consumer Performance Test..."
echo "----------------------------------------"
docker exec $KAFKA_CONTAINER kafka-consumer-perf-test \
    --topic $TOPIC \
    --bootstrap-server $BOOTSTRAP \
    --messages $NUM_RECORDS \
    --threads 1

# Cleanup
echo ""
echo "[4/4] Cleaning up test topic..."
docker exec $KAFKA_CONTAINER kafka-topics --delete \
    --topic $TOPIC \
    --bootstrap-server $BOOTSTRAP

echo ""
echo "========================================"
echo "BENCHMARK COMPLETE"
echo "========================================"
