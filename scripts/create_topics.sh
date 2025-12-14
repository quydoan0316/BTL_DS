#!/bin/bash
# Create all required Kafka topics for the water quality pipeline

KAFKA_CONTAINER=${1:-btl_ds-kafka-1}
BOOTSTRAP_SERVER="localhost:9092"
PARTITIONS=5
REPLICATION=1

echo "Creating Kafka topics..."

# Raw data from producers
docker exec $KAFKA_CONTAINER kafka-topics --create \
    --topic water-quality-raw \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --partitions $PARTITIONS \
    --replication-factor $REPLICATION \
    --if-not-exists

# Enriched data after preprocessing  
docker exec $KAFKA_CONTAINER kafka-topics --create \
    --topic water-quality-enriched \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --partitions $PARTITIONS \
    --replication-factor $REPLICATION \
    --if-not-exists

# ML predictions
docker exec $KAFKA_CONTAINER kafka-topics --create \
    --topic water-quality-prediction \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --partitions 3 \
    --replication-factor $REPLICATION \
    --if-not-exists

# Violation events
docker exec $KAFKA_CONTAINER kafka-topics --create \
    --topic water-quality-violation \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --partitions 3 \
    --replication-factor $REPLICATION \
    --if-not-exists

# Spark metrics/aggregations
docker exec $KAFKA_CONTAINER kafka-topics --create \
    --topic water-quality-metrics \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --partitions 3 \
    --replication-factor $REPLICATION \
    --if-not-exists

echo ""
echo "Listing all topics:"
docker exec $KAFKA_CONTAINER kafka-topics --list --bootstrap-server $BOOTSTRAP_SERVER

echo ""
echo "Done! Topics created successfully."
