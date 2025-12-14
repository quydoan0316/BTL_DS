#!/bin/bash
# Demo script to run the complete water quality pipeline

echo "============================================"
echo "Water Quality Pipeline Demo"
echo "============================================"

# Step 1: Start infrastructure
echo ""
echo "[1/4] Starting infrastructure (Zookeeper, Kafka, Grafana)..."
docker-compose up -d zookeeper kafka grafana
sleep 10

# Step 2: Create topics
echo ""
echo "[2/4] Creating Kafka topics..."
./scripts/create_topics.sh

# Step 3: Start consumers
echo ""
echo "[3/4] Starting consumers..."
docker-compose up -d preprocess-consumer ml-consumer violation-consumer
sleep 5

# Step 4: Run producers
echo ""
echo "[4/4] To run producers, choose one of:"
echo ""
echo "  Option A - Run all producers in Docker:"
echo "    docker-compose --profile producers up -d"
echo ""
echo "  Option B - Run single region locally:"
echo "    cd producers && python region_producer.py --region AN --limit 100"
echo ""
echo "  Option C - Run all regions locally:"
echo "    cd producers && python run_all_producers.py --limit 100"
echo ""
echo "============================================"
echo "Services Status:"
echo "============================================"
docker-compose ps

echo ""
echo "============================================"
echo "Access Points:"
echo "============================================"
echo "  Grafana:     http://localhost:3000 (admin/admin)"
echo "  Kafka:       localhost:9092"
echo "============================================"
