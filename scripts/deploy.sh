#!/bin/bash
# Deploy script for AWS with Neon PostgreSQL
# Run this after aws_setup.sh and re-login

set -e

echo "============================================"
echo "Water Quality - AWS Deployment Script"
echo "(Using Neon PostgreSQL)"
echo "============================================"

# Check arguments
if [ -z "$1" ]; then
    echo ""
    echo "Usage: ./deploy.sh <EC2_PUBLIC_IP> [NEON_CONN_STR]"
    echo ""
    echo "Examples:"
    echo "  ./deploy.sh 54.123.45.67"
    echo "  ./deploy.sh 54.123.45.67 'postgresql://user:pass@host.neon.tech/db?sslmode=require'"
    echo ""
    exit 1
fi

EC2_IP=$1
NEON_CONN="${2:-}"

echo ""
echo "Configuration:"
echo "  EC2 IP: $EC2_IP"
if [ -n "$NEON_CONN" ]; then
    echo "  Neon: Custom connection string provided"
else
    echo "  Neon: Using default from docker-compose.aws.yml"
fi

# Replace placeholder in docker-compose
echo ""
echo "[1/4] Updating docker-compose.aws.yml..."
sed -i "s/<EC2_PUBLIC_IP>/$EC2_IP/g" docker-compose.aws.yml

# Export Neon connection string if provided
if [ -n "$NEON_CONN" ]; then
    export NEON_CONN_STR="$NEON_CONN"
fi

# Start services
echo ""
echo "[2/4] Starting Docker services..."
docker-compose -f docker-compose.aws.yml up -d

# Wait for Kafka to be ready
echo ""
echo "[3/4] Waiting for Kafka to be ready (30s)..."
sleep 30

# Create topics
echo ""
echo "[4/4] Creating Kafka topics..."
docker exec kafka kafka-topics --create --topic water-quality-raw --bootstrap-server localhost:9092 --partitions 5 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --create --topic water-quality-enriched --bootstrap-server localhost:9092 --partitions 5 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --create --topic water-quality-prediction --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --create --topic water-quality-violation --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --create --topic water-quality-metrics --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists

echo ""
echo "============================================"
echo "DEPLOYMENT COMPLETE!"
echo "============================================"
echo ""
echo "Services status:"
docker-compose -f docker-compose.aws.yml ps
echo ""
echo "Kafka topics:"
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
echo ""
echo "============================================"
echo "ACCESS URLS:"
echo "  Grafana: http://$EC2_IP:3000 (admin/admin)"
echo "  Kafka:   $EC2_IP:9092"
echo ""
echo "GRAFANA POSTGRESQL CONFIG (Neon):"
echo "  Host: ep-silent-water-a1q1b7so-pooler.ap-southeast-1.aws.neon.tech:5432"
echo "  Database: neondb"
echo "  User: neondb_owner"
echo "  SSL Mode: require"
echo ""
echo "FROM LOCAL WINDOWS, run:"
echo "  cd producers"
echo "  python region_producer.py --region AN --limit 500 --kafka $EC2_IP:9092"
echo "============================================"
