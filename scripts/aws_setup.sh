#!/bin/bash
# AWS EC2 Quick Setup Script
# Run this on a fresh EC2 instance (Amazon Linux 2023 or Ubuntu 22.04)

set -e

echo "============================================"
echo "Water Quality Monitoring - AWS Setup Script"
echo "============================================"

# Detect OS
if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS=$ID
else
    OS="unknown"
fi

echo "Detected OS: $OS"

# Install Docker
echo ""
echo "[1/5] Installing Docker..."
if [ "$OS" == "amzn" ]; then
    sudo yum update -y
    sudo yum install docker git -y
elif [ "$OS" == "ubuntu" ]; then
    sudo apt update
    sudo apt install -y docker.io git
else
    echo "Unsupported OS. Please install Docker manually."
    exit 1
fi

sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker $USER

# Install Docker Compose
echo ""
echo "[2/5] Installing Docker Compose..."
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Verify installations
echo ""
echo "[3/5] Verifying installations..."
docker --version
docker-compose --version

# Get EC2 Public IP
echo ""
echo "[4/5] Getting EC2 Public IP..."
EC2_IP=$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4)
echo "EC2 Public IP: $EC2_IP"

# Create setup info
echo ""
echo "[5/5] Creating setup info..."
cat > ~/setup_info.txt << EOF
============================================
AWS SETUP COMPLETE
============================================
EC2 Public IP: $EC2_IP
Date: $(date)

NEXT STEPS:
1. Log out and log back in (to apply docker group)
2. Clone your repository:
   git clone <your-repo-url>
   cd BTL_DS

3. Update docker-compose.aws.yml:
   sed -i 's/<EC2_PUBLIC_IP>/$EC2_IP/g' docker-compose.aws.yml

4. Start services:
   docker-compose -f docker-compose.aws.yml up -d

5. Create Kafka topics:
   docker exec kafka kafka-topics --create --topic water-quality-raw --bootstrap-server localhost:9092 --partitions 5 --replication-factor 1 --if-not-exists
   docker exec kafka kafka-topics --create --topic water-quality-enriched --bootstrap-server localhost:9092 --partitions 5 --replication-factor 1 --if-not-exists
   docker exec kafka kafka-topics --create --topic water-quality-prediction --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
   docker exec kafka kafka-topics --create --topic water-quality-violation --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists

ACCESS URLS:
- Grafana: http://$EC2_IP:3000 (admin/admin)
- Kafka: $EC2_IP:9092

FROM LOCAL WINDOWS:
cd producers
python region_producer.py --region AN --limit 500 --kafka $EC2_IP:9092
============================================
EOF

cat ~/setup_info.txt

echo ""
echo "============================================"
echo "SETUP COMPLETE!"
echo "Please log out and log back in, then check ~/setup_info.txt"
echo "============================================"
