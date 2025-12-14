# Hướng Dẫn Demo và Deploy Hệ Thống Water Quality Monitoring

## Mục Lục

1. [Tổng Quan Kiến Trúc](#1-tổng-quan-kiến-trúc)
2. [Chạy Demo Local](#2-chạy-demo-local)
3. [Deploy lên AWS](#3-deploy-lên-aws)
4. [Kết Nối Producer Local với AWS](#4-kết-nối-producer-local-với-aws)
5. [Monitoring và Troubleshooting](#5-monitoring-và-troubleshooting)

---

## 1. Tổng Quan Kiến Trúc

![alt text](image.png)

---

## 2. Chạy Demo Local

### 2.1 Yêu Cầu

- **Docker Desktop** for Windows (với WSL2)
- **Python 3.11+**
- **Git**
- **PostgreSQL** (local hoặc Neon)

### 2.2 Clone và Setup

```bash
git clone <your-repo-url>
cd BTL_DS

cd producers
pip install -r requirements.txt
cd ..
```

### 2.3 Khởi Động Infrastructure

```bash
docker-compose up -d zookeeper kafka grafana
```
Doi run xong
```bash
docker-compose ps
```

### 2.4 Tạo Kafka Topics

```bash
docker exec btl_ds-kafka-1 kafka-topics --create --topic water-quality-raw --bootstrap-server localhost:9092 --partitions 5 --replication-factor 1 --if-not-exists
docker exec btl_ds-kafka-1 kafka-topics --create --topic water-quality-enriched --bootstrap-server localhost:9092 --partitions 5 --replication-factor 1 --if-not-exists
docker exec btl_ds-kafka-1 kafka-topics --create --topic water-quality-prediction --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
docker exec btl_ds-kafka-1 kafka-topics --create --topic water-quality-violation --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
```

### 2.5 Cấu Hình PostgreSQL

Sửa `CONN_STR` trong `docker-compose.yml`:

```yaml
# Local PostgreSQL
- CONN_STR=postgresql://postgres:admin@host.docker.internal:5433/hpb

# Hoặc Neon PostgreSQL
- CONN_STR=postgresql://neondb_owner:password@host.neon.tech/neondb?sslmode=require
```

### 2.6 Khởi Động và Chạy

```bash
docker-compose up -d preprocess-consumer ml-consumer violation-consumer

cd producers
python region_producer.py --region AN --limit 200 --delay 0.05
```

### 2.7 Kiểm Tra

```bash
docker-compose logs --tail=50 violation-consumer
# Mở browser: http://localhost:3000 (admin/admin)
```

---

## 3. Deploy lên AWS (với Neon PostgreSQL)

### 3.1 Yêu Cầu

| Component | Chi tiết |
|-----------|----------|
| **AWS EC2** | t3.medium (2 vCPU, 4GB RAM) |
| **Neon PostgreSQL** | Free tier đủ dùng |
| **Security Group** | Ports: 22, 9092, 3000 |

### 3.2 Tạo Neon Database

1. Đăng ký tại https://neon.tech
2. Create Project → Create Database
3. Lấy Connection String:
   ```
   postgresql://user:password@ep-xxx.region.aws.neon.tech/dbname?sslmode=require
   ```

### 3.3 Tạo EC2 Instance

1. **AMI**: Amazon Linux 2023 / Ubuntu 22.04
2. **Instance Type**: `t3.medium`
3. **Security Group**:
   ```
   Inbound:
   - SSH (22): Your IP
   - Custom TCP (9092): 0.0.0.0/0  # Kafka
   - Custom TCP (3000): 0.0.0.0/0  # Grafana
   ```
4. **Storage**: 30GB gp3

### 3.4 Setup EC2

```bash
# SSH vào EC2
ssh -i your-key.pem ec2-user@<EC2_PUBLIC_IP>

# Chạy script setup (hoặc setup manual)
# Option 1: Dùng script
curl -O https://raw.githubusercontent.com/<repo>/main/scripts/aws_setup.sh
chmod +x aws_setup.sh
./aws_setup.sh

# Option 2: Manual
sudo yum install docker git -y
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker $USER
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Logout và login lại
exit
ssh -i your-key.pem ec2-user@<EC2_PUBLIC_IP>
```

### 3.5 Clone và Deploy

```bash
git clone <your-repo-url>
cd BTL_DS

# Cấu hình EC2 IP và Neon connection
export EC2_IP="<your-ec2-public-ip>"
export NEON_CONN_STR="postgresql://neondb_owner:password@ep-xxx.neon.tech/neondb?sslmode=require"

# Sửa docker-compose.aws.yml
sed -i "s/<EC2_PUBLIC_IP>/$EC2_IP/g" docker-compose.aws.yml

# Deploy
docker-compose -f docker-compose.aws.yml up -d

# Đợi Kafka ready
sleep 30

# Tạo topics
docker exec kafka kafka-topics --create --topic water-quality-raw --bootstrap-server localhost:9092 --partitions 5 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --create --topic water-quality-enriched --bootstrap-server localhost:9092 --partitions 5 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --create --topic water-quality-prediction --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
docker exec kafka kafka-topics --create --topic water-quality-violation --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 --if-not-exists
```

### 3.6 Verify Deployment

```bash
# Kiểm tra services
docker-compose -f docker-compose.aws.yml ps

# Kiểm tra topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Xem logs
docker-compose -f docker-compose.aws.yml logs -f
```

---

## 4. Kết Nối Producer Local với AWS

### 4.1 Từ Windows 10 Local

```powershell
# PowerShell
cd producers
python region_producer.py --region AN --limit 500 --delay 0.05 --kafka <EC2_PUBLIC_IP>:9092
```

```bash
# Git Bash
cd producers
python region_producer.py --region AN --limit 500 --delay 0.05 --kafka <EC2_PUBLIC_IP>:9092

# Chạy tất cả regions
python run_all_producers.py --limit 200 --kafka <EC2_PUBLIC_IP>:9092
```

### 4.2 Verify

```bash
# Trên EC2
docker exec kafka kafka-console-consumer \
  --topic water-quality-raw \
  --bootstrap-server localhost:9092 \
  --max-messages 5

docker-compose -f docker-compose.aws.yml logs --tail=50 violation-consumer
```

---

## 5. Monitoring và Troubleshooting

### 5.1 Grafana với Neon PostgreSQL

1. Truy cập: `http://<EC2_PUBLIC_IP>:3000`
2. Login: `admin` / `admin`
3. Add Data Source → PostgreSQL:
   - **Host**: `ep-xxx.neon.tech:5432`
   - **Database**: `neondb`
   - **User**: `neondb_owner`
   - **Password**: `<your-password>`
   - **SSL Mode**: `require`

### 5.2 Useful Commands

```bash
# Logs
docker-compose -f docker-compose.aws.yml logs -f ml-consumer

# Restart
docker-compose -f docker-compose.aws.yml restart violation-consumer

# Rebuild
docker-compose -f docker-compose.aws.yml up -d --build preprocess-consumer

# Consumer groups
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list
```

### 5.3 Common Issues

| Vấn đề | Giải pháp |
|--------|-----------|
| Producer không kết nối Kafka | Kiểm tra Security Group port 9092 |
| Neon connection timeout | Kiểm tra `sslmode=require` trong CONN_STR |
| Consumer không nhận message | Restart consumer hoặc reset offset |

---

## Quick Reference

### Local Demo
```bash
docker-compose up -d zookeeper kafka grafana
docker-compose up -d preprocess-consumer ml-consumer violation-consumer
cd producers && python region_producer.py --region AN --limit 200
```

### AWS Deploy
```bash
# Trên EC2
docker-compose -f docker-compose.aws.yml up -d

# Từ Windows local
cd producers
python region_producer.py --region AN --kafka <EC2_IP>:9092 --limit 500
```

### Endpoints
- **Grafana**: http://<EC2_IP>:3000
- **Kafka**: <EC2_IP>:9092
- **Neon**: ep-xxx.neon.tech:5432

---

**Ngày tạo**: 2025-12-14  
**BTL Môn Hệ Phân Bố**
