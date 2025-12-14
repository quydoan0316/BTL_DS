# Water Quality Monitoring System - Multi-Producer & Multi-Consumer Architecture

Hệ thống giám sát chất lượng nước theo kiến trúc **event-driven phân tán** với Apache Kafka.

## Kiến trúc

```
<img width="2277" height="633" alt="image" src="https://github.com/user-attachments/assets/8a543a3d-3845-4929-9a20-7cae1aea5d12" />

```

## Cấu trúc thư mục

```
BTL_DS/
├── producers/                    # Multi-producer IoT simulation
│   ├── base_producer.py          # Base class
│   ├── region_producer.py        # Region-specific producer
│   ├── run_all_producers.py      # Parallel orchestrator
│   ├── Dockerfile
│   └── requirements.txt
│
├── consumers/                    # Multi-consumer processing
│   ├── preprocess_consumer.py    # Consumer 1: Data enrichment
│   ├── ml_consumer.py            # Consumer 2: ML prediction
│   ├── violation_consumer.py     # Consumer 3: Rule-based alerts
│   ├── Dockerfile
│   ├── requirements.txt
│   └── utils/
│       ├── config.py             # Centralized config
│       ├── coordinate_utils.py   # OSGB36 → WGS84
│       └── db_writer.py          # Thread-safe DB writer
│
├── spark/                        # Spark analytics
│   ├── spark_streaming.py        # Real-time aggregation
│   └── spark_batch_ml.py         # Batch ML training
│
├── docs/
│   └── DEPLOYMENT_GUIDE.md      # Chi tiết deploy local và AWS
│
├── scripts/
│   ├── create_topics.sh          # Create Kafka topics
│   ├── run_demo.sh               # Demo runner
│   ├── aws_setup.sh              # AWS EC2 setup script
│   └── deploy.sh                 # One-command AWS deploy
│
├── grafana/                      # Dashboard provisioning
├── docker-compose.yml            # Local development
├── docker-compose.aws.yml        # AWS production
├── .env.example                  # Environment template
└── 2025-C.csv                    # Data source
```

## Tài Liệu

| Tài liệu | Mô tả |
|----------|-------|
| [DEPLOYMENT_GUIDE.md](docs/DEPLOYMENT_GUIDE.md) | Hướng dẫn chi tiết chạy demo local và deploy AWS |
| [docker-compose.aws.yml](docker-compose.aws.yml) | Docker Compose cho AWS với PostgreSQL |

## Quick Start

### 1. Khởi động infrastructure

```bash
# Start Kafka và Grafana
docker-compose up -d zookeeper kafka grafana

# Đợi Kafka ready (~10s)
sleep 10

# Tạo topics
bash scripts/create_topics.sh
```

### 2. Khởi động consumers

```bash
docker-compose up -d preprocess-consumer ml-consumer violation-consumer
```

### 3. Chạy producers

**Option A: Chạy local (recommended cho demo)**
```bash
cd producers
pip install -r requirements.txt

# Chạy 1 region
python region_producer.py --region AN --limit 100

# Hoặc chạy tất cả regions
python run_all_producers.py --limit 100
```

**Option B: Chạy trong Docker**
```bash
docker-compose --profile producers up -d
```

### 4. Xem kết quả

- **Grafana**: http://localhost:3000 (admin/admin)
- **Kafka topics**: `docker exec btl_ds-kafka-1 kafka-topics --list --bootstrap-server localhost:9092`

## Kafka Topics

| Topic | Partitions | Mô tả |
|-------|------------|-------|
| `water-quality-raw` | 5 | Dữ liệu gốc từ producers |
| `water-quality-enriched` | 5 | Đã chuẩn hóa, có tọa độ |
| `water-quality-prediction` | 3 | Kết quả ML dự báo |
| `water-quality-violation` | 3 | Cảnh báo vi phạm |
| `water-quality-metrics` | 3 | Aggregations từ Spark |

## Cấu hình

Copy `.env.example` thành `.env` và điều chỉnh:

```bash
cp .env.example .env
```

Các biến quan trọng:
- `CONN_STR`: Connection string PostgreSQL
- `KAFKA_BOOTSTRAP`: Kafka server address
- `TRAIN_THRESHOLD`: Số samples tối thiểu để train RandomForest

## Kiểm tra

```bash
# Xem messages trong topic
docker exec btl_ds-kafka-1 kafka-console-consumer \
    --topic water-quality-enriched \
    --bootstrap-server localhost:9092 \
    --from-beginning --max-messages 5

# Xem logs consumer
docker-compose logs -f ml-consumer
```

## UK Regulatory Limits

| Determinand | Limit | Unit |
|-------------|-------|------|
| BOD ATU | 20.0 | mg/L |
| Ammonia (N) | 5.0 | mg/L |
| Oil | 10.0 | mg/L |
| Lead - as Pb | 0.5 | mg/L |

## Regions

| Prefix | Region | Delay |
|--------|--------|-------|
| AN | Anglian | 1.0s |
| SO | Southern | 0.8s |
| SW | South West | 1.2s |
| NW | North West | 0.9s |
| MD | Midlands/Other | 1.5s |

---

**BTL Môn Hệ Phân Bố** - Kiến trúc Event-Driven với Apache Kafka
