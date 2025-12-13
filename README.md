# Water Quality Monitoring System

Hệ thống giám sát chất lượng nước sử dụng Kafka để streaming dữ liệu và Machine Learning để dự đoán BOD ATU.

## Kiến trúc

```
CSV File → Producer → Kafka → Consumer → PostgreSQL (Neon)
                                  ↓
                            ML Models (SGD + RandomForest)
```

## Yêu cầu

- Docker và Docker Compose
- File dữ liệu `2025-C.csv` trong thư mục gốc
- PostgreSQL database (Neon hoặc local)

## Cấu hình môi trường

### Bước 1: Tạo file .env

Sao chép file `.env.example` thành `.env`:

```bash
cp .env.example .env
```

### Bước 2: Chỉnh sửa .env

Mở file `.env` và cập nhật các giá trị:

```bash
# Thay đổi connection string PostgreSQL của bạn
CONN_STR=postgresql://your_user:your_password@your_host/your_database?sslmode=require
```

## Cách chạy

### Chạy với Docker Compose (Khuyến nghị)

```bash
# Build và start tất cả services
docker-compose up --build

# Hoặc chạy ở background
docker-compose up --build -d

# Xem logs
docker-compose logs -f

# Xem logs của service cụ thể
docker-compose logs -f producer
docker-compose logs -f consumer

# Dừng services
docker-compose down
```

### Chạy không dùng Docker (Local)

#### 1. Khởi động Kafka infrastructure

```bash
docker-compose up -d zookeeper kafka
```

#### 2. Cài đặt dependencies

```bash
pip install -r requirements.txt
```

#### 3. Chạy Producer

```bash
# Set environment variable (Windows)
set KAFKA_BOOTSTRAP=localhost:9092

# Hoặc (Git Bash/Linux)
export KAFKA_BOOTSTRAP=localhost:9092

# Chạy producer
python producer_csv.py
```

#### 4. Chạy Consumer (trong terminal khác)

```bash
cd analyze

# Set environment variables
set KAFKA_BOOTSTRAP=localhost:9092
set CONN_STR=your_postgresql_connection_string

# Chạy consumer
python consumer.py
```

## Services

### Zookeeper
- Port: `2181`
- Quản lý Kafka cluster

### Kafka
- Internal port: `29092` (dùng bởi containers)
- External port: `9092` (dùng từ host machine)
- Topic: `water-quality`

### Producer
- Đọc file `2025-C.csv`
- Stream từng dòng vào Kafka topic mỗi giây
- Có thể tùy chỉnh `SLEEP_SECONDS` trong `.env`

### Consumer
- Consume messages từ Kafka
- Train ML models:
  - **Online model**: SGDRegressor (realtime learning)
  - **Offline model**: RandomForest (background training sau 500+ samples)
- Dự đoán BOD ATU values
- Lưu vào PostgreSQL với predictions
- Models được lưu trong `./analyze/models/`

## Cấu trúc dữ liệu

### Input (CSV)
Các cột quan trọng:
- `@id`: Unique identifier
- `sample.samplingPoint.label`: Điểm lấy mẫu
- `sample.sampleDateTime`: Thời gian lấy mẫu
- `determinand.label`: Loại chất đo (BOD ATU, Ammonia, etc.)
- `result`: Giá trị đo được
- `sample.samplingPoint.easting/northing`: Tọa độ OSGB36

### Output (PostgreSQL)
Table: `water_quality`
- Thông tin cơ bản: id, sampling_point, sample_time, determinand, value
- Tọa độ: easting, northing, latitude, longitude, region
- ML predictions: predicted_value, predicted_model
- Metadata: violation status, raw_data

## Troubleshooting

### Kafka không start được

```bash
# Kiểm tra logs
docker-compose logs kafka

# Xóa volumes và restart
docker-compose down -v
docker-compose up --build
```

### Producer không kết nối được Kafka

```bash
# Đảm bảo Kafka đã sẵn sàng
docker-compose logs kafka | grep "started"

# Kiểm tra KAFKA_BOOTSTRAP variable
echo $KAFKA_BOOTSTRAP
```

### Consumer không ghi được database

- Kiểm tra `CONN_STR` trong `.env`
- Đảm bảo database tồn tại và có quyền ghi
- Xem logs: `docker-compose logs consumer`

### Out of memory

Nếu file CSV quá lớn (62MB như `2025-C.csv`), tăng memory cho Docker:
- Docker Desktop > Settings > Resources > Memory: tăng lên 4GB+

## Giám sát

### Xem ML models

```bash
# List models đã train
ls -lh analyze/models/
```

### Query database

```sql
-- Số lượng records theo determinand
SELECT determinand, COUNT(*) 
FROM water_quality 
GROUP BY determinand;

-- Xem predictions cho BOD ATU
SELECT sample_time, value, predicted_value, predicted_model
FROM water_quality
WHERE determinand = 'BOD ATU' AND predicted_value IS NOT NULL
ORDER BY sample_time DESC
LIMIT 10;

-- Violations
SELECT determinand, COUNT(*)
FROM water_quality
WHERE violation = true
GROUP BY determinand;
```

## Phát triển

### Thêm determinand mới để predict

Chỉnh sửa `analyze/consumer.py`:
1. Thêm limit vào `LIMITS` dict (dòng 55-61)
2. Thêm logic prediction trong `parse_message()` (dòng 529-540)

### Thay đổi ML model

Chỉnh sửa `background_trainer()` trong `analyze/consumer.py`:
- Thay đổi features (dòng 413-431)
- Thay model (dòng 438)

## License

MIT
