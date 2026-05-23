# Real-Time Crypto Trading Terminal

Hệ thống xử lý dữ liệu crypto theo thời gian thực (real-time) sử dụng Apache Iceberg, Spark Structured Streaming, Kafka, và Streamlit Dashboard.

## Mục lục

- [Tính năng](#-tính-năng)
- [Kiến trúc](#-kiến-trúc)
- [Cấu trúc thư mục](#-cấu-trúc-thư-mục)
- [Yêu cầu](#-yêu-cầu)
- [Khởi động](#-khởi-động)
- [Các dịch vụ](#-các-dịch-vụ)
- [Truy cập](#-truy-cập)
- [Monitoring](#-monitoring)
- [Xử lý sự cố](#-xử-lý-sự-cố)

## Tính năng

- **Real-Time Streaming**: Spark Structured Streaming xử lý dữ liệu từ Kafka với micro-batch 200ms
- **ACID Storage**: Apache Iceberg với Nessie Catalog cho versioning và schema enforcement
- **Biểu đồ Binance Style**: Streamlit dashboard với candlestick chart và technical indicators
- **AI Signals**: ML models (LSTM, XGBoost, Isolation Forest) cho trend prediction và anomaly detection
- **Timezone Support**: Tất cả hiển thị theo giờ Việt Nam (UTC+7)

## Kiến trúc

```
┌─────────────────┐     ┌─────────────┐     ┌──────────────────┐
│  Crypto Feeder  │────▶│   Kafka    │────▶│  Spark Streaming │
│  (Simulator)    │     │ crypto_ticks│     │  Processing      │
└─────────────────┘     └─────────────┘     └────────┬─────────┘
                                                      │
                                                      ▼
┌─────────────────┐     ┌─────────────┐     ┌──────────────────┐
│ Streamlit       │◀────│   DuckDB    │◀────│  MinIO (S3)      │
│ Dashboard       │     │ + Iceberg   │     │  Iceberg Tables  │
└─────────────────┘     └─────────────┘     └──────────────────┘
                              ▲
                              │
                     ┌────────┴────────┐
                     │    Nessie       │
                     │    Catalog      │
                     └─────────────────┘
```

1. **Crypto Feeder**: Simulator gửi tick data (BTC, ETH) vào Kafka mỗi 2 giây
2. **Kafka**: Event streaming platform với topic `crypto_ticks` (retention 7 ngày)
3. **Spark Streaming**: Đọc từ Kafka, aggregate thành OHLCV candles (1 phút), ghi vào Iceberg
4. **Iceberg + MinIO**: ACID-compliant storage với partitioning và compression
5. **Nessie Catalog**: Git-like versioning cho Iceberg tables
6. **Streamlit Dashboard**: Query Iceberg qua DuckDB, hiển thị real-time charts và AI signals

## Cấu trúc thư mục

```
real_time_data_lake/
├── docker-compose.yaml        # Docker orchestration
├── simulator/                 # Crypto data simulator
│   ├── simulator.py          # Gửi tick data vào Kafka
│   └── Dockerfile
├── spark-jobs/               # Spark Structured Streaming
│   ├── main.py              # Main streaming job
│   ├── schemas.py           # Data schemas (OHLCV)
│   ├── data_processing.py   # Business logic
│   ├── iceberg_operations.py # Iceberg write operations
│   ├── table_creation.py    # Table initialization
│   └── Dockerfile
├── src/
│   └── dashboard/           # Streamlit Dashboard
│       ├── app.py           # Main dashboard app
│       └── Dockerfile
└── warehouse/                # MinIO data (bind mount hoặc S3)
    └── gold/               # Iceberg tables
        └── crypto_ohlcv_*/
            ├── data/        # Parquet data files
            └── metadata/    # Iceberg metadata
```

## Yêu cầu

- Docker Desktop (Windows/macOS) hoặc Docker Engine (Linux)
- 8GB+ RAM cho toàn bộ stack
- Python 3.11+ (nếu chạy local)

## Khởi động

### 1. Start tất cả services

```bash
cd real_time_data_lake
docker-compose up -d
```

### 2. Kiểm tra trạng thái

```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Output mong đợi:
```
NAMES              STATUS
spark-master       Up (healthy)
crypto-feeder      Up
crypto-dashboard   Up
storage           Up (healthy)
kafka            Up (healthy)
catalog          Up
```

### 3. Chờ 30-60 giây để Spark job khởi động và bắt đầu xử lý

## Các dịch vụ

| Service | Port | Mô tả |
|---------|------|--------|
| Dashboard | 8501 | Streamlit dashboard (xem biểu đồ, signals) |
| MinIO Console | 9001 | MinIO web console (user: admin, pass: password) |
| Spark UI | 4040 | Spark Structured Streaming UI |
| Kafka | 9092 | Kafka broker (internal) |
| Nessie | 19120 | Nessie catalog API (internal) |

## Truy cập

### Dashboard
```
http://localhost:8501
```

Dashboard bao gồm:
- Real-time candlestick chart (Binance style)
- AI Signals: LSTM Trend, XGBoost Risk, Whale Alert
- Pipeline Status: Lag indicator, Last Candle time
- Market stats: Price, Volume, RSI, Volatility

### MinIO Console
```
http://localhost:9001
```
- Username: `admin`
- Password: `password`

### Spark UI
```
http://localhost:4040
```

## Monitoring

### Kiểm tra Kafka consumer lag

```bash
docker exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --all-groups --describe
```

### Kiểm tra Spark batch progress

```bash
docker logs --since 30s spark-master 2>&1 | Select-String "OffsetsBehindLatest"
```

Output mong đợi: `"maxOffsetsBehindLatest" : "0"`

### Kiểm tra Iceberg metadata files

```bash
docker exec storage mc ls minio/warehouse/gold/crypto_ohlcv_*/metadata/ | Select-Object -Last 5
```

### Kiểm tra dashboard data freshness

```bash
docker exec crypto-dashboard python -c "import duckdb; ..."
```

## Xử lý sự cố

### Dashboard hiển thị dữ liệu cũ

1. Force refresh trình duyệt: `Ctrl+Shift+R` (Windows) hoặc `Cmd+Shift+R` (Mac)
2. Hoặc mở trong tab ẩn danh
3. Kiểm tra data freshness:

```bash
docker exec crypto-dashboard python -c "
import boto3
from botocore.config import Config
import duckdb
from datetime import datetime, timezone, timedelta

s3 = boto3.client('s3', endpoint_url='http://storage:9000',
    aws_access_key_id='admin', aws_secret_access_key='password',
    region_name='us-east-1', config=Config(signature_version='s3v4'))
resp = s3.list_objects_v2(Bucket='warehouse', Prefix='gold/', Delimiter='/')
# ... check latest metadata
"
```

### Spark job dừng xử lý

1. Restart Spark:

```bash
docker restart spark-master
```

2. Kiểm tra logs:

```bash
docker logs --since 60s spark-master 2>&1
```

### Kafka topic có vấn đề retention

Mặc định topic `crypto_ticks` có retention 7 ngày. Nếu cần thay đổi:

```bash
docker exec kafka kafka-configs --bootstrap-server kafka:9092 \
    --alter --topic crypto_ticks --add-config retention.ms=604800000
```

### Reset toàn bộ pipeline

```bash
# Stop all
docker-compose down

# Xóa data cũ (optional)
docker volume rm real_time_data_lake_warehouse

# Restart
docker-compose up -d
```

## Kỹ thuật

### Data Flow

1. **Tick Data**: `symbol, price, volume, timestamp`
2. **Spark Aggregation**: Tumbling window 1 phút → OHLCV candle
3. **Iceberg Write**: Append-only với partition by date
4. **Dashboard Query**: DuckDB iceberg_scan() → Streamlit visualization

### Key Configurations

- **Spark Trigger**: 200ms micro-batch
- **Kafka Retention**: 7 ngày (604800000ms)
- **Iceberg Partition**: By day (window_start)
- **Dashboard Refresh**: 5 giây (fragment)

## License

MIT License
