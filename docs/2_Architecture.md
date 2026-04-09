# 🏗️ 2. Thiết kế kiến trúc (ADD)

## 1. Tổng thể hệ thống
Hệ thống chạy pseudo-distributed bằng Docker Compose gồm:
- Zookeeper, Kafka (3 brokers)
- Spark (1 master, 2 workers)
- Hadoop (NameNode, DataNode)
- PostgreSQL, Airflow, Streamlit

## 2. Data Flow theo Medallion

### 2.1 Bronze Layer
- Python producer kết nối Coinbase WebSocket.
- Parse JSON tick -> serialize (Avro) -> publish Kafka topic `crypto_ticks`.
- Message key là `product_id` để giữ thứ tự event theo cặp giao dịch.

### 2.2 Silver Layer
- Spark Structured Streaming đọc từ Kafka.
- Áp dụng watermark 1 phút để xử lý late data.
- Áp dụng tumbling window 1 phút để tạo OHLCV.
- Tạo feature kỹ thuật và chạy Pandas UDF (Arrow) cho ML inference.

### 2.3 Gold Layer
- Nhánh Hot: append vào PostgreSQL (partition theo ngày).
- Nhánh Cold: ghi Parquet lên HDFS để lưu lịch sử.

## 3. Orchestration
- Airflow chạy nightly batch:
  - Compaction file nhỏ trên HDFS bằng `repartition(n)` động.
  - Tuyệt đối không dùng `coalesce(1)`.

## 4. BI Layer
- Streamlit truy vấn PostgreSQL.
- Ưu tiên câu truy vấn dạng `SELECT DISTINCT ON` cho bản ghi mới nhất mỗi symbol.

## 5. Reliability & vận hành
- Spark checkpoint để recover trạng thái streaming.
- Producer/Spark có xử lý exception mạng và shutdown an toàn.
- Logging theo mức INFO/WARN/ERROR để phục vụ debug nhanh.

## 6. Cấu hình bắt buộc
- `spark.sql.execution.arrow.maxRecordsPerBatch=10000`.
- Trigger micro-batch ổn định, phù hợp tài nguyên máy.
- Kafka replication >= 3 trong mô hình giả lập nhiều broker.
