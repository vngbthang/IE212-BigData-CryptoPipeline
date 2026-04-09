# 🚀 Real-time Crypto Data Pipeline

Dự án xây dựng pipeline xử lý dữ liệu tiền điện tử theo thời gian thực với kiến trúc Medallion, chạy trên cụm pseudo-distributed bằng Docker Compose.

## 📌 Mục tiêu
- Nhận tick data realtime từ Coinbase.
- Chuẩn hóa và đẩy vào Kafka.
- Tính nến 1 phút (OHLCV) bằng Spark Structured Streaming.
- Chạy suy luận ML (Pandas UDF + Arrow).
- Ghi dữ liệu vào PostgreSQL (Hot) và HDFS (Cold).
- Hiển thị realtime trên Streamlit.

## 🧱 Kiến trúc Medallion
- Bronze: Ingestion Python WebSocket -> Avro -> Kafka (topic `crypto_ticks`, key `product_id`).
- Silver: Spark đọc Kafka, watermark 1 phút, window 1 phút, tạo OHLCV + feature + prediction.
- Gold: Ghi append vào PostgreSQL (partition theo ngày) và ghi Parquet lên HDFS.
- Orchestration: Airflow chạy compaction HDFS ban đêm bằng `repartition(n)`.
- BI: Streamlit query PostgreSQL bằng `SELECT DISTINCT ON`.

## 🧰 Tech Stack
- Python, Apache Kafka, Apache Spark Structured Streaming
- Apache Hadoop HDFS, PostgreSQL, Apache Airflow
- Streamlit, Docker Compose

## ⚙️ Quick Start
```bash
docker-compose up -d
python src/producer/producer.py
spark-submit --master spark://localhost:7077 src/spark/stream_processor.py
```

Dashboard: http://localhost:8501

## 📚 Tài liệu
1. [Đặc tả yêu cầu (SRS)](./docs/1_SRS.md)
2. [Thiết kế kiến trúc (ADD)](./docs/2_Architecture.md)
3. [Kế hoạch triển khai 6 tuần (WBS)](./docs/3_WBS_Plan.md)

## ✅ Ràng buộc kỹ thuật bắt buộc
- Luôn bật: `spark.sql.execution.arrow.maxRecordsPerBatch=10000`.
- Không dùng `coalesce(1)` cho compaction HDFS; dùng `repartition(n)` động.
- Producer/Streaming phải có xử lý lỗi mạng và graceful shutdown.
