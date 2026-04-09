# 🚀 Real-time Crypto Data Pipeline

Pipeline này triển khai kiến trúc Medallion cho bài toán xử lý dữ liệu tiền điện tử theo thời gian thực, chạy trên cụm pseudo-distributed bằng Docker Compose. Mục tiêu học thuật là chứng minh khả năng thiết kế hệ thống streaming có tính nhất quán dữ liệu, chịu lỗi, và tối ưu truy vấn phục vụ phân tích gần thời gian thực.

## Tổng quan chuyên sâu
Hệ thống bắt đầu từ tầng Bronze, nơi tiến trình Python kết nối Coinbase WebSocket tại địa chỉ ws-feed.exchange.coinbase.com, lắng nghe kênh ticker cho BTC-USD và ETH-USD. Producer cấu hình Kafka với bootstrap nhiều broker, cơ chế acks=all, enable.idempotence=true và nén snappy để giảm mất dữ liệu khi mạng dao động. Mô hình dữ liệu nguồn được chuẩn hóa theo schema kiểu Avro gồm 9 trường cốt lõi: timestamp, product_id, price, size, ask, bid, side, sequence, ingestion_timestamp; trong đó timestamp và ingestion_timestamp dùng đơn vị mili giây nhằm đồng bộ logic event-time ở Spark.

Tại tầng Silver, Spark Structured Streaming đọc topic crypto_ticks với chế độ startingOffsets=latest. Pipeline áp dụng Watermark 1 phút và tumbling window 1 phút để tổng hợp OHLCV theo từng product_id, đồng thời kích hoạt checkpoint riêng cho từng nhánh ghi nhằm đảm bảo khả năng khôi phục. Ở phần nâng cao, pipeline sử dụng Pandas UDF cho suy luận ML với Apache Arrow và ràng buộc spark.sql.execution.arrow.maxRecordsPerBatch=10000 để kiểm soát bộ nhớ khi chuyển đổi Spark DataFrame sang Pandas DataFrame theo lô.

Tầng Gold chia thành hai nhánh lưu trữ: PostgreSQL cho dữ liệu nóng và HDFS Parquet cho dữ liệu lạnh. Trong PostgreSQL, bảng gold_crypto_ohlcv dùng Declarative Partitioning kiểu PARTITION BY RANGE theo ngày, kết hợp composite index theo symbol, timestamp giảm dần để tối ưu truy vấn DISTINCT ON và truy vấn chuỗi thời gian. HDFS dùng định dạng Parquet nén snappy để tối ưu dung lượng và I/O tuần tự.

Lớp orchestration dùng Airflow chạy tác vụ compaction ban đêm, ưu tiên repartition động thay cho coalesce(1) để tránh tạo nút nghẽn single-task. Lớp BI dùng Streamlit kết nối PostgreSQL, hiển thị nến, chỉ số và trạng thái pipeline theo thời gian thực.

## Tech Stack
- Python, WebSocket client, Confluent Kafka client
- Apache Kafka, Apache Spark Structured Streaming, Apache Arrow
- PostgreSQL, Hadoop HDFS, Apache Airflow
- Streamlit, Plotly, Docker Compose

## Quick Start
1. Khởi động hạ tầng: docker-compose up -d
2. Chạy ingestion: python src/producer/producer.py
3. Chạy Spark job: spark-submit --master spark://localhost:7077 src/spark/stream_processor.py
4. Mở dashboard: http://localhost:8501

## Tài liệu chi tiết
- [SRS Document](./docs/1_SRS.md)
- [System Architecture](./docs/2_Architecture.md)
- [WBS Plan](./docs/3_WBS_Plan.md)