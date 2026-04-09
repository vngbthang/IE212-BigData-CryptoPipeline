# 📝 1. Đặc tả yêu cầu (SRS)

## 1. Mục tiêu hệ thống
Xây dựng hệ thống Data Engineering xử lý luồng crypto realtime từ Coinbase, tạo nến 1 phút, bổ sung dự đoán ML và cung cấp dashboard theo thời gian thực.

## 2. Phạm vi
### 2.1 In-scope
- Ingestion realtime qua WebSocket.
- Streaming ETL theo kiến trúc Bronze -> Silver -> Gold.
- Tính OHLCV 1 phút + chỉ báo kỹ thuật cơ bản.
- ML inference bằng Pandas UDF.
- Lưu trữ 2 tầng: PostgreSQL (hot), HDFS Parquet (cold).
- Dashboard Streamlit và orchestration Airflow.

### 2.2 Out-of-scope
- Triển khai production trên cloud.
- Hệ thống auto-trading.
- Cam kết độ chính xác mô hình tuyệt đối.

## 3. Yêu cầu chức năng
### FR-01 Ingestion
- Kết nối Coinbase WebSocket, nhận tick liên tục.
- Chuẩn hóa dữ liệu và serialize trước khi publish Kafka.
- Retry/reconnect khi đứt mạng.

### FR-02 Streaming & Aggregation
- Spark Structured Streaming đọc Kafka theo micro-batch.
- Watermark 1 phút, window 1 phút.
- Tính Open/High/Low/Close/Volume.

### FR-03 ML Inference
- Chạy mô hình pre-trained qua Pandas UDF.
- Trả ra nhãn dự đoán và độ tin cậy.

### FR-04 Storage
- Branch 1: append vào PostgreSQL bảng partition theo ngày.
- Branch 2: ghi Parquet lên HDFS.

### FR-05 Dashboard
- Hiển thị giá, nến, trạng thái pipeline gần realtime.

### FR-06 Batch Orchestration
- Airflow chạy nightly compaction HDFS và các job bảo trì.

## 4. Yêu cầu phi chức năng
- Độ trễ end-to-end mục tiêu dưới 3 giây.
- Tính sẵn sàng cao trong môi trường đồ án.
- Chống mất dữ liệu bằng Kafka replication + Spark checkpoint.
- Chống OOM với `spark.sql.execution.arrow.maxRecordsPerBatch=10000`.
- Khả năng khôi phục sau lỗi tiến trình.

## 5. Tiêu chí nghiệm thu
- Dữ liệu đi đủ pipeline từ Coinbase đến Dashboard.
- Spark restart vẫn khôi phục qua checkpoint.
- PostgreSQL truy vấn realtime ổn định.
- HDFS lưu được dữ liệu Parquet đúng partition.
