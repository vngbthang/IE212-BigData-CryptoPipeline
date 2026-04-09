# 📝 1. Đặc tả Yêu cầu (SRS)

## 1. Mục tiêu
Xây dựng pipeline phân tán xử lý dữ liệu crypto realtime từ Coinbase, tạo nến 1 phút, suy luận xu hướng giá bằng ML và trực quan hóa phục vụ phân tích.

## 2. Phạm vi
### In-scope
- Ingestion realtime bằng WebSocket và publish Kafka.
- Streaming ETL theo kiến trúc Bronze-Silver-Gold.
- Tính OHLCV 1 phút và suy luận ML qua Pandas UDF.
- Lưu trữ hai tầng: PostgreSQL hot storage, HDFS cold storage.
- Dashboard Streamlit và orchestration Airflow.

### Out-of-scope
- Auto-trading và tích hợp sàn để đặt lệnh.
- Triển khai production cloud và cam kết độ chính xác ML tuyệt đối.

## 3. Yêu cầu chức năng nâng cao

### FR-01 Ingestion và chuẩn hóa schema
Producer phải đọc thông điệp ticker và ánh xạ về schema chuẩn gồm chính xác 9 trường:
1. timestamp: long, mili giây theo event-time
2. product_id: string
3. price: double
4. size: double
5. ask: null hoặc double
6. bid: null hoặc double
7. side: null hoặc string
8. sequence: long
9. ingestion_timestamp: long, mili giây khi hệ thống nhận

Yêu cầu vận hành:
- Publish vào topic crypto_ticks với key là product_id để duy trì thứ tự theo cặp giao dịch.
- Bật acks=all, enable.idempotence=true, compression.type=snappy.
- Có xử lý lỗi JSON parse, lỗi mạng và đóng tiến trình an toàn.

### FR-02 Streaming và tổng hợp OHLCV
Spark phải:
- Đọc Kafka topic crypto_ticks với startingOffsets=latest.
- Parse payload thành các cột dữ liệu chuẩn.
- Tạo event_time từ timestamp.
- Áp dụng watermark 1 phút.
- Áp dụng window 1 phút và aggregate theo product_id hoặc symbol để tạo:
  open, high, low, close, volume
- Ghi ra 2 nhánh:
  - PostgreSQL append mode
  - HDFS Parquet nén snappy

### FR-03 ML inference
Mô-đun inference trong stream_processor sử dụng Pandas UDF với Arrow.
- Input feature vector:
  open, high, low, close, volume, ma_5m, ma_20m
- Output dự đoán:
  predicted_direction, confidence_score, model_version
- Nếu model không khả dụng, hệ thống fallback về logic baseline dựa trên quan hệ close so với open.

### FR-04 Dashboard realtime
Ứng dụng Streamlit phải:
- Truy vấn dữ liệu Gold từ PostgreSQL.
- Hiển thị nến OHLCV, thống kê giá và khối lượng.
- Cập nhật gần realtime và có thông báo khi không có dữ liệu.

### FR-05 Orchestration
Airflow chạy tác vụ ban đêm:
- Compaction file nhỏ trên HDFS
- Sử dụng repartition động, không dùng coalesce(1)

## 4. Yêu cầu phi chức năng
- Độ trễ mục tiêu end-to-end dưới 3 giây trong tải chuẩn.
- Chống mất dữ liệu bằng Kafka replication và Spark checkpoint.
- Ổn định bộ nhớ Pandas UDF với spark.sql.execution.arrow.maxRecordsPerBatch=10000.
- Có logging phân cấp và khả năng khôi phục sau sự cố tiến trình.

## 5. Tiêu chí nghiệm thu
- Dữ liệu đi trọn vẹn từ Coinbase đến PostgreSQL và HDFS.
- Spark restart vẫn khôi phục state từ checkpoint.
- Dashboard hiển thị dữ liệu mới nhất đúng theo từng symbol.
