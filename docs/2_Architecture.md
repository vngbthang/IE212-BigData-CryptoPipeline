# 🏗️ 2. Thiết kế Kiến trúc (ADD)

## 1. Tổng quan hệ thống
Cụm triển khai gồm Zookeeper, Kafka đa broker, Spark Master-Workers, PostgreSQL, HDFS, Airflow và Streamlit. Luồng chính là streaming liên tục; luồng phụ là batch ban đêm cho bảo trì dữ liệu.

## 2. Data Flow theo Medallion

### Bronze Layer
- Producer Python kết nối Coinbase WebSocket.
- Lọc bản tin ticker, chuẩn hóa dữ liệu theo schema 9 trường.
- Publish vào Kafka topic crypto_ticks với key product_id.

### Silver Layer
- Spark Structured Streaming đọc Kafka và parse payload.
- Chuyển timestamp sang event_time.
- Áp dụng watermark 1 phút và window 1 phút để tạo OHLCV.
- Thực thi inference bằng Pandas UDF với Arrow.
- Ghi song song sang PostgreSQL và HDFS.

### Gold Layer
- PostgreSQL lưu dữ liệu phục vụ truy vấn nhanh cho BI.
- HDFS Parquet lưu lịch sử phục vụ phân tích dài hạn và batch maintenance.

### BI Layer
- Streamlit truy vấn bảng Gold để dựng biểu đồ nến và chỉ số vận hành.
- Truy vấn latest-record ưu tiên mẫu DISTINCT ON theo symbol.

## 3. Orchestration
Airflow điều phối tác vụ ban đêm:
- Compaction file nhỏ HDFS bằng repartition động theo khối lượng dữ liệu.
- Không sử dụng coalesce(1) để tránh bottleneck một task.

## 4. Các Quyết Định Thiết Kế & Đánh Đổi (Design Decisions & Trade-offs)

### 4.1 PostgreSQL PARTITION BY RANGE và Composite Index
Quyết định:
- Bảng gold_crypto_ohlcv được partition theo RANGE trên ngày của timestamp.
- Tạo composite index trọng yếu:
  - symbol, timestamp giảm dần
  - timestamp giảm dần, symbol
- Tạo thêm partial index cho quality_status và predicted_direction khác null.

Lý do:
- Dữ liệu chuỗi thời gian tăng nhanh, partition theo ngày giúp retention, vacuum, backup và prune partition hiệu quả.
- Composite index tối ưu truy vấn dạng latest-by-symbol, DISTINCT ON, range-scan theo thời gian.
- Partial index giảm kích thước index, tăng tốc truy vấn dashboard có điều kiện.

Đánh đổi:
- Tăng độ phức tạp quản trị partition.
- Tăng chi phí ghi do duy trì nhiều index.

### 4.2 Watermark 1 phút và giới hạn Arrow batch 10000
Quyết định:
- Watermark event_time là 1 phút.
- spark.sql.execution.arrow.maxRecordsPerBatch đặt 10000 cho Pandas UDF.

Lý do:
- Watermark 1 phút cân bằng giữa độ đầy đủ dữ liệu và độ trễ đầu ra, chấp nhận late event trong biên xử lý.
- Giới hạn batch Arrow ở 10000 giảm nguy cơ OOM khi chuyển đổi Spark sang Pandas trong inference.

Đánh đổi:
- Watermark lớn hơn có thể tăng độ trễ kết quả đóng cửa sổ.
- Batch nhỏ hơn an toàn bộ nhớ nhưng tăng overhead số lần gọi UDF.

### 4.3 Chiến lược reconnect của Producer
Quan sát hiện trạng:
- Producer dùng WebSocketApp.run_forever với callback on_error và on_close.
- Chưa có lớp exponential backoff tường minh trong mã hiện tại.

Lý do chọn hiện trạng:
- Cấu trúc đơn giản, triển khai nhanh, phù hợp giai đoạn prototype.
- Dễ theo dõi lỗi qua logging.

Đánh đổi:
- Thiếu kiểm soát reconnect nâng cao khi mạng chập chờn dài hạn.
- Nên bổ sung backoff theo cấp số nhân và jitter ở phiên bản tiếp theo để tăng tính production-grade.

## 5. Ràng buộc kỹ thuật bắt buộc
- Bật Arrow và khóa maxRecordsPerBatch=10000 cho luồng có Pandas UDF.
- Compaction HDFS phải dùng repartition động, không dùng coalesce(1).
- Toàn pipeline cần xử lý lỗi mạng và shutdown an toàn.
