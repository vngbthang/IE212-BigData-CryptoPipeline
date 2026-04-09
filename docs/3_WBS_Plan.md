# 📅 3. Kế hoạch quản lý dự án & WBS (6 tuần)

## 1. Mục tiêu quản lý dự án
Mục tiêu của kế hoạch là đảm bảo nhóm 3 thành viên triển khai thành công pipeline xử lý dữ liệu crypto thời gian thực theo kiến trúc Medallion, đáp ứng các ràng buộc kỹ thuật đã chốt:
- Bronze: ingestion WebSocket -> Kafka với dữ liệu chuẩn hóa.
- Silver: Spark Structured Streaming với watermark 1 phút, window 1 phút, và suy luận ML qua Pandas UDF.
- Gold: ghi song song PostgreSQL (hot) và HDFS Parquet (cold).
- Orchestration: Airflow chạy nightly compaction bằng `repartition(n)`, không dùng `coalesce(1)`.
- BI: Streamlit hiển thị dữ liệu realtime phục vụ demo học thuật.

## 2. Cơ cấu nhân sự và trách nhiệm
- **TV1 - Infrastructure Lead**: thiết kế hạ tầng Docker Compose, Kafka/HDFS/Spark cluster, Airflow vận hành.
- **TV2 - Streaming & Data Processing Lead**: xây dựng producer, Spark streaming job, feature engineering, tích hợp inference.
- **TV3 - Database & BI Lead**: thiết kế schema PostgreSQL, tối ưu truy vấn, xây dashboard Streamlit, hỗ trợ kiểm thử dữ liệu.

## 3. Work Breakdown Structure theo tuần

### Tuần 1 - Khởi động & chốt thiết kế
**Mục tiêu tuần:** đóng băng yêu cầu, thống nhất kiến trúc và data contract.

**Công việc chính:**
- Hoàn thiện SRS, Architecture, tiêu chí nghiệm thu.
- Chốt schema dữ liệu tick và mapping Bronze -> Silver -> Gold.
- Thiết lập quy ước nhánh Git, commit convention, logging standard.

**Phân công:**
- TV1: chuẩn hóa cấu trúc repo, baseline docker-compose.
- TV2: đề xuất data flow streaming và mô hình aggregation 1 phút.
- TV3: thiết kế sơ bộ bảng Gold và chiến lược index.

**Đầu ra:**
- Tài liệu baseline (SRS/ADD/WBS).
- Danh sách task chi tiết cho tuần 2-6.

### Tuần 2 - Dựng hạ tầng pseudo-distributed
**Mục tiêu tuần:** toàn bộ service cốt lõi khởi động ổn định.

**Công việc chính:**
- Hoàn thiện Docker Compose cho Zookeeper, Kafka (3 broker), Spark master/workers, HDFS, PostgreSQL, Airflow, Streamlit.
- Thiết lập network nội bộ, volume persistence, health check.
- Kiểm thử kết nối liên service (Kafka, JDBC PostgreSQL, HDFS path).

**Phân công:**
- TV1: chủ trì hạ tầng và script kiểm tra.
- TV2: xác thực kết nối Spark -> Kafka.
- TV3: xác thực PostgreSQL schema init và quyền truy cập.

**Đầu ra:**
- Hạ tầng chạy ổn định với checklist health.

### Tuần 3 - Hoàn thiện Bronze (Ingestion)
**Mục tiêu tuần:** dữ liệu tick vào Kafka liên tục và đúng schema.

**Công việc chính:**
- Hoàn thiện producer kết nối Coinbase WebSocket.
- Chuẩn hóa payload theo các trường: timestamp, product_id, price, size, ask, bid, side, sequence, ingestion_timestamp.
- Cấu hình producer: `acks=all`, `enable.idempotence=true`, `compression.type=snappy`.
- Bổ sung xử lý lỗi parse và cơ chế reconnect/shutdown an toàn.

**Phân công:**
- TV2: phát triển producer và logging xử lý lỗi.
- TV1: giám sát Kafka topic/partition và kiểm tra throughput.
- TV3: kiểm tra chất lượng dữ liệu đầu vào phục vụ Gold schema.

**Đầu ra:**
- Pipeline Coinbase -> Kafka chạy ổn định.

### Tuần 4 - Hoàn thiện Silver (Streaming)
**Mục tiêu tuần:** tạo OHLCV 1 phút và sẵn sàng inference.

**Công việc chính:**
- Spark đọc `crypto_ticks`, parse schema, tạo `event_time`.
- Áp dụng `withWatermark("event_time", "1 minute")` và tumbling window 1 phút.
- Tính OHLCV: open/high/low/close/volume theo `product_id`.
- Thiết lập checkpoint cho fault recovery.
- Bật cấu hình bắt buộc: `spark.sql.execution.arrow.maxRecordsPerBatch=10000`.

**Phân công:**
- TV2: phát triển stream processor và logic aggregation.
- TV1: tối ưu tài nguyên Spark (executor/driver/checkpoint path).
- TV3: xác thực dữ liệu Silver phục vụ truy vấn BI.

**Đầu ra:**
- Streaming job tạo nến 1 phút và không vượt giới hạn bộ nhớ.

### Tuần 5 - Hoàn thiện Gold + Orchestration
**Mục tiêu tuần:** dữ liệu đầu ra sẵn sàng phân tích và bảo trì tự động.

**Công việc chính:**
- Ghi nhánh 1 vào PostgreSQL theo append-only.
- Ghi nhánh 2 vào HDFS định dạng Parquet nén snappy.
- Hoàn thiện DDL partition theo ngày và composite index cho truy vấn latest-by-symbol.
- Xây Airflow DAG nightly compaction dùng `repartition(n)` động.

**Phân công:**
- TV3: tối ưu PostgreSQL (partition/index/query pattern).
- TV1: DAG Airflow và vận hành batch.
- TV2: đồng bộ schema giữa Silver output và Gold sink.

**Đầu ra:**
- Gold storage hoạt động song song PostgreSQL + HDFS.
- Batch compaction chạy định kỳ ban đêm.

### Tuần 6 - BI, kiểm thử, tối ưu và demo
**Mục tiêu tuần:** chốt sản phẩm, kiểm thử toàn diện và sẵn sàng bảo vệ.

**Công việc chính:**
- Hoàn thiện Streamlit dashboard (candlestick, metrics, trạng thái pipeline).
- Kiểm thử end-to-end: ingestion -> processing -> storage -> visualization.
- Đo latency/throughput, xử lý điểm nghẽn còn lại.
- Chuẩn hóa tài liệu vận hành, slide và kịch bản demo.

**Phân công:**
- TV3: dashboard và truy vấn tối ưu.
- TV2: kiểm thử pipeline streaming + tính ổn định.
- TV1: kiểm thử vận hành cụm và phương án khôi phục sự cố.

**Đầu ra:**
- Bản release đồ án có thể demo trực tiếp.

## 4. KPI theo dõi tiến độ
- **KPI-1: Tính sẵn sàng dịch vụ**: các container cốt lõi hoạt động ổn định trong phiên test liên tục.
- **KPI-2: Tính toàn vẹn dữ liệu**: dữ liệu vào/ra đồng nhất theo schema đã định nghĩa.
- **KPI-3: Hiệu năng streaming**: cập nhật nến 1 phút đúng chu kỳ; không tràn bộ nhớ Pandas UDF.
- **KPI-4: Chất lượng truy vấn BI**: dashboard truy vấn được dữ liệu mới nhất theo symbol.
- **KPI-5: Khả năng phục hồi**: restart job vẫn tiếp tục xử lý dựa trên checkpoint.

## 5. Rủi ro chính và phương án giảm thiểu
- **Rủi ro thiếu RAM khi inference**: khóa `maxRecordsPerBatch=10000`, giảm batch/trigger khi cần.
- **Rủi ro file nhỏ trên HDFS**: compaction định kỳ bằng repartition động.
- **Rủi ro đứt kết nối WebSocket/Kafka**: tăng logging cảnh báo, cơ chế reconnect và giám sát topic lag.
- **Rủi ro lệch tiến độ nhóm**: họp ngắn hàng ngày, khóa phạm vi theo tuần, ưu tiên task critical path.

## 6. Deliverables cuối kỳ
- Pipeline realtime end-to-end chạy ổn định theo kiến trúc Medallion.
- Bộ tài liệu hoàn chỉnh phục vụ báo cáo học thuật.
- Dashboard realtime hỗ trợ quan sát dữ liệu và trạng thái hệ thống.
- Kịch bản demo + checklist vận hành và kiểm thử.
