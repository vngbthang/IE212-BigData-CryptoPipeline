# 📅 3. Kế hoạch triển khai 6 tuần (WBS)

## 1. Phân vai 3 thành viên
- TV1 (Infra): Docker, Kafka, HDFS, Airflow, mạng nội bộ cụm.
- TV2 (Streaming): Producer, Spark streaming, feature engineering, inference.
- TV3 (DBA/BI): PostgreSQL schema/tối ưu query, dashboard Streamlit, kiểm thử dữ liệu.

## 2. Kế hoạch theo tuần

### Tuần 1: Khởi tạo dự án
- Chốt yêu cầu SRS và thiết kế ADD.
- Thống nhất schema dữ liệu và quy ước code/log.

### Tuần 2: Dựng hạ tầng
- Hoàn thiện docker-compose cho toàn bộ service.
- Kiểm tra health check Kafka/Spark/HDFS/PostgreSQL.

### Tuần 3: Ingestion (Bronze)
- Hoàn thiện producer WebSocket -> Kafka.
- Bổ sung retry, reconnect, graceful shutdown.

### Tuần 4: Streaming (Silver)
- Spark đọc Kafka, watermark + window 1 phút.
- Tính OHLCV, feature và tích hợp Pandas UDF ML.

### Tuần 5: Gold + Orchestration
- Ghi PostgreSQL append-only và HDFS Parquet.
- Tạo DAG Airflow compaction bằng `repartition(n)`.

### Tuần 6: Dashboard + Testing + Demo
- Hoàn thiện Streamlit dashboard.
- Unit test + integration test end-to-end.
- Tối ưu hiệu năng, hoàn tất báo cáo và demo.

## 3. Deliverables cuối kỳ
- Pipeline realtime end-to-end chạy ổn định.
- Bộ tài liệu chuẩn hóa trong thư mục docs.
- Dashboard realtime phục vụ demo.
- Checklist vận hành, kiểm thử và hướng dẫn chạy.
