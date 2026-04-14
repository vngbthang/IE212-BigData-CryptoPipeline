## 📋 Khắc phục sự cố: Kafka Metadata Timeout & Connection Refused

Lỗi `Metadata timeout` hoặc `Connection refused` thường xảy ra khi Producer/Consumer không tìm thấy cụm Kafka hoặc cấu hình Listener nội bộ bị sai. Hãy thực hiện kiểm tra theo trình tự sau:

### Bước 1: Kiểm tra "Trái tim" của cụm (Zookeeper)
Kafka phụ thuộc hoàn toàn vào Zookeeper. Nếu Zookeeper sập, toàn bộ Kafka sẽ tê liệt.
- **Lệnh kiểm tra:** `docker compose logs zookeeper | tail -n 20`
- **Dấu hiệu bình thường:** Container trạng thái `Up (healthy)`, không có lỗi `FATAL`, đang bind port `2181`.

### Bước 2: Kiểm tra trạng thái Cluster Kafka
Xác nhận 3 node Kafka đang nhận diện được nhau và cùng tham gia vào một cụm.
- **Lệnh kiểm tra:** `docker exec -it kafka-0 kafka-broker-api-versions --bootstrap-server localhost:9092`
- **Dấu hiệu bình thường:** Trả về danh sách đầy đủ 3 broker (id: 0, 1, 2).

### Bước 3: Kiểm tra kết nối mạng nội bộ (Docker Network)
Xác nhận container sinh dữ liệu có thể "nhìn thấy" Kafka thông qua tên miền DNS nội bộ của Docker.
- **Lệnh kiểm tra:** `docker exec -it crypto-producer-mock ping kafka-0 -c 4`
- **Dấu hiệu bình thường:** Có luồng mạng trả về (0% packet loss). 
- *Cách sửa nếu lỗi:* Kiểm tra lại khai báo `networks: - crypto-network` trong file cấu hình của container bị lỗi.

### Bước 4: Kiểm tra sự tồn tại của Topic
Tránh lỗi `UNKNOWN_TOPIC_OR_PARTITION` khi Producer cố đẩy dữ liệu vào nơi không tồn tại (do tắt tính năng tự động tạo topic).
- **Lệnh kiểm tra:** `docker exec -it kafka-0 kafka-topics --bootstrap-server localhost:9092 --list`
- **Dấu hiệu bình thường:** Topic `crypto_ticks` phải xuất hiện trong danh sách.

### Bước 5: Khởi động lại an toàn (Graceful Restart)
Nếu cụm Kafka bị kẹt (zombie state), tuyệt đối không dùng lệnh `kill`.
- **Lệnh chuẩn:** `docker compose restart kafka-0 kafka-1 kafka-2`

## 📘 Sổ tay Vận hành Hạ tầng (Infrastructure Runbook)
Tài liệu này hướng dẫn các thao tác kỹ thuật để quản lý và giám sát hạ tầng của dự án Crypto Data Pipeline.

### 1. Lệnh Start/Stop (Điều khiển hệ thống)
Hệ thống sử dụng Docker Compose Profiles để tách biệt môi trường Dev và Prod.

- Khởi động môi trường Phát triển (Dev Mode): Chạy tất cả các service cốt lõi cùng với Mock Producer để sinh dữ liệu giả lập.

**Lệnh**: `COMPOSE_PROFILES=dev docker compose up -d --build`

- Khởi động môi trường Thực tế (Prod Mode): Chạy tất cả các service cốt lõi cùng với Coinbase Producer để thu thập dữ liệu thật.

**Lệnh**: `COMPOSE_PROFILES=prod docker compose up -d --build`

- Dừng hệ thống (Stop) cho Dev: Dừng các tiến trình đang chạy nhưng giữ lại dữ liệu trong các Volume.

**Lệnh**: `COMPOSE_PROFILES=dev docker compose stop`

- Dừng hệ thống (Stop) cho Prod: Dừng các tiến trình đang chạy nhưng giữ lại dữ liệu trong các Volume.

**Lệnh**: `COMPOSE_PROFILES=prod docker compose stop`

- Xóa sạch hạ tầng (Down) cho Dev: Dừng container và xóa bỏ hoàn toàn các Volume dữ liệu (Dùng khi muốn reset database/HDFS).

**Lệnh**: `COMPOSE_PROFILES=dev docker compose down -v`

- Xóa sạch hạ tầng (Down) cho Dev: Dừng container và xóa bỏ hoàn toàn các Volume dữ liệu (Dùng khi muốn reset database/HDFS).

**Lệnh**: `COMPOSE_PROFILES=prod docker compose down -v`

### 2. Kiểm tra sức khỏe (Healthcheck)
Xác nhận trạng thái hoạt động của các thành phần trong Medallion Architecture.

- Kiểm tra trạng thái Container: Yêu cầu các service quan trọng (zookeeper, kafka-0, postgres, spark-master) phải ở trạng thái Up.

**Lệnh**: `docker compose ps`

- Xem Log chi tiết của từng Service: Dùng để theo dõi lỗi lúc khởi tạo.

**Lệnh kiểm tra Kafka**: `docker logs -f kafka-0`

**Lệnh kiểm tra Postgres**: `docker logs -f postgres`

### 3. Xác minh luồng dữ liệu vào Topic (Verify Data)
Đảm bảo dữ liệu Tick được đẩy thành công vào Kafka (Bronze Layer).

- Kiểm tra cấu hình Topic crypto_ticks: Xác nhận Topic đã được tạo đúng cấu trúc.

**Lệnh**: `docker exec -it kafka-0 kafka-topics --bootstrap-server localhost:9092 --describe --topic crypto_ticks`

- Kiểm tra dòng dữ liệu từ Producer: Xác nhận code Python đang chạy và gửi payload.

**Lệnh xem Mock Producer**: `docker logs -f crypto-producer-mock`

**Lệnh xem Coinbase Producer**: `docker logs -f crypto-producer-coinbase`

- Đọc thử dữ liệu thực tế trong Kafka: Sử dụng consumer nội bộ để hứng dữ liệu đang chảy.

**Lệnh**: `docker exec -it kafka-0 kafka-console-consumer --bootstrap-server localhost:9092 --topic crypto_ticks --from-beginning --max-messages 10`

### 4. Xử lý khi Broker chưa sẵn sàng (Troubleshooting)
Hướng dẫn xử lý các lỗi khi Producer không đẩy được dữ liệu do Broker khởi động chậm hoặc treo.

- Kiểm tra Zookeeper: Kafka không thể sống nếu thiếu Zookeeper. Đảm bảo Zookeeper đang chạy ổn định và đã mở cổng.

**Lệnh**: `docker logs zookeeper | grep "binding to port"`

- Khắc phục lỗi Metadata Timeout: Lỗi này thường do Producer không phân giải được mạng. Hãy kiểm tra kết nối nội bộ.

**Lệnh test mạng**: `docker exec -it crypto-producer-mock ping kafka-0 -c 4`

- Khởi động lại cụm Kafka: Nếu Broker kẹt (zombie state), tuyệt đối không dùng lệnh kill, hãy khởi động lại an toàn.

**Lệnh**: `docker compose restart kafka-0 kafka-1 kafka-2`

- Khởi động lại Producer: Nếu Broker mất quá lâu để khởi động khiến Producer báo lỗi và crash trước, hãy gọi Producer dậy lại sau khi Broker đã xanh.

**Lệnh**: `docker compose restart crypto-producer-mock`