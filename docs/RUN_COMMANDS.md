# Production Run Commands — Crypto Pipeline

---

## Production Startup Sequence

### 1. Start Full Production Stack

```bash
# Start all production services (no dev profile)
docker compose up -d

# Verify all services are healthy
docker compose ps
```

**Services included:**
- Kafka cluster (3 brokers for fault tolerance)
- Apache Spark (master + 2 workers)
- PostgreSQL (gold layer feature store)
- HDFS (archival storage)
- Airflow (scheduling & maintenance DAGs)
- Streamlit dashboard (real-time UI)
- Coinbase producer (live data source)

### 2. Verify Infrastructure Health

```bash
# Check HDFS capacity and datanode status
docker compose exec namenode hdfs dfsadmin -report

# Verify Kafka broker status
docker compose exec kafka-0 kafka-topics --bootstrap-server localhost:9092 --describe --topic crypto_ticks

# Check Postgres connectivity
docker compose exec postgres psql -U crypto_user -d cryptopipeline -c "SELECT 'Database ready';"
```

---

## Real-Time Data Flow

### 3. Monitor Live Data Ingestion

```bash
# Tail Coinbase producer logs (live ticks)
docker logs --follow crypto-producer-coinbase

# Count ticks reaching Kafka
docker compose exec kafka-0 kafka-console-consumer --bootstrap-server localhost:9092 --topic crypto_ticks --max-messages 5 --timeout-ms 5000
```

### 4. Check Spark Stream Processor

```bash
# Follow processor logs
docker compose exec spark-master bash -lc "tail -f /tmp/stream_processor_local.log"

# Verify micro-batches are processing
docker compose exec spark-master bash -lc "grep -E 'BATCH|DB_WRITE|Writing OHLCV' /tmp/stream_processor_local.log | tail -20"
```

### 5. Verify Gold Layer Freshness

```bash
# Check latest candle timestamp and row count
docker compose exec postgres psql -U crypto_user -d cryptopipeline -c \
  "SELECT COUNT(*) as gold_rows, MAX(process_timestamp) as latest_ts FROM gold.gold_crypto_ohlcv;"

# Query with features (last 5 candles)
docker compose exec postgres psql -U crypto_user -d cryptopipeline -c \
  "SELECT timestamp, symbol, close, log_returns, volatility_30m, z_score, is_outlier FROM gold.gold_crypto_ohlcv ORDER BY timestamp DESC LIMIT 5;"
```

---

## Maintenance & Archival

### 6. Trigger Hourly Archive to HDFS

```bash
# Manually trigger the archive DAG for current hour
docker compose exec airflow-webserver airflow dags trigger archive_to_hdfs_dag

# Monitor archive runs
docker compose exec airflow-webserver airflow dags list-runs -d archive_to_hdfs_dag | head -10

# Verify Parquet files in HDFS
docker compose exec namenode hdfs dfs -ls -R /user/data/archive/
```

### 7. Daily Data Retention & Cleanup

```bash
# Trigger retention DAG (removes data older than 60 days)
docker compose exec airflow-webserver airflow dags trigger retention_dag

# Check retention DAG status
docker compose exec airflow-webserver airflow dags list-runs -d retention_dag | head -5
```

---

## Dashboard Access

### 8. Real-Time Visualization

```bash
# Dashboard automatically runs on startup
# Access at: http://localhost:8501

# Check dashboard health
curl -s http://localhost:8501 | head -20

# Tail dashboard logs
docker logs --follow crypto-dashboard
```

---

## Debugging & Troubleshooting

### 9. Full System Diagnostics

```bash
# Run read-only validation queries directly against the live tables
docker compose exec postgres psql -U crypto_user -d cryptopipeline -c \
  "SELECT COUNT(*) as gold_rows, MAX(process_timestamp) as latest_ts FROM gold.gold_crypto_ohlcv;"

docker compose exec postgres psql -U crypto_user -d cryptopipeline -c \
  "SELECT ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_ms)::NUMERIC, 2) as p50_ms,\
          ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms)::NUMERIC, 2) as p95_ms,\
          ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms)::NUMERIC, 2) as p99_ms,\
          ROUND(MAX(latency_ms)::NUMERIC, 2) as max_ms\
   FROM gold.pipeline_latency_samples\
   WHERE measured_at >= CURRENT_TIMESTAMP - INTERVAL '5 minutes';"

docker compose exec postgres psql -U crypto_user -d cryptopipeline -c \
  "SELECT COUNT(*) as error_count FROM gold.gold_crypto_ohlcv WHERE error_flag = TRUE;"
```

### 10. Check Latency Percentiles (Last 5 minutes)

```bash
docker compose exec postgres psql -U crypto_user -d cryptopipeline -c \
  "SELECT 
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_ms)::NUMERIC, 2) as p50_ms,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms)::NUMERIC, 2) as p95_ms,
    ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms)::NUMERIC, 2) as p99_ms,
    ROUND(MAX(latency_ms)::NUMERIC, 2) as max_ms
   FROM gold.pipeline_latency_samples
   WHERE measured_at >= CURRENT_TIMESTAMP - INTERVAL '5 minutes';"
```

### 11. Monitor DLQ for Invalid Records

```bash
# Consume from dead letter queue (shows rejected records)
docker compose exec kafka-0 kafka-console-consumer --bootstrap-server localhost:9092 --topic crypto_ticks_dead_letter --max-messages 5 --timeout-ms 5000

# Count errors in gold table
docker compose exec postgres psql -U crypto_user -d cryptopipeline -c \
  "SELECT COUNT(*) as error_count FROM gold.gold_crypto_ohlcv WHERE error_flag = TRUE LIMIT 100;"
```

---

## Safe Shutdown

### 12. Graceful Shutdown Sequence

```bash
# Stop producer first (no new data)
docker compose stop producer-coinbase

# Wait 30 seconds for in-flight batches to complete
sleep 30

# Stop Spark processor gracefully
docker compose stop spark-master

# Stop remaining services
docker compose down

# Preserve volumes and data
# Data in PostgreSQL, HDFS, Kafka will persist for next startup
```

---

## Restart Procedures

### 13. Full Restart (Preserves Data)

```bash
# Clean restart without losing state
docker compose up -d

# Processor will resume from last checkpoint
# Verify data continuity:
docker compose exec postgres psql -U crypto_user -d cryptopipeline -c \
  "SELECT COUNT(*) FROM gold.gold_crypto_ohlcv WHERE timestamp >= NOW() - INTERVAL '1 hour';"
```

### 14. Complete Reset (WARNING: Clears Data)

```bash
# Only run this if you need to reset all state
docker compose down -v

# Rebuild and start fresh
docker compose up -d

# Initialize database schema
docker compose exec postgres psql -U crypto_user -d cryptopipeline -f sql/init.sql
```

---

## Performance Tuning

### 15. Check Spark Memory Usage

```bash
# Memory pressure on Spark executor
docker stats spark-master | grep -E 'NAME|spark'

# Monitor HDFS checkpoint growth
docker compose exec namenode hdfs dfs -du -sh /spark/checkpoints/
```

### 16. Review Streaming Metrics

```bash
# Processing latency and throughput
docker compose exec postgres psql -U crypto_user -d cryptopipeline -c \
  "SELECT 
    COUNT(*) as sample_count,
    ROUND(AVG(latency_ms)::NUMERIC, 2) as avg_latency_ms,
    ROUND(MAX(latency_ms)::NUMERIC, 2) as max_latency_ms
   FROM gold.pipeline_latency_samples
   WHERE measured_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour';"
```

---

## Production SLAs

| Metric | Target | Command to Check |
|--------|--------|---|
| **Data Freshness** | < 5 seconds | `SELECT CURRENT_TIMESTAMP - MAX(process_timestamp) FROM gold.gold_crypto_ohlcv;` |
| **P99 Latency** | < 2 seconds | `SELECT PERCENTILE_CONT(0.99) ... FROM gold.pipeline_latency_samples;` |
| **HDFS Capacity** | > 1 TB available | `hdfs dfsadmin -report \| grep "DFS Remaining"` |
| **Kafka Lag** | 0 (real-time) | `kafka-consumer-groups --bootstrap-server ... --describe` |
| **Gold Row Growth** | 60+ rows/minute | Monitor gold table count every 60s |

---

## Quick Reference: One-Liner Commands

```bash
# Start production
docker compose up -d

# Check data is flowing (should be > 0)
docker compose exec postgres psql -U crypto_user -d cryptopipeline -c "SELECT COUNT(*) FROM gold.gold_crypto_ohlcv;"

# Trigger archive
docker compose exec airflow-webserver airflow dags trigger archive_to_hdfs_dag

# Dashboard URL
echo "Open http://localhost:8501"

# Stop all
docker compose down
```

---

**For production support, refer to the RUN_COMMANDS.md guide in the repository.**