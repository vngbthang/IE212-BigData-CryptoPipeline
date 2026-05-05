# Run Commands — Crypto Pipeline

This document contains copy-paste commands to run the Crypto Pipeline services and common troubleshooting checks. Do not modify code — only run commands as needed.

---

## 1. Start full stack (Docker Compose)

From repository root:

```bash
# Start all services in background
docker compose up -d

# View status
docker compose ps
```

## 2. Check containers & logs

```bash
# List running containers
docker ps --format "table {{.Names}}\t{{.Status}}"

# Follow logs for all services (CTRL+C to stop)
docker compose logs --follow

# Follow a single service logs
docker logs --follow crypto-producer-coinbase
docker logs --follow spark-master
docker logs --follow postgres
```

## 3. Kafka quick checks

```bash
# Describe topic
docker exec kafka-0 bash -lc "kafka-topics --bootstrap-server localhost:9092 --describe --topic crypto_ticks"

# Consume a few messages (non-continuous)
docker exec kafka-0 bash -lc "kafka-console-consumer --bootstrap-server localhost:9092 --topic crypto_ticks --max-messages 5 --timeout-ms 5000"
```

## 4. Spark stream processor

Start (if you need to run manually inside `spark-master` container):

```bash
# Start in background on spark-master container
docker exec -d spark-master bash -lc "nohup /opt/spark/bin/spark-submit --master local[*] --deploy-mode client --packages \
  org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3,org.apache.spark:spark-avro_2.12:3.5.1 \
  /app/src/processor/stream_processor.py > /tmp/stream_processor_local.log 2>&1 &"

# Tail logs
docker exec spark-master bash -lc "tail -n 200 /tmp/stream_processor_local.log"
```

## 5. Producer (Coinbase)

If running via Docker Compose the container is `crypto-producer-coinbase`.

```bash
# Follow producer logs
docker logs --follow crypto-producer-coinbase

# Or run locally (debug)
python3 src/ingestion/coinbase_producer.py
```

## 6. Dashboard (Streamlit)

```bash
# If container exists via compose
docker compose up -d streamlit
docker logs --follow streamlit

# Or run locally
pip install -r requirements.txt
streamlit run src/dashboard/app.py
# Open http://localhost:8501
```

## 7. Postgres checks

```bash
# Enter psql in postgres container
docker exec -it postgres psql -U crypto_user -d cryptopipeline

# Example queries
SELECT MAX(process_timestamp) FROM gold.gold_crypto_ohlcv;
SELECT COUNT(*) FROM gold.pipeline_latency_samples WHERE measured_at >= CURRENT_TIMESTAMP - INTERVAL '5 minutes';
```

## 8. HDFS checks (if used)

```bash
docker exec namenode bash -c "hdfs dfsadmin -report"
docker exec namenode bash -c "hdfs fsck / -files -blocks | head -n 200"
docker exec namenode bash -c "hdfs dfsadmin -safemode get"
```

## 9. Tests and smoke scripts

```bash
# Run project test scripts
./run_tests.sh
bash smoke_tests_merged.sh
python3 automated_tests.py
```

## 10. Checkpoint cleanup (CAUTION)

Only remove checkpoints when you understand the implications (may trigger re-processing or duplicates).

```bash
# Remove checkpoint dir on spark-master
docker exec spark-master bash -lc "rm -rf /tmp/spark_checkpoints/crypto_stream/ohlcv"
```

## 11. Git: commit & push new documentation

```bash
# Show current branch
git branch --show-current

# Add, commit and push
git add docs/RUN_COMMANDS.md
git commit -m "docs: add RUN_COMMANDS.md - run & debug commands"
# Push to current branch
git push origin $(git branch --show-current)
```

---

If you want, I can: create a smaller quick-check script, or generate a checklist `scripts/start.sh` (safe commands only).