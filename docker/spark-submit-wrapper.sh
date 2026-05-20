#!/bin/bash
set -e

echo "=============================================="
echo "CryptoPipeline Spark Submit Wrapper"
echo "=============================================="

# Kafka settings
KAFKA_BROKERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka-1:9092,kafka-2:9092,kafka-3:9092}"
KAFKA_TOPIC="${KAFKA_TOPIC:-crypto_ticks}"

# MinIO / S3
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-admin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-password}"

# Spark / Iceberg
CHECKPOINT="s3a://crypto-lake/checkpoints/gold_ohlcv"
WAREHOUSE="s3a://crypto-lake/warehouse"

echo "Kafka: $KAFKA_BROKERS / topic=$KAFKA_TOPIC"
echo "MinIO: $MINIO_ENDPOINT"

# Install Python deps
pip install boto3 -q

echo "[WRAPPER] Waiting for Spark master..."
for i in $(seq 1 60); do
    if curl -sf "http://spark-master:8080" >/dev/null 2>&1; then
        echo "[WRAPPER] Spark master is ready."
        break
    fi
    echo "[WRAPPER] Waiting for Spark master... ($i/60)"
    sleep 2
done

echo "[WRAPPER] Starting spark-submit..."
exec /opt/spark/bin/spark-submit \
    --master "spark://spark-master:7077" \
    --deploy-mode client \
    --name "CryptoPipeline-Iceberg" \
    --conf spark.driver.memory=1g \
    --conf spark.executor.memory=2g \
    --conf spark.executor.cores=2 \
    --conf spark.task.cpus=1 \
    --conf spark.sql.shuffle.partitions=8 \
    --conf spark.default.parallelism=8 \
    --conf spark.streaming.stopGracefullyOnShutdown=true \
    --conf spark.task.maxFailures=8 \
    --conf spark.streaming.backpressure.enabled=true \
    --conf spark.driver.host=spark-app \
    --conf spark.driver.bindAddress=0.0.0.0 \
    --conf spark.network.timeout=300s \
    --conf spark.executor.heartbeatInterval=60s \
    --conf spark.sql.streaming.checkpointLocation="${CHECKPOINT}" \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions \
    --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
    --conf spark.sql.catalog.nessie.uri=http://nessie:19120/api/v1 \
    --conf spark.sql.catalog.nessie.ref=main \
    --conf spark.sql.catalog.nessie.warehouse="${WAREHOUSE}" \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.endpoint="${MINIO_ENDPOINT}" \
    --conf spark.hadoop.fs.s3a.access.key="${MINIO_ACCESS_KEY}" \
    --conf spark.hadoop.fs.s3a.secret.key="${MINIO_SECRET_KEY}" \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.sql.adaptive.enabled=true \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    /app/src/processor/stream_processor.py
