#!/bin/bash
# ============================================================================
# Spark Application Startup Script
# Runs stream_processor.py via spark-submit
# Kafka → Spark Structured Streaming → Iceberg on MinIO
# ============================================================================

SPARK_MASTER="${SPARK_MASTER:-spark://spark-master:7077}"
KAFKA_BROKERS="${KAFKA_BROKERS:-kafka-1:9092,kafka-2:9092,kafka-3:9092}"
KAFKA_TOPIC="${KAFKA_TOPIC:-crypto_ticks}"
KAFKA_DLQ_TOPIC="${KAFKA_DLQ_TOPIC:-crypto_ticks_dead_letter}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin123}"
HIVE_METASTORE_URI="${HIVE_METASTORE_URI:-thrift://hive-metastore:9083}"
WAREHOUSE="${SPARK_ICEBERG_WAREHOUSE:-s3a://crypto-lake/}"
CHECKPOINT_DIR="${SPARK_ICEBERG_WAREHOUSE:-s3a://crypto-lake/}spark/checkpoints"
STARTING_OFFSETS="${SPARK_STARTING_OFFSETS:-earliest}"

echo "============================================================"
echo "CryptoPipeline Spark - Iceberg on MinIO"
echo "============================================================"
echo "Master: ${SPARK_MASTER}"
echo "Kafka: ${KAFKA_BROKERS}"
echo "Topic: ${KAFKA_TOPIC}"
echo "MinIO: ${MINIO_ENDPOINT}"
echo "Warehouse: ${WAREHOUSE}"
echo "Hive Metastore: ${HIVE_METASTORE_URI}"
echo "============================================================"

# Wait for Spark master
echo "[STARTUP] Waiting for Spark master..."
for i in $(seq 1 60); do
    if curl -sf "http://spark-master:8080" >/dev/null 2>&1; then
        echo "[STARTUP] Spark master ready!"
        break
    fi
    if [ "$i" -eq 60 ]; then
        echo "[STARTUP] ERROR: Spark master not ready after 120s"
        exit 1
    fi
    sleep 2
done

# Wait for Hive Metastore
echo "[STARTUP] Waiting for Hive Metastore..."
for i in $(seq 1 60); do
    if bash -c "exec 3<>/dev/tcp/hive-metastore/9083" 2>/dev/null; then
        echo "[STARTUP] Hive Metastore ready!"
        break
    fi
    if [ "$i" -eq 60 ]; then
        echo "[STARTUP] ERROR: Hive Metastore not ready after 120s"
        exit 1
    fi
    sleep 2
done

# Wait for MinIO
echo "[STARTUP] Waiting for MinIO..."
for i in $(seq 1 30); do
    if curl -sf "http://minio:9000/minio/health/live" >/dev/null 2>&1; then
        echo "[STARTUP] MinIO ready!"
        break
    fi
    sleep 2
done

echo "[STARTUP] Starting spark-submit..."

iteration=0
while true; do
    iteration=$((iteration + 1))
    echo "[STARTUP] === Iteration $iteration ==="

    /opt/spark/bin/spark-submit \
        --master "${SPARK_MASTER}" \
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
        --conf spark.executor.failuresValidityInterval=1h \
        --conf spark.streaming.kafka.maxRetries=3 \
        --conf spark.streaming.backpressure.enabled=true \
        --conf spark.streaming.kafka.maxRatePerPartition=200 \
        --conf spark.executor.extraJavaOptions="-XX:+UseG1GC -XX:MaxGCPauseMillis=100" \
        --conf spark.driver.host=spark-app \
        --conf spark.driver.bindAddress=0.0.0.0 \
        --conf spark.network.timeout=300s \
        --conf spark.executor.heartbeatInterval=60s \
        --conf spark.rpc.message.maxSize=256 \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.endpoint="${MINIO_ENDPOINT}" \
        --conf spark.hadoop.fs.s3a.access.key="${MINIO_ACCESS_KEY}" \
        --conf spark.hadoop.fs.s3a.secret.key="${MINIO_SECRET_KEY}" \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
        --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
        --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.local.type=hive \
        --conf spark.sql.catalog.local.uri="${HIVE_METASTORE_URI}" \
        --conf spark.sql.catalog.local.warehouse="${WAREHOUSE}" \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-avro_2.12:3.5.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.3 \
        /app/src/processor/stream_processor.py \
        --kafka-brokers "${KAFKA_BROKERS}" \
        --kafka-topic "${KAFKA_TOPIC}" \
        --kafka-dlq-topic "${KAFKA_DLQ_TOPIC}" \
        --starting-offsets "${STARTING_OFFSETS}" \
        --minio-endpoint "${MINIO_ENDPOINT}" \
        --minio-access-key "${MINIO_ACCESS_KEY}" \
        --minio-secret-key "${MINIO_SECRET_KEY}" \
        --hive-metastore-uri "${HIVE_METASTORE_URI}" \
        --iceberg-warehouse "${WAREHOUSE}" \
        --checkpoint-dir "${CHECKPOINT_DIR}"

    exit_code=$?
    echo "[STARTUP] spark-submit exited with code $exit_code"

    if [ "$exit_code" -eq 0 ]; then
        echo "[STARTUP] spark-submit stopped normally. Exiting."
        exit 0
    fi

    echo "[STARTUP] spark-submit crashed. Restarting in 5s..."
    sleep 5
done
