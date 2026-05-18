#!/bin/bash
# ============================================================================
# Kafka Topic Init Script
# Runs before Kafka brokers are fully ready to create required topics
# with proper retention and replication settings.
# ============================================================================
set -e

KAFKA_BROKER="${KAFKA_INIT_BROKER:-kafka-0:9092}"
TOPIC="${KAFKA_INIT_TOPIC:-crypto_ticks}"
DLQ_TOPIC="${KAFKA_INIT_DLQ_TOPIC:-crypto_ticks_dead_letter}"
RETENTION_HOURS="${KAFKA_INIT_RETENTION_HOURS:-168}"  # 7 days = 168 hours

echo "============================================================"
echo "Kafka Topic Initializer"
echo "============================================================"
echo "Broker: ${KAFKA_BROKER}"
echo "Topic: ${TOPIC}"
echo "DLQ Topic: ${DLQ_TOPIC}"
echo "Retention: ${RETENTION_HOURS} hours"

# Wait for Kafka to be ready
echo "[INIT] Waiting for Kafka broker ${KAFKA_BROKER} to be ready..."
for i in $(seq 1 30); do
    if kafka-topics --bootstrap-server "${KAFKA_BROKER}" --list >/dev/null 2>&1; then
        echo "[INIT] Kafka broker is ready!"
        break
    fi
    echo "[INIT] Waiting... ($i/30)"
    sleep 2
done

# Create main topic
echo "[INIT] Creating topic '${TOPIC}'..."
kafka-topics --bootstrap-server "${KAFKA_BROKER}" \
    --create \
    --if-not-exists \
    --topic "${TOPIC}" \
    --partitions 6 \
    --replication-factor 3 \
    --config retention.ms=$((RETENTION_HOURS * 3600 * 1000)) \
    --config min.insync.replicas=2 \
    --config cleanup.policy=delete

echo "[INIT] Topic '${TOPIC}' created/verified."

# Create DLQ topic
echo "[INIT] Creating DLQ topic '${DLQ_TOPIC}'..."
kafka-topics --bootstrap-server "${KAFKA_BROKER}" \
    --create \
    --if-not-exists \
    --topic "${DLQ_TOPIC}" \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=$((7 * 24 * 3600 * 1000))

echo "[INIT] DLQ topic '${DLQ_TOPIC}' created/verified."

# Verify topics
echo "[INIT] Current topics:"
kafka-topics --bootstrap-server "${KAFKA_BROKER}" --list

echo "============================================================"
echo "[INIT] Done! Topics ready."
echo "============================================================"
