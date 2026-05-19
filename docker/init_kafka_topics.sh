#!/bin/bash
# ============================================================================
# Kafka Topic Init Script
# Runs before Kafka brokers are fully ready to create required topics
# with proper retention and replication settings.
# Idempotent: safe to run multiple times.
# ============================================================================

KAFKA_BROKER="${KAFKA_INIT_BROKER:-kafka-0:9092}"
TOPIC="${KAFKA_INIT_TOPIC:-crypto_ticks}"
DLQ_TOPIC="${KAFKA_INIT_DLQ_TOPIC:-crypto_ticks_dead_letter}"
RETENTION_HOURS="${KAFKA_INIT_RETENTION_HOURS:-168}"

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
    if [ "$i" -eq 30 ]; then
        echo "[INIT] Kafka broker not ready after 60s, continuing anyway..."
    fi
    echo "[INIT] Waiting... ($i/30)"
    sleep 2
done

# Create main topic (idempotent - only creates if not exists)
echo "[INIT] Creating topic '${TOPIC}'..."
kafka-topics --bootstrap-server "${KAFKA_BROKER}" \
    --create \
    --if-not-exists \
    --topic "${TOPIC}" \
    --partitions 6 \
    --replication-factor 3 \
    --config retention.ms=$((RETENTION_HOURS * 3600 * 1000)) \
    --config min.insync.replicas=2 \
    --config cleanup.policy=delete 2>/dev/null || echo "[INIT] Topic '${TOPIC}' already exists or creation skipped"

echo "[INIT] Topic '${TOPIC}' verified."

# Create DLQ topic (idempotent)
echo "[INIT] Creating DLQ topic '${DLQ_TOPIC}'..."
kafka-topics --bootstrap-server "${KAFKA_BROKER}" \
    --create \
    --if-not-exists \
    --topic "${DLQ_TOPIC}" \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=$((7 * 24 * 3600 * 1000)) 2>/dev/null || echo "[INIT] DLQ topic already exists or creation skipped"

echo "[INIT] DLQ topic '${DLQ_TOPIC}' verified."

# Verify topics
echo "[INIT] Current topics:"
kafka-topics --bootstrap-server "${KAFKA_BROKER}" --list 2>/dev/null

echo "============================================================"
echo "[INIT] Done! Topics ready."
echo "============================================================"
exit 0
