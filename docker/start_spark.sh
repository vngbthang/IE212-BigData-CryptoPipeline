#!/bin/bash
# ============================================================================
# Spark Streaming Start Script with Time-Travel Resilience
# ============================================================================
# Handles cold-start scenarios after multi-day shutdown:
# 1. Checks if checkpoint is older than 24 hours → wipe and start fresh
# 2. Uses "latest" offsets on fresh start to avoid DataLoss errors
# 3. Falls back gracefully when Kafka offsets are gone
# ============================================================================

set -e

CHECKPOINT_BASE="/tmp/spark_checkpoints/crypto_stream"
STALE_THRESHOLD_HOURS=24
SPARK_SPARK_CHECKPOINT="${CHECKPOINT_BASE}/raw_ticks"
SPARK_OHLCV_CHECKPOINT="${CHECKPOINT_BASE}/ohlcv/postgres"

echo "============================================================"
echo "Spark Streaming Start with Time-Travel Resilience Protocol"
echo "============================================================"
echo "Checkpoint base: ${CHECKPOINT_BASE}"
echo "Stale threshold: ${STALE_THRESHOLD_HOURS} hours"

# Function to check if a checkpoint directory is stale (>24h old)
is_stale_checkpoint() {
    local checkpoint_dir="$1"
    if [ ! -d "$checkpoint_dir" ]; then
        echo "  [INFO] No checkpoint found at: $checkpoint_dir"
        return 0  # No checkpoint = stale (fresh start needed)
    fi

    # Find the most recent file modification in the checkpoint directory
    local last_modified=$(find "$checkpoint_dir" -type f -name "*.delta" -o -name "*.checkpoint" 2>/dev/null | xargs -I{} stat -c '%Y' {} 2>/dev/null | sort -n | tail -1)

    if [ -z "$last_modified" ]; then
        echo "  [INFO] Checkpoint exists but empty/corrupted: $checkpoint_dir"
        return 0  # Corrupted = stale
    fi

    local now=$(date +%s)
    local age_hours=$(( (now - last_modified) / 3600 ))

    echo "  [INFO] Checkpoint age: ${age_hours} hours"

    if [ $age_hours -gt $STALE_THRESHOLD_HOURS ]; then
        echo "  [WARN] Checkpoint is stale (>${STALE_THRESHOLD_HOURS}h)"
        return 0  # Stale
    fi
    return 1  # Not stale
}

# Function to wipe a checkpoint directory
wipe_checkpoint() {
    local checkpoint_dir="$1"
    if [ -d "$checkpoint_dir" ]; then
        echo "  [WARN] Wiping stale checkpoint: $checkpoint_dir"
        rm -rf "$checkpoint_dir"
        echo "  [OK] Checkpoint wiped"
    fi
}

# Determine starting offsets based on checkpoint freshness
USE_LATEST_OFFSETS="false"

echo ""
echo "Checking raw_ticks checkpoint..."
if is_stale_checkpoint "$SPARK_SPARK_CHECKPOINT"; then
    USE_LATEST_OFFSETS="true"
    wipe_checkpoint "$SPARK_SPARK_CHECKPOINT"
fi

echo ""
echo "Checking OHLCV checkpoint..."
if is_stale_checkpoint "$SPARK_OHLCV_CHECKPOINT"; then
    USE_LATEST_OFFSETS="true"
    wipe_checkpoint "$SPARK_OHLCV_CHECKPOINT"
fi

echo ""
if [ "$USE_LATEST_OFFSETS" == "true" ]; then
    echo "[RESILIENCE] Fresh start: Using startingOffsets=latest"
    STARTING_OFFSETS="latest"
else
    echo "[RESILIENCE] Resuming from checkpoint: Using startingOffsets=earliest"
    STARTING_OFFSETS="earliest"
fi

echo "============================================================"
echo ""

# Execute spark-submit with appropriate starting offsets
# Pass starting offsets via environment variable
export SPARK_STARTING_OFFSETS="${STARTING_OFFSETS}"

exec /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-avro_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
    --conf spark.executor.memory=1g \
    --conf spark.executor.cores=1 \
    --conf spark.task.cpus=1 \
    --conf spark.sql.streaming.stopGracefullyOnShutdown=true \
    --conf spark.task.maxFailures=8 \
    --conf spark.stage.maxConsecutiveAttempts=8 \
    --conf spark.network.timeout=300s \
    --conf spark.executor.heartbeatInterval=60s \
    --conf spark.deploy.maxExecutorRetries=10 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    "$@"
