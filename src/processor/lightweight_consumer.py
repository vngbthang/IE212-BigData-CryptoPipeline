#!/usr/bin/env python3
"""
Lightweight Real-time Kafka Consumer - No Spark
================================================
Directly reads Avro-encoded ticks from Kafka → PostgreSQL (silver.raw_ticks)
PostgreSQL TRIGGER automatically aggregates to gold.gold_crypto_ohlcv

Target: <30s end-to-end latency on laptop hardware

RESILIENCE FEATURES:
1. Auto-reconnect with exponential backoff (no data loss on disconnect)
2. Graceful shutdown with flush-and-commit
3. Automatic partition management (current + 7 days ahead)
4. Topic existence check on startup
"""

import io
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import avro.schema
import avro.io

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "psycopg2-binary"])
    import psycopg2
    from psycopg2.extras import execute_values

try:
    from confluent_kafka import Consumer, KafkaError, KafkaException
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "confluent-kafka"])
    from confluent_kafka import Consumer, KafkaError, KafkaException

# ============================================================================
# CONFIGURATION
# ============================================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-0:9092,kafka-1:9092,kafka-2:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto_ticks")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "lightweight-consumer-v1")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
BATCH_FLUSH_INTERVAL_SEC = float(os.getenv("BATCH_FLUSH_INTERVAL_SEC", "0.5"))
# Resilience: auto-reconnect settings
RECONNECT_BASE_DELAY_SEC = float(os.getenv("RECONNECT_BASE_DELAY_SEC", "1.0"))
RECONNECT_MAX_DELAY_SEC = float(os.getenv("RECONNECT_MAX_DELAY_SEC", "60.0"))
RECONNECT_MAX_RETRIES = int(os.getenv("RECONNECT_MAX_RETRIES", "0"))  # 0 = infinite
# Resilience: startup topic check
TOPIC_CHECK_RETRIES = int(os.getenv("TOPIC_CHECK_RETRIES", "30"))
TOPIC_CHECK_INTERVAL_SEC = float(os.getenv("TOPIC_CHECK_INTERVAL_SEC", "2.0"))

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "cryptopipeline")
DB_USER = os.getenv("DB_USER", "crypto_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "crypto_password")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
APP_NAME = "LightweightCryptoConsumer"

# ============================================================================
# AVRO SCHEMA (must match coinbase_producer.py exactly)
# ============================================================================

AVRO_SCHEMA_JSON = '''
{
  "namespace": "com.cryptopipeline.bronze",
  "type": "record",
  "name": "CryptoTickEvent",
  "fields": [
    {"name": "timestamp", "type": "long"},
    {"name": "product_id", "type": "string"},
    {"name": "exchange", "type": "string", "default": "coinbase"},
    {"name": "price", "type": "double"},
    {"name": "size", "type": "double"},
    {"name": "bid", "type": ["null", "double"], "default": null},
    {"name": "ask", "type": ["null", "double"], "default": null},
    {"name": "side", "type": {"type": "enum", "name": "TradeSide", "symbols": ["buy", "sell", "unknown"]}, "default": "unknown"},
    {"name": "sequence", "type": "long"},
    {"name": "ingestion_timestamp", "type": "long"}
  ]
}
'''

# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=f"%(asctime)s [{APP_NAME}] %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


# ============================================================================
# AVRO DECODER
# ============================================================================

def decode_avro(raw_bytes: bytes) -> Optional[Dict[str, Any]]:
    """Decode Avro binary message to Python dict."""
    try:
        schema = avro.schema.parse(AVRO_SCHEMA_JSON)
        reader = avro.io.DatumReader(schema)
        bytes_io = io.BytesIO(raw_bytes)
        decoder = avro.io.BinaryDecoder(bytes_io)
        return reader.read(decoder)
    except Exception:
        return None


# ============================================================================
# PARTITION MANAGEMENT
# ============================================================================

def _is_partition_covered(cursor, target_date: datetime) -> bool:
    """Check if a date is covered by any existing partition."""
    date_str = target_date.strftime('%Y%m%d')
    cursor.execute("""
        SELECT COUNT(*)
        FROM pg_class c
        JOIN pg_inherits i ON c.oid = i.inhrelid
        JOIN pg_class parent ON parent.oid = i.inhparent
        WHERE parent.relname = 'gold_crypto_ohlcv'
          AND parent.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'gold')
          AND c.relname LIKE %s
    """, (f'%{date_str}',))
    return cursor.fetchone()[0] > 0


def _ensure_future_partitions(cursor, days_ahead: int = 7) -> int:
    """Ensure partitions exist for today + N days ahead. Returns number of partitions created."""
    logger = logging.getLogger(__name__)
    created = 0
    today = datetime.now(timezone.utc).date()
    for i in range(days_ahead + 1):
        target_date = today + timedelta(days=i)
        if _is_partition_covered(cursor, datetime.combine(target_date, datetime.min.time())):
            continue
        partition_name = f"gold_crypto_ohlcv_{target_date.strftime('%Y%m%d')}"
        week_start = target_date - timedelta(days=target_date.weekday())
        week_end = week_start + timedelta(days=7)
        try:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS gold.{partition_name}
                PARTITION OF gold.gold_crypto_ohlcv
                FOR VALUES FROM ('{week_start}')
                             TO ('{week_end}')
            """)
            logger.info(f"[PARTITION_CREATED] {partition_name} ({week_start} to {week_end})")
            created += 1
        except psycopg2.errors.DuplicateTable:
            logger.debug(f"[PARTITION_EXISTS] {partition_name}")
        except Exception as part_err:
            if 'overlaps with' in str(part_err) or 'already exists' in str(part_err).lower():
                logger.debug(f"[PARTITION_SKIP] {partition_name}: already covered")
            else:
                logger.warning(f"[PARTITION_ERROR] {partition_name}: {part_err}")
    return created


# ============================================================================
# DATABASE SCHEMA SETUP
# ============================================================================

def setup_database():
    """Create required tables, OHLCV trigger function, and dynamic partitions."""
    conn = psycopg2.connect(
        host=DB_HOST,
        port=int(DB_PORT),
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    conn.autocommit = True
    cursor = conn.cursor()

    logger.info("[DB_SETUP] Creating silver.raw_ticks table...")

    # silver.raw_ticks - individual ticks for true real-time display
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS silver.raw_ticks (
            id BIGSERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL,
            exchange VARCHAR(20),
            price NUMERIC(20, 8) NOT NULL,
            size NUMERIC(20, 8),
            bid NUMERIC(20, 8),
            ask NUMERIC(20, 8),
            side VARCHAR(10),
            tick_timestamp TIMESTAMPTZ NOT NULL,
            ingest_timestamp TIMESTAMPTZ NOT NULL,
            process_timestamp TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            source_partition INTEGER,
            source_offset BIGINT
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_raw_ticks_symbol_time
        ON silver.raw_ticks (symbol, tick_timestamp DESC)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_raw_ticks_time
        ON silver.raw_ticks (tick_timestamp DESC)
    """)

    # gold.gold_crypto_ohlcv - OHLCV candles (populated by trigger)
    # IMPORTANT: Must use PARTITION BY RANGE to match init.sql schema.
    # If a non-partitioned table exists (from a previous broken run), we need to
    # detect it and warn. The docker-entrypoint-initdb.d (init.sql) creates the
    # partitioned table on first start; this block ensures idempotency.
    cursor.execute("""
        SELECT COUNT(*) FROM pg_tables
        WHERE schemaname = 'gold' AND tablename = 'gold_crypto_ohlcv'
    """)
    table_exists = cursor.fetchone()[0] > 0

    if not table_exists:
        cursor.execute("""
            CREATE TABLE gold.gold_crypto_ohlcv (
                id BIGSERIAL,
                timestamp TIMESTAMPTZ NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                exchange VARCHAR(50) DEFAULT 'coinbase',
                open NUMERIC(20, 8),
                high NUMERIC(20, 8),
                low NUMERIC(20, 8),
                close NUMERIC(20, 8),
                volume NUMERIC(20, 8),
                ingest_timestamp TIMESTAMPTZ,
                process_timestamp TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                display_timestamp TIMESTAMPTZ,
                record_count INTEGER DEFAULT 1,
                log_returns DOUBLE PRECISION,
                volatility_30m DOUBLE PRECISION,
                z_score DOUBLE PRECISION,
                is_outlier BOOLEAN DEFAULT FALSE,
                error_flag BOOLEAN DEFAULT FALSE,
                error_messages TEXT,
                source_partition INTEGER,
                source_offset BIGINT,
                updated_at TIMESTAMPTZ DEFAULT clock_timestamp(),
                PRIMARY KEY (timestamp, symbol),
                UNIQUE (timestamp, symbol)
            ) PARTITION BY RANGE (timestamp)
        """)
        logger.info("[DB_SETUP] Created partitioned gold_crypto_ohlcv")
    else:
        # Table exists — check if it's partitioned (not from a broken previous run)
        cursor.execute("""
            SELECT relkind FROM pg_class
            WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'gold')
              AND relname = 'gold_crypto_ohlcv'
        """)
        row = cursor.fetchone()
        if row and row[0] == 'p':
            logger.info("[DB_SETUP] Partitioned gold_crypto_ohlcv already exists")
        else:
            logger.error("[DB_SETUP] FATAL: gold_crypto_ohlcv exists but is NOT partitioned!")
            logger.error("[DB_SETUP] This table was likely created by a broken previous run.")
            logger.error("[DB_SETUP] Run: docker-compose down -v  (to wipe volumes) then docker-compose up -d")
            raise RuntimeError(
                "gold.gold_crypto_ohlcv exists but is not partitioned. "
                "This prevents the trigger from writing correctly. "
                "Run 'docker-compose down -v' to wipe volumes and start fresh."
            )
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_ohlcv_symbol_time
        ON gold.gold_crypto_ohlcv (symbol, timestamp DESC)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_ohlcv_timestamp
        ON gold.gold_crypto_ohlcv (timestamp DESC)
    """)

    # OHLCV aggregation trigger function - only create if not exists
    logger.info("[DB_SETUP] Creating OHLCV aggregation trigger...")
    cursor.execute("""
        SELECT prosrc FROM pg_proc WHERE proname = 'raw_tick_ohlcv_trigger'
    """)
    existing = cursor.fetchone()

    if not existing or 'ON CONFLICT' in (existing[0] or ''):
        # Recreate with correct version (no ON CONFLICT in INSERT)
        cursor.execute("""
            CREATE OR REPLACE FUNCTION silver.raw_tick_ohlcv_trigger()
            RETURNS TRIGGER AS $$
            DECLARE
                v_bucket TIMESTAMPTZ;
            BEGIN
                v_bucket := date_trunc('minute', NEW.tick_timestamp)::TIMESTAMPTZ;

                UPDATE gold.gold_crypto_ohlcv SET
                    high = GREATEST(high, NEW.price),
                    low = LEAST(low, NEW.price),
                    close = NEW.price,
                    volume = volume + COALESCE(NEW.size, 0),
                    record_count = record_count + 1,
                    process_timestamp = clock_timestamp(),
                    display_timestamp = clock_timestamp(),
                    updated_at = clock_timestamp(),
                    source_partition = NEW.source_partition,
                    source_offset = NEW.source_offset
                WHERE timestamp = v_bucket AND symbol = NEW.symbol;

                IF NOT FOUND THEN
                    INSERT INTO gold.gold_crypto_ohlcv
                        (timestamp, symbol, exchange, open, high, low, close, volume,
                         ingest_timestamp, process_timestamp, display_timestamp, record_count,
                         source_partition, source_offset)
                    VALUES (
                        v_bucket, NEW.symbol, COALESCE(NEW.exchange, 'coinbase'),
                        NEW.price, NEW.price, NEW.price, NEW.price, COALESCE(NEW.size, 0),
                        NEW.ingest_timestamp, clock_timestamp(), clock_timestamp(), 1,
                        NEW.source_partition, NEW.source_offset
                    );
                END IF;

                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        """)
        cursor.execute("DROP TRIGGER IF EXISTS trg_raw_tick_ohlcv ON silver.raw_ticks")
        cursor.execute("""
            CREATE TRIGGER trg_raw_tick_ohlcv
            AFTER INSERT ON silver.raw_ticks
            FOR EACH ROW EXECUTE FUNCTION silver.raw_tick_ohlcv_trigger()
        """)
        logger.info("[DB_SETUP] Trigger installed")
    else:
        logger.info("[DB_SETUP] Trigger already correct, skipping")

    # Heartbeat table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gold.pipeline_heartbeat (
            id SMALLINT PRIMARY KEY,
            updated_at TIMESTAMPTZ NOT NULL
        )
    """)
    cursor.execute("""
        INSERT INTO gold.pipeline_heartbeat (id, updated_at) VALUES (1, clock_timestamp())
        ON CONFLICT (id) DO UPDATE SET updated_at = clock_timestamp()
    """)

    # Latency samples
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gold.pipeline_latency_samples (
            measured_at TIMESTAMPTZ PRIMARY KEY,
            latency_ms DOUBLE PRECISION NOT NULL
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_latency_samples_time
        ON gold.pipeline_latency_samples (measured_at DESC)
    """)

    # Dynamic partitions: ensure today + 7 days ahead exist
    cursor.close()
    conn.close()
    logger.info("[DB_SETUP] Done. Setting up dynamic partitions...")
    _setup_partitions()
    logger.info("[DB_SETUP] All done.")


def _setup_partitions():
    """Setup partitions on startup (called separately for reuse)."""
    conn = psycopg2.connect(
        host=DB_HOST, port=int(DB_PORT), database=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )
    conn.autocommit = True
    cursor = conn.cursor()
    created = _ensure_future_partitions(cursor, days_ahead=7)
    cursor.close()
    conn.close()
    if created > 0:
        logger.info(f"[DB_SETUP] Created {created} new partition(s)")
    else:
        logger.info("[DB_SETUP] All partitions up-to-date")


# ============================================================================
# ML FEATURES COMPUTER (runs as background thread)
# ============================================================================
# Computes: log_returns, volatility_30m, z_score, is_outlier
# Safe: only reads gold.gold_crypto_ohlcv, writes computed cols back.
# Idempotent: runs every 30s and overwrites with latest values.
# ============================================================================

def _compute_ml_features() -> None:
    """Background thread: compute rolling ML features and write back to gold table."""
    logger = logging.getLogger(__name__ + ".ml_features")
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=int(DB_PORT), database=DB_NAME,
                user=DB_USER, password=DB_PASSWORD,
            )
            conn.autocommit = True
            cursor = conn.cursor()

            # Find all symbols with recent data
            cursor.execute("""
                SELECT DISTINCT symbol FROM gold.gold_crypto_ohlcv
                WHERE error_flag = FALSE
                  AND close IS NOT NULL
                ORDER BY symbol
            """)
            symbols = [row[0] for row in cursor.fetchall()]

            if not symbols:
                cursor.close()
                conn.close()
                time.sleep(30)
                continue

            for symbol in symbols:
                try:
                    # Fetch last 60 candles for rolling window
                    cursor.execute("""
                        SELECT timestamp, close
                        FROM gold.gold_crypto_ohlcv
                        WHERE symbol = %s
                          AND error_flag = FALSE
                          AND close IS NOT NULL
                          AND timestamp >= NOW() - INTERVAL '2 hours'
                        ORDER BY timestamp ASC
                    """, (symbol,))

                    rows = cursor.fetchall()
                    if len(rows) < 2:
                        continue

                    timestamps = [r[0] for r in rows]
                    closes = [float(r[1]) for r in rows]

                    # Compute log_returns: ln(close_t / close_t-1)
                    log_returns = []
                    for i in range(len(closes)):
                        if i == 0 or closes[i - 1] <= 0 or closes[i] <= 0:
                            log_returns.append(None)
                        else:
                            import math
                            log_returns.append(math.log(closes[i] / closes[i - 1]))

                    # Compute volatility_30m: rolling 30-min std dev of log_returns
                    volatility = []
                    for i in range(len(log_returns)):
                        window = log_returns[max(0, i - 29):i + 1]
                        valid = [v for v in window if v is not None]
                        if len(valid) >= 2:
                            mean = sum(valid) / len(valid)
                            variance = sum((v - mean) ** 2 for v in valid) / len(valid)
                            volatility.append(variance ** 0.5)
                        else:
                            volatility.append(None)

                    # Compute rolling mean and std for z_score
                    rolling_mean = []
                    rolling_std = []
                    for i in range(len(closes)):
                        window = closes[max(0, i - 29):i + 1]
                        if len(window) >= 2:
                            mean = sum(window) / len(window)
                            variance = sum((c - mean) ** 2 for c in window) / len(window)
                            rolling_mean.append(mean)
                            rolling_std.append(variance ** 0.5)
                        else:
                            rolling_mean.append(None)
                            rolling_std.append(None)

                    z_scores = []
                    for i in range(len(closes)):
                        if rolling_std[i] is not None and rolling_std[i] > 0 and rolling_mean[i] is not None:
                            z_scores.append((closes[i] - rolling_mean[i]) / rolling_std[i])
                        else:
                            z_scores.append(None)

                    # Build UPDATE rows (batch for efficiency)
                    update_rows = []
                    for i, ts in enumerate(timestamps):
                        vr = volatility[i]
                        vs = log_returns[i]
                        zs = z_scores[i]
                        is_out = abs(zs) >= 3.0 if zs is not None else False
                        update_rows.append((vs, vr, zs, is_out, ts, symbol))

                    # Batch UPDATE
                    if update_rows:
                        execute_values(
                            cursor,
                            """
                            UPDATE gold.gold_crypto_ohlcv AS tgt SET
                                log_returns = src.log_returns,
                                volatility_30m = src.volatility_30m,
                                z_score = src.z_score,
                                is_outlier = src.is_outlier
                            FROM (VALUES %s)
                            AS src (log_returns, volatility_30m, z_score, is_outlier, ts, sym)
                            WHERE tgt.timestamp = src.ts AND tgt.symbol = src.sym
                            """,
                            update_rows,
                            page_size=min(1000, len(update_rows)),
                        )

                    logger.debug(f"[ML_FEATURES] {symbol}: updated {len(update_rows)} candles")

                except Exception as e:
                    logger.warning(f"[ML_FEATURES] Error processing {symbol}: {e}")

            cursor.close()
            conn.close()

        except Exception as e:
            logger.warning(f"[ML_FEATURES] Compute loop error: {e}")

        time.sleep(30)  # Re-compute every 30 seconds


# ============================================================================
# BATCH WRITER
# ============================================================================

class BatchWriter:
    """Batches ticks in memory and flushes to PostgreSQL periodically."""

    def __init__(self):
        self.batch: List[Dict[str, Any]] = []
        self.batch_lock = threading.Lock()
        self.last_flush = time.time()
        self.total_written = 0
        self.flush_count = 0
        self._stop = False
        self._consumer = None  # For manual offset commit
        self._max_offset = -1  # Track max offset for commit

        # Start background flush thread
        self.flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self.flush_thread.start()

        # Start heartbeat thread
        self.hb_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self.hb_thread.start()

    def set_consumer(self, consumer):
        """Set the Kafka consumer for manual offset commit."""
        self._consumer = consumer

    def add(self, tick: Dict[str, Any]) -> None:
        with self.batch_lock:
            self.batch.append(tick)
            batch_size_now = len(self.batch)
            msg_offset = tick.get("source_offset", -1)
            if msg_offset > self._max_offset:
                self._max_offset = msg_offset
        if batch_size_now >= BATCH_SIZE:
            self._do_flush()

    def _should_flush(self) -> bool:
        return (time.time() - self.last_flush) >= BATCH_FLUSH_INTERVAL_SEC and self.batch

    def _do_flush(self) -> None:
        """Flush current batch to PostgreSQL."""
        with self.batch_lock:
            if not self.batch:
                return
            batch_to_flush = self.batch[:]
            self.batch.clear()
            self.last_flush = time.time()

        if not batch_to_flush:
            return

        conn = None
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=int(DB_PORT),
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
            )
            cursor = conn.cursor()

            rows = []
            now = datetime.now(timezone.utc)
            total_latency = 0.0

            for tick in batch_to_flush:
                ts_ms = tick.get("timestamp", 0)
                ingest_ms = tick.get("ingestion_timestamp", 0)
                tick_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
                ingest_dt = datetime.fromtimestamp(ingest_ms / 1000.0, tz=timezone.utc)
                latency_ms = max((now - ingest_dt).total_seconds() * 1000, 0)
                total_latency += latency_ms

                rows.append((
                    tick.get("product_id"),
                    tick.get("exchange", "coinbase"),
                    float(tick.get("price", 0)),
                    float(tick.get("size")) if tick.get("size") is not None else None,
                    float(tick.get("bid")) if tick.get("bid") is not None else None,
                    float(tick.get("ask")) if tick.get("ask") is not None else None,
                    str(tick.get("side", "unknown")),
                    tick_dt,
                    ingest_dt,
                    tick_dt,
                    tick.get("source_partition"),
                    tick.get("source_offset"),
                ))

            execute_values(
                cursor,
                """
                INSERT INTO silver.raw_ticks (
                    symbol, exchange, price, size, bid, ask, side,
                    tick_timestamp, ingest_timestamp, process_timestamp,
                    source_partition, source_offset
                ) VALUES %s
                """,
                rows,
                page_size=min(2000, len(rows))
            )

            avg_latency = total_latency / len(rows) if rows else 0
            cursor.execute("""
                INSERT INTO gold.pipeline_latency_samples (measured_at, latency_ms)
                VALUES (clock_timestamp(), %s)
                ON CONFLICT DO NOTHING
            """, (avg_latency,))
            cursor.execute("""
                DELETE FROM gold.pipeline_latency_samples
                WHERE measured_at < CURRENT_TIMESTAMP - INTERVAL '24 hours'
            """)

            conn.commit()
            self.total_written += len(batch_to_flush)
            self.flush_count += 1

            # Manual offset commit after successful DB write
            if self._consumer and self._max_offset >= 0:
                try:
                    self._consumer.commit(asynchronous=False)
                except Exception as e:
                    logger.warning(f"[COMMIT_ERROR] {e}")

            if self.flush_count % 60 == 0:
                logger.info(f"[STATS] total_written={self.total_written} flushes={self.flush_count} "
                           f"batch_size={len(batch_to_flush)} avg_latency_ms={avg_latency:.1f}")

        except Exception as e:
            logger.error(f"[FLUSH_ERROR] {type(e).__name__}: {e}", exc_info=True)
            if conn:
                conn.rollback()
            with self.batch_lock:
                self.batch = batch_to_flush + self.batch
        finally:
            if conn:
                conn.close()

    def _flush_loop(self) -> None:
        while not self._stop:
            time.sleep(0.5)
            try:
                if self._should_flush():
                    self._do_flush()
            except Exception as e:
                logger.error(f"[FLUSH_LOOP_ERROR] {e}")

    def _heartbeat_loop(self) -> None:
        while not self._stop:
            try:
                conn = psycopg2.connect(
                    host=DB_HOST,
                    port=int(DB_PORT),
                    database=DB_NAME,
                    user=DB_USER,
                    password=DB_PASSWORD,
                )
                conn.autocommit = True
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO gold.pipeline_heartbeat (id, updated_at)
                    VALUES (1, clock_timestamp())
                    ON CONFLICT (id) DO UPDATE SET updated_at = clock_timestamp()
                """)
                cursor.close()
                conn.close()
            except Exception as e:
                logger.warning(f"[HEARTBEAT_ERROR] {e}")
            time.sleep(1)

    def stop(self) -> None:
        self._stop = True
        self._do_flush()
        logger.info(f"[SHUTDOWN] Total written: {self.total_written}, flushes: {self.flush_count}")


# ============================================================================
# TOPIC EXISTENCE CHECK
# ============================================================================

def wait_for_topic(consumer: Consumer, topic: str) -> bool:
    """Wait for topic to exist with retries. Returns True if found."""
    for attempt in range(1, TOPIC_CHECK_RETRIES + 1):
        try:
            metadata = consumer.list_topics(timeout=5)
            if topic in metadata.topics:
                logger.info(f"[TOPIC] Topic '{topic}' found (attempt {attempt})")
                return True
        except Exception as e:
            logger.warning(f"[TOPIC_CHECK] Attempt {attempt}/{TOPIC_CHECK_RETRIES}: {e}")

        remaining = TOPIC_CHECK_RETRIES - attempt
        if remaining > 0:
            logger.info(f"[TOPIC_CHECK] Topic '{topic}' not found yet. "
                        f"Waiting {TOPIC_CHECK_INTERVAL_SEC}s... ({remaining} retries left)")
            time.sleep(TOPIC_CHECK_INTERVAL_SEC)

    logger.error(f"[TOPIC] Topic '{topic}' not found after {TOPIC_CHECK_RETRIES} attempts. "
                  f"Check Kafka broker and topic configuration.")
    return False


# ============================================================================
# KAFKA CONSUMER WITH AUTO-RECONNECT
# ============================================================================

def run_consumer(batch_writer: BatchWriter, starting_offsets: str = "earliest"):
    """Main consumer loop with exponential-backoff auto-reconnect.

    Key resilience features:
    - Topic existence check on startup (wait up to 60s for Kafka init)
    - Exponential backoff on connection failures (1s → 2s → 4s → ... → 60s max)
    - Consumer re-subscribe after every reconnect
    - Flush-and-commit on shutdown
    """
    logger.info(f"[CONSUMER] Starting with bootstrap_servers={KAFKA_BOOTSTRAP_SERVERS}, "
               f"topic={KAFKA_TOPIC}, group_id={KAFKA_GROUP_ID}")

    retry_count = 0
    delay_sec = RECONNECT_BASE_DELAY_SEC

    while True:
        consumer = None
        try:
            conf = {
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                'group.id': KAFKA_GROUP_ID,
                'auto.offset.reset': starting_offsets,
                'enable.auto.commit': False,
                'session.timeout.ms': 45000,
                'max.poll.interval.ms': 300000,
                'heartbeat.interval.ms': 10000,
            }
            consumer = Consumer(conf)
            consumer.subscribe([KAFKA_TOPIC])
            batch_writer.set_consumer(consumer)

            # Wait for topic to exist before polling
            if not wait_for_topic(consumer, KAFKA_TOPIC):
                raise KafkaException(KafkaError(_PARTITION_EOF, "Topic not found"))

            logger.info(f"[CONSUMER] Connected. Waiting for messages...")
            retry_count = 0
            delay_sec = RECONNECT_BASE_DELAY_SEC

            while True:
                try:
                    msg = consumer.poll(timeout=1.0)
                    if msg is None:
                        continue

                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            logger.debug(f"End of partition reached")
                        elif msg.error().code() == KafkaError._ALL_BROKERS_DOWN:
                            logger.warning(f"[POLL] All brokers down, reconnecting...")
                            raise msg.error()
                        else:
                            logger.warning(f"[POLL_ERROR] {msg.error()}")
                        continue

                    tick = decode_avro(msg.value())
                    if tick is None:
                        continue

                    tick["source_partition"] = msg.partition()
                    tick["source_offset"] = msg.offset()
                    batch_writer.add(tick)

                except KafkaException as e:
                    if e.args[0].code() in (
                        KafkaError._ALL_BROKERS_DOWN,
                        KafkaError._NETWORK_EXCEPTION,
                        KafkaError._UNKNOWN_TOPIC_OR_PART,
                    ):
                        logger.warning(f"[POLL] Kafka exception: {e}. Reconnecting...")
                        break
                    raise

        except KeyboardInterrupt:
            logger.info("[CONSUMER] Interrupted by user")
            break
        except Exception as e:
            if RECONNECT_MAX_RETRIES > 0 and retry_count >= RECONNECT_MAX_RETRIES:
                logger.error(f"[CONSUMER] Max retries ({RECONNECT_MAX_RETRIES}) reached. Exiting.")
                break

            retry_count += 1
            logger.warning(f"[RECONNECT] Consumer error (attempt {retry_count}): {type(e).__name__}: {e}")
            logger.info(f"[RECONNECT] Sleeping {delay_sec}s before retry...")

            try:
                if consumer:
                    consumer.close()
            except Exception:
                pass

            time.sleep(delay_sec)
            delay_sec = min(delay_sec * 2, RECONNECT_MAX_DELAY_SEC)
            continue

        finally:
            if consumer:
                try:
                    consumer.close()
                except Exception:
                    pass
            batch_writer.stop()
            logger.info("[CONSUMER] Shutdown complete")


# ============================================================================
# MAIN
# ============================================================================

def main():
    logger.info("=" * 60)
    logger.info(f"{APP_NAME} - Lightweight Real-time Crypto Pipeline")
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS} / {KAFKA_TOPIC}")
    logger.info(f"DB: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    logger.info(f"Batch flush: every {BATCH_FLUSH_INTERVAL_SEC}s or {BATCH_SIZE} records")
    logger.info(f"Auto-reconnect: base={RECONNECT_BASE_DELAY_SEC}s, max={RECONNECT_MAX_DELAY_SEC}s")
    logger.info("=" * 60)

    # Step 1: Setup DB schema, trigger, and dynamic partitions
    setup_database()

    # Step 2: Also setup partitions on every startup (idempotent)
    _setup_partitions()

    batch_writer = BatchWriter()

    # Start ML features computation thread (log_returns, volatility_30m, z_score, is_outlier)
    ml_thread = threading.Thread(target=_compute_ml_features, daemon=True)
    ml_thread.start()
    logger.info("[STARTUP] ML features computation thread started (30s interval)")

    def shutdown_handler(signum, frame):
        logger.info(f"[SIGNAL] Received signal {signum}, shutting down...")
        batch_writer.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    run_consumer(batch_writer)


if __name__ == "__main__":
    main()
