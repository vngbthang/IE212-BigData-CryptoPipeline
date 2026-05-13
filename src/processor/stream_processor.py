import logging
import os
import threading
import time
import subprocess
import sys
from datetime import datetime, timedelta, timezone

# Auto-install psycopg2 if not available
try:
    import psycopg2
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "psycopg2-binary"])
    import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    base64, col, current_timestamp, lit, struct, to_json, window,
    min as spark_min, max as spark_max, last, sum as spark_sum,
    count, first, when, concat_ws, to_timestamp, coalesce, regexp_replace,
    avg as spark_avg, stddev_samp, lag, log, abs as spark_abs
)
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.types import DecimalType, TimestampType, BooleanType, IntegerType, LongType

APP_NAME = "CryptoPipeline_Silver_Processor"
KAFKA_BOOTSTRAP_SERVERS = "kafka-0:9092,kafka-1:9092,kafka-2:9092"
KAFKA_TOPIC = "crypto_ticks"
DLQ_TOPIC = "crypto_ticks_dead_letter"
# Use the local filesystem for checkpoints so the realtime stream does not
# depend on HDFS datanode availability.
SPARK_CHECKPOINT_BASE = "file:///tmp/spark_checkpoints/crypto_stream"
CHECKPOINT_LOCATION = f"{SPARK_CHECKPOINT_BASE}/raw_ticks"
OHLCV_CHECKPOINT_LOCATION = f"{SPARK_CHECKPOINT_BASE}/ohlcv"
DLQ_CHECKPOINT_LOCATION = f"{SPARK_CHECKPOINT_BASE}/dead_letter"
# Disable HDFS writes from the realtime stream processor to avoid small-file storm.
# HDFS ingestion is handled by a separate batch job. Keep this unset to prevent accidental writes.
HDFS_GOLD_OHLCV_PATH = None

# PostgreSQL Configuration
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "cryptopipeline")
DB_USER = os.getenv("DB_USER", "crypto_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "crypto_password")
DB_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
HEARTBEAT_TABLE = "gold.pipeline_heartbeat"
HEARTBEAT_SAMPLES_TABLE = "gold.pipeline_latency_samples"
PIPELINE_ALERTS_TABLE = "gold.pipeline_alerts"

# Feature flags
WRITE_TO_DB = True
WRITE_TO_CONSOLE = os.getenv("WRITE_TO_CONSOLE", "false").lower() == "true"
# Debug flag: enable writing raw validated ticks to console for troubleshooting
DEBUG_RAW = os.getenv("DEBUG_RAW", "false").lower() == "true"
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "1 second")
FORCE_WRITE_RAW = os.getenv("FORCE_WRITE_RAW", "false").lower() == "true"


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def ensure_heartbeat_table() -> None:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=int(DB_PORT),
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    conn.autocommit = True
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS gold.pipeline_heartbeat (
                id SMALLINT PRIMARY KEY,
                updated_at TIMESTAMPTZ NOT NULL
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS gold.pipeline_latency_samples (
                measured_at TIMESTAMPTZ PRIMARY KEY,
                latency_ms DOUBLE PRECISION NOT NULL
            )
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_pipeline_latency_samples_measured_at
            ON gold.pipeline_latency_samples (measured_at DESC)
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS gold.pipeline_alerts (
                alert_id BIGSERIAL PRIMARY KEY,
                alert_type VARCHAR(50) NOT NULL,
                severity VARCHAR(20) NOT NULL,
                symbol VARCHAR(20),
                alert_time TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                gap_start TIMESTAMPTZ,
                gap_end TIMESTAMPTZ,
                details TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
            )
            """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_pipeline_alerts_alert_time
            ON gold.pipeline_alerts (alert_time DESC)
            """
        )
        cursor.execute("ALTER TABLE gold.gold_crypto_ohlcv ADD COLUMN IF NOT EXISTS log_returns DOUBLE PRECISION")
        cursor.execute("ALTER TABLE gold.gold_crypto_ohlcv ADD COLUMN IF NOT EXISTS volatility_30m DOUBLE PRECISION")
        cursor.execute("ALTER TABLE gold.gold_crypto_ohlcv ADD COLUMN IF NOT EXISTS z_score DOUBLE PRECISION")
        cursor.execute("ALTER TABLE gold.gold_crypto_ohlcv ADD COLUMN IF NOT EXISTS is_outlier BOOLEAN DEFAULT FALSE")
    finally:
        cursor.close()
        conn.close()


def start_heartbeat_thread() -> threading.Thread:
    logger = logging.getLogger(__name__)
    last_beat = time.time()

    def heartbeat_loop() -> None:
        nonlocal last_beat
        while True:
            try:
                now_beat = time.time()
                latency_ms = max((now_beat - last_beat) * 1000.0, 0.0)
                last_beat = now_beat

                conn = psycopg2.connect(
                    host=DB_HOST,
                    port=int(DB_PORT),
                    database=DB_NAME,
                    user=DB_USER,
                    password=DB_PASSWORD,
                )
                conn.autocommit = True
                cursor = conn.cursor()
                cursor.execute(
                    f"""
                    INSERT INTO {HEARTBEAT_TABLE} (id, updated_at)
                    VALUES (1, clock_timestamp())
                    ON CONFLICT (id)
                    DO UPDATE SET updated_at = EXCLUDED.updated_at
                    """
                )
                cursor.execute(
                    f"""
                    INSERT INTO {HEARTBEAT_SAMPLES_TABLE} (measured_at, latency_ms)
                    VALUES (clock_timestamp(), %s)
                    """,
                    (latency_ms,),
                )
                cursor.execute(
                    f"""
                    DELETE FROM {HEARTBEAT_SAMPLES_TABLE}
                    WHERE measured_at < CURRENT_TIMESTAMP - INTERVAL '24 hours'
                    """
                )
                cursor.close()
                conn.close()
            except Exception as exc:
                logger.warning("[HEARTBEAT_ERROR] %s", exc)
            time.sleep(1)

    thread = threading.Thread(target=heartbeat_loop, daemon=True)
    thread.start()
    return thread


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName(APP_NAME)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.postgresql:postgresql:42.7.3,"
            "org.apache.spark:spark-avro_2.12:3.5.1",
        )
        .config("spark.sql.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.task.maxFailures", "8")
        .config("spark.stage.maxConsecutiveAttempts", "8")
        .config("spark.network.timeout", "300s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.deploy.maxExecutorRetries", "10")
        .getOrCreate()
    )


def create_tick_schema() -> StructType:
    """Avro schema for deserialized tick data (matches coinbase_producer.py)"""
    return StructType(
        [
            StructField("timestamp", LongType(), False),  # Unix timestamp in ms
            StructField("product_id", StringType(), False),
            StructField("exchange", StringType(), True),
            StructField("price", DecimalType(20, 8), False),
            StructField("size", DecimalType(20, 8), False),
            StructField("bid", DecimalType(20, 8), True),
            StructField("ask", DecimalType(20, 8), True),
            StructField("side", StringType(), True),  # Enum: buy/sell/unknown
            StructField("sequence", LongType(), False),
            StructField("ingestion_timestamp", LongType(), False),  # Unix timestamp in ms
        ]
    )


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_tick_payload(raw_df: DataFrame, schema: StructType) -> DataFrame:
    """Deserialize Avro-encoded Kafka messages and extract fields"""
    
    # Avro schema JSON string (must match coinbase_producer.py exactly)
    avro_schema_json = '''
    {
      "namespace": "com.cryptopipeline.bronze",
      "type": "record",
      "name": "CryptoTickEvent",
      "fields": [
        {"name": "timestamp", "type": "long"},
        {"name": "product_id", "type": "string"},
        {"name": "exchange", "type": "string"},
        {"name": "price", "type": "double"},
        {"name": "size", "type": "double"},
        {"name": "bid", "type": ["null", "double"]},
        {"name": "ask", "type": ["null", "double"]},
        {"name": "side", "type": {"type": "enum", "name": "TradeSide", "symbols": ["buy", "sell", "unknown"]}},
        {"name": "sequence", "type": "long"},
        {"name": "ingestion_timestamp", "type": "long"}
      ]
    }
    '''
    
    return (
        raw_df
        .select(
            col("value").alias("raw_value"),
            from_avro(col("value"), avro_schema_json, {"mode": "PERMISSIVE"}).alias("tick"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp")
        )
        .select(
            col("raw_value"),
            (col("tick.timestamp") / 1000).cast(TimestampType()).alias("time"),  # Convert ms -> seconds -> timestamp
            col("tick.timestamp").alias("raw_timestamp"),
            col("tick.product_id").alias("product_id"),
            col("tick.exchange").alias("exchange"),
            col("tick.price").cast(DecimalType(20, 8)).alias("price"),
            col("tick.size").cast(DecimalType(20, 8)).alias("size"),
            col("tick.bid").cast(DecimalType(20, 8)).alias("bid"),
            col("tick.ask").cast(DecimalType(20, 8)).alias("ask"),
            col("tick.side").alias("side"),
            col("tick.sequence").alias("sequence"),
            (col("tick.ingestion_timestamp") / 1000).cast(TimestampType()).alias("ingest_timestamp"),
            col("tick.ingestion_timestamp").alias("raw_ingestion_timestamp"),
            col("partition").alias("source_partition"),
            col("offset").alias("source_offset"),
            col("kafka_timestamp").alias("kafka_timestamp"),
            col("tick").isNull().alias("deserialization_failed"),
        )
    )


def enrich_ohlcv_features(batch_df: DataFrame) -> DataFrame:
    """Compute rolling features on the current micro-batch plus recent history."""
    symbol_window = Window.partitionBy("symbol").orderBy("timestamp")
    rolling_window = symbol_window.rowsBetween(-29, 0)

    close_double = col("close").cast("double")

    enriched = (
        batch_df
        .withColumn("prev_close", lag(close_double).over(symbol_window))
        .withColumn("rolling_mean_close", spark_avg(close_double).over(rolling_window))
        .withColumn("volatility_30m", stddev_samp(close_double).over(rolling_window))
        .withColumn(
            "log_returns",
            when(
                col("prev_close").isNull() | (col("prev_close") <= 0) | close_double.isNull() | (close_double <= 0),
                lit(None).cast("double"),
            ).otherwise(log(close_double / col("prev_close")))
        )
        .withColumn(
            "z_score",
            when(
                col("volatility_30m").isNull() | (col("volatility_30m") <= 0) | col("rolling_mean_close").isNull(),
                lit(None).cast("double"),
            ).otherwise((close_double - col("rolling_mean_close")) / col("volatility_30m"))
        )
        .withColumn(
            "is_outlier",
            when(spark_abs(col("z_score")) >= 3.0, lit(True)).otherwise(lit(False))
        )
        .drop("prev_close", "rolling_mean_close")
    )
    return enriched


def build_data_gap_alerts(rows, latest_timestamps=None):
    """Build alert rows for missing 1-minute OHLCV windows."""
    alerts = []
    rows_by_symbol = {}
    latest_timestamps = latest_timestamps or {}

    for row in rows:
        symbol = row.get("symbol")
        if not symbol or row.get("timestamp") is None:
            continue
        rows_by_symbol.setdefault(symbol, []).append(row)

    for symbol, symbol_rows in rows_by_symbol.items():
        symbol_rows.sort(key=lambda item: item["timestamp"])
        previous_timestamp = latest_timestamps.get(symbol)
        for row in symbol_rows:
            current_timestamp = row["timestamp"]
            if previous_timestamp is not None:
                gap_minutes = int((current_timestamp - previous_timestamp).total_seconds() // 60) - 1
                if gap_minutes > 0:
                    alert_details = (
                        f"Missing {gap_minutes} one-minute window(s) between "
                        f"{previous_timestamp.isoformat()} and {current_timestamp.isoformat()}"
                    )
                    alerts.append(
                        (
                            "DATA_GAP",
                            "WARN",
                            symbol,
                            previous_timestamp,
                            current_timestamp,
                            alert_details,
                        )
                    )
            previous_timestamp = current_timestamp

    return alerts


def insert_pipeline_alerts(cursor, alerts) -> None:
    """Insert pipeline alert rows into Postgres."""
    if not alerts:
        return

    execute_values(
        cursor,
        f"""
        INSERT INTO {PIPELINE_ALERTS_TABLE} (
            alert_type, severity, symbol, gap_start, gap_end, details
        ) VALUES %s
        """,
        alerts,
        page_size=min(500, len(alerts)),
    )


def validate_record(df: DataFrame) -> DataFrame:
    """Add error detection for malformed records"""
    validation_issue = when(col("deserialization_failed"), "AVRO_DESERIALIZATION_FAILED") \
        .when(col("price").isNull(), "NULL_PRICE") \
        .when(col("price") <= 0, "INVALID_PRICE") \
        .when(col("size").isNull(), "NULL_SIZE") \
        .when(col("size") < 0, "INVALID_SIZE") \
        .when(col("time").isNull(), "NULL_TIME") \
        .otherwise("")

    return (
        df
        .withColumn("error_messages", validation_issue)
        .withColumn("error_flag", col("error_messages") != "")
        .withColumn("dlq_reason", col("error_messages"))
        .withColumn(
            "dlq_payload",
            to_json(
                struct(
                    col("dlq_reason").alias("reason"),
                    col("product_id"),
                    col("exchange"),
                    col("raw_timestamp"),
                    col("raw_ingestion_timestamp"),
                    col("source_partition"),
                    col("source_offset"),
                    col("kafka_timestamp"),
                    base64(col("raw_value")).alias("raw_value_base64"),
                )
            )
        )
    )


def keep_valid_records(df: DataFrame) -> DataFrame:
    """Filter to keep only valid records (error_flag == False)"""
    return df.filter((col("error_flag") == False) & (col("deserialization_failed") == False))


def build_dead_letter_records(df: DataFrame) -> DataFrame:
    """Build Kafka DLQ rows for records that fail deserialization or validation."""
    return (
        df.filter(col("error_flag") == True)
        .select(
            coalesce(col("product_id"), lit("unknown")).alias("key"),
            col("dlq_payload").cast(StringType()).alias("value"),
        )
    )


def write_to_dead_letter_kafka(dlq_df: DataFrame):
    """Write rejected records to Kafka DLQ."""
    return (
        dlq_df.writeStream.format("kafka")
        .outputMode("append")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", DLQ_TOPIC)
        .option("checkpointLocation", DLQ_CHECKPOINT_LOCATION)
        .start()
    )


def aggregate_to_ohlcv(validated_df: DataFrame) -> DataFrame:
    """Aggregate raw ticks to 1-minute OHLCV candles"""
    # Only process records with non-null timestamps (required for windowing)
    ohlcv = (
        validated_df
        .filter(col("time").isNotNull())  # Must have valid time for windowing
        .withWatermark("time", WATERMARK_DELAY)
        .groupBy(
            window(col("time"), "1 minute", "1 minute").alias("time_window"),
            col("product_id")
        )
        .agg(
            # OHLCV values
            first(col("price")).alias("open"),
            spark_max(col("price")).alias("high"),
            spark_min(col("price")).alias("low"),
            last(col("price")).alias("close"),
            spark_sum(col("size")).alias("volume"),
            
            # Observability
            spark_max(col("ingest_timestamp")).alias("ingest_timestamp"),
            current_timestamp().alias("process_timestamp"),
            current_timestamp().alias("display_timestamp"),
            count(col("price")).alias("record_count"),
            
            # Lineage
            first(col("source_partition")).alias("source_partition"),
            first(col("source_offset")).alias("source_offset"),
        )
        .select(
            col("time_window.start").cast(TimestampType()).alias("timestamp"),
            col("product_id").alias("symbol"),
            lit("coinbase").alias("exchange"),
            col("open").cast(DecimalType(20, 8)),
            col("high").cast(DecimalType(20, 8)),
            col("low").cast(DecimalType(20, 8)),
            col("close").cast(DecimalType(20, 8)),
            col("volume").cast(DecimalType(20, 8)),
            col("ingest_timestamp"),
            col("process_timestamp"),
            col("display_timestamp"),
            col("record_count"),
            lit(False).alias("error_flag"),  # Clean records
            lit(None).alias("error_messages").cast(StringType()),
            col("source_partition"),
            col("source_offset"),
        )
    )
    return ohlcv


def write_to_console(ohlcv_df: DataFrame):
    """Write to console for debugging (optional)"""
    if not WRITE_TO_CONSOLE:
        return None
    
    return (
        ohlcv_df.writeStream.format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("checkpointLocation", f"{OHLCV_CHECKPOINT_LOCATION}/console")
        .start()
    )


def log_stream_progress(stream_query, logger: logging.Logger) -> None:
    def watcher() -> None:
        while stream_query.isActive:
            progress = stream_query.lastProgress
            if progress:
                logger.info(
                    "[BATCH] batchId=%s numInputRows=%s inputRowsPerSecond=%s processedRowsPerSecond=%s",
                    progress.get("batchId"),
                    progress.get("numInputRows"),
                    progress.get("inputRowsPerSecond"),
                    progress.get("processedRowsPerSecond"),
                )
            time.sleep(2)

    thread = threading.Thread(target=watcher, daemon=True)
    thread.start()


def write_to_postgres(ohlcv_df: DataFrame, epoch_id: int):
    """Real-time upsert sink for PostgreSQL using psycopg2.
    
    Uses INSERT ... ON CONFLICT DO UPDATE to enable live candle updates
    without duplicate key errors. Each candle is continuously overwritten
    as new tick data arrives within the same 1-minute window.
    """
    logger = logging.getLogger(__name__)
    spark = ohlcv_df.sparkSession
    batch_df = ohlcv_df.withColumn("_is_current_batch", lit(True))

    batch_row_count = batch_df.count()
    if batch_row_count == 0:
        logger.info("[FOREACH_BATCH] epoch_id=%s empty batch, skipping", epoch_id)
        return

    symbols = [row["symbol"] for row in batch_df.select("symbol").distinct().collect()]
    min_timestamp_row = batch_df.agg(spark_min("timestamp").alias("min_timestamp")).collect()[0]
    min_timestamp = min_timestamp_row["min_timestamp"]

    combined_df = batch_df
    # Attempt to load a short window of recent history for rolling-feature accuracy.
    # Build a safe SQL IN-list for symbols by doubling single quotes.
    try:
        if symbols and min_timestamp is not None:
            symbol_filter = ", ".join(["'%s'" % s.replace("'", "''") for s in symbols])
            history_cutoff = min_timestamp - timedelta(minutes=29)
            history_query = (
                "(SELECT timestamp, symbol, exchange, open, high, low, close, volume, "
                "ingest_timestamp, process_timestamp, display_timestamp, record_count, "
                "error_flag, error_messages, source_partition, source_offset, "
                "log_returns, volatility_30m, z_score, is_outlier "
                + "FROM gold.gold_crypto_ohlcv WHERE symbol IN (" + symbol_filter + ") "
                + "AND timestamp >= TIMESTAMP '" + history_cutoff.strftime('%Y-%m-%d %H:%M:%S') + "') AS gold_history"
            )
            try:
                history_df = (
                    spark.read.format("jdbc")
                    .option("url", DB_URL)
                    .option("dbtable", history_query)
                    .option("user", DB_USER)
                    .option("password", DB_PASSWORD)
                    .option("driver", "org.postgresql.Driver")
                    .load()
                    .withColumn("_is_current_batch", lit(False))
                )
                combined_df = history_df.unionByName(batch_df, allowMissingColumns=True)
            except Exception as history_err:
                logger.warning("[FEATURE_HISTORY_LOAD_ERROR] epoch_id=%s error=%s", epoch_id, history_err)
    except Exception:
        # Do not fail the batch if symbol quoting or date math fails; fall back to batch-only features.
        logger.warning("[FEATURE_HISTORY_PREP_ERROR] epoch_id=%s proceeding without DB history", epoch_id)

    enriched_df = enrich_ohlcv_features(combined_df)
    current_df = (
        enriched_df
        .filter(col("_is_current_batch") == True)
        .drop("_is_current_batch")
        .orderBy("symbol", "timestamp")
    )

    rows = [row.asDict(recursive=True) for row in current_df.collect()]
    row_count = len(rows)

    if row_count == 0:
        logger.info("[FOREACH_BATCH] epoch_id=%s empty batch, skipping", epoch_id)
        return

    logger.info("[FOREACH_BATCH] epoch_id=%s batch_size=%s", epoch_id, row_count)

    conn = psycopg2.connect(
        host=DB_HOST,
        port=int(DB_PORT),
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    cursor = conn.cursor()

    try:
        latest_timestamps: dict[str, datetime] = {}
        if symbols:
            cursor.execute(
                """
                SELECT symbol, MAX(timestamp) AS latest_timestamp
                FROM gold.gold_crypto_ohlcv
                WHERE symbol = ANY(%s)
                GROUP BY symbol
                """,
                (symbols,),
            )
            latest_timestamps = {symbol: latest_timestamp for symbol, latest_timestamp in cursor.fetchall()}

        alerts = build_data_gap_alerts(rows, latest_timestamps)
        insert_pipeline_alerts(cursor, alerts)

        cursor.execute("DROP TABLE IF EXISTS tmp_gold_ohlcv_merge")
        cursor.execute(
            """
            CREATE TEMP TABLE tmp_gold_ohlcv_merge (
                timestamp TIMESTAMP,
                symbol VARCHAR(20),
                exchange VARCHAR(50),
                open NUMERIC(20, 8),
                high NUMERIC(20, 8),
                low NUMERIC(20, 8),
                close NUMERIC(20, 8),
                volume NUMERIC(20, 8),
                ingest_timestamp TIMESTAMPTZ,
                process_timestamp TIMESTAMPTZ,
                record_count INTEGER,
                log_returns DOUBLE PRECISION,
                volatility_30m DOUBLE PRECISION,
                z_score DOUBLE PRECISION,
                is_outlier BOOLEAN,
                error_flag BOOLEAN,
                error_messages TEXT,
                source_partition INTEGER,
                source_offset BIGINT
            ) ON COMMIT DROP
            """
        )

        insert_rows = [
            (
                row["timestamp"],
                row["symbol"],
                row["exchange"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["volume"],
                row["ingest_timestamp"],
                row["process_timestamp"],
                int(row["record_count"]) if row["record_count"] is not None else 0,
                float(row["log_returns"]) if row.get("log_returns") is not None else None,
                float(row["volatility_30m"]) if row.get("volatility_30m") is not None else None,
                float(row["z_score"]) if row.get("z_score") is not None else None,
                bool(row["is_outlier"]) if row.get("is_outlier") is not None else False,
                row["error_flag"],
                row["error_messages"],
                int(row["source_partition"]) if row["source_partition"] is not None else None,
                int(row["source_offset"]) if row["source_offset"] is not None else None,
            )
            for row in rows
        ]

        execute_values(
            cursor,
            """
            INSERT INTO tmp_gold_ohlcv_merge (
                timestamp, symbol, exchange, open, high, low, close, volume,
                ingest_timestamp, process_timestamp, record_count, log_returns,
                volatility_30m, z_score, is_outlier, error_flag,
                error_messages, source_partition, source_offset
            ) VALUES %s
            """,
            insert_rows,
            page_size=min(1000, row_count),
        )

        cursor.execute(
            """
            UPDATE gold.gold_crypto_ohlcv AS tgt SET
                exchange = src.exchange,
                open = src.open,
                high = src.high,
                low = src.low,
                close = src.close,
                volume = src.volume,
                ingest_timestamp = src.ingest_timestamp,
                process_timestamp = src.process_timestamp,
                display_timestamp = clock_timestamp(),
                updated_at = clock_timestamp(),
                record_count = src.record_count,
                log_returns = src.log_returns,
                volatility_30m = src.volatility_30m,
                z_score = src.z_score,
                is_outlier = src.is_outlier,
                error_flag = src.error_flag,
                error_messages = src.error_messages,
                source_partition = src.source_partition,
                source_offset = src.source_offset
            FROM tmp_gold_ohlcv_merge AS src
            WHERE tgt.timestamp = src.timestamp
              AND tgt.symbol = src.symbol
            """
        )

        cursor.execute(
            """
            INSERT INTO gold.gold_crypto_ohlcv (
                timestamp, symbol, exchange, open, high, low, close, volume,
                ingest_timestamp, process_timestamp, display_timestamp, record_count,
                log_returns, volatility_30m, z_score, is_outlier,
                error_flag, error_messages, source_partition, source_offset, updated_at
            )
            SELECT
                src.timestamp, src.symbol, src.exchange, src.open, src.high, src.low,
                src.close, src.volume, src.ingest_timestamp, src.process_timestamp,
                clock_timestamp(), src.record_count, src.log_returns, src.volatility_30m,
                src.z_score, src.is_outlier, src.error_flag, src.error_messages,
                src.source_partition, src.source_offset, clock_timestamp()
            FROM tmp_gold_ohlcv_merge AS src
            WHERE NOT EXISTS (
                SELECT 1
                FROM gold.gold_crypto_ohlcv AS tgt
                WHERE tgt.timestamp = src.timestamp
                  AND tgt.symbol = src.symbol
            )
            """
        )

        conn.commit()
        logger.info("[DB_WRITE] epoch_id=%s rows_upserted=%s", epoch_id, row_count)
    except Exception as db_err:
        conn.rollback()
        logger.error("[DB_WRITE_ERROR] epoch_id=%s error=%s", epoch_id, str(db_err))
        raise
    finally:
        cursor.close()
        conn.close()


def main() -> None:
    configure_logging()
    logger = logging.getLogger(__name__)
    logger.info("Stream processor path: %s", os.path.abspath(__file__))
    logger.info(f"DB_HOST={DB_HOST}, WRITE_TO_DB={WRITE_TO_DB}, WRITE_TO_CONSOLE={WRITE_TO_CONSOLE}")

    ensure_heartbeat_table()
    start_heartbeat_thread()

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Step 1: Read from Kafka
    schema = create_tick_schema()
    raw_df = read_kafka_stream(spark)

    # Step 2: Parse JSON
    parsed_df = parse_tick_payload(raw_df, schema)
    logger.info("Parsed DataFrame Schema:")
    parsed_df.printSchema()

    # Step 3: Validate records (add error flags)
    validated_df = validate_record(parsed_df)
    dlq_df = build_dead_letter_records(validated_df)
    valid_df = keep_valid_records(validated_df)

    # Optional debug: write raw validated ticks to console to confirm ingestion
    if DEBUG_RAW:
        raw_console = (
            validated_df.writeStream.format("console")
            .outputMode("append")
            .option("truncate", "false")
            .option("checkpointLocation", f"{OHLCV_CHECKPOINT_LOCATION}/raw_console")
            .start()
        )
        logger.info("Debug: writing raw validated ticks to console")

    # Step 4: Aggregate to 1-minute OHLCV
    ohlcv_df = aggregate_to_ohlcv(valid_df)
    logger.info("OHLCV DataFrame Schema:")
    ohlcv_df.printSchema()

    # Step 5: Setup write paths
    active_query = None
    dlq_query = None

    if dlq_df is not None:
        dlq_query = write_to_dead_letter_kafka(dlq_df)
        log_stream_progress(dlq_query, logger)
        logger.info(f"Writing rejected records to Kafka DLQ topic {DLQ_TOPIC}")

    # Path A: Write to console (for debugging)
    if WRITE_TO_CONSOLE:
        active_query = write_to_console(ohlcv_df)
        if active_query:
            log_stream_progress(active_query, logger)
            logger.info("Writing to console output")

    # Path B: Write to PostgreSQL (production) - REAL-TIME MODE
    active_query = (
        ohlcv_df.writeStream
        .outputMode("update")
        .option("checkpointLocation", f"{OHLCV_CHECKPOINT_LOCATION}/postgres")
        .trigger(processingTime="500 milliseconds")
        .foreachBatch(write_to_postgres)
        .start()
    )
    log_stream_progress(active_query, logger)
    logger.info(f"Writing OHLCV to {DB_URL}")

    if FORCE_WRITE_RAW:
        raw_query = (
            valid_df.writeStream
            .option("checkpointLocation", f"{OHLCV_CHECKPOINT_LOCATION}/raw_validated")
            .foreachBatch(write_to_postgres)
            .start()
        )
        log_stream_progress(raw_query, logger)
        logger.info("Force-writing raw validated_df to Postgres/HDFS for debug")

    if active_query:
        logger.info("Streaming processor is running. Press Ctrl+C to stop.")
        active_query.awaitTermination()
    else:
        logger.error("No active write path configured. Exiting.")


if __name__ == "__main__":
    main()
