"""
Crypto Stream Processor — Modern Data Lakehouse
Apache Spark Structured Streaming → Apache Iceberg → MinIO via Nessie Catalog
"""

import logging
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("crypto_stream_processor")


# ---------------------------------------------------------------------------
# Schema Definition — crypto_ticks Kafka payload
# ---------------------------------------------------------------------------
CRYPTO_TICKS_SCHEMA = StructType(
    [
        StructField("trade_id",    StringType(),  nullable=False),
        StructField("symbol",       StringType(),  nullable=False),
        StructField("price",        DoubleType(),  nullable=False),
        StructField("size",         DoubleType(),  nullable=False),
        StructField("timestamp",    StringType(),  nullable=False),
        StructField("exchange",     StringType(),  nullable=True),
        StructField("trade_type",   StringType(),  nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# Iceberg Table Output Schema (gold.crypto_ohlcv)
# ---------------------------------------------------------------------------
GOLD_SCHEMA = StructType(
    [
        # Window / identifier
        StructField("window_start",   TimestampType(), nullable=False),
        StructField("window_end",     TimestampType(), nullable=False),
        StructField("symbol",         StringType(),    nullable=False),
        # OHLCV base
        StructField("open",           DoubleType(),    nullable=True),
        StructField("high",           DoubleType(),    nullable=True),
        StructField("low",            DoubleType(),    nullable=True),
        StructField("close",          DoubleType(),    nullable=True),
        StructField("volume",         DoubleType(),    nullable=True),
        StructField("trade_count",    LongType(),      nullable=True),
        StructField("vwap",           DoubleType(),    nullable=True),
        # ML Features
        StructField("log_return",    DoubleType(),    nullable=True),
        StructField("volatility_30m", DoubleType(),    nullable=True),
        StructField("z_score",        DoubleType(),    nullable=True),
        # Metadata
        StructField("event_time",     TimestampType(), nullable=True),
        StructField("processed_at",   TimestampType(), nullable=False),
    ]
)


# ---------------------------------------------------------------------------
# Spark Session Builder
# ---------------------------------------------------------------------------
def build_spark_session() -> SparkSession:
    """
    Configure SparkSession with Iceberg, Nessie catalog, and MinIO (S3A).
    """
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic     = os.getenv("KAFKA_TOPIC",             "crypto_ticks")
    kafka_starting_offset = os.getenv("KAFKA_STARTING_OFFSETS", "latest")

    logger.info("Building Spark session with Iceberg + Nessie + MinIO config ...")

    builder = (
        SparkSession.builder.appName("crypto-stream-processor")
        .config("spark.sql.streaming.checkpointLocation", "s3a://crypto-lake/checkpoints/gold_ohlcv")
        # ── Required Packages ──────────────────────────────────────────────
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
            "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4",
        )
        # ── Spark Extensions ───────────────────────────────────────────────
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
        )
        # ── Nessie Catalog ──────────────────────────────────────────────────
        .config("spark.sql.catalog.nessie",                    "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl",       "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri",               "http://nessie:19120/api/v1")
        .config("spark.sql.catalog.nessie.ref",               "main")
        .config("spark.sql.catalog.nessie.warehouse",          "s3a://crypto-lake/warehouse")
        # ── MinIO / S3A ───────────────────────────────────────────────────
        .config("spark.hadoop.fs.s3a.endpoint",             "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key",          "admin")
        .config("spark.hadoop.fs.s3a.secret.key",          "password")
        .config("spark.hadoop.fs.s3a.path.style.access",   "true")
        .config("spark.hadoop.fs.s3a.impl",                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # ── Streaming defaults ──────────────────────────────────────────────
        .config("spark.sql.streaming.pollTriggerMs", "1000")
        .config("spark.sql.adaptive.enabled",        "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    logger.info(
        f"Spark session ready. Kafka: {kafka_bootstrap} | Topic: {kafka_topic}"
    )
    return spark


# ---------------------------------------------------------------------------
# Kafka Source
# ---------------------------------------------------------------------------
def read_from_kafka(spark: SparkSession) -> DataFrame:
    """
    Subscribe to the crypto_ticks Kafka topic and deserialise JSON payloads.
    """
    kafka_bootstrap       = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic           = os.getenv("KAFKA_TOPIC",             "crypto_ticks")
    kafka_starting_offset = os.getenv("KAFKA_STARTING_OFFSETS",  "latest")

    logger.info(f"Reading from Kafka: {kafka_bootstrap} / {kafka_topic}")

    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers",   kafka_bootstrap)
        .option("subscribe",                kafka_topic)
        .option("startingOffsets",           kafka_starting_offset)
        .option("failOnDataLoss",            "false")
        .load()
    )

    # Parse JSON payload from Kafka value column
    parsed_df = raw_df.select(
        F.from_json(F.col("value").cast("string"), CRYPTO_TICKS_SCHEMA).alias(
            "payload"
        ),
        F.col("timestamp").alias("kafka_ts"),
    ).select("payload.*", "kafka_ts")

    return parsed_df


# ---------------------------------------------------------------------------
# Timestamp Normalisation
# ---------------------------------------------------------------------------
def with_event_time(df: DataFrame) -> DataFrame:
    """
    Parse the ISO-8601 trade timestamp into a Spark Timestamp column and
    register it as the streaming watermark / event-time column.
    """
    ts_expr = F.coalesce(
        F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"),
        F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        F.to_timestamp(F.col("timestamp")),
    )

    return df.withColumn("event_time", ts_expr)


# ---------------------------------------------------------------------------
# 1-Minute Tumbling OHLCV Aggregation
# ---------------------------------------------------------------------------
def aggregate_ohlcv(df: DataFrame) -> DataFrame:
    """
    Group by 1-minute tumbling window on event_time and compute:
      open, high, low, close, volume, trade_count, vwap
    """
    window_1m = Window.partitionBy(
        F.col("symbol"),
        F.window(F.col("event_time"), "1 minute"),
    )

    ohlcv_df = df.groupBy(
        F.col("symbol"),
        F.window(F.col("event_time"), "1 minute").alias("window"),
    ).agg(
        F.first(F.col("price")).alias("open"),
        F.max(F.col("price")).alias("high"),
        F.min(F.col("price")).alias("low"),
        F.last(F.col("price")).alias("close"),
        F.sum(F.col("size")).alias("volume"),
        F.count("*").alias("trade_count"),
        F.sum(F.col("price") * F.col("size")).alias("price_volume_sum"),
        F.sum(F.col("size")).alias("total_size"),
    ).select(
        F.col("symbol"),
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("open"),
        F.col("high"),
        F.col("low"),
        F.col("close"),
        F.col("volume"),
        F.col("trade_count"),
        (
            F.col("price_volume_sum") / F.col("total_size")
        ).alias("vwap"),
    )

    return ohlcv_df


# ---------------------------------------------------------------------------
# ML Features — log_return, volatility_30m, z_score
# ---------------------------------------------------------------------------
def add_ml_features(df: DataFrame) -> DataFrame:
    """
    Append three ML-ready features per symbol:

      log_return    — log(close / lag_close) within the symbol series
      volatility_30m — rolling 30-minute standard deviation of log returns
      z_score       — (close - rolling_mean_30m) / rolling_std_30m
    """
    # Window specs
    window_30m = (
        Window.partitionBy("symbol")
        .orderBy("window_start")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    window_lag = (
        Window.partitionBy("symbol")
        .orderBy("window_start")
        .rowsBetween(-1, -1)
    )

    return df.withColumn(
        "log_return",
        F.log(F.col("close") / F.lag("close", 1).over(window_lag)),
    ).withColumn(
        "volatility_30m",
        F.stddev("log_return").over(window_30m),
    ).withColumn(
        "rolling_mean_30m",
        F.avg("close").over(window_30m),
    ).withColumn(
        "rolling_std_30m",
        F.stddev("close").over(window_30m),
    ).withColumn(
        "z_score",
        (F.col("close") - F.col("rolling_mean_30m")) / F.col("rolling_std_30m"),
    ).drop("rolling_mean_30m", "rolling_std_30m")


# ---------------------------------------------------------------------------
# Metadata Columns
# ---------------------------------------------------------------------------
def with_metadata(df: DataFrame) -> DataFrame:
    """
    Attach processing-time metadata and ensure schema compatibility with
    the Iceberg gold table.
    """
    return df.withColumn("processed_at", F.current_timestamp())


# ---------------------------------------------------------------------------
# Ensure Iceberg Table Exists
# ---------------------------------------------------------------------------
def ensure_gold_table(spark: SparkSession) -> None:
    """
    Create the nessie.gold.crypto_ohlcv Iceberg table if it does not already
    exist, using the GOLD_SCHEMA definition.
    """
    create_sql = """
    CREATE TABLE IF NOT EXISTS nessie.gold.crypto_ohlcv (
        window_start   TIMESTAMP NOT NULL,
        window_end     TIMESTAMP NOT NULL,
        symbol         STRING    NOT NULL,
        open           DOUBLE,
        high           DOUBLE,
        low            DOUBLE,
        close          DOUBLE,
        volume         DOUBLE,
        trade_count    BIGINT,
        vwap           DOUBLE,
        log_return     DOUBLE,
        volatility_30m DOUBLE,
        z_score        DOUBLE,
        event_time     TIMESTAMP,
        processed_at   TIMESTAMP NOT NULL
    )
    USING iceberg
    PARTITIONED BY (days(window_start), symbol)
    """
    spark.sql(create_sql)
    logger.info("Table nessie.gold.crypto_ohlcv ensured.")


# ---------------------------------------------------------------------------
# foreachBatch Sink — write each micro-batch to Iceberg
# ---------------------------------------------------------------------------
def write_to_iceberg(batch_df: DataFrame, batch_id: int) -> None:
    """
    Called once per streaming micro-batch. Writes aggregated OHLCV + ML
    features to the Iceberg gold table using append mode.
    """
    logger.info(f"[Batch {batch_id}] Processing {batch_df.count()} rows ...")

    if batch_df.isEmpty():
        logger.info(f"[Batch {batch_id}] Empty batch, skipping.")
        return

    try:
        batch_df.writeTo("nessie.gold.crypto_ohlcv").append()
        logger.info(f"[Batch {batch_id}] Appended successfully.")
    except Exception as exc:
        logger.error(f"[Batch {batch_id}] Iceberg write failed: {exc}", exc_info=True)
        raise


# ---------------------------------------------------------------------------
# Main Pipeline
# ---------------------------------------------------------------------------
def main() -> None:
    logger.info("=" * 60)
    logger.info("Crypto Stream Processor — Starting")
    logger.info("=" * 60)

    spark = build_spark_session()
    ensure_gold_table(spark)

    # ── Stream Graph ────────────────────────────────────────────────────────
    raw_df       = read_from_kafka(spark)
    timed_df     = with_event_time(raw_df)
    ohlcv_df     = aggregate_ohlcv(timed_df)
    features_df  = add_ml_features(ohlcv_df)
    final_df     = with_metadata(features_df)

    # ── Write to Iceberg ────────────────────────────────────────────────────
    checkpoint = "s3a://crypto-lake/checkpoints/gold_ohlcv"

    query = (
        final_df.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="10 seconds")
        .foreachBatch(write_to_iceberg)
        .start()
    )

    logger.info("Streaming query started — writing to nessie.gold.crypto_ohlcv")
    logger.info(f"Checkpoint: {checkpoint}")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.warning("Interrupted. Stopping query ...")
        query.stop()
        spark.stop()
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    main()
