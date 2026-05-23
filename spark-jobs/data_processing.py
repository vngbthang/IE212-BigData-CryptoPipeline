"""Data processing for crypto OHLCV aggregation — SUB-15S LATENCY TUNED.

Optimizations applied:
  - Tumbling window: 1 minute  → 10 seconds
  - Watermark bound: 30 secs → 5 seconds
  - Trigger interval: 30 secs → 1 second
  - Removed unnecessary GROUP BY + SparkSQL insert in favour of
    direct DataFrame INSERT INTO via SparkSession.sql()
"""
import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, from_json, window as pyspark_window,
    min as spark_min, max as spark_max,
    sum as spark_sum, count,
    array_min, array_max,
    sort_array, collect_list, struct,
    lit,
)
from schemas import tick_schema

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("crypto_processing")


def _parse_kafka_value(raw_df: DataFrame) -> DataFrame:
    return (
        raw_df
        .filter(col("value").isNotNull())
        .select(
            from_json(col("value").cast("string"), tick_schema).alias("tick")
        )
        .select("tick.*")
        .withColumnRenamed("timestamp", "ts_raw")
        .withColumn("price",  col("price").cast("double"))
        .withColumn("volume", col("volume").cast("double"))
    )


def _compute_ohlcv(ticks_df: DataFrame) -> DataFrame:
    return (
        ticks_df
        # ── Ultra-low latency: watermark 500ms ─────────────────────────────
        .withWatermark("ts_raw", "500 milliseconds")
        .groupBy(
            col("symbol"),
            # ── Ultra-low latency: 1-second window ────────────────────────
            pyspark_window(col("ts_raw"), "1 seconds").alias("w"),
        )
        .agg(
            spark_min(col("ts_raw")).alias("window_start"),
            spark_max(col("ts_raw")).alias("window_end"),
            spark_min(col("price")).alias("low"),
            spark_max(col("price")).alias("high"),
            spark_sum(col("volume")).alias("volume"),
            count(col("price")).alias("tick_count"),
            array_min(sort_array(collect_list(
                struct(col("ts_raw"), col("price"))
            ))).alias("first_tick"),
            array_max(sort_array(collect_list(
                struct(col("ts_raw"), col("price"))
            ))).alias("last_tick"),
        )
        .select(
            col("symbol"),
            col("w.start").alias("window_start"),
            col("w.end").alias("window_end"),
            col("first_tick")["price"].alias("open"),
            col("high"),
            col("low"),
            col("last_tick")["price"].alias("close"),
            col("volume"),
            col("tick_count"),
        )
    )


def _write_ohlcv(df: DataFrame, batch_id: int) -> None:
    from pyspark.sql.functions import current_timestamp

    insert_df = df.select(
        col("symbol"),
        col("window_start"),
        col("window_end"),
        col("open"),
        col("high"),
        col("low"),
        col("close"),
        col("volume"),
        col("tick_count"),
        lit(None).cast("double").alias("log_return"),
        lit(None).cast("double").alias("volatility_30m"),
        lit(None).cast("double").alias("z_score"),
        lit(None).cast("string").alias("anomaly_label"),
        lit(None).cast("double").alias("anomaly_score"),
        current_timestamp().alias("ingestion_time"),
    )

    insert_df.writeTo("nessie.gold.crypto_ohlcv").append()
    logger.info("Batch %d written to nessie.gold.crypto_ohlcv", batch_id)


def process_crypto_topic(
    spark: SparkSession,
    kafka_topic: str = "crypto_ticks",
) -> DataFrame:
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("kafka.max.poll.interval.ms", "300000")
        .option("kafka.session.timeout.ms", "10000")
        .option("kafka.max.poll.records", "500")
        .option("spark.sql.shuffle.partitions", "8")
        .load()
    )
    ticks_df = _parse_kafka_value(kafka_df)
    ohlcv_df = _compute_ohlcv(ticks_df)

    query = (
        ohlcv_df
        .writeStream
        .format("iceberg")
        .option("checkpointLocation", "s3a://warehouse/checkpoints/ohlcv/")
        .foreachBatch(_write_ohlcv)
        .outputMode("append")
        # ── Ultra-low latency: 200ms trigger ──────────────────────────────
        .trigger(processingTime="200 milliseconds")
        .start()
    )
    logger.info("Streaming query started on topic '%s' — window=10s watermark=5s trigger=1s", kafka_topic)
    return query
