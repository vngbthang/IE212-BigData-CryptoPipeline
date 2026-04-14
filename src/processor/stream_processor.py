import logging
import os
import threading
import time
from datetime import datetime, timezone
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, from_json, window, current_timestamp,
    min as spark_min, max as spark_max, last, sum as spark_sum,
    count, first, lit, when, concat_ws, to_timestamp, coalesce, regexp_replace
)
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.types import DecimalType, TimestampType, BooleanType, IntegerType, LongType

APP_NAME = "CryptoPipeline_Silver_Processor"
KAFKA_BOOTSTRAP_SERVERS = "kafka-0:9092,kafka-1:9092,kafka-2:9092"
KAFKA_TOPIC = "crypto_ticks"
CHECKPOINT_LOCATION = "/tmp/spark_checkpoints/raw_ticks"
OHLCV_CHECKPOINT_LOCATION = "/tmp/spark_checkpoints/ohlcv_processor"

# PostgreSQL Configuration
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "cryptopipeline")
DB_USER = os.getenv("DB_USER", "crypto_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "crypto_password")
DB_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Feature flags
WRITE_TO_DB = os.getenv("WRITE_TO_DB", "true").lower() == "true"
WRITE_TO_CONSOLE = os.getenv("WRITE_TO_CONSOLE", "true").lower() == "true"


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName(APP_NAME)
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.postgresql:postgresql:42.7.3",
        )
        .getOrCreate()
    )


def create_tick_schema() -> StructType:
    return StructType(
        [
            StructField("type", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("price", StringType(), True),
            StructField("time", StringType(), True),
            StructField("size", StringType(), True),
        ]
    )


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )


def parse_tick_payload(raw_df: DataFrame, schema: StructType) -> DataFrame:
    """Parse Kafka JSON and cast to proper types"""
    return (
        raw_df.selectExpr("CAST(value AS STRING) AS value", "partition", "offset", "timestamp AS kafka_timestamp")
        .select(from_json(col("value"), schema).alias("payload"), "partition", "offset", "kafka_timestamp")
        .select(
            col("payload.type").alias("type"),
            col("payload.product_id").alias("product_id"),
            col("payload.price").cast(DecimalType(18, 8)).alias("price"),
            coalesce(
                col("payload.time").cast(TimestampType()),
                to_timestamp(
                    regexp_replace(
                        regexp_replace(col("payload.time"), "T", " "),
                        "Z$",
                        ""
                    )
                )
            ).alias("time"),
            col("payload.size").cast(DecimalType(18, 8)).alias("size"),
            col("partition").alias("source_partition"),
            col("offset").alias("source_offset"),
            col("kafka_timestamp").alias("ingest_timestamp"),
        )
    )


def validate_record(df: DataFrame) -> DataFrame:
    """Add error detection for malformed records"""
    return df.withColumn(
        "error_flag",
        when(
            (col("price").isNull()) | (col("price") <= 0) |
            (col("size").isNull()) | (col("size") < 0) |
            (col("time").isNull()),
            True
        ).otherwise(False)
    ).withColumn(
        "error_messages",
        when(col("price").isNull(), "NULL_PRICE")
        .when(col("price") <= 0, "INVALID_PRICE")
        .when(col("size").isNull(), "NULL_SIZE")
        .when(col("size") < 0, "INVALID_SIZE")
        .when(col("time").isNull(), "NULL_TIME")
        .otherwise("")
    )


def aggregate_to_ohlcv(validated_df: DataFrame) -> DataFrame:
    """Aggregate raw ticks to 1-minute OHLCV candles"""
    ohlcv = (
        validated_df
        .filter(col("error_flag") == False)  # Only process valid records
        .withWatermark("time", "1 minute")  # Allow 1 minute late data
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
            first(col("ingest_timestamp")).alias("ingest_timestamp"),
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


def write_to_postgres(ohlcv_df: DataFrame, epoch_id: int):
    """Micro-batch write to PostgreSQL (called every micro-batch)"""
    if not WRITE_TO_DB or ohlcv_df.count() == 0:
        return
    
    try:
        ohlcv_df.write \
            .format("jdbc") \
            .mode("append") \
            .option("url", DB_URL) \
            .option("dbtable", "gold.gold_crypto_ohlcv") \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .option("batchsize", 1000) \
            .option("isolationLevel", "READ_COMMITTED") \
            .save()
        
        count = ohlcv_df.count()
        logger = logging.getLogger(__name__)
        logger.info(f"[DB_WRITE] epoch_id={epoch_id} rows_written={count}")
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"[DB_WRITE_ERROR] epoch_id={epoch_id} error={str(e)}")
        raise


def log_stream_progress(query, logger: logging.Logger) -> threading.Thread:
    def watcher() -> None:
        last_batch_id = None
        while query.isActive:
            progress = query.lastProgress
            if progress and progress.get("batchId") != last_batch_id:
                last_batch_id = progress.get("batchId")
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
    return thread


def main() -> None:
    configure_logging()
    logger = logging.getLogger(__name__)
    logger.info("Stream processor path: %s", os.path.abspath(__file__))
    logger.info(f"DB_HOST={DB_HOST}, WRITE_TO_DB={WRITE_TO_DB}, WRITE_TO_CONSOLE={WRITE_TO_CONSOLE}")

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
    
    # Step 4: Aggregate to 1-minute OHLCV
    ohlcv_df = aggregate_to_ohlcv(validated_df)
    logger.info("OHLCV DataFrame Schema:")
    ohlcv_df.printSchema()
    
    # Step 5: Setup write paths
    active_query = None
    
    # Path A: Write to console (for debugging)
    if WRITE_TO_CONSOLE:
        active_query = write_to_console(ohlcv_df)
        if active_query:
            log_stream_progress(active_query, logger)
            logger.info("Writing to console output")
    
    # Path B: Write to PostgreSQL (production)
    if WRITE_TO_DB:
        active_query = (
            ohlcv_df.writeStream
            .format("jdbc")
            .option("url", DB_URL)
            .option("dbtable", "gold.gold_crypto_ohlcv")
            .option("user", DB_USER)
            .option("password", DB_PASSWORD)
            .option("checkpointLocation", f"{OHLCV_CHECKPOINT_LOCATION}/postgres")
            .foreachBatch(write_to_postgres)
            .start()
        )
        log_stream_progress(active_query, logger)
        logger.info(f"Writing OHLCV to {DB_URL}")
    
    # Wait for termination
    if active_query:
        logger.info("Streaming processor is running. Press Ctrl+C to stop.")
        active_query.awaitTermination()
    else:
        logger.error("No active write path configured. Exiting.")


if __name__ == "__main__":
    main()
