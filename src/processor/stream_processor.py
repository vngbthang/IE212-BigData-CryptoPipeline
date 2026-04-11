import logging
import os
import threading
import time
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.types import DecimalType, TimestampType

APP_NAME = "CryptoPipeline_Silver_Processor"
KAFKA_BOOTSTRAP_SERVERS = "kafka-0:9092,kafka-1:9092,kafka-2:9092"
KAFKA_TOPIC = "crypto_ticks"
CHECKPOINT_LOCATION = "/tmp/spark_checkpoints/raw_ticks"


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
    return (
        raw_df.selectExpr("CAST(value AS STRING) AS value")
        .select(from_json(col("value"), schema).alias("payload"))
        .select(
            col("payload.type").alias("type"),
            col("payload.product_id").alias("product_id"),
            col("payload.price").cast(DecimalType(18, 8)).alias("price"),
            col("payload.time").cast(TimestampType()).alias("time"),
            col("payload.size").cast(DecimalType(18, 8)).alias("size"),
        )
    )


def write_to_console(parsed_df: DataFrame):
    return (
        parsed_df.writeStream.format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )


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

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    schema = create_tick_schema()
    raw_df = read_kafka_stream(spark)
    parsed_df = parse_tick_payload(raw_df, schema)

    parsed_df.printSchema()

    query = write_to_console(parsed_df)
    log_stream_progress(query, logger)

    logger.info("Streaming skeleton is running. Press Ctrl+C to stop.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
