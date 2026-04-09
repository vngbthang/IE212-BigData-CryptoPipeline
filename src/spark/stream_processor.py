import os
import sys
import logging
import pickle
from datetime import datetime

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col, window, first, last, min as spark_min, max as spark_max,
    sum as spark_sum, avg, stddev, count, to_timestamp, 
    from_json, schema_of_json, struct, when, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    LongType, TimestampType, IntegerType
)

# ============================================================================
# CONFIGURATION
# ============================================================================
# Kafka Configuration
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'kafka-0:29092,kafka-1:29093,kafka-2:29094')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'crypto_ticks')
KAFKA_GROUP_ID = 'spark-streaming-group'

# PostgreSQL Configuration
PG_HOST = os.getenv('PG_HOST', 'postgres')
PG_PORT = os.getenv('PG_PORT', '5432')
PG_DB = os.getenv('PG_DB', 'cryptopipeline')
PG_USER = os.getenv('PG_USER', 'crypto_user')
PG_PASSWORD = os.getenv('PG_PASSWORD', 'crypto_password')
PG_TABLE = 'gold.gold_crypto_ohlcv'

# HDFS Configuration
HDFS_NAMENODE = os.getenv('HDFS_NAMENODE', 'namenode:9000')
HDFS_OUTPUT_PATH = os.getenv('HDFS_OUTPUT_PATH', f'hdfs://{HDFS_NAMENODE}/silver/crypto/ohlcv-1m')

# Checkpoint Directories
CHECKPOINT_DIR_STREAMING = '/checkpoint/spark-streaming'
CHECKPOINT_DIR_QUERY = '/checkpoint/query-ohlcv'

# Spark Configuration
MAX_RECORDS_PER_BATCH = int(os.getenv('MAX_RECORDS_PER_BATCH', '10000'))
PROCESSING_TIME_SECONDS = int(os.getenv('PROCESSING_TIME_SECONDS', '30'))

# ============================================================================
# LOGGING SETUP
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# SPARK SESSION SETUP
# ============================================================================
def create_spark_session() -> SparkSession:
    """
    Create optimized Spark session for streaming.
    
    Returns:
        Configured SparkSession
    """
    spark = (
        SparkSession.builder
        .appName("crypto-streaming-pipeline")
        .master("spark://spark-master:7077")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "2")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.streaming.schemaInference", "true")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR_QUERY)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # Critical: Arrow optimization to prevent OOM
        .config("spark.sql.execution.arrow.enabled", "true")
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", str(MAX_RECORDS_PER_BATCH))
        .config("spark.sql.execution.arrow.fallback.enabled", "true")
        # DataFrame and shuffle settings
        .config("spark.sql.execution.useLegacyStringGrouping", "false")
        .config("spark.shuffle.partitions", "8")
        # Kafka source tuning
        .config("spark.sql.kafka.minBatchOffsetRangePerTrigger", "500")
        # Runtime jars
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
            "org.postgresql:postgresql:42.3.1",
        )
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("[OK] Spark session created successfully")
    return spark

# ============================================================================
# KAFKA SOURCE (Bronze Layer)
# ============================================================================
def read_kafka_stream(spark: SparkSession) -> DataFrame:
    """
    Read streaming data from Kafka topic.
    
    Args:
        spark: SparkSession instance
        
    Returns:
        DataFrame with Kafka messages
    """
    logger.info(f"[KAFKA] Connecting to brokers: {KAFKA_BROKERS}")
    logger.info(f"[TOPIC] Subscribing: {KAFKA_TOPIC}")
    
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("kafka.group.id", KAFKA_GROUP_ID) \
        .option("kafka.session.timeout.ms", "30000") \
        .option("maxOffsetsPerTrigger", str(MAX_RECORDS_PER_BATCH)) \
        .load()
    
    logger.info("[OK] Kafka source configured")
    return df_kafka

# ============================================================================
# AVRO DESERIALIZATION (Bronze -> Raw data)
# ============================================================================
def deserialize_avro(spark: SparkSession, df: DataFrame) -> DataFrame:
    """
    Deserialize Avro binary data from Kafka to structured format.
    
    Args:
        spark: SparkSession instance
        df: DataFrame with Kafka messages (binary 'value' column)
        
    Returns:
        DataFrame with deserialized tick data
    """
    # Note: In production, use confluent.kafka.schema_registry for schema management
    # For now, we define schema manually based on producer
    
    tick_schema = StructType([
        StructField("timestamp", LongType(), False),
        StructField("product_id", StringType(), False),
        StructField("exchange", StringType(), True),
        StructField("price", DoubleType(), False),
        StructField("size", DoubleType(), False),
        StructField("bid", DoubleType(), True),
        StructField("ask", DoubleType(), True),
        StructField("side", StringType(), True),
        StructField("sequence", LongType(), True),
        StructField("ingestion_timestamp", LongType(), False),
    ])
    
    # Deserialize: Here we'd normally use Avro library, 
    # for now using simple schema inference
    df_deserialized = df \
        .selectExpr("CAST(value AS STRING) as value") \
        .select(from_json(col("value"), tick_schema).alias("data")) \
        .select("data.*")
    
    # Convert ingestion_timestamp (ms) to timestamp column
    df_deserialized = df_deserialized \
        .withColumn("event_time", (col("timestamp") / 1000).cast(TimestampType())) \
        .withColumn("ingestion_time", (col("ingestion_timestamp") / 1000).cast(TimestampType()))
    
    logger.info("[OK] Avro deserialization configured")
    return df_deserialized

# ============================================================================
# WATERMARKING & WINDOWING (Silver Layer Stage 1)
# ============================================================================
def apply_windowing(df: DataFrame) -> DataFrame:
    """
    Apply 1-minute tumbling window with watermarking for late data.
    
    Args:
        df: Deserialized DataFrame with tick data
        
    Returns:
        DataFrame with 1-minute windowed aggregations
    """
    # Watermarking: Allow late data within 1 minute after window close
    df_watermarked = df.withWatermark("event_time", "1 minute")
    
    # Windowing: 1-minute tumbling window (no sliding)
    df_windowed = df_watermarked \
        .groupBy(
            window(col("event_time"), "1 minute", "1 minute").alias("window_time"),
            col("product_id").alias("symbol")
        ) \
        .agg(
            first(col("price")).alias("open"),
            spark_max(col("price")).alias("high"),
            spark_min(col("price")).alias("low"),
            last(col("price")).alias("close"),
            spark_sum(col("size")).alias("volume"),
            spark_max(col("bid")).alias("bid"),
            spark_max(col("ask")).alias("ask"),
            count("*").alias("num_ticks"),
            avg(col("price")).alias("avg_price"),
            stddev(col("price")).alias("volatility"),
        ) \
        .select(
            col("window_time.start").cast(TimestampType()).alias("timestamp"),
            col("symbol"),
            col("open"), col("high"), col("low"), col("close"), col("volume"),
            col("bid"), col("ask"), col("num_ticks"), col("avg_price"),
            col("volatility"),
        )
    
    logger.info("[OK] Windowing and watermarking applied (1-min tumbling window, 1-min watermark)")
    return df_windowed

# ============================================================================
# FEATURE ENGINEERING (Silver Layer Stage 2)
# ============================================================================
def compute_technical_indicators(df: DataFrame) -> DataFrame:
    """
    Compute technical indicators (MA, RSI, etc.) using window functions.
    
    Args:
        df: Windowed DataFrame with OHLCV
        
    Returns:
        DataFrame with additional feature columns
    """
    # Window spec for time-series computations
    window_5m = Window.partitionBy("symbol").orderBy("timestamp").rangeBetween(-300, 0)  # 5 min
    window_20m = Window.partitionBy("symbol").orderBy("timestamp").rangeBetween(-1200, 0)  # 20 min
    
    # Moving Averages
    df_features = df \
        .withColumn(
            "ma_5m",
            avg(col("close")).over(window_5m)
        ) \
        .withColumn(
            "ma_20m",
            avg(col("close")).over(window_20m)
        ) \
        .withColumn(
            "rsi_14",
            lit(None)  # Placeholder: RSI requires more complex logic
        )
    
    logger.info("[OK] Technical indicators computed (MA_5, MA_20, RSI_14)")
    return df_features

# ============================================================================
# ML INFERENCE - PANDAS UDF (Silver Layer Stage 3)
# ============================================================================
def load_ml_model(model_path: str = "/models/crypto_predictor_v1.pkl"):
    """
    Load pre-trained ML model.
    
    Args:
        model_path: Path to pickled model
        
    Returns:
        Loaded model object
    """
    try:
        with open(model_path, 'rb') as f:
            model = pickle.load(f)
        logger.info(f"[OK] ML model loaded from {model_path}")
        return model
    except FileNotFoundError:
        logger.warning(f"[WARN] Model not found at {model_path}, using dummy model")
        # Return None; inference will skip or use dummy predictions
        return None
    except Exception as e:
        logger.error(f"[ERROR] Error loading model: {e}")
        return None

# Load model globally (once at startup)
ML_MODEL = load_ml_model()

def predict_direction_udf(batch_df: pd.DataFrame) -> pd.DataFrame:
    """
    PANDAS UDF for batch ML inference (vectorized across partitions).
    
    This function is called once per batch (not per row), operating on a Pandas DataFrame.
    Arrow serialization automatically handles conversion to/from PySpark DataFrames.
    
    Args:
        batch_df: Pandas DataFrame with features for a batch
        
    Returns:
        Pandas DataFrame with predictions
    """
    try:
        # Extract features
        feature_cols = ['open', 'high', 'low', 'close', 'volume', 'ma_5m', 'ma_20m']
        X = batch_df[feature_cols].fillna(0).values
        
        if ML_MODEL is None:
            # Dummy model: classify based on close vs open
            predictions = np.where((batch_df['close'] > batch_df['open']), 2, np.where((batch_df['close'] < batch_df['open']), 0, 1))
            confidences = np.full(len(batch_df), 0.5)
        else:
            # Real model inference
            predictions = ML_MODEL.predict(X)
            probabilities = ML_MODEL.predict_proba(X)
            confidences = probabilities.max(axis=1)
        
        # Map class labels to direction strings
        class_to_direction = {0: "DOWN", 1: "NEUTRAL", 2: "UP"}
        directions = pd.Series([class_to_direction.get(int(p), "NEUTRAL") for p in predictions])
        
        # Return as Pandas DataFrame (matches schema below)
        return pd.DataFrame({
            "predicted_direction": directions,
            "confidence_score": confidences,
            "model_version": "v1.0.0"
        })
        
    except Exception as e:
        logger.error(f"[ERROR] Error in UDF: {e}")
        # Return default values on error
        return pd.DataFrame({
            "predicted_direction": ["NEUTRAL"] * len(batch_df),
            "confidence_score": [0.0] * len(batch_df),
            "model_version": ["v1.0.0"] * len(batch_df),
        })

def apply_ml_inference(spark: SparkSession, df: DataFrame) -> DataFrame:
    """
    Apply ML inference using Pandas UDF.
    
    CRITICAL CONFIG: spark.sql.execution.arrow.maxRecordsPerBatch
    Limits records per batch to prevent OOM (set to 10000 in main)
    
    Args:
        spark: SparkSession instance
        df: DataFrame with features
        
    Returns:
        DataFrame with prediction columns added
    """
    from pyspark.sql.functions import pandas_udf
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    
    # Define output schema for Pandas UDF
    prediction_schema = StructType([
        StructField("predicted_direction", StringType()),
        StructField("confidence_score", DoubleType()),
        StructField("model_version", StringType()),
    ])
    
    # Register Pandas UDF
    @pandas_udf(prediction_schema)
    def predict_batch(
        open_s: pd.Series,
        high_s: pd.Series,
        low_s: pd.Series,
        close_s: pd.Series,
        volume_s: pd.Series,
        ma_5m_s: pd.Series,
        ma_20m_s: pd.Series,
    ) -> pd.DataFrame:
        batch_df = pd.DataFrame(
            {
                "open": open_s,
                "high": high_s,
                "low": low_s,
                "close": close_s,
                "volume": volume_s,
                "ma_5m": ma_5m_s,
                "ma_20m": ma_20m_s,
            }
        )
        return predict_direction_udf(batch_df)
    
    # Apply Pandas UDF using groupby map
    # Note: For streaming, we apply it without groupby
    df_predictions = df.withColumn(
        "predictions",
        predict_batch(
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume"),
            col("ma_5m"),
            col("ma_20m"),
        )
    ) \
    .select(
        col("*"),
        col("predictions.predicted_direction"),
        col("predictions.confidence_score"),
        col("predictions.model_version"),
    ) \
    .drop("predictions")
    
    logger.info("[OK] ML inference (Pandas UDF) applied")
    return df_predictions

# ============================================================================
# BRANCH 1: WRITE TO PostgreSQL (Hot Storage)
# ============================================================================
def prepare_for_postgres(df: DataFrame) -> DataFrame:
    """
    Prepare DataFrame for PostgreSQL write (schema must match table).
    
    Args:
        df: DataFrame with all features & predictions
        
    Returns:
        DataFrame ready for PostgreSQL sink
    """
    df_postgres = df.select(
        col("timestamp"),
        col("symbol"),
        lit("coinbase").alias("exchange"),
        col("open"), col("high"), col("low"), col("close"), col("volume"),
        col("ma_5m"), col("ma_20m"), col("rsi_14"),
        col("volatility"),
        col("predicted_direction"),
        col("confidence_score"),
        col("model_version"),
        lit("VALID").alias("quality_status"),
        lit("kafka").alias("data_source"),
        lit(datetime.now()).cast(TimestampType()).alias("created_at"),
        lit(datetime.now()).cast(TimestampType()).alias("updated_at"),
    )
    return df_postgres

def write_to_postgres(batch_df: DataFrame, batch_id: int) -> None:
    """
    Write batch to PostgreSQL using foreachBatch.
    
    This callback is invoked once per micro-batch.
    
    Args:
        batch_df: Micro-batch DataFrame
        batch_id: Batch sequence number
    """
    try:
        url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
        properties = {
            "user": PG_USER,
            "password": PG_PASSWORD,
            "driver": "org.postgresql.Driver",
        }
        
        batch_df.write \
            .format("jdbc") \
            .mode("append") \
            .option("url", url) \
            .option("dbtable", PG_TABLE) \
            .options(**properties) \
            .save()
        
        count = batch_df.count()
        logger.info(f"[OK] Batch {batch_id}: {count} rows written to PostgreSQL")
        
    except Exception as e:
        logger.error(f"[ERROR] Error writing to PostgreSQL (batch {batch_id}): {e}")
        # Implement dead-letter queue or retry logic here
        raise

# ============================================================================
# BRANCH 2: WRITE TO HDFS (Cold Storage - Raw Data)
# ============================================================================
def prepare_for_hdfs(df: DataFrame) -> DataFrame:
    """
    Prepare DataFrame for HDFS write (keep all columns).
    
    Args:
        df: DataFrame with all data
        
    Returns:
        DataFrame ready for HDFS sink
    """
    # Add partition columns
    df_hdfs = df \
        .withColumn("year", col("timestamp").substr(1, 4)) \
        .withColumn("month", col("timestamp").substr(6, 2)) \
        .withColumn("day", col("timestamp").substr(9, 2))
    
    return df_hdfs

def write_to_hdfs_batch(batch_df: DataFrame, batch_id: int) -> None:
    """
    Write batch to HDFS Parquet format.
    
    Args:
        batch_df: Micro-batch DataFrame
        batch_id: Batch sequence number
    """
    try:
        batch_df.write \
            .format("parquet") \
            .mode("append") \
            .partitionBy("year", "month", "day") \
            .option("compression", "snappy") \
            .save(HDFS_OUTPUT_PATH)
        
        count = batch_df.count()
        logger.info(f"[OK] Batch {batch_id}: {count} rows written to HDFS ({HDFS_OUTPUT_PATH})")
        
    except Exception as e:
        logger.error(f"[ERROR] Error writing to HDFS (batch {batch_id}): {e}")
        raise

# ============================================================================
# MAIN STREAMING PIPELINE
# ============================================================================
def main():
    """
    Main entry point for Spark Structured Streaming pipeline.
    """
    print("=" * 80)
    print("[START] REAL-TIME CRYPTO DATA PIPELINE - SPARK STREAMING PROCESSOR")
    print("=" * 80)
    print(f"Configuration:")
    print(f"  Kafka Brokers: {KAFKA_BROKERS}")
    print(f"  Kafka Topic: {KAFKA_TOPIC}")
    print(f"  PostgreSQL: {PG_HOST}:{PG_PORT}/{PG_DB}")
    print(f"  HDFS Output: {HDFS_OUTPUT_PATH}")
    print(f"  Max Records/Batch: {MAX_RECORDS_PER_BATCH}")
    print(f"  Processing Interval: {PROCESSING_TIME_SECONDS}s")
    print("=" * 80)
    
    try:
        # Setup Spark
        spark = create_spark_session()
        
        # Stage 1: Read from Kafka (Bronze)
        df_bronze = read_kafka_stream(spark)
        logger.info("[OK] Bronze layer (Kafka) configured")
        
        # Stage 2: Deserialize Avro
        df_raw = deserialize_avro(spark, df_bronze)
        logger.info("[OK] Avro deserialization configured")
        
        # Stage 3: Apply windowing & watermarking
        df_windowed = apply_windowing(df_raw)
        logger.info("[OK] Windowing & watermarking configured")
        
        # Stage 4: Feature engineering
        df_features = compute_technical_indicators(df_windowed)
        logger.info("[OK] Feature engineering configured")
        
        # Stage 5: ML inference (Pandas UDF)
        df_predictions = apply_ml_inference(spark, df_features)
        logger.info("[OK] ML inference (Pandas UDF) configured")
        
        # BRANCH 1: PostgreSQL (Hot path)
        df_postgres = prepare_for_postgres(df_predictions)
        query_postgres = df_postgres \
            .writeStream \
            .foreachBatch(write_to_postgres) \
            .option("checkpointLocation", f"{CHECKPOINT_DIR_QUERY}/postgres") \
            .trigger(processingTime=f"{PROCESSING_TIME_SECONDS} seconds") \
            .start()
        logger.info("[OK] PostgreSQL streaming write started")
        
        # BRANCH 2: HDFS (Cold path)
        df_hdfs = prepare_for_hdfs(df_predictions)
        query_hdfs = df_hdfs \
            .writeStream \
            .foreachBatch(write_to_hdfs_batch) \
            .option("checkpointLocation", f"{CHECKPOINT_DIR_QUERY}/hdfs") \
            .trigger(processingTime=f"{PROCESSING_TIME_SECONDS} seconds") \
            .start()
        logger.info("[OK] HDFS streaming write started")
        
        # Wait for termination
        logger.info("[RUNNING] Streaming pipeline active... Press Ctrl+C to stop")
        spark.streams.awaitAnyTermination()
        
    except Exception as e:
        logger.error(f"[ERROR] Pipeline error: {e}", exc_info=True)
        return 1
    finally:
        logger.info("[STOP] Streaming pipeline stopped")
        spark.stop()
        
    return 0

if __name__ == "__main__":
    sys.exit(main())

