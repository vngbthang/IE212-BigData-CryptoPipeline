from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Raw tick schema consumed from Kafka
# Note: price/volume are StringType because the simulator serializes them as JSON strings
# to avoid floating-point precision loss. Spark casts them implicitly in the SELECT.
tick_schema = StructType([
    StructField("symbol",    StringType(),    False),
    StructField("price",     StringType(),    False),
    StructField("volume",    StringType(),    False),
    StructField("timestamp", TimestampType(), False),
    StructField("exchange",  StringType(),    True),
])

# OHLCV aggregation output schema — matches nessie.gold.crypto_ohlcv Iceberg table
ohlcv_schema = StructType([
    StructField("symbol",          StringType(),    False),
    StructField("window_start",    TimestampType(), False),
    StructField("window_end",      TimestampType(), False),
    StructField("open",            DoubleType(),    False),
    StructField("high",            DoubleType(),    False),
    StructField("low",             DoubleType(),    False),
    StructField("close",           DoubleType(),    False),
    StructField("volume",          DoubleType(),    False),
    StructField("tick_count",      DoubleType(),    False),
    StructField("log_return",      DoubleType(),    True),
    StructField("volatility_30m",  DoubleType(),   True),
    StructField("z_score",         DoubleType(),    True),
    StructField("anomaly_label",   StringType(),    True),
    StructField("anomaly_score",   DoubleType(),    True),
    StructField("ingestion_time",  TimestampType(), False),
])
