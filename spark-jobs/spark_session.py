from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "CryptoOHLCVStreaming") -> SparkSession:
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
        .config("spark.sql.catalog.nessie.uri", "http://catalog:19120/api/v1") \
        .config("spark.sql.catalog.nessie.ref", "main") \
        .config("spark.sql.catalog.nessie.authentication.type", "NONE") \
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse/") \
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.nessie.cache-enabled", "true") \
        .config("spark.sql.catalog.nessie.gc-enabled", "true") \
        .config("spark.sql.defaultCatalog", "nessie") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://storage:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.sql.streaming.checkpointLocation", "s3a://warehouse/checkpoints/") \
        .config("spark.driver.allowMultipleContexts", "true")

    # Iceberg snapshot retention for time-travel and metadata cleanup
    # Retains 1 hour of history for time-travel queries
    builder = builder \
        .config("spark.sql.catalog.nessie.write.wap.enabled", "true") \
        .config("spark.sql.catalog.nessie.snapshot-age-grace", "3600000") \
        .config("spark.sql.catalog.nessie.expire-snapshots-enabled", "true")

    return builder.getOrCreate()
