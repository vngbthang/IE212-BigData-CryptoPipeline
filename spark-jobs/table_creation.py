"""Iceberg table creation — SUB-15S LATENCY TUNED.

Storage optimizations applied:
  - write.format.default         = 'parquet'
  - write.parquet.compression-codec = 'zstd'   (fastest decompression for Trino)
  - write.object-storage.enabled = 'true'      (deterministic hash-path on MinIO,
                                                bypasses linear dir-indexing)
  - write.metadata.delete-after-commit.enabled = 'true'
  - write.metadata.previous-versions-max = '5'

Time-travel & snapshot retention (orchestrator requirements):
  - history.expire.max-snapshot-age-ms = 3600000 (1 hour — lean metadata for DuckDB)
  - rewrites.deletes.min-files-to-rewrite = 100  (skip tiny file rewrites)
  - write.wap.enabled = 'true'                   (Write-Audit-Publish for Nessie branches)
"""
from pyspark.sql import SparkSession


def create_tables(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")

    # ── Gold layer: OHLCV aggregates ─────────────────────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.gold.crypto_ohlcv (
            symbol          STRING,
            window_start    TIMESTAMP,
            window_end      TIMESTAMP,
            open            DOUBLE,
            high            DOUBLE,
            low             DOUBLE,
            close           DOUBLE,
            volume          DOUBLE,
            tick_count      DOUBLE,
            log_return      DOUBLE,
            volatility_30m  DOUBLE,
            z_score         DOUBLE,
            anomaly_label   STRING,
            anomaly_score   DOUBLE,
            ingestion_time  TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (symbol, days(window_start))
        TBLPROPERTIES (
            -- Format & compression
            'format-version'                           = '2',
            'write.format.default'                    = 'parquet',
            'write.parquet.compression-codec'         = 'zstd',
            -- Object storage hash-path (eliminates MinIO dir-indexing lag)
            'write.object-storage.enabled'            = 'true',
            -- Commit-time metadata cleanup
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max'   = '5',
            -- Snapshot expiration: 1-hour retention (lean metadata for DuckDB)
            'history.expire.max-snapshot-age-ms'     = '3600000',
            'expire.snapshot.min-snapshot-age-ms'    = '3600000',
            -- Write-Audit-Publish for Nessie branch safety
            'write.wap.enabled'                      = 'true',
            -- Garbage collection
            'gc-enabled'                              = 'true',
            -- Split sizing for Trino/DuckDB predicate pushdown
            'read.split.target-size'                   = '134217728'
        )
    """)

    # ── Bronze layer: raw ticks ──────────────────────────────────────────────
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.bronze.crypto_ticks (
            symbol         STRING,
            price          DOUBLE,
            volume         DOUBLE,
            timestamp      TIMESTAMP,
            exchange       STRING,
            ingestion_time TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (symbol, days(timestamp))
        TBLPROPERTIES (
            'format-version'                           = '2',
            'write.format.default'                    = 'parquet',
            'write.parquet.compression-codec'         = 'zstd',
            'write.object-storage.enabled'            = 'true',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max'   = '5',
            -- Snapshot expiration: 1-hour retention (lean metadata for DuckDB)
            'history.expire.max-snapshot-age-ms'     = '3600000',
            'expire.snapshot.min-snapshot-age-ms'    = '3600000',
            'write.wap.enabled'                      = 'true',
            'gc-enabled'                              = 'true',
            'read.split.target-size'                   = '134217728'
        )
    """)
