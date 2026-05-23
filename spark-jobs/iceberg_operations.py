"""Iceberg operations specific to the crypto pipeline: compaction and orphan-file cleanup."""
import time
from pyspark.sql import SparkSession


def count_data_files(spark: SparkSession, table_name: str) -> int:
    files_df = spark.read.format("iceberg").load(f"nessie.gold.{table_name}.files")
    return files_df.count()


def compact_table(
    spark: SparkSession,
    table_name: str,
    file_count_threshold: int = 200,
    file_size_threshold: int = 64 * 1024 * 1024,
    min_input_files: int = 5,
) -> None:
    count = count_data_files(spark, table_name)
    print(f"[{table_name}] data file count: {count}")
    if count > file_count_threshold:
        print(f"[{table_name}] Triggering data-file compaction …")
        spark.sql(f"""
            CALL nessie.system.rewrite_data_files(
                table => 'nessie.gold.{table_name}',
                FILE_SIZE_THRESHOLD => {file_size_threshold},
                MIN_INPUT_FILES => {min_input_files}
            )
        """).show(truncate=False)


def remove_orphan_files(
    spark: SparkSession,
    table_name: str,
    older_than_seconds: int = 3600,
) -> None:
    older_than_ms = int(time.time() * 1000) - older_than_seconds * 1000
    print(f"[{table_name}] Removing orphan files older than {older_than_seconds}s …")
    spark.sql(f"""
        CALL nessie.system.remove_orphan_files(
            table   => 'nessie.gold.{table_name}',
            older_than => {older_than_ms}
        )
    """).show(truncate=False)


def expire_snapshots(
    spark: SparkSession,
    table_name: str,
    older_than_hours: int = 24,
) -> None:
    older_than_ms = int(time.time() * 1000) - older_than_hours * 3600 * 1000
    print(f"[{table_name}] Expiring snapshots older than {older_than_hours}h …")
    spark.sql(f"""
        CALL nessie.system.expire_snapshots(
            table => 'nessie.gold.{table_name}',
            older_than => {older_than_ms},
            retain_last => 10
        )
    """).show(truncate=False)
