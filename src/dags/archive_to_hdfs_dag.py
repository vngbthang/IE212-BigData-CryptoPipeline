"""Airflow DAG for hourly Gold-layer archival to HDFS.

This DAG extracts the previous hour from gold.gold_crypto_ohlcv and writes a
single Parquet file to HDFS to avoid small-file proliferation in cold storage.
"""

from __future__ import annotations

import importlib
import logging
import os
import subprocess
import sys
import tempfile
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

HDFS_ARCHIVE_ROOT = "hdfs://namenode:8020/archive"
WEBHDFS_USER = "root"


def _ensure_module(module_name: str, package_name: str | None = None) -> None:
    try:
        importlib.import_module(module_name)
    except ImportError:
        package = package_name or module_name
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", package])


def build_dag() -> DAG:
    with DAG(
        dag_id="archive_to_hdfs_dag",
        description="Hourly archival of previous-hour gold OHLCV data to HDFS as a single Parquet file",
        default_args=DEFAULT_ARGS,
        start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
        schedule_interval="@hourly",
        catchup=False,
        max_active_runs=1,
        tags=["gold", "archive", "hdfs", "parquet", "maintenance"],
        doc_md=__doc__,
    ) as dag:
        start = EmptyOperator(task_id="start")

        @task(task_id="archive_previous_hour_to_hdfs")
        def archive_previous_hour(**context):
            _ensure_module("pandas")
            _ensure_module("pyarrow")
            _ensure_module("requests")

            import pandas as pd
            import pyarrow as pa
            import pyarrow.parquet as pq
            import requests

            hook = PostgresHook(postgres_conn_id="postgres_default")
            conn = hook.get_conn()

            interval_start = context["data_interval_start"].in_timezone("UTC")
            interval_end = context["data_interval_end"].in_timezone("UTC")

            query = """
                SELECT
                    timestamp,
                    symbol,
                    exchange,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    ingest_timestamp,
                    process_timestamp,
                    display_timestamp,
                    record_count,
                    error_flag,
                    error_messages,
                    source_partition,
                    source_offset,
                    created_at,
                    updated_at
                FROM gold.gold_crypto_ohlcv
                WHERE timestamp >= %s
                  AND timestamp < %s
                ORDER BY timestamp ASC, symbol ASC
            """

            frame = pd.read_sql_query(query, conn, params=(interval_start, interval_end))
            conn.close()

            if frame.empty:
                logger.info(
                    "No gold rows found for %s -> %s; skipping archive write",
                    interval_start,
                    interval_end,
                )
                return {"rows": 0, "path": None}

            archive_dir_path = (
                f"/archive/year={interval_start.strftime('%Y')}"
                f"/month={interval_start.strftime('%m')}"
                f"/day={interval_start.strftime('%d')}"
                f"/hour={interval_start.strftime('%H')}"
            )
            archive_file_path = f"{archive_dir_path}/part-00000.snappy.parquet"
            archive_file_uri = f"hdfs://namenode:8020{archive_file_path}"

            table = pa.Table.from_pandas(frame, preserve_index=False)

            local_file = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
            try:
                local_file.close()
                pq.write_table(table, local_file.name, compression="snappy")

                namenode_http = "http://namenode:9870"
                mkdirs_response = requests.put(
                    f"{namenode_http}/webhdfs/v1{archive_dir_path}?op=MKDIRS&user.name={WEBHDFS_USER}",
                    timeout=30,
                )
                mkdirs_response.raise_for_status()

                create_response = requests.put(
                    f"{namenode_http}/webhdfs/v1{archive_file_path}?op=CREATE&overwrite=true&user.name={WEBHDFS_USER}",
                    allow_redirects=False,
                    timeout=30,
                )
                create_response.raise_for_status()

                redirect_url = create_response.headers.get("Location")
                if not redirect_url:
                    raise RuntimeError("WebHDFS did not return a redirect location for file create")

                with open(local_file.name, "rb") as parquet_stream:
                    upload_response = requests.put(
                        redirect_url,
                        data=parquet_stream,
                        timeout=300,
                    )
                    upload_response.raise_for_status()
            finally:
                try:
                    os.unlink(local_file.name)
                except OSError:
                    pass

            logger.info("Archived %s rows to %s", len(frame), archive_file_uri)
            return {"rows": len(frame), "path": archive_file_uri}

        end = EmptyOperator(task_id="end")

        start >> archive_previous_hour() >> end

    return dag


logger.info("Initializing archive DAG module: gold_hourly_archive_to_hdfs")
dag = build_dag()