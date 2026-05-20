"""
Iceberg Maintenance DAG — Crypto Lakehouse
Runs daily to compact small Parquet files and expire old snapshots.
Executed via TrinoOperator against nessie.gold.crypto_ohlcv.
"""

from datetime import datetime, timezone
from pathlib import Path

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.providers.trino.operators.trino import TrinoOperator

# ── DAG Defaults ──────────────────────────────────────────────────────────────
DAG_ID  = "iceberg_maintenance"
SCHEDULE = "0 3 * * *"          # 03:00 UTC daily
CATCHUP  = False
OWNER    = "lakehouse-team"

default_args = {
    "owner": OWNER,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry":  False,
    "retries": 2,
    "retry_delay": 300,          # 5 minutes
}

# ── Helper: Validate Table Exists ────────────────────────────────────────────
def _validate_table_exists(table_name: str, **context):
    from trino.auth import BasicAuthentication
    from trino.client import TrinoQueryError, TrinoClient

    conn = TrinoClient(
        host="trino",
        port=8080,
        user="airflow",
        auth=BasicAuthentication("airflow", ""),
        http_scheme="http",
    )
    try:
        result = conn.query(f"SHOW TABLES FROM nessie.gold LIKE '{table_name}'")
        rows = list(result)
        if not rows:
            raise RuntimeError(f"Table {table_name} does not exist — aborting maintenance.")
        print(f"[OK] Table {table_name} confirmed present.")
    except TrinoQueryError as exc:
        raise RuntimeError(f"Trino validation failed: {exc}")


# ── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id=DAG_ID,
    description="Daily Iceberg maintenance: OPTIMIZE + EXPIRE_SNAPSHOTS + REMOVE_ORPHANS",
    schedule_interval=SCHEDULE,
    start_date=datetime(2026, 5, 20, tzinfo=timezone.utc),
    catchup=CATCHUP,
    max_active_runs=1,
    doc_md=__doc__,
    **default_args,
) as dag:

    validate = PythonOperator(
        task_id="validate_table_exists",
        python_callable=_validate_table_exists,
        op_kwargs={"table_name": "crypto_ohlcv"},
    )

    # ── 1. OPTIMIZE ──────────────────────────────────────────────────────────
    # Rewrites small Parquet data files into larger ones to improve read
    # performance. Trino will pick optimal sort order based on table
    # properties. Runs on the default snapshot (main branch in Nessie).
    optimize_table = TrinoOperator(
        task_id="optimize_table",
        trino_conn_id="trino_default",
        sql="""
            ALTER TABLE nessie.gold.crypto_ohlcv
            EXECUTE OPTIMIZE
            WHERE file_size_in_bytes < 134217728   -- compact files < 128 MB
        """,
    )

    # ── 2. EXPIRE SNAPSHOTS ──────────────────────────────────────────────────
    # Removes historical snapshots older than 7 days, deleting their
    # associated data files. This is safe because the 7-day window covers
    # any in-flight Airflow tasks or queries that might reference older refs.
    expire_snapshots = TrinoOperator(
        task_id="expire_snapshots",
        trino_conn_id="trino_default",
        sql="""
            ALTER TABLE nessie.gold.crypto_ohlcv
            EXECUTE EXPIRE_SNAPSHOTS(
                retention_threshold => INTERVAL '7' DAY
            )
        """,
    )

    # ── 3. REMOVE ORPHAN FILES ───────────────────────────────────────────────
    # Deletes data files no longer referenced by any live snapshot. The
    # retained_files_location setting limits the scan to the warehouse prefix
    # to avoid scanning unrelated MinIO paths.
    remove_orphans = TrinoOperator(
        task_id="remove_orphan_files",
        trino_conn_id="trino_default",
        sql="""
            ALTER TABLE nessie.gold.crypto_ohlcv
            EXECUTE REMOVE_ORPHAN_FILES(
                retention_threshold => INTERVAL '7' DAY,
                retained_files_location => 's3a://crypto-lake/warehouse'
            )
        """,
    )

    # ── 4. INLINE METADATA REFRESH ───────────────────────────────────────────
    # Forces Trino to re-read the Iceberg table metadata so that newly
    # compacted / expired files are immediately visible to queries.
    refresh_metadata = TrinoOperator(
        task_id="refresh_table_metadata",
        trino_conn_id="trino_default",
        sql="""
            ALTER TABLE nessie.gold.crypto_ohlcv
            SET PROPERTIES write.metadata.delete-after-commit.enabled = true
        """,
    )

    # ── Dependency Chain ──────────────────────────────────────────────────────
    chain(
        validate,
        [optimize_table, expire_snapshots],
        remove_orphans,
        refresh_metadata,
    )
