"""Airflow DAG for PostgreSQL data retention and partition maintenance.

This DAG runs daily at midnight, ensures future partitions exist for
`gold.gold_crypto_ohlcv`, and removes Gold-layer OHLCV records older than 7 days.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
	"owner": "data-engineering",
	"depends_on_past": False,
	"retries": 3,
	"retry_delay": timedelta(minutes=5),
}

SQL_DELETE_OLD_GOLD_ROWS = """
DELETE FROM gold.gold_crypto_ohlcv
WHERE "timestamp" < (CURRENT_TIMESTAMP - INTERVAL '7 days');
"""

SQL_CREATE_FUTURE_PARTITIONS = """
-- Call the function from init.sql that creates WEEKLY partitions
-- (matching the partition strategy from init.sql)
SELECT gold.ensure_future_partitions(days_ahead => 7);
"""


def build_dag() -> DAG:
	"""Create the retention DAG."""

	with DAG(
		dag_id="retention_dag",
		description=(
			"Daily retention cleanup plus partition pre-allocation for gold.gold_crypto_ohlcv"
		),
		default_args=DEFAULT_ARGS,
		start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
		schedule_interval="@daily",
		catchup=False,
		max_active_runs=1,
		tags=["gold", "retention", "postgres", "maintenance"],
		doc_md=__doc__,
	) as dag:
		start = EmptyOperator(task_id="start")

		ensure_future_partitions = PostgresOperator(
			task_id="ensure_future_gold_partitions",
			postgres_conn_id="postgres_default",
			sql=SQL_CREATE_FUTURE_PARTITIONS,
			autocommit=True,
		)

		delete_old_records = PostgresOperator(
			task_id="delete_old_gold_ohlcv_records",
			postgres_conn_id="postgres_default",
			sql=SQL_DELETE_OLD_GOLD_ROWS,
			autocommit=True,
		)

		end = EmptyOperator(task_id="end")

		start >> ensure_future_partitions >> delete_old_records >> end

	return dag


logger.info("Initializing retention DAG module: gold_data_retention_daily")
dag = build_dag()
