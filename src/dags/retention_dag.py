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
DO $$
DECLARE
	partition_date DATE;
	partition_name TEXT;
	partition_covered BOOLEAN;
BEGIN
	FOR i IN 0..7 LOOP
		partition_date := CURRENT_DATE + i;
		partition_name := format('gold_crypto_ohlcv_%s', to_char(partition_date, 'YYYY_MM_DD'));

		SELECT EXISTS (
			SELECT 1
			FROM pg_inherits inh
			JOIN pg_class child ON child.oid = inh.inhrelid
			CROSS JOIN LATERAL (
				SELECT
					split_part(
						split_part(pg_get_expr(child.relpartbound, child.oid), 'FROM (''', 2),
						''') TO',
						1
					)::date AS lower_bound,
					split_part(
						split_part(pg_get_expr(child.relpartbound, child.oid), 'TO (''', 2),
						''')',
						1
					)::date AS upper_bound
			) bounds
			WHERE inh.inhparent = 'gold.gold_crypto_ohlcv'::regclass
			  AND partition_date >= bounds.lower_bound
			  AND partition_date < bounds.upper_bound
		) INTO partition_covered;

		IF partition_covered THEN
			CONTINUE;
		END IF;

		EXECUTE format(
			'CREATE TABLE IF NOT EXISTS gold.%I PARTITION OF gold.gold_crypto_ohlcv '
			'FOR VALUES FROM (%L) TO (%L);',
			partition_name,
			partition_date::text,
			(partition_date + 1)::text
		);
	END LOOP;
END $$;
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
