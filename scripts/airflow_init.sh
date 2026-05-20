-- Airflow connection init script: runs once on first Airflow startup
-- Creates the Trino connection so TrinoOperator tasks work out of the box

-- The trino_default connection (created via Airflow UI or REST API):
--   Conn Id:     trino_default
--   Conn Type:   Trino
--   Host:        trino
--   Port:        8080
--   Extra:       {"catalog": "nessie", "schema": "gold"}
