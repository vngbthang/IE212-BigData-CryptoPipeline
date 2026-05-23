# IE212 Big Data Crypto Pipeline

Real-time crypto data pipeline for a Big Data course demo.

The currently verified pipeline is:

```text
crypto-feeder -> Kafka -> Spark Structured Streaming -> Iceberg/Nessie/MinIO -> DuckDB -> Streamlit dashboard
```

This README describes the repository as it works today. It does not describe planned components as active services.

## Project Overview

The project ingests live crypto market data from Binance through the feeder container, publishes tick events to Kafka, aggregates them into OHLCV candles with Spark Structured Streaming, writes the result as an Apache Iceberg table using a Nessie catalog and MinIO object storage, and visualizes the latest candles in a Streamlit dashboard through DuckDB's Iceberg scanner.

The dashboard has ML signal sections, but the current verified state uses fallback model outputs unless real model artifacts are added.

## Verified Architecture

```text
+----------------+      +----------------+      +--------------------------+
| crypto-feeder  | ---> | Kafka          | ---> | Spark Structured         |
| simulator.py   |      | crypto_ticks   |      | Streaming                |
+----------------+      +----------------+      +-------------+------------+
                                                              |
                                                              v
                                             +----------------+-------------+
                                             | Iceberg table                |
                                             | nessie.gold.crypto_ohlcv    |
                                             +----------------+-------------+
                                                              |
                                +-----------------------------+-------------+
                                |                                           |
                                v                                           v
                         +------+-------+                           +-------+------+
                         | Nessie       |                           | MinIO        |
                         | catalog      |                           | S3 storage   |
                         +--------------+                           +-------+------+
                                                                          |
                                                                          v
                                                                +---------+--------+
                                                                | DuckDB           |
                                                                | httpfs + iceberg |
                                                                +---------+--------+
                                                                          |
                                                                          v
                                                                +---------+--------+
                                                                | Streamlit        |
                                                                | dashboard        |
                                                                +------------------+
```

## Tech Stack

- Python
- Docker Compose
- Kafka, using `confluentinc/cp-kafka:7.6.1`
- Spark Structured Streaming
- Apache Iceberg
- Project Nessie, using `projectnessie/nessie:0.76.6`
- MinIO object storage
- DuckDB with `httpfs` and `iceberg` extensions
- Streamlit dashboard
- Binance/ccxt feeder

## Folder Structure

```text
.
|-- docker-compose.yaml
|-- README.md
|-- simulator/
|   |-- Dockerfile
|   |-- requirements.txt
|   `-- simulator.py
|-- spark/
|   `-- Dockerfile
|-- spark-jobs/
|   |-- main.py
|   |-- spark_session.py
|   |-- table_creation.py
|   |-- data_processing.py
|   |-- iceberg_operations.py
|   |-- schemas.py
|   `-- __init__.py
|-- src/
|   `-- dashboard/
|       |-- Dockerfile
|       |-- requirements.txt
|       |-- app.py
|       `-- __init__.py
|-- scripts/
|   `-- helper scripts
`-- docs/
    `-- images/
```

## Data Flow

1. `crypto-feeder` reads live Binance market data with ccxt.
2. The feeder publishes tick messages to Kafka topic `crypto_ticks`.
3. Spark reads `crypto_ticks` as a streaming source.
4. Spark parses tick data and aggregates it into OHLCV candles.
5. Spark writes the candle table to Iceberg as `nessie.gold.crypto_ohlcv`.
6. Iceberg data and metadata are stored in MinIO under the `warehouse` bucket.
7. Nessie tracks the Iceberg catalog state.
8. The dashboard uses boto3 to find the newest Iceberg metadata file in MinIO.
9. DuckDB loads `httpfs` and `iceberg`, scans that metadata with `iceberg_scan(...)`, and returns candle rows.
10. Streamlit renders the latest candles, KPI sections, and fallback ML signal widgets.

## Docker Compose Services

| Compose service | Container name | Purpose | Port |
| --- | --- | --- | --- |
| `catalog` | `catalog` | Nessie catalog | internal `19120` |
| `storage` | `storage` | MinIO object storage | `9000`, `9001` |
| `mc` | `mc` | MinIO bucket initialization | none |
| `kafka` | `kafka` | Kafka broker and topic setup | `9092` |
| `spark` | `spark-master` | Spark streaming job | `4040`, `7077` |
| `crypto-feeder` | `crypto-feeder` | Binance/ccxt Kafka producer | none |
| `dashboard` | `crypto-dashboard` | Streamlit dashboard | `8501` |

Services currently verified as part of the running stack: `catalog`, `storage`, `mc`, `kafka`, `spark`, `crypto-feeder`, and `dashboard`.

## Run On Windows PowerShell

From the repository root:

```powershell
docker compose config
docker compose build
docker compose up -d
docker compose ps
```

Open the dashboard:

```text
http://localhost:8501/
```

Useful local URLs:

```text
Streamlit dashboard: http://localhost:8501/
MinIO console:       http://localhost:9001/    user: admin, password: password
Spark UI:            http://localhost:4040/
```

## Operational Scripts

PowerShell helper scripts are available under `scripts/ops/`.

Available scripts:

```powershell
.\scripts\ops\start.ps1
.\scripts\ops\status.ps1
.\scripts\ops\logs.ps1
.\scripts\ops\stop.ps1
.\scripts\ops\reset-checkpoint.ps1
```

Run them from the repository root. Direct execution works when local PowerShell script execution is allowed:

```powershell
.\scripts\ops\status.ps1
```

If PowerShell blocks script execution, use a process-scoped execution policy bypass:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\ops\status.ps1
```

Script purposes:

- `start.ps1`: validates Compose config, builds images, starts the stack, and prints service status.
- `status.ps1`: shows Compose status, checks catalog/storage/Kafka health, looks for Spark writes to `nessie.gold.crypto_ohlcv`, verifies a real Iceberg row through DuckDB in the dashboard container, and checks whether the dashboard responds at `http://localhost:8501`.
- `logs.ps1`: prints useful recent logs for `crypto-feeder`, `spark`, `dashboard`, `catalog`, and `kafka`.
- `stop.ps1`: stops Compose services without deleting volumes, MinIO data, Iceberg data, or Nessie data.
- `reset-checkpoint.ps1`: deletes only the Spark OHLCV streaming checkpoint at `s3a://warehouse/checkpoints/ohlcv/` using the MinIO path `minio/warehouse/checkpoints/ohlcv`, then restarts Spark. Use this when Spark keeps old streaming settings from a previous checkpoint.

Recommended daily/demo workflow:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\ops\start.ps1
powershell -ExecutionPolicy Bypass -File .\scripts\ops\status.ps1
```

Then open:

```text
http://localhost:8501
```

For debugging:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\ops\logs.ps1
```

When finished:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\ops\stop.ps1
```

Recovery workflow:

Use `reset-checkpoint.ps1` only when Spark streaming checkpoint/state causes issues, such as old streaming settings being preserved after a config change.

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\ops\reset-checkpoint.ps1
```

This deletes only:

```text
s3a://warehouse/checkpoints/ohlcv/
minio/warehouse/checkpoints/ohlcv
```

It does not delete:

- Iceberg table data
- Nessie catalog data
- MinIO warehouse data
- Docker volumes

The operational scripts were verified in this repository state:

```text
start.ps1 PASS
status.ps1 PASS
logs.ps1 PASS
stop.ps1 PASS
restart after stop PASS
reset-checkpoint.ps1 PASS
final status.ps1 PASS
```

## Verify Each Layer

### 1. Feeder

```powershell
docker compose logs --tail 80 crypto-feeder
```

Expected evidence:

```text
Kafka BTC/USDT
Kafka ETH/USDT
```

or similar messages showing live symbols being published.

### 2. Kafka

```powershell
docker compose ps kafka
```

Expected state: the `kafka` service is running and healthy.

Optional topic check:

```powershell
docker compose exec kafka kafka-topics --bootstrap-server kafka:9092 --list
```

Expected topic:

```text
crypto_ticks
```

### 3. Spark Structured Streaming

```powershell
docker compose logs --tail 150 spark
```

Expected evidence:

```text
spark.sql.shuffle.partitions -> 4
Batch 0 written to nessie.gold.crypto_ohlcv
```

Later batch numbers are also valid.

### 4. Nessie, Iceberg, and MinIO

```powershell
docker compose ps catalog storage
```

Expected state: `catalog` and `storage` are running and healthy.

Spark logs should show successful writes to:

```text
nessie.gold.crypto_ohlcv
```

Iceberg metadata is stored in MinIO under a path similar to:

```text
s3://warehouse/gold/crypto_ohlcv_<uuid>/metadata/<version>.metadata.json
```

### 5. Dashboard

```powershell
docker compose logs --tail 120 dashboard
```

Expected evidence: no critical errors related to DuckDB, `httpfs`, Iceberg, S3 credentials, missing metadata, or empty query results.

Open:

```text
http://localhost:8501/
```

Expected UI:

- Page loads without a Streamlit crash.
- Latest candle timestamp is visible.
- Candlestick/price chart renders.
- Volume or KPI sections render.

### 6. Optional DuckDB/Iceberg Query Verification

This verifies the dashboard container can load DuckDB extensions, find the latest Iceberg metadata in MinIO, and read at least one candle row.

```powershell
@'
import os
import boto3
import duckdb

endpoint = os.getenv('MINIO_ENDPOINT', 'storage:9000')
bucket = os.getenv('MINIO_BUCKET', 'warehouse')
access = os.getenv('MINIO_ACCESS_KEY', 'admin')
secret = os.getenv('MINIO_SECRET_KEY', 'password')

s3 = boto3.client(
    's3',
    endpoint_url=f'http://{endpoint}',
    aws_access_key_id=access,
    aws_secret_access_key=secret,
    region_name='us-east-1',
)

resp = s3.list_objects_v2(Bucket=bucket, Prefix='gold/', Delimiter='/')
prefixes = [
    p['Prefix'].rstrip('/')
    for p in resp.get('CommonPrefixes', [])
    if 'crypto_ohlcv' in p['Prefix']
]

metas = []
for prefix in prefixes:
    meta_resp = s3.list_objects_v2(Bucket=bucket, Prefix=f'{prefix}/metadata/')
    metas.extend(
        obj for obj in meta_resp.get('Contents', [])
        if obj['Key'].endswith('.metadata.json')
    )

if not metas:
    raise SystemExit('No Iceberg metadata files found')

metas.sort(key=lambda obj: obj['LastModified'], reverse=True)
metadata_uri = f"s3://{bucket}/{metas[0]['Key']}"

conn = duckdb.connect(':memory:')
conn.execute('LOAD httpfs;')
conn.execute('LOAD iceberg;')
conn.execute(f"SET s3_endpoint='{endpoint}';")
conn.execute(f"SET s3_access_key_id='{access}';")
conn.execute(f"SET s3_secret_access_key='{secret}';")
conn.execute("SET s3_url_style='path';")
conn.execute("SET s3_use_ssl=false;")
conn.execute("SET s3_region='us-east-1';")

rows = conn.execute(f"""
    SELECT symbol, window_start, open, high, low, close, volume
    FROM iceberg_scan('{metadata_uri}')
    ORDER BY window_start DESC
    LIMIT 1
""").fetchall()

print('row_count=', len(rows))
print('rows=', rows)
'@ | docker compose exec -T dashboard python -
```

Expected result:

```text
row_count= 1
```

or a larger non-zero result if the query is changed to return more rows.

## Known Limitations

- ML models are fallback only unless real model artifacts are added and wired into the dashboard.
- The dashboard reads the latest Iceberg metadata directly from MinIO and scans it with DuckDB. It does not query the table through Nessie catalog APIs.
- The current verified streaming output is the Iceberg table `nessie.gold.crypto_ohlcv`. Do not assume a full Bronze/Silver/Gold lakehouse is implemented unless the code is extended.
- Streamlit logs currently include deprecation warnings for `use_container_width`; these warnings do not block the dashboard.
- The feeder depends on network access to Binance. If Binance or outbound network access is unavailable, the feeder cannot produce live data.

## Troubleshooting

### Docker daemon is not running

Symptom:

```text
Cannot connect to the Docker daemon
```

Fix:

Start Docker Desktop, wait until it reports that the engine is running, then retry:

```powershell
docker compose ps
```

### Nessie image pull or tag issue

Symptom:

```text
projectnessie/nessie:<tag>: not found
```

The current verified image is:

```text
projectnessie/nessie:0.76.6
```

Validate and pull only the catalog image:

```powershell
docker compose config
docker compose pull catalog
```

### DuckDB extension issue

Symptom:

```text
LOAD httpfs failed
LOAD iceberg failed
```

The dashboard image pre-installs DuckDB extensions during build. Rebuild the dashboard image with network access:

```powershell
docker compose build dashboard
docker compose run --rm --no-deps dashboard python -c "import duckdb; c=duckdb.connect(':memory:'); c.execute('LOAD httpfs;'); c.execute('LOAD iceberg;'); print('duckdb extensions loaded')"
```

### Spark checkpoint preserves old configs

Symptom:

```text
spark.sql.shuffle.partitions -> 200
```

The current Spark config sets:

```text
spark.sql.shuffle.partitions -> 4
```

If logs still show `200`, reset only the streaming checkpoint for the OHLCV query. Do not delete the Iceberg warehouse or Nessie/MinIO data.

The verified checkpoint path for the current query is:

```text
s3a://warehouse/checkpoints/ohlcv/
```

Equivalent MinIO path:

```text
minio/warehouse/checkpoints/ohlcv
```

Safe targeted reset:

```powershell
docker compose stop spark
docker compose run --rm --entrypoint /bin/sh mc -c "mc alias set minio http://storage:9000 admin password && mc rm --recursive --force minio/warehouse/checkpoints/ohlcv"
docker compose up -d spark
docker compose logs --tail 200 spark
```

### Binance or network access issue

Symptom:

```text
Binance cannot be reached
```

or feeder logs show repeated exchange/network errors.

Check feeder logs:

```powershell
docker compose logs --tail 120 crypto-feeder
```

The current feeder uses live Binance/ccxt behavior. It does not currently switch to a fake simulator when Binance is unavailable.

## Current Demo Readiness

The project is ready for a course demo of the verified streaming data engineering pipeline:

```text
crypto-feeder -> Kafka -> Spark Structured Streaming -> Iceberg/Nessie/MinIO -> DuckDB -> Streamlit
```

For the demo, describe the ML widgets as fallback/demo signals unless real model artifacts are added.
