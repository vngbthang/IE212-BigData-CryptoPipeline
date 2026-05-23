$ErrorActionPreference = "Stop"

Set-Location (Join-Path $PSScriptRoot "..\..")

function Write-Section {
    param([string]$Title)
    Write-Host ""
    Write-Host "== $Title =="
}

function Test-ServiceHealthy {
    param(
        [string]$ServiceName,
        [string]$ContainerName
    )

    $state = docker inspect --format "{{.State.Health.Status}}" $ContainerName 2>$null
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($state)) {
        Write-Host "$ServiceName health: unavailable"
        return
    }

    Write-Host "$ServiceName health: $state"
}

Write-Section "Compose Services"
docker compose ps

Write-Section "Health Checks"
Test-ServiceHealthy -ServiceName "catalog" -ContainerName "catalog"
Test-ServiceHealthy -ServiceName "storage" -ContainerName "storage"
Test-ServiceHealthy -ServiceName "kafka" -ContainerName "kafka"

Write-Section "Spark Iceberg Writes"
try {
    $sparkLogs = & docker compose logs --tail 300 spark 2>&1
    $sparkWrites = $sparkLogs | Select-String -Pattern "Batch .* written to nessie.gold.crypto_ohlcv"
    if ($sparkWrites) {
        Write-Host "Spark writes: found"
        $sparkWrites | Select-Object -Last 5
    } else {
        Write-Host "Spark writes: not found in the last 300 Spark log lines"
    }
} catch {
    Write-Host "Spark writes: log hint unavailable"
    Write-Host "Warning: $($_.Exception.Message)"
    Write-Host "Continuing to the real DuckDB/Iceberg data check..."
}

Write-Section "Iceberg Data Check"
$dataCheckScript = @'
import os
import sys

import boto3
import duckdb

endpoint = os.getenv("MINIO_ENDPOINT", "storage:9000")
bucket = os.getenv("MINIO_BUCKET", "warehouse")
access = os.getenv("MINIO_ACCESS_KEY", "admin")
secret = os.getenv("MINIO_SECRET_KEY", "password")

try:
    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{endpoint}",
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name="us-east-1",
    )

    response = s3.list_objects_v2(Bucket=bucket, Prefix="gold/", Delimiter="/")
    table_prefixes = [
        item["Prefix"].rstrip("/")
        for item in response.get("CommonPrefixes", [])
        if "crypto_ohlcv" in item["Prefix"]
    ]

    if not table_prefixes:
        raise RuntimeError("No gold crypto_ohlcv table prefix found in MinIO")

    metadata_files = []
    for table_prefix in table_prefixes:
        metadata_response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=f"{table_prefix}/metadata/",
        )
        metadata_files.extend(
            item for item in metadata_response.get("Contents", [])
            if item["Key"].endswith(".metadata.json")
        )

    if not metadata_files:
        raise RuntimeError("No Iceberg metadata JSON files found for crypto_ohlcv")

    metadata_files.sort(key=lambda item: item["LastModified"], reverse=True)
    metadata_uri = f"s3://{bucket}/{metadata_files[0]['Key']}"

    conn = duckdb.connect(":memory:")
    conn.execute("LOAD httpfs;")
    conn.execute("LOAD iceberg;")
    conn.execute(f"SET s3_endpoint='{endpoint}';")
    conn.execute(f"SET s3_access_key_id='{access}';")
    conn.execute(f"SET s3_secret_access_key='{secret}';")
    conn.execute("SET s3_url_style='path';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_region='us-east-1';")

    row_count = conn.execute(
        f"SELECT COUNT(*) FROM (SELECT 1 FROM iceberg_scan('{metadata_uri}') LIMIT 1)"
    ).fetchone()[0]

    print(f"metadata={metadata_uri}")
    print(f"row_count={row_count}")
    if row_count > 0:
        print("PASS: DuckDB can read at least one Iceberg candle row")
        sys.exit(0)

    raise RuntimeError("Iceberg scan returned zero rows")
except Exception as exc:
    print(f"FAIL: DuckDB/Iceberg data check failed: {exc}")
    print("Action: check Spark logs, MinIO availability, DuckDB extensions, and whether Spark has committed a crypto_ohlcv batch.")
    sys.exit(1)
'@

try {
    $dataCheckScript | docker compose exec -T dashboard python -
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Iceberg data check: failed"
    }
} catch {
    Write-Host "Iceberg data check: failed"
    Write-Host "Error: $($_.Exception.Message)"
    Write-Host "Action: ensure the dashboard container is running, then rerun .\scripts\ops\status.ps1"
}

Write-Section "Dashboard"
try {
    $response = Invoke-WebRequest -Uri "http://localhost:8501" -UseBasicParsing -TimeoutSec 10
    Write-Host "Dashboard reachable: HTTP $($response.StatusCode)"
} catch {
    Write-Host "Dashboard reachable: no"
    Write-Host "Error: $($_.Exception.Message)"
}
