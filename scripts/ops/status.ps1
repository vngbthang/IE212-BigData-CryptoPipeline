$ErrorActionPreference = "Stop"

Set-Location (Join-Path $PSScriptRoot "..\..")

$Failures = New-Object System.Collections.Generic.List[string]
$Warnings = New-Object System.Collections.Generic.List[string]

function Add-Failure {
    param([string]$Message)
    $Failures.Add($Message) | Out-Null
    Write-Host "FAIL: $Message" -ForegroundColor Red
}

function Add-Warning {
    param([string]$Message)
    $Warnings.Add($Message) | Out-Null
    Write-Host "WARNING: $Message" -ForegroundColor Yellow
}

function Write-Section {
    param([string]$Title)
    Write-Host ""
    Write-Host "== $Title =="
}

function Get-ServiceContainerId {
    param([string]$ServiceName)
    try {
        $containerId = docker compose ps -q $ServiceName 2>$null
        if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($containerId)) {
            return $null
        }
        return $containerId.Trim()
    }
    catch {
        return $null
    }
}

function Test-ServiceRunning {
    param([string]$ServiceName)
    $containerId = Get-ServiceContainerId $ServiceName
    if ($null -eq $containerId) {
        return $false
    }

    try {
        $running = docker inspect -f "{{.State.Running}}" $containerId 2>$null
        return ($LASTEXITCODE -eq 0 -and $running.Trim() -eq "true")
    }
    catch {
        return $false
    }
}

function Test-ServiceHealthy {
    param([string]$ServiceName)
    $containerId = Get-ServiceContainerId $ServiceName
    if ($null -eq $containerId) {
        return $false
    }

    try {
        $health = docker inspect -f "{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}" $containerId 2>$null
        return ($LASTEXITCODE -eq 0 -and $health.Trim() -eq "healthy")
    }
    catch {
        return $false
    }
}

function Get-ComposeLogs {
    param(
        [string]$ServiceName,
        [int]$Tail = 200
    )

    $oldPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $logs = docker compose logs --tail $Tail $ServiceName 2>&1
        if ($LASTEXITCODE -ne 0 -and [string]::IsNullOrWhiteSpace(($logs -join ""))) {
            return $null
        }
        return ($logs -join "`n")
    }
    catch {
        return $null
    }
    finally {
        $ErrorActionPreference = $oldPreference
    }
}

Write-Section "Compose Services"
try {
    docker compose ps
}
catch {
    Add-Failure "docker compose ps threw an error: $($_.Exception.Message)"
}

Write-Section "Health Checks"
foreach ($service in @("catalog", "storage", "kafka")) {
    if (Test-ServiceHealthy $service) {
        Write-Host "$service health: healthy"
    }
    elseif (Test-ServiceRunning $service) {
        Add-Failure "$service is running but not healthy"
    }
    else {
        Add-Failure "$service is not running or unavailable"
    }
}

Write-Section "Spark Iceberg Writes"
$sparkLogs = Get-ComposeLogs "spark" 300
if ([string]::IsNullOrWhiteSpace($sparkLogs)) {
    Add-Warning "Spark logs are unavailable or Spark is not running"
}
elseif ($sparkLogs -match "written to nessie\.gold\.crypto_ohlcv") {
    Write-Host "Spark writes: found"
}
else {
    Add-Failure "Spark write evidence was not found in the last 300 Spark log lines"
}

Write-Section "Iceberg Data Check"
if (Test-ServiceRunning "dashboard") {
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
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Iceberg data check: PASS"
        }
        else {
            Add-Failure "Iceberg data check failed with exit code $LASTEXITCODE"
        }
    }
    catch {
        Add-Failure "Iceberg data check could not be executed: $($_.Exception.Message)"
    }
}
else {
    Add-Warning "Dashboard container is not running; skipping DuckDB/Iceberg data check"
}

Write-Section "Dashboard"
try {
    $response = Invoke-WebRequest -Uri "http://localhost:8501" -UseBasicParsing -TimeoutSec 10
    Write-Host "Dashboard reachable: HTTP $($response.StatusCode)"
}
catch {
    Add-Failure "Dashboard did not return HTTP 200 at http://localhost:8501"
}

Write-Section "Summary"
if ($Failures.Count -eq 0 -and $Warnings.Count -eq 0) {
    Write-Host "STATUS PASS: all required checks passed" -ForegroundColor Green
    exit 0
}

if ($Failures.Count -eq 0) {
    Write-Host "STATUS WARNING: the stack is up, but one or more checks need attention" -ForegroundColor Yellow
    foreach ($warning in $Warnings) {
        Write-Host "- $warning" -ForegroundColor Yellow
    }
    exit 0
}

Write-Host "STATUS FAIL: one or more required checks failed" -ForegroundColor Red
foreach ($failure in $Failures) {
    Write-Host "- $failure" -ForegroundColor Red
}
foreach ($warning in $Warnings) {
    Write-Host "- $warning" -ForegroundColor Yellow
}
exit 1
