param(
    [switch]$Rebuild,
    [switch]$NoOpenBrowser,
    [switch]$NoTerminals,
    [int]$MaxWaitSeconds = 180
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

function Write-Step {
    param([string]$Message)
    Write-Host "`n==> $Message" -ForegroundColor Cyan
}

function Write-Ok {
    param([string]$Message)
    Write-Host "[OK] $Message" -ForegroundColor Green
}

function Write-Warn {
    param([string]$Message)
    Write-Host "[WARN] $Message" -ForegroundColor Yellow
}

Write-Step "Preflight checks"
docker --version | Out-Null
docker compose version | Out-Null
Write-Ok "Docker and Docker Compose are available"

Write-Step "Handle postgres name conflict (if any)"
$composePostgresId = (docker compose ps -q postgres).Trim()
$globalPostgres = (docker ps -a --filter "name=^/postgres$" --format "{{.ID}}").Trim()
if ($globalPostgres -and -not $composePostgresId) {
    $newName = "postgres_legacy_$(Get-Date -Format 'yyyyMMdd_HHmmss')"
    docker rename postgres $newName | Out-Null
    Write-Ok "Renamed external container 'postgres' -> '$newName'"
} else {
    Write-Ok "No blocking postgres container conflict"
}

Write-Step "Start compose services"
if ($Rebuild) {
    docker compose --profile dev up -d --build
} else {
    docker compose --profile dev up -d
}
Write-Ok "Compose stack started"

Write-Step "Verify critical services"
docker compose ps

Write-Step "Restart Spark stream processor (detached)"
docker exec spark-master sh -c "pkill -f stream_processor.py || true; pkill -f 'SparkSubmit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1' || true" | Out-Null
$i = 0
while ($i -lt 10) {
    $existing = (docker exec spark-master sh -c "ps -ef | grep -E 'stream_processor.py|SparkSubmit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1' | grep -v grep | wc -l").Trim()
    if ($existing -eq "0") { break }
    Start-Sleep -Seconds 1
    $i++
}
$submitCmd = @(
    "/opt/spark/bin/spark-submit",
    "--packages",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3",
    "/app/src/processor/stream_processor.py",
    ">",
    "/tmp/stream_processor.log",
    "2>&1"
) -join " "
docker exec -d spark-master sh -c $submitCmd | Out-Null
Start-Sleep -Seconds 4

$procCheck = docker exec spark-master sh -c "ps -ef | grep stream_processor.py | grep -v grep"
if (-not $procCheck) {
    throw "Spark stream_processor.py is not running"
}
Write-Ok "Spark stream processor is running"

Write-Step "Wait for first rows in gold.gold_crypto_ohlcv"
$start = Get-Date
$rows = 0
while (((Get-Date) - $start).TotalSeconds -lt $MaxWaitSeconds) {
    $rowsText = (docker exec postgres psql -U crypto_user -d cryptopipeline -t -A -c "SELECT COUNT(*) FROM gold.gold_crypto_ohlcv;").Trim()
    if ([int]::TryParse($rowsText, [ref]$rows) -and $rows -gt 0) {
        break
    }
    Start-Sleep -Seconds 5
}

if ($rows -gt 0) {
    Write-Ok "Gold table has data: $rows rows"
} else {
    Write-Warn "No rows yet after $MaxWaitSeconds seconds (likely watermark/window delay or runtime issue)"
}

Write-Step "Latest DB snapshot"
docker exec postgres psql -U crypto_user -d cryptopipeline -c "SELECT COUNT(*) AS total_rows, MAX(timestamp) AS last_ts FROM gold.gold_crypto_ohlcv;"

docker exec postgres psql -U crypto_user -d cryptopipeline -c "SELECT symbol, timestamp, open, close, volume FROM gold.gold_crypto_ohlcv ORDER BY timestamp DESC LIMIT 5;"

if (-not $NoTerminals) {
    Write-Step "Open showcase terminals"
    $producerCmd = "Set-Location '$PSScriptRoot'; docker logs -f crypto-producer-mock"
    $sparkCmd = "Set-Location '$PSScriptRoot'; docker exec spark-master sh -c 'tail -f /tmp/stream_processor.log'"
    $dbCmd = @"
Set-Location '$PSScriptRoot'
while (`$true) {
    docker exec postgres psql -U crypto_user -d cryptopipeline -c "SELECT COUNT(*) AS total_rows, MAX(timestamp) AS last_ts FROM gold.gold_crypto_ohlcv;"
    Start-Sleep -Seconds 5
}
"@

    Start-Process powershell -ArgumentList "-NoExit", "-Command", $producerCmd | Out-Null
    Start-Process powershell -ArgumentList "-NoExit", "-Command", $sparkCmd | Out-Null
    Start-Process powershell -ArgumentList "-NoExit", "-Command", $dbCmd | Out-Null
    Write-Ok "Opened 3 terminals: producer log, spark log, db watcher"
} else {
    Write-Warn "Skipped opening extra terminals (-NoTerminals)"
}

if (-not $NoOpenBrowser) {
    Write-Step "Open dashboard"
    Start-Process "http://localhost:8501"
    Write-Ok "Dashboard opened at http://localhost:8501"
}

Write-Host "`nSHOWCASE READY" -ForegroundColor Green
Write-Host "- Terminal 1: producer log ([MOCK] Sent ...)" -ForegroundColor Gray
Write-Host "- Terminal 2: spark log (Batch + DB_WRITE)" -ForegroundColor Gray
Write-Host "- Terminal 3: db watcher (row count + last_ts)" -ForegroundColor Gray
Write-Host "- Browser: http://localhost:8501" -ForegroundColor Gray
