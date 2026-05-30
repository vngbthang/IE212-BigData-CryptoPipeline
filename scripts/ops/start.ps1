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

function Get-ServiceContainerId {
    param([string]$ServiceName)
    $containerId = docker compose ps -q $ServiceName 2>$null
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($containerId)) {
        return $null
    }
    return $containerId.Trim()
}

function Test-ServiceRunning {
    param([string]$ServiceName)
    $containerId = Get-ServiceContainerId $ServiceName
    if ($null -eq $containerId) {
        return $false
    }

    $running = docker inspect -f "{{.State.Running}}" $containerId 2>$null
    return ($LASTEXITCODE -eq 0 -and $running.Trim() -eq "true")
}

function Test-ServiceHealthy {
    param([string]$ServiceName)
    $containerId = Get-ServiceContainerId $ServiceName
    if ($null -eq $containerId) {
        return $false
    }

    $health = docker inspect -f "{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}" $containerId 2>$null
    return ($LASTEXITCODE -eq 0 -and $health.Trim() -eq "healthy")
}

function Wait-ForCondition {
    param(
        [string]$Label,
        [scriptblock]$Condition,
        [int]$TimeoutSeconds = 60,
        [int]$IntervalSeconds = 5
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        if (& $Condition) {
            Write-Host "PASS: $Label" -ForegroundColor Green
            return $true
        }
        Start-Sleep -Seconds $IntervalSeconds
    }

    return $false
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
        return ($logs -join "`n")
    }
    finally {
        $ErrorActionPreference = $oldPreference
    }
}

Write-Host "Validating Docker Compose configuration..."
docker compose config | Out-Null
Write-Host "PASS: docker compose config" -ForegroundColor Green

Write-Host "Building and starting the full verified pipeline..."
docker compose up -d --build

Write-Host ""
Write-Host "Current service status:"
docker compose ps

Write-Host ""
Write-Host "Waiting for core service readiness..."

foreach ($service in @("catalog", "storage", "kafka")) {
    $serviceName = $service
    $condition = { Test-ServiceHealthy $serviceName }.GetNewClosure()
    $ok = Wait-ForCondition "$serviceName is healthy" $condition -TimeoutSeconds 90 -IntervalSeconds 5
    if (-not $ok) {
        Add-Failure "$serviceName did not become healthy"
    }
}

foreach ($service in @("crypto-feeder", "spark")) {
    $serviceName = $service
    $condition = { Test-ServiceRunning $serviceName }.GetNewClosure()
    $ok = Wait-ForCondition "$serviceName is running" $condition -TimeoutSeconds 60 -IntervalSeconds 5
    if (-not $ok) {
        Add-Failure "$serviceName is not running"
    }
}

Write-Host ""
Write-Host "Checking live feeder output..."
$feederOk = Wait-ForCondition "crypto-feeder is publishing BTC/USDT or ETH/USDT messages" {
    $logs = Get-ComposeLogs "crypto-feeder" 160
    return ($logs -match "Kafka BTC/USDT" -or $logs -match "Kafka ETH/USDT")
} -TimeoutSeconds 90 -IntervalSeconds 5

if (-not $feederOk) {
    Add-Failure "crypto-feeder logs do not show Kafka BTC/USDT or Kafka ETH/USDT messages"
}

Write-Host ""
Write-Host "Checking Spark Iceberg writes..."
$sparkOk = Wait-ForCondition "Spark wrote a batch to nessie.gold.crypto_ohlcv" {
    $logs = Get-ComposeLogs "spark" 400
    return ($logs -match "written to nessie\.gold\.crypto_ohlcv")
} -TimeoutSeconds 120 -IntervalSeconds 10

if (-not $sparkOk) {
    Add-Warning "Spark has not logged a write to nessie.gold.crypto_ohlcv yet"
}

Write-Host ""
Write-Host "Checking dashboard reachability..."
$dashboardOk = Wait-ForCondition "dashboard returns HTTP 200 at http://localhost:8501" {
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:8501" -UseBasicParsing -TimeoutSec 5
        return ($response.StatusCode -eq 200)
    }
    catch {
        return $false
    }
} -TimeoutSeconds 60 -IntervalSeconds 5

if (-not $dashboardOk) {
    Add-Failure "dashboard did not return HTTP 200 at http://localhost:8501"
}

Write-Host ""
Write-Host "Final service status:"
docker compose ps

Write-Host ""
if ($Failures.Count -eq 0 -and $Warnings.Count -eq 0) {
    Write-Host "STARTUP PASS: full pipeline is running" -ForegroundColor Green
    exit 0
}

if ($Failures.Count -eq 0) {
    Write-Host "STARTUP WARNING: full stack is up, but one verification layer needs attention" -ForegroundColor Yellow
    foreach ($warning in $Warnings) {
        Write-Host "- $warning" -ForegroundColor Yellow
    }
    Write-Host "Action: check feeder logs, check spark logs, then run status.ps1. Use reset-checkpoint.ps1 only if a Spark checkpoint/state issue is suspected." -ForegroundColor Yellow
    exit 0
}

Write-Host "STARTUP FAIL: one or more required layers did not start correctly" -ForegroundColor Red
foreach ($failure in $Failures) {
    Write-Host "- $failure" -ForegroundColor Red
}
foreach ($warning in $Warnings) {
    Write-Host "- $warning" -ForegroundColor Yellow
}
Write-Host "Action: check feeder logs, check spark logs, then run status.ps1. Use reset-checkpoint.ps1 only if a Spark checkpoint/state issue is suspected." -ForegroundColor Yellow
exit 1
