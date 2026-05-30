$ErrorActionPreference = "Stop"

Set-Location (Join-Path $PSScriptRoot "..\..")

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

function Write-ServiceLogs {
    param(
        [string]$ServiceName,
        [int]$Tail = 120
    )

    Write-Section "$ServiceName logs"

    $containerId = Get-ServiceContainerId $ServiceName
    if ($null -eq $containerId) {
        Write-Host "WARNING: $ServiceName is not running or unavailable" -ForegroundColor Yellow
        return
    }

    $oldPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $logs = docker compose logs --tail $Tail $ServiceName 2>&1
        if ($LASTEXITCODE -ne 0 -and [string]::IsNullOrWhiteSpace(($logs -join ""))) {
            Write-Host "WARNING: unable to read logs for $ServiceName" -ForegroundColor Yellow
            return
        }

        if ($logs) {
            $logs
        }
        else {
            Write-Host "WARNING: no recent logs found for $ServiceName" -ForegroundColor Yellow
        }
    }
    catch {
        Write-Host "WARNING: failed to read logs for ${ServiceName}: $($_.Exception.Message)" -ForegroundColor Yellow
    }
    finally {
        $ErrorActionPreference = $oldPreference
    }
}

foreach ($service in @("crypto-feeder", "spark", "dashboard", "catalog", "kafka")) {
    Write-ServiceLogs -ServiceName $service
}
