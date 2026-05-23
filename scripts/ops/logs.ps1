$ErrorActionPreference = "Stop"

Set-Location (Join-Path $PSScriptRoot "..\..")

$services = @(
    "crypto-feeder",
    "spark",
    "dashboard",
    "catalog",
    "kafka"
)

foreach ($service in $services) {
    Write-Host ""
    Write-Host "== $service logs =="
    docker compose logs --tail 120 $service
}
