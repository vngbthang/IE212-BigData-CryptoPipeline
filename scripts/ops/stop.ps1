$ErrorActionPreference = "Stop"

Set-Location (Join-Path $PSScriptRoot "..\..")

Write-Host "Stopping Docker Compose services without deleting volumes or data..."
docker compose stop
