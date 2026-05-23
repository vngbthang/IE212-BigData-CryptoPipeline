$ErrorActionPreference = "Stop"

Set-Location (Join-Path $PSScriptRoot "..\..")

Write-Host "Validating Docker Compose configuration..."
docker compose config

Write-Host "Building and starting the verified pipeline..."
docker compose up -d --build

Write-Host "Current service status:"
docker compose ps
