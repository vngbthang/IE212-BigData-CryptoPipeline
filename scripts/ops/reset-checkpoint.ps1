$ErrorActionPreference = "Stop"

Set-Location (Join-Path $PSScriptRoot "..\..")

$checkpointUri = "s3a://warehouse/checkpoints/ohlcv/"
$minioCheckpointPath = "minio/warehouse/checkpoints/ohlcv"

Write-Host "Resetting only the Spark OHLCV streaming checkpoint."
Write-Host "Checkpoint URI: $checkpointUri"
Write-Host "MinIO path:     $minioCheckpointPath"
Write-Host ""
Write-Host "Safety:"
Write-Host "- This script deletes only the OHLCV streaming checkpoint path."
Write-Host "- It does not delete Iceberg table data."
Write-Host "- It does not delete Nessie catalog data."
Write-Host "- It does not delete the MinIO warehouse bucket."
Write-Host "- It does not delete Docker volumes."
Write-Host ""

Write-Host "Stopping Spark before checkpoint deletion..."
docker compose stop spark

Write-Host "Deleting checkpoint path: $minioCheckpointPath"
docker compose run --rm --entrypoint /bin/sh mc -c "mc alias set minio http://storage:9000 admin password && mc rm --recursive --force $minioCheckpointPath"

Write-Host "Restarting Spark..."
docker compose up -d spark

Write-Host "Recent Spark logs:"
docker compose logs --tail 200 spark
