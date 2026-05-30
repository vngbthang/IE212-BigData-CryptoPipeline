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
try {
	docker compose stop spark
	if ($LASTEXITCODE -ne 0) {
		Write-Host "FAIL: unable to stop Spark (exit code $LASTEXITCODE)" -ForegroundColor Red
		exit $LASTEXITCODE
	}
}
catch {
	Write-Host "FAIL: unable to stop Spark: $($_.Exception.Message)" -ForegroundColor Red
	exit 1
}

Write-Host "Deleting checkpoint path: $minioCheckpointPath"
try {
	docker compose run --rm --entrypoint /bin/sh mc -c "mc alias set minio http://storage:9000 admin password && mc rm --recursive --force $minioCheckpointPath"
	if ($LASTEXITCODE -ne 0) {
		Write-Host "FAIL: checkpoint deletion failed with exit code $LASTEXITCODE" -ForegroundColor Red
		exit $LASTEXITCODE
	}
}
catch {
	Write-Host "FAIL: checkpoint deletion threw an error: $($_.Exception.Message)" -ForegroundColor Red
	exit 1
}

Write-Host "Restarting Spark..."
try {
	docker compose up -d spark
	if ($LASTEXITCODE -ne 0) {
		Write-Host "FAIL: unable to restart Spark (exit code $LASTEXITCODE)" -ForegroundColor Red
		exit $LASTEXITCODE
	}
}
catch {
	Write-Host "FAIL: unable to restart Spark: $($_.Exception.Message)" -ForegroundColor Red
	exit 1
}

Write-Host "Recent Spark logs:"
docker compose logs --tail 200 spark
Write-Host "RESET PASS: Spark checkpoint was reset without deleting Iceberg/Nessie data or Docker volumes" -ForegroundColor Green
