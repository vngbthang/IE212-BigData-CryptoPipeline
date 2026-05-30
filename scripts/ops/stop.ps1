$ErrorActionPreference = "Stop"

Set-Location (Join-Path $PSScriptRoot "..\..")

Write-Host "Stopping Docker Compose services without deleting volumes or data..."
try {
	docker compose stop
	if ($LASTEXITCODE -eq 0) {
		Write-Host "STOP PASS: services stopped without removing volumes or data" -ForegroundColor Green
	}
	else {
		Write-Host "STOP FAIL: docker compose stop returned exit code $LASTEXITCODE" -ForegroundColor Red
		exit $LASTEXITCODE
	}
}
catch {
	Write-Host "STOP FAIL: docker compose stop threw an error: $($_.Exception.Message)" -ForegroundColor Red
	exit 1
}
