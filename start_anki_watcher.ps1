# Start Anki Watcher
# This script runs the Anki watcher that automatically syncs when Anki opens/closes

Write-Host "=" -ForegroundColor Cyan -NoNewline
Write-Host ("=" * 79) -ForegroundColor Cyan
Write-Host "ANKI WATCHER - Auto-sync on Anki Open/Close" -ForegroundColor Cyan
Write-Host "=" -ForegroundColor Cyan -NoNewline
Write-Host ("=" * 79) -ForegroundColor Cyan
Write-Host ""

# Check if sync service is running
Write-Host "Checking if sync service is running..." -ForegroundColor Yellow

$syncRunning = $false
try {
    $response = Invoke-WebRequest -Uri "http://localhost:8000/health" -TimeoutSec 2 -ErrorAction SilentlyContinue
    if ($response.StatusCode -eq 200) {
        $syncRunning = $true
        Write-Host "✓ Sync service is running" -ForegroundColor Green
    }
} catch {
    Write-Host "✗ Sync service is NOT running" -ForegroundColor Red
    Write-Host ""
    Write-Host "Please start the sync service first:" -ForegroundColor Yellow
    Write-Host "  cd c:\Projects\jarvis-beeper-bridge" -ForegroundColor White
    Write-Host "  docker compose up -d sync-service" -ForegroundColor White
    Write-Host ""
    exit 1
}

Write-Host ""

# Start the watcher
Write-Host "Starting Anki Watcher..." -ForegroundColor Yellow
Write-Host "  - Will sync when Anki Desktop opens" -ForegroundColor White
Write-Host "  - Will sync when Anki Desktop closes" -ForegroundColor White
Write-Host "  - Checks every 10 seconds" -ForegroundColor White
Write-Host ""
Write-Host "Press Ctrl+C to stop" -ForegroundColor Gray
Write-Host ""

python anki_watcher.py
