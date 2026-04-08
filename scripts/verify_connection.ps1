# =============================================================================
# verify_connection.ps1
# =============================================================================
# Quick connectivity test for local dev (Windows PowerShell) and EC2.
#
# Usage (run from project root):
#   .\scripts\verify_connection.ps1
#
# It reads credentials from your .env file automatically.
# =============================================================================

$ErrorActionPreference = "Stop"

# --- Load .env ----------------------------------------------------------------
Write-Host "`n[1/4] Loading .env..." -ForegroundColor Cyan
$envFile = Join-Path $PSScriptRoot "..\\.env"
if (-not (Test-Path $envFile)) {
    Write-Error ".env not found at $envFile. Create it from .env.example first."
    exit 1
}

Get-Content $envFile | Where-Object { $_ -match "^\s*[^#]" -and $_ -match "=" } | ForEach-Object {
    $parts = $_ -split "=", 2
    [System.Environment]::SetEnvironmentVariable($parts[0].Trim(), $parts[1].Trim(), "Process")
}

$PG_HOST = $env:POSTGRES_HOST
$PG_PORT = $env:POSTGRES_PORT
$PG_USER = $env:POSTGRES_USER
$PG_DB   = $env:POSTGRES_DB

Write-Host "  HOST : $PG_HOST" -ForegroundColor White
Write-Host "  PORT : $PG_PORT" -ForegroundColor White
Write-Host "  USER : $PG_USER" -ForegroundColor White
Write-Host "  DB   : $PG_DB"   -ForegroundColor White

# --- TCP Reachability ---------------------------------------------------------
Write-Host "`n[2/4] TCP reachability check ($PG_HOST`:$PG_PORT)..." -ForegroundColor Cyan
try {
    $tcp = New-Object System.Net.Sockets.TcpClient
    $tcp.Connect($PG_HOST, [int]$PG_PORT)
    $tcp.Close()
    Write-Host "  ✅ TCP connection OK" -ForegroundColor Green
} catch {
    Write-Host "  ❌ TCP FAILED: $_" -ForegroundColor Red
    Write-Host "  → Check: RDS Security Group inbound rule for port $PG_PORT from your IP" -ForegroundColor Yellow
    exit 1
}

# --- psql query ---------------------------------------------------------------
Write-Host "`n[3/4] psql SELECT 1 test..." -ForegroundColor Cyan
$env:PGPASSWORD = $env:POSTGRES_PASSWORD
$result = psql -h $PG_HOST -p $PG_PORT -U $PG_USER -d $PG_DB -c "SELECT 1 AS ping;" 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "  ✅ psql OK" -ForegroundColor Green
    Write-Host $result
} else {
    Write-Host "  ❌ psql FAILED" -ForegroundColor Red
    Write-Host $result
    Write-Host "  → Wrong password? Check POSTGRES_PASSWORD in .env" -ForegroundColor Yellow
    exit 1
}

# --- Schema check -------------------------------------------------------------
Write-Host "`n[4/4] Raw schema table count..." -ForegroundColor Cyan
$query = "SELECT COUNT(*) AS tables FROM information_schema.tables WHERE table_schema = 'raw';"
$result = psql -h $PG_HOST -p $PG_PORT -U $PG_USER -d $PG_DB -c $query 2>&1
Write-Host $result

Write-Host "`n🎉 All checks passed! Pipeline is ready to connect." -ForegroundColor Green
Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
