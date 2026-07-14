param(
    [string]$Python = "python"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $RepoRoot

function Invoke-Native {
    param(
        [string]$FilePath,
        [string[]]$Arguments
    )
    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code ${LASTEXITCODE}: $FilePath $($Arguments -join ' ')"
    }
}

$VenvPython = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$LiveDb = Join-Path $RepoRoot "data\live_smoke.sqlite3"
New-Item -ItemType Directory -Force -Path (Join-Path $RepoRoot "data") | Out-Null

if (-not (Get-Command $Python -ErrorAction SilentlyContinue)) {
    throw "Python command '$Python' was not found. Install Python 3.10+ or pass -Python <path>."
}
if (-not (Test-Path $VenvPython)) {
    Write-Host "Creating .venv..."
    Invoke-Native $Python @("-m", "venv", ".venv")
}

Write-Host "Installing dependencies..."
Invoke-Native $VenvPython @("-m", "pip", "install", "-r", "requirements-dev.txt")
Invoke-Native $VenvPython @("-m", "pip", "install", "-e", ".", "--no-build-isolation")

$env:DATABASE_URL = "sqlite:///$($LiveDb.Replace('\', '/'))"

Write-Host "Initializing SQLite schema..."
Invoke-Native $VenvPython @("-m", "automotive_data_project", "init-db")

Write-Host "Running very small live smoke test. This may stop safely on 403, 429, or CAPTCHA."
Invoke-Native $VenvPython @(
    "-m",
    "automotive_data_project",
    "scrape",
    "--make",
    "Toyota",
    "--model",
    "Corolla",
    "--year-from",
    "2019",
    "--year-to",
    "2021",
    "--max-pages",
    "1",
    "--max-listings",
    "3",
    "--concurrency",
    "1",
    "--delay",
    "4",
    "--jitter",
    "2",
    "--timeout",
    "20"
)

Write-Host "LIVE SMOKE TEST COMMAND FINISHED"
