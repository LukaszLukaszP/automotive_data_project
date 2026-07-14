param(
    [string]$Python = "python"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $RepoRoot

function Invoke-Step {
    param(
        [string]$Label,
        [scriptblock]$Command
    )
    Write-Host ""
    Write-Host $Label
    try {
        & $Command
    }
    catch {
        Write-Host ""
        Write-Error "FAILED during: $Label"
        throw
    }
}

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
$VerificationDb = Join-Path $RepoRoot "data\offline_verification.sqlite3"
New-Item -ItemType Directory -Force -Path (Join-Path $RepoRoot "data") | Out-Null

Invoke-Step "[1/6] Creating virtual environment..." {
    if (-not (Get-Command $Python -ErrorAction SilentlyContinue)) {
        throw "Python command '$Python' was not found. Install Python 3.10+ or pass -Python <path>."
    }
    if (-not (Test-Path $VenvPython)) {
        Invoke-Native $Python @("-m", "venv", ".venv")
    }
}

Invoke-Step "[2/6] Installing dependencies..." {
    Invoke-Native $VenvPython @("-m", "pip", "install", "--upgrade", "pip")
    Invoke-Native $VenvPython @("-m", "pip", "install", "-r", "requirements-dev.txt")
    Invoke-Native $VenvPython @("-m", "pip", "install", "-e", ".", "--no-build-isolation")
}

Invoke-Step "[3/6] Running tests..." {
    Invoke-Native $VenvPython @("-m", "pytest", "-v", "-p", "no:cacheprovider")
}

Invoke-Step "[4/6] Running fixture parser..." {
    Invoke-Native $VenvPython @("-m", "automotive_data_project", "parse-fixture", "tests\fixtures\offer_complete.html")
}

Invoke-Step "[5/6] Running offline ETL..." {
    if (Test-Path $VerificationDb) {
        Remove-Item -LiteralPath $VerificationDb -Force
    }
    $env:DATABASE_URL = "sqlite:///$($VerificationDb.Replace('\', '/'))"
    Invoke-Native $VenvPython @(
        "scripts\offline_smoke_test.py",
        "--fixtures",
        "tests\fixtures",
        "--database-url",
        $env:DATABASE_URL
    )
}

Invoke-Step "[6/6] Running example analysis..." {
    $env:DATABASE_URL = "sqlite:///$($VerificationDb.Replace('\', '/'))"
    Invoke-Native $VenvPython @("examples\example_analysis.py")
}

Write-Host ""
Write-Host "PROJECT VERIFICATION PASSED"
