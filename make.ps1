# ==============================================================================
# rygrante_liga_bd - Windows (PowerShell) equivalent of the Makefile
#
# Usage:  .\make.ps1 <target> [-Service <name>]
#         .\make.ps1 help
#         .\make.ps1 logs -Service airflow-webserver
#
# Same targets as the Makefile. Called by MakefileW (make -f MakefileW <target>),
# or run directly without GNU make / Git Bash / WSL.
# Requires: Docker Desktop (running), uv, PowerShell 5.1+ or 7+.
# If scripts are blocked: Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
# ==============================================================================

param(
    [Parameter(Position = 0)]
    [string]$Target = "help",
    [string]$Service = ""
)

$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

$GitleaksVersion = "v8.30.1"
$SemgrepImage = "semgrep/semgrep:latest"
$TrivyImage = "aquasec/trivy:latest"
# Local-only paths CI never sees - keep them out of the Trivy scan.
$TrivySkipDirs = "data,lakehouse,logs,.venv,.idea,.ruff_cache,.pytest_cache"
$TrivySkipFiles = ".env"
$Repo = (Get-Location).Path

function Run {
    # Run a native command and stop on a non-zero exit code (like make).
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Cmd)
    Write-Host "> $($Cmd -join ' ')" -ForegroundColor DarkGray
    $rest = @($Cmd | Select-Object -Skip 1)
    & $Cmd[0] @rest
    if ($LASTEXITCODE -ne 0) {
        Write-Host "x '$($Cmd -join ' ')' failed (exit $LASTEXITCODE)" -ForegroundColor Red
        exit $LASTEXITCODE
    }
}

function Ok([string]$Msg) { Write-Host "+ $Msg" -ForegroundColor Green }

$Targets = [ordered]@{
    # --- CI (mirrors .github/workflows/ci.yml) ---
    "ci"        = "Run the full CI pipeline locally (all 5 jobs)"
    "ci-fast"   = "Run only lint + tests (quick pre-push check)"
    "lint"      = "Job lint-and-uv: uv lock, Ruff lint/format, script + Compose syntax"
    "fix"       = "Auto-fix Ruff lint issues and reformat code"
    "test"      = "Job unit-tests: pytest inside the Spark container (no local Java needed)"
    "test-host" = "Job unit-tests exactly like CI (needs Java 17 on your machine)"
    "gitleaks"  = "Job gitleaks: scan committed Git history for secrets"
    "semgrep"   = "Job semgrep: static security analysis"
    "trivy"     = "Job trivy: CRITICAL/HIGH vulnerability, secret and IaC scans"
    # --- Docker stack ---
    "up"        = "Start the stack (Spark, Airflow, Streamlit) in the background"
    "build"     = "Rebuild images and start the stack"
    "down"      = "Stop the stack (keeps data and volumes)"
    "restart"   = "Restart the stack"
    "ps"        = "Show container status"
    "logs"      = "Follow logs (all, or one: .\make.ps1 logs -Service airflow-webserver)"
    # --- Windows setup ---
    "setup"     = "Create .env (never overwrites), install Python 3.11 + deps"
    "data"      = "Download + verify the dataset into data\landing"
    "first-run" = "Fresh clone to working stack: setup, data, build, test"
    "doctor"    = "Diagnose common Windows problems (Docker, WSL, uv, .env, line endings)"
}

function Show-Help {
    Write-Host "`nUsage: make -f MakefileW <target>   (or without make: .\make.ps1 <target>)`n"
    $groups = @{ "ci" = "CI (mirrors .github/workflows/ci.yml)"; "up" = "Docker stack"; "setup" = "Windows setup" }
    foreach ($name in $Targets.Keys) {
        if ($groups.ContainsKey($name)) { Write-Host "`n$($groups[$name])" -ForegroundColor White }
        Write-Host ("  {0,-10} " -f $name) -ForegroundColor Cyan -NoNewline
        Write-Host $Targets[$name]
    }
    Write-Host ""
}

function Invoke-Lint {
    Run uv lock --check
    Run uv sync --frozen --dev --quiet
    Run uv run ruff check .
    Run uv run ruff format --check .
    if (Get-Command bash -ErrorAction SilentlyContinue) {
        Run bash -n scripts/download_data.sh
    } else {
        Write-Host "! bash not found: skipping 'bash -n scripts/download_data.sh' (CI still runs it)" -ForegroundColor Yellow
    }
    Run docker compose config --quiet
    Ok "lint-and-uv passed"
}

function Invoke-Test { Run docker compose run --rm spark-master pytest -v }

function Invoke-Gitleaks {
    Run docker run --rm -v "${Repo}:/repo" "zricethezav/gitleaks:$GitleaksVersion" `
        git --verbose --redact --config /repo/.gitleaks.toml /repo
}

function Invoke-Semgrep {
    Run docker run --rm -v "${Repo}:/src" -w /src $SemgrepImage `
        semgrep scan --config p/python --config p/dockerfile `
        --config p/docker-compose --config p/secrets --error --metrics=off
}

function Invoke-Trivy {
    Run docker run --rm -v "${Repo}:/src" -v trivy-cache:/root/.cache $TrivyImage `
        fs --scanners vuln,secret --severity CRITICAL,HIGH --ignore-unfixed --exit-code 1 `
        --skip-dirs $TrivySkipDirs --skip-files $TrivySkipFiles /src
    Run docker run --rm -v "${Repo}:/src" -v trivy-cache:/root/.cache $TrivyImage `
        config --severity CRITICAL,HIGH --exit-code 1 --skip-dirs $TrivySkipDirs /src
}

function Invoke-Setup {
    if (Test-Path .env) { Write-Host ".env exists, keeping it" }
    else {
        Copy-Item .env.example .env
        Write-Host "! Created .env: fill in the R2_* values" -ForegroundColor Yellow
    }
    Run uv python install 3.11
    Run uv sync --frozen --dev
    Ok "setup done"
}

function Invoke-Data {
    & "$PSScriptRoot\scripts\download_data.ps1"
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    Ok "data ready"
}

function Invoke-Doctor {
    $script:ok = $true
    function Check([bool]$Cond, [string]$Pass, [string]$Fail) {
        if ($Cond) { Write-Host "+ $Pass" -ForegroundColor Green }
        else { Write-Host "x $Fail" -ForegroundColor Red; $script:ok = $false }
    }
    $hasDocker = [bool](Get-Command docker -ErrorAction SilentlyContinue)
    Check $hasDocker "docker CLI installed" "docker missing: winget install --id Docker.DockerDesktop -e --source winget"
    if ($hasDocker) {
        docker info *> $null
        Check ($LASTEXITCODE -eq 0) "Docker Desktop running" "Docker not reachable: start Docker Desktop and wait until it says running"
        docker compose version *> $null
        Check ($LASTEXITCODE -eq 0) "docker compose v2" "docker compose missing: update Docker Desktop"
    }
    $hasWsl = [bool](Get-Command wsl -ErrorAction SilentlyContinue)
    Check $hasWsl "WSL installed" "WSL missing: run 'wsl --install' as Administrator, then reboot"
    Check ([bool](Get-Command uv -ErrorAction SilentlyContinue)) "uv installed" `
        "uv missing: powershell -ExecutionPolicy ByPass -c `"irm https://astral.sh/uv/install.ps1 | iex`""
    $hasGit = [bool](Get-Command git -ErrorAction SilentlyContinue)
    Check $hasGit "git installed" "git missing: winget install --id Git.Git -e --source winget"
    if ($hasGit) {
        $autocrlf = (git config --get core.autocrlf) 2>$null
        Check ($autocrlf -ne "true") "core.autocrlf is not 'true'" `
            "core.autocrlf=true can break .sh files in containers: git config --global core.autocrlf input, then re-clone"
    }
    if (Test-Path .env) {
        $envText = Get-Content .env
        Check (($envText -match '^AIRFLOW_UID=50000$').Count -gt 0) "AIRFLOW_UID=50000" "set AIRFLOW_UID=50000 in .env"
        $empty = ($envText -match '^R2_[A-Z_]+=$').Count
        Check ($empty -eq 0) "R2_* values filled" "$empty R2_* values empty in .env"
    } else {
        Check $false "" ".env missing: .\make.ps1 setup"
    }
    $landing = "data\landing"
    Check ((Test-Path $landing) -and ((Get-ChildItem $landing -Directory -ErrorAction SilentlyContinue).Count -ge 10)) `
        "10 landing datasets present" "dataset missing: .\make.ps1 data"
    if ($script:ok) { Ok "all checks passed" } else { exit 1 }
}

switch ($Target) {
    "help"      { Show-Help }
    "ci"        { Invoke-Lint; Invoke-Test; Invoke-Gitleaks; Invoke-Semgrep; Invoke-Trivy; Ok "All CI jobs passed" }
    "ci-fast"   { Invoke-Lint; Invoke-Test; Ok "lint + tests passed" }
    "lint"      { Invoke-Lint }
    "fix"       { Run uv run ruff check --fix .; Run uv run ruff format . }
    "test"      { Invoke-Test }
    "test-host" { Run uv run pytest -v }
    "gitleaks"  { Invoke-Gitleaks }
    "semgrep"   { Invoke-Semgrep }
    "trivy"     { Invoke-Trivy }
    "up"        { Run docker compose up -d }
    "build"     { Run docker compose up -d --build }
    "down"      { Run docker compose down }
    "restart"   { Run docker compose down; Run docker compose up -d }
    "ps"        { Run docker compose ps -a }
    "logs"      {
        if ($Service) { Run docker compose logs -f --tail 100 $Service }
        else { Run docker compose logs -f --tail 100 }
    }
    "setup"     { Invoke-Setup }
    "data"      { Invoke-Data }
    "first-run" { Invoke-Setup; Invoke-Data; Run docker compose up -d --build; Invoke-Test }
    "doctor"    { Invoke-Doctor }
    default     {
        Write-Host "Unknown target '$Target'." -ForegroundColor Red
        Show-Help
        exit 2
    }
}
