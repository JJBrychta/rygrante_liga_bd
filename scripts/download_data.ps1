[CmdletBinding()]
param(
    [string]$Url = "",
    [string]$R2AccountId = "",
    [string]$R2Bucket = "",
    [string]$R2Prefix = "",
    [string]$R2AccessKey = "",
    [string]$R2SecretKey = "",
    [string]$ManifestName = "",
    [string]$EnvFile = "",
    [string]$Dest = "./data/landing",
    [switch]$VerifyOnly,
    [switch]$Force,
    [switch]$Help,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ExtraArgs = @()
)

$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$PyScript = Join-Path $RepoRoot "scripts\download_data.py"
$ResolvedEnvFile = if ([string]::IsNullOrWhiteSpace($EnvFile)) { Join-Path $RepoRoot ".env" } else { $EnvFile }

$PyArgs = @()
if ($Help) {
    $PyArgs += "--help"
} else {
    $PyArgs += @("--env-file", $ResolvedEnvFile, "--dest", $Dest)
    if (-not [string]::IsNullOrWhiteSpace($Url)) { $PyArgs += @("--url", $Url) }
    if (-not [string]::IsNullOrWhiteSpace($R2AccountId)) { $PyArgs += @("--r2-account-id", $R2AccountId) }
    if (-not [string]::IsNullOrWhiteSpace($R2Bucket)) { $PyArgs += @("--r2-bucket", $R2Bucket) }
    if (-not [string]::IsNullOrWhiteSpace($R2Prefix)) { $PyArgs += @("--r2-prefix", $R2Prefix) }
    if (-not [string]::IsNullOrWhiteSpace($R2AccessKey)) { $PyArgs += @("--r2-access-key", $R2AccessKey) }
    if (-not [string]::IsNullOrWhiteSpace($R2SecretKey)) { $PyArgs += @("--r2-secret-key", $R2SecretKey) }
    if (-not [string]::IsNullOrWhiteSpace($ManifestName)) { $PyArgs += @("--manifest-name", $ManifestName) }
    if ($VerifyOnly) { $PyArgs += "--verify-only" }
    if ($Force) { $PyArgs += "--force" }
}
if ($ExtraArgs.Count -gt 0) {
    $PyArgs += $ExtraArgs
}

$NeedsBoto3 = (-not $Help) -and (-not $VerifyOnly) -and [string]::IsNullOrWhiteSpace($Url) -and (-not ($ExtraArgs -contains "--verify-only")) -and (-not ($ExtraArgs -contains "--help")) -and (-not ($ExtraArgs -contains "-h"))

if ($null -ne (Get-Command uv -ErrorAction SilentlyContinue)) {
    if ($NeedsBoto3) {
        & uv run --no-project --with boto3 python $PyScript @PyArgs
    } else {
        & uv run --no-project python $PyScript @PyArgs
    }
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    exit 0
}

$PythonCmd = Get-Command python -ErrorAction SilentlyContinue
if ($null -ne $PythonCmd) {
    $CanRunLocal = $false
    try {
        if ($NeedsBoto3) {
            & python -c "import boto3" 2>$null
        } else {
            & python -c "import sys" 2>$null
        }
        $CanRunLocal = ($LASTEXITCODE -eq 0)
    } catch {
        $CanRunLocal = $false
    }
    if ($CanRunLocal) {
        & python $PyScript @PyArgs
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        exit 0
    }
}

if ($null -ne (Get-Command docker -ErrorAction SilentlyContinue)) {
    $DockerArgs = @()
    if ($Help) {
        $DockerArgs += "--help"
    } else {
        $ContainerDest = if ($Dest -eq "./data/landing" -or $Dest -eq ".\data\landing") { "/opt/workspace/data/landing" } else { $Dest }
        $DockerArgs += @("--dest", $ContainerDest)
        if (-not [string]::IsNullOrWhiteSpace($Url)) { $DockerArgs += @("--url", $Url) }
        if (-not [string]::IsNullOrWhiteSpace($R2AccountId)) { $DockerArgs += @("--r2-account-id", $R2AccountId) }
        if (-not [string]::IsNullOrWhiteSpace($R2Bucket)) { $DockerArgs += @("--r2-bucket", $R2Bucket) }
        if (-not [string]::IsNullOrWhiteSpace($R2Prefix)) { $DockerArgs += @("--r2-prefix", $R2Prefix) }
        if (-not [string]::IsNullOrWhiteSpace($R2AccessKey)) { $DockerArgs += @("--r2-access-key", $R2AccessKey) }
        if (-not [string]::IsNullOrWhiteSpace($R2SecretKey)) { $DockerArgs += @("--r2-secret-key", $R2SecretKey) }
        if (-not [string]::IsNullOrWhiteSpace($ManifestName)) { $DockerArgs += @("--manifest-name", $ManifestName) }
        if ($VerifyOnly) { $DockerArgs += "--verify-only" }
        if ($Force) { $DockerArgs += "--force" }
    }
    if ($ExtraArgs.Count -gt 0) {
        $DockerArgs += $ExtraArgs
    }

    Push-Location $RepoRoot
    try {
        & docker compose run --rm spark-master python3 /opt/workspace/scripts/download_data.py @DockerArgs
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        exit 0
    } finally {
        Pop-Location
    }
}

throw "Could not find 'uv', 'python' (with boto3), or 'docker' to run download_data.py. Install uv or start Docker Desktop."
