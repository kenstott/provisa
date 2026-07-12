# Build Provisa Observability Windows installer. Run from repo root.
# Requires: the container tier already installed (WSL2 distro + nerdctl exist).
#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot  = Split-Path -Parent (Split-Path -Parent $ScriptDir)

Write-Host '[build-installer-obs] Preparing obs build directory...' -ForegroundColor Cyan

$BuildDir = Join-Path $ScriptDir 'build-obs'
New-Item -ItemType Directory -Path $BuildDir -Force | Out-Null

# ── Obs images only ────────────────────────────────────────────────────────────
$BuildImages = Join-Path $BuildDir 'images'
New-Item -ItemType Directory -Path $BuildImages -Force | Out-Null

$ObsImages = @('minio-latest.tar.gz','otlp2parquet-latest.tar.gz',
    'opentelemetry-collector-contrib-0.99.0.tar.gz','prometheus-v2.51.2.tar.gz',
    'tempo-2.4.1.tar.gz','grafana-10.4.2.tar.gz')

Get-ChildItem -Path (Join-Path $ScriptDir 'obs-images') -Filter '*.tar.gz' -ErrorAction SilentlyContinue |
  Where-Object { $ObsImages -contains $_.Name } |
  Copy-Item -Destination $BuildImages

# ── Extension installer script ─────────────────────────────────────────────────
$InstallObsScript = @'
# Provisa Observability Extension Installer (Windows / WSL2)
#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ProvHome = Join-Path $env:USERPROFILE '.provisa'
$ExtDir   = Join-Path $ProvHome 'extensions\observability'
$ExtCompose = Join-Path $ExtDir 'docker-compose.observability.yml'
$ImagesDir = Join-Path $PSScriptRoot 'images'

function Write-Info  { Write-Host "[provisa-obs] $args" -ForegroundColor Cyan }
function Write-Ok    { Write-Host "[provisa-obs] $args" -ForegroundColor Green }
function Write-Err   { Write-Host "[provisa-obs] $args" -ForegroundColor Red }

# Check core installed
if (-not (Test-Path (Join-Path $ProvHome 'config.yaml'))) {
    Write-Err 'Provisa Core is not installed. Run Provisa-Setup.exe first.'
    exit 1
}

# Verify WSL2 + nerdctl are available (installed by Core first-launch)
$wslTest = wsl echo ok 2>&1
if ($LASTEXITCODE -ne 0 -or $wslTest -notmatch 'ok') {
    Write-Err 'WSL2 is not responding. Run Provisa First Launch to complete Core setup first.'
    exit 1
}
$nerdctlCheck = wsl sh -c 'command -v nerdctl' 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Err 'nerdctl not found in WSL2. Run Provisa First Launch to complete Core setup first.'
    exit 1
}

# Load obs images into WSL2 via nerdctl
$TarFiles = Get-ChildItem -Path $ImagesDir -Filter '*.tar.gz'
foreach ($f in $TarFiles) {
    Write-Info "Loading: $($f.Name)"
    $drive = $f.FullName[0].ToString().ToLower()
    $rest  = $f.FullName.Substring(2) -replace '\\', '/'
    $wslPath = "/mnt/$drive$rest"
    wsl nerdctl load -i $wslPath
    if ($LASTEXITCODE -ne 0) { Write-Err "Failed to load $($f.Name)"; exit 1 }
}
Write-Ok "Obs images loaded."

# Write extension compose file
New-Item -ItemType Directory -Path $ExtDir -Force | Out-Null
$ComposeSrc = Join-Path $ProvHome 'compose\docker-compose.observability.yml'
if (Test-Path $ComposeSrc) {
    Copy-Item $ComposeSrc $ExtCompose -Force
    Write-Ok "Extension compose file written: $ExtCompose"
} else {
    Write-Err "docker-compose.observability.yml not found in $ProvHome\compose."
    Write-Err "Reinstall Provisa Core, then retry."
    exit 1
}

Write-Ok 'Observability installed.'
Write-Host ''
Write-Host 'Restart Provisa to activate the observability stack.' -ForegroundColor Green
Write-Host 'Grafana:    http://localhost:3100'
Write-Host 'Prometheus: http://localhost:9090'
Write-Host ''
'@

$InstallObsScript | Set-Content -Path (Join-Path $BuildDir 'install-obs.ps1') -Encoding UTF8

# ── dist dir ──────────────────────────────────────────────────────────────────
$DistDir = Join-Path $ScriptDir 'dist'
New-Item -ItemType Directory -Path $DistDir -Force | Out-Null

# ── NSIS script ───────────────────────────────────────────────────────────────
$NsiPath = Join-Path $BuildDir 'installer-obs.nsi'
$Version = if ($env:VERSION) { $env:VERSION } else { 'dev' }
$OutExe  = Join-Path $DistDir 'Provisa-Obs-Setup.exe'

@"
!define PRODUCT_NAME "Provisa Observability"
!define PRODUCT_VERSION "$Version"
!define INSTALLER_NAME "Provisa-Obs-Setup.exe"

Name "`${PRODUCT_NAME} `${PRODUCT_VERSION}"
OutFile "$OutExe"
InstallDir "`$APPDATA\Provisa-Obs"
RequestExecutionLevel user
SetCompressor lzma

Page instfiles

Section "Install"
  SetOutPath "`$INSTDIR"
  File /r "$BuildDir\images"
  File "$BuildDir\install-obs.ps1"
  ExecWait 'powershell.exe -ExecutionPolicy Bypass -File "`$INSTDIR\install-obs.ps1"'
SectionEnd
"@ | Set-Content -Path $NsiPath -Encoding UTF8

Write-Host '[build-installer-obs] Running makensis...' -ForegroundColor Cyan
& makensis $NsiPath
if ($LASTEXITCODE -ne 0) {
    throw "makensis failed with exit code $LASTEXITCODE"
}

Write-Host '[build-installer-obs] Obs installer created.' -ForegroundColor Green

# ── Code signing ───────────────────────────────────────────────────────────────
if ($env:WINDOWS_CERT_PFX_BASE64) {
    Write-Host '[build-installer-obs] Signing installer...' -ForegroundColor Cyan
    $PfxPath = [System.IO.Path]::Combine([System.IO.Path]::GetTempPath(), 'provisa-signing.pfx')
    try {
        [System.Convert]::FromBase64String($env:WINDOWS_CERT_PFX_BASE64) `
            | Set-Content -Path $PfxPath -AsByteStream
        $TimestampUrl = if ($env:WINDOWS_CERT_TIMESTAMP_URL) {
            $env:WINDOWS_CERT_TIMESTAMP_URL
        } else {
            'http://timestamp.digicert.com'
        }
        & signtool sign `
            /f  $PfxPath `
            /p  $env:WINDOWS_CERT_PFX_PASSWORD `
            /tr $TimestampUrl `
            /td sha256 `
            /fd sha256 `
            $OutExe
        if ($LASTEXITCODE -ne 0) {
            throw "signtool failed with exit code $LASTEXITCODE"
        }
        Write-Host '[build-installer-obs] Installer signed.' -ForegroundColor Green
    } finally {
        Remove-Item -Path $PfxPath -Force -ErrorAction SilentlyContinue
    }
} else {
    Write-Host '[build-installer-obs] WINDOWS_CERT_PFX_BASE64 not set — skipping signing.' -ForegroundColor Yellow
}

Write-Host "Output: $OutExe"
