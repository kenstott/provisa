# Build Provisa Observability Windows installer. Run from repo root.
# Requires: Core installer already installed (VirtualBox VM exists).
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
# Provisa Observability Extension Installer (Windows / VirtualBox)
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

# Check VirtualBox VM exists and is running
$VmName = 'Provisa'
try {
    $vmState = & VBoxManage showvminfo $VmName --machinereadable 2>&1 |
        Select-String '^VMState=' | ForEach-Object { $_ -replace '^VMState=','' -replace '"','' }
} catch {
    Write-Err "VBoxManage not found or VM '$VmName' not registered. Install Provisa Core first."
    exit 1
}
if ($vmState -ne 'running') {
    Write-Err "Provisa VM is not running. Start Provisa first (provisa start), then retry."
    exit 1
}

# Load obs images into VM via docker load
$TarFiles = Get-ChildItem -Path $ImagesDir -Filter '*.tar.gz'
foreach ($f in $TarFiles) {
    Write-Info "Loading: $($f.Name)"
    $result = & VBoxManage guestcontrol $VmName run `
        --exe '/bin/bash' --username root `
        -- '-c' "gunzip -c '$($f.FullName)' | docker load" 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Err "Failed to load $($f.Name): $result"
        exit 1
    }
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
InstallDir "`$LOCALAPPDATA\Provisa-Obs"
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
