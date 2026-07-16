# Build Provisa Demo Windows installer. Run from repo root.
# Requires: Core + Observability installers already installed.
#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot  = Split-Path -Parent (Split-Path -Parent $ScriptDir)

Write-Host '[build-installer-demo] Preparing demo build directory...' -ForegroundColor Cyan

$BuildDir = Join-Path $ScriptDir 'build-demo'
New-Item -ItemType Directory -Path $BuildDir -Force | Out-Null

# -- Demo images only -----------------------------------------------------------
$BuildImages = Join-Path $BuildDir 'images'
New-Item -ItemType Directory -Path $BuildImages -Force | Out-Null

Get-ChildItem -Path (Join-Path $ScriptDir 'demo-images') -Filter '*.tar.gz' -ErrorAction SilentlyContinue |
  Copy-Item -Destination $BuildImages

# -- Extension installer script -------------------------------------------------
$InstallDemoScript = @'
# Provisa Demo Extension Installer (Windows / VirtualBox)
# Requires Observability extension to be installed first.
#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ProvHome   = Join-Path $env:USERPROFILE '.provisa'
$ObsExt     = Join-Path $ProvHome 'extensions\observability\docker-compose.observability.yml'
$ExtDir     = Join-Path $ProvHome 'extensions\demo'
$ExtCompose = Join-Path $ExtDir 'docker-compose.demo.yml'
$ImagesDir  = Join-Path $PSScriptRoot 'images'

function Write-Info  { Write-Host "[provisa-demo] $args" -ForegroundColor Cyan }
function Write-Ok    { Write-Host "[provisa-demo] $args" -ForegroundColor Green }
function Write-Err   { Write-Host "[provisa-demo] $args" -ForegroundColor Red }

# Check core installed
if (-not (Test-Path (Join-Path $ProvHome 'config.yaml'))) {
    Write-Err 'Provisa Core is not installed. Run Provisa-Setup.exe first.'
    exit 1
}
# Check obs extension installed
if (-not (Test-Path $ObsExt)) {
    Write-Err 'Provisa Observability is not installed. Run Provisa-Obs-Setup.exe first.'
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

# Load demo images into WSL2 via nerdctl
$TarFiles = Get-ChildItem -Path $ImagesDir -Filter '*.tar.gz'
foreach ($f in $TarFiles) {
    Write-Info "Loading: $($f.Name)"
    $drive = $f.FullName[0].ToString().ToLower()
    $rest  = $f.FullName.Substring(2) -replace '\\', '/'
    $wslPath = "/mnt/$drive$rest"
    wsl nerdctl load -i $wslPath
    if ($LASTEXITCODE -ne 0) { Write-Err "Failed to load $($f.Name)"; exit 1 }
}
Write-Ok "Demo images loaded."

# Write extension compose file
New-Item -ItemType Directory -Path $ExtDir -Force | Out-Null
$ComposeSrc = Join-Path $ProvHome 'compose\docker-compose.demo.yml'
if (Test-Path $ComposeSrc) {
    Copy-Item $ComposeSrc $ExtCompose -Force
    Write-Ok "Extension compose file written: $ExtCompose"
} else {
    # Write inline demo compose - graphql-demo uses pre-built local image
    @"
# Provisa Demo Extension - petstore-mock + graphql-demo
services:
  petstore-mock:
    image: swaggerapi/petstore3:unstable
    platform: linux/amd64
    ports:
      - "18080:8080"
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:8080/api/v3/pet/findByStatus?status=available || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 12
      start_period: 30s

  graphql-demo:
    image: provisa/graphql-demo:local
    ports:
      - "4000:4000"
    healthcheck:
      test: ["CMD-SHELL", "python -c \`"import urllib.request; urllib.request.urlopen('http://localhost:4000/graphql?query=%7B__typename%7D')\`""]
      interval: 10s
      timeout: 5s
      retries: 10
      start_period: 30s
"@ | Set-Content -Path $ExtCompose -Encoding UTF8
    Write-Ok "Extension compose file written: $ExtCompose"
}

Write-Ok 'Demo installed.'
Write-Host ''
Write-Host 'Restart Provisa to activate the demo services.' -ForegroundColor Green
Write-Host 'Petstore API: http://localhost:18080/api/v3'
Write-Host 'GraphQL demo: http://localhost:4000/graphql'
Write-Host ''
'@

$InstallDemoScript | Set-Content -Path (Join-Path $BuildDir 'install-demo.ps1') -Encoding UTF8

# -- dist dir ------------------------------------------------------------------
$DistDir = Join-Path $ScriptDir 'dist'
New-Item -ItemType Directory -Path $DistDir -Force | Out-Null

# -- NSIS script ---------------------------------------------------------------
$NsiPath = Join-Path $BuildDir 'installer-demo.nsi'
$Version = if ($env:VERSION) { $env:VERSION } else { 'dev' }
$OutExe  = Join-Path $DistDir 'Provisa-Demo-Setup.exe'

@"
!define PRODUCT_NAME "Provisa Demo"
!define PRODUCT_VERSION "$Version"
!define INSTALLER_NAME "Provisa-Demo-Setup.exe"

Name "`${PRODUCT_NAME} `${PRODUCT_VERSION}"
OutFile "$OutExe"
InstallDir "`$APPDATA\Provisa-Demo"
RequestExecutionLevel user
SetCompressor lzma

Page instfiles

Section "Install"
  SetOutPath "`$INSTDIR"
  File /r "$BuildDir\images"
  File "$BuildDir\install-demo.ps1"
  ExecWait 'powershell.exe -ExecutionPolicy Bypass -File "`$INSTDIR\install-demo.ps1"'
SectionEnd
"@ | Set-Content -Path $NsiPath -Encoding UTF8

Write-Host '[build-installer-demo] Running makensis...' -ForegroundColor Cyan
& makensis $NsiPath
if ($LASTEXITCODE -ne 0) {
    throw "makensis failed with exit code $LASTEXITCODE"
}

Write-Host '[build-installer-demo] Demo installer created.' -ForegroundColor Green

# -- Code signing ---------------------------------------------------------------
if ($env:WINDOWS_CERT_PFX_BASE64) {
    Write-Host '[build-installer-demo] Signing installer...' -ForegroundColor Cyan
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
        Write-Host '[build-installer-demo] Installer signed.' -ForegroundColor Green
    } finally {
        Remove-Item -Path $PfxPath -Force -ErrorAction SilentlyContinue
    }
} else {
    Write-Host '[build-installer-demo] WINDOWS_CERT_PFX_BASE64 not set - skipping signing.' -ForegroundColor Yellow
}

Write-Host "Output: $OutExe"
