# Build Provisa Windows installer. Run from repo root.
#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot  = Split-Path -Parent (Split-Path -Parent $ScriptDir)

Write-Host '[build-installer] Preparing build directory...' -ForegroundColor Cyan

# ── Assemble build tree ────────────────────────────────────────────────────────
$BuildDir = Join-Path $ScriptDir 'build'

# images/
$BuildImages = Join-Path $BuildDir 'images'
New-Item -ItemType Directory -Path $BuildImages -Force | Out-Null
Copy-Item (Join-Path $ScriptDir 'images\*.tar') $BuildImages

# compose/
$BuildCompose = Join-Path $BuildDir 'compose'
New-Item -ItemType Directory -Path $BuildCompose -Force | Out-Null
Copy-Item (Join-Path $RepoRoot 'docker-compose.yml')      $BuildCompose
Copy-Item (Join-Path $RepoRoot 'docker-compose.prod.yml') $BuildCompose
Copy-Item (Join-Path $RepoRoot 'config')  (Join-Path $BuildCompose 'config')  -Recurse -Force
Copy-Item (Join-Path $RepoRoot 'db')      (Join-Path $BuildCompose 'db')      -Recurse -Force
Copy-Item (Join-Path $RepoRoot 'trino')   (Join-Path $BuildCompose 'trino')   -Recurse -Force

# scripts
Copy-Item (Join-Path $ScriptDir 'first-launch.ps1') $BuildDir
Copy-Item (Join-Path $ScriptDir 'provisa.ps1')       $BuildDir
Copy-Item (Join-Path $ScriptDir 'provisa.cmd')       $BuildDir

# VM image (OVA) — built by the build-vm-image CI job
$OvaSrc = Join-Path $ScriptDir 'provisa-runtime.ova'
if (-not (Test-Path $OvaSrc)) {
  throw "provisa-runtime.ova not found at $OvaSrc — run the build-vm-image CI job first."
}
Copy-Item $OvaSrc $BuildDir

# VirtualBox silent installer — downloaded by CI or placed manually
$BuildRedist = Join-Path $BuildDir 'redist'
New-Item -ItemType Directory -Path $BuildRedist -Force | Out-Null
$VBoxSrc = Join-Path $ScriptDir 'redist\VirtualBox-setup.exe'
if (-not (Test-Path $VBoxSrc)) {
  throw "VirtualBox-setup.exe not found at $VBoxSrc — CI should download it before building."
}
Copy-Item $VBoxSrc $BuildRedist

# ── Create dist dir ────────────────────────────────────────────────────────────
$DistDir = Join-Path $ScriptDir 'dist'
New-Item -ItemType Directory -Path $DistDir -Force | Out-Null

# ── Run NSIS ───────────────────────────────────────────────────────────────────
Write-Host '[build-installer] Running makensis...' -ForegroundColor Cyan
$NsiScript = Join-Path $ScriptDir 'installer.nsi'
$Version   = if ($env:VERSION) { $env:VERSION } else { 'dev' }
& makensis /DVERSION=$Version $NsiScript
if ($LASTEXITCODE -ne 0) {
  throw "makensis failed with exit code $LASTEXITCODE"
}

Write-Host '[build-installer] Installer created.' -ForegroundColor Green

# ── Code signing ───────────────────────────────────────────────────────────────
$ExePath = Join-Path $DistDir 'Provisa-Setup.exe'
if ($env:WINDOWS_CERT_PFX_BASE64) {
    Write-Host '[build-installer] Signing installer...' -ForegroundColor Cyan
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
            $ExePath
        if ($LASTEXITCODE -ne 0) {
            throw "signtool failed with exit code $LASTEXITCODE"
        }
        Write-Host '[build-installer] Installer signed.' -ForegroundColor Green
    } finally {
        Remove-Item -Path $PfxPath -Force -ErrorAction SilentlyContinue
    }
} else {
    Write-Host '[build-installer] WINDOWS_CERT_PFX_BASE64 not set — skipping signing.' -ForegroundColor Yellow
}

Write-Host "Output: $ExePath"
