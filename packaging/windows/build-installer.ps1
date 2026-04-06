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

# ── Create dist dir ────────────────────────────────────────────────────────────
$DistDir = Join-Path $ScriptDir 'dist'
New-Item -ItemType Directory -Path $DistDir -Force | Out-Null

# ── Run NSIS ───────────────────────────────────────────────────────────────────
Write-Host '[build-installer] Running makensis...' -ForegroundColor Cyan
$NsiScript = Join-Path $ScriptDir 'installer.nsi'
$Version = if ($env:VERSION) { $env:VERSION } else { 'dev' }
& makensis /DVERSION=$Version $NsiScript
if ($LASTEXITCODE -ne 0) {
  throw "makensis failed with exit code $LASTEXITCODE"
}

Write-Host '[build-installer] Installer created.' -ForegroundColor Green
Write-Host "Output: $(Join-Path $DistDir 'Provisa-Setup.exe')"
