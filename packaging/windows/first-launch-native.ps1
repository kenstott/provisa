# Provisa first-launch (native tier, REQ-979) for Windows. Stages the bundled
# standalone Python runtime to %USERPROFILE%\.provisa\runtime, writes config, and
# starts the app. No Docker, no VirtualBox, no containers. Mirrors the native path
# in macOS first-launch.sh (stage_native_runtime + write_config).
#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ScriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProvisaHome = Join-Path $env:USERPROFILE '.provisa'
$ConfigPath  = Join-Path $ProvisaHome 'config.yaml'
$RuntimeSrc  = Join-Path $ScriptDir 'runtime'
$RuntimeDst  = Join-Path $ProvisaHome 'runtime'
$Sentinel    = Join-Path $ProvisaHome '.setup-complete'

function Write-Info { param($Msg) Write-Host "[provisa] $Msg" -ForegroundColor Cyan }
function Write-Err  { param($Msg) Write-Host "[provisa] $Msg" -ForegroundColor Red }
function Write-Ok   { param($Msg) Write-Host "[provisa] $Msg" -ForegroundColor Green }

New-Item -ItemType Directory -Path $ProvisaHome -Force | Out-Null

# ── Stage the native runtime ──────────────────────────────────────────────────
# Re-stages when the installed bundle's VERSION differs from what is already staged, so an UPGRADE
# actually replaces the runtime. The old "skip if python.exe exists" logic pinned users to their
# first install's runtime forever — an install that shipped a missing/updated dependency never took
# effect (e.g. aiosqlite added in a later build was never delivered).
function Stage-Runtime {
  $dstPy      = Join-Path $RuntimeDst 'python.exe'
  $srcPy      = Join-Path $RuntimeSrc 'python.exe'
  $bundleVer  = if (Test-Path (Join-Path $ScriptDir 'VERSION')) {
    (Get-Content (Join-Path $ScriptDir 'VERSION') -Raw).Trim()
  } else { '' }
  $stagedVerF = Join-Path $RuntimeDst '.runtime-version'
  $stagedVer  = if (Test-Path $stagedVerF) { (Get-Content $stagedVerF -Raw).Trim() } else { '' }

  # Up to date: a runtime exists and (no version info OR the versions match) → keep it.
  if ((Test-Path $dstPy) -and (($bundleVer -eq '') -or ($bundleVer -eq $stagedVer))) { return }

  if (-not (Test-Path $srcPy)) {
    Write-Err "Native runtime not found beside the installer. This installer was not built with the native tier."
    exit 1
  }
  if (Test-Path $RuntimeDst) {
    Write-Info "Upgrading native runtime to bundle $bundleVer (was $stagedVer)..."
    # A Provisa instance from a prior install keeps python.exe running and LOCKS the runtime's
    # DLLs (libcrypto etc.), so Remove-Item fails and the upgrade silently aborts — leaving the
    # stale process serving the old (configless, no-demo) app. Stop anything running out of the
    # staged runtime (and the app scripts) first, then re-stage.
    Get-Process -ErrorAction SilentlyContinue | Where-Object {
      $_.Path -and ($_.Path -like "$RuntimeDst\*" -or $_.Path -like "$ScriptDir\*")
    } | Stop-Process -Force -ErrorAction SilentlyContinue
    if (Test-Path (Join-Path $ProvisaHome '.native.pid')) {
      foreach ($procId in (Get-Content (Join-Path $ProvisaHome '.native.pid'))) {
        if ($procId) { Stop-Process -Id ([int]$procId) -Force -ErrorAction SilentlyContinue }
      }
    }
    Start-Sleep -Seconds 2
    Remove-Item $RuntimeDst -Recurse -Force
  } else {
    Write-Info "Staging native runtime to $RuntimeDst..."
  }
  Copy-Item -Path $RuntimeSrc -Destination $RuntimeDst -Recurse -Force
  if ($bundleVer -ne '') { Set-Content -Path $stagedVerF -Value $bundleVer -Encoding ASCII }
  Write-Ok 'Native runtime staged.'
}

# ── Deployment selection (parity with macOS SwiftUI wizard, REQ-972..979) ─────
# Non-interactive mode reads the same env the macOS wizard forwards. Interactive
# mode prompts for the subset the native tier can fulfil; Trino and the Docker
# Grafana demo require the container tier (install-container.ps1), so they are
# rejected here rather than written to a config the native runtime cannot honor.
function Resolve-Deployment {
  if ($env:PROVISA_NONINTERACTIVE) {
    $script:Engine        = if ($env:PROVISA_ENGINE)        { $env:PROVISA_ENGINE }        else { 'duckdb' }
    $script:EngineUrl     = if ($env:PROVISA_ENGINE_URL)    { $env:PROVISA_ENGINE_URL }    else { '' }
    $script:MaterializeUrl= if ($env:PROVISA_MATERIALIZE_URL){ $env:PROVISA_MATERIALIZE_URL } else { '' }
    $script:ObsMode       = if ($env:PROVISA_OBS_MODE)      { $env:PROVISA_OBS_MODE }      else { 'none' }
    $script:OtlpEndpoint  = if ($env:PROVISA_OTLP_ENDPOINT) { $env:PROVISA_OTLP_ENDPOINT } else { '' }
    $script:Demo          = if ($env:PROVISA_INSTALL_DEMO -match '^(y|Y|true)') { 'true' } else { 'false' }
    if ($script:Engine -eq 'trino' -or $script:ObsMode -eq 'docker') {
      Write-Err "engine=trino / obs=docker require the container tier. Run install-container.ps1."
      exit 1
    }
    return
  }

  Write-Host ''
  Write-Host 'Federation engine' -ForegroundColor White
  Write-Host '  1) Embedded database (recommended)'
  Write-Host '  2) Trino - Docker (container tier)'
  Write-Host '  3) External engine'
  switch ((Read-Host 'Choose 1-3 [1]')) {
    '2' {
      Write-Err 'Trino requires the container tier. Run install-container.ps1 instead.'
      exit 1
    }
    '3' {
      $script:Engine = 'sqlalchemy'
      $script:EngineUrl     = Read-Host 'External engine URL (e.g. postgresql+psycopg://user:pass@host:5432/db)'
      $script:MaterializeUrl= Read-Host 'Materialization store URL (optional)'
    }
    default { $script:Engine = 'duckdb'; $script:EngineUrl = ''; $script:MaterializeUrl = '' }
  }

  Write-Host ''
  Write-Host 'Observability integration' -ForegroundColor White
  Write-Host '  1) Built-in only'
  Write-Host '  2) Bundled Grafana/Prometheus demo (Docker, container tier)'
  Write-Host '  3) Export to my collector'
  switch ((Read-Host 'Choose 1-3 [1]')) {
    '2' {
      Write-Err 'The Docker Grafana demo requires the container tier. Run install-container.ps1 instead.'
      exit 1
    }
    '3' { $script:ObsMode = 'collector'; $script:OtlpEndpoint = Read-Host 'OTLP collector endpoint (e.g. http://collector-host:4317)' }
    default { $script:ObsMode = 'none'; $script:OtlpEndpoint = '' }
  }

  Write-Host 'The demo is a complete, fully functional install — pick it with confidence; nothing is limited.' -ForegroundColor DarkGray
  Write-Host 'To reconfigure with other options later, just run this setup again.' -ForegroundColor DarkGray
  $script:Demo = if ((Read-Host 'Install the demo dataset with guided tour (y/N)') -match '^(y|Y)') { 'true' } else { 'false' }
}

# ── Write config (native tier) ────────────────────────────────────────────────
function Write-ProvisaConfig {
  if (Test-Path $ConfigPath) { return }
  $hostname = if ($env:PROVISA_HOSTNAME)  { $env:PROVISA_HOSTNAME }  else { 'localhost' }
  $uiPort   = if ($env:PROVISA_UI_PORT)   { $env:PROVISA_UI_PORT }   else { '3000' }
  $apiPort  = if ($env:PROVISA_API_PORT)  { $env:PROVISA_API_PORT }  else { '8000' }
  Resolve-Deployment
  @"
# Provisa configuration — generated by installer (native tier)
hostname: $hostname
ui_port: $uiPort
api_port: $apiPort
auto_open_browser: true
runtime: native
# Deployment (REQ-972..979): parity with the macOS SwiftUI wizard.
engine: $script:Engine
engine_url: "$script:EngineUrl"
materialize_url: "$script:MaterializeUrl"
obs_mode: $script:ObsMode
otlp_endpoint: "$script:OtlpEndpoint"
demo: $script:Demo
demo_mode: native
"@ | Set-Content -Path $ConfigPath -Encoding UTF8
  Write-Ok "Config written to $ConfigPath"
}

# ── Next-steps guidance (REQ-1005) ────────────────────────────────────────────
# The native tier ships core + DuckDB only. Tell the user, in tier order, that the
# federation engine (Trino), observability stack, and demo data pack are NOT part
# of the native tier and how to add each via the layered installers. Trino is what
# initializes the federation engine, so it comes first (Obs requires the container
# tier; Demo requires Core + Obs).
function Show-NextSteps {
  Write-Host ''
  Write-Host 'Next steps — extend your native install' -ForegroundColor White
  Write-Host '═══════════════════════════════════════════════════'
  Write-Host 'You installed the native tier: local query engine (core + embedded database).'
  Write-Host 'The federation engine (Trino), the observability stack, and the demo'
  Write-Host 'data pack are NOT part of the native tier. Add them via layered installers:'
  Write-Host ''
  Write-Host '  1) Container installer (Provisa-Container-*.exe)' -ForegroundColor Cyan
  Write-Host '     Provisions WSL2 + containerd + Trino. Run this to initialize the'
  Write-Host '     federation engine (Trino). Required before Obs or Demo.'
  Write-Host '  2) Obs installer (Provisa-Obs-*.exe)' -ForegroundColor Cyan
  Write-Host '     Observability stack (collector + Prometheus + Grafana).'
  Write-Host '     Requires the container tier.'
  Write-Host '  3) Demo installer (Provisa-Demo-*.exe)' -ForegroundColor Cyan
  Write-Host '     Demo data pack with guided tour. Requires Core + Obs.'
  Write-Host ''
  Write-Host 'To initialize the federation engine now, run the Container installer' -ForegroundColor Yellow
  Write-Host '(Provisa-Container-*.exe), then re-run setup and choose Trino.' -ForegroundColor Yellow
  Write-Host ''
}

# ── Main ──────────────────────────────────────────────────────────────────────
Write-Host ''
Write-Host 'Provisa — First Launch Setup (native — no Docker)' -ForegroundColor White
Write-Host '═══════════════════════════════════════════════════'
Write-Host ''

Stage-Runtime
Write-ProvisaConfig
New-Item -ItemType File -Path $Sentinel -Force | Out-Null
Write-Ok 'First-launch setup complete.'

Show-NextSteps

# Hand off to the native CLI to start + open the UI.
# restart (not start): an install/rerun must apply the freshly written config — stop any running
# instance first, then start with the new engine/demo/auto-open choices. start alone would no-op
# against an already-running app and silently ignore the new choices.
& powershell.exe -ExecutionPolicy Bypass -File (Join-Path $ScriptDir 'provisa-native.ps1') restart
