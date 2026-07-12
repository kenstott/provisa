# Provisa native-tier CLI for Windows (REQ-979). Runs the bundled standalone
# Python runtime directly — no Docker, no VM, no containers. Mirrors the native
# commands in scripts/provisa (cmd_start_native / cmd_stop_native).
#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ProvisaHome = Join-Path $env:USERPROFILE '.provisa'
$ConfigPath  = Join-Path $ProvisaHome 'config.yaml'
$RuntimeDir  = Join-Path $ProvisaHome 'runtime'
$RuntimePy   = Join-Path $RuntimeDir 'python.exe'
$NativeDir   = Join-Path $ProvisaHome 'native'
$LogDir      = Join-Path $ProvisaHome '.logs'
$PidFile     = Join-Path $ProvisaHome '.native.pid'

function Write-Info { param($Msg) Write-Host "[provisa] $Msg" -ForegroundColor Cyan }
function Write-Err  { param($Msg) Write-Host "[provisa] $Msg" -ForegroundColor Red }
function Write-Ok   { param($Msg) Write-Host "[provisa] $Msg" -ForegroundColor Green }

# ── Config reader (no yaml parser needed; mirrors scripts/provisa read_config) ─
function Read-Config {
  param([string]$Key, [string]$Default)
  if (Test-Path $ConfigPath) {
    foreach ($line in Get-Content $ConfigPath) {
      if ($line -match "^\s*$([regex]::Escape($Key))\s*:\s*""?([^""]*?)""?\s*$") {
        $v = $Matches[1].Trim()
        if ($v) { return $v }
      }
    }
  }
  return $Default
}

$UiPort       = [int](Read-Config 'ui_port' '3000')
$ApiPort      = [int](Read-Config 'api_port' '8000')
$DeployEngine = Read-Config 'engine' 'duckdb'
$EngineUrl    = Read-Config 'engine_url' ''
$AutoOpen     = (Read-Config 'auto_open_browser' 'true') -eq 'true'

function Require-Runtime {
  if (-not (Test-Path $RuntimePy)) {
    Write-Err "Native runtime missing at $RuntimeDir. Re-run Provisa first-launch to complete setup."
    exit 1
  }
}

# Deployment env for the native process (mirrors scripts/provisa native_env_pairs
# and provisa.core.desktop_profile). SQLite URLs need forward slashes.
function Native-Env {
  $platformDb = (Join-Path $NativeDir 'platform.db') -replace '\\','/'
  $tenantDb   = (Join-Path $NativeDir 'tenant.db')   -replace '\\','/'
  $e = @{
    'PROVISA_ENGINE'        = $DeployEngine
    'PROVISA_REDIS_EMBEDDED' = '1'
    'PLATFORM_DATABASE_URL' = "sqlite+aiosqlite:///$platformDb"
    'TENANT_DATABASE_URL'   = "sqlite+aiosqlite:///$tenantDb"
  }
  if ($EngineUrl) { $e['PROVISA_ENGINE_URL'] = $EngineUrl }
  return $e
}

function Native-Running {
  if (-not (Test-Path $PidFile)) { return $false }
  foreach ($procId in (Get-Content $PidFile)) {
    if ($procId -and (Get-Process -Id ([int]$procId) -ErrorAction SilentlyContinue)) { return $true }
  }
  return $false
}

# Two uvicorn processes: the API app (packaged factory) and the UI server, which
# proxies API calls to PROVISA_API_URL and serves the built static UI.
function Start-Native {
  Require-Runtime
  New-Item -ItemType Directory -Path $LogDir, $NativeDir -Force | Out-Null
  if (Native-Running) { Write-Info 'Provisa is already running.'; return }

  $baseEnv = Native-Env
  foreach ($k in $baseEnv.Keys) { Set-Item -Path "Env:$k" -Value $baseEnv[$k] }

  $api = Start-Process -FilePath $RuntimePy `
    -ArgumentList @('-m','uvicorn','provisa.api.app:create_app','--factory','--host','0.0.0.0','--port',"$ApiPort") `
    -WindowStyle Hidden -PassThru `
    -RedirectStandardOutput (Join-Path $LogDir 'native-api.log') `
    -RedirectStandardError  (Join-Path $LogDir 'native-api.err.log')

  $env:PROVISA_API_URL = "http://localhost:$ApiPort"
  $ui = Start-Process -FilePath $RuntimePy `
    -ArgumentList @('-m','uvicorn','provisa.ui_server:app','--host','0.0.0.0','--port',"$UiPort") `
    -WindowStyle Hidden -PassThru `
    -RedirectStandardOutput (Join-Path $LogDir 'native-ui.log') `
    -RedirectStandardError  (Join-Path $LogDir 'native-ui.err.log')

  "$($api.Id)`n$($ui.Id)" | Set-Content -Path $PidFile -Encoding ASCII

  Write-Ok 'Provisa is running.'
  Write-Info "UI:  http://localhost:$UiPort"
  Write-Info "API: http://localhost:$ApiPort"
}

function Stop-Native {
  Write-Info 'Stopping Provisa...'
  if (Test-Path $PidFile) {
    foreach ($procId in (Get-Content $PidFile)) {
      if ($procId) { Stop-Process -Id ([int]$procId) -Force -ErrorAction SilentlyContinue }
    }
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
  }
  Write-Ok 'Provisa has been shut down.'
}

function Status-Native {
  if (Native-Running) { Write-Ok 'Provisa is running.' } else { Write-Info 'Provisa is not running.' }
}

function Open-Native {
  $url = "http://localhost:$UiPort"
  Write-Info "Opening $url"
  Start-Process $url
}

function Show-Help {
  Write-Host 'Usage: provisa <command>'
  Write-Host ''
  Write-Host 'Commands:'
  Write-Host '  start    Start Provisa (native — no Docker)'
  Write-Host '  stop     Stop Provisa'
  Write-Host '  restart  Restart Provisa'
  Write-Host '  status   Show run status'
  Write-Host '  open     Open the UI in your browser'
  Write-Host '  help     Show this help'
}

$command = if ($args.Count -gt 0) { $args[0] } else { 'help' }
switch ($command) {
  'start'   { Start-Native; if ($AutoOpen) { Start-Sleep 2; Open-Native } }
  'stop'    { Stop-Native }
  'restart' { Stop-Native; Start-Native }
  'status'  { Status-Native }
  'open'    { Open-Native }
  'help'    { Show-Help }
  default   { Write-Err "Unknown command: $command"; Show-Help; exit 1 }
}
