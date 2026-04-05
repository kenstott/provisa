# Provisa CLI for Windows — wraps docker compose commands.
#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ConfigPath = Join-Path $env:USERPROFILE '.provisa\config.yaml'

function Write-Info { param($Msg) Write-Host "[provisa] $Msg" -ForegroundColor Cyan }
function Write-Err  { param($Msg) Write-Host "[provisa] $Msg" -ForegroundColor Red }

# ── Read config ────────────────────────────────────────────────────────────────
function Read-Config {
  if (-not (Test-Path $ConfigPath)) {
    Write-Err "Config not found: $ConfigPath"
    Write-Err 'Run first-launch.ps1 to initialise Provisa.'
    exit 1
  }

  $projectDir = $null
  $uiPort     = 3000
  $apiPort    = 8000

  foreach ($line in Get-Content $ConfigPath) {
    if ($line -match '^\s*project_dir\s*:\s*"?([^"]+)"?\s*$') {
      $projectDir = $Matches[1].Trim()
    }
    if ($line -match '^\s*ui_port\s*:\s*(\d+)') {
      $uiPort = [int]$Matches[1]
    }
    if ($line -match '^\s*api_port\s*:\s*(\d+)') {
      $apiPort = [int]$Matches[1]
    }
  }

  if (-not $projectDir) {
    Write-Err 'project_dir not set in config.yaml'
    exit 1
  }

  return @{ ProjectDir = $projectDir; UiPort = $uiPort; ApiPort = $apiPort }
}

# ── Compose helper ─────────────────────────────────────────────────────────────
function Invoke-Compose {
  param([hashtable]$Config, [string[]]$ComposeArgs)
  $compose1 = Join-Path $Config.ProjectDir 'docker-compose.yml'
  $compose2 = Join-Path $Config.ProjectDir 'docker-compose.prod.yml'
  docker compose -f $compose1 -f $compose2 @ComposeArgs
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

# ── Commands ───────────────────────────────────────────────────────────────────
function cmd-start {
  param([hashtable]$Config)
  Write-Info 'Starting Provisa services...'
  Invoke-Compose $Config @('up', '-d', '--remove-orphans')
  Write-Info "UI:  http://localhost:$($Config.UiPort)"
  Write-Info "API: http://localhost:$($Config.ApiPort)"
}

function cmd-stop {
  param([hashtable]$Config)
  Write-Info 'Stopping Provisa services...'
  Invoke-Compose $Config @('down')
}

function cmd-restart {
  param([hashtable]$Config)
  Write-Info 'Restarting Provisa services...'
  Invoke-Compose $Config @('restart')
}

function cmd-status {
  param([hashtable]$Config)
  Invoke-Compose $Config @('ps')
}

function cmd-open {
  param([hashtable]$Config)
  $url = "http://localhost:$($Config.UiPort)"
  Write-Info "Opening $url"
  Start-Process $url
}

function cmd-logs {
  param([hashtable]$Config)
  Invoke-Compose $Config @('logs', '--follow')
}

function cmd-help {
  Write-Host 'Usage: provisa <command>'
  Write-Host ''
  Write-Host 'Commands:'
  Write-Host '  start    Start all Provisa services'
  Write-Host '  stop     Stop all Provisa services'
  Write-Host '  restart  Restart all services'
  Write-Host '  status   Show service status'
  Write-Host '  open     Open the UI in your browser'
  Write-Host '  logs     Follow service logs'
  Write-Host '  help     Show this help'
}

# ── Dispatch ───────────────────────────────────────────────────────────────────
$command = if ($args.Count -gt 0) { $args[0] } else { 'help' }

switch ($command) {
  'start'   { cmd-start   (Read-Config) }
  'stop'    { cmd-stop    (Read-Config) }
  'restart' { cmd-restart (Read-Config) }
  'status'  { cmd-status  (Read-Config) }
  'open'    { cmd-open    (Read-Config) }
  'logs'    { cmd-logs    (Read-Config) }
  'help'    { cmd-help }
  default {
    Write-Err "Unknown command: $command"
    cmd-help
    exit 1
  }
}
