# Provisa CLI for Windows — manages the VM and wraps docker compose commands.
#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ConfigPath = Join-Path $env:USERPROFILE '.provisa\config.yaml'

function Write-Info { param($Msg) Write-Host "[provisa] $Msg" -ForegroundColor Cyan }
function Write-Err  { param($Msg) Write-Host "[provisa] $Msg" -ForegroundColor Red }
function Write-Ok   { param($Msg) Write-Host "[provisa] $Msg" -ForegroundColor Green }

# ── Read config ────────────────────────────────────────────────────────────────
function Read-Config {
  if (-not (Test-Path $ConfigPath)) {
    Write-Err "Config not found: $ConfigPath"
    Write-Err 'Run first-launch.ps1 to initialise Provisa.'
    exit 1
  }

  $cfg = @{
    ProjectDir = $null
    UiPort     = 3000
    ApiPort    = 8000
    Runtime    = 'virtualbox'
    VmName     = 'Provisa'
    DockerHost = 'tcp://127.0.0.1:2375'
  }

  foreach ($line in Get-Content $ConfigPath) {
    if ($line -match '^\s*project_dir\s*:\s*"?([^"]+)"?\s*$')  { $cfg.ProjectDir = $Matches[1].Trim() }
    if ($line -match '^\s*ui_port\s*:\s*(\d+)')                { $cfg.UiPort     = [int]$Matches[1] }
    if ($line -match '^\s*api_port\s*:\s*(\d+)')               { $cfg.ApiPort    = [int]$Matches[1] }
    if ($line -match '^\s*runtime\s*:\s*"?([^"]+)"?\s*$')      { $cfg.Runtime    = $Matches[1].Trim() }
    if ($line -match '^\s*vm_name\s*:\s*"?([^"]+)"?\s*$')      { $cfg.VmName     = $Matches[1].Trim() }
    if ($line -match '^\s*docker_host\s*:\s*"?([^"]+)"?\s*$')  { $cfg.DockerHost = $Matches[1].Trim() }
  }

  if (-not $cfg.ProjectDir) {
    Write-Err 'project_dir not set in config.yaml'; exit 1
  }

  return $cfg
}

# ── Locate VBoxManage ─────────────────────────────────────────────────────────
function Find-VBoxManage {
  $cmd = Get-Command VBoxManage -ErrorAction SilentlyContinue
  if ($cmd) { return $cmd.Source }
  foreach ($p in @(
    "$env:ProgramFiles\Oracle\VirtualBox\VBoxManage.exe",
    "${env:ProgramFiles(x86)}\Oracle\VirtualBox\VBoxManage.exe"
  )) {
    if (Test-Path $p) { return $p }
  }
  return $null
}

# ── Ensure VM is running ──────────────────────────────────────────────────────
function Ensure-VmRunning {
  param([hashtable]$Config)

  if ($Config.Runtime -ne 'virtualbox') { return }

  $vbox = Find-VBoxManage
  if (-not $vbox) {
    Write-Err 'VBoxManage not found. Run first-launch.ps1 to complete setup.'
    exit 1
  }

  $env:DOCKER_HOST = $Config.DockerHost

  $info  = & $vbox showvminfo $Config.VmName --machinereadable 2>&1
  $state = ($info | Select-String 'VMState=').ToString() -replace '.*="(.*)".*', '$1'

  if ($state -eq 'running') { return }

  Write-Info 'Starting Provisa VM...'
  & $vbox startvm $Config.VmName --type headless
  if ($LASTEXITCODE -ne 0) { Write-Err 'Failed to start VM.'; exit 1 }

  $retries = 30
  while ($retries -gt 0) {
    $out = docker info 2>&1
    if ($LASTEXITCODE -eq 0) { break }
    Start-Sleep 2
    $retries--
  }
  if ($retries -eq 0) { Write-Err 'Docker API did not respond.'; exit 1 }
  Write-Ok 'VM ready.'
}

# ── Stop VM ───────────────────────────────────────────────────────────────────
function Stop-Vm {
  param([hashtable]$Config)

  if ($Config.Runtime -ne 'virtualbox') { return }

  $vbox = Find-VBoxManage
  if (-not $vbox) { return }

  $info  = & $vbox showvminfo $Config.VmName --machinereadable 2>&1
  $state = ($info | Select-String 'VMState=').ToString() -replace '.*="(.*)".*', '$1'

  if ($state -ne 'running') {
    Write-Info 'VM is not running.'
    return
  }

  Write-Info 'Stopping Provisa VM...'
  & $vbox controlvm $Config.VmName acpipowerbutton
  Write-Ok 'VM shutdown initiated.'
}

# ── Compose helper ─────────────────────────────────────────────────────────────
function Invoke-Compose {
  param([hashtable]$Config, [string[]]$ComposeArgs)
  $env:DOCKER_HOST = $Config.DockerHost
  $compose1 = Join-Path $Config.ProjectDir 'docker-compose.core.yml'
  $compose2 = Join-Path $Config.ProjectDir 'docker-compose.app.yml'
  $compose3 = Join-Path $Config.ProjectDir 'docker-compose.airgap.yml'
  docker compose -f $compose1 -f $compose2 -f $compose3 @ComposeArgs
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

# ── Commands ───────────────────────────────────────────────────────────────────
function cmd-start {
  param([hashtable]$Config)
  Ensure-VmRunning $Config
  Write-Info 'Starting Provisa services...'
  Invoke-Compose $Config @('up', '-d', '--remove-orphans')
  Write-Info "UI:  http://localhost:$($Config.UiPort)"
  Write-Info "API: http://localhost:$($Config.ApiPort)"
}

function cmd-stop {
  param([hashtable]$Config)
  Write-Info 'Stopping Provisa services...'
  $env:DOCKER_HOST = $Config.DockerHost
  Invoke-Compose $Config @('down')
  Stop-Vm $Config
}

function cmd-restart {
  param([hashtable]$Config)
  Ensure-VmRunning $Config
  Write-Info 'Restarting Provisa services...'
  Invoke-Compose $Config @('restart')
}

function cmd-status {
  param([hashtable]$Config)
  Ensure-VmRunning $Config
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
  Ensure-VmRunning $Config
  Invoke-Compose $Config @('logs', '--follow')
}

function cmd-help {
  Write-Host 'Usage: provisa <command>'
  Write-Host ''
  Write-Host 'Commands:'
  Write-Host '  start    Start Provisa VM and all services'
  Write-Host '  stop     Stop all services and shut down VM'
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
