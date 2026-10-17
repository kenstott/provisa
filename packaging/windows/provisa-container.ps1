# Provisa container-tier CLI for Windows (REQ-889 / REQ-633). Routes the full
# compose stack (Trino + compute) through nerdctl inside the Provisa WSL2 distro
# - the Windows equivalent of the macOS Lima tier. No VirtualBox. The base native
# tier (provisa-native.ps1) stays available; this tier is an additive, reversible
# upgrade. Mirrors the compose routing in scripts/provisa (RUNTIME=lima).
#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ProvisaHome = Join-Path $env:USERPROFILE '.provisa'
$ConfigPath  = Join-Path $ProvisaHome 'config.yaml'
$Distro      = 'provisa'
$ComposeGuest = '/opt/provisa/compose'   # compose tree copied into the distro at install

function Write-Info { param($Msg) Write-Host "[provisa] $Msg" -ForegroundColor Cyan }
function Write-Err  { param($Msg) Write-Host "[provisa] $Msg" -ForegroundColor Red }
function Write-Ok   { param($Msg) Write-Host "[provisa] $Msg" -ForegroundColor Green }

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

$UiPort  = [int](Read-Config 'ui_port' '3000')
$ApiPort = [int](Read-Config 'api_port' '8000')
$Obs     = (Read-Config 'obs' 'false') -eq 'true'
$Demo    = (Read-Config 'demo' 'false') -eq 'true'

# -- WSL runtime guards --------------------------------------------------------
function Require-Distro {
  $null = & wsl.exe --list --quiet 2>$null
  if ($LASTEXITCODE -ne 0) { Write-Err 'WSL2 is not available. Run install-container.ps1 to complete setup.'; exit 1 }
  $distros = (& wsl.exe --list --quiet) -replace "`0", '' | ForEach-Object { $_.Trim() } | Where-Object { $_ }
  if ($distros -notcontains $Distro) {
    Write-Err "Provisa WSL distro '$Distro' not found. Run install-container.ps1 to complete setup."
    exit 1
  }
}

function Ensure-Containerd {
  & wsl.exe -d $Distro -u root sh /opt/provisa/wsl/start-containerd.sh
  if ($LASTEXITCODE -ne 0) { Write-Err 'containerd failed to start inside WSL.'; exit 1 }
}

# Windows path -> WSL /mnt path (C:\a\b -> /mnt/c/a/b).
function To-WslPath {
  param([string]$WinPath)
  $drive = $WinPath[0].ToString().ToLower()
  $rest  = $WinPath.Substring(2) -replace '\\', '/'
  return "/mnt/$drive$rest"
}

# -- Store credentials ---------------------------------------------------------
# The compose stack sets no credential of its own (docker-compose.core.yml). This machine's are
# generated once and kept in the user's profile, as scripts/provisa keeps them on macOS/Linux, and
# handed to compose as its env file. The stores publish on loopback: nothing else dials them.
$CredentialsPath = Join-Path $ProvisaHome 'store-credentials.env'

function New-Secret {
  $bytes = New-Object byte[] 24
  [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
  return -join ($bytes | ForEach-Object { $_.ToString('x2') })
}

function Ensure-StoreCredentials {
  $values = [ordered]@{}
  if (Test-Path $CredentialsPath) {
    foreach ($line in Get-Content $CredentialsPath) {
      if ($line -match '^([A-Z_]+)=(.*)$') { $values[$Matches[1]] = $Matches[2] }
    }
  }
  foreach ($name in 'PROVISA_PG_PASSWORD', 'PROVISA_REDIS_PASSWORD', 'PROVISA_MINIO_ROOT_PASSWORD',
                    'PROVISA_GRAFANA_ADMIN_PASSWORD') {
    if (-not $values.Contains($name) -or -not $values[$name]) { $values[$name] = New-Secret }
  }
  $values['PROVISA_STORE_BIND_IP'] = '127.0.0.1'
  $values['PROVISA_MINIO_ROOT_USER'] = 'provisa'
  $values['CONFIG_DB_PASSWORD'] = $values['PROVISA_PG_PASSWORD']
  $values['PROVISA_OTEL_S3_ACCESS_KEY'] = 'provisa'
  $values['PROVISA_OTEL_S3_SECRET_KEY'] = $values['PROVISA_MINIO_ROOT_PASSWORD']
  New-Item -ItemType Directory -Force -Path $ProvisaHome | Out-Null
  Set-Content -Path $CredentialsPath -Encoding ascii -Value ($values.Keys | ForEach-Object { "$_=$($values[$_])" })
  # The user's own account alone may read it.
  & icacls.exe $CredentialsPath /inheritance:r /grant:r "$($env:USERNAME):(R,W)" | Out-Null
}

# Compose file list, mirroring scripts/provisa _build_compose_files (airgap overlay
# replaces build: with image:; extensions appended automatically).
function Compose-FileArgs {
  $files = @(
    "$ComposeGuest/docker-compose.core.yml"
    "$ComposeGuest/docker-compose.app.yml"
    "$ComposeGuest/docker-compose.airgap.yml"
  )
  if ($Obs)  { $files += "$ComposeGuest/docker-compose.observability.yml" }
  if ($Demo) { $files += "$ComposeGuest/docker-compose.demo.yml" }
  # Installed extensions live host-side under %USERPROFILE%\.provisa\extensions
  # (written by install-obs.ps1 / install-demo.ps1); reference them via /mnt.
  $extDir = Join-Path $ProvisaHome 'extensions'
  if (Test-Path $extDir) {
    Get-ChildItem -Path $extDir -Recurse -Filter 'docker-compose.*.yml' -ErrorAction SilentlyContinue |
      ForEach-Object { $files += (To-WslPath $_.FullName) }
  }
  $args = @()
  foreach ($f in $files) { $args += '-f'; $args += $f }
  return $args
}

function Invoke-Compose {
  param([string[]]$ComposeArgs)
  Require-Distro
  Ensure-Containerd
  Ensure-StoreCredentials
  $fileArgs = @('--env-file', (To-WslPath $CredentialsPath)) + (Compose-FileArgs)
  $inner = "cd $ComposeGuest && nerdctl compose " + (($fileArgs + $ComposeArgs) -join ' ')
  & wsl.exe -d $Distro -u root sh -c $inner
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

# -- Commands ------------------------------------------------------------------
function Start-Container {
  # Stop the native tier if it is running - the compose stack owns the ports now.
  $nativePid = Join-Path $ProvisaHome '.native.pid'
  if (Test-Path $nativePid) {
    foreach ($procId in (Get-Content $nativePid)) {
      if ($procId) { Stop-Process -Id ([int]$procId) -Force -ErrorAction SilentlyContinue }
    }
    Remove-Item $nativePid -Force -ErrorAction SilentlyContinue
  }
  Write-Info 'Starting Provisa services (container tier)...'
  Invoke-Compose @('up', '-d', '--remove-orphans')
  Write-Info "UI:  http://localhost:$UiPort"
  Write-Info "API: http://localhost:$ApiPort"
}

function Stop-Container {
  Write-Info 'Stopping Provisa services...'
  Invoke-Compose @('down')
}

function Restart-Container { Invoke-Compose @('restart') }
function Status-Container  { Invoke-Compose @('ps') }
function Logs-Container    { Invoke-Compose @('logs', '--follow') }

function Open-Container {
  # Open at ?tour=1 when the demo was installed so the guided tour auto-starts
  # (App.tsx reads the query param), mirroring the macOS launcher.
  $url = if ($Demo) { "http://localhost:$UiPort/?tour=1" } else { "http://localhost:$UiPort" }
  Write-Info "Opening $url"
  Start-Process $url
}

function Show-Help {
  Write-Host 'Usage: provisa-container <command>'
  Write-Host ''
  Write-Host 'Commands:'
  Write-Host '  start    Start the container-tier services (Trino + compute) in WSL2'
  Write-Host '  stop     Stop all services'
  Write-Host '  restart  Restart all services'
  Write-Host '  status   Show service status'
  Write-Host '  open     Open the UI in your browser'
  Write-Host '  logs     Follow service logs'
  Write-Host '  help     Show this help'
}

$command = if ($args.Count -gt 0) { $args[0] } else { 'help' }
switch ($command) {
  'start'   { Start-Container; Start-Sleep 2; Open-Container }
  'stop'    { Stop-Container }
  'restart' { Restart-Container }
  'status'  { Status-Container }
  'open'    { Open-Container }
  'logs'    { Logs-Container }
  'help'    { Show-Help }
  default   { Write-Err "Unknown command: $command"; Show-Help; exit 1 }
}
