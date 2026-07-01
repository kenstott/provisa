# Provisa Uninstaller
#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# Removes the runtime (VirtualBox VM or Docker containers/images), user config,
# install directory, Start Menu entries, and the Add/Remove Programs key.
#
#   .\uninstall.ps1              # remove everything except loaded Docker images
#   .\uninstall.ps1 -PruneImages # also remove images loaded into Docker Desktop

param([switch]$PruneImages)

$InstallDir   = Join-Path $env:APPDATA 'Programs\Provisa'
$ProvisaHome  = Join-Path $env:USERPROFILE '.provisa'
$ConfigPath   = Join-Path $ProvisaHome 'config.yaml'
$StartMenuDir = Join-Path $env:APPDATA 'Microsoft\Windows\Start Menu\Programs\Provisa'

function Write-Info { param($Msg) Write-Host "[provisa-uninstall] $Msg" -ForegroundColor Cyan }
function Write-Ok   { param($Msg) Write-Host "[provisa-uninstall] $Msg" -ForegroundColor Green }
function Write-Warn { param($Msg) Write-Host "[provisa-uninstall] $Msg" -ForegroundColor Yellow }

# -- Parse the runtime out of config.yaml (best effort) -----------------------
$runtime    = $null
$vmName     = 'Provisa'
$dockerHost = $null
$projectDir = $null
if (Test-Path $ConfigPath) {
  foreach ($line in Get-Content $ConfigPath) {
    if ($line -match '^\s*runtime\s*:\s*"?([^"]+)"?\s*$')      { $runtime    = $Matches[1].Trim() }
    if ($line -match '^\s*vm_name\s*:\s*"?([^"]+)"?\s*$')      { $vmName     = $Matches[1].Trim() }
    if ($line -match '^\s*docker_host\s*:\s*"?([^"]+)"?\s*$')  { $dockerHost = $Matches[1].Trim() }
    if ($line -match '^\s*project_dir\s*:\s*"?([^"]+)"?\s*$')  { $projectDir = $Matches[1].Trim() }
  }
}

function Find-VBoxManage {
  $cmd = Get-Command VBoxManage -ErrorAction SilentlyContinue
  if ($cmd) { return $cmd.Source }
  foreach ($p in @(
    "$env:ProgramFiles\Oracle\VirtualBox\VBoxManage.exe",
    "${env:ProgramFiles(x86)}\Oracle\VirtualBox\VBoxManage.exe"
  )) { if (Test-Path $p) { return $p } }
  return $null
}

# -- 1. Tear down the runtime -------------------------------------------------
if ($runtime -eq 'virtualbox' -or (-not $runtime)) {
  # Default to the VM path when the runtime is unknown; harmless if absent.
  $vbox = Find-VBoxManage
  if ($vbox) {
    & $vbox showvminfo $vmName --machinereadable 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) {
      Write-Info "Removing runtime VM '$vmName'..."
      & $vbox controlvm $vmName poweroff 2>&1 | Out-Null
      Start-Sleep 2
      & $vbox unregistervm $vmName --delete 2>&1 | Out-Null
      Write-Ok 'Runtime VM removed.'
    }
  } else {
    Write-Warn 'VBoxManage not found - skipping VM cleanup.'
  }
}

if ($runtime -eq 'docker') {
  if (Get-Command docker -ErrorAction SilentlyContinue) {
    if ($dockerHost) { $env:DOCKER_HOST = $dockerHost }
    if ($projectDir) {
      Write-Info 'Stopping Docker services...'
      $c1 = Join-Path $projectDir 'docker-compose.core.yml'
      $c2 = Join-Path $projectDir 'docker-compose.app.yml'
      $c3 = Join-Path $projectDir 'docker-compose.airgap.yml'
      $downArgs = @('compose', '-f', $c1, '-f', $c2, '-f', $c3, 'down')
      if ($PruneImages) { $downArgs += '--rmi'; $downArgs += 'local' }
      & docker @downArgs 2>$null
      Write-Ok 'Docker services stopped.'
    }
  } else {
    Write-Warn 'Docker CLI not found - skipping container cleanup.'
  }
}

# -- 2. Remove user config (sentinel + config.yaml) ---------------------------
if (Test-Path $ProvisaHome) {
  Remove-Item -Recurse -Force $ProvisaHome
  Write-Info "Removed $ProvisaHome"
}

# -- 3. Remove install dir, Start Menu, ARP key -------------------------------
if (Test-Path $InstallDir)   { Remove-Item -Recurse -Force $InstallDir;   Write-Info "Removed $InstallDir" }
if (Test-Path $StartMenuDir) { Remove-Item -Recurse -Force $StartMenuDir; Write-Info 'Removed Start Menu entries' }
Remove-Item -Path 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall\Provisa' `
  -Force -ErrorAction SilentlyContinue

Write-Host ''
Write-Ok 'Provisa uninstalled.'
Write-Warn 'VirtualBox and Docker Desktop were left installed (may be used by other apps).'
