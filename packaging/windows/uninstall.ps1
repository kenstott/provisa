# Provisa Uninstaller
#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# Removes the native runtime processes, user config + bundled runtime, install
# directory, Start Menu entries, and the Add/Remove Programs key.

$InstallDir   = Join-Path $env:APPDATA 'Programs\Provisa'
$ProvisaHome  = Join-Path $env:USERPROFILE '.provisa'
$PidFile      = Join-Path $ProvisaHome '.native.pid'
$StartMenuDir = Join-Path $env:APPDATA 'Microsoft\Windows\Start Menu\Programs\Provisa'

function Write-Info { param($Msg) Write-Host "[provisa-uninstall] $Msg" -ForegroundColor Cyan }
function Write-Ok   { param($Msg) Write-Host "[provisa-uninstall] $Msg" -ForegroundColor Green }
function Write-Warn { param($Msg) Write-Host "[provisa-uninstall] $Msg" -ForegroundColor Yellow }

# -- 1. Stop the native runtime processes -------------------------------------
if (Test-Path $PidFile) {
  Write-Info 'Stopping Provisa...'
  foreach ($procId in (Get-Content $PidFile)) {
    if ($procId) { Stop-Process -Id ([int]$procId) -Force -ErrorAction SilentlyContinue }
  }
  Write-Ok 'Provisa stopped.'
}

# -- 2. Tear down the container tier (WSL2 distro), if the upgrade was installed
$null = & wsl.exe --list --quiet 2>$null
if ($LASTEXITCODE -eq 0) {
  $distros = (& wsl.exe --list --quiet) -replace "`0", '' | ForEach-Object { $_.Trim() }
  if ($distros -contains 'provisa') {
    Write-Info 'Removing Provisa WSL2 distro...'
    & wsl.exe --unregister provisa 2>&1 | Out-Null
    Write-Ok 'WSL2 distro removed.'
  }
}
$ContainerDir = Join-Path $env:APPDATA 'Programs\Provisa-Container'
if (Test-Path $ContainerDir) { Remove-Item -Recurse -Force $ContainerDir; Write-Info "Removed $ContainerDir" }

# -- 3. Remove user config + bundled runtime ----------------------------------
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
