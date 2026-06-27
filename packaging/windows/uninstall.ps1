# Provisa Core Uninstaller
#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$InstallDir = Join-Path $env:APPDATA 'Programs\Provisa'

# Stop Provisa services running in WSL
try { wsl sh -c 'nerdctl compose down 2>/dev/null' 2>$null } catch {}

# Remove files
if (Test-Path $InstallDir) { Remove-Item -Recurse -Force $InstallDir }

# Remove Start Menu
$StartMenuDir = Join-Path $env:APPDATA 'Microsoft\Windows\Start Menu\Programs\Provisa'
if (Test-Path $StartMenuDir) { Remove-Item -Recurse -Force $StartMenuDir }

# Remove registry entry
Remove-Item -Path 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall\Provisa' -Force -ErrorAction SilentlyContinue

Write-Host 'Provisa uninstalled.' -ForegroundColor Green
