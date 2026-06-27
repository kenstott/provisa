# Provisa Core Installer — runs from inside 7-Zip SFX extraction directory
#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$InstallDir = Join-Path $env:APPDATA 'Programs\Provisa'
$ExtractDir = $PSScriptRoot

function Write-Info { Write-Host "[provisa-install] $args" -ForegroundColor Cyan }
function Write-Ok   { Write-Host "[provisa-install] $args" -ForegroundColor Green }
function Write-Err  { Write-Host "[provisa-install] $args" -ForegroundColor Red }

Write-Info "Installing Provisa to $InstallDir ..."

# Copy payload to install dir
New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
robocopy $ExtractDir $InstallDir /E /NP /NFL /NDL | Out-Null
if ($LASTEXITCODE -ge 8) { throw "robocopy failed: $LASTEXITCODE" }

# Add/Remove Programs entry (per-user — no admin required)
$UninstallKey = 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall\Provisa'
New-Item -Path $UninstallKey -Force | Out-Null
Set-ItemProperty -Path $UninstallKey -Name 'DisplayName'      -Value "Provisa $env:PROVISA_VERSION"
Set-ItemProperty -Path $UninstallKey -Name 'UninstallString'  -Value "$InstallDir\uninstall.ps1"
Set-ItemProperty -Path $UninstallKey -Name 'DisplayVersion'   -Value "$env:PROVISA_VERSION"
Set-ItemProperty -Path $UninstallKey -Name 'Publisher'        -Value 'Provisa'

# Start Menu shortcut
$StartMenuDir = Join-Path $env:APPDATA 'Microsoft\Windows\Start Menu\Programs\Provisa'
New-Item -ItemType Directory -Path $StartMenuDir -Force | Out-Null
$WshShell = New-Object -ComObject WScript.Shell
$Shortcut = $WshShell.CreateShortcut("$StartMenuDir\Provisa First Launch.lnk")
$Shortcut.TargetPath    = 'powershell.exe'
$Shortcut.Arguments     = "-ExecutionPolicy Bypass -File `"$InstallDir\first-launch-gui.ps1`""
$Shortcut.WorkingDirectory = $InstallDir
$Shortcut.Save()

# Add $InstallDir to user PATH (HKCU — no admin required)
$CurrentPath = [Environment]::GetEnvironmentVariable('Path', 'User')
if ($CurrentPath -notlike "*$InstallDir*") {
    [Environment]::SetEnvironmentVariable('Path', "$CurrentPath;$InstallDir", 'User')
}

Write-Ok "Provisa installed to $InstallDir"
Write-Host ''
Write-Host 'Run "Provisa First Launch" from the Start Menu to complete setup.' -ForegroundColor Green
