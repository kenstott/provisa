#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# Re-enable the Windows Hypervisor stack (Hyper-V / WSL2 / Virtual Machine
# Platform) so Docker Desktop can run. This is the inverse of the disable
# steps used to run the bundled VirtualBox runtime. Must run ELEVATED; a
# reboot is required to take effect.
#
#   .\enable-hyperv.ps1            # enable, then prompt to reboot
#   .\enable-hyperv.ps1 -Reboot    # enable and reboot immediately

param([switch]$Reboot)

# -- Require elevation --------------------------------------------------------
$admin = ([Security.Principal.WindowsPrincipal] `
  [Security.Principal.WindowsIdentity]::GetCurrent()
).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $admin) {
  Write-Host 'This script must run as Administrator. Re-launching elevated...' -ForegroundColor Yellow
  $argList = @('-NoProfile','-ExecutionPolicy','Bypass','-File',"`"$PSCommandPath`"")
  if ($Reboot) { $argList += '-Reboot' }
  Start-Process powershell.exe -ArgumentList $argList -Verb RunAs
  exit
}

Write-Host '=== Enabling Windows Hypervisor (Hyper-V / WSL2) ===' -ForegroundColor Cyan

# 1. Boot the hypervisor at startup.
& "$env:SystemRoot\System32\bcdedit.exe" /set hypervisorlaunchtype auto | Out-Null
Write-Host 'hypervisorlaunchtype = Auto'

# 2. Enable the platform features Docker Desktop depends on.
$features = @(
  'Microsoft-Hyper-V-All',
  'VirtualMachinePlatform',
  'Microsoft-Windows-Subsystem-Linux'
)
foreach ($f in $features) {
  Write-Host "Enabling $f ..."
  & dism.exe /Online /Enable-Feature /FeatureName:$f /All /NoRestart | Out-Null
  if ($LASTEXITCODE -ne 0 -and $LASTEXITCODE -ne 3010) {
    Write-Host "  WARNING: DISM returned $LASTEXITCODE for $f" -ForegroundColor Yellow
  }
}

Write-Host ''
Write-Host 'Done. A reboot is required before Docker Desktop / WSL2 will start.' -ForegroundColor Green
Write-Host 'Note: with Hyper-V on, the bundled VirtualBox runtime will run slowly.' -ForegroundColor Yellow
Write-Host 'Select the Docker runtime in Provisa Setup after rebooting.'

if ($Reboot) {
  Write-Host 'Rebooting in 10s... (Ctrl+C to cancel)'
  shutdown.exe /r /t 10 /c 'Provisa: rebooting to enable Hyper-V'
} else {
  $ans = Read-Host 'Reboot now? (y/N)'
  if ($ans -match '^(y|yes)$') { shutdown.exe /r /t 5 /c 'Provisa: rebooting to enable Hyper-V' }
}
