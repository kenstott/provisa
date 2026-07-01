#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Continue'

# Diagnose the Windows Hypervisor conflict that stalls the Provisa runtime.
# When Hyper-V / WSL2 / Virtual Machine Platform is active it claims VT-x,
# forcing the VirtualBox guest through a slow compat layer so dockerd never
# answers /_ping. Run in an elevated PowerShell.
#
#   .\diagnose-hyperv.ps1            # detection only
#   .\diagnose-hyperv.ps1 -BootTest  # also start the VM and poll /_ping

param(
  [switch]$BootTest,
  [int]$TimeoutSec = 120
)

Write-Host '=== Provisa Hyper-V conflict diagnostic ===' -ForegroundColor Cyan

# 1. Detection - mirrors Test-HyperVActive in first-launch-gui.ps1 ----------
$live = [bool](Get-Process -Name 'vmmem','vmmemWSL','vmmemProxy' -ErrorAction SilentlyContinue)
$bcd  = (& "$env:SystemRoot\System32\bcdedit.exe" /enum '{current}' 2>$null) -join ' '
$launch = if ($bcd -match 'hypervisorlaunchtype\s+(\w+)') { $Matches[1] } else { '(unset = Auto)' }
$active = $live -or ($launch -ne 'Off')

Write-Host "vmmem process running : $live"
Write-Host "hypervisorlaunchtype  : $launch"
if ($active) {
  Write-Host '=> Hyper-V ACTIVE - runtime will likely fail to boot.' -ForegroundColor Red
  Write-Host ''
  Write-Host 'Remedy (elevated PowerShell, then reboot):' -ForegroundColor Yellow
  Write-Host '    bcdedit /set hypervisorlaunchtype off'
  Write-Host '    dism.exe /Online /Disable-Feature:Microsoft-Hyper-V-All /NoRestart'
  Write-Host '    dism.exe /Online /Disable-Feature:VirtualMachinePlatform /NoRestart'
  Write-Host '    shutdown /r /t 0'
  Write-Host '    (re-enable later with: bcdedit /set hypervisorlaunchtype auto)'
} else {
  Write-Host '=> Hyper-V inactive - VirtualBox should get native VT-x.' -ForegroundColor Green
}

if (-not $BootTest) {
  Write-Host ''
  Write-Host 'Re-run with -BootTest to start the VM and confirm dockerd serves /_ping.'
  exit ($(if ($active) { 1 } else { 0 }))
}

# 2. Boot proof - start the VM and poll /_ping, exactly as the installer does
Write-Host ''
Write-Host '=== Boot test ===' -ForegroundColor Cyan
$vbm = $null
foreach ($regPath in @('HKLM:\SOFTWARE\Oracle\VirtualBox','HKLM:\SOFTWARE\WOW6432Node\Oracle\VirtualBox')) {
  $r = Get-ItemProperty $regPath -ErrorAction SilentlyContinue
  if ($r -and $r.InstallDir) {
    $c = Join-Path $r.InstallDir 'VBoxManage.exe'
    if (Test-Path $c) { $vbm = $c; break }
  }
}
if (-not $vbm) { Write-Host 'VBoxManage.exe not found - is the Federation Engine installed?' -ForegroundColor Red; exit 2 }

& $vbm startvm Provisa --type headless 2>&1 | Out-Null
$curl = Join-Path $env:SystemRoot 'System32\curl.exe'
$ready = $false
$elapsed = 0
while ($elapsed -lt $TimeoutSec) {
  $ping = & $curl --silent --max-time 5 'http://127.0.0.1:2375/_ping' 2>$null
  if ($LASTEXITCODE -eq 0 -and $ping -match 'OK') { $ready = $true; break }
  Start-Sleep 5
  $elapsed += 5
  Write-Host "  waiting... ${elapsed}s"
}
if ($ready) {
  Write-Host "READY after ${elapsed}s - runtime serves /_ping. Fix confirmed." -ForegroundColor Green
  exit 0
} else {
  Write-Host "NOT READY after ${TimeoutSec}s - dockerd never served /_ping." -ForegroundColor Red
  if ($active) { Write-Host 'Hyper-V is active - apply the remedy above and reboot.' -ForegroundColor Yellow }
  exit 1
}
