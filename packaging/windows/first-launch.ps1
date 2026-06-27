# First-launch setup for Windows installer.
# Ensures WSL2 + nerdctl are available, loads bundled container images,
# and writes Provisa config.
#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ScriptDir      = Split-Path -Parent (Resolve-Path $MyInvocation.MyCommand.Path)
$ImagesDir      = Join-Path $ScriptDir 'images'
$ComposeDir     = Join-Path $ScriptDir 'compose'
$SourceDir      = Join-Path $ScriptDir 'provisa-source'
$RedistDir      = Join-Path $ScriptDir 'redist'
$NerdctlBundle  = Join-Path $RedistDir 'nerdctl-full.tar.gz'
$ProvisaHome    = Join-Path $env:USERPROFILE '.provisa'
$Sentinel       = Join-Path $ProvisaHome '.first-launch-complete'

function Write-Info  { param($Msg) Write-Host "[provisa] $Msg" -ForegroundColor Cyan }
function Write-Ok    { param($Msg) Write-Host "[provisa] $Msg" -ForegroundColor Green }
function Write-Err   { param($Msg) Write-Host "[provisa] $Msg" -ForegroundColor Red }

function ConvertTo-WslPath {
  param([string]$WinPath)
  $drive = $WinPath[0].ToString().ToLower()
  $rest  = $WinPath.Substring(2) -replace '\\', '/'
  return "/mnt/$drive$rest"
}

# ── Derive Trino worker count from RAM budget ─────────────────────────────────
function Get-WorkersFromBudget {
  param([int]$Gb)
  if ($Gb -ge 96) { return 4 }
  if ($Gb -ge 48) { return 2 }
  if ($Gb -ge 24) { return 1 }
  return 0
}

# ── Ask RAM budget at first launch ────────────────────────────────────────────
function Ask-RamBudget {
  $totalBytes = (Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory
  $totalGb    = [int][Math]::Floor($totalBytes / 1GB)

  Write-Host ''
  Write-Host 'RAM Budget' -ForegroundColor White
  Write-Host "How much RAM should Provisa use? (host total: ${totalGb}GB)"
  Write-Host ''

  $options = @()
  foreach ($size in @(4, 8, 16, 32, 64, 128)) {
    if ($size -le $totalGb) { $options += "${size}GB" }
  }
  $options += "All (${totalGb}GB)"

  for ($i = 0; $i -lt $options.Count; $i++) {
    Write-Host "  [$($i+1)] $($options[$i])"
  }
  Write-Host ''

  do {
    $choice = Read-Host "Enter choice [1-$($options.Count)]"
    $valid  = $choice -match '^\d+$' -and [int]$choice -ge 1 -and [int]$choice -le $options.Count
    if (-not $valid) { Write-Host 'Invalid choice. Try again.' }
  } while (-not $valid)

  $selected = $options[[int]$choice - 1]
  $script:BudgetGb = if ($selected -like 'All*') { $totalGb } else { [int]($selected -replace 'GB', '') }
  $script:TrinoWorkers = Get-WorkersFromBudget -Gb $script:BudgetGb
  Write-Ok "RAM budget: $($script:BudgetGb)GB -> Trino workers: $($script:TrinoWorkers)"
}

# ── Ensure WSL2 is enabled and nerdctl-full is installed ─────────────────────
function Ensure-WSL2 {
  Write-Info 'Checking WSL2 status...'
  $wslStatus = wsl --status 2>&1
  $wslEnabled = ($LASTEXITCODE -eq 0) -and ($wslStatus -match 'Default Version: 2' -or $wslStatus -match 'WSL 2')

  if (-not $wslEnabled) {
    Write-Info 'WSL2 is not enabled. Elevation required to install WSL2.'
    Write-Host ''
    Write-Host 'Windows Subsystem for Linux 2 must be installed.' -ForegroundColor Yellow
    Write-Host 'A UAC prompt will appear. Accept it to continue.' -ForegroundColor Yellow
    Write-Host ''

    $proc = Start-Process -FilePath 'wsl.exe' `
      -ArgumentList '--install', '--no-distribution' `
      -Verb RunAs -Wait -PassThru
    if ($proc.ExitCode -ne 0) {
      Write-Err "WSL2 installation failed (exit code $($proc.ExitCode)). Reboot may be required."
      Write-Err 'After rebooting, re-run Provisa to complete setup.'
      exit 1
    }
    Write-Ok 'WSL2 installed. A reboot may be required before continuing.'
  } else {
    Write-Ok 'WSL2 is enabled.'
  }

  # Verify WSL can execute commands
  $wslTest = wsl echo ok 2>&1
  if ($LASTEXITCODE -ne 0 -or $wslTest -notmatch 'ok') {
    Write-Err 'WSL2 is installed but not responding. A reboot may be required.'
    exit 1
  }

  # Install nerdctl-full if not already present
  $nerdctlCheck = wsl sh -c 'command -v nerdctl' 2>&1
  if ($LASTEXITCODE -eq 0 -and $nerdctlCheck -match 'nerdctl') {
    Write-Ok "nerdctl already installed: $nerdctlCheck"
    return
  }

  if (-not (Test-Path $NerdctlBundle)) {
    Write-Err "nerdctl-full bundle not found at: $NerdctlBundle"
    Write-Err 'Reinstall Provisa to restore bundled components.'
    exit 1
  }

  Write-Info 'Installing nerdctl-full into WSL2...'

  # Convert Windows path to WSL path for the bundle
  $wslBundle = ConvertTo-WslPath $NerdctlBundle

  # Extract nerdctl-full into /usr/local (requires root in WSL)
  wsl sh -c "sudo tar -C /usr/local -xzf '$wslBundle'"
  if ($LASTEXITCODE -ne 0) {
    Write-Err 'Failed to install nerdctl-full into WSL2.'
    exit 1
  }

  # Start containerd if not running
  wsl sh -c 'sudo nohup containerd > /dev/null 2>&1 &'

  $nerdctlVerify = wsl nerdctl version 2>&1
  if ($LASTEXITCODE -ne 0) {
    Write-Err "nerdctl not functional after installation: $nerdctlVerify"
    exit 1
  }
  Write-Ok 'nerdctl-full installed and containerd running.'
}

# ── Load images ────────────────────────────────────────────────────────────────
function Load-Images {
  Write-Info 'Loading bundled container images (no network required)...'
  $tars = Get-ChildItem -Path $ImagesDir -Filter '*.tar.gz' -ErrorAction Stop
  foreach ($tar in $tars) {
    Write-Info "  Loading: $($tar.Name)"
    $wslTarPath = ConvertTo-WslPath $tar.FullName
    wsl nerdctl load -i $wslTarPath
    if ($LASTEXITCODE -ne 0) { Write-Err "Failed to load image: $($tar.Name)"; exit 1 }
  }
  Write-Ok "Loaded $($tars.Count) images."
}

# ── Build provisa image from bundled source ───────────────────────────────────
function Build-ProvisaImage {
  Write-Info 'Building provisa/provisa:local from bundled source...'
  if (-not (Test-Path $SourceDir)) {
    Write-Err "provisa-source not found at $SourceDir. Reinstall Provisa."
    exit 1
  }
  $wslSourceDir = ConvertTo-WslPath $SourceDir
  wsl nerdctl build -t provisa/provisa:local $wslSourceDir
  if ($LASTEXITCODE -ne 0) { Write-Err 'Failed to build provisa image.'; exit 1 }
  Write-Ok 'provisa/provisa:local built.'
}

# ── Discover or download extension images (obs / demo) ───────────────────────
function Get-ExtensionImages {
  param(
    [string]$Label,      # e.g. "Observability"
    [string]$Filename,   # e.g. "provisa-obs-images-v0.1.0-alpha.115.tar.gz"
    [string]$DestDir     # directory to extract into
  )

  if ((Test-Path $DestDir) -and (Get-ChildItem $DestDir -ErrorAction SilentlyContinue)) {
    Write-Info "$Label images already present - skipping."
    return
  }

  New-Item -ItemType Directory -Path $DestDir -Force | Out-Null

  # Discovery order: Downloads folder, script dir, any mounted volumes
  $src = $null
  $downloads = Join-Path $env:USERPROFILE 'Downloads'
  if (Test-Path (Join-Path $downloads $Filename)) { $src = Join-Path $downloads $Filename }
  if (-not $src) {
    $scriptSibling = Join-Path $ScriptDir $Filename
    if (Test-Path $scriptSibling) { $src = $scriptSibling }
  }
  if (-not $src) {
    foreach ($drive in [System.IO.DriveInfo]::GetDrives() | Where-Object { $_.DriveType -eq 'CDRom' -or $_.DriveType -eq 'Removable' }) {
      $candidate = Join-Path $drive.RootDirectory.FullName $Filename
      if (Test-Path $candidate) { $src = $candidate; break }
    }
  }

  if ($src) {
    Write-Info "Extracting $Label images from $src..."
    tar -xzf $src -C $DestDir
    if ($LASTEXITCODE -ne 0) { Write-Err "Extraction failed for $Label images."; Remove-Item $DestDir -Recurse -Force; return }
    Write-Ok "$Label images extracted."
    return
  }

  # Offer GitHub download
  $version = $env:PROVISA_VERSION
  if (-not $version) {
    try { $version = (& provisa version 2>$null | Select-Object -First 1).Split()[-1] } catch {}
  }

  $downloadUrl = $null
  if ($version) {
    $downloadUrl = "https://github.com/kenstott/provisa/releases/download/${version}/${Filename}"
  }

  $nonInteractive = $env:PROVISA_NONINTERACTIVE
  $answer = 'n'
  if ($nonInteractive) {
    $obsVal  = if ($env:PROVISA_INSTALL_OBS)  { $env:PROVISA_INSTALL_OBS }  else { 'n' }
    $demoVal = if ($env:PROVISA_INSTALL_DEMO) { $env:PROVISA_INSTALL_DEMO } else { 'n' }
    $answer  = if ($Label -like 'Demo*') { $demoVal } else { $obsVal }
  } elseif ($downloadUrl) {
    Write-Host ''
    Write-Host "$Label Extension" -ForegroundColor White
    Write-Host "No local $Label images found."
    $answer = Read-Host "Download now from GitHub? (~1-2 GB) [y/N]"
    $answer = $answer.Trim().ToLower()
  } else {
    Write-Info "No local $Label images found. Place $Filename in Downloads and re-run setup to install later."
    Remove-Item $DestDir -Recurse -Force -ErrorAction SilentlyContinue
    return
  }

  if ($answer -eq 'y' -and $downloadUrl) {
    Write-Info "Downloading $Filename..."
    $tmpFile = Join-Path $ProvisaHome $Filename
    try {
      Invoke-WebRequest -Uri $downloadUrl -OutFile $tmpFile -UseBasicParsing
      tar -xzf $tmpFile -C $DestDir
      if ($LASTEXITCODE -ne 0) { throw "tar extraction failed" }
      Remove-Item $tmpFile -Force
      Write-Ok "$Label images downloaded and extracted."
    } catch {
      Write-Err "Download failed: $_. Place $Filename in Downloads and re-run setup."
      Remove-Item $DestDir -Recurse -Force -ErrorAction SilentlyContinue
    }
  } else {
    Write-Info "Skipping $Label extension. Install later by placing $Filename in Downloads and running 'provisa install-$($Label.ToLower())'."
    Remove-Item $DestDir -Recurse -Force -ErrorAction SilentlyContinue
  }
}

# ── Install optional obs/demo extensions ─────────────────────────────────────
function Install-Extensions {
  $version = $env:PROVISA_VERSION
  if (-not $version) {
    try { $version = (& provisa version 2>$null | Select-Object -First 1).Split()[-1] } catch {}
  }
  $obsFile  = "provisa-obs-images-${version}.tar.gz"
  $demoFile = "provisa-demo-images-${version}.tar.gz"

  Get-ExtensionImages -Label 'Observability' -Filename $obsFile  -DestDir (Join-Path $ProvisaHome 'obs-images')
  Get-ExtensionImages -Label 'Demo'          -Filename $demoFile -DestDir (Join-Path $ProvisaHome 'demo-images')
}

# ── Ask hostname ─────────────────────────────────────────────────────────────
function Ask-Hostname {
  $inputVal = Read-Host 'Hostname for Provisa [localhost]'
  $inputVal = $inputVal.Trim()
  if ([string]::IsNullOrEmpty($inputVal)) { return 'localhost' }
  return $inputVal
}

# ── Ask UI port ───────────────────────────────────────────────────────────────
function Ask-UiPort {
  do {
    $inputVal = Read-Host 'Web UI port [3000]'
    $inputVal = $inputVal.Trim()
    if ([string]::IsNullOrEmpty($inputVal)) { $inputVal = '3000' }
    $valid = $inputVal -match '^\d+$' -and [int]$inputVal -ge 1024 -and [int]$inputVal -le 65535
    if (-not $valid) { Write-Host 'Invalid port. Enter a number between 1024 and 65535.' }
  } while (-not $valid)
  return [int]$inputVal
}

# ── Write config ───────────────────────────────────────────────────────────────
function Write-Config {
  if (-not (Test-Path $ProvisaHome)) {
    New-Item -ItemType Directory -Path $ProvisaHome -Force | Out-Null
  }
  $configPath = Join-Path $ProvisaHome 'config.yaml'
  if (Test-Path $configPath) { return }

  $hostname  = Ask-Hostname
  $uiPort    = Ask-UiPort
  $apiPort   = $uiPort + 1
  Write-Info "Hostname: $hostname  |  UI port: $uiPort  |  API port: $apiPort"

  $composeDirFwd = $ComposeDir -replace '\\', '/'
  $content = @"
# Provisa configuration — generated by Windows installer
project_dir: "$composeDirFwd"
hostname: $hostname
ui_port: $uiPort
api_port: $apiPort
auto_open_browser: true
runtime: wsl2-nerdctl
federation_workers: $($script:TrinoWorkers)
"@
  Set-Content -Path $configPath -Value $content -Encoding UTF8
  Write-Ok "Config written to $configPath"
}

# ── Main ───────────────────────────────────────────────────────────────────────
$script:BudgetGb     = 8
$script:TrinoWorkers = 0

Write-Host ''
Write-Host 'Provisa — First Launch Setup' -ForegroundColor White
Write-Host '===========================================' -ForegroundColor White
Write-Host ''
Write-Info 'Setting up Provisa (no internet required)...'

Ask-RamBudget
Ensure-WSL2
Load-Images
Build-ProvisaImage
Install-Extensions
Write-Config

if (-not (Test-Path $ProvisaHome)) {
  New-Item -ItemType Directory -Path $ProvisaHome -Force | Out-Null
}
New-Item -ItemType File -Path $Sentinel -Force | Out-Null

Write-Ok 'First-launch setup complete.'
Write-Host ''
Write-Host 'Provisa is ready.' -ForegroundColor Green
Write-Host 'Run: provisa start' -ForegroundColor White
Write-Host ''
