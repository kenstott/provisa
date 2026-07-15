# Provisa container-tier setup for Windows (REQ-889 / REQ-633). Provisions a WSL2
# distro with containerd + nerdctl, loads the bundled core images, copies the
# compose tree in, and starts the stack. The Windows equivalent of the macOS
# Lima tier — no VirtualBox. Additive/reversible: the native base tier is left
# intact and can be switched back to with `provisa-native.ps1`.
#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ScriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProvisaHome = Join-Path $env:USERPROFILE '.provisa'
$ConfigPath  = Join-Path $ProvisaHome 'config.yaml'
$Distro      = 'provisa'
$DistroDir   = Join-Path $ProvisaHome 'wsl\provisa'

$RootfsTar   = Join-Path $ScriptDir 'rootfs.tar.gz'
$NerdctlTar  = Get-ChildItem -Path $ScriptDir -Filter 'nerdctl-full-*.tar.gz' | Select-Object -First 1
$ImagesDir   = Join-Path $ScriptDir 'images'
$ComposeSrc  = Join-Path $ScriptDir 'compose'
$WslSrc      = Join-Path $ScriptDir 'wsl'
$VersionFile = Join-Path $ScriptDir 'VERSION'
$Version     = if (Test-Path $VersionFile) { (Get-Content $VersionFile -Raw).Trim() } else { 'dev' }

function Write-Info { param($Msg) Write-Host "[provisa-container] $Msg" -ForegroundColor Cyan }
function Write-Err  { param($Msg) Write-Host "[provisa-container] $Msg" -ForegroundColor Red }
function Write-Ok   { param($Msg) Write-Host "[provisa-container] $Msg" -ForegroundColor Green }

# Windows path → WSL /mnt path (C:\a\b → /mnt/c/a/b).
function To-WslPath {
  param([string]$WinPath)
  $drive = $WinPath[0].ToString().ToLower()
  $rest  = $WinPath.Substring(2) -replace '\\', '/'
  return "/mnt/$drive$rest"
}

# ── 1. Ensure WSL2 ────────────────────────────────────────────────────────────
function Ensure-Wsl2 {
  $null = & wsl.exe --status 2>$null
  if ($LASTEXITCODE -ne 0) {
    Write-Info 'Enabling WSL2 (no distribution)...'
    & wsl.exe --install --no-distribution
    if ($LASTEXITCODE -ne 0) {
      Write-Err 'WSL2 could not be enabled automatically. Enable "Virtual Machine Platform" + "Windows Subsystem for Linux" in Windows Features, reboot, then re-run.'
      exit 1
    }
  }
  & wsl.exe --set-default-version 2 | Out-Null
}

# ── 2. Import the Provisa distro ──────────────────────────────────────────────
function Import-Distro {
  $existing = (& wsl.exe --list --quiet) -replace "`0", '' | ForEach-Object { $_.Trim() }
  if ($existing -contains $Distro) { Write-Info "WSL distro '$Distro' already present."; return }
  if (-not (Test-Path $RootfsTar)) { Write-Err "rootfs.tar.gz not found at $RootfsTar."; exit 1 }
  New-Item -ItemType Directory -Path $DistroDir -Force | Out-Null
  Write-Info "Importing WSL distro '$Distro'..."
  & wsl.exe --import $Distro $DistroDir $RootfsTar --version 2
  if ($LASTEXITCODE -ne 0) { Write-Err 'wsl --import failed.'; exit 1 }
  Write-Ok 'Distro imported.'
}

# ── 3. Provision containerd + nerdctl ─────────────────────────────────────────
function Provision-Containerd {
  if (-not $NerdctlTar) { Write-Err 'nerdctl-full archive not found beside installer.'; exit 1 }
  # Copy the wsl helper scripts into the distro.
  $wslGuestSetup = "mkdir -p /opt/provisa/wsl && cp -r '$(To-WslPath $WslSrc)/.' /opt/provisa/wsl/ && chmod +x /opt/provisa/wsl/*.sh"
  & wsl.exe -d $Distro -u root sh -c $wslGuestSetup
  if ($LASTEXITCODE -ne 0) { Write-Err 'Failed to stage WSL scripts.'; exit 1 }
  Write-Info 'Installing containerd + nerdctl inside WSL...'
  & wsl.exe -d $Distro -u root sh /opt/provisa/wsl/provision-containerd.sh (To-WslPath $NerdctlTar.FullName)
  if ($LASTEXITCODE -ne 0) { Write-Err 'containerd provisioning failed.'; exit 1 }
  & wsl.exe -d $Distro -u root sh /opt/provisa/wsl/start-containerd.sh
  if ($LASTEXITCODE -ne 0) { Write-Err 'containerd failed to start.'; exit 1 }
  Write-Ok 'containerd + nerdctl ready.'
}

# ── 4. Obtain + load core images ──────────────────────────────────────────────
# Images are NOT bundled in the installer (they exceed GitHub's 2 GB asset limit).
# Prefer a local images/ dir (airgap: user extracted the core-images zip beside the
# installer); otherwise download provisa-core-images-amd64-<VERSION>.zip from the
# matching GitHub release and extract it.
function Resolve-ImagesDir {
  if (Test-Path (Join-Path $ImagesDir '*.tar.gz')) { return $ImagesDir }
  $localZip = Get-ChildItem -Path $ScriptDir -Filter 'provisa-core-images-amd64-*.zip' -ErrorAction SilentlyContinue | Select-Object -First 1
  $dl = Join-Path $ProvisaHome 'container-images'
  New-Item -ItemType Directory -Path $dl -Force | Out-Null
  if (-not $localZip) {
    $zipName = "provisa-core-images-amd64-$Version.zip"
    $url = "https://github.com/kenstott/provisa/releases/download/$Version/$zipName"
    $localZip = Join-Path $dl $zipName
    Write-Info "Downloading core images: $url"
    Invoke-WebRequest -Uri $url -OutFile $localZip -UseBasicParsing
  }
  Write-Info 'Extracting core images...'
  Expand-Archive -Path $localZip -DestinationPath $dl -Force
  return $dl
}

function Load-Images {
  $dir = Resolve-ImagesDir
  $tars = Get-ChildItem -Path $dir -Filter '*.tar.gz' -ErrorAction SilentlyContinue
  if (-not $tars) { Write-Err "No image tarballs found in $dir."; exit 1 }
  foreach ($t in $tars) {
    Write-Info "Loading image: $($t.Name)"
    & wsl.exe -d $Distro -u root nerdctl load -i (To-WslPath $t.FullName)
    if ($LASTEXITCODE -ne 0) { Write-Err "Failed to load $($t.Name)."; exit 1 }
  }
  Write-Ok 'Core images loaded.'
}

# ── 5. Copy the compose tree into the distro ──────────────────────────────────
function Stage-Compose {
  $cp = "mkdir -p /opt/provisa/compose && cp -r '$(To-WslPath $ComposeSrc)/.' /opt/provisa/compose/"
  & wsl.exe -d $Distro -u root sh -c $cp
  if ($LASTEXITCODE -ne 0) { Write-Err 'Failed to stage compose tree.'; exit 1 }
  Write-Ok 'Compose tree staged.'
}

# ── 6. Write / update config (runtime=container) ──────────────────────────────
function Write-ContainerConfig {
  $uiPort  = if ($env:PROVISA_UI_PORT)  { $env:PROVISA_UI_PORT }  else { '3000' }
  $apiPort = if ($env:PROVISA_API_PORT) { $env:PROVISA_API_PORT } else { '8000' }
  New-Item -ItemType Directory -Path $ProvisaHome -Force | Out-Null
  # Preserve any keys already present; rewrite the runtime-relevant ones.
  $keep = @()
  if (Test-Path $ConfigPath) {
    $keep = Get-Content $ConfigPath | Where-Object {
      $_ -notmatch '^\s*(runtime|ui_port|api_port|project_dir|obs|demo)\s*:'
    }
  }
  $keep += 'runtime: container'
  $keep += "ui_port: $uiPort"
  $keep += "api_port: $apiPort"
  $keep += "project_dir: `"$(Join-Path $ProvisaHome 'compose')`""
  # Add-on selection (parity with macOS wizard). Non-interactive reads env; the
  # container tier runs Trino, so the engine is fixed and only obs/demo are asked.
  $keep += 'engine: trino'
  if ($env:PROVISA_NONINTERACTIVE) {
    $obsFlag  = if ($env:PROVISA_OBS -or $env:PROVISA_OBS_MODE -eq 'docker') { 'true' } else { 'false' }
    $demoFlag = if ($env:PROVISA_INSTALL_DEMO -match '^(y|Y|true)') { 'true' } else { 'false' }
  } else {
    $obsFlag  = if ((Read-Host 'Install bundled Grafana/Prometheus observability (y/N)') -match '^(y|Y)') { 'true' } else { 'false' }
    Write-Host 'The demo is a complete, fully functional install — pick it with confidence; nothing is limited.' -ForegroundColor DarkGray
    Write-Host 'To reconfigure with other options later, just run this installer again.' -ForegroundColor DarkGray
    $demoFlag = if ((Read-Host 'Install the demo dataset with guided tour (y/N)') -match '^(y|Y)') { 'true' } else { 'false' }
  }
  $keep += "obs: $obsFlag"
  $keep += "demo: $demoFlag"
  $keep | Set-Content -Path $ConfigPath -Encoding UTF8
  # Compose reads relative bind-mount sources; also keep a host copy for parity.
  if (-not (Test-Path (Join-Path $ProvisaHome 'compose'))) {
    Copy-Item -Path $ComposeSrc -Destination (Join-Path $ProvisaHome 'compose') -Recurse -Force
  }
  Write-Ok "Config written to $ConfigPath"
}

# ── Main ──────────────────────────────────────────────────────────────────────
Write-Host ''
Write-Host 'Provisa — Container Tier Setup (WSL2 + containerd)' -ForegroundColor White
Write-Host '════════════════════════════════════════════════════'
Write-Host ''
if (-not (Test-Path $ConfigPath)) {
  Write-Err 'Provisa Core (native tier) is not installed. Run Provisa-Setup.exe first.'
  exit 1
}

Ensure-Wsl2
Import-Distro
Provision-Containerd
Load-Images
Stage-Compose
Write-ContainerConfig

Write-Ok 'Container tier installed.'
Write-Host ''
Write-Info 'Starting the container stack...'
& powershell.exe -ExecutionPolicy Bypass -File (Join-Path $ScriptDir 'provisa-container.ps1') start
