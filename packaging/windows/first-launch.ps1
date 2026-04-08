# First-launch setup for Windows installer.
# Installs VirtualBox if needed, imports and starts the Provisa VM,
# loads bundled Docker images, and writes Provisa config.
#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ScriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$ImagesDir   = Join-Path $ScriptDir 'images'
$ComposeDir  = Join-Path $ScriptDir 'compose'
$OvaPath     = Join-Path $ScriptDir 'provisa-runtime.ova'
$VBoxSetup   = Join-Path $ScriptDir 'redist\VirtualBox-setup.exe'
$ProvisaHome = Join-Path $env:USERPROFILE '.provisa'
$Sentinel    = Join-Path $ProvisaHome '.first-launch-complete'
$VmName      = 'Provisa'

function Write-Info  { param($Msg) Write-Host "[provisa] $Msg" -ForegroundColor Cyan }
function Write-Ok    { param($Msg) Write-Host "[provisa] $Msg" -ForegroundColor Green }
function Write-Err   { param($Msg) Write-Host "[provisa] $Msg" -ForegroundColor Red }

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
  Write-Ok "RAM budget: $($script:BudgetGb)GB → Trino workers: $($script:TrinoWorkers)"
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

# ── Install VirtualBox if needed ─────────────────────────────────────────────
function Ensure-VirtualBox {
  $script:VBoxManage = Find-VBoxManage
  if ($script:VBoxManage) {
    Write-Ok "VirtualBox found: $($script:VBoxManage)"
    return
  }

  if (-not (Test-Path $VBoxSetup)) {
    Write-Err "VirtualBox installer not found at: $VBoxSetup"
    Write-Err 'Reinstall Provisa to restore bundled components.'
    exit 1
  }

  Write-Info 'Installing VirtualBox (~2 minutes)...'
  $proc = Start-Process -FilePath $VBoxSetup `
    -ArgumentList '--silent', '--ignore-reboot' `
    -Wait -PassThru
  if ($proc.ExitCode -ne 0) {
    Write-Err "VirtualBox installation failed (exit code $($proc.ExitCode))."
    exit 1
  }

  # Refresh PATH so VBoxManage is findable without rebooting
  $env:PATH = [System.Environment]::GetEnvironmentVariable('PATH', 'Machine') + ';' +
              [System.Environment]::GetEnvironmentVariable('PATH', 'User')

  $script:VBoxManage = Find-VBoxManage
  if (-not $script:VBoxManage) {
    Write-Err 'VBoxManage not found after installation. A reboot may be required.'
    exit 1
  }
  Write-Ok "VirtualBox installed: $($script:VBoxManage)"
}

# ── Import OVA if VM does not exist ──────────────────────────────────────────
function Import-Vm {
  $existing = & $script:VBoxManage list vms 2>&1
  if ($existing -match "`"$VmName`"") {
    Write-Info 'Provisa VM already imported.'
    return
  }

  if (-not (Test-Path $OvaPath)) {
    Write-Err "VM image not found: $OvaPath"
    Write-Err 'Reinstall Provisa to restore bundled components.'
    exit 1
  }

  Write-Info 'Importing Provisa VM...'
  & $script:VBoxManage import $OvaPath --vsys 0 --vmname $VmName
  if ($LASTEXITCODE -ne 0) { Write-Err 'VM import failed.'; exit 1 }

  # Allocate RAM budget to the VM
  $vmRamMb = $script:BudgetGb * 1024
  & $script:VBoxManage modifyvm $VmName --memory $vmRamMb
  Write-Info "VM RAM set to ${vmRamMb}MB."

  # Port forwarding: Docker API + all service ports
  Write-Info 'Configuring port forwarding...'
  $rules = @(
    'docker,tcp,127.0.0.1,2375,,2375',
    'postgres,tcp,127.0.0.1,5432,,5432',
    'pgbouncer,tcp,127.0.0.1,6432,,6432',
    'redis,tcp,127.0.0.1,6379,,6379',
    'minio-api,tcp,127.0.0.1,9000,,9000',
    'minio-console,tcp,127.0.0.1,9001,,9001',
    'trino,tcp,127.0.0.1,8080,,8080',
    'kafka,tcp,127.0.0.1,9092,,9092',
    'zaychik,tcp,127.0.0.1,8480,,8480',
    'elasticsearch,tcp,127.0.0.1,9200,,9200',
    'debezium,tcp,127.0.0.1,8083,,8083',
    'schema-registry,tcp,127.0.0.1,8081,,8081',
    'mongodb,tcp,127.0.0.1,27017,,27017',
    'provisa-ui,tcp,127.0.0.1,3000,,3000',
    'provisa-api,tcp,127.0.0.1,8000,,8000'
  )
  foreach ($rule in $rules) {
    & $script:VBoxManage modifyvm $VmName --natpf1 $rule | Out-Null
  }

  Write-Ok 'VM imported and configured.'
}

# ── Start VM and wait for Docker API ─────────────────────────────────────────
function Start-Vm {
  $info  = & $script:VBoxManage showvminfo $VmName --machinereadable 2>&1
  $state = ($info | Select-String 'VMState=').ToString() -replace '.*="(.*)".*', '$1'

  if ($state -eq 'running') {
    Write-Info 'Provisa VM is already running.'
  } else {
    Write-Info 'Starting Provisa VM (headless)...'
    & $script:VBoxManage startvm $VmName --type headless
    if ($LASTEXITCODE -ne 0) { Write-Err 'Failed to start Provisa VM.'; exit 1 }
  }

  $env:DOCKER_HOST = 'tcp://127.0.0.1:2375'
  Write-Info 'Waiting for Docker API (up to 2 minutes)...'
  $retries = 60
  while ($retries -gt 0) {
    $out = docker info 2>&1
    if ($LASTEXITCODE -eq 0) { break }
    Start-Sleep 2
    $retries--
  }
  if ($retries -eq 0) {
    Write-Err 'Docker API did not respond. Check the VM with VBoxManage showvminfo Provisa.'
    exit 1
  }
  Write-Ok 'VM started and Docker API ready.'
}

# ── Load images ────────────────────────────────────────────────────────────────
function Load-Images {
  Write-Info 'Loading bundled container images (no network required)...'
  $tars = Get-ChildItem -Path $ImagesDir -Filter '*.tar.gz' -ErrorAction Stop
  foreach ($tar in $tars) {
    Write-Info "  Loading: $($tar.Name)"
    # Decompress to a temp .tar, then load — docker load does not accept .gz on Windows
    $tmpTar = [System.IO.Path]::Combine([System.IO.Path]::GetTempPath(), [System.IO.Path]::GetFileNameWithoutExtension($tar.Name))
    try {
      $inStream  = [System.IO.File]::OpenRead($tar.FullName)
      $gzStream  = New-Object System.IO.Compression.GZipStream($inStream, [System.IO.Compression.CompressionMode]::Decompress)
      $outStream = [System.IO.File]::Create($tmpTar)
      $gzStream.CopyTo($outStream)
      $outStream.Close(); $gzStream.Close(); $inStream.Close()
      docker load -i $tmpTar
      if ($LASTEXITCODE -ne 0) { Write-Err "Failed to load image: $($tar.Name)"; exit 1 }
    } finally {
      Remove-Item -Path $tmpTar -Force -ErrorAction SilentlyContinue
    }
  }
  Write-Ok "Loaded $($tars.Count) images."
}

# ── Ask hostname ─────────────────────────────────────────────────────────────
function Ask-Hostname {
  $input = Read-Host 'Hostname for Provisa [localhost]'
  $input = $input.Trim()
  if ([string]::IsNullOrEmpty($input)) { return 'localhost' }
  return $input
}

# ── Ask UI port ───────────────────────────────────────────────────────────────
function Ask-UiPort {
  do {
    $input = Read-Host 'Web UI port [3000]'
    $input = $input.Trim()
    if ([string]::IsNullOrEmpty($input)) { $input = '3000' }
    $valid = $input -match '^\d+$' -and [int]$input -ge 1024 -and [int]$input -le 65535
    if (-not $valid) { Write-Host 'Invalid port. Enter a number between 1024 and 65535.' }
  } while (-not $valid)
  return [int]$input
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
runtime: virtualbox
vm_name: $VmName
docker_host: "tcp://127.0.0.1:2375"
federation_workers: $($script:TrinoWorkers)
"@
  Set-Content -Path $configPath -Value $content -Encoding UTF8
  Write-Ok "Config written to $configPath"
}

# ── Persist DOCKER_HOST in user environment ───────────────────────────────────
function Set-DockerHostEnv {
  [System.Environment]::SetEnvironmentVariable(
    'DOCKER_HOST', 'tcp://127.0.0.1:2375', 'User')
  $env:DOCKER_HOST = 'tcp://127.0.0.1:2375'
  Write-Ok 'DOCKER_HOST set in user environment.'
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
Ensure-VirtualBox
Import-Vm
Start-Vm
Load-Images
Write-Config
Set-DockerHostEnv

if (-not (Test-Path $ProvisaHome)) {
  New-Item -ItemType Directory -Path $ProvisaHome -Force | Out-Null
}
New-Item -ItemType File -Path $Sentinel -Force | Out-Null

Write-Ok 'First-launch setup complete.'
Write-Host ''
Write-Host 'Provisa is ready.' -ForegroundColor Green
Write-Host 'Run: provisa start' -ForegroundColor White
Write-Host ''
