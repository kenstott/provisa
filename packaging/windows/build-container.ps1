# Build the Provisa Windows container-tier installer (REQ-889 / REQ-633) with Inno
# Setup. This is the separate, on-demand upgrade that adds the compute stack
# (Trino + services) via WSL2 + containerd — the Windows equivalent of the macOS
# Lima tier. NO VirtualBox. The base native installer (build-sfx.ps1) ships
# without any of this; users download and run this only to upgrade tiers.
#
# CI stages the core image tarballs into packaging/windows/images before running.
#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot  = Split-Path -Parent (Split-Path -Parent $ScriptDir)

# Pins (overridable). nerdctl-full version must match what the macOS Lima tier
# provisions so both tiers run the same containerd/nerdctl.
$NerdctlVersion = if ($env:NERDCTL_VERSION) { $env:NERDCTL_VERSION } else { '2.2.2' }
$RootfsUrl = if ($env:ROOTFS_URL) { $env:ROOTFS_URL } else {
  'https://cloud-images.ubuntu.com/wsl/jammy/current/ubuntu-jammy-wsl-amd64-ubuntu22.04lts.rootfs.tar.gz'
}

Write-Host '[build-container] Preparing build directory...' -ForegroundColor Cyan
$BuildDir = Join-Path $ScriptDir 'build-container'
if (Test-Path $BuildDir) { Remove-Item $BuildDir -Recurse -Force }
New-Item -ItemType Directory -Path $BuildDir -Force | Out-Null

# ── Compose tree (mirrors the macOS container tier's embed_compose) ───────────
$BuildCompose = Join-Path $BuildDir 'compose'
New-Item -ItemType Directory -Path $BuildCompose -Force | Out-Null
Copy-Item (Join-Path $RepoRoot 'docker-compose.core.yml')   $BuildCompose
Copy-Item (Join-Path $RepoRoot 'docker-compose.app.yml')    $BuildCompose
Copy-Item (Join-Path $RepoRoot 'docker-compose.airgap.yml') $BuildCompose
Copy-Item (Join-Path $RepoRoot 'docker-compose.observability.yml') $BuildCompose
Copy-Item (Join-Path $RepoRoot 'config') (Join-Path $BuildCompose 'config') -Recurse -Force
Copy-Item (Join-Path $RepoRoot 'db')     (Join-Path $BuildCompose 'db')     -Recurse -Force
Copy-Item (Join-Path $RepoRoot 'observability') (Join-Path $BuildCompose 'observability') -Recurse -Force

# Demo overlay: rewrite build: contexts to prebuilt images (no build context on
# the user's machine).
$DemoComposeSrc = Join-Path $RepoRoot 'docker-compose.demo.yml'
if (Test-Path $DemoComposeSrc) {
  ((Get-Content $DemoComposeSrc -Raw) `
    -replace 'build:\s*\./demo/graphql_server',  'image: provisa/graphql-demo:local' `
    -replace 'build:\s*\./demo/petstore_server', 'image: provisa/petstore-demo:local') |
    Set-Content -Path (Join-Path $BuildCompose 'docker-compose.demo.yml') -Encoding UTF8
}

# Trino config WITHOUT plugins (plugins are a separate download). Drop dev-only
# catalogs whose services no compose defines so the coordinator does not wedge.
$TrinoDst = Join-Path $BuildCompose 'trino'
New-Item -ItemType Directory -Path $TrinoDst -Force | Out-Null
Get-ChildItem -Path (Join-Path $RepoRoot 'trino') -Exclude 'plugins' | Copy-Item -Destination $TrinoDst -Recurse -Force
foreach ($stale in 'mongodb', 'support_kafka', 'reviews_mongo') {
  Remove-Item (Join-Path $TrinoDst "catalog\$stale.properties") -Force -ErrorAction SilentlyContinue
}

$DemoFilesSrc = Join-Path $RepoRoot 'demo\files'
if (Test-Path $DemoFilesSrc) {
  $DemoDst = Join-Path $BuildCompose 'demo\files'
  New-Item -ItemType Directory -Path $DemoDst -Force | Out-Null
  Copy-Item -Path (Join-Path $DemoFilesSrc '*') -Destination $DemoDst -Recurse -Force
}

# ── WSL guest scripts + tier scripts ──────────────────────────────────────────
Copy-Item (Join-Path $ScriptDir 'wsl') (Join-Path $BuildDir 'wsl') -Recurse -Force
Copy-Item (Join-Path $ScriptDir 'install-container.ps1')  $BuildDir
Copy-Item (Join-Path $ScriptDir 'provisa-container.ps1')  $BuildDir
Copy-Item (Join-Path $ScriptDir 'provisa.ico')            $BuildDir

# ── Core images: fetched on demand, NOT bundled ──────────────────────────────
# The ~1.9 GB core image tarballs would push this installer past GitHub's 2 GB
# release-asset limit. Per the "separate optional download" design, install-container.ps1
# fetches provisa-core-images-amd64-<VERSION>.zip from the release at install time
# (or uses a local images/ dir dropped beside the installer for airgap). We only
# stamp the version so the installer knows which release to pull from.
$Version = if ($env:VERSION) { $env:VERSION } else { 'dev' }
[System.IO.File]::WriteAllText((Join-Path $BuildDir 'VERSION'), $Version, [System.Text.Encoding]::ASCII)

# ── nerdctl-full archive (linux-amd64) ────────────────────────────────────────
$NerdctlArchive = "nerdctl-full-$NerdctlVersion-linux-amd64.tar.gz"
$NerdctlUrl = "https://github.com/containerd/nerdctl/releases/download/v$NerdctlVersion/$NerdctlArchive"
Write-Host "[build-container] Downloading $NerdctlArchive..." -ForegroundColor Cyan
Invoke-WebRequest -Uri $NerdctlUrl -OutFile (Join-Path $BuildDir $NerdctlArchive) -UseBasicParsing

# ── WSL base rootfs ───────────────────────────────────────────────────────────
Write-Host '[build-container] Downloading WSL base rootfs...' -ForegroundColor Cyan
Invoke-WebRequest -Uri $RootfsUrl -OutFile (Join-Path $BuildDir 'rootfs.tar.gz') -UseBasicParsing

# ── Inno Setup ────────────────────────────────────────────────────────────────
Write-Host '[build-container] Installing Inno Setup...' -ForegroundColor Cyan
choco install innosetup --no-progress -y
if ($LASTEXITCODE -ne 0) { throw "choco install innosetup failed" }
$IsccCandidates = @(
  'C:\Program Files (x86)\Inno Setup 6\ISCC.exe', 'C:\Program Files\Inno Setup 6\ISCC.exe',
  'C:\Program Files (x86)\Inno Setup 7\ISCC.exe', 'C:\Program Files\Inno Setup 7\ISCC.exe'
)
$Iscc = $IsccCandidates | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $Iscc) {
  $Iscc = Get-ChildItem 'C:\Program Files (x86)' -Filter 'ISCC.exe' -Recurse -ErrorAction SilentlyContinue |
          Select-Object -First 1 -ExpandProperty FullName
}
if (-not $Iscc) { throw "ISCC.exe not found after installing Inno Setup" }

$DistDir = Join-Path $ScriptDir 'dist'
New-Item -ItemType Directory -Path $DistDir -Force | Out-Null
$InstallerPath = Join-Path $DistDir 'Provisa-Container-Setup.exe'

$IssPath = Join-Path $env:TEMP 'provisa-container.iss'
$IssContent = @"
[Setup]
AppName=Provisa Container Tier
AppVersion=$Version
AppPublisher=Provisa
DefaultDirName={userappdata}\Programs\Provisa-Container
DefaultGroupName=Provisa
OutputDir=$DistDir
OutputBaseFilename=Provisa-Container-Setup
Compression=lzma2/ultra64
SolidCompression=yes
PrivilegesRequired=lowest
SetupIconFile=$BuildDir\provisa.ico
UninstallDisplayName=Provisa Container Tier

[Files]
Source: "$BuildDir\*"; DestDir: "{app}"; Flags: recursesubdirs createallsubdirs

[Run]
Filename: "powershell.exe"; Parameters: "-ExecutionPolicy Bypass -File ""{app}\install-container.ps1"""; Description: "Set up the Provisa container tier (WSL2)"; Flags: postinstall
"@
[System.IO.File]::WriteAllText($IssPath, $IssContent, [System.Text.Encoding]::UTF8)

Write-Host '[build-container] Compiling installer with Inno Setup...' -ForegroundColor Cyan
& $Iscc $IssPath
if ($LASTEXITCODE -ne 0) { throw "ISCC.exe failed with exit code $LASTEXITCODE" }
if (-not (Test-Path $InstallerPath)) { throw "Expected output $InstallerPath not found" }
Write-Host "[build-container] Installer created: $InstallerPath" -ForegroundColor Green

# ── Code signing ──────────────────────────────────────────────────────────────
if ($env:WINDOWS_CERT_PFX_BASE64) {
    Write-Host '[build-container] Signing installer...' -ForegroundColor Cyan
    $PfxPath = [System.IO.Path]::Combine([System.IO.Path]::GetTempPath(), 'provisa-signing.pfx')
    try {
        [System.Convert]::FromBase64String($env:WINDOWS_CERT_PFX_BASE64) | Set-Content -Path $PfxPath -AsByteStream
        $TimestampUrl = if ($env:WINDOWS_CERT_TIMESTAMP_URL) { $env:WINDOWS_CERT_TIMESTAMP_URL } else { 'http://timestamp.digicert.com' }
        & signtool sign /f $PfxPath /p $env:WINDOWS_CERT_PFX_PASSWORD /tr $TimestampUrl /td sha256 /fd sha256 $InstallerPath
        if ($LASTEXITCODE -ne 0) { throw "signtool failed with exit code $LASTEXITCODE" }
        Write-Host '[build-container] Installer signed.' -ForegroundColor Green
    } finally {
        Remove-Item -Path $PfxPath -Force -ErrorAction SilentlyContinue
    }
} else {
    Write-Host '[build-container] WINDOWS_CERT_PFX_BASE64 not set — skipping signing.' -ForegroundColor Yellow
}

Write-Host "Output: $InstallerPath"
