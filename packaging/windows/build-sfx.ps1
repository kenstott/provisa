# Build Provisa Windows installer using Inno Setup.
# Inno Setup uses sequential file I/O (not mmap) so handles multi-GB output.
# Replaces NSIS which fails with 32-bit mmap limits on large payloads.
#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot  = Split-Path -Parent (Split-Path -Parent $ScriptDir)

Write-Host '[build-sfx] Preparing build directory...' -ForegroundColor Cyan

# ── Assemble build tree (mirrors build-installer.ps1) ─────────────────────────
$BuildDir = Join-Path $ScriptDir 'build'


$BuildCompose = Join-Path $BuildDir 'compose'
New-Item -ItemType Directory -Path $BuildCompose -Force | Out-Null
Copy-Item (Join-Path $RepoRoot 'docker-compose.core.yml')   $BuildCompose
Copy-Item (Join-Path $RepoRoot 'docker-compose.app.yml')    $BuildCompose
Copy-Item (Join-Path $RepoRoot 'docker-compose.airgap.yml') $BuildCompose
Copy-Item (Join-Path $RepoRoot 'config')  (Join-Path $BuildCompose 'config')  -Recurse -Force
Copy-Item (Join-Path $RepoRoot 'db')      (Join-Path $BuildCompose 'db')      -Recurse -Force
# Copy trino WITHOUT plugins/ — plugins ship as a separate release asset.
$TrinoSrc = Join-Path $RepoRoot 'trino'
$TrinoDst = Join-Path $BuildCompose 'trino'
New-Item -ItemType Directory -Path $TrinoDst -Force | Out-Null
Get-ChildItem -Path $TrinoSrc -Exclude 'plugins' | Copy-Item -Destination $TrinoDst -Recurse -Force

$BuildSrc = Join-Path $BuildDir 'provisa-source'
New-Item -ItemType Directory -Path $BuildSrc -Force | Out-Null
Copy-Item (Join-Path $RepoRoot 'Dockerfile')    $BuildSrc
Copy-Item (Join-Path $RepoRoot 'main.py')        $BuildSrc
Copy-Item (Join-Path $RepoRoot 'pyproject.toml') $BuildSrc
Copy-Item (Join-Path $RepoRoot 'provisa')   (Join-Path $BuildSrc 'provisa')   -Recurse -Force

Copy-Item (Join-Path $ScriptDir 'first-launch.ps1')     $BuildDir
Copy-Item (Join-Path $ScriptDir 'first-launch-gui.ps1') $BuildDir
Copy-Item (Join-Path $ScriptDir 'launch-gui.vbs')       $BuildDir
Copy-Item (Join-Path $ScriptDir 'provisa.ps1')       $BuildDir
Copy-Item (Join-Path $ScriptDir 'provisa.cmd')       $BuildDir
Copy-Item (Join-Path $ScriptDir 'install.ps1')       $BuildDir
Copy-Item (Join-Path $ScriptDir 'uninstall.ps1')     $BuildDir
Copy-Item (Join-Path $ScriptDir 'enable-hyperv.ps1')   $BuildDir
Copy-Item (Join-Path $ScriptDir 'diagnose-hyperv.ps1') $BuildDir

$BuildRedist = Join-Path $BuildDir 'redist'
New-Item -ItemType Directory -Path $BuildRedist -Force | Out-Null
$VBoxSrc = Join-Path $ScriptDir 'redist\VirtualBox-setup.exe'
if (-not (Test-Path $VBoxSrc)) {
  throw "VirtualBox-setup.exe not found at $VBoxSrc -- CI should download it before building."
}
Copy-Item $VBoxSrc $BuildRedist

$OvaSrc = Join-Path $ScriptDir 'provisa-runtime.ova'
if (-not (Test-Path $OvaSrc)) {
  throw "provisa-runtime.ova not found at $OvaSrc -- CI should download it before building."
}
Copy-Item $OvaSrc $BuildDir

# ── Install Inno Setup via chocolatey ─────────────────────────────────────────
Write-Host '[build-sfx] Installing Inno Setup...' -ForegroundColor Cyan
choco install innosetup --no-progress -y
if ($LASTEXITCODE -ne 0) { throw "choco install innosetup failed" }

# Find ISCC.exe
$IsccCandidates = @(
  'C:\Program Files (x86)\Inno Setup 6\ISCC.exe',
  'C:\Program Files\Inno Setup 6\ISCC.exe',
  'C:\Program Files (x86)\Inno Setup 7\ISCC.exe',
  'C:\Program Files\Inno Setup 7\ISCC.exe'
)
$Iscc = $IsccCandidates | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $Iscc) {
  $Iscc = Get-ChildItem 'C:\Program Files (x86)' -Filter 'ISCC.exe' -Recurse -ErrorAction SilentlyContinue |
          Select-Object -First 1 -ExpandProperty FullName
}
if (-not $Iscc) { throw "ISCC.exe not found after installing Inno Setup" }
Write-Host "[build-sfx] Found ISCC.exe: $Iscc" -ForegroundColor Cyan

# ── Create dist dir ───────────────────────────────────────────────────────────
$DistDir = Join-Path $ScriptDir 'dist'
New-Item -ItemType Directory -Path $DistDir -Force | Out-Null

$Version      = if ($env:VERSION) { $env:VERSION } else { 'dev' }
$InstallerPath = Join-Path $DistDir 'Provisa-Setup.exe'

[System.IO.File]::WriteAllText((Join-Path $BuildDir 'VERSION'), $Version, [System.Text.Encoding]::ASCII)

# ── Generate Inno Setup script ────────────────────────────────────────────────
$IssPath = Join-Path $env:TEMP 'provisa-installer.iss'

# Inno Setup uses ; for comments, not //
# {src} = source directory of the .iss file (we pass /D switches for paths)
$IssContent = @"
[Setup]
AppName=Provisa
AppVersion=$Version
AppPublisher=Provisa
DefaultDirName={userappdata}\Programs\Provisa
DefaultGroupName=Provisa
OutputDir=$DistDir
OutputBaseFilename=Provisa-Setup
Compression=lzma2/ultra64
SolidCompression=yes
PrivilegesRequired=lowest
UninstallDisplayName=Provisa
UninstallDisplayIcon={app}\uninstall.ps1

[Files]
Source: "$BuildDir\*"; DestDir: "{app}"; Flags: recursesubdirs createallsubdirs

[Icons]
Name: "{group}\Provisa First Launch"; Filename: "powershell.exe"; Parameters: "-ExecutionPolicy Bypass -WindowStyle Normal -File ""{app}\first-launch-gui.ps1"""

[Run]
Filename: "wscript.exe"; Parameters: "/nologo ""{app}\launch-gui.vbs"""; Description: "Launch Provisa first-run setup (download and configure)"; Flags: postinstall nowait

[Registry]
Root: HKCU; Subkey: "Software\Microsoft\Windows\CurrentVersion\Uninstall\Provisa"; ValueType: string; ValueName: "DisplayName"; ValueData: "Provisa $Version"
Root: HKCU; Subkey: "Software\Microsoft\Windows\CurrentVersion\Uninstall\Provisa"; ValueType: string; ValueName: "DisplayVersion"; ValueData: "$Version"
Root: HKCU; Subkey: "Software\Microsoft\Windows\CurrentVersion\Uninstall\Provisa"; ValueType: string; ValueName: "Publisher"; ValueData: "Provisa"

[UninstallRun]
Filename: "powershell.exe"; Parameters: "-ExecutionPolicy Bypass -File ""{app}\uninstall.ps1"""; RunOnceId: "ProvUninstall"
"@

[System.IO.File]::WriteAllText($IssPath, $IssContent, [System.Text.Encoding]::UTF8)

# ── Run Inno Setup compiler ────────────────────────────────────────────────────
Write-Host '[build-sfx] Compiling installer with Inno Setup...' -ForegroundColor Cyan
& $Iscc $IssPath
if ($LASTEXITCODE -ne 0) { throw "ISCC.exe failed with exit code $LASTEXITCODE" }

if (-not (Test-Path $InstallerPath)) { throw "Expected output $InstallerPath not found" }
Write-Host "[build-sfx] Installer created: $InstallerPath" -ForegroundColor Green

# ── Code signing ───────────────────────────────────────────────────────────────
if ($env:WINDOWS_CERT_PFX_BASE64) {
    Write-Host '[build-sfx] Signing installer...' -ForegroundColor Cyan
    $PfxPath = [System.IO.Path]::Combine([System.IO.Path]::GetTempPath(), 'provisa-signing.pfx')
    try {
        [System.Convert]::FromBase64String($env:WINDOWS_CERT_PFX_BASE64) `
            | Set-Content -Path $PfxPath -AsByteStream
        $TimestampUrl = if ($env:WINDOWS_CERT_TIMESTAMP_URL) {
            $env:WINDOWS_CERT_TIMESTAMP_URL
        } else {
            'http://timestamp.digicert.com'
        }
        & signtool sign `
            /f  $PfxPath `
            /p  $env:WINDOWS_CERT_PFX_PASSWORD `
            /tr $TimestampUrl `
            /td sha256 `
            /fd sha256 `
            $InstallerPath
        if ($LASTEXITCODE -ne 0) { throw "signtool failed with exit code $LASTEXITCODE" }
        Write-Host '[build-sfx] Installer signed.' -ForegroundColor Green
    } finally {
        Remove-Item -Path $PfxPath -Force -ErrorAction SilentlyContinue
    }
} else {
    Write-Host '[build-sfx] WINDOWS_CERT_PFX_BASE64 not set — skipping signing.' -ForegroundColor Yellow
}

Write-Host "Output: $InstallerPath"
