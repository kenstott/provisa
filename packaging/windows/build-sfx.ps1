# Build the Provisa Windows base installer (native tier, REQ-979) using Inno Setup.
# The native tier runs Provisa on a self-contained standalone Python interpreter
# (python-build-standalone + provisa wheel + duckdb/pg_duckdb + aiosqlite) with NO
# Docker, VM, or container images. Mirrors macOS build-dmg.sh bundle_native_runtime.
# The compute/container tier (WSL2 + Trino) is a separate on-demand download, not
# bundled here — the base installer ships no container images (REQ-889, REQ-979).
#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot  = Split-Path -Parent (Split-Path -Parent $ScriptDir)

# Pins are overridable so the builder can bump CPython without editing this file.
# Match macOS build-dmg.sh (PBS_RELEASE / PBS_PYTHON) so both tiers ship the same
# interpreter version.
$PbsRelease = if ($env:PBS_RELEASE) { $env:PBS_RELEASE } else { '20250612' }
$PbsPython  = if ($env:PBS_PYTHON)  { $env:PBS_PYTHON }  else { '3.12.11' }

Write-Host '[build-sfx] Preparing build directory...' -ForegroundColor Cyan

# ── Assemble build tree ───────────────────────────────────────────────────────
$BuildDir = Join-Path $ScriptDir 'build'
if (Test-Path $BuildDir) { Remove-Item $BuildDir -Recurse -Force }
New-Item -ItemType Directory -Path $BuildDir -Force | Out-Null

# Launch + lifecycle scripts for the native tier (no VirtualBox, no compose).
Copy-Item (Join-Path $ScriptDir 'first-launch-native.ps1') $BuildDir
Copy-Item (Join-Path $ScriptDir 'provisa-native.ps1')      $BuildDir
Copy-Item (Join-Path $ScriptDir 'launch-gui.vbs')          $BuildDir
Copy-Item (Join-Path $ScriptDir 'provisa.cmd')             $BuildDir
Copy-Item (Join-Path $ScriptDir 'uninstall.ps1')           $BuildDir
Copy-Item (Join-Path $ScriptDir 'provisa.ico')             $BuildDir
Copy-Item (Join-Path $ScriptDir 'provisa-mark.png')        $BuildDir

# ── Build the React UI (served from <site-packages>/static by ui_server) ──────
Write-Host '[build-sfx] Building provisa-ui...' -ForegroundColor Cyan
Push-Location (Join-Path $RepoRoot 'provisa-ui')
try {
  & npm ci
  if ($LASTEXITCODE -ne 0) { throw "npm ci failed" }
  & npm run build
  if ($LASTEXITCODE -ne 0) { throw "npm run build failed" }
} finally { Pop-Location }
$UiDist = Join-Path $RepoRoot 'provisa-ui\dist'
if (-not (Test-Path $UiDist)) { throw "provisa-ui\dist not found after build" }

# ── Bundle the standalone native Python runtime (REQ-979) ─────────────────────
# Download python-build-standalone (relocatable CPython for Windows x86_64),
# pip-install provisa + uvicorn INTO it, and drop the built UI where ui_server
# resolves it (<site-packages>\static). first-launch-native.ps1 stages this to
# %USERPROFILE%\.provisa\runtime and provisa-native.ps1 runs uvicorn against it.
$RuntimeDst = Join-Path $BuildDir 'runtime'
$Tarball    = "cpython-$PbsPython+$PbsRelease-x86_64-pc-windows-msvc-install_only.tar.gz"
$PbsUrl     = "https://github.com/astral-sh/python-build-standalone/releases/download/$PbsRelease/$Tarball"
$Tmp        = Join-Path $ScriptDir 'tmp-pbs'
if (Test-Path $Tmp) { Remove-Item $Tmp -Recurse -Force }
New-Item -ItemType Directory -Path $Tmp -Force | Out-Null

Write-Host "[build-sfx] Downloading python-build-standalone $PbsPython (Windows x86_64)..." -ForegroundColor Cyan
$TarballPath = Join-Path $Tmp $Tarball
Invoke-WebRequest -Uri $PbsUrl -OutFile $TarballPath -UseBasicParsing
# bsdtar (bundled with Windows 10+) extracts .tar.gz natively.
& tar -xzf $TarballPath -C $Tmp
if ($LASTEXITCODE -ne 0) { throw "tar extraction failed for $Tarball" }

$PbsPy = Join-Path $Tmp 'python\python.exe'   # PBS extracts to $Tmp\python\
if (-not (Test-Path $PbsPy)) { throw "python-build-standalone extraction failed (no python\python.exe)" }
Move-Item (Join-Path $Tmp 'python') $RuntimeDst
$RuntimePy = Join-Path $RuntimeDst 'python.exe'

Write-Host '[build-sfx] Installing provisa + deps into the native runtime...' -ForegroundColor Cyan
& $RuntimePy -m pip install --upgrade pip --quiet
if ($LASTEXITCODE -ne 0) { throw "pip upgrade failed" }
& $RuntimePy -m pip install --quiet $RepoRoot uvicorn
if ($LASTEXITCODE -ne 0) { throw "pip install provisa failed" }

# Place the built UI where ui_server resolves it: <site-packages>\static.
$Site = & $RuntimePy -c "import sysconfig; print(sysconfig.get_paths()['purelib'])"
if ($LASTEXITCODE -ne 0 -or -not $Site) { throw "could not resolve site-packages purelib" }
$StaticDst = Join-Path $Site 'static'
New-Item -ItemType Directory -Path $StaticDst -Force | Out-Null
Copy-Item -Path (Join-Path $UiDist '*') -Destination $StaticDst -Recurse -Force

Remove-Item $Tmp -Recurse -Force
Write-Host '[build-sfx] Native runtime bundled.' -ForegroundColor Green

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

$Version       = if ($env:VERSION) { $env:VERSION } else { 'dev' }
$InstallerPath = Join-Path $DistDir 'Provisa-Setup.exe'

[System.IO.File]::WriteAllText((Join-Path $BuildDir 'VERSION'), $Version, [System.Text.Encoding]::ASCII)

# ── Generate Inno Setup script ────────────────────────────────────────────────
$IssPath = Join-Path $env:TEMP 'provisa-installer.iss'

# Inno Setup uses ; for comments, not //
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
SetupIconFile=$BuildDir\provisa.ico
UninstallDisplayName=Provisa
UninstallDisplayIcon={app}\provisa.ico

[Files]
Source: "$BuildDir\*"; DestDir: "{app}"; Flags: recursesubdirs createallsubdirs

[Icons]
Name: "{group}\Provisa"; Filename: "wscript.exe"; Parameters: "/nologo ""{app}\launch-gui.vbs"""; IconFilename: "{app}\provisa.ico"

[Run]
Filename: "wscript.exe"; Parameters: "/nologo ""{app}\launch-gui.vbs"""; Description: "Launch Provisa"; Flags: postinstall nowait

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
