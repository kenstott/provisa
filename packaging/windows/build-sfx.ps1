# Build Provisa Windows installer as 7-Zip SFX. Run from repo root.
# Uses 7-Zip SFX instead of NSIS to handle multi-GB payloads without 32-bit limits.
#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot  = Split-Path -Parent (Split-Path -Parent $ScriptDir)

Write-Host '[build-sfx] Preparing build directory...' -ForegroundColor Cyan

# ── Assemble build tree (mirrors build-installer.ps1) ─────────────────────────
$BuildDir = Join-Path $ScriptDir 'build'

$BuildImages = Join-Path $BuildDir 'images'
New-Item -ItemType Directory -Path $BuildImages -Force | Out-Null
$ObsImages = @('minio-latest.tar.gz','otlp2parquet-latest.tar.gz',
    'opentelemetry-collector-contrib-0.99.0.tar.gz','prometheus-v2.51.2.tar.gz',
    'tempo-2.4.1.tar.gz','grafana-10.4.2.tar.gz',
    'petstore3-unstable.tar.gz','graphql-demo-local.tar.gz')
Get-ChildItem -Path (Join-Path $ScriptDir 'images') -Filter '*.tar.gz' |
  Where-Object { $_.Name -ne 'provisa-local.tar.gz' -and $ObsImages -notcontains $_.Name } |
  Copy-Item -Destination $BuildImages

$BuildCompose = Join-Path $BuildDir 'compose'
New-Item -ItemType Directory -Path $BuildCompose -Force | Out-Null
Copy-Item (Join-Path $RepoRoot 'docker-compose.core.yml')   $BuildCompose
Copy-Item (Join-Path $RepoRoot 'docker-compose.app.yml')    $BuildCompose
Copy-Item (Join-Path $RepoRoot 'docker-compose.airgap.yml') $BuildCompose
Copy-Item (Join-Path $RepoRoot 'config')  (Join-Path $BuildCompose 'config')  -Recurse -Force
Copy-Item (Join-Path $RepoRoot 'db')      (Join-Path $BuildCompose 'db')      -Recurse -Force
Copy-Item (Join-Path $RepoRoot 'trino')   (Join-Path $BuildCompose 'trino')   -Recurse -Force

$BuildSrc = Join-Path $BuildDir 'provisa-source'
New-Item -ItemType Directory -Path $BuildSrc -Force | Out-Null
Copy-Item (Join-Path $RepoRoot 'Dockerfile')    $BuildSrc
Copy-Item (Join-Path $RepoRoot 'main.py')        $BuildSrc
Copy-Item (Join-Path $RepoRoot 'pyproject.toml') $BuildSrc
Copy-Item (Join-Path $RepoRoot 'provisa')   (Join-Path $BuildSrc 'provisa')   -Recurse -Force

Copy-Item (Join-Path $ScriptDir 'first-launch.ps1') $BuildDir
Copy-Item (Join-Path $ScriptDir 'provisa.ps1')       $BuildDir
Copy-Item (Join-Path $ScriptDir 'provisa.cmd')       $BuildDir
Copy-Item (Join-Path $ScriptDir 'install.ps1')       $BuildDir
Copy-Item (Join-Path $ScriptDir 'uninstall.ps1')     $BuildDir

$BuildRedist = Join-Path $BuildDir 'redist'
New-Item -ItemType Directory -Path $BuildRedist -Force | Out-Null
$NerdctlSrc = Join-Path $ScriptDir 'redist\nerdctl-full.tar.gz'
if (-not (Test-Path $NerdctlSrc)) {
  throw "nerdctl-full.tar.gz not found at $NerdctlSrc — CI should download it before building."
}
Copy-Item $NerdctlSrc $BuildRedist

# ── Find 7-Zip (pre-installed on GitHub Actions windows-latest) ───────────────
$SevenZip = 'C:\Program Files\7-Zip\7z.exe'
if (-not (Test-Path $SevenZip)) { throw "7z.exe not found at $SevenZip" }
$SfxModule = 'C:\Program Files\7-Zip\7zSD.sfx'
if (-not (Test-Path $SfxModule)) { throw "7zSD.sfx not found at $SfxModule" }

# ── Create dist dir ───────────────────────────────────────────────────────────
$DistDir = Join-Path $ScriptDir 'dist'
New-Item -ItemType Directory -Path $DistDir -Force | Out-Null

$Version     = if ($env:VERSION) { $env:VERSION } else { 'dev' }
$ArchivePath = Join-Path $env:TEMP 'provisa-core.7z'
$InstallerPath = Join-Path $DistDir 'Provisa-Setup.exe'

# ── SFX config — RunProgram executes install.ps1 after extraction ─────────────
$SfxConfig = ";!@Install@!UTF-8!`nTitle=`"Provisa $Version`"`nRunProgram=`"powershell.exe -ExecutionPolicy Bypass -File install.ps1`"`n;!@InstallEnd@!"
$ConfigPath = Join-Path $env:TEMP 'provisa-sfx-config.txt'
[System.IO.File]::WriteAllText($ConfigPath, $SfxConfig, [System.Text.Encoding]::UTF8)

# ── Create 7z archive ─────────────────────────────────────────────────────────
Write-Host '[build-sfx] Creating 7z archive (LZMA2, this may take several minutes)...' -ForegroundColor Cyan
& $SevenZip a -t7z -m0=lzma2 -mx=5 -mmt=on "$ArchivePath" "$BuildDir\*"
if ($LASTEXITCODE -ne 0) { throw "7z a failed with exit code $LASTEXITCODE" }

# ── Combine SFX module + config + archive into .exe ───────────────────────────
Write-Host '[build-sfx] Combining SFX module, config, and archive...' -ForegroundColor Cyan
$sfxBytes     = [System.IO.File]::ReadAllBytes($SfxModule)
$configBytes  = [System.IO.File]::ReadAllBytes($ConfigPath)
$archiveBytes = [System.IO.File]::ReadAllBytes($ArchivePath)

$outStream = [System.IO.File]::Create($InstallerPath)
$outStream.Write($sfxBytes,    0, $sfxBytes.Length)
$outStream.Write($configBytes, 0, $configBytes.Length)
$outStream.Write($archiveBytes,0, $archiveBytes.Length)
$outStream.Close()

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
