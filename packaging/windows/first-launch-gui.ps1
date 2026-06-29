#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$LogPath = Join-Path $env:TEMP 'provisa-first-launch.log'
Start-Transcript -Path $LogPath -Append -ErrorAction SilentlyContinue

try {

Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing
[System.Windows.Forms.Application]::EnableVisualStyles()

$ScriptDir     = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }
$VersionFile   = Join-Path $ScriptDir 'VERSION'
$EmbeddedVersion = if (Test-Path $VersionFile) { (Get-Content $VersionFile -Raw).Trim() } else { $null }
$ImagesDir     = Join-Path $ScriptDir 'images'
$ComposeDir    = Join-Path $ScriptDir 'compose'
$SourceDir     = Join-Path $ScriptDir 'provisa-source'
$RedistDir     = Join-Path $ScriptDir 'redist'
$NerdctlBundle = Join-Path $RedistDir 'nerdctl-full.tar.gz'
$ProvisaHome   = Join-Path $env:USERPROFILE '.provisa'
$Sentinel      = Join-Path $ProvisaHome '.first-launch-complete'

if (Test-Path $Sentinel) {
  [System.Windows.Forms.MessageBox]::Show(
    'Provisa is already set up. Run: provisa start',
    'Provisa', 'OK', 'Information') | Out-Null
  exit 0
}

function ConvertTo-WslPath {
  param([string]$WinPath)
  $d = $WinPath[0].ToString().ToLower()
  $r = $WinPath.Substring(2) -replace '\\', '/'
  return "/mnt/$d$r"
}

function Find-ExtFile {
  param([string]$Pattern)
  $candidates = @()
  $candidates += @(Get-ChildItem -Path $ScriptDir -Filter $Pattern -ErrorAction SilentlyContinue)
  $dl = Join-Path $env:USERPROFILE 'Downloads'
  $candidates += @(Get-ChildItem -Path $dl -Filter $Pattern -ErrorAction SilentlyContinue)
  foreach ($drv in [System.IO.DriveInfo]::GetDrives() | Where-Object { $_.DriveType -eq 'Removable' -or $_.DriveType -eq 'CDRom' }) {
    $candidates += @(Get-ChildItem -Path $drv.RootDirectory.FullName -Filter $Pattern -ErrorAction SilentlyContinue)
  }
  return ($candidates | Select-Object -First 1)
}

$coreFile = Find-ExtFile 'provisa-core-images-*.tar.gz'
$obsFile  = Find-ExtFile 'provisa-obs-images-*.tar.gz'
$demoFile = Find-ExtFile 'provisa-demo-images-*.tar.gz'

# ── RAM options ───────────────────────────────────────────────────────────────
$totalBytes = (Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory
$totalGb    = [int][Math]::Floor($totalBytes / 1GB)
$ramOptions = [System.Collections.ArrayList]::new()
foreach ($s in @(4, 8, 16, 32, 64, 128)) {
  if ($s -le $totalGb) { $null = $ramOptions.Add("${s}GB") }
}
$null = $ramOptions.Add("All (${totalGb}GB)")

# ── Form ──────────────────────────────────────────────────────────────────────
$form = New-Object System.Windows.Forms.Form
$form.Text            = 'Provisa Setup'
$form.ClientSize      = New-Object System.Drawing.Size(600, 544)
$form.StartPosition   = 'CenterScreen'
$form.FormBorderStyle = 'FixedSingle'
$form.MaximizeBox     = $false
$form.BackColor       = [System.Drawing.Color]::White
$form.Font            = New-Object System.Drawing.Font('Segoe UI', 9)

# Header
$header           = New-Object System.Windows.Forms.Panel
$header.Dock      = 'Top'
$header.Height    = 72
$header.BackColor = [System.Drawing.Color]::FromArgb(24, 24, 24)
$form.Controls.Add($header)

$lbTitle          = New-Object System.Windows.Forms.Label
$lbTitle.Text     = 'Provisa'
$lbTitle.Font     = New-Object System.Drawing.Font('Segoe UI', 22, [System.Drawing.FontStyle]::Bold)
$lbTitle.ForeColor = [System.Drawing.Color]::White
$lbTitle.AutoSize = $true
$lbTitle.Location = New-Object System.Drawing.Point(20, 12)
$header.Controls.Add($lbTitle)

$lbSub            = New-Object System.Windows.Forms.Label
$lbSub.Text       = 'First-time Setup'
$lbSub.Font       = New-Object System.Drawing.Font('Segoe UI', 10)
$lbSub.ForeColor  = [System.Drawing.Color]::FromArgb(170, 170, 170)
$lbSub.AutoSize   = $true
$lbSub.Location   = New-Object System.Drawing.Point(130, 28)
$header.Controls.Add($lbSub)

# ── Panel 1 : Config ──────────────────────────────────────────────────────────
$pConfig          = New-Object System.Windows.Forms.Panel
$pConfig.Location = New-Object System.Drawing.Point(0, 72)
$pConfig.Size     = New-Object System.Drawing.Size(600, 472)
$pConfig.Visible  = $true
$form.Controls.Add($pConfig)

function Lbl { param($Text, $X, $Y, $Bold)
  $l = New-Object System.Windows.Forms.Label
  $l.Text     = $Text
  $l.AutoSize = $true
  $l.Location = New-Object System.Drawing.Point($X, $Y)
  if ($Bold) { $l.Font = New-Object System.Drawing.Font('Segoe UI', 10, [System.Drawing.FontStyle]::Bold) }
  $pConfig.Controls.Add($l)
}

Lbl 'RAM Budget'   20 18 $true
Lbl "How much RAM can Provisa use?  (system: ${totalGb} GB)" 20 44 $false

$radios = @()
$rx = 20
foreach ($opt in $ramOptions) {
  $rb          = New-Object System.Windows.Forms.RadioButton
  $rb.Text     = $opt
  $rb.AutoSize = $true
  $rb.Location = New-Object System.Drawing.Point($rx, 70)
  $rb.Checked  = ($opt -eq '16GB')
  $pConfig.Controls.Add($rb)
  $radios += $rb
  $rx += [int]($rb.PreferredSize.Width) + 12
}
if (-not ($radios | Where-Object { $_.Checked })) { $radios[0].Checked = $true }

Lbl 'Hostname'  20 118 $true
$tbHost          = New-Object System.Windows.Forms.TextBox
$tbHost.Text     = 'localhost'
$tbHost.Font     = New-Object System.Drawing.Font('Segoe UI', 10)
$tbHost.Location = New-Object System.Drawing.Point(20, 142)
$tbHost.Width    = 260
$pConfig.Controls.Add($tbHost)

Lbl 'Web UI Port'  20 178 $true
$nudPort          = New-Object System.Windows.Forms.NumericUpDown
$nudPort.Minimum  = 1024
$nudPort.Maximum  = 65535
$nudPort.Value    = 3000
$nudPort.Font     = New-Object System.Drawing.Font('Segoe UI', 10)
$nudPort.Location = New-Object System.Drawing.Point(20, 202)
$nudPort.Width    = 100
$pConfig.Controls.Add($nudPort)

Lbl 'Core Images'  20 238 $true
$lbCoreStatus           = New-Object System.Windows.Forms.Label
$lbCoreStatus.AutoSize  = $true
$lbCoreStatus.Location  = New-Object System.Drawing.Point(20, 258)
if ($coreFile) {
  $lbCoreStatus.Text     = $coreFile.Name
  $lbCoreStatus.ForeColor = [System.Drawing.Color]::FromArgb(0, 160, 0)
} else {
  $lbCoreStatus.Text     = 'not found locally - will download (required)'
  $lbCoreStatus.ForeColor = [System.Drawing.Color]::FromArgb(180, 100, 0)
}
$pConfig.Controls.Add($lbCoreStatus)

Lbl 'Extensions'  20 288 $true

$cbObs          = New-Object System.Windows.Forms.CheckBox
$cbObs.Text     = 'Observability'
$cbObs.AutoSize = $true
$cbObs.Location = New-Object System.Drawing.Point(20, 314)
$cbObs.Checked  = ($null -ne $obsFile)
$pConfig.Controls.Add($cbObs)

$lbObsStatus           = New-Object System.Windows.Forms.Label
$lbObsStatus.AutoSize  = $true
$lbObsStatus.Location  = New-Object System.Drawing.Point(160, 316)
if ($obsFile) {
  $lbObsStatus.Text     = $obsFile.Name
  $lbObsStatus.ForeColor = [System.Drawing.Color]::FromArgb(0, 160, 0)
} else {
  $lbObsStatus.Text     = 'not found locally - will download if checked'
  $lbObsStatus.ForeColor = [System.Drawing.Color]::FromArgb(160, 160, 160)
}
$pConfig.Controls.Add($lbObsStatus)

$cbDemo          = New-Object System.Windows.Forms.CheckBox
$cbDemo.Text     = 'Demo'
$cbDemo.AutoSize = $true
$cbDemo.Location = New-Object System.Drawing.Point(20, 340)
$cbDemo.Checked  = ($null -ne $demoFile)
$pConfig.Controls.Add($cbDemo)

$lbDemoStatus           = New-Object System.Windows.Forms.Label
$lbDemoStatus.AutoSize  = $true
$lbDemoStatus.Location  = New-Object System.Drawing.Point(160, 342)
if ($demoFile) {
  $lbDemoStatus.Text     = $demoFile.Name
  $lbDemoStatus.ForeColor = [System.Drawing.Color]::FromArgb(0, 160, 0)
} else {
  $lbDemoStatus.Text     = 'not found locally - will download if checked'
  $lbDemoStatus.ForeColor = [System.Drawing.Color]::FromArgb(160, 160, 160)
}
$pConfig.Controls.Add($lbDemoStatus)

$btnInstall            = New-Object System.Windows.Forms.Button
$btnInstall.Text       = 'Install'
$btnInstall.Font       = New-Object System.Drawing.Font('Segoe UI', 10, [System.Drawing.FontStyle]::Bold)
$btnInstall.Size       = New-Object System.Drawing.Size(110, 36)
$btnInstall.Location   = New-Object System.Drawing.Point(470, 376)
$btnInstall.BackColor  = [System.Drawing.Color]::FromArgb(0, 120, 215)
$btnInstall.ForeColor  = [System.Drawing.Color]::White
$btnInstall.FlatStyle  = 'Flat'
$btnInstall.FlatAppearance.BorderSize = 0
$pConfig.Controls.Add($btnInstall)

# ── Panel 2 : Progress ────────────────────────────────────────────────────────
$pProg          = New-Object System.Windows.Forms.Panel
$pProg.Location = New-Object System.Drawing.Point(0, 72)
$pProg.Size     = New-Object System.Drawing.Size(600, 472)
$pProg.Visible  = $false
$form.Controls.Add($pProg)

$pb               = New-Object System.Windows.Forms.ProgressBar
$pb.Location      = New-Object System.Drawing.Point(20, 20)
$pb.Size          = New-Object System.Drawing.Size(560, 22)
$pb.Minimum       = 0
$pb.Maximum       = 100
$pProg.Controls.Add($pb)

$lbStatus          = New-Object System.Windows.Forms.Label
$lbStatus.Text     = 'Starting...'
$lbStatus.AutoSize = $true
$lbStatus.Location = New-Object System.Drawing.Point(20, 50)
$pProg.Controls.Add($lbStatus)

$lbDownload           = New-Object System.Windows.Forms.Label
$lbDownload.AutoSize  = $true
$lbDownload.Location  = New-Object System.Drawing.Point(20, 72)
$lbDownload.ForeColor = [System.Drawing.Color]::FromArgb(160, 160, 160)
$lbDownload.Text      = ''
$lbDownload.Visible   = $false
$pProg.Controls.Add($lbDownload)

$pbDownload          = New-Object System.Windows.Forms.ProgressBar
$pbDownload.Location = New-Object System.Drawing.Point(20, 90)
$pbDownload.Size     = New-Object System.Drawing.Size(560, 14)
$pbDownload.Minimum  = 0
$pbDownload.Maximum  = 100
$pbDownload.Visible  = $false
$pProg.Controls.Add($pbDownload)

$rtb               = New-Object System.Windows.Forms.RichTextBox
$rtb.Location      = New-Object System.Drawing.Point(20, 110)
$rtb.Size          = New-Object System.Drawing.Size(560, 274)
$rtb.ReadOnly      = $true
$rtb.Font          = New-Object System.Drawing.Font('Consolas', 8)
$rtb.BackColor     = [System.Drawing.Color]::FromArgb(18, 18, 18)
$rtb.ForeColor     = [System.Drawing.Color]::FromArgb(204, 204, 204)
$rtb.BorderStyle   = 'None'
$pProg.Controls.Add($rtb)

$btnFinish            = New-Object System.Windows.Forms.Button
$btnFinish.Text       = 'Finish'
$btnFinish.Font       = New-Object System.Drawing.Font('Segoe UI', 10, [System.Drawing.FontStyle]::Bold)
$btnFinish.Size       = New-Object System.Drawing.Size(110, 36)
$btnFinish.Location   = New-Object System.Drawing.Point(470, 426)
$btnFinish.BackColor  = [System.Drawing.Color]::FromArgb(0, 120, 215)
$btnFinish.ForeColor  = [System.Drawing.Color]::White
$btnFinish.FlatStyle  = 'Flat'
$btnFinish.FlatAppearance.BorderSize = 0
$btnFinish.Enabled    = $false
$pProg.Controls.Add($btnFinish)

$btnFinish.Add_Click({ $form.Close() })

# ── Synchronized state ────────────────────────────────────────────────────────
$sync = [hashtable]::Synchronized(@{
  Queue         = [System.Collections.Queue]::Synchronized((New-Object System.Collections.Queue))
  Progress      = 0
  Status        = 'Starting...'
  Done          = $false
  Error         = $null
  DownloadPct   = -1
  DownloadLabel = ''
})

# ── Timer (polls background runspace) ────────────────────────────────────────
$timer          = New-Object System.Windows.Forms.Timer
$timer.Interval = 150

$timer.Add_Tick({
  while ($sync.Queue.Count -gt 0) {
    $msg = $sync.Queue.Dequeue()
    $rtb.AppendText("$msg`n")
    $rtb.ScrollToCaret()
  }
  if ($sync.Progress -gt $pb.Value) { $pb.Value = [Math]::Min($sync.Progress, 100) }
  if ($sync.Status)                 { $lbStatus.Text = $sync.Status }
  if ($sync.DownloadPct -ge 0) {
    $pbDownload.Visible   = $true
    $lbDownload.Visible   = $true
    $lbDownload.Text      = $sync.DownloadLabel
    $pbDownload.Value     = [Math]::Min($sync.DownloadPct, 100)
  } else {
    $pbDownload.Visible = $false
    $lbDownload.Visible = $false
  }
  if ($sync.Done) {
    $timer.Stop()
    if ($sync.Error) {
      $rtb.SelectionStart  = $rtb.TextLength
      $rtb.SelectionLength = 0
      $rtb.SelectionColor  = [System.Drawing.Color]::FromArgb(255, 80, 80)
      $rtb.AppendText("`nERROR: $($sync.Error)`n")
      $lbStatus.Text   = 'Setup failed. See log above.'
      $btnFinish.Text  = 'Close'
    } else {
      $lbStatus.Text   = 'Setup complete!'
      $btnFinish.Text  = 'Finish'
    }
    $btnFinish.Enabled = $true
  }
})

# ── Install click ─────────────────────────────────────────────────────────────
$btnInstall.Add_Click({
  $sel = $radios | Where-Object { $_.Checked } | Select-Object -First 1
  $ramText = $sel.Text
  if ($ramText -like 'All*') { $budgetGb = $totalGb }
  else                       { $budgetGb = [int]($ramText -replace 'GB', '') }

  if ($budgetGb -ge 96)    { $workers = 4 }
  elseif ($budgetGb -ge 48){ $workers = 2 }
  elseif ($budgetGb -ge 24){ $workers = 1 }
  else                     { $workers = 0 }

  $hostname = $tbHost.Text.Trim()
  if ([string]::IsNullOrEmpty($hostname)) { $hostname = 'localhost' }
  $uiPort = [int]$nudPort.Value

  $coreFilePath = if ($coreFile) { $coreFile.FullName } else { $null }
  $installObs  = $cbObs.Checked
  $installDemo = $cbDemo.Checked
  $obsFilePath  = if ($obsFile)  { $obsFile.FullName }  else { $null }
  $demoFilePath = if ($demoFile) { $demoFile.FullName } else { $null }

  # WSL2 elevation must happen on the UI thread before the runspace starts
  $wslStatus  = wsl --status 2>&1
  $wslEnabled = ($LASTEXITCODE -eq 0) -and ($wslStatus -match 'Default Version: 2' -or $wslStatus -match 'WSL 2')
  if (-not $wslEnabled) {
    [System.Windows.Forms.MessageBox]::Show(
      "WSL2 must be installed.`nA UAC prompt will appear. Accept it to continue.",
      'Provisa Setup', 'OK', 'Information') | Out-Null
    $proc = Start-Process -FilePath 'wsl.exe' -ArgumentList '--install','--no-distribution' -Verb RunAs -Wait -PassThru
    if ($proc.ExitCode -ne 0) {
      [System.Windows.Forms.MessageBox]::Show(
        "WSL2 installation failed (exit $($proc.ExitCode)).`nReboot and re-run setup.",
        'Provisa Setup', 'OK', 'Error') | Out-Null
      return
    }
    $wslTest = wsl echo ok 2>&1
    if ($LASTEXITCODE -ne 0) {
      [System.Windows.Forms.MessageBox]::Show(
        'WSL2 installed but not responding. Reboot and re-run setup.',
        'Provisa Setup', 'OK', 'Warning') | Out-Null
      return
    }
  }

  $pConfig.Visible = $false
  $pProg.Visible   = $true

  # ── Background runspace ───────────────────────────────────────────────────
  $rs = [runspacefactory]::CreateRunspace()
  $rs.ApartmentState = 'STA'
  $rs.ThreadOptions  = 'ReuseThread'
  $rs.Open()
  $rs.SessionStateProxy.SetVariable('sync',          $sync)
  $rs.SessionStateProxy.SetVariable('ScriptDir',     $ScriptDir)
  $rs.SessionStateProxy.SetVariable('ImagesDir',     $ImagesDir)
  $rs.SessionStateProxy.SetVariable('ComposeDir',    $ComposeDir)
  $rs.SessionStateProxy.SetVariable('SourceDir',     $SourceDir)
  $rs.SessionStateProxy.SetVariable('NerdctlBundle', $NerdctlBundle)
  $rs.SessionStateProxy.SetVariable('ProvisaHome',   $ProvisaHome)
  $rs.SessionStateProxy.SetVariable('Sentinel',      $Sentinel)
  $rs.SessionStateProxy.SetVariable('BudgetGb',      $budgetGb)
  $rs.SessionStateProxy.SetVariable('Workers',       $workers)
  $rs.SessionStateProxy.SetVariable('Hostname',      $hostname)
  $rs.SessionStateProxy.SetVariable('UiPort',        $uiPort)
  $rs.SessionStateProxy.SetVariable('EmbeddedVersion', $EmbeddedVersion)
  $rs.SessionStateProxy.SetVariable('CoreFilePath',  $coreFilePath)
  $rs.SessionStateProxy.SetVariable('InstallObs',    $installObs)
  $rs.SessionStateProxy.SetVariable('InstallDemo',   $installDemo)
  $rs.SessionStateProxy.SetVariable('ObsFilePath',   $obsFilePath)
  $rs.SessionStateProxy.SetVariable('DemoFilePath',  $demoFilePath)

  $ps = [powershell]::Create()
  $ps.Runspace = $rs
  $null = $ps.AddScript({
    function Invoke-Download {
      param([string]$Url, [string]$OutFile, [string]$Label)
      $sync.DownloadLabel = $Label
      $sync.DownloadPct   = 0
      try {
        $req  = [System.Net.HttpWebRequest]::Create($Url)
        $req.UserAgent = 'Provisa-Installer'
        $resp = $req.GetResponse()
        $total = $resp.ContentLength
        $src   = $resp.GetResponseStream()
        $dst   = [System.IO.File]::OpenWrite($OutFile)
        $buf   = New-Object byte[] 65536
        $received = [long]0
        do {
          $n = $src.Read($buf, 0, $buf.Length)
          if ($n -gt 0) { $dst.Write($buf, 0, $n); $received += $n }
          if ($total -gt 0) {
            $sync.DownloadPct = [int](($received / $total) * 100)
          }
        } while ($n -gt 0)
        $dst.Close(); $src.Close(); $resp.Close()
      } catch {
        if ($dst)  { try { $dst.Close()  } catch {} }
        if ($src)  { try { $src.Close()  } catch {} }
        if ($resp) { try { $resp.Close() } catch {} }
        throw
      } finally {
        $sync.DownloadPct = -1
      }
    }
    function Log { param($Msg)
      $sync.Queue.Enqueue($Msg)
      $sync.Status = $Msg
    }
    function Wsl2Path { param($W)
      $d = $W[0].ToString().ToLower()
      $r = $W.Substring(2) -replace '\\', '/'
      return "/mnt/$d$r"
    }
    try {
      # nerdctl
      Log 'Checking nerdctl...'
      $sync.Progress = 5
      $nc = wsl sh -c 'command -v nerdctl' 2>&1
      if ($LASTEXITCODE -ne 0 -or $nc -notmatch 'nerdctl') {
        Log 'Installing nerdctl-full...'
        if (-not (Test-Path $NerdctlBundle)) { throw "nerdctl bundle missing: $NerdctlBundle" }
        $wb = Wsl2Path $NerdctlBundle
        wsl -u root sh -c "tar -C /usr/local -xzf '$wb'"
        if ($LASTEXITCODE -ne 0) { throw 'nerdctl-full extraction failed.' }
        Log 'nerdctl installed.'
      } else {
        Log "nerdctl present: $nc"
      }
      $sync.Progress = 15

      # containerd
      Log 'Starting containerd...'
      wsl -u root sh -c 'nohup containerd > /dev/null 2>&1 & sleep 3'
      $sync.Progress = 20

      # Ensure core images are present (extract from tarball if images dir is empty)
      $hasBundledImages = (Test-Path $ImagesDir) -and (@(Get-ChildItem -Path $ImagesDir -Filter '*.tar.gz' -ErrorAction SilentlyContinue).Count -gt 0)
      if (-not $hasBundledImages) {
        if (-not $CoreFilePath) {
          # Attempt GitHub download
          Log 'Core images not found locally - attempting download...'
          $ver = $env:PROVISA_VERSION
          if (-not $ver) { $ver = $EmbeddedVersion }
          if (-not $ver) {
            try { $o = (& provisa version 2>$null | Select-Object -First 1); if ($o) { $ver = $o.Trim().Split()[-1] } } catch {}
          }
          if ($ver) {
            $fname   = "provisa-core-images-${ver}.tar.gz"
            $url     = "https://github.com/kenstott/provisa/releases/download/${ver}/${fname}"
            $tmpFile = Join-Path $ProvisaHome $fname
            Log "Downloading: $url"
            Log "Saving to:   $tmpFile"
            New-Item -ItemType Directory -Path $ProvisaHome -Force | Out-Null
            try {
              Invoke-Download -Url $url -OutFile $tmpFile -Label "Downloading $fname..."
              $CoreFilePath = $tmpFile
            } catch { throw "Core images download failed: $_. Place provisa-core-images-${ver}.tar.gz beside the installer and re-run." }
          } else { throw "Core images not found and version unknown. Place provisa-core-images-<version>.tar.gz beside the installer and re-run." }
        }
        Log "Extracting core images from $(Split-Path $CoreFilePath -Leaf)..."
        New-Item -ItemType Directory -Path $ImagesDir -Force | Out-Null
        $wpSrc = Wsl2Path $CoreFilePath
        $wpDst = Wsl2Path $ImagesDir
        wsl -u root sh -c "tar -xzf '$wpSrc' -C '$wpDst'"
        if ($LASTEXITCODE -ne 0) { throw "Core images extraction failed." }
        Log "Core images extracted."
      }

      # Load images
      Log 'Loading container images (this takes a few minutes)...'
      $tars  = @(Get-ChildItem -Path $ImagesDir -Filter '*.tar.gz' -ErrorAction Stop)
      $total = $tars.Count
      $i     = 0
      foreach ($tar in $tars) {
        Log "  $($tar.Name)"
        $wp = Wsl2Path $tar.FullName
        wsl -u root nerdctl load -i $wp
        if ($LASTEXITCODE -ne 0) { throw "Failed to load image: $($tar.Name)" }
        $i++
        $sync.Progress = 20 + [int](($i / $total) * 45)
      }
      Log "Loaded $total images."
      $sync.Progress = 65

      # Build provisa image
      Log 'Building provisa/provisa:local...'
      if (-not (Test-Path $SourceDir)) { throw "provisa-source not found at $SourceDir" }
      $ws = Wsl2Path $SourceDir
      wsl -u root nerdctl build -t provisa/provisa:local $ws
      if ($LASTEXITCODE -ne 0) { throw 'provisa image build failed.' }
      Log 'Build complete.'
      $sync.Progress = 88

      # Write config
      Log 'Writing config...'
      if (-not (Test-Path $ProvisaHome)) {
        New-Item -ItemType Directory -Path $ProvisaHome -Force | Out-Null
      }
      $cfgPath = Join-Path $ProvisaHome 'config.yaml'
      if (-not (Test-Path $cfgPath)) {
        $fwd = $ComposeDir -replace '\\', '/'
        @"
# Provisa configuration - generated by Windows installer
project_dir: "$fwd"
hostname: $Hostname
ui_port: $UiPort
api_port: $($UiPort + 1)
auto_open_browser: true
runtime: wsl2-nerdctl
federation_workers: $Workers
"@ | Set-Content -Path $cfgPath -Encoding UTF8
      }
      $sync.Progress = 92

      # Optional extensions (obs / demo)
      function Get-ExtVersion {
        $v = $env:PROVISA_VERSION
        if ($v) { return $v }
        if ($EmbeddedVersion) { return $EmbeddedVersion }
        try {
          $out = (& provisa version 2>$null | Select-Object -First 1)
          if ($out) { return $out.Trim().Split()[-1] }
        } catch {}
        return $null
      }
      function Load-ExtImages {
        param($Label, $Slug, $FilePath, $ExtDir)
        if (-not $FilePath) {
          $ver = Get-ExtVersion
          if ($ver) {
            $fname = "provisa-${Slug}-images-${ver}.tar.gz"
            $url   = "https://github.com/kenstott/provisa/releases/download/${ver}/${fname}"
            Log "  ${Label}: downloading $url"
            $tmpFile = Join-Path $ProvisaHome $fname
            New-Item -ItemType Directory -Path $ProvisaHome -Force | Out-Null
            try {
              Invoke-Download -Url $url -OutFile $tmpFile -Label "Downloading $fname..."
              $FilePath = $tmpFile
            } catch {
              Log "  ${Label}: download failed - skipping."
              return
            }
          } else {
            Log "  ${Label}: no tarball and version unknown - skipping."
            return
          }
        }
        Log "Extracting $Label images from $(Split-Path $FilePath -Leaf)..."
        New-Item -ItemType Directory -Path $ExtDir -Force | Out-Null
        $wpSrc = Wsl2Path $FilePath
        $wpDst = Wsl2Path $ExtDir
        wsl -u root sh -c "tar -xzf '$wpSrc' -C '$wpDst'"
        if ($LASTEXITCODE -ne 0) { Log "  ${Label}: extraction failed - skipping."; return }
        $imgs = @(Get-ChildItem -Path $ExtDir -Filter '*.tar.gz' -ErrorAction SilentlyContinue)
        foreach ($img in $imgs) {
          Log "  $($img.Name)"
          wsl -u root nerdctl load -i (Wsl2Path $img.FullName)
        }
        Log "$Label extension loaded ($($imgs.Count) images)."
      }

      if ($InstallObs) {
        Log 'Loading Observability extension...'
        Load-ExtImages 'Observability' 'obs' $ObsFilePath (Join-Path $ProvisaHome 'obs-images')
      }
      $sync.Progress = 95
      if ($InstallDemo) {
        Log 'Loading Demo extension...'
        Load-ExtImages 'Demo' 'demo' $DemoFilePath (Join-Path $ProvisaHome 'demo-images')
      }
      $sync.Progress = 98

      # Sentinel + Start Menu shortcut
      New-Item -ItemType File -Path $Sentinel -Force | Out-Null
      $smDir = Join-Path $env:APPDATA 'Microsoft\Windows\Start Menu\Programs\Provisa'
      New-Item -ItemType Directory -Path $smDir -Force | Out-Null
      $wsh  = New-Object -ComObject WScript.Shell
      $link = $wsh.CreateShortcut("$smDir\Start Provisa.lnk")
      $link.TargetPath        = 'powershell.exe'
      $link.Arguments         = "-NoExit -ExecutionPolicy Bypass -Command `"& '$ScriptDir\provisa.cmd' start`""
      $link.WorkingDirectory  = $ScriptDir
      $link.Save()

      $sync.Progress = 100
      Log 'Setup complete! Start Menu -> Provisa -> Start Provisa'
      $sync.Done = $true
    } catch {
      $sync.Error = $_.ToString()
      $sync.Done  = $true
    }
  })

  $script:psHandle   = $ps.BeginInvoke()
  $script:psInstance = $ps
  $timer.Start()
})

[System.Windows.Forms.Application]::Run($form)

} catch {
    $errText = $_.ToString() + "`n`n" + $_.ScriptStackTrace
    try {
        [System.Windows.Forms.MessageBox]::Show(
            $errText,
            'Provisa Setup Error', 'OK', 'Error') | Out-Null
    } catch {
        $errText | Out-File -FilePath $LogPath -Append -Encoding UTF8
    }
} finally {
    Stop-Transcript -ErrorAction SilentlyContinue
}
