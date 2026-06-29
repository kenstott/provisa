#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$LogPath = Join-Path $env:TEMP 'provisa-first-launch.log'
Start-Transcript -Path $LogPath -Append -ErrorAction SilentlyContinue

try {

Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing
[System.Windows.Forms.Application]::EnableVisualStyles()

$ScriptDir    = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }
$VersionFile  = Join-Path $ScriptDir 'VERSION'
$EmbeddedVersion = if (Test-Path $VersionFile) { (Get-Content $VersionFile -Raw).Trim() } else { $null }
$ComposeDir   = Join-Path $ScriptDir 'compose'
$RedistDir    = Join-Path $ScriptDir 'redist'
$VBoxInstaller = Join-Path $RedistDir 'VirtualBox-setup.exe'
$OvaPath      = Join-Path $ScriptDir 'provisa-runtime.ova'
$ProvisaHome  = Join-Path $env:USERPROFILE '.provisa'
$Sentinel     = Join-Path $ProvisaHome '.first-launch-complete'

if (Test-Path $Sentinel) {
  [System.Windows.Forms.MessageBox]::Show(
    'Provisa is already set up. Run: provisa start',
    'Provisa', 'OK', 'Information') | Out-Null
  exit 0
}

# -- VirtualBox detection -----------------------------------------------------
$VBoxFound = $false
foreach ($p in @(
  "$env:ProgramFiles\Oracle\VirtualBox\VBoxManage.exe",
  "${env:ProgramFiles(x86)}\Oracle\VirtualBox\VBoxManage.exe"
)) {
  if (Test-Path $p) { $VBoxFound = $true; break }
}
if (-not $VBoxFound) {
  $cmd = Get-Command VBoxManage -ErrorAction SilentlyContinue
  if ($cmd) { $VBoxFound = $true }
}

# -- RAM options ---------------------------------------------------------------
$totalBytes = (Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory
$totalGb    = [int][Math]::Floor($totalBytes / 1GB)
$ramOptions = [System.Collections.ArrayList]::new()
foreach ($s in @(4, 8, 16, 32, 64, 128)) {
  if ($s -le $totalGb) { $null = $ramOptions.Add("${s}GB") }
}
$null = $ramOptions.Add("All (${totalGb}GB)")

# -- Form ----------------------------------------------------------------------
$form = New-Object System.Windows.Forms.Form
$form.Text            = 'Provisa Setup'
$form.ClientSize      = New-Object System.Drawing.Size(600, 480)
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

$lbTitle           = New-Object System.Windows.Forms.Label
$lbTitle.Text      = 'Provisa'
$lbTitle.Font      = New-Object System.Drawing.Font('Segoe UI', 22, [System.Drawing.FontStyle]::Bold)
$lbTitle.ForeColor = [System.Drawing.Color]::White
$lbTitle.AutoSize  = $true
$lbTitle.Location  = New-Object System.Drawing.Point(20, 12)
$header.Controls.Add($lbTitle)

$lbSub            = New-Object System.Windows.Forms.Label
$lbSub.Text       = 'First-time Setup'
$lbSub.Font       = New-Object System.Drawing.Font('Segoe UI', 10)
$lbSub.ForeColor  = [System.Drawing.Color]::FromArgb(170, 170, 170)
$lbSub.AutoSize   = $true
$lbSub.Location   = New-Object System.Drawing.Point(130, 28)
$header.Controls.Add($lbSub)

# -- Panel 1 : Config ---------------------------------------------------------
$pConfig          = New-Object System.Windows.Forms.Panel
$pConfig.Location = New-Object System.Drawing.Point(0, 72)
$pConfig.Size     = New-Object System.Drawing.Size(600, 408)
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

Lbl 'RAM Budget' 20 18 $true
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

Lbl 'Hostname' 20 118 $true
$tbHost          = New-Object System.Windows.Forms.TextBox
$tbHost.Text     = 'localhost'
$tbHost.Font     = New-Object System.Drawing.Font('Segoe UI', 10)
$tbHost.Location = New-Object System.Drawing.Point(20, 142)
$tbHost.Width    = 260
$pConfig.Controls.Add($tbHost)

Lbl 'Web UI Port' 20 178 $true
$nudPort          = New-Object System.Windows.Forms.NumericUpDown
$nudPort.Minimum  = 1024
$nudPort.Maximum  = 65535
$nudPort.Value    = 3000
$nudPort.Font     = New-Object System.Drawing.Font('Segoe UI', 10)
$nudPort.Location = New-Object System.Drawing.Point(20, 202)
$nudPort.Width    = 100
$pConfig.Controls.Add($nudPort)

Lbl 'VirtualBox' 20 238 $true
$lbVBoxStatus           = New-Object System.Windows.Forms.Label
$lbVBoxStatus.AutoSize  = $true
$lbVBoxStatus.Location  = New-Object System.Drawing.Point(20, 258)
if ($VBoxFound) {
  $lbVBoxStatus.Text      = 'Installed'
  $lbVBoxStatus.ForeColor = [System.Drawing.Color]::FromArgb(0, 160, 0)
} else {
  $lbVBoxStatus.Text      = 'Not found - will install from bundled installer'
  $lbVBoxStatus.ForeColor = [System.Drawing.Color]::FromArgb(180, 100, 0)
}
$pConfig.Controls.Add($lbVBoxStatus)

$btnInstall            = New-Object System.Windows.Forms.Button
$btnInstall.Text       = 'Install'
$btnInstall.Font       = New-Object System.Drawing.Font('Segoe UI', 10, [System.Drawing.FontStyle]::Bold)
$btnInstall.Size       = New-Object System.Drawing.Size(110, 36)
$btnInstall.Location   = New-Object System.Drawing.Point(470, 310)
$btnInstall.BackColor  = [System.Drawing.Color]::FromArgb(0, 120, 215)
$btnInstall.ForeColor  = [System.Drawing.Color]::White
$btnInstall.FlatStyle  = 'Flat'
$btnInstall.FlatAppearance.BorderSize = 0
$pConfig.Controls.Add($btnInstall)

# -- Panel 2 : Progress -------------------------------------------------------
$pProg          = New-Object System.Windows.Forms.Panel
$pProg.Location = New-Object System.Drawing.Point(0, 72)
$pProg.Size     = New-Object System.Drawing.Size(600, 408)
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

$rtb               = New-Object System.Windows.Forms.RichTextBox
$rtb.Location      = New-Object System.Drawing.Point(20, 72)
$rtb.Size          = New-Object System.Drawing.Size(560, 270)
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
$btnFinish.Location   = New-Object System.Drawing.Point(470, 358)
$btnFinish.BackColor  = [System.Drawing.Color]::FromArgb(0, 120, 215)
$btnFinish.ForeColor  = [System.Drawing.Color]::White
$btnFinish.FlatStyle  = 'Flat'
$btnFinish.FlatAppearance.BorderSize = 0
$btnFinish.Enabled    = $false
$pProg.Controls.Add($btnFinish)

$btnFinish.Add_Click({ $form.Close() })

# -- Synchronized state -------------------------------------------------------
$sync = [hashtable]::Synchronized(@{
  Queue    = [System.Collections.Queue]::Synchronized((New-Object System.Collections.Queue))
  Progress = 0
  Status   = 'Starting...'
  Done     = $false
  Error    = $null
})

# -- Timer (polls background runspace) ----------------------------------------
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
  if ($sync.Done) {
    $timer.Stop()
    if ($sync.Error) {
      $rtb.SelectionStart  = $rtb.TextLength
      $rtb.SelectionLength = 0
      $rtb.SelectionColor  = [System.Drawing.Color]::FromArgb(255, 80, 80)
      $rtb.AppendText("`nERROR: $($sync.Error)`n")
      $lbStatus.Text  = 'Setup failed. See log above.'
      $btnFinish.Text = 'Close'
    } else {
      $lbStatus.Text  = 'Setup complete!'
      $btnFinish.Text = 'Finish'
    }
    $btnFinish.Enabled = $true
  }
})

# -- Install click ------------------------------------------------------------
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

  $pConfig.Visible = $false
  $pProg.Visible   = $true

  # -- Background runspace ----------------------------------------------------
  $rs = [runspacefactory]::CreateRunspace()
  $rs.ApartmentState = 'STA'
  $rs.ThreadOptions  = 'ReuseThread'
  $rs.Open()
  $rs.SessionStateProxy.SetVariable('sync',            $sync)
  $rs.SessionStateProxy.SetVariable('ScriptDir',       $ScriptDir)
  $rs.SessionStateProxy.SetVariable('ComposeDir',      $ComposeDir)
  $rs.SessionStateProxy.SetVariable('VBoxInstaller',   $VBoxInstaller)
  $rs.SessionStateProxy.SetVariable('OvaPath',         $OvaPath)
  $rs.SessionStateProxy.SetVariable('ProvisaHome',     $ProvisaHome)
  $rs.SessionStateProxy.SetVariable('Sentinel',        $Sentinel)
  $rs.SessionStateProxy.SetVariable('BudgetGb',        $budgetGb)
  $rs.SessionStateProxy.SetVariable('Workers',         $workers)
  $rs.SessionStateProxy.SetVariable('Hostname',        $hostname)
  $rs.SessionStateProxy.SetVariable('UiPort',          $uiPort)
  $rs.SessionStateProxy.SetVariable('EmbeddedVersion', $EmbeddedVersion)

  $ps = [powershell]::Create()
  $ps.Runspace = $rs
  $null = $ps.AddScript({
    function Log { param($Msg)
      $sync.Queue.Enqueue($Msg)
      $sync.Status = $Msg
    }
    try {
      # Step 1: Ensure VirtualBox -------------------------------------------
      Log 'Checking VirtualBox...'
      $sync.Progress = 5
      $VBoxManage = $null
      foreach ($p in @(
        "$env:ProgramFiles\Oracle\VirtualBox\VBoxManage.exe",
        "${env:ProgramFiles(x86)}\Oracle\VirtualBox\VBoxManage.exe"
      )) {
        if (Test-Path $p) { $VBoxManage = $p; break }
      }
      if (-not $VBoxManage) {
        $cmd = Get-Command VBoxManage -ErrorAction SilentlyContinue
        if ($cmd) { $VBoxManage = $cmd.Source }
      }

      if (-not $VBoxManage) {
        Log 'Installing VirtualBox (UAC prompt may appear)...'
        if (-not (Test-Path $VBoxInstaller)) {
          throw "VirtualBox installer not found: $VBoxInstaller"
        }
        $proc = Start-Process -FilePath $VBoxInstaller `
          -ArgumentList '--silent','--ignore-reboot' `
          -Verb RunAs -Wait -PassThru
        # Exit 3010 = success, reboot required (VBox typically does not need one)
        if ($proc.ExitCode -ne 0 -and $proc.ExitCode -ne 3010) {
          throw "VirtualBox installation failed (exit $($proc.ExitCode))."
        }
        foreach ($p in @(
          "$env:ProgramFiles\Oracle\VirtualBox\VBoxManage.exe",
          "${env:ProgramFiles(x86)}\Oracle\VirtualBox\VBoxManage.exe"
        )) {
          if (Test-Path $p) { $VBoxManage = $p; break }
        }
        if (-not $VBoxManage) {
          throw 'VirtualBox installed but VBoxManage.exe not found. Reboot and re-run setup.'
        }
        Log 'VirtualBox installed.'
      } else {
        Log "VirtualBox: $VBoxManage"
      }
      $sync.Progress = 15

      # Step 2: Import OVA --------------------------------------------------
      Log 'Importing Provisa VM...'
      if (-not (Test-Path $OvaPath)) { throw "OVA not found: $OvaPath" }
      $vmList = & $VBoxManage list vms 2>&1
      if ($vmList -notmatch '"Provisa"') {
        $importOut = & $VBoxManage import $OvaPath --vsys 0 --vmname 'Provisa' 2>&1
        $importOut | ForEach-Object { Log "  $_" }
        if ($LASTEXITCODE -ne 0) { throw "OVA import failed (exit $LASTEXITCODE). See log above." }
        Log 'VM imported.'
      } else {
        Log 'VM already imported.'
      }
      $sync.Progress = 35

      # Step 3: Configure VM ------------------------------------------------
      Log 'Configuring VM...'
      $vmRamMb = $BudgetGb * 1024
      & $VBoxManage modifyvm 'Provisa' --memory $vmRamMb 2>&1 | Out-Null
      $ApiPort = $UiPort + 1
      # Delete before re-adding to avoid duplicate-rule errors
      & $VBoxManage modifyvm 'Provisa' --natpf1 delete 'docker' 2>&1 | Out-Null
      & $VBoxManage modifyvm 'Provisa' --natpf1 "docker,tcp,,2375,,2375" 2>&1 | Out-Null
      & $VBoxManage modifyvm 'Provisa' --natpf1 delete 'ui' 2>&1 | Out-Null
      & $VBoxManage modifyvm 'Provisa' --natpf1 "ui,tcp,,$UiPort,,$UiPort" 2>&1 | Out-Null
      & $VBoxManage modifyvm 'Provisa' --natpf1 delete 'api' 2>&1 | Out-Null
      & $VBoxManage modifyvm 'Provisa' --natpf1 "api,tcp,,$ApiPort,,$ApiPort" 2>&1 | Out-Null
      Log "VM RAM: ${vmRamMb} MB, ports: docker=2375, ui=${UiPort}, api=${ApiPort}"
      $sync.Progress = 45

      # Step 4: Start VM ----------------------------------------------------
      Log 'Starting Provisa VM...'
      $vmInfo  = & $VBoxManage showvminfo 'Provisa' --machinereadable 2>&1
      $vmState = ($vmInfo | Select-String 'VMState=').ToString() -replace '.*="(.*)".*', '$1'
      if ($vmState -ne 'running') {
        $startOut = & $VBoxManage startvm 'Provisa' --type headless 2>&1
        $startOut | ForEach-Object { Log "  $_" }
        if ($LASTEXITCODE -ne 0) { throw "Failed to start VM (exit $LASTEXITCODE)." }
      } else {
        Log 'VM already running.'
      }
      $sync.Progress = 55

      # Step 5: Wait for Docker TCP -----------------------------------------
      Log 'Waiting for Docker to become ready...'
      $ready = $false
      for ($i = 0; $i -lt 120; $i++) {
        try {
          $tcp = New-Object System.Net.Sockets.TcpClient
          $tcp.Connect('localhost', 2375)
          $tcp.Close()
          $ready = $true
          break
        } catch {}
        Start-Sleep 3
        if ($i % 10 -eq 9) { Log "  Still waiting... ($([int](($i+1)*3))s elapsed)" }
        $sync.Progress = 55 + [int](($i / 120) * 30)
      }
      if (-not $ready) { throw 'Docker TCP did not respond within 360s.' }
      Log 'Docker ready.'
      $sync.Progress = 60

      # Step 6: Find or download core images zip, then load into Docker -----
      $sync.Status = 'Locating container images...'
      $CoreZip = $null
      foreach ($searchDir in @($ScriptDir, (Split-Path -Parent $ScriptDir))) {
        $found = Get-ChildItem -Path $searchDir -Filter 'provisa-core-images-amd64-*.zip' -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($found) { $CoreZip = $found.FullName; break }
      }
      if (-not $CoreZip) {
        if (-not $EmbeddedVersion) { throw 'VERSION file missing — cannot determine download URL for container images.' }
        $downloadUrl  = "https://github.com/kenstott/provisa/releases/download/$EmbeddedVersion/provisa-core-images-amd64-$EmbeddedVersion.zip"
        $localZipPath = Join-Path $env:TEMP "provisa-core-images-amd64-$EmbeddedVersion.zip"
        Log "Downloading container images ($EmbeddedVersion)..."
        $request = [System.Net.HttpWebRequest]::Create($downloadUrl)
        $request.UserAgent = 'Provisa-Installer/1.0'
        $response   = $request.GetResponse()
        $totalBytes = $response.ContentLength
        $respStream = $response.GetResponseStream()
        $fs         = [System.IO.File]::Create($localZipPath)
        $buf        = New-Object byte[] 65536
        $downloaded = [long]0
        while (($read = $respStream.Read($buf, 0, $buf.Length)) -gt 0) {
          $fs.Write($buf, 0, $read)
          $downloaded += $read
          if ($totalBytes -gt 0) {
            $pct = [int](($downloaded / $totalBytes) * 100)
            $sync.Progress = 60 + [int]($pct * 0.15)
            $sync.Status   = "Downloading images: $pct% ($([int]($downloaded/1MB)) / $([int]($totalBytes/1MB)) MB)"
          }
        }
        $fs.Close()
        $respStream.Close()
        $response.Close()
        Log "Download complete."
        $CoreZip = $localZipPath
      } else {
        Log "Found local images: $CoreZip"
      }
      $sync.Progress = 75

      Log 'Extracting images...'
      $ExtractDir = Join-Path $env:TEMP 'provisa-images-extract'
      if (Test-Path $ExtractDir) { Remove-Item -Recurse -Force $ExtractDir }
      New-Item -ItemType Directory -Path $ExtractDir -Force | Out-Null
      Add-Type -AssemblyName System.IO.Compression.FileSystem
      [System.IO.Compression.ZipFile]::ExtractToDirectory($CoreZip, $ExtractDir)
      $sync.Progress = 78

      $tarballs = Get-ChildItem -Path $ExtractDir -Filter '*.tar.gz' | Sort-Object Name
      $total    = $tarballs.Count
      $idx      = 0
      foreach ($tb in $tarballs) {
        $idx++
        Log "Loading image $idx/$total: $($tb.Name)..."
        $uri = 'http://127.0.0.1:2375/images/load'
        $fs  = [System.IO.File]::OpenRead($tb.FullName)
        try {
          $req             = [System.Net.WebRequest]::Create($uri)
          $req.Method      = 'POST'
          $req.ContentType = 'application/x-tar'
          $req.SendChunked = $true
          $reqStream = $req.GetRequestStream()
          $fs.CopyTo($reqStream)
          $reqStream.Close()
          $resp   = $req.GetResponse()
          $reader = New-Object System.IO.StreamReader($resp.GetResponseStream())
          $out    = $reader.ReadToEnd()
          $resp.Close()
          Log "  $($out.Trim())"
        } finally {
          $fs.Close()
        }
        $sync.Progress = 78 + [int](($idx / $total) * 7)
      }
      Log 'All images loaded.'
      Remove-Item -Recurse -Force $ExtractDir -ErrorAction SilentlyContinue
      $sync.Progress = 85

      # Step 7: Write config
      Log 'Writing config...'
      New-Item -ItemType Directory -Path $ProvisaHome -Force | Out-Null
      $cfgPath = Join-Path $ProvisaHome 'config.yaml'
      $ApiPort = $UiPort + 1
      $fwd = $ComposeDir -replace '\\', '/'
@"
# Provisa configuration - generated by Windows installer
project_dir: "$fwd"
hostname: $Hostname
ui_port: $UiPort
api_port: $ApiPort
auto_open_browser: true
runtime: virtualbox
vm_name: Provisa
docker_host: tcp://127.0.0.1:2375
federation_workers: $Workers
"@ | Set-Content -Path $cfgPath -Encoding UTF8
      $sync.Progress = 92

      # Step 7: Sentinel + Start Menu shortcut ------------------------------
      New-Item -ItemType File -Path $Sentinel -Force | Out-Null
      $smDir = Join-Path $env:APPDATA 'Microsoft\Windows\Start Menu\Programs\Provisa'
      New-Item -ItemType Directory -Path $smDir -Force | Out-Null
      $wsh  = New-Object -ComObject WScript.Shell
      $link = $wsh.CreateShortcut("$smDir\Start Provisa.lnk")
      $link.TargetPath       = 'powershell.exe'
      $link.Arguments        = "-NoExit -ExecutionPolicy Bypass -Command `"& '$ScriptDir\provisa.cmd' start`""
      $link.WorkingDirectory = $ScriptDir
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

$form.Add_Shown({ $form.Activate() })
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
