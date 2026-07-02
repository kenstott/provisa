#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$LogPath = Join-Path $env:TEMP 'provisa-first-launch.log'
Start-Transcript -Path $LogPath -Append -ErrorAction SilentlyContinue

try {

Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing
[System.Windows.Forms.Application]::EnableVisualStyles()

# Give the process its own taskbar identity so the taskbar button uses our
# window icon instead of inheriting powershell.exe's.
Add-Type -Namespace Win32 -Name Taskbar -MemberDefinition @'
[System.Runtime.InteropServices.DllImport("shell32.dll", SetLastError=true)]
public static extern int SetCurrentProcessExplicitAppUserModelID(string AppID);
'@
try { [Win32.Taskbar]::SetCurrentProcessExplicitAppUserModelID('Provisa.Setup') | Out-Null } catch {}


$ScriptDir    = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }
$VersionFile  = Join-Path $ScriptDir 'VERSION'
$EmbeddedVersion = if (Test-Path $VersionFile) { (Get-Content $VersionFile -Raw).Trim() } else { $null }
$ComposeDir   = Join-Path $ScriptDir 'compose'
$RedistDir    = Join-Path $ScriptDir 'redist'
$VBoxInstaller = Join-Path $RedistDir 'VirtualBox-setup.exe'
$OvaPath      = Join-Path $ScriptDir 'provisa-runtime.ova'
$ProvisaHome  = Join-Path $env:USERPROFILE '.provisa'
$Sentinel     = Join-Path $ProvisaHome '.first-launch-complete'

# A prior install is detected by the sentinel + config.yaml. Rather than exit,
# we open the same panel in "manage" mode (upgrade or reconfigure) with the
# current demo/obs/RAM pre-filled.
$ConfigYaml         = Join-Path $ProvisaHome 'config.yaml'
$ManageMode         = (Test-Path $Sentinel) -and (Test-Path $ConfigYaml)
$CurDemo            = $false
$CurObs             = $false
$InstalledVersion   = $null
if ($ManageMode) {
  foreach ($line in Get-Content $ConfigYaml) {
    if ($line -match '^\s*demo\s*:\s*(true|false)\s*$')    { $CurDemo          = ($Matches[1] -eq 'true') }
    if ($line -match '^\s*obs\s*:\s*(true|false)\s*$')     { $CurObs           = ($Matches[1] -eq 'true') }
    if ($line -match '^\s*version\s*:\s*"?([^"]+)"?\s*$')  { $InstalledVersion = $Matches[1].Trim() }
  }
}
$IsUpgrade = $ManageMode -and $EmbeddedVersion -and ($InstalledVersion -ne $EmbeddedVersion)

# -- VirtualBox detection -----------------------------------------------------
$VBoxFound = $false
$_vboxReg = Get-ItemProperty 'HKLM:\SOFTWARE\Oracle\VirtualBox' -ErrorAction SilentlyContinue
if ($_vboxReg -and $_vboxReg.InstallDir -and (Test-Path (Join-Path $_vboxReg.InstallDir 'VBoxManage.exe'))) {
  $VBoxFound = $true
}
if (-not $VBoxFound) {
  $cmd = Get-Command VBoxManage -ErrorAction SilentlyContinue
  if ($cmd) { $VBoxFound = $true }
}

# -- Docker Desktop detection -------------------------------------------------
# A reachable host Docker daemon lets Provisa skip the bundled VM entirely,
# sidestepping the Hyper-V / VirtualBox conflict.
function Test-DockerReady {
  $docker = Get-Command docker -ErrorAction SilentlyContinue
  if (-not $docker) { return $false }
  & $docker.Source version --format '{{.Server.Version}}' 2>$null | Out-Null
  return ($LASTEXITCODE -eq 0)
}
$DockerReady = Test-DockerReady

# The Docker daemon's real memory ceiling (the WSL2 VM's RAM, set in
# %USERPROFILE%\.wslconfig) - NOT system RAM. 0 if unknown.
function Get-DockerMemGb {
  $docker = Get-Command docker -ErrorAction SilentlyContinue
  if (-not $docker) { return 0 }
  $bytes = & $docker.Source info --format '{{.MemTotal}}' 2>$null
  if ($LASTEXITCODE -ne 0 -or -not ($bytes -match '^\d+$')) { return 0 }
  return [int][Math]::Floor([long]$bytes / 1GB)
}
$DockerMemGb = if ($DockerReady) { Get-DockerMemGb } else { 0 }

# -- Windows Hypervisor detection ---------------------------------------------
# Hyper-V / WSL2 claims VT-x; VirtualBox then runs the guest through a slow
# compat layer and the runtime often never boots. Docker Desktop REQUIRES it.
function Test-HyperVActive {
  if (Get-Process -Name 'vmmem','vmmemWSL','vmmemProxy' -ErrorAction SilentlyContinue) { return $true }
  $bcd = (& "$env:SystemRoot\System32\bcdedit.exe" /enum '{current}' 2>$null) -join ' '
  if ($bcd -match 'hypervisorlaunchtype\s+(\w+)') { return ($Matches[1] -ne 'Off') }
  return $false
}
$HyperVActive = Test-HyperVActive

# -- Recommended backend ------------------------------------------------------
# docker: use the running Docker Desktop.  virtualbox: bundled VM/OVA.
if ($DockerReady)          { $RecommendedBackend = 'docker' }
elseif (-not $HyperVActive){ $RecommendedBackend = 'virtualbox' }
else                       { $RecommendedBackend = 'virtualbox' }  # conflicted; UI warns

# -- RAM ceiling ---------------------------------------------------------------
# System RAM; the actual "All" ceiling is per-runtime (Docker's allocation vs
# the whole machine) and is applied when the radios are (re)built.
$totalBytes = (Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory
$totalGb    = [int][Math]::Floor($totalBytes / 1GB)

# -- Form ----------------------------------------------------------------------
$form = New-Object System.Windows.Forms.Form
$form.Text            = 'Provisa Setup'
$form.ClientSize      = New-Object System.Drawing.Size(600, 480)
$form.StartPosition   = 'CenterScreen'
$form.FormBorderStyle = 'FixedSingle'
$form.MaximizeBox     = $false
$form.BackColor       = [System.Drawing.Color]::White
$form.Font            = New-Object System.Drawing.Font('Segoe UI', 9)
$iconPath = Join-Path $ScriptDir 'provisa.ico'
if (Test-Path $iconPath) { $form.Icon = New-Object System.Drawing.Icon $iconPath }

# Header
$header           = New-Object System.Windows.Forms.Panel
$header.Dock      = 'Top'
$header.Height    = 72
$header.BackColor = [System.Drawing.Color]::FromArgb(31, 41, 51)
$form.Controls.Add($header)

# Brand mark
$logoPath = Join-Path $ScriptDir 'provisa-mark.png'
if (Test-Path $logoPath) {
  $logo          = New-Object System.Windows.Forms.PictureBox
  $logo.Image    = [System.Drawing.Image]::FromFile($logoPath)
  $logo.SizeMode = 'Zoom'
  $logo.Size     = New-Object System.Drawing.Size(48, 48)
  $logo.Location = New-Object System.Drawing.Point(20, 12)
  $header.Controls.Add($logo)
}

$lbTitle           = New-Object System.Windows.Forms.Label
$lbTitle.Text      = 'Provisa'
$lbTitle.Font      = New-Object System.Drawing.Font('Segoe UI', 22, [System.Drawing.FontStyle]::Bold)
$lbTitle.ForeColor = [System.Drawing.Color]::White
$lbTitle.AutoSize  = $true
$lbTitle.Location  = New-Object System.Drawing.Point(80, 14)
$header.Controls.Add($lbTitle)

$lbSub            = New-Object System.Windows.Forms.Label
$lbSub.Text       = if ($IsUpgrade) { "Upgrade $InstalledVersion -> $EmbeddedVersion" } elseif ($ManageMode) { 'Reconfigure' } else { 'First-time Setup' }
$lbSub.Font       = New-Object System.Drawing.Font('Segoe UI', 10)
$lbSub.ForeColor  = [System.Drawing.Color]::FromArgb(170, 170, 170)
$lbSub.AutoSize   = $true
$lbSub.Location   = New-Object System.Drawing.Point(196, 30)
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

$lbRamHdr          = New-Object System.Windows.Forms.Label
$lbRamHdr.AutoSize = $true
$lbRamHdr.Location = New-Object System.Drawing.Point(20, 18)
$lbRamHdr.Font     = New-Object System.Drawing.Font('Segoe UI', 10, [System.Drawing.FontStyle]::Bold)
$lbRamHdr.Text     = 'RAM Budget'
$pConfig.Controls.Add($lbRamHdr)

$lbRamQ          = New-Object System.Windows.Forms.Label
$lbRamQ.AutoSize = $false
$lbRamQ.Size     = New-Object System.Drawing.Size(560, 20)
$lbRamQ.Location = New-Object System.Drawing.Point(20, 44)
$lbRamQ.Text     = 'How much RAM can Provisa use?'
$pConfig.Controls.Add($lbRamQ)

# RAM radios are rebuilt whenever the ceiling changes: system RAM for the VM,
# Docker's allocated memory for Docker mode. $script:ramCeiling holds the
# current "All" value (used by Install).
$script:radios     = @()
$script:ramCeiling = $totalGb
function Set-RamOptions {
  param([int]$CeilingGb)
  if ($CeilingGb -lt 4) { $CeilingGb = 4 }
  if ($script:ramCeiling -eq $CeilingGb -and $script:radios.Count -gt 0) { return }
  $script:ramCeiling = $CeilingGb

  $prev = ($script:radios | Where-Object { $_.Checked } | Select-Object -First 1)
  $prevText = if ($prev) { $prev.Text } else { '16GB' }
  foreach ($rb in $script:radios) { $pConfig.Controls.Remove($rb); $rb.Dispose() }
  $script:radios = @()

  $rx = 20
  foreach ($v in @(4, 8, 16, 32, 64, 128)) {
    if ($v -lt $CeilingGb) {
      $rb          = New-Object System.Windows.Forms.RadioButton
      $rb.Text     = "${v}GB"
      $rb.AutoSize = $true
      $rb.Location = New-Object System.Drawing.Point($rx, 70)
      $pConfig.Controls.Add($rb)
      $script:radios += $rb
      $rx += [int]($rb.PreferredSize.Width) + 12
    }
  }
  $rbAll          = New-Object System.Windows.Forms.RadioButton
  $rbAll.Text     = "All (${CeilingGb}GB)"
  $rbAll.AutoSize = $true
  $rbAll.Location = New-Object System.Drawing.Point($rx, 70)
  $pConfig.Controls.Add($rbAll)
  $script:radios += $rbAll

  $keep = $script:radios | Where-Object { $_.Text -eq $prevText } | Select-Object -First 1
  if (-not $keep) { $keep = $script:radios | Where-Object { $_.Text -eq '16GB' } | Select-Object -First 1 }
  if (-not $keep) { $keep = $script:radios[0] }
  $keep.Checked = $true
}
Set-RamOptions $totalGb

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

$cbDemo          = New-Object System.Windows.Forms.CheckBox
$cbDemo.Text     = 'Demo dataset && services'
$cbDemo.AutoSize = $true
$cbDemo.Checked  = $CurDemo
$cbDemo.Location = New-Object System.Drawing.Point(160, 190)
$pConfig.Controls.Add($cbDemo)

$cbObs           = New-Object System.Windows.Forms.CheckBox
$cbObs.Text      = 'Observability stack (metrics/traces)'
$cbObs.AutoSize  = $true
$cbObs.Checked   = $CurObs
$cbObs.Location  = New-Object System.Drawing.Point(160, 214)
$pConfig.Controls.Add($cbObs)

Lbl 'Runtime' 20 238 $true

# Runtime radios live in their own panel so WinForms treats them as a separate
# radio group from the RAM-budget radios (radios group by immediate parent).
$pRuntime          = New-Object System.Windows.Forms.Panel
$pRuntime.Location  = New-Object System.Drawing.Point(18, 258)
$pRuntime.Size      = New-Object System.Drawing.Size(440, 28)
$pConfig.Controls.Add($pRuntime)

$rbDocker          = New-Object System.Windows.Forms.RadioButton
$rbDocker.Text     = 'Use existing Docker Desktop'
$rbDocker.AutoSize = $true
$rbDocker.Location = New-Object System.Drawing.Point(2, 4)
$pRuntime.Controls.Add($rbDocker)

$rbVBox          = New-Object System.Windows.Forms.RadioButton
$rbVBox.Text     = 'Bundled VM (VirtualBox)'
$rbVBox.AutoSize = $true
$rbVBox.Location = New-Object System.Drawing.Point(212, 4)
$pRuntime.Controls.Add($rbVBox)

$btnRecheck          = New-Object System.Windows.Forms.Button
$btnRecheck.Text     = 'Re-check'
$btnRecheck.Size     = New-Object System.Drawing.Size(90, 24)
$btnRecheck.Location = New-Object System.Drawing.Point(470, 260)
$btnRecheck.FlatStyle = 'System'
$pConfig.Controls.Add($btnRecheck)

$lbNotice          = New-Object System.Windows.Forms.Label
$lbNotice.AutoSize = $false
$lbNotice.Size     = New-Object System.Drawing.Size(560, 40)
$lbNotice.Location = New-Object System.Drawing.Point(20, 288)
$pConfig.Controls.Add($lbNotice)

# Escape-hatch actions: install Docker Desktop when it's absent, or disable
# Hyper-V so the bundled VM can boot. Visibility is set in Update-BackendUi.
$btnInstallDocker           = New-Object System.Windows.Forms.Button
$btnInstallDocker.Text      = 'Install Docker Desktop'
$btnInstallDocker.Size      = New-Object System.Drawing.Size(170, 26)
$btnInstallDocker.Location  = New-Object System.Drawing.Point(20, 330)
$btnInstallDocker.FlatStyle = 'System'
$btnInstallDocker.Visible   = $false
$pConfig.Controls.Add($btnInstallDocker)

$btnDisableHyperV           = New-Object System.Windows.Forms.Button
$btnDisableHyperV.Text      = 'Disable Hyper-V (reboot)'
$btnDisableHyperV.Size      = New-Object System.Drawing.Size(170, 26)
$btnDisableHyperV.Location  = New-Object System.Drawing.Point(200, 330)
$btnDisableHyperV.FlatStyle = 'System'
$btnDisableHyperV.Visible   = $false
$pConfig.Controls.Add($btnDisableHyperV)

$btnInstallDocker.Add_Click({
  # winget elevates the Docker Desktop MSI itself; run in a visible window so
  # the user sees progress, then they click Re-check when it's up.
  Start-Process powershell.exe -ArgumentList @(
    '-NoExit','-Command',
    'winget install -e --id Docker.DockerDesktop --accept-source-agreements --accept-package-agreements'
  )
  [System.Windows.Forms.MessageBox]::Show($form,
    "Docker Desktop is installing in a separate window.`r`n`r`n" +
    "When it finishes and Docker Desktop is running, click Re-check to select the Docker runtime.",
    'Provisa Setup', 'OK', 'Information') | Out-Null
})

$btnDisableHyperV.Add_Click({
  $c = [System.Windows.Forms.MessageBox]::Show($form,
    "This disables Hyper-V / WSL2 / Virtual Machine Platform and reboots.`r`n`r`n" +
    "Docker Desktop will not run until you re-enable them (enable-hyperv.ps1).`r`n`r`nContinue?",
    'Provisa Setup', 'YesNo', 'Warning')
  if ($c -ne 'Yes') { return }
  $disable = 'bcdedit /set hypervisorlaunchtype off; ' +
             'dism.exe /Online /Disable-Feature /FeatureName:Microsoft-Hyper-V-All /NoRestart; ' +
             'dism.exe /Online /Disable-Feature /FeatureName:VirtualMachinePlatform /NoRestart; ' +
             'shutdown /r /t 10 /c "Provisa: rebooting to disable Hyper-V"'
  Start-Process powershell.exe -ArgumentList @('-NoProfile','-Command',$disable) -Verb RunAs
})

# Picks the initial radio from detection (setting Checked fires the handler).
function Set-DefaultBackend {
  if ($DockerReady) { $rbDocker.Checked = $true } else { $rbVBox.Checked = $true }
}

# Refreshes notice, RAM-question label, and button visibility for the CURRENT
# selection - so the text tracks the radio, not just the last detection.
function Update-BackendUi {
  $rbDocker.Enabled = $DockerReady
  # Offer the Docker install whenever no daemon is reachable.
  $btnInstallDocker.Visible = (-not $DockerReady)
  # Offer the Hyper-V disable only when the VM path is chosen and Hyper-V is on.
  $btnDisableHyperV.Visible = ($rbVBox.Checked -and $HyperVActive)

  # RAM ceiling: Docker's allocated memory in Docker mode, whole machine for
  # the VM. Rebuilds the radios only when the ceiling actually changes.
  if ($rbDocker.Checked -and $DockerMemGb -gt 0) { Set-RamOptions $DockerMemGb }
  else                                           { Set-RamOptions $totalGb }

  if ($rbDocker.Checked) {
    $lbNotice.ForeColor = [System.Drawing.Color]::FromArgb(0, 140, 0)
    $lbNotice.Text = 'Docker runtime: uses Docker Desktop, which requires Hyper-V / WSL2 (currently enabled). No VM.'
  } else {
    if ($HyperVActive) {
      $lbNotice.ForeColor = [System.Drawing.Color]::FromArgb(190, 90, 0)
      $lbNotice.Text = 'Bundled VM (VirtualBox) needs Hyper-V / WSL2 OFF; it is currently ON, so the VM may not boot. ' +
                       'Disabling Hyper-V (below) will stop Docker Desktop from working.'
    } else {
      $lbNotice.ForeColor = [System.Drawing.Color]::FromArgb(90, 90, 90)
      $lbNotice.Text = if ($VBoxFound) { 'Bundled VM (VirtualBox): Hyper-V is off, so the VM gets native VT-x. (VirtualBox installed.)' }
                       else            { 'Bundled VM (VirtualBox): Hyper-V is off, so the VM gets native VT-x. (VirtualBox will be installed.)' }
    }
  }
}

# Re-evaluate the selected backend's text whenever the user toggles runtime.
$rbDocker.Add_CheckedChanged({ Update-BackendUi })

$btnRecheck.Add_Click({
  $script:DockerReady  = Test-DockerReady
  $script:HyperVActive = Test-HyperVActive
  $script:DockerMemGb  = if ($script:DockerReady) { Get-DockerMemGb } else { 0 }
  Set-DefaultBackend
  Update-BackendUi
})

Set-DefaultBackend
Update-BackendUi

$btnInstall            = New-Object System.Windows.Forms.Button
$btnInstall.Text       = if ($ManageMode) { 'Apply' } else { 'Install' }
$btnInstall.Font       = New-Object System.Drawing.Font('Segoe UI', 10, [System.Drawing.FontStyle]::Bold)
$btnInstall.Size       = New-Object System.Drawing.Size(110, 36)
$btnInstall.Location   = New-Object System.Drawing.Point(470, 352)
$btnInstall.BackColor  = [System.Drawing.Color]::FromArgb(16, 185, 129)
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
$rtb.BackColor     = [System.Drawing.Color]::FromArgb(13, 20, 26)
$rtb.ForeColor     = [System.Drawing.Color]::FromArgb(204, 204, 204)
$rtb.BorderStyle   = 'None'
$pProg.Controls.Add($rtb)

$btnFinish            = New-Object System.Windows.Forms.Button
$btnFinish.Text       = 'Finish'
$btnFinish.Font       = New-Object System.Drawing.Font('Segoe UI', 10, [System.Drawing.FontStyle]::Bold)
$btnFinish.Size       = New-Object System.Drawing.Size(110, 36)
$btnFinish.Location   = New-Object System.Drawing.Point(470, 358)
$btnFinish.BackColor  = [System.Drawing.Color]::FromArgb(16, 185, 129)
$btnFinish.ForeColor  = [System.Drawing.Color]::White
$btnFinish.FlatStyle  = 'Flat'
$btnFinish.FlatAppearance.BorderSize = 0
$btnFinish.Enabled    = $false
$pProg.Controls.Add($btnFinish)

$btnFinish.Add_Click({ $form.Close() })

$btnReboot            = New-Object System.Windows.Forms.Button
$btnReboot.Text       = 'Reboot Now'
$btnReboot.Font       = New-Object System.Drawing.Font('Segoe UI', 10, [System.Drawing.FontStyle]::Bold)
$btnReboot.Size       = New-Object System.Drawing.Size(130, 36)
$btnReboot.Location   = New-Object System.Drawing.Point(330, 358)
$btnReboot.BackColor  = [System.Drawing.Color]::FromArgb(16, 185, 129)
$btnReboot.ForeColor  = [System.Drawing.Color]::White
$btnReboot.FlatStyle  = 'Flat'
$btnReboot.FlatAppearance.BorderSize = 0
$btnReboot.Visible    = $false
$pProg.Controls.Add($btnReboot)

$btnReboot.Add_Click({
  $runOnce = 'HKCU:\SOFTWARE\Microsoft\Windows\CurrentVersion\RunOnce'
  $script  = "`"$ScriptDir\first-launch-gui.ps1`""
  Set-ItemProperty -Path $runOnce -Name 'ProvisaSetup' `
    -Value "powershell.exe -ExecutionPolicy Bypass -WindowStyle Normal -File $script"
  shutdown.exe /r /t 10 /c 'Provisa: Rebooting to complete setup'
  $form.Close()
})

# -- Synchronized state -------------------------------------------------------
$sync = [hashtable]::Synchronized(@{
  Queue       = [System.Collections.Queue]::Synchronized((New-Object System.Collections.Queue))
  Progress    = 0
  Status      = 'Starting...'
  Done        = $false
  Error       = $null
  NeedsReboot = $false
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
    if ($sync.NeedsReboot) {
      $lbStatus.Text     = 'Reboot required to complete setup.'
      $btnFinish.Text    = 'Close'
      $btnFinish.Enabled = $true
      $btnReboot.Visible = $true
    } elseif ($sync.Error) {
      $rtb.SelectionStart  = $rtb.TextLength
      $rtb.SelectionLength = 0
      $rtb.SelectionColor  = [System.Drawing.Color]::FromArgb(255, 80, 80)
      $rtb.AppendText("`nERROR: $($sync.Error)`n")
      $lbStatus.Text  = 'Setup failed. See log above.'
      $btnFinish.Text = 'Close'
      $btnFinish.Enabled = $true
    } else {
      $lbStatus.Text  = 'Setup complete!'
      $btnFinish.Text = 'Finish'
      $btnFinish.Enabled = $true
    }
  }
})

# -- Install click ------------------------------------------------------------
$btnInstall.Add_Click({
  $sel = $script:radios | Where-Object { $_.Checked } | Select-Object -First 1
  if (-not $sel) { $sel = $script:radios[0] }
  $ramText = $sel.Text
  if ($ramText -like 'All*') { $budgetGb = $script:ramCeiling }
  else                       { $budgetGb = [int]($ramText -replace 'GB', '') }

  $backend = if ($rbDocker.Checked) { 'docker' } else { 'virtualbox' }
  $demo    = [bool]$cbDemo.Checked
  $obs     = [bool]$cbObs.Checked

  # Docker Desktop mode is single-node dev/demo: coordinator-only, no workers.
  # VirtualBox mode sizes worker count from the VM's RAM budget.
  if ($backend -eq 'docker') {
    $workers = 0
  } else {
    if ($budgetGb -ge 96)    { $workers = 4 }
    elseif ($budgetGb -ge 48){ $workers = 2 }
    elseif ($budgetGb -ge 24){ $workers = 1 }
    else                     { $workers = 0 }
  }

  $hostname = $tbHost.Text.Trim()
  if ([string]::IsNullOrEmpty($hostname)) { $hostname = 'localhost' }
  $uiPort = [int]$nudPort.Value

  # Warn before committing to the VM path on a machine where Hyper-V will
  # cripple it.
  if ($backend -eq 'virtualbox' -and $HyperVActive) {
    $resp = [System.Windows.Forms.MessageBox]::Show(
      $form,
      "Hyper-V / WSL2 is active. The bundled VM usually fails to boot in this state.`r`n`r`n" +
      "Recommended: click No, start Docker Desktop, then Re-check to use the Docker runtime instead.`r`n`r`n" +
      "Continue with the VM anyway?",
      'Provisa Setup', 'YesNo', 'Warning')
    if ($resp -ne 'Yes') { return }
  }

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
  $rs.SessionStateProxy.SetVariable('Backend',         $backend)
  $rs.SessionStateProxy.SetVariable('Demo',            $demo)
  $rs.SessionStateProxy.SetVariable('Obs',             $obs)
  $rs.SessionStateProxy.SetVariable('ManageMode',      $ManageMode)

  $ps = [powershell]::Create()
  $ps.Runspace = $rs
  $null = $ps.AddScript({
    function Log { param($Msg)
      $sync.Queue.Enqueue($Msg)
      $sync.Status = $Msg
    }
    try {
      # Remedy text shared between the early warning and the /_ping timeout.
      $HyperVRemedy = @(
        'The Windows Hypervisor (Hyper-V / WSL2 / Virtual Machine Platform) is active.',
        'It claims hardware virtualization (VT-x), forcing the Federation Engine to run',
        'the guest through a slow compatibility layer - the runtime often never finishes',
        'booting. Disable it in an ELEVATED PowerShell, then reboot:',
        '',
        '    bcdedit /set hypervisorlaunchtype off',
        '    dism.exe /Online /Disable-Feature:Microsoft-Hyper-V-All /NoRestart',
        '    dism.exe /Online /Disable-Feature:VirtualMachinePlatform /NoRestart',
        '    shutdown /r /t 0',
        '',
        '(Re-enable later with: bcdedit /set hypervisorlaunchtype auto)'
      )
      function Test-HyperVActive {
        # A running vmmem/vmmemWSL process means the hypervisor is live now.
        if (Get-Process -Name 'vmmem','vmmemWSL','vmmemProxy' -ErrorAction SilentlyContinue) { return $true }
        # Otherwise consult the boot configuration.
        $bcd = (& "$env:SystemRoot\System32\bcdedit.exe" /enum '{current}' 2>$null) -join ' '
        if ($bcd -match 'hypervisorlaunchtype\s+(\w+)') {
          return ($Matches[1] -ne 'Off')
        }
        return $false
      }

      # $DockerApiBase is the Docker Engine endpoint the image-load and
      # config-write steps talk to. VirtualBox backend forwards it on 2375;
      # the Docker backend uses the docker CLI instead (curl can't reach the
      # Windows named pipe), so it stays $null there.
      $DockerApiBase = $null
      $UseDockerCli  = ($Backend -eq 'docker')

      if ($Backend -eq 'docker') {
        # ---- Docker backend: reuse the host's Docker Desktop --------------
        Log 'Using existing Docker Desktop - no VM required.'
        $dockerCmd = Get-Command docker -ErrorAction SilentlyContinue
        if (-not $dockerCmd) { throw 'Docker CLI not found. Start Docker Desktop and rerun setup.' }
        $DockerCli = $dockerCmd.Source
        & $DockerCli version --format '{{.Server.Version}}' 2>$null | Out-Null
        if ($LASTEXITCODE -ne 0) { throw 'Docker daemon not responding. Is Docker Desktop running?' }
        $srvVer = (& $DockerCli version --format '{{.Server.Version}}' 2>$null)
        Log "Docker Engine $srvVer ready."
        $sync.Progress = 55
      }
      else {

      # Step 0: Detect Windows Hypervisor conflict --------------------------
      if (Test-HyperVActive) {
        Log 'WARNING: Windows Hypervisor detected - the runtime may fail to boot.'
        $HyperVRemedy | ForEach-Object { Log "  $_" }
      }

      # Step 1: Ensure Federation Engine -------------------------------------------
      Log 'Checking Federation Engine...'
      $sync.Progress = 5
      $VBoxManage = $null
      function Find-VBoxManage {
        foreach ($regPath in @(
          'HKLM:\SOFTWARE\Oracle\VirtualBox',
          'HKLM:\SOFTWARE\WOW6432Node\Oracle\VirtualBox'
        )) {
          $r = Get-ItemProperty $regPath -ErrorAction SilentlyContinue
          if ($r -and $r.InstallDir) {
            $c = Join-Path $r.InstallDir 'VBoxManage.exe'
            if (Test-Path $c) { return $c }
          }
        }
        $pf    = [System.Environment]::GetEnvironmentVariable('ProgramFiles',      'Machine')
        $pfx86 = [System.Environment]::GetEnvironmentVariable('ProgramFiles(x86)', 'Machine')
        foreach ($p in @(
          'C:\Program Files\Oracle\VirtualBox\VBoxManage.exe',
          'C:\Program Files (x86)\Oracle\VirtualBox\VBoxManage.exe',
          "$pf\Oracle\VirtualBox\VBoxManage.exe",
          "$pfx86\Oracle\VirtualBox\VBoxManage.exe"
        )) {
          if ($p -and (Test-Path $p)) { return $p }
        }
        $cmd = Get-Command VBoxManage -ErrorAction SilentlyContinue
        if ($cmd) { return $cmd.Source }
        return $null
      }

      $VBoxManage = Find-VBoxManage
      if (-not $VBoxManage) {
        Log 'Installing Federation Engine (UAC prompt may appear)...'
        if (-not (Test-Path $VBoxInstaller)) {
          throw "Federation Engine installer not found: $VBoxInstaller"
        }
        $proc = Start-Process -FilePath $VBoxInstaller `
          -ArgumentList '--silent','--ignore-reboot' `
          -Verb RunAs -Wait -PassThru
        $VBoxManage = Find-VBoxManage
        if (-not $VBoxManage) {
          throw "Federation Engine installation failed (exit $($proc.ExitCode))."
        }
        Log 'Federation Engine installed.'
      } else {
        Log 'Federation Engine: ready.'
      }

      # Check federation driver is loaded
      $drvState = (sc.exe query vboxsup 2>&1) -join ' '
      if ($drvState -match 'STATE\s+:\s+1\s+STOPPED') {
        Log 'Federation driver not loaded - reboot required.'
        $sync.NeedsReboot = $true
        $sync.Done        = $true
        return
      }
      $sync.Progress = 15

      # Step 2: Import OVA --------------------------------------------------
      Log 'Provisioning runtime environment...'
      if (-not (Test-Path $OvaPath)) { throw "Runtime package not found: $OvaPath" }
      & $VBoxManage showvminfo 'Provisa' --machinereadable 2>&1 | Out-Null
      $vmRegistered = ($LASTEXITCODE -eq 0)
      if (-not $vmRegistered) {
        # Clean up any stale registration before importing
        & $VBoxManage unregistervm 'Provisa' --delete 2>&1 | Out-Null
        $importOut = & $VBoxManage import $OvaPath --vsys 0 --vmname 'Provisa' 2>&1
        $importOut | ForEach-Object { Log "  $_" }
        if ($LASTEXITCODE -ne 0) { throw "Runtime environment provisioning failed (exit $LASTEXITCODE). See log above." }
        Log 'Runtime environment provisioned.'
      } else {
        Log 'Runtime environment already provisioned.'
      }
      $sync.Progress = 35

      # Step 3: Configure VM ------------------------------------------------
      Log 'Configuring runtime environment...'
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
      Log "Resources: ${vmRamMb} MB RAM | UI: ${UiPort} | API: ${ApiPort}"
      $sync.Progress = 45

      # Step 4: Start VM ----------------------------------------------------
      Log 'Starting Provisa...'
      $vmInfo       = & $VBoxManage showvminfo 'Provisa' --machinereadable 2>&1
      $vmStateMatch = $vmInfo | Select-String 'VMState=' | Select-Object -First 1
      $vmState      = if ($vmStateMatch) { $vmStateMatch.Line -replace '.*="(.*)".*','$1' } else { 'poweroff' }
      if ($vmState -ne 'running') {
        $startOut = & $VBoxManage startvm 'Provisa' --type headless 2>&1
        $startOut | ForEach-Object { Log "  $_" }
        if ($LASTEXITCODE -ne 0) { throw "Failed to start runtime environment (exit $LASTEXITCODE)." }
      } else {
        Log 'Runtime environment already running.'
      }
      $sync.Progress = 55

      # Step 5: Wait for Docker daemon to actually serve requests -----------
      # A raw TCP connect is a false positive: VirtualBox's NAT port-forward
      # proxy accepts the handshake on the host side before dockerd inside the
      # guest is listening. Gate on a real HTTP 200 from /_ping instead.
      Log 'Waiting for Coordination Engine to become ready...'
      $curlExe = Join-Path $env:SystemRoot 'System32\curl.exe'
      if (-not (Test-Path $curlExe)) { throw 'curl.exe not found - Windows 10 version 1803 or later required.' }
      $ready     = $false
      $okStreak  = 0
      for ($i = 0; $i -lt 120; $i++) {
        try {
          $ping = & $curlExe --silent --max-time 5 'http://127.0.0.1:2375/_ping' 2>$null
          if ($LASTEXITCODE -eq 0 -and $ping -match 'OK') {
            # Require two consecutive successes so a flapping NAT proxy
            # doesn't read as ready.
            $okStreak++
            if ($okStreak -ge 2) { $ready = $true; break }
          } else {
            $okStreak = 0
          }
        } catch { $okStreak = 0 }
        Start-Sleep 3
        if ($i % 10 -eq 9) { Log "  Still waiting... ($([int](($i+1)*3))s elapsed)" }
        $sync.Progress = 55 + [int](($i / 120) * 30)
      }
      if (-not $ready) {
        if (Test-HyperVActive) {
          Log 'Coordination Engine did not respond within 360s.'
          $HyperVRemedy | ForEach-Object { Log "  $_" }
          throw 'Coordination Engine did not respond to /_ping within 360s - Windows Hypervisor conflict (see remedy above).'
        }
        throw 'Coordination Engine did not respond to /_ping within 360s.'
      }
      Log 'Coordination Engine ready.'
      $DockerApiBase = 'http://127.0.0.1:2375'
      $sync.Progress = 60

      } # end virtualbox backend

      # Step 6: Find or download core images zip, then load into Docker -----
      $sync.Status = 'Locating service packages...'
      $CoreZip = $null
      foreach ($searchDir in @($ScriptDir, (Split-Path -Parent $ScriptDir))) {
        $found = Get-ChildItem -Path $searchDir -Filter 'provisa-core-images-amd64-*.zip' -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($found) { $CoreZip = $found.FullName; break }
      }
      if (-not $CoreZip) {
        if (-not $EmbeddedVersion) { throw 'VERSION file missing - cannot determine download URL.' }
        $downloadUrl  = "https://github.com/kenstott/provisa/releases/download/$EmbeddedVersion/provisa-core-images-amd64-$EmbeddedVersion.zip"
        $localZipPath = Join-Path $env:TEMP "provisa-core-images-amd64-$EmbeddedVersion.zip"
        Log "Downloading Core Services ($EmbeddedVersion)..."
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
            $sync.Status   = "Downloading Core Services: $pct% ($([int]($downloaded/1MB)) / $([int]($totalBytes/1MB)) MB)"
          }
        }
        $fs.Close()
        $respStream.Close()
        $response.Close()
        Log "Download complete."
        $CoreZip = $localZipPath
      } else {
        Log "Found bundled service packages."
      }
      $sync.Progress = 75

      Log 'Preparing service packages...'
      $ExtractDir = Join-Path $env:TEMP 'provisa-images-extract'
      if (Test-Path $ExtractDir) { Remove-Item -Recurse -Force $ExtractDir }
      New-Item -ItemType Directory -Path $ExtractDir -Force | Out-Null
      Add-Type -AssemblyName System.IO.Compression.FileSystem
      [System.IO.Compression.ZipFile]::ExtractToDirectory($CoreZip, $ExtractDir)
      $sync.Progress = 78

      # tar.exe reads image manifests; curl.exe drives the VirtualBox Docker API.
      $tarExe = Join-Path $env:SystemRoot 'System32\tar.exe'
      if (-not (Test-Path $tarExe)) { throw 'tar.exe not found - Windows 10 version 1803 or later required.' }
      $curlExe = Join-Path $env:SystemRoot 'System32\curl.exe'
      if (-not $UseDockerCli -and -not (Test-Path $curlExe)) {
        throw 'curl.exe not found - Windows 10 version 1803 or later required.'
      }

      # Returns the RepoTags an image tarball is expected to load (from its
      # embedded manifest.json), or @() if it can't be determined.
      function Get-ExpectedTags { param($TarPath)
        try {
          $json = (& $tarExe -xzOf $TarPath 'manifest.json' 2>$null) -join "`n"
          if (-not $json) { return @() }
          $m = $json | ConvertFrom-Json
          return @($m | ForEach-Object { $_.RepoTags } | Where-Object { $_ })
        } catch { return @() }
      }

      # True only when every tag in $Tags is present in the daemon's image list.
      # Exact tag match, not a count delta: counts miss shared-layer/retag loads
      # and /images/json hides dangling images by default.
      function Test-TagsPresent { param($Tags)
        if (-not $Tags -or $Tags.Count -eq 0) { return $false }
        try {
          $present = (& $curlExe --silent --max-time 15 "$DockerApiBase/images/json" | ConvertFrom-Json) |
                     ForEach-Object { $_.RepoTags } | Where-Object { $_ }
        } catch { return $false }
        foreach ($t in $Tags) { if ($present -notcontains $t) { return $false } }
        return $true
      }

      # Find (next to the installer) or download a release image tarball, then
      # load every *.tar.gz inside it into the daemon. Used for demo + obs.
      function Install-ReleaseImages { param($Pattern, $Asset, $Label)
        $tar = $null
        foreach ($searchDir in @($ScriptDir, (Split-Path -Parent $ScriptDir))) {
          $f = Get-ChildItem -Path $searchDir -Filter $Pattern -ErrorAction SilentlyContinue | Select-Object -First 1
          if ($f) { $tar = $f.FullName; break }
        }
        if (-not $tar -and $EmbeddedVersion) {
          $url = "https://github.com/kenstott/provisa/releases/download/$EmbeddedVersion/$Asset"
          $dst = Join-Path $env:TEMP $Asset
          Log "Downloading $Label ($EmbeddedVersion)..."
          $req = [System.Net.HttpWebRequest]::Create($url); $req.UserAgent = 'Provisa-Installer/1.0'
          $resp = $req.GetResponse(); $ins = $resp.GetResponseStream(); $fs = [System.IO.File]::Create($dst)
          $buf = New-Object byte[] 65536
          while (($r = $ins.Read($buf, 0, $buf.Length)) -gt 0) { $fs.Write($buf, 0, $r) }
          $fs.Close(); $ins.Close(); $resp.Close(); $tar = $dst
        }
        if (-not $tar) { Log "WARNING: $Label images not found - services may not start."; return }
        Log "Installing $Label..."
        $ex = Join-Path $env:TEMP 'provisa-img-extract'
        if (Test-Path $ex) { Remove-Item -Recurse -Force $ex }
        New-Item -ItemType Directory -Path $ex -Force | Out-Null
        & $tarExe -xzf $tar -C $ex 2>&1 | ForEach-Object { }
        foreach ($img in (Get-ChildItem -Path $ex -Filter '*.tar.gz')) {
          if ($UseDockerCli) { & $DockerCli load -i $img.FullName 2>&1 | ForEach-Object { } }
          else { & $curlExe --silent --show-error --max-time 3600 -X POST "$DockerApiBase/images/load" -H 'Content-Type: application/x-tar' --data-binary "@$($img.FullName)" | Out-Null }
        }
        Remove-Item -Recurse -Force $ex -ErrorAction SilentlyContinue
        Log "$Label installed."
      }

      # Comment (obs off) or uncomment (obs on) the OTel lines in the staged
      # Trino config. The otel-collector only exists in the obs stack; without
      # it Trino's OTel export throws and fails even SELECT 1.
      function Set-TrinoOtel { param([bool]$Enabled)
        $trinoEtc = Join-Path $ComposeDir 'trino\etc'
        function _toggleOtel { param($Path, $Pat, $On)
          if (-not (Test-Path $Path)) { return }
          $out = foreach ($line in (Get-Content $Path)) {
            if ($line -match "^\s*#?\s*($Pat)") {
              $bare = $line -replace '^\s*#\s*', ''
              if ($On) { $bare } else { "#$bare" }
            } else { $line }
          }
          $out | Set-Content -Path $Path -Encoding ASCII
        }
        $tp = 'tracing\.enabled|otel\.exporter\.endpoint'
        _toggleOtel (Join-Path $trinoEtc 'config.properties')        $tp $Enabled
        _toggleOtel (Join-Path $trinoEtc 'worker\config.properties') $tp $Enabled
        _toggleOtel (Join-Path $trinoEtc 'jvm.config') '-javaagent:.*opentelemetry|-Dotel\.' $Enabled
      }

      # Fault-tolerant execution (retry-policy=TASK) spools every query to the
      # exchange volume, which the trino user can't write on a single node -
      # SELECT 1 dies with AccessDeniedException. FTE only makes sense with
      # workers, so use TASK when workers>0, NONE otherwise.
      function Set-TrinoFte { param([bool]$Enabled)
        $val = if ($Enabled) { 'TASK' } else { 'NONE' }
        $trinoEtc = Join-Path $ComposeDir 'trino\etc'
        foreach ($rel in 'config.properties', 'worker\config.properties') {
          $cf = Join-Path $trinoEtc $rel
          if (-not (Test-Path $cf)) { continue }
          $seen = $false
          $out = foreach ($line in (Get-Content $cf)) {
            if ($line -match '^\s*retry-policy\s*=') { $seen = $true; "retry-policy=$val" } else { $line }
          }
          if (-not $seen) { $out += "retry-policy=$val" }
          $out | Set-Content -Path $cf -Encoding ASCII
        }
      }

      $tarballs = Get-ChildItem -Path $ExtractDir -Filter '*.tar.gz' | Sort-Object Name
      $total    = $tarballs.Count
      $idx      = 0
      foreach ($tb in $tarballs) {
        $idx++
        Log "Installing service package $idx of ${total}..."
        $expectedTags = Get-ExpectedTags $tb.FullName

        if ($UseDockerCli) {
          # Docker backend: load straight through the CLI (no NAT flakiness).
          $out = & $DockerCli load -i $tb.FullName 2>&1
          if ($LASTEXITCODE -ne 0) { throw "Failed to install service package $($tb.Name): $out" }
          Log "  Package installed."
          $sync.Progress = 78 + [int](($idx / $total) * 7)
          continue
        }

        $respFile = Join-Path $env:TEMP "provisa-load-$idx.txt"
        Remove-Item $respFile -ErrorAction SilentlyContinue
        $out = & $curlExe --silent --show-error --max-time 3600 `
          -X POST 'http://127.0.0.1:2375/images/load' `
          -H 'Content-Type: application/x-tar' `
          --output $respFile `
          --data-binary "@$($tb.FullName)" 2>&1
        $curlExit = $LASTEXITCODE
        $respBody = if (Test-Path $respFile) { Get-Content $respFile -Raw -ErrorAction SilentlyContinue } else { '' }
        Remove-Item $respFile -ErrorAction SilentlyContinue

        if ($curlExit -eq 0 -and $respBody -match '"stream"\s*:\s*"Loaded image') {
          # Clean load with explicit daemon confirmation.
          Log "  Package installed."
        } elseif (Test-TagsPresent $expectedTags) {
          # Connection reset (exit 56) or truncated response, but the daemon
          # finished loading - confirmed by exact tag presence.
          Log "  Package installed."
        } elseif ($curlExit -eq 56 -or $curlExit -eq 0) {
          # Data sent but not yet confirmed - poll for the expected tags with a
          # visible heartbeat so the UI isn't frozen.
          if ($expectedTags.Count -eq 0) {
            throw "Service package $($tb.Name) could not be confirmed (no manifest tags; curl exit $curlExit): $respBody"
          }
          Log "  Package transmitted - waiting for processing..."
          $waited = 0
          $loaded = $false
          while ($waited -lt 300 -and -not $loaded) {
            Start-Sleep 5; $waited += 5
            $sync.Status = "Installing service package $idx of ${total} - loading... (${waited}s)"
            if (Test-TagsPresent $expectedTags) { $loaded = $true }
          }
          if (-not $loaded) { throw "Service package $($tb.Name) did not confirm in Docker after 300s." }
          Log "  Package installed."
        } else {
          throw "Failed to install service package $($tb.Name) (curl exit $curlExit): $out"
        }
        $sync.Progress = 78 + [int](($idx / $total) * 7)
      }
      Log 'All service packages installed.'
      Remove-Item -Recurse -Force $ExtractDir -ErrorAction SilentlyContinue
      $sync.Progress = 85

      # Step 6b: Trino plugins ---------------------------------------------
      # The trino service bind-mounts ./trino/plugins/trino-*; an empty plugin
      # dir makes Trino fail to boot. Plugins (925 MB) ship as a separate
      # release asset - find it next to the installer or download it, then
      # extract into the compose dir.
      $PluginsDir = Join-Path $ComposeDir 'trino\plugins'
      $pluginsPresent = (Test-Path (Join-Path $PluginsDir 'trino-file'))
      if (-not $pluginsPresent) {
        $sync.Status = 'Locating query engine plugins...'
        $PluginsTar = $null
        foreach ($searchDir in @($ScriptDir, (Split-Path -Parent $ScriptDir))) {
          $found = Get-ChildItem -Path $searchDir -Filter 'provisa-trino-plugins-*.tar.gz' -ErrorAction SilentlyContinue | Select-Object -First 1
          if ($found) { $PluginsTar = $found.FullName; break }
        }
        if (-not $PluginsTar) {
          if (-not $EmbeddedVersion) { throw 'VERSION file missing - cannot download query engine plugins.' }
          $pUrl  = "https://github.com/kenstott/provisa/releases/download/$EmbeddedVersion/provisa-trino-plugins-$EmbeddedVersion.tar.gz"
          $pDest = Join-Path $env:TEMP "provisa-trino-plugins-$EmbeddedVersion.tar.gz"
          Log "Downloading query engine plugins ($EmbeddedVersion)..."
          $preq = [System.Net.HttpWebRequest]::Create($pUrl)
          $preq.UserAgent = 'Provisa-Installer/1.0'
          $presp = $preq.GetResponse()
          $ptot  = $presp.ContentLength
          $pin   = $presp.GetResponseStream()
          $pfs   = [System.IO.File]::Create($pDest)
          $pbuf  = New-Object byte[] 65536
          $pdl   = [long]0
          while (($pread = $pin.Read($pbuf, 0, $pbuf.Length)) -gt 0) {
            $pfs.Write($pbuf, 0, $pread)
            $pdl += $pread
            if ($ptot -gt 0) {
              $ppct = [int](($pdl / $ptot) * 100)
              $sync.Status = "Downloading query engine plugins: $ppct% ($([int]($pdl/1MB)) / $([int]($ptot/1MB)) MB)"
            }
          }
          $pfs.Close(); $pin.Close(); $presp.Close()
          Log 'Plugin download complete.'
          $PluginsTar = $pDest
        } else {
          Log 'Found bundled query engine plugins.'
        }
        Log 'Installing query engine plugins...'
        New-Item -ItemType Directory -Path $PluginsDir -Force | Out-Null
        # Tarball expands to trino-file/, trino-sharepoint/, trino-splunk/ directly.
        & $tarExe -xzf $PluginsTar -C $PluginsDir 2>&1 | ForEach-Object { }
        if ($LASTEXITCODE -ne 0) { throw "Failed to extract query engine plugins (tar exit $LASTEXITCODE)." }
        if (-not (Test-Path (Join-Path $PluginsDir 'trino-file'))) {
          throw 'Query engine plugins did not extract correctly (trino-file missing).'
        }
        Log 'Query engine plugins installed.'
      } else {
        Log 'Query engine plugins already present.'
      }
      $sync.Progress = 90

      # Step 6c: Demo + Observability overlays --------------------------------
      $CatalogDir = Join-Path $ComposeDir 'trino\catalog'
      if ($Demo) {
        $sync.Status = 'Installing demo services...'
        Install-ReleaseImages 'provisa-demo-images-*.tar.gz' "provisa-demo-images-$EmbeddedVersion.tar.gz" 'demo services'
      } else {
        # Core: drop the demo source catalogs so Trino stays a clean, empty
        # platform (Provisa creates catalogs dynamically per registered source).
        $keep = @('provisa_admin', 'files', 'otel', 'results')
        Get-ChildItem -Path $CatalogDir -Filter '*.properties' -ErrorAction SilentlyContinue | ForEach-Object {
          if ($keep -notcontains $_.BaseName) { Remove-Item $_.FullName -Force -ErrorAction SilentlyContinue }
        }
      }

      # Observability: load its images and enable Trino OTel; otherwise strip
      # OTel so the coordinator healthcheck can pass without a collector.
      if ($Obs) {
        $sync.Status = 'Installing observability stack...'
        Install-ReleaseImages 'provisa-obs-images-*.tar.gz' "provisa-obs-images-$EmbeddedVersion.tar.gz" 'observability stack'
      }
      Set-TrinoOtel -Enabled:$Obs
      Set-TrinoFte  -Enabled:($Workers -gt 0)
      $sync.Progress = 91

      # Step 7: Write config
      Log 'Writing config...'
      New-Item -ItemType Directory -Path $ProvisaHome -Force | Out-Null
      $cfgPath = Join-Path $ProvisaHome 'config.yaml'
      $ApiPort = $UiPort + 1
      $fwd = $ComposeDir -replace '\\', '/'
      if ($Backend -eq 'docker') {
        $runtimeLines = @(
          'runtime: docker'
          'docker_host: npipe:////./pipe/docker_engine'
        ) -join "`n"
      } else {
        $runtimeLines = @(
          'runtime: virtualbox'
          'vm_name: Provisa'
          'docker_host: tcp://127.0.0.1:2375'
        ) -join "`n"
      }
@"
# Provisa configuration - generated by Windows installer
project_dir: "$fwd"
hostname: $Hostname
ui_port: $UiPort
api_port: $ApiPort
auto_open_browser: true
$runtimeLines
federation_workers: $Workers
demo: $($Demo.ToString().ToLower())
obs: $($Obs.ToString().ToLower())
version: $EmbeddedVersion
"@ | Set-Content -Path $cfgPath -Encoding UTF8
      $sync.Progress = 92

      # Step 7: Sentinel + Start Menu shortcut ------------------------------
      New-Item -ItemType File -Path $Sentinel -Force | Out-Null
      $smDir = Join-Path $env:APPDATA 'Microsoft\Windows\Start Menu\Programs\Provisa'
      New-Item -ItemType Directory -Path $smDir -Force | Out-Null
      $wsh  = New-Object -ComObject WScript.Shell
      $link = $wsh.CreateShortcut("$smDir\Start Provisa.lnk")
      # Launch the GUI dialog (hidden console via wscript) instead of leaving a
      # bare PowerShell window open.
      $link.TargetPath       = 'wscript.exe'
      $link.Arguments        = "/nologo `"$ScriptDir\start-gui.vbs`""
      $link.WorkingDirectory = $ScriptDir
      $iconPath = Join-Path $ScriptDir 'provisa.ico'
      if (Test-Path $iconPath) { $link.IconLocation = $iconPath }
      $link.Save()

      # Reconfigure/upgrade: full down + up so removed overlays are dropped
      # (--remove-orphans) and Trino is recreated with the new OTel config.
      if ($ManageMode) {
        Log 'Applying changes (restarting services)...'
        $provisaPs1 = Join-Path $ScriptDir 'provisa.ps1'
        & powershell.exe -ExecutionPolicy Bypass -File $provisaPs1 stop  2>&1 | ForEach-Object { Log "  $_" }
        & powershell.exe -ExecutionPolicy Bypass -File $provisaPs1 start 2>&1 | ForEach-Object { Log "  $_" }
        Log 'Changes applied.'
      }

      $sync.Progress = 100
      Log $(if ($ManageMode) { 'Done. Start Menu -> Provisa -> Start Provisa' } else { 'Setup complete! Start Menu -> Provisa -> Start Provisa' })
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
