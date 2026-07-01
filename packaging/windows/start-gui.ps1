#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# GUI wrapper for `provisa start`: runs the CLI in the background and shows a
# small dialog with progress and an "Open Provisa" button, instead of leaving a
# bare console window open.

Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing
[System.Windows.Forms.Application]::EnableVisualStyles()

$ScriptDir  = if ($PSScriptRoot) { $PSScriptRoot } else { Split-Path -Parent $MyInvocation.MyCommand.Path }
$ProvisaPs1 = Join-Path $ScriptDir 'provisa.ps1'
$ConfigPath = Join-Path $env:USERPROFILE '.provisa\config.yaml'

# -- UI port (for the Open button) --------------------------------------------
$UiPort = 3000
if (Test-Path $ConfigPath) {
  foreach ($line in Get-Content $ConfigPath) {
    if ($line -match '^\s*ui_port\s*:\s*(\d+)') { $UiPort = [int]$Matches[1] }
  }
}
$UiUrl = "http://localhost:$UiPort"

# -- Form ----------------------------------------------------------------------
$form = New-Object System.Windows.Forms.Form
$form.Text            = 'Provisa'
$form.ClientSize      = New-Object System.Drawing.Size(460, 300)
$form.StartPosition   = 'CenterScreen'
$form.FormBorderStyle = 'FixedSingle'
$form.MaximizeBox     = $false
$form.BackColor       = [System.Drawing.Color]::White
$form.Font            = New-Object System.Drawing.Font('Segoe UI', 9)

$header           = New-Object System.Windows.Forms.Panel
$header.Dock      = 'Top'
$header.Height    = 56
$header.BackColor = [System.Drawing.Color]::FromArgb(24, 24, 24)
$form.Controls.Add($header)

$lbTitle           = New-Object System.Windows.Forms.Label
$lbTitle.Text      = 'Provisa'
$lbTitle.Font      = New-Object System.Drawing.Font('Segoe UI', 18, [System.Drawing.FontStyle]::Bold)
$lbTitle.ForeColor = [System.Drawing.Color]::White
$lbTitle.AutoSize  = $true
$lbTitle.Location  = New-Object System.Drawing.Point(16, 10)
$header.Controls.Add($lbTitle)

$lbStatus          = New-Object System.Windows.Forms.Label
$lbStatus.Text     = 'Starting Provisa services...'
$lbStatus.AutoSize = $true
$lbStatus.Location = New-Object System.Drawing.Point(16, 68)
$form.Controls.Add($lbStatus)

$pb          = New-Object System.Windows.Forms.ProgressBar
$pb.Style    = 'Marquee'
$pb.Location = New-Object System.Drawing.Point(16, 92)
$pb.Size     = New-Object System.Drawing.Size(428, 16)
$form.Controls.Add($pb)

$rtb             = New-Object System.Windows.Forms.RichTextBox
$rtb.Location    = New-Object System.Drawing.Point(16, 118)
$rtb.Size        = New-Object System.Drawing.Size(428, 120)
$rtb.ReadOnly    = $true
$rtb.Font        = New-Object System.Drawing.Font('Consolas', 8)
$rtb.BackColor   = [System.Drawing.Color]::FromArgb(18, 18, 18)
$rtb.ForeColor   = [System.Drawing.Color]::FromArgb(204, 204, 204)
$rtb.BorderStyle = 'None'
$form.Controls.Add($rtb)

$btnOpen           = New-Object System.Windows.Forms.Button
$btnOpen.Text      = 'Open Provisa'
$btnOpen.Font      = New-Object System.Drawing.Font('Segoe UI', 10, [System.Drawing.FontStyle]::Bold)
$btnOpen.Size      = New-Object System.Drawing.Size(140, 34)
$btnOpen.Location  = New-Object System.Drawing.Point(16, 250)
$btnOpen.BackColor = [System.Drawing.Color]::FromArgb(0, 120, 215)
$btnOpen.ForeColor = [System.Drawing.Color]::White
$btnOpen.FlatStyle = 'Flat'
$btnOpen.FlatAppearance.BorderSize = 0
$btnOpen.Enabled   = $false
$form.Controls.Add($btnOpen)
$btnOpen.Add_Click({ Start-Process $UiUrl })

$btnClose          = New-Object System.Windows.Forms.Button
$btnClose.Text     = 'Close'
$btnClose.Size     = New-Object System.Drawing.Size(100, 34)
$btnClose.Location = New-Object System.Drawing.Point(344, 250)
$btnClose.FlatStyle = 'System'
$form.Controls.Add($btnClose)
$btnClose.Add_Click({ $form.Close() })

# -- Background runspace: run `provisa.ps1 start` ------------------------------
$sync = [hashtable]::Synchronized(@{
  Queue = [System.Collections.Queue]::Synchronized((New-Object System.Collections.Queue))
  Done  = $false
  Ok    = $false
})

$rs = [runspacefactory]::CreateRunspace()
$rs.ApartmentState = 'STA'
$rs.ThreadOptions  = 'ReuseThread'
$rs.Open()
$rs.SessionStateProxy.SetVariable('sync',       $sync)
$rs.SessionStateProxy.SetVariable('ProvisaPs1', $ProvisaPs1)

$ps = [powershell]::Create()
$ps.Runspace = $rs
$null = $ps.AddScript({
  try {
    & powershell.exe -ExecutionPolicy Bypass -File $ProvisaPs1 start 2>&1 | ForEach-Object {
      $sync.Queue.Enqueue([string]$_)
    }
    $sync.Ok = ($LASTEXITCODE -eq 0)
  } catch {
    $sync.Queue.Enqueue("ERROR: $($_.ToString())")
    $sync.Ok = $false
  } finally {
    $sync.Done = $true
  }
})
$null = $ps.BeginInvoke()

# -- Timer: drain output, flip UI on completion -------------------------------
$timer          = New-Object System.Windows.Forms.Timer
$timer.Interval = 200
$timer.Add_Tick({
  while ($sync.Queue.Count -gt 0) {
    $rtb.AppendText(($sync.Queue.Dequeue() + "`n"))
    $rtb.ScrollToCaret()
  }
  if ($sync.Done) {
    $timer.Stop()
    $pb.Style = 'Continuous'
    $pb.Value = 100
    if ($sync.Ok) {
      $lbStatus.Text   = "Provisa is running at $UiUrl"
      $btnOpen.Enabled = $true
    } else {
      $lbStatus.Text      = 'Provisa failed to start. See log above.'
      $lbStatus.ForeColor = [System.Drawing.Color]::FromArgb(200, 40, 40)
    }
  }
})
$timer.Start()

$form.Add_Shown({ $form.Activate() })
[System.Windows.Forms.Application]::Run($form)
