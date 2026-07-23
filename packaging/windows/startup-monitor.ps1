# Provisa startup monitor (native tier). A small always-on-top WinForms splash that makes the slow
# first launch legible: it launches the worker (first-launch-native.ps1) HIDDEN, tails the worker's
# progress breadcrumbs (.startup-status), polls the API /health, then opens the browser and closes
# itself when the app is genuinely ready.
#
# Why this exists: the launcher chain runs -WindowStyle Hidden, so a first launch showed NOTHING for
# the tens of seconds the API spends loading the demo config + introspecting sources - and a fatal
# staging failure (a locked runtime that could not be replaced) was written to a hidden console and
# lost, so the user saw "nothing happened." This window shows progress AND surfaces those errors.
#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ScriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProvisaHome = Join-Path $env:USERPROFILE '.provisa'
$ConfigPath  = Join-Path $ProvisaHome 'config.yaml'
$StatusFile  = Join-Path $ProvisaHome '.startup-status'
$LogDir      = Join-Path $ProvisaHome '.logs'
$Worker      = Join-Path $ScriptDir 'first-launch-native.ps1'

# -- Config reader (mirrors provisa-native.ps1 Read-Config) --------------------
function Read-Cfg {
  param([string]$Key, [string]$Default)
  if (Test-Path $ConfigPath) {
    foreach ($line in Get-Content $ConfigPath) {
      if ($line -match "^\s*$([regex]::Escape($Key))\s*:\s*""?([^""]*?)""?\s*$") {
        $v = $Matches[1].Trim()
        if ($v) { return $v }
      }
    }
  }
  return $Default
}

# Ports / demo flag: the wizard's env wins on a first run (config is not written yet when the monitor
# starts), else the freshly written config, else the documented defaults.
function Resolve-UiPort  { if ($env:PROVISA_UI_PORT)  { $env:PROVISA_UI_PORT }  else { Read-Cfg 'ui_port'  '3000' } }
function Resolve-ApiPort { if ($env:PROVISA_API_PORT) { $env:PROVISA_API_PORT } else { Read-Cfg 'api_port' '8000' } }
function Resolve-Demo {
  if ($env:PROVISA_INSTALL_DEMO) { return $env:PROVISA_INSTALL_DEMO -match '^(y|Y|true)' }
  return (Read-Cfg 'demo' 'false') -eq 'true'
}

# The URL to open once ready - ?tour=1 auto-starts the guided tour for a demo install (App.tsx reads
# the query param), mirroring provisa-native.ps1 Open-Native.
function Resolve-OpenUrl {
  param([int]$UiPort, [bool]$Demo)
  if ($Demo) { "http://localhost:$UiPort/?tour=1" } else { "http://localhost:$UiPort" }
}

# The last non-empty "STATE|message" breadcrumb, or $null if none yet. A one-tick read of a file the
# worker is writing this instant can throw a sharing violation; that is contention, not an error, so
# this tick returns $null and the next tick re-reads (the label just holds its previous value).
function Get-LastStatus {
  param([string]$Path)
  if (-not (Test-Path $Path)) { return $null }
  try { $lines = Get-Content -Path $Path -ErrorAction Stop } catch { return $null }
  for ($i = $lines.Count - 1; $i -ge 0; $i--) {
    if ("$($lines[$i])".Trim()) { return "$($lines[$i])" }
  }
  return $null
}

# Split a breadcrumb into its state token and human message. "STAGING|Staging runtime..." -> both.
function Parse-Status {
  param([string]$Line)
  if (-not $Line) { return $null }
  $i = $Line.IndexOf('|')
  if ($i -lt 0) { return [pscustomobject]@{ State = $Line.Trim(); Message = '' } }
  return [pscustomobject]@{ State = $Line.Substring(0, $i).Trim(); Message = $Line.Substring($i + 1).Trim() }
}

# Is a local TCP port accepting connections? A fast pre-check before the (heavier) /health GET so the
# UI-thread timer never blocks on a dead port.
function Test-PortOpen {
  param([int]$Port, [int]$TimeoutMs = 250)
  $client = New-Object System.Net.Sockets.TcpClient
  try {
    $iar = $client.BeginConnect('127.0.0.1', $Port, $null, $null)
    if (-not $iar.AsyncWaitHandle.WaitOne($TimeoutMs)) { return $false }
    $client.EndConnect($iar)
    return $true
  } catch { return $false } finally { $client.Close() }
}

# Ready = the API answers /ready 200 and the UI port is listening. /ready (not /health) is the warmth
# gate: the API flips it to 200 only AFTER its boot warmup probe has attached the store and warmed the
# engine terminal, so we don't open onto a still-cold app whose first query stalls. It returns 503
# ("warming") until then. Invoke-WebRequest treats 503 as a terminating error, so a non-200/refused
# response is simply "not ready yet" and the caller keeps polling.
function Test-Ready {
  param([int]$UiPort, [int]$ApiPort)
  if (-not (Test-PortOpen $ApiPort)) { return $false }
  try { $r = Invoke-WebRequest -UseBasicParsing -TimeoutSec 3 "http://localhost:$ApiPort/ready" }
  catch { return $false }
  if ($r.StatusCode -ne 200) { return $false }
  return (Test-PortOpen $UiPort)
}

# Prime the endpoints the UI hits on first paint so the user's first interaction isn't the cold one.
# /health passing only means the API's lifespan startup finished; the first DATA queries still lazily
# introspect sources, attach the store, and compile plans. Warming them here (through the UI proxy,
# so both the UI server and the API warm) moves that cold hit off the user's first click. Best-effort
# by design: a warmup miss must never block opening the app, so failures are swallowed with a comment.
function Invoke-Warmup {
  param([int]$UiPort)
  $paths = @('/setup/status', '/auth/me', '/admin/settings', '/data/domains')
  foreach ($p in $paths) {
    try { Invoke-WebRequest -UseBasicParsing -TimeoutSec 8 "http://localhost:$UiPort$p" | Out-Null }
    catch { }  # priming only; a cold/errored endpoint is warmed enough by the attempt itself
  }
}

# The tail of the API error log, for the failure panel when the worker died without a clean ERROR
# breadcrumb (e.g. an unexpected crash rather than the guarded staging failure).
function Get-ErrorDetail {
  param([string]$LogName = 'native-api.err.log', [int]$MaxLines = 12)
  $f = Join-Path $LogDir $LogName
  if (-not (Test-Path $f)) { return '' }
  try { (Get-Content -Path $f -Tail $MaxLines -ErrorAction Stop) -join "`r`n" } catch { return '' }
}

# ==============================================================================
# GUI
# ==============================================================================
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing

# Opt into per-monitor DPI awareness before any window exists. Without this, Windows bitmap-stretches
# the whole dialog on high-DPI displays, which is what made the logo look jagged/pixelated.
try {
  Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
public static class DpiAwareness {
  [DllImport("user32.dll")] public static extern bool SetProcessDpiAwarenessContext(IntPtr value);
  [DllImport("shcore.dll")] public static extern int SetProcessDpiAwareness(int value);
  [DllImport("user32.dll")] public static extern bool SetProcessDPIAware();
  public static void Enable() {
    // -4 = PER_MONITOR_AWARE_V2 (Win10 1703+); fall back to older APIs on downlevel systems.
    try { if (SetProcessDpiAwarenessContext(new IntPtr(-4))) return; } catch {}
    try { SetProcessDpiAwareness(2); return; } catch {}
    try { SetProcessDPIAware(); } catch {}
  }
}
'@
  [DpiAwareness]::Enable()
} catch {}

[System.Windows.Forms.Application]::EnableVisualStyles()
[System.Windows.Forms.Application]::SetCompatibleTextRenderingDefault($false)

# Downscale a source image to an exact pixel size with high-quality bicubic interpolation. WinForms'
# PictureBox.SizeMode='Zoom' uses low-quality scaling, which visibly aliases a 256px mark down to 48px.
function New-ScaledBitmap {
  param([System.Drawing.Image]$Source, [int]$Width, [int]$Height)
  $bmp = New-Object System.Drawing.Bitmap($Width, $Height)
  $g = [System.Drawing.Graphics]::FromImage($bmp)
  $g.InterpolationMode  = [System.Drawing.Drawing2D.InterpolationMode]::HighQualityBicubic
  $g.SmoothingMode      = [System.Drawing.Drawing2D.SmoothingMode]::HighQuality
  $g.PixelOffsetMode    = [System.Drawing.Drawing2D.PixelOffsetMode]::HighQuality
  $g.CompositingQuality = [System.Drawing.Drawing2D.CompositingQuality]::HighQuality
  $g.DrawImage($Source, 0, 0, $Width, $Height)
  $g.Dispose()
  return $bmp
}

$UiPort  = [int](Resolve-UiPort)
$ApiPort = [int](Resolve-ApiPort)
$Demo    = [bool](Resolve-Demo)

# Fresh run: drop any stale breadcrumb trail before the worker starts appending.
Remove-Item $StatusFile -Force -ErrorAction SilentlyContinue

$form = New-Object System.Windows.Forms.Form
$form.Text = 'Starting Provisa'
$form.Size = New-Object System.Drawing.Size(460, 250)
$form.StartPosition = 'CenterScreen'
$form.FormBorderStyle = 'FixedDialog'
$form.MaximizeBox = $false
$form.MinimizeBox = $false
$form.TopMost = $true
$form.Font = New-Object System.Drawing.Font('Segoe UI', 9)
$icon = Join-Path $ScriptDir 'provisa.ico'
if (Test-Path $icon) { $form.Icon = New-Object System.Drawing.Icon($icon) }

$logoFile = Join-Path $ScriptDir 'provisa-mark.png'
if (Test-Path $logoFile) {
  # Read the file into memory first so the on-disk handle isn't locked, then pre-scale to the exact
  # DPI-adjusted pixel size with bicubic interpolation for a crisp mark at any scale factor.
  $srcBytes = [System.IO.File]::ReadAllBytes($logoFile)
  $ms = New-Object System.IO.MemoryStream(,$srcBytes)
  $srcImg = [System.Drawing.Image]::FromStream($ms)
  $dpiScale = [double]$form.DeviceDpi / 96.0
  $logoPx = [int][Math]::Round(48 * $dpiScale)
  $logo = New-Object System.Windows.Forms.PictureBox
  $logo.Image = New-ScaledBitmap -Source $srcImg -Width $logoPx -Height $logoPx
  $srcImg.Dispose(); $ms.Dispose()
  $logo.SizeMode = 'Zoom'
  $logo.Location = New-Object System.Drawing.Point(20, 20)
  $logo.Size = New-Object System.Drawing.Size(48, 48)
  $form.Controls.Add($logo)
  $headerX = 80
} else { $headerX = 20 }

$header = New-Object System.Windows.Forms.Label
$header.Text = 'Starting Provisa'
$header.Font = New-Object System.Drawing.Font('Segoe UI', 13, [System.Drawing.FontStyle]::Bold)
$header.Location = New-Object System.Drawing.Point($headerX, 30)
$header.AutoSize = $true
$form.Controls.Add($header)

$status = New-Object System.Windows.Forms.Label
$status.Text = 'Preparing...'
$status.Location = New-Object System.Drawing.Point(20, 90)
$status.Size = New-Object System.Drawing.Size(420, 60)
$form.Controls.Add($status)

$bar = New-Object System.Windows.Forms.ProgressBar
$bar.Style = 'Marquee'
$bar.MarqueeAnimationSpeed = 30
$bar.Location = New-Object System.Drawing.Point(20, 150)
$bar.Size = New-Object System.Drawing.Size(420, 20)
$form.Controls.Add($bar)

$btnClose = New-Object System.Windows.Forms.Button
$btnClose.Text = 'Close'
$btnClose.Location = New-Object System.Drawing.Point(350, 180)
$btnClose.Size = New-Object System.Drawing.Size(90, 28)
$btnClose.Visible = $false
$btnClose.Add_Click({ $form.Close() })
$form.Controls.Add($btnClose)

$btnOpen = New-Object System.Windows.Forms.Button
$btnOpen.Text = 'Open anyway'
$btnOpen.Location = New-Object System.Drawing.Point(240, 180)
$btnOpen.Size = New-Object System.Drawing.Size(100, 28)
$btnOpen.Visible = $false
$btnOpen.Add_Click({
  # Never open blind: if nothing is listening on the UI port yet, the browser would just show a
  # connection error ("no server"), which is exactly the dead-end this button used to cause. Only
  # open when the port is actually up; otherwise tell the user it is still starting.
  if (Test-PortOpen $UiPort) {
    Start-Process (Resolve-OpenUrl -UiPort $UiPort -Demo $Demo)
    $form.Close()
  } else {
    [System.Windows.Forms.MessageBox]::Show(
      "Provisa isn't serving on port $UiPort yet, so the browser would show nothing. It's still starting - keep waiting. If this persists, check %USERPROFILE%\.provisa\.logs.",
      'Still starting', [System.Windows.Forms.MessageBoxButtons]::OK,
      [System.Windows.Forms.MessageBoxIcon]::Information) | Out-Null
  }
})
$form.Controls.Add($btnOpen)

# -- Launch the worker HIDDEN; the monitor owns readiness + the browser --------
# PROVISA_STARTUP_UI tells the worker chain to (a) emit breadcrumbs and (b) NOT open the browser
# itself - this window opens it exactly once, when /health passes.
$env:PROVISA_STARTUP_UI = '1'
$worker = Start-Process -FilePath 'powershell.exe' `
  -ArgumentList @('-ExecutionPolicy','Bypass','-WindowStyle','Hidden','-File', $Worker) `
  -WindowStyle Hidden -PassThru

$script:Human = @{
  STAGING = 'Staging the runtime...'
  CONFIG  = 'Writing configuration...'
  DEMO    = 'Starting demo services...'
  START   = 'Starting the engine and UI...'
  WAIT    = 'Waiting for the engine to become ready (this can take a minute on first run)...'
}
$script:Elapsed = 0
$script:Done    = $false

$timer = New-Object System.Windows.Forms.Timer
$timer.Interval = 1000
$timer.Add_Tick({
  if ($script:Done) { return }
  $script:Elapsed += 1

  $parsed = Parse-Status (Get-LastStatus $StatusFile)

  # Hard failure: the worker wrote an ERROR breadcrumb (e.g. the runtime could not be replaced).
  if ($parsed -and $parsed.State -eq 'ERROR') {
    $script:Done = $true
    $timer.Stop()
    $bar.Style = 'Continuous'; $bar.Value = 0
    $header.Text = 'Provisa could not start'
    $header.ForeColor = [System.Drawing.Color]::Firebrick
    $status.Text = if ($parsed.Message) { $parsed.Message } else { 'Startup failed. See %USERPROFILE%\.provisa\.logs.' }
    $btnClose.Visible = $true
    return
  }

  # Ready: warm the first-paint endpoints, then open the browser once and close.
  if (Test-Ready -UiPort $UiPort -ApiPort $ApiPort) {
    $script:Done = $true
    $timer.Stop()
    $status.Text = 'Warming up...'
    [System.Windows.Forms.Application]::DoEvents()  # repaint the label before the blocking warmup burst
    Invoke-Warmup -UiPort $UiPort
    Start-Process (Resolve-OpenUrl -UiPort $UiPort -Demo $Demo)
    $form.Close()
    return
  }

  # Progress label from the latest breadcrumb.
  if ($parsed -and $script:Human.ContainsKey($parsed.State)) { $status.Text = $script:Human[$parsed.State] }
  elseif ($parsed -and $parsed.Message)                      { $status.Text = $parsed.Message }

  # The worker process exited but the app never became ready and no ERROR was written - surface the
  # API log tail so an unexpected crash is visible rather than silent. (The worker exiting is normal
  # BEFORE health when it only launches the detached servers, so we also require it to be past the
  # WAIT stage before treating exit as a failure.)
  if ($worker.HasExited -and $parsed -and $parsed.State -eq 'WAIT' -and $script:Elapsed -gt 20 -and -not (Test-PortOpen $ApiPort)) {
    $script:Done = $true
    $timer.Stop()
    $bar.Style = 'Continuous'; $bar.Value = 0
    $header.Text = 'Provisa could not start'
    $header.ForeColor = [System.Drawing.Color]::Firebrick
    $detail = Get-ErrorDetail
    $status.Text = if ($detail) { "The engine exited during startup:`r`n$detail" } else { 'The engine exited during startup. See %USERPROFILE%\.provisa\.logs.' }
    $btnClose.Visible = $true
    return
  }

  # UI server died: the API bound its port (we got past START) but the UI port never came up. Without
  # this the dead UI is invisible and the app never opens - the readiness gate needs BOTH ports, but
  # the exit check above only watches the API. A UI uvicorn binds in a second or two, so a port still
  # closed at 30s means it crashed (typically the UI port already in use). Surface its log tail.
  if ($worker.HasExited -and (Test-PortOpen $ApiPort) -and $script:Elapsed -gt 30 -and -not (Test-PortOpen $UiPort)) {
    $script:Done = $true
    $timer.Stop()
    $bar.Style = 'Continuous'; $bar.Value = 0
    $header.Text = 'Provisa could not start'
    $header.ForeColor = [System.Drawing.Color]::Firebrick
    $detail = Get-ErrorDetail 'native-ui.err.log'
    $status.Text = if ($detail) { "The UI server exited during startup (is port $UiPort already in use?):`r`n$detail" } else { "The UI server did not come up on port $UiPort. See %USERPROFILE%\.provisa\.logs." }
    $btnClose.Visible = $true
    return
  }

  # Taking unusually long: let the user open it manually or wait it out (keep polling).
  if ($script:Elapsed -eq 180) {
    $status.Text = 'Provisa is taking longer than usual to start. You can keep waiting, or open it now.'
    $btnOpen.Visible = $true
    $btnClose.Visible = $true
  }
})
$timer.Start()

[System.Windows.Forms.Application]::Run($form)
