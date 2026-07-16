# Provisa Windows setup wizard (native tier, REQ-972..979). A WinForms GUI that mirrors the macOS
# SwiftUI DeploymentView - federation engine, observability, demo - then hands off to
# first-launch-native.ps1 in NON-INTERACTIVE mode with the chosen PROVISA_* env. The dialog renders
# even when this script is launched with a hidden console (launch-gui.vbs), so no console prompts.
#
# Trino, the bundled Grafana/Prometheus stack, and the Docker demo need the container tier
# (install-container.ps1); the native installer offers only what it can fulfil.
#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ScriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProvisaHome = Join-Path $env:USERPROFILE '.provisa'
$ConfigPath  = Join-Path $ProvisaHome 'config.yaml'

function Start-App {
  # first-launch stages the runtime on first run (config already present), then starts the app.
  & powershell.exe -ExecutionPolicy Bypass -WindowStyle Hidden `
    -File (Join-Path $ScriptDir 'first-launch-native.ps1')
}

# The installer's wizard pages write config.yaml with the chosen deployment. When it exists we are
# already configured -> just start. This GUI wizard is only a FALLBACK for a launch with no config
# (e.g. a silent/unattended install that skipped the wizard pages).
if (Test-Path $ConfigPath) { Start-App; return }

Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing
[System.Windows.Forms.Application]::EnableVisualStyles()

$form = New-Object System.Windows.Forms.Form
$form.Text = 'Provisa Setup'
$form.Size = New-Object System.Drawing.Size(560, 560)
$form.StartPosition = 'CenterScreen'
$form.FormBorderStyle = 'FixedDialog'
$form.MaximizeBox = $false
$form.MinimizeBox = $false
$form.Font = New-Object System.Drawing.Font('Segoe UI', 9)

$header = New-Object System.Windows.Forms.Label
$header.Text = 'Choose your deployment'
$header.Font = New-Object System.Drawing.Font('Segoe UI', 13, [System.Drawing.FontStyle]::Bold)
$header.Location = New-Object System.Drawing.Point(20, 15)
$header.AutoSize = $true
$form.Controls.Add($header)

# -- Federation engine ---------------------------------------------------------
$grpEngine = New-Object System.Windows.Forms.GroupBox
$grpEngine.Text = 'Federation engine'
$grpEngine.Location = New-Object System.Drawing.Point(20, 55)
$grpEngine.Size = New-Object System.Drawing.Size(510, 150)
$form.Controls.Add($grpEngine)

$rbDuck = New-Object System.Windows.Forms.RadioButton
$rbDuck.Text = 'Embedded database - zero-config (recommended)'
$rbDuck.Location = New-Object System.Drawing.Point(15, 25)
$rbDuck.Size = New-Object System.Drawing.Size(480, 22)
$rbDuck.Checked = $true
$grpEngine.Controls.Add($rbDuck)

$rbExternal = New-Object System.Windows.Forms.RadioButton
$rbExternal.Text = 'External engine (connect to an existing engine)'
$rbExternal.Location = New-Object System.Drawing.Point(15, 50)
$rbExternal.Size = New-Object System.Drawing.Size(480, 22)
$grpEngine.Controls.Add($rbExternal)

$lblEngineUrl = New-Object System.Windows.Forms.Label
$lblEngineUrl.Text = 'Engine URL'
$lblEngineUrl.Location = New-Object System.Drawing.Point(35, 78)
$lblEngineUrl.Size = New-Object System.Drawing.Size(90, 22)
$grpEngine.Controls.Add($lblEngineUrl)

$txtEngineUrl = New-Object System.Windows.Forms.TextBox
$txtEngineUrl.Location = New-Object System.Drawing.Point(130, 76)
$txtEngineUrl.Size = New-Object System.Drawing.Size(360, 22)
$txtEngineUrl.Enabled = $false
$grpEngine.Controls.Add($txtEngineUrl)

$lblMatUrl = New-Object System.Windows.Forms.Label
$lblMatUrl.Text = 'Materialize URL'
$lblMatUrl.Location = New-Object System.Drawing.Point(35, 106)
$lblMatUrl.Size = New-Object System.Drawing.Size(90, 22)
$grpEngine.Controls.Add($lblMatUrl)

$txtMatUrl = New-Object System.Windows.Forms.TextBox
$txtMatUrl.Location = New-Object System.Drawing.Point(130, 104)
$txtMatUrl.Size = New-Object System.Drawing.Size(360, 22)
$txtMatUrl.Enabled = $false
$grpEngine.Controls.Add($txtMatUrl)

$rbExternal.Add_CheckedChanged({
  $txtEngineUrl.Enabled = $rbExternal.Checked
  $txtMatUrl.Enabled = $rbExternal.Checked
})

# -- Observability -------------------------------------------------------------
$grpObs = New-Object System.Windows.Forms.GroupBox
$grpObs.Text = 'Observability'
$grpObs.Location = New-Object System.Drawing.Point(20, 215)
$grpObs.Size = New-Object System.Drawing.Size(510, 120)
$form.Controls.Add($grpObs)

$rbObsNone = New-Object System.Windows.Forms.RadioButton
$rbObsNone.Text = 'Built-in only'
$rbObsNone.Location = New-Object System.Drawing.Point(15, 25)
$rbObsNone.Size = New-Object System.Drawing.Size(480, 22)
$rbObsNone.Checked = $true
$grpObs.Controls.Add($rbObsNone)

$rbObsCollector = New-Object System.Windows.Forms.RadioButton
$rbObsCollector.Text = 'Export to my OpenTelemetry collector'
$rbObsCollector.Location = New-Object System.Drawing.Point(15, 50)
$rbObsCollector.Size = New-Object System.Drawing.Size(480, 22)
$grpObs.Controls.Add($rbObsCollector)

$lblOtlp = New-Object System.Windows.Forms.Label
$lblOtlp.Text = 'OTLP endpoint'
$lblOtlp.Location = New-Object System.Drawing.Point(35, 82)
$lblOtlp.Size = New-Object System.Drawing.Size(90, 22)
$grpObs.Controls.Add($lblOtlp)

$txtOtlp = New-Object System.Windows.Forms.TextBox
$txtOtlp.Location = New-Object System.Drawing.Point(130, 80)
$txtOtlp.Size = New-Object System.Drawing.Size(360, 22)
$txtOtlp.Enabled = $false
$txtOtlp.Text = 'http://localhost:4317'
$grpObs.Controls.Add($txtOtlp)

$rbObsCollector.Add_CheckedChanged({ $txtOtlp.Enabled = $rbObsCollector.Checked })

# -- Demo ----------------------------------------------------------------------
$chkDemo = New-Object System.Windows.Forms.CheckBox
$chkDemo.Text = 'Install the demo dataset and open the guided tour'
$chkDemo.Location = New-Object System.Drawing.Point(25, 348)
$chkDemo.Size = New-Object System.Drawing.Size(500, 22)
$form.Controls.Add($chkDemo)

$note = New-Object System.Windows.Forms.Label
$note.Text = 'The demo is a complete, fully functional install - pick it with confidence; rerun this installer ' +
  'anytime to reconfigure. Trino / the Docker demo require the Container installer.'
$note.Location = New-Object System.Drawing.Point(25, 378)
$note.Size = New-Object System.Drawing.Size(505, 40)
$note.ForeColor = [System.Drawing.Color]::Gray
$form.Controls.Add($note)

# -- Ports ---------------------------------------------------------------------
$lblUi = New-Object System.Windows.Forms.Label
$lblUi.Text = 'UI port'
$lblUi.Location = New-Object System.Drawing.Point(25, 425)
$lblUi.Size = New-Object System.Drawing.Size(55, 22)
$form.Controls.Add($lblUi)
$txtUi = New-Object System.Windows.Forms.TextBox
$txtUi.Text = '3000'
$txtUi.Location = New-Object System.Drawing.Point(85, 423)
$txtUi.Size = New-Object System.Drawing.Size(70, 22)
$form.Controls.Add($txtUi)

$lblApi = New-Object System.Windows.Forms.Label
$lblApi.Text = 'API port'
$lblApi.Location = New-Object System.Drawing.Point(180, 425)
$lblApi.Size = New-Object System.Drawing.Size(55, 22)
$form.Controls.Add($lblApi)
$txtApi = New-Object System.Windows.Forms.TextBox
$txtApi.Text = '8000'
$txtApi.Location = New-Object System.Drawing.Point(240, 423)
$txtApi.Size = New-Object System.Drawing.Size(70, 22)
$form.Controls.Add($txtApi)

# -- Buttons -------------------------------------------------------------------
$btnInstall = New-Object System.Windows.Forms.Button
$btnInstall.Text = 'Install'
$btnInstall.Location = New-Object System.Drawing.Point(340, 480)
$btnInstall.Size = New-Object System.Drawing.Size(90, 30)
$btnInstall.DialogResult = [System.Windows.Forms.DialogResult]::OK
$form.Controls.Add($btnInstall)
$form.AcceptButton = $btnInstall

$btnCancel = New-Object System.Windows.Forms.Button
$btnCancel.Text = 'Cancel'
$btnCancel.Location = New-Object System.Drawing.Point(440, 480)
$btnCancel.Size = New-Object System.Drawing.Size(90, 30)
$btnCancel.DialogResult = [System.Windows.Forms.DialogResult]::Cancel
$form.Controls.Add($btnCancel)
$form.CancelButton = $btnCancel

if ($form.ShowDialog() -ne [System.Windows.Forms.DialogResult]::OK) { return }

# -- Hand off the chosen deployment to first-launch (non-interactive) ----------
$env:PROVISA_NONINTERACTIVE = '1'
if ($rbExternal.Checked) {
  $env:PROVISA_ENGINE = 'sqlalchemy'
  $env:PROVISA_ENGINE_URL = $txtEngineUrl.Text
  $env:PROVISA_MATERIALIZE_URL = $txtMatUrl.Text
} else {
  $env:PROVISA_ENGINE = 'duckdb'
  $env:PROVISA_ENGINE_URL = ''
  $env:PROVISA_MATERIALIZE_URL = ''
}
if ($rbObsCollector.Checked) {
  $env:PROVISA_OBS_MODE = 'collector'
  $env:PROVISA_OTLP_ENDPOINT = $txtOtlp.Text
} else {
  $env:PROVISA_OBS_MODE = 'none'
  $env:PROVISA_OTLP_ENDPOINT = ''
}
$env:PROVISA_INSTALL_DEMO = if ($chkDemo.Checked) { 'y' } else { 'n' }
$env:PROVISA_UI_PORT = $txtUi.Text
$env:PROVISA_API_PORT = $txtApi.Text

& powershell.exe -ExecutionPolicy Bypass -WindowStyle Hidden `
  -File (Join-Path $ScriptDir 'first-launch-native.ps1')
