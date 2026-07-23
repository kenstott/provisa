# Provisa native-tier CLI for Windows (REQ-979). Runs the bundled standalone
# Python runtime directly - no Docker, no VM, no containers. Mirrors the native
# commands in scripts/provisa (cmd_start_native / cmd_stop_native).
#Requires -Version 5.1
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ScriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProvisaHome = Join-Path $env:USERPROFILE '.provisa'
$ConfigPath  = Join-Path $ProvisaHome 'config.yaml'
$RuntimeDir  = Join-Path $ProvisaHome 'runtime'
$RuntimePy   = Join-Path $RuntimeDir 'python.exe'
$NativeDir   = Join-Path $ProvisaHome 'native'
$LogDir      = Join-Path $ProvisaHome '.logs'
$PidFile     = Join-Path $ProvisaHome '.native.pid'

# Native demo (REQ-979): mock servers + config bundled beside this script under {app}.
$DemoConfig  = Join-Path $ScriptDir 'config\provisa-install.yaml'
$DemoPidFile = Join-Path $ProvisaHome '.demo.pid'
$PetPort     = 18080
$GqlPort     = 4000

function Write-Info { param($Msg) Write-Host "[provisa] $Msg" -ForegroundColor Cyan }
function Write-Err  { param($Msg) Write-Host "[provisa] $Msg" -ForegroundColor Red }
function Write-Ok   { param($Msg) Write-Host "[provisa] $Msg" -ForegroundColor Green }

# Progress breadcrumb for the startup-monitor GUI (see startup-monitor.ps1). Append-only, best-effort:
# a failed progress write must never break start/stop, the one justified SilentlyContinue here.
$StatusFile = Join-Path $ProvisaHome '.startup-status'
function Write-Status {
  param([string]$State, [string]$Msg)
  Add-Content -Path $StatusFile -Value "$State|$Msg" -Encoding UTF8 -ErrorAction SilentlyContinue
}

# -- Config reader (no yaml parser needed; mirrors scripts/provisa read_config) -
function Read-Config {
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

$UiPort        = [int](Read-Config 'ui_port' '3000')
$ApiPort       = [int](Read-Config 'api_port' '8000')
$DeployEngine  = Read-Config 'engine' 'duckdb'
$EngineUrl     = Read-Config 'engine_url' ''
$MaterializeUrl= Read-Config 'materialize_url' ''
$OtlpEndpoint  = Read-Config 'otlp_endpoint' ''
$Demo          = (Read-Config 'demo' 'false') -eq 'true'
$AutoOpen      = (Read-Config 'auto_open_browser' 'true') -eq 'true'

function Require-Runtime {
  if (-not (Test-Path $RuntimePy)) {
    Write-Err "Native runtime missing at $RuntimeDir. Re-run Provisa first-launch to complete setup."
    exit 1
  }
}

# Deployment env for the native process (mirrors scripts/provisa native_env_pairs
# and provisa.core.desktop_profile). SQLite URLs need forward slashes.
function Native-Env {
  $platformDb = (Join-Path $NativeDir 'platform.db') -replace '\\','/'
  $tenantDb   = (Join-Path $NativeDir 'tenant.db')   -replace '\\','/'
  $e = @{
    'PROVISA_ENGINE'        = $DeployEngine
    'PROVISA_REDIS_EMBEDDED' = '1'
    'PLATFORM_DATABASE_URL' = "sqlite+aiosqlite:///$platformDb"
    'TENANT_DATABASE_URL'   = "sqlite+aiosqlite:///$tenantDb"
  }
  # Remote MCP on by default for the native/desktop tier (REQ-1101): a ready same-machine Claude
  # Desktop connector. Loopback bind (127.0.0.1) keeps the always-on server off the LAN. The role
  # is pinned explicitly (not a silent admin default) - native runs auth:none as one local admin
  # user, so the governed MCP role is admin. Override any by exporting the env before launch.
  if (-not $env:PROVISA_MCP_PORT) { $e['PROVISA_MCP_PORT'] = '8009' }
  if (-not $env:PROVISA_MCP_HOST) { $e['PROVISA_MCP_HOST'] = '127.0.0.1' }
  if (-not $env:PROVISA_MCP_ROLE) { $e['PROVISA_MCP_ROLE'] = 'admin' }
  # MCP stays plain HTTP: Claude Desktop reaches a LOCAL server only via the claude_desktop_config
  # stdio bridge (mcp-proxy), which works over http. Its "Add custom connector" URL path is brokered
  # from Anthropic's servers (needs a public internet HTTPS endpoint), so TLS buys nothing locally.
  # TLS remains available opt-in (PROVISA_MCP_TLS=1) for a publicly-exposed deployment (REQ-1106).
  # The bundled runtime python (host-accessible, ships mcp-proxy) that Claude Desktop launches as
  # the Node-free stdio bridge (REQ-1104). Only the native tier sets this - the Explore/MCP panel
  # emits the ready-to-paste config only when it is present.
  $e['PROVISA_MCP_BRIDGE_COMMAND'] = $RuntimePy
  if ($EngineUrl)      { $e['PROVISA_ENGINE_URL'] = $EngineUrl }
  if ($MaterializeUrl) { $e['PROVISA_MATERIALIZE_URL'] = $MaterializeUrl }
  if ($OtlpEndpoint)   { $e['OTEL_EXPORTER_OTLP_ENDPOINT'] = $OtlpEndpoint }
  # Demo: load the bundled native demo config (engine: duckdb, auth: none -> no setup wizard),
  # mark the run as a demo, and point the OpenAPI/GraphQL sources at the local host mock servers.
  # The relative ./demo/files/*.sqlite paths in the config resolve against the app CWD ({app}).
  if ($Demo) {
    $e['PROVISA_CONFIG']    = $DemoConfig
    $e['PROVISA_DEMO']      = '1'
    $e['PETSTORE_BASE_URL'] = "http://localhost:$PetPort/api/v3"
    $e['GRAPHQL_DEMO_URL']  = "http://localhost:$GqlPort/graphql"
  }
  return $e
}

# -- Demo mock servers (native, no Docker) -------------------------------------
# Petstore (OpenAPI) and shelter (GraphQL) run as host Python processes off the bundled runtime,
# replacing the petstore-mock / graphql-demo containers. Federated with two embedded SQLite files.
function Start-DemoServers {
  $petDir = Join-Path $ScriptDir 'demo\petstore_server'
  $gqlDir = Join-Path $ScriptDir 'demo\graphql_server'
  if (-not (Test-Path (Join-Path $petDir 'server.py'))) {
    Write-Err "Demo assets not bundled beside the installer; cannot start the demo."
    return
  }
  $pet = Start-Process -FilePath $RuntimePy `
    -ArgumentList @('-m','uvicorn','server:app','--app-dir',$petDir,'--host','127.0.0.1','--port',"$PetPort") `
    -WindowStyle Hidden -PassThru `
    -RedirectStandardOutput (Join-Path $LogDir 'demo-petstore.log') `
    -RedirectStandardError  (Join-Path $LogDir 'demo-petstore.err.log')
  $gql = Start-Process -FilePath $RuntimePy `
    -ArgumentList @('-m','uvicorn','server:app','--app-dir',$gqlDir,'--host','127.0.0.1','--port',"$GqlPort") `
    -WindowStyle Hidden -PassThru `
    -RedirectStandardOutput (Join-Path $LogDir 'demo-graphql.log') `
    -RedirectStandardError  (Join-Path $LogDir 'demo-graphql.err.log')
  "$($pet.Id)`n$($gql.Id)" | Set-Content -Path $DemoPidFile -Encoding ASCII
  # Wait until both mocks actually answer before returning. The API loads the demo config (and the
  # guided tour queries these sources) against the live endpoints; starting them fire-and-forget
  # raced the API load and left the demo empty. Mirrors demo/run-demo-servers.sh, which curls both
  # before proceeding.
  #
  # Poll BOTH in one shared window (Wait-HttpReadyAll) rather than 30s + 30s sequentially: on a cold
  # first launch off a freshly-staged runtime (Defender scanning every .pyd), the first server's own
  # cold import can eat most of a tight 30s window, so the second falsely "timed out" though it had
  # bound. One 90s concurrent window both fits the cold start and stops the false-failure errors.
  $petUrl = "http://localhost:$PetPort/api/v3/pet/findByStatus?status=available"
  $gqlUrl = "http://localhost:$GqlPort/graphql?query=%7B__typename%7D"
  $notReady = Wait-HttpReadyAll @($petUrl, $gqlUrl) 90
  if ($notReady -contains $petUrl) { Write-Err "Demo petstore server did not become ready on port $PetPort within 90s." }
  if ($notReady -contains $gqlUrl) { Write-Err "Demo graphql server did not become ready on port $GqlPort within 90s." }
  if ($notReady.Count -eq 0) { Write-Info "Demo mock servers ready (petstore :$PetPort, graphql :$GqlPort)." }
  else { Write-Info "Demo mock servers started (petstore :$PetPort, graphql :$GqlPort)." }
}

function Stop-DemoServers {
  if (Test-Path $DemoPidFile) {
    foreach ($procId in (Get-Content $DemoPidFile)) {
      if ($procId) { Stop-Process -Id ([int]$procId) -Force -ErrorAction SilentlyContinue }
    }
    Remove-Item $DemoPidFile -Force -ErrorAction SilentlyContinue
  }
}

function Native-Running {
  if (-not (Test-Path $PidFile)) { return $false }
  foreach ($procId in (Get-Content $PidFile)) {
    if ($procId -and (Get-Process -Id ([int]$procId) -ErrorAction SilentlyContinue)) { return $true }
  }
  return $false
}

# Two uvicorn processes: the API app (packaged factory) and the UI server, which
# proxies API calls to PROVISA_API_URL and serves the built static UI.
function Start-Native {
  Require-Runtime
  New-Item -ItemType Directory -Path $LogDir, $NativeDir -Force | Out-Null
  if (Native-Running) { Write-Info 'Provisa is already running.'; return }

  # The demo mock servers must be up before the API loads the demo config (its OpenAPI/GraphQL
  # sources introspect the live endpoints at startup).
  if ($Demo) { Write-Status 'DEMO' 'Starting demo services'; Start-DemoServers }

  Write-Status 'START' 'Starting the engine and UI'
  $baseEnv = Native-Env
  foreach ($k in $baseEnv.Keys) { Set-Item -Path "Env:$k" -Value $baseEnv[$k] }

  # CWD = {app}: the demo config references ./demo/files/*.sqlite relative to the working dir.
  $api = Start-Process -FilePath $RuntimePy -WorkingDirectory $ScriptDir `
    -ArgumentList @('-m','uvicorn','provisa.api.app:create_app','--factory','--host','0.0.0.0','--port',"$ApiPort") `
    -WindowStyle Hidden -PassThru `
    -RedirectStandardOutput (Join-Path $LogDir 'native-api.log') `
    -RedirectStandardError  (Join-Path $LogDir 'native-api.err.log')

  $env:PROVISA_API_URL = "http://localhost:$ApiPort"
  $ui = Start-Process -FilePath $RuntimePy -WorkingDirectory $ScriptDir `
    -ArgumentList @('-m','uvicorn','provisa.ui_server:app','--host','0.0.0.0','--port',"$UiPort") `
    -WindowStyle Hidden -PassThru `
    -RedirectStandardOutput (Join-Path $LogDir 'native-ui.log') `
    -RedirectStandardError  (Join-Path $LogDir 'native-ui.err.log')

  "$($api.Id)`n$($ui.Id)" | Set-Content -Path $PidFile -Encoding ASCII

  Write-Status 'WAIT' 'Waiting for the engine to become ready'
  Write-Ok 'Provisa is running.'
  Write-Info "UI:  http://localhost:$UiPort"
  Write-Info "API: http://localhost:$ApiPort"
}

function Stop-Native {
  Write-Info 'Stopping Provisa...'
  if (Test-Path $PidFile) {
    foreach ($procId in (Get-Content $PidFile)) {
      if ($procId) { Stop-Process -Id ([int]$procId) -Force -ErrorAction SilentlyContinue }
    }
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
  }
  # Belt-and-suspenders: also kill any orphaned interpreter still running out of the staged runtime
  # (a crashed/prior-install process the PID file no longer tracks would otherwise keep serving).
  Get-Process -ErrorAction SilentlyContinue | Where-Object {
    $_.Path -and ($_.Path -like "$RuntimeDir\*")
  } | Stop-Process -Force -ErrorAction SilentlyContinue
  Stop-DemoServers
  Write-Ok 'Provisa has been shut down.'
}

function Status-Native {
  if (Native-Running) { Write-Ok 'Provisa is running.' } else { Write-Info 'Provisa is not running.' }
}

# Poll an HTTP endpoint until it answers (any status = the server has bound) or the timeout
# elapses. A fixed sleep opened the browser to a connection error (the servers take several
# seconds to bind), which read as "nothing happened". A 4xx/5xx still means the server is up.
function Wait-HttpReady {
  param([string]$Url, [int]$TimeoutSec = 40)
  $deadline = (Get-Date).AddSeconds($TimeoutSec)
  while ((Get-Date) -lt $deadline) {
    try {
      Invoke-WebRequest -UseBasicParsing -TimeoutSec 2 $Url | Out-Null
      return $true
    } catch {
      if ($_.Exception.Response) { return $true }
      Start-Sleep -Milliseconds 500
    }
  }
  return $false
}

# Wait until ALL given URLs answer (any HTTP status = the server has bound) within one shared window,
# polling every cycle. Returns the URLs still unanswered (empty array = all ready). Concurrent by
# construction: N cold servers warm inside the SAME timeout instead of stacking per-server waits.
function Wait-HttpReadyAll {
  param([string[]]$Urls, [int]$TimeoutSec = 90)
  $deadline = (Get-Date).AddSeconds($TimeoutSec)
  $pending = [System.Collections.Generic.List[string]]::new()
  $Urls | ForEach-Object { $pending.Add($_) }
  while (((Get-Date) -lt $deadline) -and ($pending.Count -gt 0)) {
    foreach ($u in @($pending)) {
      try { Invoke-WebRequest -UseBasicParsing -TimeoutSec 2 $u | Out-Null; [void]$pending.Remove($u) }
      catch { if ($_.Exception.Response) { [void]$pending.Remove($u) } }
    }
    if ($pending.Count -gt 0) { Start-Sleep -Milliseconds 500 }
  }
  return ,($pending.ToArray())
}

function Open-Native {
  # Open at ?tour=1 when the demo was installed so the guided tour auto-starts
  # (App.tsx reads the query param), mirroring the macOS launcher.
  $url = if ($Demo) { "http://localhost:$UiPort/?tour=1" } else { "http://localhost:$UiPort" }
  # Gate on the API's /health, not just the UI port. uvicorn only serves /health after the
  # lifespan startup (the heavy demo config-load) completes, so this waits for a genuinely
  # usable API. The UI proxy binds near-instantly and would otherwise open the browser onto a
  # still-loading API whose calls 502 - which is exactly why the demo "final step" looked stuck.
  if (-not (Wait-HttpReady "http://localhost:$ApiPort/health" 120)) {
    Write-Err "API did not become ready on port $ApiPort within 120s; opening anyway."
  } elseif (-not (Wait-HttpReady "http://localhost:$UiPort/" 40)) {
    Write-Err "UI did not become ready on port $UiPort within 40s; opening anyway."
  }
  Write-Info "Opening $url"
  Start-Process $url
}

function Show-Help {
  Write-Host 'Usage: provisa <command>'
  Write-Host ''
  Write-Host 'Commands:'
  Write-Host '  start    Start Provisa (native - no Docker)'
  Write-Host '  stop     Stop Provisa'
  Write-Host '  restart  Restart Provisa'
  Write-Host '  status   Show run status'
  Write-Host '  open     Open the UI in your browser'
  Write-Host '  help     Show this help'
}

# When the startup-monitor GUI is driving the launch (PROVISA_STARTUP_UI=1) it owns the readiness
# wait and opens the browser itself, so the CLI must NOT also open it - that would race and double-
# open. A plain CLI/shortcut start still opens the browser here.
$MonitorDriven = [bool]$env:PROVISA_STARTUP_UI
$command = if ($args.Count -gt 0) { $args[0] } else { 'help' }
switch ($command) {
  'start'   { Start-Native; if ($AutoOpen -and -not $MonitorDriven) { Open-Native } }
  'stop'    { Stop-Native }
  'restart' { Stop-Native; Start-Native; if ($AutoOpen -and -not $MonitorDriven) { Open-Native } }
  'status'  { Status-Native }
  'open'    { Open-Native }
  'help'    { Show-Help }
  default   { Write-Err "Unknown command: $command"; Show-Help; exit 1 }
}
