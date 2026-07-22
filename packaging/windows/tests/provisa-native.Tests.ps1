# Pester tests for the Windows native-tier launcher (provisa-native.ps1).
#
# The script drives Windows-only surfaces (Start-Process -WindowStyle Hidden, wscript, the
# browser), so a true end-to-end run needs Windows. These tests instead load the REAL functions
# out of the script (via AST, skipping its module init + command switch) and exercise their
# logic with the OS cmdlets mocked. Focus: the config reader, the demo/non-demo env, and the
# readiness gating added to fix the demo "final step" hang (Start-DemoServers waits for both
# mocks; Open-Native waits for the API /health, not just the UI proxy).

BeforeAll {
  $sut = Join-Path (Split-Path -Parent $PSScriptRoot) 'provisa-native.ps1'
  if (-not (Test-Path $sut)) { throw "SUT not found: $sut" }

  # Pull each top-level function definition out of the script and define it here, WITHOUT running
  # the module-scope initialization (config read against $env:USERPROFILE) or the trailing switch.
  $parseErr = $null
  $ast = [System.Management.Automation.Language.Parser]::ParseFile($sut, [ref]$null, [ref]$parseErr)
  if ($parseErr -and $parseErr.Count) { throw "parse errors: $($parseErr -join '; ')" }
  $fns = $ast.FindAll(
    { param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] }, $false)
  foreach ($f in $fns) { . ([scriptblock]::Create($f.Extent.Text)) }

  # A .NET-faithful exception whose Response member exists (so the SUT's $_.Exception.Response read
  # is valid). A live-but-erroring server (4xx/5xx) surfaces this with a non-null Response.
  class FakeHttpError : System.Exception {
    [object] $Response
    FakeHttpError([string] $m) : base($m) {}
  }
  $script:FakeHttpErrorType = [FakeHttpError]
}

Describe 'Read-Config' {
  BeforeAll {
    $script:cfg = Join-Path ([System.IO.Path]::GetTempPath()) ("prov-cfg-" + [guid]::NewGuid() + ".yaml")
    @(
      'hostname: localhost'
      'ui_port: 3000'
      'api_port: 8000'
      'auto_open_browser: true'
      'engine: duckdb'
      'engine_url: ""'
      'demo: true'
      'demo_mode: native'
    ) -join "`n" | Set-Content -Path $cfg -Encoding ASCII
  }
  AfterAll { Remove-Item $script:cfg -Force -ErrorAction SilentlyContinue }

  It 'reads a plain scalar' {
    $ConfigPath = $script:cfg
    Read-Config 'hostname' 'fallback' | Should -Be 'localhost'
  }
  It 'reads a numeric-looking scalar as string' {
    $ConfigPath = $script:cfg
    Read-Config 'ui_port' '9999' | Should -Be '3000'
  }
  It 'returns the default for an empty quoted value (engine_url: "")' {
    $ConfigPath = $script:cfg
    # Empty value must fall through to the caller default, not return ''.
    Read-Config 'engine_url' 'DEFAULT' | Should -Be 'DEFAULT'
  }
  It 'returns the default for a missing key' {
    $ConfigPath = $script:cfg
    Read-Config 'nonexistent' 'theDefault' | Should -Be 'theDefault'
  }
  It 'does not confuse key "demo" with "demo_mode"' {
    $ConfigPath = $script:cfg
    Read-Config 'demo' 'false' | Should -Be 'true'
    Read-Config 'demo_mode' 'x' | Should -Be 'native'
  }
  It 'returns the default when the config file is absent' {
    $ConfigPath = Join-Path ([System.IO.Path]::GetTempPath()) 'does-not-exist-xyz.yaml'
    Read-Config 'ui_port' '3000' | Should -Be '3000'
  }
}

Describe 'Wait-HttpReady' {
  It 'returns true when the endpoint answers 2xx' {
    Mock Invoke-WebRequest { [pscustomobject]@{ StatusCode = 200 } }
    Wait-HttpReady 'http://localhost:1/' 5 | Should -BeTrue
    Should -Invoke Invoke-WebRequest -Times 1 -Exactly
  }
  It 'treats a 4xx/5xx (server is up) as ready' {
    Mock Invoke-WebRequest {
      $e = $script:FakeHttpErrorType::new('boom'); $e.Response = 'present'; throw $e
    }
    Wait-HttpReady 'http://localhost:1/' 5 | Should -BeTrue
  }
  It 'returns false and retries while the port refuses, up to the timeout' {
    # WebException has a real (null) Response member -> the connect-refused branch that keeps polling.
    Mock Invoke-WebRequest { throw [System.Net.WebException]::new('refused') }
    $r = Wait-HttpReady 'http://localhost:1/' 1
    $r | Should -BeFalse
    Should -Invoke Invoke-WebRequest -Times 2 -Because 'it must poll more than once before giving up'
  }
}

Describe 'Wait-HttpReadyAll (concurrent readiness window)' {
  It 'returns empty when every endpoint answers' {
    Mock Invoke-WebRequest { [pscustomobject]@{ StatusCode = 200 } }
    (Wait-HttpReadyAll @('http://localhost:1/', 'http://localhost:2/') 5).Count | Should -Be 0
  }
  It 'returns the URLs still unanswered after the timeout' {
    Mock Invoke-WebRequest { throw [System.Net.WebException]::new('refused') }
    $r = Wait-HttpReadyAll @('http://localhost:1/', 'http://localhost:2/') 1
    $r.Count | Should -Be 2
  }
  It 'drops only the endpoints that answer, keeps the rest' {
    Mock Invoke-WebRequest {
      if ($Uri -match ':1/') { [pscustomobject]@{ StatusCode = 200 } }
      else { throw [System.Net.WebException]::new('refused') }
    }
    Wait-HttpReadyAll @('http://localhost:1/', 'http://localhost:2/') 1 | Should -Be @('http://localhost:2/')
  }
}

Describe 'Native-Env' {
  BeforeEach {
    # Backslash path with NO drive letter: Join-Path tolerates it on Linux (a drive-qualified
    # 'C:\...' would raise "drive C does not exist"), yet it still exercises the \ -> / conversion.
    $script:NativeDir     = '\Users\x\.provisa\native'
    $script:DeployEngine  = 'duckdb'
    $script:EngineUrl     = ''
    $script:MaterializeUrl= ''
    $script:OtlpEndpoint  = ''
    $script:Demo          = $false
    $script:DemoConfig    = 'C:\app\config\provisa-install.yaml'
    $script:PetPort       = 18080
    $script:GqlPort       = 4000
  }

  It 'builds sqlite control-plane URLs with forward slashes' {
    $e = Native-Env
    $e['PLATFORM_DATABASE_URL'] | Should -BeLike 'sqlite+aiosqlite:///*'
    $e['PLATFORM_DATABASE_URL'] | Should -Not -Match '\\'
    $e['TENANT_DATABASE_URL']   | Should -BeLike 'sqlite+aiosqlite:///*'
    $e['PROVISA_ENGINE']        | Should -Be 'duckdb'
    $e['PROVISA_REDIS_EMBEDDED']| Should -Be '1'
  }
  It 'omits demo keys when not a demo install' {
    $e = Native-Env
    $e.ContainsKey('PROVISA_CONFIG') | Should -BeFalse
    $e.ContainsKey('PROVISA_DEMO')   | Should -BeFalse
  }
  It 'adds engine/materialize URLs only when set' {
    (Native-Env).ContainsKey('PROVISA_ENGINE_URL') | Should -BeFalse
    $script:EngineUrl = 'postgresql+psycopg://u:p@h:5432/db'
    $script:MaterializeUrl = 'duckdb:///mat'
    $e = Native-Env
    $e['PROVISA_ENGINE_URL']      | Should -Be 'postgresql+psycopg://u:p@h:5432/db'
    $e['PROVISA_MATERIALIZE_URL'] | Should -Be 'duckdb:///mat'
  }
  It 'wires the demo config + mock URLs when demo is on' {
    $script:Demo = $true
    $e = Native-Env
    $e['PROVISA_CONFIG']    | Should -Be 'C:\app\config\provisa-install.yaml'
    $e['PROVISA_DEMO']      | Should -Be '1'
    $e['PETSTORE_BASE_URL'] | Should -Be 'http://localhost:18080/api/v3'
    $e['GRAPHQL_DEMO_URL']  | Should -Be 'http://localhost:4000/graphql'
  }
}

Describe 'Start-DemoServers (readiness gate - the demo fix)' {
  BeforeEach {
    # Drive-less paths so Join-Path works under the Linux test host (see Native-Env note).
    $script:ScriptDir   = '/app'
    $script:LogDir      = '/home/.provisa/.logs'
    $script:RuntimePy   = '/home/.provisa/runtime/python.exe'
    $script:PetPort     = 18080
    $script:GqlPort     = 4000
    $script:DemoPidFile = '/home/.provisa/.demo.pid'
    Mock Test-Path { $true }                              # server.py present
    Mock Start-Process { [pscustomobject]@{ Id = 4242 } }
    Mock Set-Content { }
    Mock Write-Info { }
    Mock Write-Err  { }
  }

  It 'waits for BOTH mock endpoints in one concurrent window' {
    Mock Wait-HttpReadyAll { @() }
    Start-DemoServers
    Should -Invoke Wait-HttpReadyAll -Times 1 -Exactly
    Should -Invoke Wait-HttpReadyAll -Times 1 -ParameterFilter {
      (($Urls -join ' ') -match ':18080/api/v3/pet/findByStatus') -and (($Urls -join ' ') -match ':4000/graphql')
    }
  }
  It 'launches both uvicorn mock servers' {
    Mock Wait-HttpReadyAll { @() }
    Start-DemoServers
    Should -Invoke Start-Process -Times 2 -Exactly
  }
  It 'reports an error for each mock that never becomes ready' {
    Mock Wait-HttpReadyAll {
      @('http://localhost:18080/api/v3/pet/findByStatus?status=available',
        'http://localhost:4000/graphql?query=%7B__typename%7D')
    }
    Start-DemoServers
    Should -Invoke Write-Err -Times 2 -Exactly -Because 'both petstore and graphql failed readiness'
  }
  It 'aborts early (no launch) when demo assets are missing' {
    Mock Test-Path { $false }
    Mock Wait-HttpReadyAll { @() }
    Start-DemoServers
    Should -Invoke Start-Process -Times 0 -Exactly
    Should -Invoke Write-Err -Times 1 -Exactly
  }
}

Describe 'Open-Native (browser gated on the API, not the UI proxy)' {
  BeforeEach {
    $script:UiPort  = 3000
    $script:ApiPort = 8000
    $script:Demo    = $false
    Mock Start-Process { }
    Mock Write-Info { }
    Mock Write-Err  { }
  }

  It 'waits on the API /health before opening' {
    Mock Wait-HttpReady { $true }
    Open-Native
    Should -Invoke Wait-HttpReady -Times 1 -ParameterFilter { $Url -eq 'http://localhost:8000/health' }
  }
  It 'opens the plain UI url for a non-demo install' {
    Mock Wait-HttpReady { $true }
    Open-Native
    Should -Invoke Start-Process -Times 1 -ParameterFilter { $FilePath -eq 'http://localhost:3000' }
  }
  It 'opens ?tour=1 for a demo install' {
    $script:Demo = $true
    Mock Wait-HttpReady { $true }
    Open-Native
    Should -Invoke Start-Process -Times 1 -ParameterFilter { $FilePath -eq 'http://localhost:3000/?tour=1' }
  }
  It 'still opens (with a warning) if the API never reports healthy' {
    Mock Wait-HttpReady { $false }   # both health and UI probes fail
    Open-Native
    Should -Invoke Write-Err -Times 1 -Exactly
    Should -Invoke Start-Process -Times 1 -Exactly
  }
}

Describe 'Native-Running' {
  BeforeEach { $script:PidFile = 'C:\home\.provisa\.native.pid' }

  It 'is false when no pid file exists' {
    Mock Test-Path { $false }
    Native-Running | Should -BeFalse
  }
  It 'is true when a tracked pid is alive' {
    Mock Test-Path { $true }
    Mock Get-Content { '4242' }
    Mock Get-Process { [pscustomobject]@{ Id = 4242 } }
    Native-Running | Should -BeTrue
  }
  It 'is false when the tracked pid is dead' {
    Mock Test-Path { $true }
    Mock Get-Content { '4242' }
    Mock Get-Process { $null }
    Native-Running | Should -BeFalse
  }
}
