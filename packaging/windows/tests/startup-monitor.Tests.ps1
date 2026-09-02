# Pester tests for the Windows startup-monitor splash (startup-monitor.ps1).
#
# The GUI (WinForms) needs Windows, so these load the REAL pure helpers out of the script via AST
# (skipping module init + the Application.Run at the tail) and exercise their logic. Focus: the
# breadcrumb parser, the last-status reader (including a locked/missing file), and the open URL.

BeforeAll {
  $sut = Join-Path (Split-Path -Parent $PSScriptRoot) 'startup-monitor.ps1'
  if (-not (Test-Path $sut)) { throw "SUT not found: $sut" }
  $parseErr = $null
  $ast = [System.Management.Automation.Language.Parser]::ParseFile($sut, [ref]$null, [ref]$parseErr)
  if ($parseErr -and $parseErr.Count) { throw "parse errors: $($parseErr -join '; ')" }
  $fns = $ast.FindAll(
    { param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] }, $false)
  foreach ($f in $fns) { . ([scriptblock]::Create($f.Extent.Text)) }
}

Describe 'Parse-Status' {
  It 'splits a STATE|message breadcrumb' {
    $p = Parse-Status 'STAGING|Staging the runtime'
    $p.State   | Should -Be 'STAGING'
    $p.Message | Should -Be 'Staging the runtime'
  }
  It 'keeps a message that itself contains a pipe' {
    (Parse-Status 'ERROR|a|b').Message | Should -Be 'a|b'
  }
  It 'treats a bare token as a state with no message' {
    $p = Parse-Status 'WAIT'
    $p.State   | Should -Be 'WAIT'
    $p.Message | Should -Be ''
  }
  It 'returns null for an empty line' {
    Parse-Status '' | Should -BeNullOrEmpty
  }
}

Describe 'Get-LastStatus' {
  BeforeEach {
    $script:tmp = Join-Path ([System.IO.Path]::GetTempPath()) ("prov-status-" + [guid]::NewGuid())
  }
  AfterEach { Remove-Item $script:tmp -Force -ErrorAction SilentlyContinue }

  It 'returns the last non-empty line (ignoring trailing blanks)' {
    Set-Content -Path $script:tmp -Value @('STAGING|a', 'CONFIG|b', '')
    Get-LastStatus $script:tmp | Should -Be 'CONFIG|b'
  }
  It 'returns null when the file does not exist' {
    Get-LastStatus (Join-Path ([System.IO.Path]::GetTempPath()) 'does-not-exist-xyz') | Should -BeNullOrEmpty
  }
  It 'returns null for an empty file' {
    Set-Content -Path $script:tmp -Value ''
    Get-LastStatus $script:tmp | Should -BeNullOrEmpty
  }
}

Describe 'Resolve-OpenUrl' {
  It 'appends ?tour=1 for a demo install (auto-starts the guided tour)' {
    Resolve-OpenUrl -UiPort 3000 -Demo $true | Should -Be 'http://localhost:3000/?tour=1'
  }
  It 'opens the plain UI url for a non-demo install' {
    Resolve-OpenUrl -UiPort 3000 -Demo $false | Should -Be 'http://localhost:3000'
  }
  It 'honors a non-default UI port' {
    Resolve-OpenUrl -UiPort 8080 -Demo $false | Should -Be 'http://localhost:8080'
  }
}

Describe 'Test-ServerDead' {
  BeforeEach {
    $script:pidf = Join-Path ([System.IO.Path]::GetTempPath()) ("prov-pid-" + [guid]::NewGuid())
  }
  AfterEach { Remove-Item $script:pidf -Force -ErrorAction SilentlyContinue }

  It 'is not dead while the pid is still running (a booting engine has not bound its port yet)' {
    Set-Content -Path $script:pidf -Value @("$PID", "$PID")
    Test-ServerDead $script:pidf 0 | Should -BeFalse
    Test-ServerDead $script:pidf 1 | Should -BeFalse
  }
  It 'is dead once the pid is gone' {
    # No -WindowStyle: it is a Windows-only Start-Process parameter and these helper tests also run
    # on the CI's non-Windows pwsh, where passing it throws NotSupportedException.
    $p = Start-Process -FilePath (Get-Process -Id $PID).Path -ArgumentList @('-NoProfile','-Command','exit') -PassThru
    $p.WaitForExit()
    Set-Content -Path $script:pidf -Value @("$($p.Id)")
    Test-ServerDead $script:pidf 0 | Should -BeTrue
  }
  It 'is not dead when the pid file does not exist yet (the worker has not launched the servers)' {
    Test-ServerDead (Join-Path ([System.IO.Path]::GetTempPath()) 'no-such-pid-file-xyz') 0 | Should -BeFalse
  }
  It 'is not dead when the file has no line for that server yet' {
    Set-Content -Path $script:pidf -Value @("$PID")
    Test-ServerDead $script:pidf 1 | Should -BeFalse
  }
  It 'is not dead when the line is blank' {
    Set-Content -Path $script:pidf -Value @('', '')
    Test-ServerDead $script:pidf 0 | Should -BeFalse
  }
}
