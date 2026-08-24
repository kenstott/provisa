<#
.SYNOPSIS
    Provisions the Microsoft Graph app registration Provisa uses to send platform mail (REQ-1576).

.DESCRIPTION
    Brings the "Provisa" Entra app to exactly the state the mail adapter in provisa/core/mail.py
    needs, and no further:

      1. Removes the four Application-scoped Mail.Read* / Mail.ReadWrite permissions. The adapter
         only POSTs /sendMail; every read permission is tenant-wide ("all mailboxes"), so consenting
         to one would hand the client secret every mailbox in the tenant.
      2. Leaves Mail.Send as the sole Application permission and grants tenant admin consent for it,
         which is what turns the current "Not granted" state into a working app-only send.
      3. Ensures the sending mailbox exists as a shared mailbox. Graph app-only send requires a real
         mailbox, not an alias.
      4. Restricts Mail.Send to that one mailbox with an Exchange ApplicationAccessPolicy. Without
         this the permission means "send as any user in the tenant".
      5. Verifies the policy with Test-ApplicationAccessPolicy.

    Idempotent: each step checks the live state first and skips work already done.

.PARAMETER AppId
    Application (client) ID of the "Provisa" registration.

.PARAMETER SenderAddress
    The mailbox every Provisa message is sent from; defaults to invites@provisa.dev. Must match
    mail.from_address in the deployment config.

.PARAMETER ScopeGroupName
    Mail-enabled security group holding the sender mailbox; the access policy is scoped to it.

.PARAMETER WhatIfOnly
    Report what each step would change and exit without writing anything.

.EXAMPLE
    # Dry run. The application id is positional, so it needs no switch and no quoting.
    ./scripts/setup-graph-mail.ps1 11111111-2222-3333-4444-555555555555 -WhatIfOnly

.EXAMPLE
    # Apply, against a sender other than the default invites@provisa.dev.
    ./scripts/setup-graph-mail.ps1 -AppId 11111111-2222-3333-4444-555555555555 `
        -SenderAddress platform@provisa.dev

.NOTES
    Requires PowerShell 7+, the Microsoft.Graph and ExchangeOnlineManagement modules, and an
    account holding both Global Administrator (for the consent grant) and Exchange Administrator
    (for the mailbox and the access policy).
#>
[CmdletBinding()]
param(
    # Positional, so the GUID can be passed bare: ./setup-graph-mail.ps1 <guid>
    [Parameter(Mandatory, Position = 0,
        HelpMessage = 'Application (client) ID from Entra -> App registrations -> Provisa -> Overview')]
    [ValidatePattern('^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$',
        ErrorMessage = "'{0}' is not an application id. Copy the Application (client) ID GUID from the Provisa app's Overview blade.")]
    [string]$AppId,

    [Parameter(Position = 1)]
    [ValidatePattern('^[^@\s]+@[^@\s]+\.[^@\s]+$')]
    [string]$SenderAddress = 'invites@provisa.dev',

    [string]$ScopeGroupName = 'ProvisaSenders',

    [switch]$WhatIfOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# The Graph resource itself, and the ids of the app-role permissions on it. These are the same in
# every tenant -- they are Microsoft's identifiers, not per-directory objects.
$GraphAppId = '00000003-0000-0000-c000-000000000000'
$MailSendId = 'b633e1c5-b582-4048-a93e-9f11b44c7e96'
$UnwantedMailRoles = @{
    'e2a3a72e-5f79-4c64-b1b1-878b674786c9' = 'Mail.ReadWrite'
    '810c84a8-4a9e-49e6-bf7d-12d183f40d01' = 'Mail.Read'
    '6be147d2-ea4f-4b5a-a3fa-3eab6f3c140a' = 'Mail.ReadBasic'
    '693c5e45-0940-467d-9b8a-1022fb9d42ef' = 'Mail.ReadBasic.All'
}

function Write-Step { param([string]$Text) Write-Host "`n== $Text" -ForegroundColor Cyan }
function Write-Done { param([string]$Text) Write-Host "   $Text" -ForegroundColor Green }
function Write-Skip { param([string]$Text) Write-Host "   $Text" -ForegroundColor DarkGray }

function Assert-Module {
    param([string]$Name)
    if (-not (Get-Module -ListAvailable -Name $Name)) {
        throw "Module '$Name' is not installed. Install-Module $Name -Scope CurrentUser"
    }
}

Write-Step 'Checking prerequisites'
Assert-Module 'Microsoft.Graph.Applications'
Assert-Module 'ExchangeOnlineManagement'
Write-Done 'Microsoft.Graph.Applications and ExchangeOnlineManagement are present'

Write-Step 'Connecting to Microsoft Graph'
Connect-MgGraph -Scopes 'Application.ReadWrite.All', 'AppRoleAssignment.ReadWrite.All', 'Directory.ReadWrite.All' -NoWelcome
$tenant = (Get-MgContext).TenantId
Write-Done "Connected to tenant $tenant"

$app = Get-MgApplication -Filter "appId eq '$AppId'"
if (-not $app) { throw "No application registration found with appId '$AppId' in tenant $tenant." }
Write-Done "Application '$($app.DisplayName)' ($($app.Id))"

# ---------------------------------------------------------------------------
# 1. Strip the read permissions off the registration.
# ---------------------------------------------------------------------------
Write-Step 'Removing Application-scoped mail read permissions'

$requiredAccess = @($app.RequiredResourceAccess)
$graphAccess = $requiredAccess | Where-Object { $_.ResourceAppId -eq $GraphAppId }
if (-not $graphAccess) { throw "The registration declares no Microsoft Graph permissions at all; expected Mail.Send." }

$doomed = @($graphAccess.ResourceAccess | Where-Object { $UnwantedMailRoles.ContainsKey($_.Id) })
if ($doomed.Count -eq 0) {
    Write-Skip 'No Mail.Read* / Mail.ReadWrite permissions are declared'
}
else {
    foreach ($role in $doomed) { Write-Host "   - $($UnwantedMailRoles[$role.Id])" -ForegroundColor Yellow }
    if ($WhatIfOnly) {
        Write-Skip 'WhatIfOnly: registration left untouched'
    }
    else {
        $kept = @($graphAccess.ResourceAccess | Where-Object { -not $UnwantedMailRoles.ContainsKey($_.Id) })
        $rewritten = @($requiredAccess | Where-Object { $_.ResourceAppId -ne $GraphAppId })
        if ($kept.Count -gt 0) {
            $rewritten += @{
                resourceAppId  = $GraphAppId
                resourceAccess = @($kept | ForEach-Object { @{ id = $_.Id; type = $_.Type } })
            }
        }
        Update-MgApplication -ApplicationId $app.Id -RequiredResourceAccess $rewritten
        Write-Done "Removed $($doomed.Count) permission(s)"
    }
}

if (-not ($graphAccess.ResourceAccess | Where-Object { $_.Id -eq $MailSendId })) {
    throw "Mail.Send is not declared on the registration. Add it under API permissions -> Microsoft Graph -> Application permissions, then re-run."
}
Write-Done 'Mail.Send is declared'

# ---------------------------------------------------------------------------
# 2. Grant admin consent for Mail.Send, and revoke consent for anything else.
# ---------------------------------------------------------------------------
Write-Step 'Granting tenant admin consent for Mail.Send'

$sp = Get-MgServicePrincipal -Filter "appId eq '$AppId'"
if (-not $sp) {
    if ($WhatIfOnly) { Write-Skip 'WhatIfOnly: would create the service principal'; }
    else {
        $sp = New-MgServicePrincipal -AppId $AppId
        Write-Done "Created service principal $($sp.Id)"
    }
}
$graphSp = Get-MgServicePrincipal -Filter "appId eq '$GraphAppId'"

if ($sp) {
    $assignments = @(Get-MgServicePrincipalAppRoleAssignment -ServicePrincipalId $sp.Id |
        Where-Object { $_.ResourceId -eq $graphSp.Id })

    $stale = @($assignments | Where-Object { $UnwantedMailRoles.ContainsKey($_.AppRoleId) })
    foreach ($grant in $stale) {
        if ($WhatIfOnly) { Write-Skip "WhatIfOnly: would revoke consent for $($UnwantedMailRoles[$grant.AppRoleId])" }
        else {
            Remove-MgServicePrincipalAppRoleAssignment -ServicePrincipalId $sp.Id -AppRoleAssignmentId $grant.Id
            Write-Done "Revoked consent for $($UnwantedMailRoles[$grant.AppRoleId])"
        }
    }

    if ($assignments | Where-Object { $_.AppRoleId -eq $MailSendId }) {
        Write-Skip 'Mail.Send is already consented'
    }
    elseif ($WhatIfOnly) {
        Write-Skip 'WhatIfOnly: would grant admin consent for Mail.Send'
    }
    else {
        New-MgServicePrincipalAppRoleAssignment -ServicePrincipalId $sp.Id `
            -PrincipalId $sp.Id -ResourceId $graphSp.Id -AppRoleId $MailSendId | Out-Null
        Write-Done 'Granted admin consent for Mail.Send'
    }
}

# ---------------------------------------------------------------------------
# 3. The sending mailbox.
# ---------------------------------------------------------------------------
Write-Step 'Connecting to Exchange Online'
Connect-ExchangeOnline -ShowBanner:$false
Write-Done 'Connected'

Write-Step "Ensuring the sending mailbox $SenderAddress exists"
$mailboxAlias = ($SenderAddress -split '@')[0]
$mailbox = Get-Mailbox -Identity $SenderAddress -ErrorAction SilentlyContinue
if ($mailbox) {
    Write-Skip "$($mailbox.RecipientTypeDetails) mailbox already exists"
}
elseif ($WhatIfOnly) {
    Write-Skip 'WhatIfOnly: would create a shared mailbox'
}
else {
    # Shared, because app-only send needs a real mailbox but no interactive sign-in and no licence.
    $mailbox = New-Mailbox -Shared -Name $mailboxAlias -DisplayName 'Provisa' -PrimarySmtpAddress $SenderAddress
    Write-Done "Created shared mailbox $SenderAddress"
}

# ---------------------------------------------------------------------------
# 4. Scope Mail.Send to that mailbox alone.
# ---------------------------------------------------------------------------
Write-Step "Scoping Mail.Send to $SenderAddress"

$domain = ($SenderAddress -split '@')[1]
$groupAddress = "$ScopeGroupName@$domain"
$group = Get-DistributionGroup -Identity $groupAddress -ErrorAction SilentlyContinue
if ($group) {
    Write-Skip "Security group $groupAddress already exists"
}
elseif ($WhatIfOnly) {
    Write-Skip "WhatIfOnly: would create security group $groupAddress"
}
else {
    $group = New-DistributionGroup -Name $ScopeGroupName -PrimarySmtpAddress $groupAddress -Type Security `
        -Members $SenderAddress -Notes 'Mailboxes the Provisa app may send as (REQ-1576)'
    Write-Done "Created security group $groupAddress"
}

if ($group -and -not $WhatIfOnly) {
    $members = @(Get-DistributionGroupMember -Identity $groupAddress | Select-Object -ExpandProperty PrimarySmtpAddress)
    if ($members -notcontains $SenderAddress) {
        Add-DistributionGroupMember -Identity $groupAddress -Member $SenderAddress
        Write-Done "Added $SenderAddress to $ScopeGroupName"
    }
    else {
        Write-Skip "$SenderAddress is already a member"
    }
}

$policy = Get-ApplicationAccessPolicy -ErrorAction SilentlyContinue |
    Where-Object { $_.AppId -eq $AppId -and $_.AccessRight -eq 'RestrictAccess' }
if ($policy) {
    Write-Skip "RestrictAccess policy already scopes the app to $($policy.ScopeIdentity)"
}
elseif ($WhatIfOnly) {
    Write-Skip 'WhatIfOnly: would create the RestrictAccess policy'
}
else {
    New-ApplicationAccessPolicy -AppId $AppId -PolicyScopeGroupId $groupAddress `
        -AccessRight RestrictAccess -Description 'Provisa invite sender only (REQ-1576)' | Out-Null
    Write-Done "Restricted the app to members of $ScopeGroupName"
}

# ---------------------------------------------------------------------------
# 5. Verify.
# ---------------------------------------------------------------------------
Write-Step 'Verifying'
if ($WhatIfOnly) {
    Write-Skip 'WhatIfOnly: nothing was changed, so nothing is verified'
    Disconnect-ExchangeOnline -Confirm:$false
    return
}

# Policy replication through Exchange is not instant; a fresh policy reads as "no policy" for a
# few minutes before it takes effect. Poll rather than fail on the first read.
$deadline = (Get-Date).AddMinutes(10)
do {
    $granted = Test-ApplicationAccessPolicy -Identity $SenderAddress -AppId $AppId
    if ($granted.AccessCheckResult -eq 'Granted') { break }
    Write-Skip "Access check reads '$($granted.AccessCheckResult)'; waiting for policy replication"
    Start-Sleep -Seconds 30
} while ((Get-Date) -lt $deadline)

if ($granted.AccessCheckResult -ne 'Granted') {
    throw "Test-ApplicationAccessPolicy still reports '$($granted.AccessCheckResult)' for $SenderAddress after 10 minutes."
}
Write-Done "$SenderAddress : Granted"

# A control mailbox, to prove the policy restricts rather than merely exists. Only real user and
# shared mailboxes qualify: the system recipients Exchange keeps (DiscoverySearchMailbox and the
# arbitration set) carry identities Test-ApplicationAccessPolicy cannot resolve.
$control = Get-Mailbox -ResultSize 50 |
    Where-Object {
        $_.RecipientTypeDetails -in @('UserMailbox', 'SharedMailbox') -and
        [string]$_.PrimarySmtpAddress -ne $SenderAddress
    } | Select-Object -First 1

if (-not $control) {
    Write-Skip 'No second mailbox in the tenant, so the restriction has no control to test against'
}
else {
    # -Identity takes a string; the property is an SmtpAddress object, and passing it unconverted
    # is what Exchange reports as "Unrecognized Guid format".
    $controlAddress = [string]$control.PrimarySmtpAddress
    $denied = Test-ApplicationAccessPolicy -Identity $controlAddress -AppId $AppId
    if ($denied.AccessCheckResult -eq 'Granted') {
        throw "The app can still send as $controlAddress; the access policy is not restricting it."
    }
    Write-Done "$controlAddress : $($denied.AccessCheckResult)"
}

$final = @(Get-MgServicePrincipalAppRoleAssignment -ServicePrincipalId $sp.Id |
    Where-Object { $_.ResourceId -eq $graphSp.Id } | ForEach-Object { $_.AppRoleId })
if ($final.Count -ne 1 -or $final[0] -ne $MailSendId) {
    throw "Expected exactly one consented Graph application permission (Mail.Send); found $($final.Count)."
}
Write-Done 'Mail.Send is the only consented Graph application permission'

Disconnect-ExchangeOnline -Confirm:$false

Write-Host "`nDone. Point the deployment at Graph with:" -ForegroundColor Cyan
Write-Host @"
   mail.provider                    = microsoft365
   mail.from_address                = $SenderAddress
   mail.microsoft365.sender         = $SenderAddress
   mail.microsoft365.tenant_id      = $tenant
   mail.microsoft365.client_id      = $AppId
   mail.microsoft365.client_secret  = \${env:PROVISA_MAIL_CLIENT_SECRET}
"@
