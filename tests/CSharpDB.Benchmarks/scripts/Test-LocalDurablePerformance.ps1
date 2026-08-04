#requires -Version 7.0

[CmdletBinding()]
param(
    [string] $PreviousRef = '',

    [string] $CandidateRef = 'HEAD',

    [string] $OutputPath = '',

    [ValidateSet(3, 5, 7, 9)]
    [int] $RepeatCount = 3,

    [ValidateRange(0, 3600)]
    [int] $PostBuildQuiescenceSeconds = 30,

    [ValidateRange(0, 100)]
    [double] $MaxThroughputRegressionPercent = 15,

    [ValidateRange(0, 500)]
    [double] $MaxP99RegressionPercent = 25,

    [ValidateRange(0, 1000)]
    [double] $MaxP99RegressionMilliseconds = 0.05,

    [ValidateSet('P95', 'P99')]
    [string] $BlockingLatencyPercentile = 'P95',

    [switch] $ConfirmDedicatedFixedSsd,

    [string] $GitHubRepository = '',

    [switch] $NoGitHubStatus
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$statusPolicy = 'durable-v2'
$canonicalRepeatCount = 3
$canonicalPostBuildQuiescenceSeconds = 30
$canonicalMaxThroughputRegressionPercent = 15.0
$canonicalMaxP99RegressionPercent = 25.0
$canonicalMaxP99RegressionMilliseconds = 0.05
$canonicalBlockingLatencyPercentile = 'P95'

if (-not $IsWindows) {
    throw 'Local durable performance qualification requires a dedicated Windows machine with a fixed SSD.'
}
if (-not $ConfirmDedicatedFixedSsd) {
    throw (
        'Confirm that this Windows machine is idle and its temporary directory is on a fixed SSD, ' +
        'then rerun with -ConfirmDedicatedFixedSsd.')
}
if (-not $NoGitHubStatus) {
    $nonCanonicalSettings = [Collections.Generic.List[string]]::new()
    if (-not [string]::IsNullOrWhiteSpace($PreviousRef)) {
        $nonCanonicalSettings.Add('PreviousRef must be automatically discovered')
    }
    if ($RepeatCount -ne $canonicalRepeatCount) {
        $nonCanonicalSettings.Add("RepeatCount must be $canonicalRepeatCount")
    }
    if ($PostBuildQuiescenceSeconds -ne $canonicalPostBuildQuiescenceSeconds) {
        $nonCanonicalSettings.Add(
            "PostBuildQuiescenceSeconds must be $canonicalPostBuildQuiescenceSeconds")
    }
    if ($MaxThroughputRegressionPercent -ne $canonicalMaxThroughputRegressionPercent) {
        $nonCanonicalSettings.Add(
            "MaxThroughputRegressionPercent must be $canonicalMaxThroughputRegressionPercent")
    }
    if ($MaxP99RegressionPercent -ne $canonicalMaxP99RegressionPercent) {
        $nonCanonicalSettings.Add(
            "MaxP99RegressionPercent must be $canonicalMaxP99RegressionPercent")
    }
    if ($MaxP99RegressionMilliseconds -ne $canonicalMaxP99RegressionMilliseconds) {
        $nonCanonicalSettings.Add(
            "MaxP99RegressionMilliseconds must be $canonicalMaxP99RegressionMilliseconds")
    }
    if ($BlockingLatencyPercentile -cne $canonicalBlockingLatencyPercentile) {
        $nonCanonicalSettings.Add(
            "BlockingLatencyPercentile must be $canonicalBlockingLatencyPercentile")
    }
    if ($nonCanonicalSettings.Count -gt 0) {
        throw (
            "The official local durable status requires canonical policy '$statusPolicy': " +
            "$($nonCanonicalSettings -join '; '). Use -NoGitHubStatus for diagnostic overrides.")
    }
}

$scriptDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
$repositoryRoot = [IO.Path]::GetFullPath(
    [IO.Path]::Combine($scriptDirectory, '..', '..', '..'))
$comparisonScript = Join-Path $scriptDirectory 'Test-PreviousReleasePerformance.ps1'
if (-not (Test-Path -LiteralPath $comparisonScript -PathType Leaf)) {
    throw "Previous-release performance script not found: $comparisonScript"
}

function Invoke-Git {
    param(
        [Parameter(Mandatory)]
        [string[]] $Arguments,

        [Parameter(Mandatory)]
        [string] $FailureMessage
    )

    $output = @(& git -C $repositoryRoot @Arguments 2>&1)
    if ($LASTEXITCODE -ne 0) {
        $details = ($output | ForEach-Object { [string] $_ }) -join [Environment]::NewLine
        if ([string]::IsNullOrWhiteSpace($details)) {
            throw $FailureMessage
        }
        throw "$FailureMessage$([Environment]::NewLine)$details"
    }

    return ($output | ForEach-Object { [string] $_ }) -join [Environment]::NewLine
}

function Invoke-GitHubStatus {
    param(
        [Parameter(Mandatory)]
        [ValidateSet('pending', 'success', 'failure')]
        [string] $State,

        [Parameter(Mandatory)]
        [string] $Description
    )

    $statusOutput = @(
        & gh api `
            --method POST `
            "repos/$GitHubRepository/statuses/$candidateCommit" `
            --field "state=$State" `
            --field "context=$statusContext" `
            --field "description=$Description" `
            --silent 2>&1
    )
    if ($LASTEXITCODE -ne 0) {
        $details = ($statusOutput | ForEach-Object { [string] $_ }) -join [Environment]::NewLine
        throw "Could not publish GitHub status '$statusContext' for $candidateCommit. $details"
    }
}

function Get-PendingRestartReasons {
    $reasons = [Collections.Generic.List[string]]::new()

    if (Test-Path -LiteralPath `
        'HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Component Based Servicing\RebootPending') {
        $reasons.Add('Component Based Servicing reports a pending restart')
    }
    if (Test-Path -LiteralPath `
        'HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\WindowsUpdate\Auto Update\RebootRequired') {
        $reasons.Add('Windows Update reports a pending restart')
    }

    return @($reasons)
}

function Get-PendingFileRenameOperationsSnapshot {
    try {
        $sessionManager = Get-Item `
            -LiteralPath 'HKLM:\SYSTEM\CurrentControlSet\Control\Session Manager' `
            -ErrorAction Stop
        $value = $sessionManager.GetValue(
            'PendingFileRenameOperations',
            $null,
            [Microsoft.Win32.RegistryValueOptions]::DoNotExpandEnvironmentNames)
    }
    catch {
        throw "Could not inspect pending file rename operations. $($_.Exception.Message)"
    }

    if ($null -eq $value) {
        return @()
    }
    if ($value -isnot [string] -and $value -isnot [string[]]) {
        throw (
            'PendingFileRenameOperations has an unsupported registry value type: ' +
            $value.GetType().FullName)
    }

    return @([string[]] $value)
}

function Get-PendingFileRenamePolicyReasons {
    param(
        [Parameter(Mandatory)]
        [AllowEmptyCollection()]
        [AllowEmptyString()]
        [string[]] $Snapshot
    )

    $reasons = [Collections.Generic.List[string]]::new()
    if (($Snapshot.Count % 2) -ne 0) {
        $reasons.Add(
            "PendingFileRenameOperations is malformed: expected source/destination pairs, " +
            "but found $($Snapshot.Count) entries")
        return @($reasons)
    }

    for ($index = 0; $index -lt $Snapshot.Count; $index += 2) {
        $source = $Snapshot[$index]
        $destination = $Snapshot[$index + 1]
        $operationNumber = ($index / 2) + 1
        if ([string]::IsNullOrWhiteSpace($source)) {
            $reasons.Add(
                "Pending file operation $operationNumber is malformed because its source is empty")
        }
        if (-not [string]::IsNullOrEmpty($destination)) {
            $reasons.Add(
                "Pending file operation $operationNumber is a rename or replacement, not " +
                'a deletion-only cleanup')
        }
    }

    return @($reasons)
}

function Get-PendingFileRenameChangeReasons {
    param(
        [Parameter(Mandatory)]
        [AllowEmptyCollection()]
        [AllowEmptyString()]
        [string[]] $Baseline,

        [Parameter(Mandatory)]
        [AllowEmptyCollection()]
        [AllowEmptyString()]
        [string[]] $Current
    )

    if ($Baseline.Count -ne $Current.Count) {
        return @(
            "Pending file operations changed from $($Baseline.Count) to $($Current.Count) entries")
    }

    for ($index = 0; $index -lt $Baseline.Count; $index++) {
        if (-not $Baseline[$index].Equals(
            $Current[$index],
            [StringComparison]::Ordinal)) {
            return @("Pending file operations changed at entry $($index + 1)")
        }
    }

    return @()
}

function Get-PendingFileRenameFingerprint {
    param(
        [Parameter(Mandatory)]
        [AllowEmptyCollection()]
        [AllowEmptyString()]
        [string[]] $Snapshot
    )

    $serializedEntries = @(
        $Snapshot | ForEach-Object { "$($_.Length):$_" }
    )
    $serialized = [string]::Join("`0", $serializedEntries)
    $bytes = [Text.Encoding]::UTF8.GetBytes($serialized)
    return [Convert]::ToHexString(
        [Security.Cryptography.SHA256]::HashData($bytes)).ToLowerInvariant()
}

function Get-MsiInstallerTransactionEvents {
    try {
        $bootUtc = [DateTimeOffset]::UtcNow.Subtract(
            [TimeSpan]::FromMilliseconds([Environment]::TickCount64)).AddSeconds(-5)
        return @(
            Get-WinEvent `
                -FilterHashtable @{
                    LogName = 'Application'
                    ProviderName = 'MsiInstaller'
                    Id = @(1040, 1042)
                    StartTime = $bootUtc.LocalDateTime
                } `
                -ErrorAction Stop |
                Sort-Object RecordId
        )
    }
    catch {
        if ($_.FullyQualifiedErrorId -like 'NoMatchingEventsFound*') {
            return @()
        }
        throw "Could not inspect Windows Installer transactions. $($_.Exception.Message)"
    }
}

function Get-MsiInstallerTransactionKey {
    param(
        [Parameter(Mandatory)]
        $Event
    )

    if ($Event.Id -notin 1040, 1042 -or
        $Event.Properties.Count -lt 2 -or
        [string]::IsNullOrWhiteSpace([string] $Event.Properties[0].Value) -or
        [string]::IsNullOrWhiteSpace([string] $Event.Properties[1].Value)) {
        throw "MsiInstaller event record $($Event.RecordId) is incomplete or malformed."
    }

    return (
        ([string] $Event.Properties[0].Value).ToUpperInvariant() + "`0" +
        [string] $Event.Properties[1].Value)
}

function Get-ActiveInstallerTransactionReasons {
    $openTransactions = @{}
    foreach ($eventRecord in @(Get-MsiInstallerTransactionEvents)) {
        $transactionKey = Get-MsiInstallerTransactionKey -Event $eventRecord
        if ($eventRecord.Id -eq 1040) {
            if (-not $openTransactions.ContainsKey($transactionKey)) {
                $openTransactions[$transactionKey] = [Collections.Generic.Queue[long]]::new()
            }
            $openTransactions[$transactionKey].Enqueue([long] $eventRecord.RecordId)
            continue
        }

        if (-not $openTransactions.ContainsKey($transactionKey) -or
            $openTransactions[$transactionKey].Count -eq 0) {
            throw (
                "MsiInstaller end event $($eventRecord.RecordId) has no matching begin event " +
                'since Windows started.')
        }
        [void] $openTransactions[$transactionKey].Dequeue()
    }

    $openRecordIds = @(
        foreach ($transactionQueue in $openTransactions.Values) {
            foreach ($recordId in $transactionQueue) {
                $recordId
            }
        }
    )
    if ($openRecordIds.Count -eq 0) {
        return @()
    }

    return @(
        'Windows Installer has unmatched begin event record(s): ' +
        (($openRecordIds | Sort-Object) -join ', '))
}

function Get-LatestApplicationEventLogEvent {
    try {
        $events = @(
            Get-WinEvent `
                -LogName 'Application' `
                -MaxEvents 1 `
                -ErrorAction Stop
        )
    }
    catch {
        if ($_.FullyQualifiedErrorId -like 'NoMatchingEventsFound*') {
            return $null
        }
        throw "Could not inspect the Windows Application event log. $($_.Exception.Message)"
    }

    if ($events.Count -eq 0) {
        return $null
    }
    return $events[0]
}

function Get-ApplicationEventLogChannelConfigurations {
    try {
        return @(
            Get-WinEvent `
                -ListLog 'Application' `
                -ErrorAction Stop
        )
    }
    catch {
        throw (
            'Could not inspect the Windows Application event-log channel configuration. ' +
            $_.Exception.Message)
    }
}

function Assert-ApplicationEventLogChannelRecording {
    $configurations = @(Get-ApplicationEventLogChannelConfigurations)
    if ($configurations.Count -ne 1 -or $null -eq $configurations[0]) {
        throw (
            'Windows Application event-log channel configuration was not unique; ' +
            "expected one result but found $($configurations.Count).")
    }

    $isEnabledProperty = $configurations[0].PSObject.Properties['IsEnabled']
    if ($null -eq $isEnabledProperty -or $isEnabledProperty.Value -isnot [bool]) {
        throw (
            'Windows Application event-log channel configuration does not expose a ' +
            'Boolean IsEnabled value, so recording cannot be proven.')
    }
    if (-not [bool] $isEnabledProperty.Value) {
        throw (
            'Windows Application event-log channel is disabled, so qualification ' +
            'cannot prove that installer activity is being recorded.')
    }

    $isLogFullProperty = $configurations[0].PSObject.Properties['IsLogFull']
    if ($null -eq $isLogFullProperty -or $isLogFullProperty.Value -isnot [bool]) {
        throw (
            'Windows Application event-log channel configuration does not expose a ' +
            'Boolean IsLogFull value, so recording cannot be proven.')
    }
    if ([bool] $isLogFullProperty.Value) {
        throw (
            'Windows Application event-log channel is full, so qualification cannot ' +
            'prove that new installer activity is being recorded.')
    }
}

function Get-ApplicationEventLogEventByRecordId {
    param(
        [Parameter(Mandatory)]
        [long] $RecordId
    )

    try {
        $events = @(
            Get-WinEvent `
                -LogName 'Application' `
                -FilterXPath "*[System[(EventRecordID=$RecordId)]]" `
                -ErrorAction Stop
        )
    }
    catch {
        if ($_.FullyQualifiedErrorId -like 'NoMatchingEventsFound*') {
            return $null
        }
        throw (
            "Could not read Windows Application event-log record $RecordId. " +
            $_.Exception.Message)
    }

    if ($events.Count -eq 0) {
        return $null
    }
    if ($events.Count -ne 1) {
        throw (
            "Windows Application event-log record $RecordId was not unique; found " +
            "$($events.Count) matching records.")
    }
    return $events[0]
}

function Get-ApplicationEventXmlFingerprint {
    param(
        [Parameter(Mandatory)]
        $Event
    )

    try {
        $eventXml = [string] $Event.ToXml()
    }
    catch {
        throw (
            "Could not serialize Windows Application event-log record " +
            "$($Event.RecordId). $($_.Exception.Message)")
    }
    if ([string]::IsNullOrWhiteSpace($eventXml)) {
        throw "Windows Application event-log record $($Event.RecordId) has empty XML."
    }

    $bytes = [Text.Encoding]::UTF8.GetBytes($eventXml)
    return [Convert]::ToHexString(
        [Security.Cryptography.SHA256]::HashData($bytes)).ToLowerInvariant()
}

function Get-ApplicationEventLogAnchor {
    Assert-ApplicationEventLogChannelRecording
    $anchorEvent = Get-LatestApplicationEventLogEvent
    if ($null -eq $anchorEvent) {
        throw (
            'The Windows Application event log is empty; local durable qualification ' +
            'cannot establish a continuity anchor.')
    }

    $anchorRecordId = [long] $anchorEvent.RecordId
    if ($anchorRecordId -le 0) {
        throw (
            'The newest Windows Application event has an invalid event-log record ID: ' +
            $anchorRecordId)
    }

    return [pscustomobject]@{
        RecordId = $anchorRecordId
        Fingerprint = Get-ApplicationEventXmlFingerprint -Event $anchorEvent
    }
}

function Get-ApplicationEventLogAnchorReasons {
    param(
        [Parameter(Mandatory)]
        $Anchor,

        [Parameter(Mandatory)]
        [string] $Stage
    )

    $anchorRecordId = [long] $Anchor.RecordId
    try {
        Assert-ApplicationEventLogChannelRecording
        $currentAnchorEvent = Get-ApplicationEventLogEventByRecordId `
            -RecordId $anchorRecordId
        if ($null -eq $currentAnchorEvent) {
            return @(
                "Windows Application event log lost continuity anchor record " +
                "$anchorRecordId $Stage; the log may have been cleared or overwritten")
        }

        $currentFingerprint = Get-ApplicationEventXmlFingerprint -Event $currentAnchorEvent
        if ($currentFingerprint -cne [string] $Anchor.Fingerprint) {
            return @(
                "Windows Application event-log continuity anchor record $anchorRecordId " +
                "changed $Stage; the log may have been cleared and the record ID reused")
        }
    }
    catch {
        return @(
            "Could not verify Windows Application event-log continuity anchor record " +
            "$anchorRecordId $Stage. $($_.Exception.Message)")
    }

    return @()
}

function Get-InstallerActivityReasons {
    param(
        [Parameter(Mandatory)]
        $ApplicationEventLogAnchor,

        [Parameter(Mandatory)]
        [DateTimeOffset] $NotBeforeUtc
    )

    $anchorIssues = @(
        Get-ApplicationEventLogAnchorReasons `
            -Anchor $ApplicationEventLogAnchor `
            -Stage 'before reading Windows Installer events'
    )
    if ($anchorIssues.Count -gt 0) {
        return $anchorIssues
    }

    $events = @(Get-MsiInstallerTransactionEvents)

    $anchorIssues = @(
        Get-ApplicationEventLogAnchorReasons `
            -Anchor $ApplicationEventLogAnchor `
            -Stage 'after reading Windows Installer events'
    )
    if ($anchorIssues.Count -gt 0) {
        return $anchorIssues
    }

    $afterRecordId = [long] $ApplicationEventLogAnchor.RecordId
    $newEvents = @(
        $events |
            Where-Object {
                [long] $_.RecordId -gt $afterRecordId -and
                ([DateTimeOffset] $_.TimeCreated).ToUniversalTime() -ge $NotBeforeUtc
            }
    )
    if ($newEvents.Count -eq 0) {
        return @()
    }

    return @(
        'Windows Installer event(s) occurred during qualification: ' +
        (($newEvents | ForEach-Object { "$($_.Id)/$($_.RecordId)" }) -join ', '))
}

function Get-PassMeasurementStartUtc {
    param(
        [Parameter(Mandatory)]
        [string] $PassOutput
    )

    $executionLogPath = Join-Path $PassOutput 'logs/execution-order.log'
    if (-not (Test-Path -LiteralPath $executionLogPath -PathType Leaf)) {
        throw "Pass execution log was not created: $executionLogPath"
    }

    $startLine = Get-Content -LiteralPath $executionLogPath |
        Where-Object { $_ -match '^[^|]+\|[^|]+\|[^|]+\|[^|]+\|START\|' } |
        Select-Object -First 1
    if ([string]::IsNullOrWhiteSpace($startLine)) {
        throw "Pass execution log contains no measurement START event: $executionLogPath"
    }

    $timestampText = ($startLine -split '\|', 2)[0]
    $measurementStartUtc = [DateTimeOffset]::MinValue
    if (-not [DateTimeOffset]::TryParse(
        $timestampText,
        [Globalization.CultureInfo]::InvariantCulture,
        [Globalization.DateTimeStyles]::RoundtripKind,
        [ref] $measurementStartUtc)) {
        throw "Pass execution log contains an invalid START timestamp: $timestampText"
    }

    return $measurementStartUtc.ToUniversalTime()
}

function Get-LocalEnvironmentIssues {
    param(
        [Parameter(Mandatory)]
        [AllowEmptyCollection()]
        [AllowEmptyString()]
        [string[]] $PendingFileRenameBaseline
    )

    $currentPendingFileRenames = @(Get-PendingFileRenameOperationsSnapshot)
    return @(
        Get-PendingRestartReasons
        Get-PendingFileRenamePolicyReasons -Snapshot $currentPendingFileRenames
        Get-PendingFileRenameChangeReasons `
            -Baseline $PendingFileRenameBaseline `
            -Current $currentPendingFileRenames
        Get-ActiveInstallerTransactionReasons
    )
}

function Assert-QuiescentLocalEnvironment {
    param(
        [Parameter(Mandatory)]
        [string] $Stage,

        [Parameter(Mandatory)]
        [AllowEmptyCollection()]
        [AllowEmptyString()]
        [string[]] $PendingFileRenameBaseline,

        [Parameter(Mandatory)]
        $ApplicationEventLogAnchor
    )

    $issues = @(
        Get-ApplicationEventLogAnchorReasons `
            -Anchor $ApplicationEventLogAnchor `
            -Stage "at $Stage"
    )
    if ($issues.Count -eq 0) {
        $issues = @(Get-LocalEnvironmentIssues `
            -PendingFileRenameBaseline $PendingFileRenameBaseline)
    }
    if ($issues.Count -gt 0) {
        throw (
            "Local durable performance qualification requires a quiescent Windows " +
            "environment at $Stage. $($issues -join '; '). Allow installers and updates " +
            'to finish; restart only when Windows reports that one is required, then retry.')
    }
}

$status = Invoke-Git `
    -Arguments @('status', '--porcelain=v1', '--untracked-files=all') `
    -FailureMessage 'Could not inspect the repository worktree.'
if (-not [string]::IsNullOrWhiteSpace($status)) {
    throw 'Local durable performance qualification requires a clean repository worktree.'
}

$candidateCommit = (Invoke-Git `
    -Arguments @('rev-parse', '--verify', "$CandidateRef^{commit}") `
    -FailureMessage "Candidate ref '$CandidateRef' does not resolve to a commit.").Trim()
$previousCommit = ''
if (-not [string]::IsNullOrWhiteSpace($PreviousRef)) {
    $previousCommit = (Invoke-Git `
        -Arguments @('rev-parse', '--verify', "$PreviousRef^{commit}") `
        -FailureMessage "Previous release ref '$PreviousRef' does not resolve to a commit.").Trim()
}

if ([string]::IsNullOrWhiteSpace($OutputPath)) {
    $OutputPath = Join-Path `
        ([IO.Path]::GetTempPath()) `
        "csharpdb-local-durable-performance-$([Guid]::NewGuid().ToString('N'))"
}
$outputRoot = [IO.Path]::GetFullPath($OutputPath)
$benchmarkTemporaryRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
$pathComparison = [StringComparison]::OrdinalIgnoreCase
$normalizedRepositoryRoot = $repositoryRoot.TrimEnd(
    [IO.Path]::DirectorySeparatorChar,
    [IO.Path]::AltDirectorySeparatorChar)
$normalizedOutputRoot = $outputRoot.TrimEnd(
    [IO.Path]::DirectorySeparatorChar,
    [IO.Path]::AltDirectorySeparatorChar)
$repositoryPrefix = $normalizedRepositoryRoot + [IO.Path]::DirectorySeparatorChar
if ($normalizedOutputRoot.Equals($normalizedRepositoryRoot, $pathComparison) -or
    $normalizedOutputRoot.StartsWith($repositoryPrefix, $pathComparison)) {
    throw "Local durable performance output must be outside the repository: $outputRoot"
}
if (Test-Path -LiteralPath $outputRoot) {
    if (-not (Test-Path -LiteralPath $outputRoot -PathType Container)) {
        throw "Local durable performance output must be a directory: $outputRoot"
    }
    if ($null -ne (Get-ChildItem -LiteralPath $outputRoot -Force | Select-Object -First 1)) {
        throw "Local durable performance output must be absent or empty: $outputRoot"
    }
}
else {
    New-Item -ItemType Directory -Path $outputRoot | Out-Null
}

$statusContext = 'csharpdb/local-durable-performance'
$pendingFileRenameBaseline = @(Get-PendingFileRenameOperationsSnapshot)
$pendingFileRenameFingerprint = Get-PendingFileRenameFingerprint `
    -Snapshot $pendingFileRenameBaseline
$applicationEventLogAnchor = Get-ApplicationEventLogAnchor
$applicationEventLogAnchorRecordId = [long] $applicationEventLogAnchor.RecordId
Assert-QuiescentLocalEnvironment `
    -Stage 'preflight' `
    -PendingFileRenameBaseline $pendingFileRenameBaseline `
    -ApplicationEventLogAnchor $applicationEventLogAnchor
if ($pendingFileRenameBaseline.Count -gt 0) {
    Write-Warning (
        "Accepting $($pendingFileRenameBaseline.Count / 2) stable deletion-only pending " +
        "file operation(s) as baseline $pendingFileRenameFingerprint. Any change fails " +
        'qualification.')
}
if (-not $NoGitHubStatus) {
    $authOutput = @(& gh auth status 2>&1)
    if ($LASTEXITCODE -ne 0) {
        throw (
            'GitHub authentication is required before the local release gate starts. ' +
            (($authOutput | ForEach-Object { [string] $_ }) -join [Environment]::NewLine))
    }
    if ([string]::IsNullOrWhiteSpace($GitHubRepository)) {
        Push-Location $repositoryRoot
        try {
            $repositoryOutput = @(
                & gh repo view --json nameWithOwner --jq '.nameWithOwner' 2>&1
            )
            if ($LASTEXITCODE -ne 0) {
                throw (
                    'Could not resolve the GitHub repository for release status. ' +
                    (($repositoryOutput | ForEach-Object { [string] $_ }) -join [Environment]::NewLine))
            }
            $GitHubRepository = (($repositoryOutput | ForEach-Object { [string] $_ }) -join '').Trim()
        }
        finally {
            Pop-Location
        }
    }
    if ($GitHubRepository -cnotmatch '^[^/\s]+/[^/\s]+$') {
        throw "GitHubRepository must use the owner/name form: $GitHubRepository"
    }
    Invoke-GitHubStatus `
        -State pending `
        -Description "policy=$statusPolicy; local durable qualification running"
}

$startedUtc = [DateTimeOffset]::UtcNow
$result = 'FAIL'
$failureMessage = ''
$passFailures = [Collections.Generic.List[string]]::new()
$durabilityVariable = 'CSHARPDB_BENCH_DURABILITY'
$priorDurability = [Environment]::GetEnvironmentVariable(
    $durabilityVariable,
    [EnvironmentVariableTarget]::Process)
[Environment]::SetEnvironmentVariable(
    $durabilityVariable,
    'Durable',
    [EnvironmentVariableTarget]::Process)

try {
    Write-Host 'Running two sequential local durable performance passes.'
    Write-Host 'Expected duration on an idle fixed-SSD machine: approximately 75-100 minutes.'
    Write-Host "Evidence root: $outputRoot"

    foreach ($qualificationPass in 1, 2) {
        Assert-QuiescentLocalEnvironment `
            -Stage "the start of pass $qualificationPass" `
            -PendingFileRenameBaseline $pendingFileRenameBaseline `
            -ApplicationEventLogAnchor $applicationEventLogAnchor
        $passOutput = Join-Path $outputRoot "pass-$qualificationPass"
        $parameters = @{
            CandidateRef = $candidateCommit
            OutputPath = $passOutput
            QualificationPass = $qualificationPass
            Paired = $true
            SuiteName = @('master-table-durable-writes')
            RepeatCount = $RepeatCount
            PostBuildQuiescenceSeconds = $PostBuildQuiescenceSeconds
            MaxThroughputRegressionPercent = $MaxThroughputRegressionPercent
            MaxP99RegressionPercent = $MaxP99RegressionPercent
            MaxP99RegressionMilliseconds = $MaxP99RegressionMilliseconds
            BlockingLatencyPercentile = $BlockingLatencyPercentile
        }
        if (-not [string]::IsNullOrWhiteSpace($previousCommit)) {
            $parameters.PreviousRef = $previousCommit
        }

        Write-Host "Starting local durable performance pass $qualificationPass of 2."
        try {
            & $comparisonScript @parameters
        }
        catch {
            $passMessage = $_.Exception.Message -replace '\r?\n', ' '
            $passFailures.Add("Pass $qualificationPass failed: $passMessage")
            if ($qualificationPass -eq 1) {
                Write-Warning 'Pass 1 failed; continuing to collect the second pass.'
            }
            else {
                Write-Warning 'Pass 2 failed.'
            }
        }

        $measurementStartUtc = Get-PassMeasurementStartUtc -PassOutput $passOutput
        $installerQuietCutoffUtc = $measurementStartUtc.AddSeconds(
            -$PostBuildQuiescenceSeconds)
        $environmentIssues = @(
            Get-LocalEnvironmentIssues `
                -PendingFileRenameBaseline $pendingFileRenameBaseline
            Get-InstallerActivityReasons `
                -ApplicationEventLogAnchor $applicationEventLogAnchor `
                -NotBeforeUtc $installerQuietCutoffUtc
        )
        if ($environmentIssues.Count -gt 0) {
            $passFailures.Add(
                "Pass $qualificationPass environment contamination: " +
                ($environmentIssues -join '; '))
            Write-Warning (
                "Pass $qualificationPass detected installer or system-state activity; " +
                'remaining passes will not run.')
            break
        }

        if ($qualificationPass -eq 1 -and [string]::IsNullOrWhiteSpace($previousCommit)) {
            $preflightPath = Join-Path $passOutput 'previous-release-performance-preflight.md'
            if (Test-Path -LiteralPath $preflightPath -PathType Leaf) {
                $previousLine = Select-String `
                    -LiteralPath $preflightPath `
                    -Pattern '^- Previous ref: `[^`]+` \(`(?<commit>[0-9a-f]{40})`\)$' |
                    Select-Object -First 1
                if ($null -ne $previousLine -and $previousLine.Matches[0].Groups['commit'].Success) {
                    $previousCommit = $previousLine.Matches[0].Groups['commit'].Value
                }
            }
            if ([string]::IsNullOrWhiteSpace($previousCommit)) {
                $passFailures.Add('Could not pin the previous-release commit from pass 1 evidence.')
            }
        }
    }
}
catch {
    $unexpectedMessage = $_.Exception.Message -replace '\r?\n', ' '
    $passFailures.Add("Unexpected wrapper failure: $unexpectedMessage")
}
finally {
    [Environment]::SetEnvironmentVariable(
        $durabilityVariable,
        $priorDurability,
        [EnvironmentVariableTarget]::Process)
}

if ($passFailures.Count -eq 0) {
    $result = 'PASS'
}
else {
    $failureMessage = $passFailures -join ' | '
}

$completedUtc = [DateTimeOffset]::UtcNow
$summaryPath = Join-Path $outputRoot 'local-durable-performance.md'
function Write-LocalSummary {
    $summaryLines = @(
        '# Local durable performance qualification',
        '',
        "- Result: **$result**",
        '- Execution: two sequential balanced paired passes on one Windows machine',
        '- Suite: `master-table-durable-writes` (10 durable write rows)',
        "- Candidate commit: ``$candidateCommit``",
        $(if ([string]::IsNullOrWhiteSpace($previousCommit)) {
            '- Previous release commit: unresolved'
        }
        else {
            "- Previous release commit: ``$previousCommit``"
        }),
        "- Repeat count per order: $RepeatCount",
        '- Durability mode: `Durable`',
        "- Status policy: ``$statusPolicy``",
        "- Blocking latency percentile: ``$BlockingLatencyPercentile``",
        $(if ($BlockingLatencyPercentile -ceq 'P99') {
            '- P99 latency: blocking for this diagnostic run'
        }
        else {
            '- P99 latency: diagnostic only'
        }),
        '- Dedicated fixed SSD: confirmed by the release operator',
        "- Pending file operation baseline entries: $($pendingFileRenameBaseline.Count)",
        "- Pending file operation baseline fingerprint: ``$pendingFileRenameFingerprint``",
        "- Windows Application event-log anchor record: $applicationEventLogAnchorRecordId",
        ("- Windows Application event-log anchor SHA-256: " +
            "``$($applicationEventLogAnchor.Fingerprint)``"),
        "- Machine: ``$env:COMPUTERNAME``",
        "- Benchmark temporary root: ``$benchmarkTemporaryRoot``",
        "- Evidence root: ``$outputRoot``",
        $(if ($NoGitHubStatus) {
            '- GitHub release status: disabled (diagnostic run only)'
        }
        else {
            "- GitHub release status: ``$statusContext`` in ``$GitHubRepository``"
        }),
        "- Started UTC: $($startedUtc.ToString('O'))",
        "- Completed UTC: $($completedUtc.ToString('O'))",
        "- Elapsed: $([Math]::Round(($completedUtc - $startedUtc).TotalMinutes, 1)) minutes",
        '- Pass 1 report: `pass-1/previous-release-performance.md`',
        '- Pass 2 report: `pass-2/previous-release-performance.md`'
    )
    if (-not [string]::IsNullOrWhiteSpace($failureMessage)) {
        $summaryLines += "- Failure: $failureMessage"
    }
    [IO.File]::WriteAllLines($summaryPath, $summaryLines)
}

Write-LocalSummary

if (-not $NoGitHubStatus) {
    try {
        if ($result -eq 'PASS') {
            $reportHashes = @(
                foreach ($pass in 1, 2) {
                    $reportPath = Join-Path `
                        (Join-Path $outputRoot "pass-$pass") `
                        'previous-release-performance.md'
                    (Get-FileHash -LiteralPath $reportPath -Algorithm SHA256).Hash.Substring(0, 8)
                }
            )
            Invoke-GitHubStatus `
                -State success `
                -Description (
                    "policy=$statusPolicy; baseline=$previousCommit; " +
                    "reports=$($reportHashes -join '/')")
        }
        else {
            Invoke-GitHubStatus `
                -State failure `
                -Description "policy=$statusPolicy; local durable qualification failed"
        }
    }
    catch {
        $statusMessage = $_.Exception.Message -replace '\r?\n', ' '
        $result = 'FAIL'
        $failureMessage = @($failureMessage, $statusMessage) |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) } |
            Join-String -Separator ' | '
        Write-LocalSummary
    }
}

Write-Host "Local durable performance summary: $summaryPath"
if ($result -ne 'PASS') {
    throw "Local durable performance qualification failed. $failureMessage"
}

Write-Host 'Local durable performance qualification passed.'
