[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateSet('Api', 'Daemon')]
    [string]$HostName,

    [Parameter(Mandatory)]
    [ValidateSet('win-x64', 'linux-x64', 'osx-arm64')]
    [string]$Runtime,

    [string]$ExecutablePath,

    [string]$WorkingDirectory,

    [string]$OutputRoot,

    [ValidateSet('Debug', 'Release')]
    [string]$Configuration = 'Release',

    [switch]$NoRestore,

    [switch]$KeepWorkingDirectory
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
. (Join-Path $PSScriptRoot 'CSharpDbHostQualificationCleanup.ps1')
$projectPath = Join-Path $repoRoot "src/CSharpDB.$HostName/CSharpDB.$HostName.csproj"
$executableName = if ($Runtime -eq 'win-x64') {
    "CSharpDB.$HostName.exe"
}
else {
    "CSharpDB.$HostName"
}

function Invoke-DotNet {
    param(
        [Parameter(Mandatory)]
        [string[]]$Arguments,

        [Parameter(Mandatory)]
        [string]$Description
    )

    Write-Host $Description
    & dotnet @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed with exit code $LASTEXITCODE."
    }
}

function Get-AvailableLoopbackPort {
    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
    try {
        $listener.Start()
        return ([Net.IPEndPoint]$listener.LocalEndpoint).Port
    }
    finally {
        $listener.Stop()
    }
}

function Invoke-SmokeRequest {
    param(
        [Parameter(Mandatory)]
        [string]$Uri,

        [int]$TimeoutSeconds = 5
    )

    return Invoke-WebRequest `
        -Uri $Uri `
        -TimeoutSec $TimeoutSeconds `
        -UseBasicParsing
}

function Write-ProcessLogs {
    param(
        [Parameter(Mandatory)]
        [Diagnostics.Process]$Process,

        [Parameter(Mandatory)]
        [Threading.Tasks.Task[string]]$StandardOutput,

        [Parameter(Mandatory)]
        [Threading.Tasks.Task[string]]$StandardError
    )

    if (-not $Process.HasExited) {
        return
    }

    Write-Host '--- host stdout ---'
    Write-Host $StandardOutput.GetAwaiter().GetResult()
    Write-Host '--- host stderr ---'
    Write-Host $StandardError.GetAwaiter().GetResult()
}

function Stop-QualifiedHost {
    param(
        [Parameter(Mandatory)]
        [Diagnostics.Process]$Process
    )

    if ($Process.HasExited) {
        if ($Process.ExitCode -ne 0) {
            throw "Published $HostName host exited with code $($Process.ExitCode) before shutdown qualification."
        }
        return $true
    }

    # ConsoleLifetime handles SIGTERM as an orderly StopApplication request on
    # Unix. Windows console processes do not expose a portable signal API via
    # System.Diagnostics.Process. CloseMainWindow is best-effort there; a
    # forced fallback is reported as cleanup-only and never as an orderly pass.
    if ($IsWindows) {
        if ($Process.CloseMainWindow() -and $Process.WaitForExit(15000)) {
            if ($Process.ExitCode -ne 0) {
                throw "Published $HostName host exited with code $($Process.ExitCode) during Windows shutdown."
            }
            return $true
        }

        $Process.Kill($true)
        if (-not $Process.WaitForExit(10000)) {
            throw "Published $HostName host process $($Process.Id) did not stop during bounded Windows cleanup."
        }
        return $false
    }

    & /bin/kill -TERM $Process.Id
    if ($LASTEXITCODE -ne 0) {
        throw "Could not request an orderly shutdown for process $($Process.Id)."
    }

    if ($Process.WaitForExit(15000)) {
        if ($Process.ExitCode -ne 0) {
            throw "Published $HostName host exited with code $($Process.ExitCode) during orderly shutdown."
        }
        return $true
    }

    throw "Published $HostName host process $($Process.Id) did not complete orderly shutdown within 15 seconds."
}

if (-not (Test-Path -LiteralPath $projectPath -PathType Leaf)) {
    throw "The $HostName host project was not found: $projectPath"
}

$temporaryRoot = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
$qualificationRoot = [IO.Path]::GetFullPath((Join-Path $temporaryRoot (
    "csharpdb-$($HostName.ToLowerInvariant())-observability-smoke-$([Guid]::NewGuid().ToString('N'))")))
if (-not $qualificationRoot.StartsWith(
        $temporaryRoot.TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar,
        [StringComparison]::OrdinalIgnoreCase)) {
    throw 'The host qualification directory must be inside the system temporary directory.'
}

$createdPublishRoot = $false
$publishRoot = $null
$process = $null
$processStarted = $false
$standardOutput = $null
$standardError = $null
try {
if ([string]::IsNullOrWhiteSpace($ExecutablePath)) {
    if ([string]::IsNullOrWhiteSpace($OutputRoot)) {
        $publishRoot = Join-Path $qualificationRoot 'publish'
        $createdPublishRoot = $true
    }
    else {
        $publishRoot = if ([IO.Path]::IsPathRooted($OutputRoot)) {
            [IO.Path]::GetFullPath($OutputRoot)
        }
        else {
            [IO.Path]::GetFullPath((Join-Path $repoRoot $OutputRoot))
        }
        $publishRoot = Join-Path $publishRoot "$($HostName.ToLowerInvariant())-$Runtime"
    }

    New-Item -ItemType Directory -Path $publishRoot -Force | Out-Null
    $publishArguments = @(
        'publish',
        $projectPath,
        '--configuration', $Configuration,
        '--runtime', $Runtime,
        '--self-contained', 'true',
        '--output', $publishRoot,
        '-p:PublishSingleFile=true',
        '-p:PublishTrimmed=false',
        '-p:IncludeNativeLibrariesForSelfExtract=true'
    )
    if ($NoRestore) {
        $publishArguments += '--no-restore'
    }

    Invoke-DotNet `
        -Arguments $publishArguments `
        -Description "Publishing CSharpDB.$HostName for $Runtime observability qualification."
    $ExecutablePath = Join-Path $publishRoot $executableName
    $WorkingDirectory = $publishRoot
}
else {
    $ExecutablePath = [IO.Path]::GetFullPath($ExecutablePath)
    if ([string]::IsNullOrWhiteSpace($WorkingDirectory)) {
        $WorkingDirectory = Split-Path -Parent $ExecutablePath
    }
    else {
        $WorkingDirectory = [IO.Path]::GetFullPath($WorkingDirectory)
    }
}

if (-not (Test-Path -LiteralPath $ExecutablePath -PathType Leaf)) {
    throw "The published $HostName executable was not found: $ExecutablePath"
}
if (-not (Test-Path -LiteralPath $WorkingDirectory -PathType Container)) {
    throw "The published $HostName working directory was not found: $WorkingDirectory"
}

New-Item -ItemType Directory -Path $qualificationRoot -Force | Out-Null
$port = Get-AvailableLoopbackPort
$baseUri = "http://127.0.0.1:$port"
$databaseAlias = "phase7-$($HostName.ToLowerInvariant())"
$privacyCanary = "private-host-path-$([Guid]::NewGuid().ToString('N'))"
$databasePath = Join-Path $qualificationRoot "$privacyCanary.db"

$startInfo = [Diagnostics.ProcessStartInfo]::new()
$startInfo.FileName = $ExecutablePath
$startInfo.WorkingDirectory = $WorkingDirectory
$startInfo.UseShellExecute = $false
$startInfo.CreateNoWindow = $true
$startInfo.RedirectStandardOutput = $true
$startInfo.RedirectStandardError = $true

$environment = [ordered]@{
    'ASPNETCORE_ENVIRONMENT' = 'Production'
    'DOTNET_ENVIRONMENT' = 'Production'
    'ASPNETCORE_URLS' = $baseUri
    'ConnectionStrings__CSharpDB' = "Data Source=$databasePath"
    'CSharpDB__Observability__Enabled' = 'true'
    'CSharpDB__Observability__DatabaseAlias' = $databaseAlias
    'CSharpDB__Observability__Logging__SqlText' = 'None'
    'CSharpDB__Observability__OpenTelemetry__Enabled' = 'false'
    'CSharpDB__Observability__Prometheus__Enabled' = 'true'
    'CSharpDB__Observability__Prometheus__AllowInsecureRemoteAccess' = 'false'
    'CSharpDB__Observability__Health__Enabled' = 'true'
    'CSharpDB__Observability__Health__LivenessPath' = '/health/live'
    'CSharpDB__Observability__Health__ReadinessPath' = '/health/ready'
    'CSharpDB__Observability__Health__ReadinessTimeout' = '00:00:05'
    'CSharpDB__Api__Security__Mode' = 'None'
    'CSharpDB__Api__Security__AllowInsecureRemoteDiagnostics' = 'false'
    'CSharpDB__Daemon__EnableRestApi' = 'true'
    'CSharpDB__Daemon__Security__Mode' = 'None'
    'CSharpDB__Daemon__Security__AllowInsecureRemoteDiagnostics' = 'false'
}
foreach ($entry in $environment.GetEnumerator()) {
    $startInfo.Environment[$entry.Key] = $entry.Value
}

$process = [Diagnostics.Process]::new()
$process.StartInfo = $startInfo
}
catch {
    if ($null -ne $process) {
        $process.Dispose()
    }
    if (-not $KeepWorkingDirectory -and
        (Test-Path -LiteralPath $qualificationRoot)) {
        Remove-CSharpDbDirectoryWithRetry -LiteralPath $qualificationRoot
    }
    throw
}

try {
    Write-Host "Starting published CSharpDB.$HostName on $baseUri."
    if (-not $process.Start()) {
        throw "Could not start the published CSharpDB.$HostName executable."
    }
    $processStarted = $true
    $standardOutput = $process.StandardOutput.ReadToEndAsync()
    $standardError = $process.StandardError.ReadToEndAsync()

    $readyResponse = $null
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(45)
    while ([DateTimeOffset]::UtcNow -lt $deadline) {
        if ($process.HasExited) {
            Write-ProcessLogs `
                -Process $process `
                -StandardOutput $standardOutput `
                -StandardError $standardError
            throw "Published CSharpDB.$HostName exited early with code $($process.ExitCode)."
        }

        try {
            $readyResponse = Invoke-SmokeRequest "$baseUri/health/ready" -TimeoutSeconds 2
            break
        }
        catch {
            Start-Sleep -Milliseconds 500
        }
    }

    if ($null -eq $readyResponse -or $readyResponse.StatusCode -ne 200) {
        throw "Published CSharpDB.$HostName readiness did not become healthy before the timeout."
    }

    $liveResponse = Invoke-SmokeRequest "$baseUri/health/live"
    if ($liveResponse.StatusCode -ne 200) {
        throw "Published CSharpDB.$HostName liveness returned HTTP $($liveResponse.StatusCode)."
    }

    $null = Invoke-SmokeRequest "$baseUri/api/info"
    $runtimeResponse = Invoke-SmokeRequest "$baseUri/api/diagnostics/runtime"
    $runtimeJson = [string]$runtimeResponse.Content
    $runtimeSnapshot = $runtimeJson | ConvertFrom-Json -Depth 100
    if ($null -eq $runtimeSnapshot.aggregate -or
        $null -eq $runtimeSnapshot.aggregate.metadata -or
        [string]$runtimeSnapshot.aggregate.metadata.databaseAlias -cne $databaseAlias -or
        [string]::IsNullOrWhiteSpace(
            [string]$runtimeSnapshot.aggregate.metadata.schemaVersion)) {
        throw "Published CSharpDB.$HostName returned an invalid runtime diagnostics topology."
    }

    $metricsResponse = Invoke-SmokeRequest "$baseUri/metrics"
    $metrics = [string]$metricsResponse.Content
    if (-not $metrics.Contains('csharpdb_', [StringComparison]::Ordinal) -or
        -not $metrics.Contains('csharpdb_health_status', [StringComparison]::Ordinal)) {
        throw "Published CSharpDB.$HostName Prometheus output did not contain CSharpDB health metrics."
    }

    foreach ($payload in @($runtimeJson, $metrics)) {
        if ($payload.Contains($privacyCanary, [StringComparison]::Ordinal) -or
            $payload.Contains($databasePath, [StringComparison]::OrdinalIgnoreCase)) {
            throw "Published CSharpDB.$HostName diagnostics leaked the private database path canary."
        }
    }

    $orderlyShutdown = Stop-QualifiedHost -Process $process
    if (-not $process.HasExited) {
        throw "Published CSharpDB.$HostName did not complete process shutdown."
    }
    if (-not $orderlyShutdown) {
        Write-Warning (
            "Windows published-host shutdown used bounded cleanup only; " +
            "orderly StopAsync behavior is qualified by the in-process host suites.")
    }

    # A closed database file proves the smoke process left no live host or
    # retained file handle behind. The file is recreated by later smokes.
    if (Test-Path -LiteralPath $databasePath) {
        Remove-Item -LiteralPath $databasePath -Force
    }

    Write-Host "Published CSharpDB.$HostName $Runtime observability qualification passed."
}
catch {
    if ($null -ne $standardOutput -and $null -ne $standardError) {
        Write-ProcessLogs `
            -Process $process `
            -StandardOutput $standardOutput `
            -StandardError $standardError
    }
    throw
}
finally {
    if ($processStarted -and -not $process.HasExited) {
        $process.Kill($true)
        $null = $process.WaitForExit(10000)
    }
    if ($null -ne $process) {
        $process.Dispose()
    }

    if ($KeepWorkingDirectory) {
        Write-Host "Host qualification working directory retained at $qualificationRoot"
    }
    elseif (Test-Path -LiteralPath $qualificationRoot) {
        Remove-CSharpDbDirectoryWithRetry -LiteralPath $qualificationRoot
    }

    if (-not $KeepWorkingDirectory -and
        $createdPublishRoot -and
        -not [string]::IsNullOrWhiteSpace($publishRoot) -and
        (Test-Path -LiteralPath $publishRoot)) {
        Remove-CSharpDbDirectoryWithRetry -LiteralPath $publishRoot
    }
}
