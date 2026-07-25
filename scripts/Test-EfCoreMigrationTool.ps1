#requires -Version 7.0

[CmdletBinding(DefaultParameterSetName = 'Pack')]
param(
    [Parameter(
        Mandatory = $true,
        ParameterSetName = 'Prepacked')]
    [string] $FeedPath,

    [Parameter(
        Mandatory = $true,
        ParameterSetName = 'Prepacked')]
    [string] $Version,

    [ValidateSet('Debug', 'Release')]
    [string] $Configuration = 'Release',

    [switch] $NoRestore
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).ProviderPath
$toolProject = Join-Path `
    $root `
    'src/CSharpDB.EntityFrameworkCore.Tools/CSharpDB.EntityFrameworkCore.Tools.csproj'
$fixtureProject = Join-Path `
    $root `
    'tests/CSharpDB.EntityFrameworkCore.Tools.Fixtures/CSharpDB.EntityFrameworkCore.Tools.Fixtures.csproj'
$baseCliProject = Join-Path `
    $root `
    'src/CSharpDB.Cli/CSharpDB.Cli.csproj'
$webSampleProject = Join-Path `
    $root `
    'samples/efcore-minimal-api/EfCoreMinimalApiSample.csproj'
$webSampleDatabase = Join-Path `
    $root `
    'samples/efcore-minimal-api/sample.db'
$temporaryParent = Join-Path $root '.tmp'
[System.IO.Directory]::CreateDirectory($temporaryParent) | Out-Null
$temporaryParentItem = Get-Item -LiteralPath $temporaryParent -Force
if (($temporaryParentItem.Attributes -band
        [System.IO.FileAttributes]::ReparsePoint) -ne 0)
{
    throw "The EF Core tool test parent cannot be a reparse point: $temporaryParent"
}

$workspace = Join-Path `
    $temporaryParent `
    ("efcore-migration-tool-" + [Guid]::NewGuid().ToString('N'))
[System.IO.Directory]::CreateDirectory($workspace) | Out-Null
$packedPackageDirectory = Join-Path $workspace 'packages'
$toolDirectory = Join-Path $workspace 'tool'
$baseCliPublishDirectory = Join-Path $workspace 'base-cli'
[System.IO.Directory]::CreateDirectory($toolDirectory) | Out-Null
[System.IO.Directory]::CreateDirectory($baseCliPublishDirectory) | Out-Null
$nugetConfig = Join-Path $workspace 'NuGet.Config'
$packageDirectory = $null
$packageVersion = $null
$packagePath = $null
$packageDigest = $null
$dotnetCommand = Get-Command dotnet -CommandType Application
$dotnetExecutable = [System.IO.Path]::GetFullPath($dotnetCommand.Source)
if (-not (Test-Path -LiteralPath $dotnetExecutable -PathType Leaf)) {
    throw 'The .NET host could not be resolved.'
}
$previousPath = $env:PATH

function Invoke-DotNet {
    param(
        [Parameter(Mandatory = $true)]
        [string[]] $Arguments,

        [Parameter(Mandatory = $true)]
        [string] $Description
    )

    Write-Host $Description
    Push-Location -LiteralPath $root
    try {
        & dotnet @Arguments
        $exitCode = $LASTEXITCODE
    }
    finally {
        Pop-Location
    }

    if ($exitCode -ne 0) {
        throw "$Description failed with exit code $exitCode."
    }
}

function Resolve-RepoPath {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path
    )

    if ([System.IO.Path]::IsPathRooted($Path)) {
        return [System.IO.Path]::GetFullPath($Path)
    }

    return [System.IO.Path]::GetFullPath(
        (Join-Path $root $Path))
}

function Get-NuGetPackageIdentity {
    param(
        [Parameter(Mandatory = $true)]
        [string] $PackagePath
    )

    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $archive = [System.IO.Compression.ZipFile]::OpenRead($PackagePath)
    try {
        $nuspecEntries = @(
            $archive.Entries |
                Where-Object { $_.FullName -like '*.nuspec' }
        )
        if ($nuspecEntries.Count -ne 1) {
            throw "Expected exactly one nuspec in $PackagePath."
        }

        $reader = [System.IO.StreamReader]::new(
            $nuspecEntries[0].Open())
        try {
            [xml] $nuspec = $reader.ReadToEnd()
        }
        finally {
            $reader.Dispose()
        }
    }
    finally {
        $archive.Dispose()
    }

    $metadata = $nuspec.SelectSingleNode(
        '/*[local-name()="package"]/*[local-name()="metadata"]')
    if ($null -eq $metadata) {
        throw "Package metadata was not found in $PackagePath."
    }

    return [pscustomobject] @{
        Id = [string] $metadata.SelectSingleNode(
            '*[local-name()="id"]').InnerText
        Version = [string] $metadata.SelectSingleNode(
            '*[local-name()="version"]').InnerText
    }
}

function Invoke-CapturedProcess {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Executable,

        [Parameter(Mandatory = $true)]
        [string[]] $Arguments,

        [Parameter(Mandatory = $true)]
        [string] $WorkingDirectory,

        [TimeSpan] $Timeout = [TimeSpan]::FromMinutes(3)
    )

    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $Executable
    $startInfo.WorkingDirectory = $WorkingDirectory
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $startInfo.CreateNoWindow = $true
    foreach ($argument in $Arguments) {
        $startInfo.ArgumentList.Add($argument)
    }

    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    try {
        if (-not $process.Start()) {
            throw 'The installed EF Core migration tool could not be started.'
        }

        $stdoutTask = $process.StandardOutput.ReadToEndAsync()
        $stderrTask = $process.StandardError.ReadToEndAsync()
        if (-not $process.WaitForExit([int]$Timeout.TotalMilliseconds)) {
            try {
                $process.Kill($true)
            }
            catch {
            }
            throw 'The installed EF Core migration tool timed out.'
        }

        $stdout = $stdoutTask.GetAwaiter().GetResult()
        $stderr = $stderrTask.GetAwaiter().GetResult()
        return [pscustomobject]@{
            ExitCode = $process.ExitCode
            Stdout = $stdout
            Stderr = $stderr
        }
    }
    finally {
        $process.Dispose()
    }
}

function Assert-SafeAnalysis {
    param(
        [Parameter(Mandatory = $true)]
        [psobject] $Result,

        [Parameter(Mandatory = $true)]
        [int] $ExpectedExitCode,

        [Parameter(Mandatory = $true)]
        [string] $ExpectedStatus,

        [Parameter(Mandatory = $true)]
        [string] $ExpectedContext,

        [Parameter(Mandatory = $true)]
        [int] $ExpectedMigrationCount
    )

    if ($Result.ExitCode -ne $ExpectedExitCode) {
        throw "Expected analysis exit code $ExpectedExitCode, but received $($Result.ExitCode)."
    }
    if ($Result.Stdout.Contains(
            'TOP-SECRET-EF-FIXTURE',
            [StringComparison]::Ordinal) -or
        $Result.Stderr.Contains(
            'TOP-SECRET-EF-FIXTURE',
            [StringComparison]::Ordinal))
    {
        throw 'Target-controlled fixture output escaped the worker boundary.'
    }

    try {
        $report = $Result.Stdout | ConvertFrom-Json -Depth 64
    }
    catch {
        throw 'The installed EF Core migration tool did not emit one JSON report.'
    }

    if ($null -eq $report -or
        [string]$report.format -cne 'csharpdb-ef-migration-analysis/v1' -or
        [string]$report.provider -cne 'CSharpDB.EntityFrameworkCore' -or
        [string]$report.status -cne $ExpectedStatus -or
        [string]$report.highestEvidence -cne 'bound' -or
        [string]$report.context -cne $ExpectedContext -or
        [int]$report.migrationCount -ne $ExpectedMigrationCount)
    {
        throw 'The installed EF Core migration tool returned an unexpected report contract.'
    }
    if ([string]$report.assemblyDigest -cnotmatch '^[0-9a-f]{64}$' -or
        [string]$report.capabilityDigest -cnotmatch '^[0-9a-f]{64}$')
    {
        throw 'The installed EF Core migration tool returned an invalid report digest.'
    }
    if ([int]$report.commandCount -gt 0 -and
        [string]$report.generatedSqlDigest -cnotmatch '^[0-9a-f]{64}$')
    {
        throw 'The installed EF Core migration tool omitted its generated SQL digest.'
    }

    return $report
}

if ($PSCmdlet.ParameterSetName -eq 'Pack' -and
    -not (Test-Path -LiteralPath $toolProject -PathType Leaf))
{
    throw "The EF Core migration tool project was not found: $toolProject"
}
if (-not (Test-Path -LiteralPath $fixtureProject -PathType Leaf)) {
    throw "The EF Core migration fixture project was not found: $fixtureProject"
}
if (-not (Test-Path -LiteralPath $baseCliProject -PathType Leaf)) {
    throw "The base CLI project was not found: $baseCliProject"
}
if (-not (Test-Path -LiteralPath $webSampleProject -PathType Leaf)) {
    throw "The EF Core Web sample project was not found: $webSampleProject"
}

try {
    if (Test-Path -LiteralPath $webSampleDatabase) {
        throw "The EF Core Web sample database must be absent before analysis: $webSampleDatabase"
    }

    if (-not $NoRestore) {
        if ($PSCmdlet.ParameterSetName -eq 'Pack') {
            Invoke-DotNet `
                -Description 'Restoring the EF Core migration tool.' `
                -Arguments @('restore', $toolProject, '--nologo')
        }
        Invoke-DotNet `
            -Description 'Restoring the EF Core migration fixtures.' `
            -Arguments @('restore', $fixtureProject, '--nologo')
        Invoke-DotNet `
            -Description 'Restoring the base CSharpDB CLI.' `
            -Arguments @('restore', $baseCliProject, '--nologo')
        Invoke-DotNet `
            -Description 'Restoring the EF Core Web sample.' `
            -Arguments @('restore', $webSampleProject, '--nologo')
    }

    $packagePrefix = 'CSharpDB.EntityFrameworkCore.Tools.'
    if ($PSCmdlet.ParameterSetName -eq 'Pack') {
        [System.IO.Directory]::CreateDirectory(
            $packedPackageDirectory) | Out-Null
        $packageDirectory = $packedPackageDirectory
        $packArguments = @(
            'pack',
            $toolProject,
            '-c',
            $Configuration,
            '--nologo',
            '--output',
            $packageDirectory
        )
        if ($NoRestore) {
            $packArguments += '--no-restore'
        }
        Invoke-DotNet `
            -Description 'Packing the EF Core migration tool.' `
            -Arguments $packArguments

        $packageFiles = @(
            Get-ChildItem `
                -LiteralPath $packageDirectory `
                -Filter 'CSharpDB.EntityFrameworkCore.Tools.*.nupkg' `
                -File |
                Where-Object { $_.Name -notlike '*.symbols.nupkg' }
        )
        if ($packageFiles.Count -ne 1) {
            throw 'Packing must produce exactly one EF Core migration tool package.'
        }
        $packagePath = $packageFiles[0].FullName
        $packageName = $packageFiles[0].Name
        $packageVersion = $packageName.Substring(
            $packagePrefix.Length,
            $packageName.Length -
                $packagePrefix.Length -
                '.nupkg'.Length)
        if ([string]::IsNullOrWhiteSpace($packageVersion)) {
            throw 'The EF Core migration tool package version is invalid.'
        }
    }
    else {
        if ($Version -cnotmatch
            '^[0-9A-Za-z]+(?:[0-9A-Za-z.-]*[0-9A-Za-z])?$' -or
            $Version.Contains(
                '..',
                [StringComparison]::Ordinal))
        {
            throw "The requested package version is invalid: $Version"
        }

        $packageDirectory = Resolve-RepoPath $FeedPath
        if (-not (Test-Path `
                -LiteralPath $packageDirectory `
                -PathType Container))
        {
            throw "The local package feed was not found: $packageDirectory"
        }

        $packageVersion = $Version
        $packagePath = Join-Path `
            $packageDirectory `
            "$packagePrefix$packageVersion.nupkg"
        if (-not (Test-Path `
                -LiteralPath $packagePath `
                -PathType Leaf))
        {
            throw "The exact EF Core migration tool package was not found: $packagePath"
        }
    }

    $packageIdentity = Get-NuGetPackageIdentity $packagePath
    if ($packageIdentity.Id -cne
            'CSharpDB.EntityFrameworkCore.Tools' -or
        $packageIdentity.Version -cne $packageVersion)
    {
        throw (
            'Package identity mismatch. Expected ' +
            "CSharpDB.EntityFrameworkCore.Tools $packageVersion, found " +
            "$($packageIdentity.Id) $($packageIdentity.Version).")
    }
    $packageDigest = (
        Get-FileHash -LiteralPath $packagePath -Algorithm SHA256
    ).Hash

    $escapedPackageDirectory =
        [System.Security.SecurityElement]::Escape($packageDirectory)
    $nugetConfigText = @"
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <clear />
    <add key="csharpdb-ef-tool-smoke" value="$escapedPackageDirectory" />
  </packageSources>
</configuration>
"@
    [System.IO.File]::WriteAllText(
        $nugetConfig,
        $nugetConfigText,
        [System.Text.UTF8Encoding]::new($false))

    Invoke-DotNet `
        -Description (
            'Installing the exact EF Core migration tool package ' +
            "$packageVersion.") `
        -Arguments @(
            'tool',
            'install',
            'CSharpDB.EntityFrameworkCore.Tools',
            '--tool-path',
            $toolDirectory,
            '--configfile',
            $nugetConfig,
            '--version',
            $packageVersion,
            '--no-cache')

    $packageDigestAfterInstall = (
        Get-FileHash -LiteralPath $packagePath -Algorithm SHA256
    ).Hash
    if ($packageDigestAfterInstall -cne $packageDigest) {
        throw 'The selected tool package changed while it was being installed.'
    }

    $toolName = if ($IsWindows) {
        'dotnet-csharpdb-ef.exe'
    }
    else {
        'dotnet-csharpdb-ef'
    }
    $toolExecutable = Join-Path $toolDirectory $toolName
    if (-not (Test-Path -LiteralPath $toolExecutable -PathType Leaf)) {
        throw 'The installed EF Core migration tool executable is missing.'
    }

    $publishArguments = @(
        'publish',
        $baseCliProject,
        '-c',
        $Configuration,
        '--nologo',
        '--output',
        $baseCliPublishDirectory
    )
    if ($NoRestore) {
        $publishArguments += '--no-restore'
    }
    Invoke-DotNet `
        -Description 'Publishing the base CSharpDB CLI for isolation checks.' `
        -Arguments $publishArguments

    $forbiddenBaseCliFiles = @(
        Get-ChildItem `
            -LiteralPath $baseCliPublishDirectory `
            -Recurse `
            -File |
            Where-Object {
                $_.Name -like 'Microsoft.EntityFrameworkCore*.dll' -or
                $_.Name -like 'CSharpDB.EntityFrameworkCore*.dll' -or
                $_.Name -like 'dotnet-csharpdb-ef*'
            }
    )
    if ($forbiddenBaseCliFiles.Count -ne 0) {
        throw 'The EF Core analyzer leaked into the base CSharpDB CLI publish output.'
    }
    $baseCliDeps = Join-Path $baseCliPublishDirectory 'csharpdb.deps.json'
    if (-not (Test-Path -LiteralPath $baseCliDeps -PathType Leaf)) {
        throw 'The published base CSharpDB CLI dependency manifest is missing.'
    }
    $baseCliDependencyText = Get-Content -LiteralPath $baseCliDeps -Raw
    if ($baseCliDependencyText.Contains(
            'Microsoft.EntityFrameworkCore',
            [StringComparison]::Ordinal) -or
        $baseCliDependencyText.Contains(
            'CSharpDB.EntityFrameworkCore',
            [StringComparison]::Ordinal))
    {
        throw 'The base CSharpDB CLI dependency graph contains the separate EF Core analyzer.'
    }

    $env:PATH = $toolDirectory +
        [System.IO.Path]::PathSeparator +
        $previousPath

    $validContext =
        'CSharpDB.EntityFrameworkCore.Tools.Fixtures.FixtureContext'
    $validResult = Invoke-CapturedProcess `
        -Executable $dotnetExecutable `
        -WorkingDirectory $root `
        -Arguments @(
            'csharpdb-ef',
            'analyze',
            '--project',
            $fixtureProject,
            '--context',
            $validContext,
            '--format',
            'json')
    $validReport = Assert-SafeAnalysis `
        -Result $validResult `
        -ExpectedExitCode 1 `
        -ExpectedStatus 'conditional' `
        -ExpectedContext $validContext `
        -ExpectedMigrationCount 2
    if ([int]$validReport.operationCount -le 0 -or
        [int]$validReport.commandCount -le 0)
    {
        throw 'The valid fixture did not exercise generated migration commands.'
    }

    $unsupportedContext =
        'CSharpDB.EntityFrameworkCore.Tools.Fixtures.UnsupportedFixtureContext'
    $unsupportedResult = Invoke-CapturedProcess `
        -Executable $dotnetExecutable `
        -WorkingDirectory $root `
        -Arguments @(
            'csharpdb-ef',
            'analyze',
            '--project',
            $fixtureProject,
            '--context',
            $unsupportedContext,
            '--format',
            'json')
    $unsupportedReport = Assert-SafeAnalysis `
        -Result $unsupportedResult `
        -ExpectedExitCode 2 `
        -ExpectedStatus 'unsupported' `
        -ExpectedContext $unsupportedContext `
        -ExpectedMigrationCount 1
    if (-not @($unsupportedReport.diagnostics).Where({
                [string]$_.ruleId -ceq
                    'csharpdb.ef.operation.schema.unsupported'
            }, 'First'))
    {
        throw 'The unsupported fixture did not report the schema rule.'
    }

    $webSampleContext = 'EfCoreMinimalApiSample.TodoDbContext'
    try {
        $webSampleResult = Invoke-CapturedProcess `
            -Executable $dotnetExecutable `
            -WorkingDirectory $root `
            -Arguments @(
                'csharpdb-ef',
                'analyze',
                '--project',
                $webSampleProject,
                '--context',
                $webSampleContext,
                '--format',
                'json')
    }
    finally {
        if (Test-Path -LiteralPath $webSampleDatabase) {
            throw (
                'EF Core migration analysis created the Web sample ' +
                "database: $webSampleDatabase")
        }
    }

    $webSampleReport = Assert-SafeAnalysis `
        -Result $webSampleResult `
        -ExpectedExitCode 1 `
        -ExpectedStatus 'conditional' `
        -ExpectedContext $webSampleContext `
        -ExpectedMigrationCount 0
    $webSampleDiagnostics = @($webSampleReport.diagnostics)
    $webSampleDigestProperty =
        $webSampleReport.PSObject.Properties['generatedSqlDigest']
    if ([int] $webSampleReport.operationCount -ne 0 -or
        [int] $webSampleReport.destructiveOperationCount -ne 0 -or
        [int] $webSampleReport.commandCount -ne 0 -or
        $null -ne $webSampleDigestProperty -and
            -not [string]::IsNullOrEmpty(
                [string] $webSampleDigestProperty.Value) -or
        [string] $webSampleReport.ruleId -cne
            'csharpdb.ef.generation.bound' -or
        $webSampleDiagnostics.Count -ne 1 -or
        [string] $webSampleDiagnostics[0].ruleId -cne
            'csharpdb.ef.generation.bound' -or
        [string] $webSampleDiagnostics[0].severity -cne 'warning' -or
        [string] $webSampleDiagnostics[0].evidence -cne 'bound' -or
        [string] $webSampleDiagnostics[0].summary -cne
            'Migration SQL generation succeeded, but the chain was not executed.' -or
        [string] $webSampleDiagnostics[0].remediation -cne
            'Validate every migration prefix in an isolated scratch database before production use.')
    {
        throw 'The EF Core Web sample returned an unexpected empty-chain report.'
    }

    Write-Host 'EF Core migration tool package and isolation checks are valid.'
}
finally {
    $env:PATH = $previousPath
    $resolvedWorkspace = [System.IO.Path]::GetFullPath($workspace)
    $resolvedParent = [System.IO.Path]::GetFullPath($temporaryParent)
    $expectedPrefix = $resolvedParent.TrimEnd(
        [System.IO.Path]::DirectorySeparatorChar,
        [System.IO.Path]::AltDirectorySeparatorChar) +
        [System.IO.Path]::DirectorySeparatorChar
    $leaf = [System.IO.Path]::GetFileName($resolvedWorkspace)
    $pathComparison = if ($IsWindows) {
        [StringComparison]::OrdinalIgnoreCase
    }
    else {
        [StringComparison]::Ordinal
    }

    if ($resolvedWorkspace.StartsWith(
            $expectedPrefix,
            $pathComparison) -and
        $leaf.StartsWith(
            'efcore-migration-tool-',
            [StringComparison]::Ordinal))
    {
        $workspaceItem = Get-Item -LiteralPath $resolvedWorkspace -Force `
            -ErrorAction SilentlyContinue
        if ($null -ne $workspaceItem) {
            if (($workspaceItem.Attributes -band
                    [System.IO.FileAttributes]::ReparsePoint) -ne 0)
            {
                throw "Refusing to clean a reparse-point workspace: $resolvedWorkspace"
            }

            Remove-Item -LiteralPath $resolvedWorkspace -Recurse -Force `
                -ErrorAction Stop
        }

        if (Test-Path -LiteralPath $resolvedWorkspace) {
            throw "The EF Core tool test workspace could not be removed: $resolvedWorkspace"
        }
    }
    else {
        throw "Refusing to clean an unexpected EF Core tool test workspace: $resolvedWorkspace"
    }
}
