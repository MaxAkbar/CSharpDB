#requires -Version 7.0

[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateNotNullOrEmpty()]
    [string] $OutputPath,

    [ValidateSet('Debug', 'Release')]
    [string] $Configuration = 'Release',

    [ValidateRange(1, 2)]
    [int] $QualificationPass = 1,

    [string] $ReleaseVersion = '',

    [string] $ReleaseCommit = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).ProviderPath
$solutionPath = Join-Path $repoRoot 'CSharpDB.slnx'
$pathComparison = if ($IsWindows) {
    [StringComparison]::OrdinalIgnoreCase
}
else {
    [StringComparison]::Ordinal
}

function Resolve-OutputPath {
    param(
        [Parameter(Mandatory)]
        [string] $Path
    )

    $resolved = if ([IO.Path]::IsPathRooted($Path)) {
        [IO.Path]::GetFullPath($Path)
    }
    else {
        [IO.Path]::GetFullPath((Join-Path (Get-Location).ProviderPath $Path))
    }

    $trimCharacters = [char[]]@(
        [IO.Path]::DirectorySeparatorChar,
        [IO.Path]::AltDirectorySeparatorChar)
    $repoPrefix =
        $repoRoot.TrimEnd($trimCharacters) +
        [IO.Path]::DirectorySeparatorChar
    if ($resolved.Equals($repoRoot, $pathComparison) -or
        $resolved.StartsWith($repoPrefix, $pathComparison)) {
        throw "Qualification output must be outside the repository: $resolved"
    }

    if (Test-Path -LiteralPath $resolved -PathType Leaf) {
        throw "Qualification output path is an existing file: $resolved"
    }

    if (Test-Path -LiteralPath $resolved -PathType Container) {
        $existingEntry =
            Get-ChildItem -LiteralPath $resolved -Force |
            Select-Object -First 1
        if ($null -ne $existingEntry) {
            throw "Qualification output directory must be empty: $resolved"
        }
    }

    return $resolved
}

function Get-RepositoryStatus {
    $status = @(
        & git -C $repoRoot status --porcelain=v1 --untracked-files=all 2>&1
    )
    if ($LASTEXITCODE -ne 0) {
        throw "Could not inspect repository status: $($status -join [Environment]::NewLine)"
    }

    return @(
        $status |
            ForEach-Object { [string] $_ } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )
}

function Assert-CleanRepository {
    param(
        [Parameter(Mandatory)]
        [string] $Stage
    )

    $changes = @(Get-RepositoryStatus)
    if ($changes.Count -gt 0) {
        throw (
            "SQL release qualification requires a clean repository $Stage. " +
            "Changes: $($changes -join ', ')")
    }
}

$resolvedOutputPath = Resolve-OutputPath $OutputPath

$hasReleaseVersion = -not [string]::IsNullOrWhiteSpace($ReleaseVersion)
$hasReleaseCommit = -not [string]::IsNullOrWhiteSpace($ReleaseCommit)
if ($hasReleaseVersion -xor $hasReleaseCommit) {
    throw 'ReleaseVersion and ReleaseCommit must be supplied together.'
}

Assert-CleanRepository -Stage 'before execution'

$evidencePath = Join-Path $resolvedOutputPath 'evidence'
$logsPath = Join-Path $evidencePath 'logs'
$testResultsPath = Join-Path $evidencePath 'test-results'
$workPath = Join-Path $resolvedOutputPath 'work'
$nugetPackagesPath = Join-Path $workPath 'nuget-packages'
$dotnetHomePath = Join-Path $workPath 'dotnet-home'

foreach ($path in @(
        $logsPath,
        $testResultsPath,
        $nugetPackagesPath,
        $dotnetHomePath)) {
    [IO.Directory]::CreateDirectory($path) | Out-Null
}

$env:NUGET_PACKAGES = $nugetPackagesPath
$env:DOTNET_CLI_HOME = $dotnetHomePath
$env:DOTNET_CLI_TELEMETRY_OPTOUT = '1'
$env:DOTNET_NOLOGO = '1'

$completedSteps = [Collections.Generic.List[string]]::new()
$currentStep = ''

function Invoke-QualificationCommand {
    param(
        [Parameter(Mandatory)]
        [string] $StepName,

        [Parameter(Mandatory)]
        [string] $FilePath,

        [Parameter(Mandatory)]
        [string[]] $ArgumentList
    )

    $script:currentStep = $StepName
    $safeName = $StepName -replace '[^A-Za-z0-9._-]+', '-'
    $logPath = Join-Path $logsPath "$safeName.log"

    Write-Host ""
    Write-Host "=== $StepName ==="

    Push-Location $repoRoot
    try {
        & $FilePath @ArgumentList 2>&1 |
            Tee-Object -FilePath $logPath
        $exitCode = $LASTEXITCODE
    }
    finally {
        Pop-Location
    }

    if ($exitCode -ne 0) {
        throw "Qualification step '$StepName' failed with exit code $exitCode."
    }

    $completedSteps.Add($StepName)
}

$startedUtc = [DateTimeOffset]::UtcNow
$failureMessage = ''

try {
    Invoke-QualificationCommand `
        -StepName 'documentation' `
        -FilePath 'pwsh' `
        -ArgumentList @(
            '-NoLogo',
            '-NoProfile',
            '-File',
            (Join-Path $PSScriptRoot 'Test-Documentation.ps1'))

    Invoke-QualificationCommand `
        -StepName 'nuget-package-closure' `
        -FilePath 'pwsh' `
        -ArgumentList @(
            '-NoLogo',
            '-NoProfile',
            '-File',
            (Join-Path $PSScriptRoot 'Test-NuGetPackageClosure.ps1'))

    Invoke-QualificationCommand `
        -StepName 'ef-core-version-consistency' `
        -FilePath 'pwsh' `
        -ArgumentList @(
            '-NoLogo',
            '-NoProfile',
            '-File',
            (Join-Path $PSScriptRoot 'Test-EfCoreVersionConsistency.ps1'))

    if ($hasReleaseVersion) {
        Invoke-QualificationCommand `
            -StepName 'release-tag' `
            -FilePath 'pwsh' `
            -ArgumentList @(
                '-NoLogo',
                '-NoProfile',
                '-File',
                (Join-Path $PSScriptRoot 'Test-ReleaseTag.ps1'),
                '-Version',
                $ReleaseVersion,
                '-TagCommit',
                $ReleaseCommit)
    }

    Invoke-QualificationCommand `
        -StepName 'restore' `
        -FilePath 'dotnet' `
        -ArgumentList @(
            'restore',
            $solutionPath)

    Invoke-QualificationCommand `
        -StepName 'build' `
        -FilePath 'dotnet' `
        -ArgumentList @(
            'build',
            $solutionPath,
            '--configuration',
            $Configuration,
            '--no-restore')

    Invoke-QualificationCommand `
        -StepName 'full-test-suite' `
        -FilePath 'dotnet' `
        -ArgumentList @(
            'test',
            $solutionPath,
            '--configuration',
            $Configuration,
            '--no-build',
            '--no-restore',
            '--verbosity',
            'minimal',
            '--logger',
            'trx',
            '--results-directory',
            $testResultsPath)

    Invoke-QualificationCommand `
        -StepName 'sqlserver-migration-isolation' `
        -FilePath 'pwsh' `
        -ArgumentList @(
            '-NoLogo',
            '-NoProfile',
            '-File',
            (Join-Path $PSScriptRoot 'Test-SqlServerMigrationIsolation.ps1'),
            '-Configuration',
            $Configuration,
            '-NoRestore')

    Invoke-QualificationCommand `
        -StepName 'mysql-migration-isolation' `
        -FilePath 'pwsh' `
        -ArgumentList @(
            '-NoLogo',
            '-NoProfile',
            '-File',
            (Join-Path $PSScriptRoot 'Test-MySqlMigrationIsolation.ps1'),
            '-Configuration',
            $Configuration,
            '-NoRestore')

    if ($IsWindows) {
        # The solution restore does not create the RID-specific assets required
        # by the Access bundle's win-x64 publish, so this step must restore them.
        Invoke-QualificationCommand `
            -StepName 'access-migration-isolation' `
            -FilePath 'pwsh' `
            -ArgumentList @(
                '-NoLogo',
                '-NoProfile',
                '-File',
                (Join-Path $PSScriptRoot 'Test-AccessMigrationIsolation.ps1'),
                '-Configuration',
                $Configuration)
    }

    Invoke-QualificationCommand `
        -StepName 'ef-core-migration-tool' `
        -FilePath 'pwsh' `
        -ArgumentList @(
            '-NoLogo',
            '-NoProfile',
            '-File',
            (Join-Path $PSScriptRoot 'Test-EfCoreMigrationTool.ps1'),
            '-Configuration',
            $Configuration,
            '-NoRestore')
}
catch {
    $failureMessage =
        "Step '$currentStep' failed: $($_.Exception.Message)"
}

try {
    Assert-CleanRepository -Stage 'after execution'
}
catch {
    $cleanlinessFailure = $_.Exception.Message
    if ([string]::IsNullOrWhiteSpace($failureMessage)) {
        $failureMessage = $cleanlinessFailure
    }
    else {
        $failureMessage += " Repository cleanliness also failed: $cleanlinessFailure"
    }
}

$finishedUtc = [DateTimeOffset]::UtcNow
$commit = (& git -C $repoRoot rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0) {
    $commit = '<unresolved>'
}

$status = if ([string]::IsNullOrWhiteSpace($failureMessage)) {
    'PASS'
}
else {
    'FAIL'
}

$summaryLines = [Collections.Generic.List[string]]::new()
$summaryLines.Add('# SQL Release Qualification')
$summaryLines.Add('')
$summaryLines.Add("- Status: **$status**")
$summaryLines.Add("- Commit: ``$commit``")
$summaryLines.Add("- Operating system: ``$($PSVersionTable.OS)``")
$summaryLines.Add("- Configuration: ``$Configuration``")
$summaryLines.Add("- Qualification pass: ``$QualificationPass``")
$summaryLines.Add("- Started (UTC): ``$($startedUtc.ToString('O'))``")
$summaryLines.Add("- Finished (UTC): ``$($finishedUtc.ToString('O'))``")
$summaryLines.Add('')
$summaryLines.Add('## Completed checks')
$summaryLines.Add('')
if ($completedSteps.Count -eq 0) {
    $summaryLines.Add('No checks completed.')
}
else {
    foreach ($step in $completedSteps) {
        $summaryLines.Add("- $step")
    }
}

if (-not [string]::IsNullOrWhiteSpace($failureMessage)) {
    $summaryLines.Add('')
    $summaryLines.Add('## Failure')
    $summaryLines.Add('')
    $summaryLines.Add($failureMessage)
}

$summaryPath = Join-Path $evidencePath 'summary.md'
Set-Content -LiteralPath $summaryPath -Value $summaryLines -Encoding utf8

if (-not [string]::IsNullOrWhiteSpace($failureMessage)) {
    throw $failureMessage
}

Write-Host ""
Write-Host "SQL release qualification pass $QualificationPass succeeded."
Write-Host "Evidence: $evidencePath"
