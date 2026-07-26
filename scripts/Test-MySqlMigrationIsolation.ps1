#requires -Version 7.0

[CmdletBinding()]
param(
    [ValidateSet('Debug', 'Release')]
    [string] $Configuration = 'Release',

    [switch] $NoRestore
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).ProviderPath
$temporaryParent = Join-Path $root '.tmp'
[System.IO.Directory]::CreateDirectory($temporaryParent) | Out-Null
$temporaryParentItem = Get-Item -LiteralPath $temporaryParent -Force
if (($temporaryParentItem.Attributes -band
        [System.IO.FileAttributes]::ReparsePoint) -ne 0)
{
    throw "The isolation workspace parent cannot be a reparse point: $temporaryParent"
}

function Set-PrivateDirectory {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path
    )

    if ($IsWindows) {
        $identity = [System.Security.Principal.WindowsIdentity]::GetCurrent(
            [System.Security.Principal.TokenAccessLevels]::Query)
        try {
            $owner = $identity.User
            if ($null -eq $owner) {
                throw 'The current Windows identity does not have a SID.'
            }
            $security = [System.Security.AccessControl.DirectorySecurity]::new()
            $security.SetOwner($owner)
            $security.SetAccessRuleProtection($true, $false)
            $trusted = @(
                $owner,
                [System.Security.Principal.SecurityIdentifier]::new(
                    [System.Security.Principal.WellKnownSidType]::LocalSystemSid,
                    $null),
                [System.Security.Principal.SecurityIdentifier]::new(
                    [System.Security.Principal.WellKnownSidType]::BuiltinAdministratorsSid,
                    $null)
            )
            foreach ($sid in $trusted) {
                $rule = [System.Security.AccessControl.FileSystemAccessRule]::new(
                    $sid,
                    [System.Security.AccessControl.FileSystemRights]::FullControl,
                    [System.Security.AccessControl.InheritanceFlags]::ContainerInherit -bor
                        [System.Security.AccessControl.InheritanceFlags]::ObjectInherit,
                    [System.Security.AccessControl.PropagationFlags]::None,
                    [System.Security.AccessControl.AccessControlType]::Allow)
                $security.AddAccessRule($rule)
            }
            [System.IO.FileSystemAclExtensions]::SetAccessControl(
                [System.IO.DirectoryInfo]::new($Path),
                $security)
        }
        finally {
            $identity.Dispose()
        }
        return
    }

    $privateMode =
        [System.IO.UnixFileMode]::UserRead -bor
        [System.IO.UnixFileMode]::UserWrite -bor
        [System.IO.UnixFileMode]::UserExecute
    [System.IO.File]::SetUnixFileMode(
        $Path,
        [System.IO.UnixFileMode] $privateMode)
}

$workspace = Join-Path `
    $temporaryParent `
    ("mysql-migration-isolation-" + [Guid]::NewGuid().ToString('N'))
[System.IO.Directory]::CreateDirectory($workspace) | Out-Null
Set-PrivateDirectory -Path $workspace

function Invoke-DotNetPublish {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Project,

        [Parameter(Mandatory = $true)]
        [string] $Destination
    )

    $arguments = @(
        'publish',
        $Project,
        '-c',
        $Configuration,
        '--nologo',
        '-o',
        $Destination,
        '-p:UseAppHost=true',
        '--self-contained',
        'false'
    )
    if ($NoRestore) {
        $arguments += '--no-restore'
    }

    & dotnet @arguments
    if ($LASTEXITCODE -ne 0) {
        throw "dotnet publish failed for $Project."
    }
}

function Assert-NoMySqlAssets {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Directory,

        [string] $ExcludedDirectory
    )

    $forbidden = @(
        'CSharpDB.Migration.MySql',
        'MySqlConnector'
    )

    $files = @(Get-ChildItem -LiteralPath $Directory -File -Recurse)
    if (-not [string]::IsNullOrWhiteSpace($ExcludedDirectory)) {
        $excludedPrefix = [System.IO.Path]::GetFullPath(
            $ExcludedDirectory).TrimEnd(
                [System.IO.Path]::DirectorySeparatorChar,
                [System.IO.Path]::AltDirectorySeparatorChar) +
            [System.IO.Path]::DirectorySeparatorChar
        $files = @($files | Where-Object {
            -not $_.FullName.StartsWith(
                $excludedPrefix,
                [StringComparison]::OrdinalIgnoreCase)
        })
    }
    foreach ($token in $forbidden) {
        $matchingFiles = @($files | Where-Object {
            $_.Name.Contains($token, [StringComparison]::OrdinalIgnoreCase)
        })
        if ($matchingFiles.Count -gt 0) {
            throw "The inspected host output contains MySQL-only asset '$token'."
        }
    }

    $dependencyFiles = @($files | Where-Object {
        $_.Extension -eq '.json' -and
        $_.Name.EndsWith('.deps.json', [StringComparison]::OrdinalIgnoreCase)
    })
    foreach ($dependencyFile in $dependencyFiles) {
        $content = [System.IO.File]::ReadAllText($dependencyFile.FullName)
        foreach ($token in $forbidden) {
            if ($content.Contains($token, [StringComparison]::OrdinalIgnoreCase)) {
                throw "The inspected host dependency graph contains MySQL-only dependency '$token'."
            }
        }
    }
}

function Assert-CommandFailure {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Executable,

        [Parameter(Mandatory = $true)]
        [string] $ConnectionEnvironmentName,

        [Parameter(Mandatory = $true)]
        [string] $OutputPath,

        [Parameter(Mandatory = $true)]
        [string] $ExpectedCode
    )

    $commandOutput = @(
        & $Executable `
            migrate inspect `
            --source mysql `
            --connection-env $ConnectionEnvironmentName `
            --out $OutputPath 2>&1
    )
    $exitCode = $LASTEXITCODE
    $text = $commandOutput -join [Environment]::NewLine

    if ($exitCode -ne 2) {
        throw "Expected MySQL inspection to fail with exit code 2, but received $exitCode."
    }
    if (-not $text.Contains($ExpectedCode, [StringComparison]::Ordinal)) {
        throw "MySQL inspection did not report the stable code $ExpectedCode."
    }
    if (Test-Path -LiteralPath $OutputPath) {
        throw 'A failed MySQL inspection published a catalog artifact.'
    }
}

function Assert-CaptureCommandFailure {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Executable,

        [Parameter(Mandatory = $true)]
        [string] $ConnectionEnvironmentName,

        [Parameter(Mandatory = $true)]
        [string] $PackagePath,

        [Parameter(Mandatory = $true)]
        [string] $OutputPath,

        [Parameter(Mandatory = $true)]
        [string] $ExpectedCode
    )

    $commandOutput = @(
        & $Executable `
            migrate inspect `
            --source mysql `
            --connection-env $ConnectionEnvironmentName `
            --package $PackagePath `
            --out $OutputPath 2>&1
    )
    $exitCode = $LASTEXITCODE
    $text = $commandOutput -join [Environment]::NewLine

    if ($exitCode -ne 2) {
        throw "Expected MySQL retained capture to fail with exit code 2, but received $exitCode."
    }
    if (-not $text.Contains($ExpectedCode, [StringComparison]::Ordinal)) {
        throw "MySQL retained capture did not report the stable code $ExpectedCode."
    }
    if (Test-Path -LiteralPath $PackagePath) {
        throw 'A failed MySQL retained capture published a package artifact.'
    }
    if (Test-Path -LiteralPath $OutputPath) {
        throw 'A failed MySQL retained capture published a catalog artifact.'
    }

    $packageParent = [System.IO.Path]::GetDirectoryName(
        [System.IO.Path]::GetFullPath($PackagePath))
    $orphaned = @(
        Get-ChildItem `
            -LiteralPath $packageParent `
            -Directory `
            -Force `
            -Filter '.csharpdb-mysql-capture-*'
    )
    if ($orphaned.Count -gt 0) {
        throw "A failed MySQL retained capture left a private workspace: $($orphaned.FullName -join ', ')"
    }
}

function Assert-ReviewedWorkerPackageClosure {
    param(
        [Parameter(Mandatory = $true)]
        [string] $DependencyPath,

        [Parameter(Mandatory = $true)]
        [string] $NoticePath
    )

    $expectedPackages = @(
        'Microsoft.Extensions.DependencyInjection.Abstractions/8.0.2'
        'Microsoft.Extensions.Logging.Abstractions/8.0.2'
        'MySqlConnector/2.6.1'
    ) | Sort-Object

    $dependencies = [System.IO.File]::ReadAllText($DependencyPath) |
        ConvertFrom-Json -AsHashtable
    $actualPackages = @(
        $dependencies['libraries'].GetEnumerator() |
            Where-Object { $_.Value['type'] -eq 'package' } |
            ForEach-Object { $_.Key } |
            Sort-Object
    )
    if (-not [string]::Equals(
            ($expectedPackages -join "`n"),
            ($actualPackages -join "`n"),
            [StringComparison]::Ordinal))
    {
        $difference = Compare-Object `
            -ReferenceObject $expectedPackages `
            -DifferenceObject $actualPackages `
            -CaseSensitive
        throw "The MySQL worker package closure differs from the reviewed inventory: $($difference.InputObject -join ', ')"
    }

    $notice = [System.IO.File]::ReadAllText($NoticePath)
    foreach ($package in $expectedPackages) {
        $separator = $package.LastIndexOf('/')
        $noticeEntry = '| ' +
            $package.Substring(0, $separator) +
            ' | ' +
            $package.Substring($separator + 1) +
            ' |'
        if (-not $notice.Contains(
                $noticeEntry,
                [StringComparison]::Ordinal))
        {
            throw "The reviewed package is absent from THIRD-PARTY-NOTICES.md: $package"
        }
    }
}

try {
    $baseOutput = Join-Path $workspace 'base'
    $bundleOutput = Join-Path $workspace 'bundle'
    [System.IO.Directory]::CreateDirectory($baseOutput) | Out-Null

    Invoke-DotNetPublish `
        -Project (Join-Path $root 'src/CSharpDB.Cli/CSharpDB.Cli.csproj') `
        -Destination $baseOutput

    $publishArguments = @{
        OutputPath = $bundleOutput
        Configuration = $Configuration
    }
    if ($NoRestore) {
        $publishArguments['NoRestore'] = $true
    }
    & (Join-Path $PSScriptRoot 'Publish-CSharpDbMySqlMigrationBundle.ps1') `
        @publishArguments

    Assert-NoMySqlAssets -Directory $baseOutput

    $workerOutput = Join-Path $bundleOutput 'adapters/mysql'
    $workerName = if ($IsWindows) {
        'csharpdb-migration-mysql-worker.exe'
    }
    else {
        'csharpdb-migration-mysql-worker'
    }
    $requiredWorkerFiles = @(
        (Join-Path $bundleOutput 'LICENSE'),
        (Join-Path $workerOutput $workerName),
        (Join-Path $workerOutput 'CSharpDB.Migration.Retained.dll'),
        (Join-Path $workerOutput 'CSharpDB.Migration.MySql.dll'),
        (Join-Path $workerOutput 'MySqlConnector.dll'),
        (Join-Path $workerOutput 'Microsoft.Extensions.DependencyInjection.Abstractions.dll'),
        (Join-Path $workerOutput 'Microsoft.Extensions.Logging.Abstractions.dll'),
        (Join-Path $workerOutput 'THIRD-PARTY-NOTICES.md'),
        (Join-Path $workerOutput 'csharpdb-migration-mysql-worker.deps.json')
    )
    foreach ($requiredFile in $requiredWorkerFiles) {
        if (-not (Test-Path -LiteralPath $requiredFile -PathType Leaf)) {
            throw "The optional MySQL worker output is missing $requiredFile."
        }
    }

    Assert-ReviewedWorkerPackageClosure `
        -DependencyPath (Join-Path $workerOutput 'csharpdb-migration-mysql-worker.deps.json') `
        -NoticePath (Join-Path $workerOutput 'THIRD-PARTY-NOTICES.md')

    Assert-NoMySqlAssets `
        -Directory $bundleOutput `
        -ExcludedDirectory $workerOutput

    $cliName = if ($IsWindows) { 'csharpdb.exe' } else { 'csharpdb' }
    $missingConnectionName =
        'CSHARPDB_PHASE7B4_MISSING_' + [Guid]::NewGuid().ToString('N')
    Assert-CommandFailure `
        -Executable (Join-Path $baseOutput $cliName) `
        -ConnectionEnvironmentName $missingConnectionName `
        -OutputPath (Join-Path $workspace 'base-catalog.json') `
        -ExpectedCode 'MIG-MYSQL-CLI-ADAPTER-001'
    Assert-CommandFailure `
        -Executable (Join-Path $bundleOutput $cliName) `
        -ConnectionEnvironmentName $missingConnectionName `
        -OutputPath (Join-Path $workspace 'bundle-catalog.json') `
        -ExpectedCode 'MIG-MYSQL-CLI-CONNECTION-001'
    Assert-CaptureCommandFailure `
        -Executable (Join-Path $baseOutput $cliName) `
        -ConnectionEnvironmentName $missingConnectionName `
        -PackagePath (Join-Path $workspace 'base-source.csdbmysql') `
        -OutputPath (Join-Path $workspace 'base-capture-catalog.json') `
        -ExpectedCode 'MIG-MYSQL-CLI-ADAPTER-001'
    Assert-CaptureCommandFailure `
        -Executable (Join-Path $bundleOutput $cliName) `
        -ConnectionEnvironmentName $missingConnectionName `
        -PackagePath (Join-Path $workspace 'bundle-source.csdbmysql') `
        -OutputPath (Join-Path $workspace 'bundle-capture-catalog.json') `
        -ExpectedCode 'MIG-MYSQL-CLI-CONNECTION-001'

    $global:LASTEXITCODE = 0
    Write-Host 'MySQL migration adapter isolation is valid.'
}
finally {
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
            'mysql-migration-isolation-',
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
            throw "The isolation workspace could not be removed: $resolvedWorkspace"
        }
    }
    else {
        throw "Refusing to clean an unexpected isolation workspace: $resolvedWorkspace"
    }
}
