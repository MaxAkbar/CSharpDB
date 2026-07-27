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
$temporaryParent = if ($IsWindows) {
    Join-Path $root '.tmp'
}
else {
    $platformTemporaryParent = [System.IO.Path]::GetTempPath()
    if ($IsMacOS -and
        ($platformTemporaryParent.StartsWith(
                '/var/',
                [StringComparison]::Ordinal) -or
         $platformTemporaryParent.StartsWith(
                '/tmp/',
                [StringComparison]::Ordinal)))
    {
        "/private$platformTemporaryParent"
    }
    else {
        $platformTemporaryParent
    }
}
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
    ("sqlserver-migration-isolation-" + [Guid]::NewGuid().ToString('N'))
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

function Assert-NoSqlServerAssets {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Directory,

        [string] $ExcludedDirectory
    )

    $forbidden = @(
        'CSharpDB.Migration.SqlServer',
        'Microsoft.Data.SqlClient',
        'Microsoft.SqlServer.TransactSql.ScriptDom',
        'Microsoft.Data.SqlClient.SNI',
        'Microsoft.IdentityModel',
        'System.IdentityModel.Tokens.Jwt',
        'Microsoft.SqlServer.Server'
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
            throw "The inspected host output contains SQL Server-only asset '$token'."
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
                throw "The inspected host dependency graph contains SQL Server-only dependency '$token'."
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
            --source sqlserver `
            --connection-env $ConnectionEnvironmentName `
            --out $OutputPath 2>&1
    )
    $exitCode = $LASTEXITCODE
    $text = $commandOutput -join [Environment]::NewLine

    if ($exitCode -ne 2) {
        throw "Expected SQL Server inspection to fail with exit code 2, but received $exitCode."
    }
    if (-not $text.Contains($ExpectedCode, [StringComparison]::Ordinal)) {
        throw "SQL Server inspection did not report the stable code $ExpectedCode."
    }
    if (Test-Path -LiteralPath $OutputPath) {
        throw 'A failed SQL Server inspection published a catalog artifact.'
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
        [string] $CatalogPath,

        [Parameter(Mandatory = $true)]
        [string] $ExpectedCode
    )

    $commandOutput = @(
        & $Executable `
            migrate inspect `
            --source sqlserver `
            --connection-env $ConnectionEnvironmentName `
            --package $PackagePath `
            --out $CatalogPath `
            --table-timeout-seconds 1 2>&1
    )
    $exitCode = $LASTEXITCODE
    $text = $commandOutput -join [Environment]::NewLine

    if ($exitCode -ne 2) {
        throw "Expected SQL Server retained capture to fail with exit code 2, but received $exitCode."
    }
    if (-not $text.Contains($ExpectedCode, [StringComparison]::Ordinal)) {
        throw (
            "SQL Server retained capture did not report the stable code " +
            "$ExpectedCode. Command output:`n$text")
    }
    if ((Test-Path -LiteralPath $PackagePath) -or
        (Test-Path -LiteralPath $CatalogPath))
    {
        throw 'A failed SQL Server retained capture published an artifact.'
    }

    $parent = [System.IO.Path]::GetDirectoryName(
        [System.IO.Path]::GetFullPath($PackagePath))
    $orphaned = @(
        Get-ChildItem `
            -LiteralPath $parent `
            -Directory `
            -Filter '.csharpdb-sqlserver-capture-*' `
            -Force
    )
    if ($orphaned.Count -gt 0) {
        throw "A failed SQL Server retained capture left a private workspace: $($orphaned.FullName -join ', ')"
    }
}

function Assert-DdlCommandFailure {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Executable,

        [Parameter(Mandatory = $true)]
        [string] $ScriptPath,

        [Parameter(Mandatory = $true)]
        [string] $ExpectedCode
    )

    $commandOutput = @(
        & $Executable `
            migrate ddl-check $ScriptPath `
            --dialect tsql 2>&1
    )
    $exitCode = $LASTEXITCODE
    $text = $commandOutput -join [Environment]::NewLine

    if ($exitCode -ne 2) {
        throw "Expected T-SQL DDL analysis to fail with exit code 2, but received $exitCode."
    }
    if (-not $text.Contains($ExpectedCode, [StringComparison]::Ordinal)) {
        throw "T-SQL DDL analysis did not report the stable code $ExpectedCode."
    }
}

function Assert-DdlCommandProof {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Executable,

        [Parameter(Mandatory = $true)]
        [string] $ScriptPath
    )

    $commandOutput = @(
        & $Executable `
            migrate ddl-check $ScriptPath `
            --dialect tsql 2>&1
    )
    $exitCode = $LASTEXITCODE
    $text = $commandOutput -join [Environment]::NewLine

    if ($exitCode -ne 1) {
        throw "Expected a proven T-SQL canonical rewrite with exit code 1, but received $exitCode."
    }
    if (-not $text.Contains(
            'Source grammar: tsql160',
            [StringComparison]::Ordinal) -or
        -not $text.Contains(
            'Status: compatible-with-rewrite',
            [StringComparison]::Ordinal))
    {
        throw 'The bundled T-SQL DDL proof did not return the expected fixed-grammar rewrite evidence.'
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
        'Microsoft.Bcl.Cryptography/9.0.13'
        'Microsoft.Data.SqlClient.Extensions.Abstractions/7.0.2'
        'Microsoft.Data.SqlClient.Internal.Logging/7.0.2'
        'Microsoft.Data.SqlClient.SNI.runtime/6.0.2'
        'Microsoft.Data.SqlClient/7.0.2'
        'Microsoft.Extensions.Caching.Abstractions/9.0.13'
        'Microsoft.Extensions.Caching.Memory/9.0.13'
        'Microsoft.Extensions.DependencyInjection.Abstractions/10.0.0'
        'Microsoft.Extensions.Logging.Abstractions/10.0.0'
        'Microsoft.Extensions.Options/9.0.13'
        'Microsoft.Extensions.Primitives/9.0.13'
        'Microsoft.IdentityModel.Abstractions/8.16.0'
        'Microsoft.IdentityModel.JsonWebTokens/8.16.0'
        'Microsoft.IdentityModel.Logging/8.16.0'
        'Microsoft.IdentityModel.Protocols.OpenIdConnect/8.16.0'
        'Microsoft.IdentityModel.Protocols/8.16.0'
        'Microsoft.IdentityModel.Tokens/8.16.0'
        'Microsoft.SqlServer.Server/1.0.0'
        'Microsoft.SqlServer.TransactSql.ScriptDom/180.59.2'
        'System.Configuration.ConfigurationManager/9.0.13'
        'System.Diagnostics.EventLog/9.0.13'
        'System.IdentityModel.Tokens.Jwt/8.16.0'
        'System.Security.Cryptography.Pkcs/9.0.13'
        'System.Security.Cryptography.ProtectedData/9.0.13'
    ) | Sort-Object

    $dependencies = [System.IO.File]::ReadAllText($DependencyPath) |
        ConvertFrom-Json -AsHashtable
    $forbiddenLibraryPrefixes = @(
        'CSharpDB.Migration.CSharpDb/',
        'CSharpDB.Migration.Files/',
        'CsvHelper/'
    )
    $unexpectedLibraries = @(
        $dependencies['libraries'].Keys |
            Where-Object {
                $library = $_
                $forbiddenLibraryPrefixes |
                    Where-Object {
                        $library.StartsWith($_, [StringComparison]::Ordinal)
                    }
            }
    )
    if ($unexpectedLibraries.Count -gt 0) {
        throw "The SQL Server worker contains excluded migration/file-import dependencies: $($unexpectedLibraries -join ', ')"
    }

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
        throw "The SQL Server worker package closure differs from the reviewed inventory: $($difference.InputObject -join ', ')"
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
    & (Join-Path $PSScriptRoot 'Publish-CSharpDbSqlServerMigrationBundle.ps1') `
        @publishArguments

    Assert-NoSqlServerAssets -Directory $baseOutput

    $workerOutput = Join-Path $bundleOutput 'adapters/sqlserver'
    $requiredWorkerFiles = @(
        (Join-Path $bundleOutput 'LICENSE'),
        (Join-Path $workerOutput 'CSharpDB.Migration.CSharpDb.Ddl.dll'),
        (Join-Path $workerOutput 'CSharpDB.Migration.Retained.dll'),
        (Join-Path $workerOutput 'CSharpDB.Migration.SqlServer.dll'),
        (Join-Path $workerOutput 'Microsoft.Data.SqlClient.dll'),
        (Join-Path $workerOutput 'Microsoft.SqlServer.TransactSql.ScriptDom.dll'),
        (Join-Path $workerOutput 'THIRD-PARTY-NOTICES.md'),
        (Join-Path $workerOutput 'licenses/Microsoft.Data.SqlClient.SNI.runtime-6.0.2-LICENSE.txt'),
        (Join-Path $workerOutput 'csharpdb-migration-sqlserver-worker.deps.json')
    )
    foreach ($requiredFile in $requiredWorkerFiles) {
        if (-not (Test-Path -LiteralPath $requiredFile -PathType Leaf)) {
            throw "The optional SQL Server worker output is missing $requiredFile."
        }
    }

    $sniLicense = Join-Path `
        $workerOutput `
        'licenses/Microsoft.Data.SqlClient.SNI.runtime-6.0.2-LICENSE.txt'
    $expectedSniLicenseHash =
        '9335E8BAD875DD7BE4EEBD55D2335EB6433D1CEA61AADB3817AF7807BEF8932A'
    $actualSniLicenseHash = (Get-FileHash `
            -LiteralPath $sniLicense `
            -Algorithm SHA256).Hash
    if (-not $actualSniLicenseHash.Equals(
            $expectedSniLicenseHash,
            [StringComparison]::OrdinalIgnoreCase))
    {
        throw 'The published SNI runtime license does not match the reviewed 6.0.2 terms.'
    }

    Assert-ReviewedWorkerPackageClosure `
        -DependencyPath (Join-Path $workerOutput 'csharpdb-migration-sqlserver-worker.deps.json') `
        -NoticePath (Join-Path $workerOutput 'THIRD-PARTY-NOTICES.md')

    Assert-NoSqlServerAssets `
        -Directory $bundleOutput `
        -ExcludedDirectory $workerOutput

    $cliName = if ($IsWindows) { 'csharpdb.exe' } else { 'csharpdb' }
    $missingConnectionName =
        'CSHARPDB_PHASE7A8_MISSING_' + [Guid]::NewGuid().ToString('N')
    Assert-CommandFailure `
        -Executable (Join-Path $baseOutput $cliName) `
        -ConnectionEnvironmentName $missingConnectionName `
        -OutputPath (Join-Path $workspace 'base-catalog.json') `
        -ExpectedCode 'MIG-SQLSERVER-CLI-ADAPTER-001'
    Assert-CommandFailure `
        -Executable (Join-Path $bundleOutput $cliName) `
        -ConnectionEnvironmentName $missingConnectionName `
        -OutputPath (Join-Path $workspace 'bundle-catalog.json') `
        -ExpectedCode 'MIG-SQLSERVER-CLI-CONNECTION-001'
    Assert-CaptureCommandFailure `
        -Executable (Join-Path $baseOutput $cliName) `
        -ConnectionEnvironmentName $missingConnectionName `
        -PackagePath (Join-Path $workspace 'base-source.csdbsqlserver') `
        -CatalogPath (Join-Path $workspace 'base-retained-catalog.json') `
        -ExpectedCode 'MIG-SQLSERVER-CLI-ADAPTER-001'
    Assert-CaptureCommandFailure `
        -Executable (Join-Path $bundleOutput $cliName) `
        -ConnectionEnvironmentName $missingConnectionName `
        -PackagePath (Join-Path $workspace 'bundle-source.csdbsqlserver') `
        -CatalogPath (Join-Path $workspace 'bundle-retained-catalog.json') `
        -ExpectedCode 'MIG-SQLSERVER-CLI-CONNECTION-001'

    $ddlPath = Join-Path $workspace 'bounded-ddl.sql'
    [System.IO.File]::WriteAllText(
        $ddlPath,
        'CREATE TABLE dbo.widgets (id int NOT NULL PRIMARY KEY);')
    Assert-DdlCommandFailure `
        -Executable (Join-Path $baseOutput $cliName) `
        -ScriptPath $ddlPath `
        -ExpectedCode 'MIG-TSQL-CLI-ADAPTER-001'
    Assert-DdlCommandProof `
        -Executable (Join-Path $bundleOutput $cliName) `
        -ScriptPath $ddlPath

    $global:LASTEXITCODE = 0
    Write-Host 'SQL Server migration adapter isolation is valid.'
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
            'sqlserver-migration-isolation-',
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
