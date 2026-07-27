#requires -Version 7.0

[CmdletBinding()]
param(
    [ValidateSet('Debug', 'Release')]
    [string] $Configuration = 'Release',

    [switch] $NoRestore
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$root = (
    Resolve-Path (Join-Path $PSScriptRoot '..')
).ProviderPath
$temporaryParent = if ($IsWindows) {
    Join-Path $root '.tmp'
}
else {
    $platformTemporaryParent = [IO.Path]::GetTempPath()
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
[IO.Directory]::CreateDirectory(
    $temporaryParent) | Out-Null

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
    ('access-migration-isolation-' +
        [Guid]::NewGuid().ToString('N'))
[IO.Directory]::CreateDirectory(
    $workspace) | Out-Null
Set-PrivateDirectory -Path $workspace

function Assert-NoAccessAssets {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Directory
    )

    $forbidden = @(
        'CSharpDB.Migration.Access',
        'System.Data.OleDb',
        'System.Configuration.ConfigurationManager',
        'System.Diagnostics.EventLog',
        'System.Diagnostics.PerformanceCounter',
        'System.Security.Cryptography.ProtectedData'
    )
    $files = @(
        Get-ChildItem `
            -LiteralPath $Directory `
            -File `
            -Recurse
    )
    foreach ($token in $forbidden) {
        $matching = @(
            $files |
                Where-Object {
                    $_.Name.Contains(
                        $token,
                        [StringComparison]::OrdinalIgnoreCase)
                })
        if ($matching.Count -gt 0) {
            throw "The base CLI contains Access-only asset '$token'."
        }
    }
    foreach ($dependencyFile in @(
        $files |
            Where-Object {
                $_.Name.EndsWith(
                    '.deps.json',
                    [StringComparison]::OrdinalIgnoreCase)
            }))
    {
        $content =
            [IO.File]::ReadAllText(
                $dependencyFile.FullName)
        foreach ($token in $forbidden) {
            if ($content.Contains(
                    $token,
                    [StringComparison]::OrdinalIgnoreCase))
            {
                throw "The base CLI dependency graph contains Access-only dependency '$token'."
            }
        }
    }
}

function Assert-AccessCommandFailure {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Executable,

        [Parameter(Mandatory = $true)]
        [string] $SourcePath,

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
            --source access `
            --input $SourcePath `
            --package $PackagePath `
            --out $CatalogPath `
            --provider ace16 2>&1
    )
    $exitCode = $LASTEXITCODE
    $text =
        $commandOutput -join
            [Environment]::NewLine

    if ($exitCode -ne 2) {
        throw "Expected Access capture to fail with exit code 2, but received $exitCode."
    }
    if (-not $text.Contains(
            $ExpectedCode,
            [StringComparison]::Ordinal))
    {
        throw (
            "Access capture did not report the stable code " +
            "$ExpectedCode. Command output:`n$text")
    }
    if ($text.Contains(
            $SourcePath,
            [StringComparison]::OrdinalIgnoreCase))
    {
        throw 'Access capture exposed the source path in command output.'
    }
    if (Test-Path -LiteralPath $PackagePath) {
        throw 'A failed Access capture published a package artifact.'
    }
    if (Test-Path -LiteralPath $CatalogPath) {
        throw 'A failed Access capture published a catalog artifact.'
    }
}

try {
    $baseOutput =
        Join-Path $workspace 'base'
    $bundleOutput =
        Join-Path $workspace 'bundle'
    [IO.Directory]::CreateDirectory(
        $baseOutput) | Out-Null

    $publishArguments = @(
        'publish',
        (Join-Path `
            $root `
            'src/CSharpDB.Cli/CSharpDB.Cli.csproj'),
        '-c',
        $Configuration,
        '--nologo',
        '-o',
        $baseOutput,
        '-p:UseAppHost=true',
        '-r',
        'win-x64',
        '--self-contained',
        'false'
    )
    if ($NoRestore) {
        $publishArguments += '--no-restore'
    }
    & dotnet @publishArguments
    if ($LASTEXITCODE -ne 0) {
        throw 'The base CSharpDB CLI publish failed.'
    }

    $bundleArguments = @{
        OutputPath = $bundleOutput
        Configuration = $Configuration
        RuntimeIdentifier = 'win-x64'
    }
    if ($NoRestore) {
        $bundleArguments['NoRestore'] = $true
    }
    & (Join-Path `
        $PSScriptRoot `
        'Publish-CSharpDbAccessMigrationBundle.ps1') `
        @bundleArguments

    Assert-NoAccessAssets `
        -Directory $baseOutput

    $worker =
        Join-Path $bundleOutput 'adapters/access'
    foreach ($required in @(
        (Join-Path `
            $bundleOutput `
            'csharpdb.exe'),
        (Join-Path `
            $worker `
            'csharpdb-migration-access-worker.exe'),
        (Join-Path `
            $worker `
            'CSharpDB.Migration.Access.dll'),
        (Join-Path `
            $worker `
            'System.Data.OleDb.dll'),
        (Join-Path `
            $worker `
            'THIRD-PARTY-NOTICES.md'),
        (Join-Path `
            $worker `
            'csharpdb-migration-access-worker.deps.json')))
    {
        if (-not (Test-Path `
                -LiteralPath $required `
                -PathType Leaf))
        {
            throw "The Access isolation bundle is missing $required."
        }
    }

    $sourcePath =
        Join-Path $workspace 'private.accdb'
    [IO.File]::WriteAllBytes(
        $sourcePath,
        [byte[]] @(1, 2, 3, 4))
    Assert-AccessCommandFailure `
        -Executable (Join-Path `
            $baseOutput `
            'csharpdb.exe') `
        -SourcePath $sourcePath `
        -PackagePath (Join-Path `
            $workspace `
            'base.csdbaccess') `
        -CatalogPath (Join-Path `
            $workspace `
            'base-catalog.json') `
        -ExpectedCode `
            'MIG-ACCESS-CLI-ADAPTER-001'
    Assert-AccessCommandFailure `
        -Executable (Join-Path `
            $bundleOutput `
            'csharpdb.exe') `
        -SourcePath $sourcePath `
        -PackagePath (Join-Path `
            $workspace `
            'bundle.csdbaccess') `
        -CatalogPath (Join-Path `
            $workspace `
            'bundle-catalog.json') `
        -ExpectedCode `
            'MIG-ACCESS-CLI-INSPECT-001'

    $global:LASTEXITCODE = 0
    Write-Host (
        'Microsoft Access migration adapter ' +
        'isolation is valid.')
}
finally {
    $resolvedWorkspace =
        [IO.Path]::GetFullPath($workspace)
    $resolvedParent =
        [IO.Path]::GetFullPath(
            $temporaryParent)
    $expectedPrefix =
        $resolvedParent.TrimEnd(
            [IO.Path]::DirectorySeparatorChar,
            [IO.Path]::AltDirectorySeparatorChar) +
        [IO.Path]::DirectorySeparatorChar
    $leaf =
        [IO.Path]::GetFileName(
            $resolvedWorkspace)
    if ($resolvedWorkspace.StartsWith(
            $expectedPrefix,
            [StringComparison]::OrdinalIgnoreCase) -and
        $leaf.StartsWith(
            'access-migration-isolation-',
            [StringComparison]::Ordinal) -and
        (Test-Path -LiteralPath $resolvedWorkspace))
    {
        Remove-Item `
            -LiteralPath $resolvedWorkspace `
            -Recurse `
            -Force
    }
}
