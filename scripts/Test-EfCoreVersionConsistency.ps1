[CmdletBinding()]
param(
    [string]$RepositoryRoot = (Split-Path -Parent $PSScriptRoot)
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$repositoryRootPath = [System.IO.Path]::GetFullPath($RepositoryRoot)
$buildPropsPath = Join-Path $repositoryRootPath "Directory.Build.props"
$dotnetToolsPath = Join-Path $repositoryRootPath ".config/dotnet-tools.json"
$qualifiedVersionProperty = "CSharpDbQualifiedEfCoreVersion"
$qualifiedVersionExpression = '$(CSharpDbQualifiedEfCoreVersion)'

function Get-CSharpProjectPaths {
    param(
        [Parameter(Mandatory)]
        [string]$RootPath
    )

    $excludedDirectoryNames = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::OrdinalIgnoreCase)
    foreach ($directoryName in @("bin", "obj", "artifacts", ".artifacts", "publish", "node_modules")) {
        [void]$excludedDirectoryNames.Add($directoryName)
    }

    $pendingDirectories = [System.Collections.Generic.Queue[string]]::new()
    $pendingDirectories.Enqueue($RootPath)
    while ($pendingDirectories.Count -gt 0) {
        $currentDirectory = $pendingDirectories.Dequeue()
        foreach ($projectPath in [System.IO.Directory]::EnumerateFiles(
                $currentDirectory,
                "*.csproj",
                [System.IO.SearchOption]::TopDirectoryOnly)) {
            $projectPath
        }

        foreach ($directoryPath in [System.IO.Directory]::EnumerateDirectories($currentDirectory)) {
            if ($excludedDirectoryNames.Contains([System.IO.Path]::GetFileName($directoryPath))) {
                continue
            }

            $attributes = [System.IO.File]::GetAttributes($directoryPath)
            if (($attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
                continue
            }

            $pendingDirectories.Enqueue($directoryPath)
        }
    }
}

if (-not (Test-Path -LiteralPath $buildPropsPath -PathType Leaf)) {
    throw "Shared build properties were not found at '$buildPropsPath'."
}

if (-not (Test-Path -LiteralPath $dotnetToolsPath -PathType Leaf)) {
    throw "Repository-local dotnet tool configuration was not found at '$dotnetToolsPath'."
}

[xml]$buildProps = Get-Content -LiteralPath $buildPropsPath -Raw
$versionNodes = @($buildProps.SelectNodes("/Project/PropertyGroup/$qualifiedVersionProperty"))
if ($versionNodes.Count -ne 1) {
    throw "Expected exactly one $qualifiedVersionProperty property in '$buildPropsPath'; found $($versionNodes.Count)."
}

$qualifiedVersion = $versionNodes[0].InnerText.Trim()
if ([string]::IsNullOrWhiteSpace($qualifiedVersion)) {
    throw "$qualifiedVersionProperty must not be empty."
}

$dotnetTools = Get-Content -LiteralPath $dotnetToolsPath -Raw | ConvertFrom-Json
$dotnetEfVersion = [string]$dotnetTools.tools.'dotnet-ef'.version
if ($dotnetEfVersion -ne $qualifiedVersion) {
    throw "EF Core version mismatch: Directory.Build.props declares '$qualifiedVersion', but .config/dotnet-tools.json pins dotnet-ef '$dotnetEfVersion'."
}

$projectRoots = @("src", "tests", "samples")
$projectFiles = @(
    foreach ($projectRoot in $projectRoots) {
        $projectRootPath = Join-Path $repositoryRootPath $projectRoot
        if (-not (Test-Path -LiteralPath $projectRootPath -PathType Container)) {
            continue
        }

        Get-CSharpProjectPaths -RootPath $projectRootPath
    }
) | Sort-Object

$qualifiedReferences = [System.Collections.Generic.List[object]]::new()
$validationErrors = [System.Collections.Generic.List[string]]::new()

foreach ($projectFile in $projectFiles) {
    [xml]$project = Get-Content -LiteralPath $projectFile -Raw
    $packageReferences = @($project.SelectNodes("/Project/ItemGroup/PackageReference"))
    $hasEfCoreReference = @(
        $packageReferences | Where-Object {
            $candidateName = $_.GetAttribute("Include")
            $candidateName -match "^Microsoft\.EntityFrameworkCore(?:\.|$)" -or
            $candidateName -eq "Microsoft.AspNetCore.Identity.EntityFrameworkCore"
        }
    ).Count -gt 0

    foreach ($packageReference in $packageReferences) {
        $packageName = $packageReference.GetAttribute("Include")
        $isEfCorePackage =
            $packageName -match "^Microsoft\.EntityFrameworkCore(?:\.|$)" -or
            $packageName -eq "Microsoft.AspNetCore.Identity.EntityFrameworkCore"
        $isEfCoreAlignedSqliteDependency =
            $hasEfCoreReference -and $packageName -eq "Microsoft.Data.Sqlite"
        if (-not $isEfCorePackage -and -not $isEfCoreAlignedSqliteDependency) {
            continue
        }

        $version = $packageReference.GetAttribute("Version")
        if ([string]::IsNullOrWhiteSpace($version) -and $null -ne $packageReference.SelectSingleNode("Version")) {
            $version = [string]$packageReference.SelectSingleNode("Version").InnerText
        }

        $relativeProjectPath = [System.IO.Path]::GetRelativePath($repositoryRootPath, $projectFile)
        $qualifiedReferences.Add([pscustomobject]@{
                Package = $packageName
                Project = $relativeProjectPath
                Version = $version
            })

        $versionOverride = $packageReference.GetAttribute("VersionOverride")
        if (-not [string]::IsNullOrWhiteSpace($versionOverride)) {
            $validationErrors.Add(
                "$relativeProjectPath overrides $packageName with version '$versionOverride'; use '$qualifiedVersionExpression' on Version instead."
            )
        }

        if ($version -ne $qualifiedVersionExpression) {
            $validationErrors.Add(
                "$relativeProjectPath references $packageName with version '$version'; expected '$qualifiedVersionExpression' (currently $qualifiedVersion)."
            )
        }
    }
}

if ($qualifiedReferences.Count -eq 0) {
    throw "No EF Core package references were found under src, tests, or samples."
}

if ($validationErrors.Count -gt 0) {
    throw "EF Core version validation failed:`n - $($validationErrors -join "`n - ")"
}

Write-Host (
    "EF Core version validation passed: {0} package references use {1} ({2}), matching dotnet-ef." -f
    $qualifiedReferences.Count,
    $qualifiedVersionExpression,
    $qualifiedVersion
)
