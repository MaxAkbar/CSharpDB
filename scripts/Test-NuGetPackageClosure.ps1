Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).ProviderPath

function Get-NormalizedRelativePath {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path
    )

    return [System.IO.Path]::GetRelativePath($root, $Path).Replace('\', '/')
}

function Resolve-RepoPath {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path
    )

    if ([System.IO.Path]::IsPathRooted($Path)) {
        return [System.IO.Path]::GetFullPath($Path)
    }

    return [System.IO.Path]::GetFullPath((Join-Path $root $Path))
}

function Get-WorkflowPackProjectPaths {
    param(
        [Parameter(Mandatory = $true)]
        [string] $WorkflowPath
    )

    $fullPath = Resolve-RepoPath $WorkflowPath
    if (-not (Test-Path -LiteralPath $fullPath)) {
        throw "Workflow does not exist: $WorkflowPath"
    }

    $projects = @()
    foreach ($line in [System.IO.File]::ReadLines($fullPath)) {
        if ($line -match 'dotnet\s+pack\s+(?<Project>src/[^\s]+?\.csproj)\b') {
            $projects += $Matches.Project
        }
    }

    if ($projects.Count -eq 0) {
        throw "No dotnet pack project lines were found in $WorkflowPath."
    }

    $duplicates = @(
        $projects |
            Group-Object |
            Where-Object { $_.Count -gt 1 } |
            Select-Object -ExpandProperty Name
    )

    if ($duplicates.Count -gt 0) {
        throw ("Duplicate dotnet pack project lines were found in {0}: {1}" -f $WorkflowPath, ($duplicates -join ', '))
    }

    return $projects
}

function Test-SameProjectSet {
    param(
        [Parameter(Mandatory = $true)]
        [string[]] $Expected,

        [Parameter(Mandatory = $true)]
        [string[]] $Actual,

        [Parameter(Mandatory = $true)]
        [string] $ActualName
    )

    $missing = @($Expected | Where-Object { $Actual -notcontains $_ })
    $extra = @($Actual | Where-Object { $Expected -notcontains $_ })

    if ($missing.Count -gt 0 -or $extra.Count -gt 0) {
        Write-Host "NuGet pack project list in $ActualName does not match .github/workflows/release.yml."
        if ($missing.Count -gt 0) {
            Write-Host ("Missing from {0}: {1}" -f $ActualName, ($missing -join ', '))
        }
        if ($extra.Count -gt 0) {
            Write-Host ("Extra in {0}: {1}" -f $ActualName, ($extra -join ', '))
        }
        exit 1
    }
}

function Get-ProjectReferencePaths {
    param(
        [Parameter(Mandatory = $true)]
        [string] $ProjectPath
    )

    [xml] $project = Get-Content -Raw $ProjectPath
    $projectDirectory = Split-Path -Parent $ProjectPath
    $referencePaths = @()

    foreach ($reference in $project.SelectNodes('/Project/ItemGroup/ProjectReference')) {
        $include = $reference.GetAttribute('Include')
        if ([string]::IsNullOrWhiteSpace($include)) {
            continue
        }

        $privateAssets = $reference.GetAttribute('PrivateAssets')
        $referenceOutputAssembly = $reference.GetAttribute('ReferenceOutputAssembly')
        $outputItemType = $reference.GetAttribute('OutputItemType')

        if ($privateAssets -eq 'all' -or
            $referenceOutputAssembly -eq 'false' -or
            $outputItemType -eq 'Analyzer') {
            continue
        }

        $referencePath = [System.IO.Path]::GetFullPath((Join-Path $projectDirectory $include))
        $relativeReferencePath = Get-NormalizedRelativePath $referencePath
        if ($relativeReferencePath -like 'src/*') {
            $referencePaths += $referencePath
        }
    }

    return $referencePaths
}

$releaseWorkflow = '.github/workflows/release.yml'
$ciWorkflow = '.github/workflows/ci.yml'
$publishedProjectPaths = @(Get-WorkflowPackProjectPaths $releaseWorkflow)
$ciProjectPaths = @(Get-WorkflowPackProjectPaths $ciWorkflow)
Test-SameProjectSet -Expected $publishedProjectPaths -Actual $ciProjectPaths -ActualName $ciWorkflow

$publishedProjectFullPaths = @{}

foreach ($projectPath in $publishedProjectPaths) {
    $fullPath = Resolve-RepoPath $projectPath
    if (-not (Test-Path -LiteralPath $fullPath)) {
        throw "Published NuGet project does not exist: $projectPath"
    }

    $publishedProjectFullPaths[$fullPath.ToUpperInvariant()] = $projectPath
}

$violations = @()

foreach ($projectPath in $publishedProjectPaths) {
    $fullPath = Resolve-RepoPath $projectPath
    foreach ($referencePath in Get-ProjectReferencePaths $fullPath) {
        if (-not $publishedProjectFullPaths.ContainsKey($referencePath.ToUpperInvariant())) {
            $violations += [pscustomobject]@{
                Project = $projectPath
                Reference = Get-NormalizedRelativePath $referencePath
            }
        }
    }
}

if ($violations.Count -gt 0) {
    Write-Host 'Published NuGet project closure is incomplete:'
    foreach ($violation in $violations) {
        Write-Host ("- {0} references {1}, but that project is not in the published NuGet project list." -f $violation.Project, $violation.Reference)
    }

    Write-Host ''
    Write-Host 'Add the missing project to the NuGet pack/publish workflows, or mark the ProjectReference as private if it should not become a package dependency.'
    exit 1
}

Write-Host 'Published NuGet project closure is complete.'
