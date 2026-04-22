[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$RunManifest,

    [string]$ReadmePath,

    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

function Test-Property {
    param(
        [Parameter(Mandatory = $true)] $Object,
        [Parameter(Mandatory = $true)] [string] $Name
    )

    return $Object.PSObject.Properties.Name -contains $Name
}

function Convert-ToDouble {
    param([Parameter(Mandatory = $true)] [string] $Value)

    return [double]::Parse($Value, [System.Globalization.CultureInfo]::InvariantCulture)
}

function Format-Rate {
    param(
        [Parameter(Mandatory = $true)] [double] $Value,
        [Parameter(Mandatory = $true)] [string] $Unit
    )

    if ([math]::Abs($Value) -ge 1000000) {
        return ("{0:N2}M {1}" -f ($Value / 1000000.0), $Unit)
    }

    if ([math]::Abs($Value) -ge 1000) {
        return ("{0:N2}K {1}" -f ($Value / 1000.0), $Unit)
    }

    return ("{0:N1} {1}" -f $Value, $Unit)
}

function Format-Cell {
    param(
        [Parameter(Mandatory = $true)] [double] $Value,
        [Parameter(Mandatory = $true)] $Cell
    )

    $format = if (Test-Property $Cell "format") { [string]$Cell.format } else { "number" }

    switch ($format) {
        "rate" {
            $unit = if (Test-Property $Cell "unit") { [string]$Cell.unit } else { "ops/sec" }
            return Format-Rate $Value $unit
        }
        "ms" {
            $decimals = if (Test-Property $Cell "decimals") { [int]$Cell.decimals } else { 4 }
            return ("{0:N$decimals} ms" -f $Value)
        }
        "integer" {
            return ("{0:N0}" -f $Value)
        }
        "number" {
            $decimals = if (Test-Property $Cell "decimals") { [int]$Cell.decimals } else { 1 }
            return ("{0:N$decimals}" -f $Value)
        }
        default {
            throw "Unknown cell format '$format'."
        }
    }
}

function Get-ExtraValue {
    param(
        [Parameter(Mandatory = $true)] [string] $ExtraInfo,
        [Parameter(Mandatory = $true)] [string] $Key
    )

    $escaped = [regex]::Escape($Key)
    $match = [regex]::Match($ExtraInfo, "(?:^|[,;]\s*)$escaped=([^,;]+)")
    if (-not $match.Success) {
        throw "ExtraInfo key '$Key' was not found in '$ExtraInfo'."
    }

    return $match.Groups[1].Value.Trim()
}

function Resolve-RepoPath {
    param(
        [Parameter(Mandatory = $true)] [string] $RepoRoot,
        [Parameter(Mandatory = $true)] [string] $Path
    )

    if ([System.IO.Path]::IsPathRooted($Path)) {
        return $Path
    }

    return (Join-Path $RepoRoot $Path)
}

function Get-CellText {
    param(
        [Parameter(Mandatory = $true)] $Cell,
        [Parameter(Mandatory = $true)] $ArtifactRows
    )

    if (Test-Property $Cell "value") {
        return [string]$Cell.value
    }

    $artifactId = [string]$Cell.artifact
    $rowName = [string]$Cell.row
    if (-not $ArtifactRows.ContainsKey($artifactId)) {
        throw "Unknown artifact id '$artifactId'."
    }

    $rowsByName = $ArtifactRows[$artifactId]
    if (-not $rowsByName.ContainsKey($rowName)) {
        throw "Row '$rowName' was not found in artifact '$artifactId'."
    }

    $csvRow = $rowsByName[$rowName]
    $rawValue = $null

    if (Test-Property $Cell "extraKey") {
        $rawValue = Get-ExtraValue ([string]$csvRow.ExtraInfo) ([string]$Cell.extraKey)
    } else {
        $columnName = if (Test-Property $Cell "column") { [string]$Cell.column } else { "OpsPerSec" }
        if (-not (Test-Property $csvRow $columnName)) {
            throw "Column '$columnName' was not found on row '$rowName'."
        }

        $rawValue = [string]$csvRow.$columnName
    }

    $value = Convert-ToDouble $rawValue
    if (Test-Property $Cell "multiplier") {
        $value *= [double]$Cell.multiplier
    }

    return Format-Cell $value $Cell
}

function Add-MarkdownTable {
    param(
        [Parameter(Mandatory = $true)] $Lines,
        [Parameter(Mandatory = $true)] $Section,
        [Parameter(Mandatory = $true)] $ArtifactRows
    )

    if ($Lines -is [object[]]) {
        $Lines = $Lines[0]
    }

    $columns = @($Section.columns | ForEach-Object { [string]$_ })
    $Lines.Add("| " + ($columns -join " | ") + " |")
    $Lines.Add("|" + (($columns | ForEach-Object { "---" }) -join "|") + "|")

    foreach ($row in @($Section.rows)) {
        $cells = @($row.cells | ForEach-Object { Get-CellText $_ $ArtifactRows })
        $Lines.Add("| " + ($cells -join " | ") + " |")
    }
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptDir "..\..\..")).Path

if ([string]::IsNullOrWhiteSpace($ReadmePath)) {
    $ReadmePath = Join-Path $repoRoot "tests\CSharpDB.Benchmarks\README.md"
} else {
    $ReadmePath = Resolve-RepoPath $repoRoot $ReadmePath
}

$manifestPath = Resolve-RepoPath $repoRoot $RunManifest
$manifest = Get-Content -Raw -LiteralPath $manifestPath | ConvertFrom-Json

$artifactRows = @{}
foreach ($artifact in @($manifest.artifacts)) {
    $artifactPath = Resolve-RepoPath $repoRoot ([string]$artifact.path)
    if (-not (Test-Path -LiteralPath $artifactPath)) {
        throw "Artifact '$($artifact.id)' does not exist at '$artifactPath'."
    }

    $rowsByName = @{}
    foreach ($row in Import-Csv -LiteralPath $artifactPath) {
        $rowsByName[[string]$row.Name] = $row
    }

    $artifactRows[[string]$artifact.id] = $rowsByName
}

$generated = [System.Collections.Generic.List[string]]::new()
$generated.Add("<!-- This block is generated by tests/CSharpDB.Benchmarks/scripts/Update-BenchmarkReadme.ps1. Edit release-core-manifest.json instead. -->")
$generated.Add("")

if (Test-Property $manifest "metadata") {
    $generated.Add("### Snapshot Metadata")
    $generated.Add("")
    $generated.Add("| Field | Value |")
    $generated.Add("|---|---|")
    foreach ($property in $manifest.metadata.PSObject.Properties) {
        $generated.Add("| $($property.Name) | $($property.Value) |")
    }
    $generated.Add("")
}

if (Test-Property $manifest "artifacts") {
    $generated.Add("### Approved Source Artifacts")
    $generated.Add("")
    $generated.Add("| Artifact | Command | Source CSV |")
    $generated.Add("|---|---|---|")
    foreach ($artifact in @($manifest.artifacts)) {
        $artifactId = [string]$artifact.id
        $artifactCommand = [string]$artifact.command
        $artifactSource = [string]$artifact.path
        $generated.Add("| " + '`' + $artifactId + '`' + " | " + '`' + $artifactCommand + '`' + " | " + '`' + $artifactSource + '`' + " |")
    }
    $generated.Add("")
}

$sections = @($manifest.sections)
for ($sectionIndex = 0; $sectionIndex -lt $sections.Count; $sectionIndex++) {
    $section = $sections[$sectionIndex]
    if ($sectionIndex -eq 1) {
        $generated.Add("## Current Core Results")
        $generated.Add("")
        $generated.Add("These detailed tables are generated from the approved source artifacts listed above.")
        $generated.Add("")
    }

    $generated.Add("### $($section.title)")
    $generated.Add("")
    if (Test-Property $section "description") {
        foreach ($line in @($section.description)) {
            $generated.Add([string]$line)
        }
        $generated.Add("")
    }

    Add-MarkdownTable -Lines (,$generated) -Section $section -ArtifactRows $artifactRows
    $generated.Add("")
}

$beginMarker = "<!-- BENCHMARK_RESULTS_BEGIN -->"
$endMarker = "<!-- BENCHMARK_RESULTS_END -->"
$newBlock = $beginMarker + "`r`n" + ($generated -join "`r`n").TrimEnd() + "`r`n" + $endMarker

$readme = Get-Content -Raw -LiteralPath $ReadmePath
$pattern = "(?s)<!-- BENCHMARK_RESULTS_BEGIN -->.*?<!-- BENCHMARK_RESULTS_END -->"
if (-not [regex]::IsMatch($readme, $pattern)) {
    throw "README '$ReadmePath' does not contain the benchmark result markers."
}

$updated = [regex]::Replace(
    $readme,
    $pattern,
    [System.Text.RegularExpressions.MatchEvaluator] { param($match) $newBlock },
    1)

if ($DryRun) {
    if ($updated -eq $readme) {
        Write-Host "README is already up to date for '$RunManifest'."
    } else {
        Write-Host "README would be updated from '$RunManifest'."
    }
    return
}

[System.IO.File]::WriteAllText($ReadmePath, $updated, [System.Text.UTF8Encoding]::new($false))
Write-Host "Updated README generated benchmark tables from '$RunManifest'."
