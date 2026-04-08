<#
.SYNOPSIS
Deletes completed GitHub Actions workflow runs older than a configured age.

.DESCRIPTION
This script uses the GitHub CLI (`gh`) to enumerate workflow runs for a
repository and delete completed runs whose `created_at` timestamp is older than
the requested cutoff.

By default it targets runs older than 7 days and infers the repository from the
local `origin` remote. It only deletes runs with `status = completed`, so
queued, in-progress, and waiting runs are left alone.

.PARAMETER OlderThanDays
Delete completed runs created before this many days ago. Defaults to 7.

.PARAMETER Repo
Repository in `owner/name` form. If omitted, the script resolves it from the
local `origin` remote URL.

.PARAMETER WorkflowName
Optional workflow name filter, for example `CI` or `Perf Guardrails`.

.PARAMETER KeepLatest
Keep this many of the newest matching runs even if they are older than the
cutoff. Defaults to 0.

.PARAMETER PassThru
Return a summary object at the end instead of only writing status lines.

.EXAMPLE
powershell -ExecutionPolicy Bypass -File .\scripts\Clear-GitHubWorkflowRuns.ps1

Deletes completed workflow runs older than 7 days for the repository inferred
from the current checkout's `origin` remote.

.EXAMPLE
.\scripts\Clear-GitHubWorkflowRuns.ps1 -OlderThanDays 30 -WhatIf

Shows which runs would be deleted without actually deleting them.

.EXAMPLE
.\scripts\Clear-GitHubWorkflowRuns.ps1 -Repo MaxAkbar/CSharpDB -WorkflowName "Perf Guardrails"

Deletes completed `Perf Guardrails` runs older than 7 days for the target
repository.
#>
[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'High')]
param(
    [ValidateRange(0, 3650)]
    [int]$OlderThanDays = 7,

    [string]$Repo,

    [string]$WorkflowName,

    [ValidateRange(0, 100000)]
    [int]$KeepLatest = 0,

    [switch]$PassThru
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Assert-GitHubCliAvailable {
    $command = Get-Command gh -ErrorAction SilentlyContinue
    if ($null -eq $command) {
        throw "GitHub CLI 'gh' was not found on PATH."
    }
}

function Resolve-RepositoryFromOrigin {
    $originUrl = git remote get-url origin 2>$null
    if (-not $originUrl) {
        throw "Unable to resolve the repository from 'origin'. Pass -Repo owner/name explicitly."
    }

    $normalized = $originUrl.Trim()
    if ($normalized -match 'github\.com[:/](?<repo>[^/\s]+/[^/\s]+?)(?:\.git)?$') {
        return $Matches.repo
    }

    throw "Could not parse a GitHub repository from origin URL '$normalized'. Pass -Repo owner/name explicitly."
}

function Assert-GitHubAuth {
    gh auth status | Out-Null
}

function Get-WorkflowRuns {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Repository
    )

    $page = 1
    $runs = New-Object System.Collections.Generic.List[object]

    while ($true) {
        $response = gh api "repos/$Repository/actions/runs?per_page=100&page=$page" | ConvertFrom-Json
        $batch = @($response.workflow_runs)
        if ($batch.Count -eq 0) {
            break
        }

        foreach ($run in $batch) {
            $runs.Add($run)
        }

        if ($batch.Count -lt 100) {
            break
        }

        $page++
    }

    return $runs
}

Assert-GitHubCliAvailable
Assert-GitHubAuth

if ([string]::IsNullOrWhiteSpace($Repo)) {
    $Repo = Resolve-RepositoryFromOrigin
}

$cutoffUtc = (Get-Date).ToUniversalTime().AddDays(-$OlderThanDays)
$allRuns = @(Get-WorkflowRuns -Repository $Repo)

$matchingRuns = @(
    $allRuns |
        Where-Object {
            $_.status -eq 'completed' -and
            ([datetime]$_.created_at) -lt $cutoffUtc -and
            ([string]::IsNullOrWhiteSpace($WorkflowName) -or $_.name -eq $WorkflowName)
        } |
        Sort-Object { [datetime]$_.created_at } -Descending
)

if ($KeepLatest -gt 0 -and $matchingRuns.Count -gt 0) {
    $matchingRuns = @($matchingRuns | Select-Object -Skip $KeepLatest)
}

$runsToDelete = @($matchingRuns | Sort-Object { [datetime]$_.created_at })

Write-Host "Repository : $Repo"
Write-Host "Cutoff UTC : $($cutoffUtc.ToString('o'))"
if (-not [string]::IsNullOrWhiteSpace($WorkflowName)) {
    Write-Host "Workflow   : $WorkflowName"
}
Write-Host "Fetched    : $($allRuns.Count) total runs"
Write-Host "Candidates : $($runsToDelete.Count) completed run(s) older than $OlderThanDays day(s)"

$deletedCount = 0
$failed = New-Object System.Collections.Generic.List[object]

foreach ($run in $runsToDelete) {
    $label = "{0} ({1:u}, {2})" -f $run.id, ([datetime]$run.created_at), $run.name
    if (-not $PSCmdlet.ShouldProcess($Repo, "Delete workflow run $label")) {
        continue
    }

    try {
        gh api -X DELETE "repos/$Repo/actions/runs/$($run.id)" | Out-Null
        $deletedCount++
        Write-Host "Deleted run $($run.id) [$($run.name)] created $($run.created_at)"
    }
    catch {
        $failed.Add([pscustomobject]@{
            Id        = $run.id
            Name      = $run.name
            CreatedAt = [datetime]$run.created_at
            Error     = $_.Exception.Message
        })
        Write-Warning "Failed to delete run $($run.id) [$($run.name)]: $($_.Exception.Message)"
    }
}

$summary = [pscustomobject]@{
    Repository          = $Repo
    CutoffUtc           = $cutoffUtc
    TotalRunsFetched    = $allRuns.Count
    CandidateRuns       = $runsToDelete.Count
    DeletedRuns         = $deletedCount
    FailedRuns          = $failed.Count
    WorkflowName        = if ([string]::IsNullOrWhiteSpace($WorkflowName)) { $null } else { $WorkflowName }
    KeepLatest          = $KeepLatest
    OlderThanDays       = $OlderThanDays
}

Write-Host "Deleted    : $deletedCount"
Write-Host "Failures   : $($failed.Count)"

if ($failed.Count -gt 0) {
    $failed | Format-Table -AutoSize | Out-String | Write-Host
}

if ($PassThru) {
    $summary
}
