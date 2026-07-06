<#
.SYNOPSIS
Runs FileSearch benchmarks against baseline and patched CSharpDB NuGet packages.

.DESCRIPTION
The script builds two local CSharpDB package feeds, creates temporary FileSearch
worktrees, rewrites CSharpDB PackageReference versions inside those worktrees,
and runs FileSearch's existing benchmark report command for each package set.

It does not modify the real FileSearch repository. Generated feeds, worktrees,
logs, raw metrics, and the summary report are written under -OutputDir.

.EXAMPLE
powershell -NoProfile -ExecutionPolicy Bypass -File .\tests\CSharpDB.Benchmarks\scripts\Run-FileSearchNuGetBeforeAfter.ps1 -Profile smoke

.EXAMPLE
powershell -NoProfile -ExecutionPolicy Bypass -File .\tests\CSharpDB.Benchmarks\scripts\Run-FileSearchNuGetBeforeAfter.ps1 -Profile standard -TimeoutSeconds 7200
#>
[CmdletBinding()]
param(
    [string]$CSharpDbRepoRoot = "",
    [string]$FileSearchRepoRoot = "",
    [string]$BaselineRef = "HEAD",
    [ValidateSet("smoke", "standard", "full")]
    [string]$Profile = "smoke",
    [string]$OutputDir = "",
    [string]$Configuration = "Release",
    [string]$DotNet = "dotnet",
    [int]$TimeoutSeconds = 3600,
    [bool]$ForceCorpus = $true,
    [bool]$ForceIndex = $true,
    [switch]$SkipBaseline,
    [switch]$KeepWorktrees
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-DefaultCSharpDbRepoRoot
{
    return (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path
}

function Resolve-DefaultFileSearchRepoRoot
{
    $candidate = Join-Path (Split-Path -Parent (Resolve-DefaultCSharpDbRepoRoot)) "FileSearcher"
    if (Test-Path $candidate)
    {
        return (Resolve-Path $candidate).Path
    }

    return ""
}

function ConvertTo-ProcessArgument
{
    param([Parameter(Mandatory = $false)][AllowEmptyString()][string]$Argument)

    if ($null -eq $Argument)
    {
        return '""'
    }

    if ($Argument.Length -gt 0 -and $Argument -notmatch '[\s"]')
    {
        return $Argument
    }

    $builder = [System.Text.StringBuilder]::new()
    [void]$builder.Append('"')
    $backslashes = 0
    foreach ($ch in $Argument.ToCharArray())
    {
        if ($ch -eq '\')
        {
            $backslashes++
            continue
        }

        if ($ch -eq '"')
        {
            [void]$builder.Append('\', $backslashes * 2 + 1)
            [void]$builder.Append('"')
            $backslashes = 0
            continue
        }

        if ($backslashes -gt 0)
        {
            [void]$builder.Append('\', $backslashes)
            $backslashes = 0
        }

        [void]$builder.Append($ch)
    }

    if ($backslashes -gt 0)
    {
        [void]$builder.Append('\', $backslashes * 2)
    }

    [void]$builder.Append('"')
    return $builder.ToString()
}

function Join-ProcessArguments
{
    param([Parameter(Mandatory = $true)][string[]]$ArgumentList)

    return (($ArgumentList | ForEach-Object { ConvertTo-ProcessArgument $_ }) -join " ")
}

function Invoke-External
{
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string[]]$ArgumentList,
        [Parameter(Mandatory = $true)][string]$WorkingDirectory,
        [Parameter(Mandatory = $true)][int]$TimeoutSeconds
    )

    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $FilePath
    $startInfo.Arguments = Join-ProcessArguments -ArgumentList $ArgumentList
    $startInfo.WorkingDirectory = $WorkingDirectory
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $startInfo.CreateNoWindow = $true

    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    if (-not $process.Start())
    {
        throw "Failed to start process '$FilePath'."
    }

    $stdoutTask = $process.StandardOutput.ReadToEndAsync()
    $stderrTask = $process.StandardError.ReadToEndAsync()
    $completed = $process.WaitForExit([Math]::Max(1, $TimeoutSeconds) * 1000)
    if (-not $completed)
    {
        try
        {
            $process.Kill()
        }
        catch
        {
        }

        try
        {
            [void]$process.WaitForExit(30000)
        }
        catch
        {
        }

        return [pscustomobject]@{
            ExitCode = $null
            TimedOut = $true
            StandardOutput = $stdoutTask.GetAwaiter().GetResult()
            StandardError = $stderrTask.GetAwaiter().GetResult()
        }
    }

    return [pscustomobject]@{
        ExitCode = $process.ExitCode
        TimedOut = $false
        StandardOutput = $stdoutTask.GetAwaiter().GetResult()
        StandardError = $stderrTask.GetAwaiter().GetResult()
    }
}

function Invoke-Checked
{
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string[]]$ArgumentList,
        [Parameter(Mandatory = $true)][string]$WorkingDirectory,
        [int]$TimeoutSeconds = 1800
    )

    $result = Invoke-External -FilePath $FilePath -ArgumentList $ArgumentList -WorkingDirectory $WorkingDirectory -TimeoutSeconds $TimeoutSeconds
    if ($result.TimedOut)
    {
        throw "Command timed out: $FilePath $($ArgumentList -join ' ')"
    }

    if ($result.ExitCode -ne 0)
    {
        throw "Command failed ($($result.ExitCode)): $FilePath $($ArgumentList -join ' ')`n$result.StandardOutput`n$result.StandardError"
    }

    return $result
}

function Get-GitText
{
    param(
        [Parameter(Mandatory = $true)][string]$Repository,
        [Parameter(Mandatory = $true)][string[]]$Arguments
    )

    $result = Invoke-Checked -FilePath "git" -ArgumentList (@("-C", $Repository) + $Arguments) -WorkingDirectory $Repository -TimeoutSeconds 120
    return $result.StandardOutput.Trim()
}

function Get-GitDirty
{
    param([Parameter(Mandatory = $true)][string]$Repository)

    $status = Get-GitText -Repository $Repository -Arguments @("status", "--porcelain")
    return -not [string]::IsNullOrWhiteSpace($status)
}

function New-DetachedWorktree
{
    param(
        [Parameter(Mandatory = $true)][string]$Repository,
        [Parameter(Mandatory = $true)][string]$Destination,
        [Parameter(Mandatory = $true)][string]$Ref
    )

    if (Test-Path $Destination)
    {
        Remove-Item -LiteralPath $Destination -Recurse -Force
    }

    Invoke-Checked `
        -FilePath "git" `
        -ArgumentList @("-C", $Repository, "worktree", "add", "--detach", $Destination, $Ref) `
        -WorkingDirectory $Repository `
        -TimeoutSeconds 300 | Out-Null
}

function Remove-DetachedWorktree
{
    param(
        [Parameter(Mandatory = $true)][string]$Repository,
        [Parameter(Mandatory = $true)][string]$Destination
    )

    if (-not (Test-Path $Destination))
    {
        return
    }

    try
    {
        Invoke-Checked `
            -FilePath "git" `
            -ArgumentList @("-C", $Repository, "worktree", "remove", "--force", $Destination) `
            -WorkingDirectory $Repository `
            -TimeoutSeconds 300 | Out-Null
    }
    catch
    {
        Write-Warning "Could not remove worktree '$Destination': $_"
    }
}

function Get-CSharpDbPackageProjects
{
    param([Parameter(Mandatory = $true)][string]$Repository)

    $relative = @(
        "src\CSharpDB.Primitives\CSharpDB.Primitives.csproj",
        "src\CSharpDB.Storage\CSharpDB.Storage.csproj",
        "src\CSharpDB.Storage.Diagnostics\CSharpDB.Storage.Diagnostics.csproj",
        "src\CSharpDB.Sql\CSharpDB.Sql.csproj",
        "src\CSharpDB.ImportExport\CSharpDB.ImportExport.csproj",
        "src\CSharpDB.Execution\CSharpDB.Execution.csproj",
        "src\CSharpDB.Engine\CSharpDB.Engine.csproj"
    )

    return [string[]]($relative | ForEach-Object { Join-Path $Repository $_ })
}

function New-CSharpDbFeed
{
    param(
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][string]$Repository,
        [Parameter(Mandatory = $true)][string]$FeedDirectory,
        [Parameter(Mandatory = $true)][string]$PackageVersion
    )

    New-Item -ItemType Directory -Force -Path $FeedDirectory | Out-Null
    foreach ($project in Get-CSharpDbPackageProjects -Repository $Repository)
    {
        if (-not (Test-Path $project))
        {
            throw "Package project was not found: $project"
        }

        Write-Host "Packing $Label package $project as $PackageVersion"
        Invoke-Checked `
            -FilePath $DotNet `
            -ArgumentList @(
                "pack",
                $project,
                "-c",
                $Configuration,
                "-o",
                $FeedDirectory,
                "--nologo",
                "-p:PackageVersion=$PackageVersion",
                "-p:Version=$PackageVersion"
            ) `
            -WorkingDirectory $Repository `
            -TimeoutSeconds 1800 | Out-Null
    }
}

function Write-NuGetConfig
{
    param(
        [Parameter(Mandatory = $true)][string]$Repository,
        [Parameter(Mandatory = $true)][string]$FeedDirectory
    )

    $escapedFeed = [System.Security.SecurityElement]::Escape($FeedDirectory)
    $config = @"
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <clear />
    <add key="local-csharpdb" value="$escapedFeed" />
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" />
  </packageSources>
</configuration>
"@
    Set-Content -Path (Join-Path $Repository "NuGet.config") -Value $config -Encoding UTF8
}

function Update-CSharpDbPackageReferences
{
    param(
        [Parameter(Mandatory = $true)][string]$Repository,
        [Parameter(Mandatory = $true)][string]$PackageVersion
    )

    $projects = Get-ChildItem -Path $Repository -Filter *.csproj -Recurse
    foreach ($project in $projects)
    {
        [xml]$xml = Get-Content -Raw -Path $project.FullName
        $changed = $false
        foreach ($reference in @($xml.SelectNodes("//*[local-name()='PackageReference']")))
        {
            $include = [string]$reference.GetAttribute("Include")
            $update = [string]$reference.GetAttribute("Update")
            if (-not ($include.StartsWith("CSharpDB", [StringComparison]::OrdinalIgnoreCase) -or
                      $update.StartsWith("CSharpDB", [StringComparison]::OrdinalIgnoreCase)))
            {
                continue
            }

            if ($reference.HasAttribute("Version"))
            {
                $reference.SetAttribute("Version", $PackageVersion)
            }
            else
            {
                $versionNode = $reference.SelectSingleNode("*[local-name()='Version']")
                if ($null -eq $versionNode)
                {
                    $versionNode = $xml.CreateElement("Version", $reference.NamespaceURI)
                    [void]$reference.AppendChild($versionNode)
                }

                $versionNode.InnerText = $PackageVersion
            }

            $changed = $true
        }

        if ($changed)
        {
            $xml.Save($project.FullName)
        }
    }
}

function Get-MetricValue
{
    param(
        [Parameter(Mandatory = $false)][AllowNull()]$Report,
        [Parameter(Mandatory = $true)][string]$Name
    )

    if ($null -eq $Report)
    {
        return $null
    }

    $metric = @($Report.Metrics | Where-Object { $_.Name -eq $Name } | Select-Object -First 1)
    if ($metric.Count -eq 0)
    {
        return $null
    }

    return [double]$metric[0].Value
}

function New-RunSummaryRow
{
    param(
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][string]$CSharpDbHead,
        [Parameter(Mandatory = $true)][bool]$CSharpDbDirty,
        [Parameter(Mandatory = $true)][string]$FileSearchHead,
        [Parameter(Mandatory = $true)][string]$PackageVersion,
        [Parameter(Mandatory = $true)][string]$Status,
        [Parameter(Mandatory = $true)][double]$WallClockSeconds,
        [Parameter(Mandatory = $false)][AllowNull()]$Report = $null,
        [string]$ErrorText = ""
    )

    return [pscustomobject]@{
        label = $Label
        profile = $Profile
        csharpdb_head = $CSharpDbHead
        csharpdb_dirty = $CSharpDbDirty
        filesearch_head = $FileSearchHead
        package_version = $PackageVersion
        status = $Status
        wall_clock_seconds = $WallClockSeconds
        initial_index_throughput = Get-MetricValue -Report $Report -Name "initial_index_throughput"
        incremental_catch_up_throughput = Get-MetricValue -Report $Report -Name "incremental_catch_up_throughput"
        index_freshness_after_event = Get-MetricValue -Report $Report -Name "index_freshness_after_event"
        indexed_content_query_p50 = Get-MetricValue -Report $Report -Name "indexed_content_query_p50"
        indexed_content_query_p95 = Get-MetricValue -Report $Report -Name "indexed_content_query_p95"
        indexed_query_phase_fts_lookup_avg = Get-MetricValue -Report $Report -Name "indexed_query_phase_fts_lookup_avg"
        metadata_query_p95 = Get-MetricValue -Report $Report -Name "metadata_query_p95"
        time_to_first_result = Get-MetricValue -Report $Report -Name "time_to_first_result"
        index_disk_size = Get-MetricValue -Report $Report -Name "index_disk_size"
        benchmark_process_working_set = Get-MetricValue -Report $Report -Name "benchmark_process_working_set"
        extraction_success_rate = Get-MetricValue -Report $Report -Name "extraction_success_rate"
        error = $ErrorText
    }
}

function Export-MetricRows
{
    param(
        [Parameter(Mandatory = $true)][object[]]$SummaryRows,
        [Parameter(Mandatory = $true)][string]$Path
    )

    $metricNames = @(
        "initial_index_throughput",
        "incremental_catch_up_throughput",
        "index_freshness_after_event",
        "indexed_content_query_p50",
        "indexed_content_query_p95",
        "indexed_query_phase_fts_lookup_avg",
        "metadata_query_p95",
        "time_to_first_result",
        "index_disk_size",
        "benchmark_process_working_set",
        "extraction_success_rate"
    )

    $rows = foreach ($name in $metricNames)
    {
        $baseline = $SummaryRows | Where-Object { $_.label -eq "baseline" } | Select-Object -First 1
        $after = $SummaryRows | Where-Object { $_.label -eq "after" } | Select-Object -First 1
        $baselineValue = if ($baseline) { $baseline.$name } else { $null }
        $afterValue = if ($after) { $after.$name } else { $null }
        [pscustomobject]@{
            metric = $name
            baseline = $baselineValue
            after = $afterValue
            after_div_baseline = if ($baselineValue -and $afterValue) { [double]$afterValue / [double]$baselineValue } else { $null }
            baseline_div_after = if ($baselineValue -and $afterValue) { [double]$baselineValue / [double]$afterValue } else { $null }
        }
    }

    $rows | Export-Csv -NoTypeInformation -Path $Path
}

function Write-MarkdownReport
{
    param(
        [Parameter(Mandatory = $true)][object[]]$SummaryRows,
        [Parameter(Mandatory = $true)][string]$MetricCsv,
        [Parameter(Mandatory = $true)][string]$SummaryCsv,
        [Parameter(Mandatory = $true)][string]$Path
    )

    $metrics = Import-Csv $MetricCsv
    $builder = New-Object System.Collections.Generic.List[string]
    $builder.Add("# FileSearch CSharpDB NuGet Before/After")
    $builder.Add("")
    $builder.Add("Profile: ``$Profile``")
    $builder.Add("")
    $builder.Add("| Label | Status | CSharpDB package | Wall seconds |")
    $builder.Add("| --- | --- | --- | ---: |")
    foreach ($row in $SummaryRows)
    {
        $builder.Add(("{0} | {1} | {2} | {3:N1} |" -f ("| " + $row.label), $row.status, $row.package_version, [double]$row.wall_clock_seconds))
    }

    $builder.Add("")
    $builder.Add("| Metric | Baseline | After | After/Baseline | Baseline/After |")
    $builder.Add("| --- | ---: | ---: | ---: | ---: |")
    foreach ($metric in $metrics)
    {
        $builder.Add(("| `{0}` | {1} | {2} | {3} | {4} |" -f
            $metric.metric,
            (Format-NullableNumber $metric.baseline),
            (Format-NullableNumber $metric.after),
            (Format-NullableNumber $metric.after_div_baseline),
            (Format-NullableNumber $metric.baseline_div_after)))
    }

    $builder.Add("")
    $builder.Add("Summary CSV: ``$SummaryCsv``")
    $builder.Add("Metric comparison CSV: ``$MetricCsv``")
    Set-Content -Path $Path -Value $builder -Encoding UTF8
}

function Format-NullableNumber
{
    param([Parameter(Mandatory = $false)][AllowNull()][string]$Value)

    if ([string]::IsNullOrWhiteSpace($Value))
    {
        return ""
    }

    $number = [double]::Parse($Value, [System.Globalization.CultureInfo]::InvariantCulture)
    if ([Math]::Abs($number) -ge 1000)
    {
        return $number.ToString("N2", [System.Globalization.CultureInfo]::InvariantCulture)
    }

    return $number.ToString("0.###", [System.Globalization.CultureInfo]::InvariantCulture)
}

function Invoke-FileSearchBenchmark
{
    param(
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][string]$Repository,
        [Parameter(Mandatory = $true)][string]$PackageVersion,
        [Parameter(Mandatory = $true)][string]$CSharpDbHead,
        [Parameter(Mandatory = $true)][bool]$CSharpDbDirty,
        [Parameter(Mandatory = $true)][string]$FileSearchHead,
        [Parameter(Mandatory = $true)][string]$RunRoot
    )

    $benchmarkProject = Join-Path $Repository "benchmarks\FileSearch.Benchmarks\FileSearch.Benchmarks.csproj"
    $logDir = Join-Path $RunRoot "logs"
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null

    Invoke-Checked `
        -FilePath $DotNet `
        -ArgumentList @("restore", $benchmarkProject, "--force-evaluate", "--no-cache") `
        -WorkingDirectory $Repository `
        -TimeoutSeconds 1800 | Out-Null

    Invoke-Checked `
        -FilePath $DotNet `
        -ArgumentList @("build", $benchmarkProject, "-c", $Configuration, "--no-restore", "--nologo") `
        -WorkingDirectory $Repository `
        -TimeoutSeconds 1800 | Out-Null

    $benchmarkRoot = Join-Path $RunRoot "benchmark-root"
    $arguments = @(
        "run",
        "-c",
        $Configuration,
        "--no-build",
        "--project",
        $benchmarkProject,
        "--",
        "report",
        "--profile",
        $Profile,
        "--root",
        $benchmarkRoot
    )
    if ($ForceCorpus)
    {
        $arguments += "--force-corpus"
    }
    if ($ForceIndex)
    {
        $arguments += "--force-index"
    }

    Write-Host "Running FileSearch $Label benchmark profile=$Profile"
    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    $result = Invoke-External -FilePath $DotNet -ArgumentList $arguments -WorkingDirectory $Repository -TimeoutSeconds $TimeoutSeconds
    $stopwatch.Stop()

    Set-Content -Path (Join-Path $logDir "stdout.log") -Value $result.StandardOutput -Encoding UTF8
    Set-Content -Path (Join-Path $logDir "stderr.log") -Value $result.StandardError -Encoding UTF8

    if ($result.TimedOut)
    {
        return New-RunSummaryRow `
            -Label $Label `
            -CSharpDbHead $CSharpDbHead `
            -CSharpDbDirty $CSharpDbDirty `
            -FileSearchHead $FileSearchHead `
            -PackageVersion $PackageVersion `
            -Status "timeout" `
            -WallClockSeconds $stopwatch.Elapsed.TotalSeconds `
            -ErrorText "Timed out after $TimeoutSeconds seconds."
    }

    if ($result.ExitCode -ne 0)
    {
        return New-RunSummaryRow `
            -Label $Label `
            -CSharpDbHead $CSharpDbHead `
            -CSharpDbDirty $CSharpDbDirty `
            -FileSearchHead $FileSearchHead `
            -PackageVersion $PackageVersion `
            -Status "failed" `
            -WallClockSeconds $stopwatch.Elapsed.TotalSeconds `
            -ErrorText (($result.StandardError + "`n" + $result.StandardOutput).Trim())
    }

    $reportPath = Join-Path $benchmarkRoot "$Profile\reports\benchmark-report.json"
    if (-not (Test-Path $reportPath))
    {
        return New-RunSummaryRow `
            -Label $Label `
            -CSharpDbHead $CSharpDbHead `
            -CSharpDbDirty $CSharpDbDirty `
            -FileSearchHead $FileSearchHead `
            -PackageVersion $PackageVersion `
            -Status "failed" `
            -WallClockSeconds $stopwatch.Elapsed.TotalSeconds `
            -ErrorText "Benchmark report was not written: $reportPath"
    }

    Copy-Item -LiteralPath $reportPath -Destination (Join-Path $RunRoot "benchmark-report.json") -Force
    $markdownReport = Join-Path $benchmarkRoot "$Profile\reports\benchmark-report.md"
    if (Test-Path $markdownReport)
    {
        Copy-Item -LiteralPath $markdownReport -Destination (Join-Path $RunRoot "benchmark-report.md") -Force
    }

    $report = Get-Content -Raw -Path $reportPath | ConvertFrom-Json
    return New-RunSummaryRow `
        -Label $Label `
        -CSharpDbHead $CSharpDbHead `
        -CSharpDbDirty $CSharpDbDirty `
        -FileSearchHead $FileSearchHead `
        -PackageVersion $PackageVersion `
        -Status "completed" `
        -WallClockSeconds $stopwatch.Elapsed.TotalSeconds `
        -Report $report
}

if ([string]::IsNullOrWhiteSpace($CSharpDbRepoRoot))
{
    $CSharpDbRepoRoot = Resolve-DefaultCSharpDbRepoRoot
}
$CSharpDbRepoRoot = (Resolve-Path $CSharpDbRepoRoot).Path

if ([string]::IsNullOrWhiteSpace($FileSearchRepoRoot))
{
    $FileSearchRepoRoot = Resolve-DefaultFileSearchRepoRoot
}
if ([string]::IsNullOrWhiteSpace($FileSearchRepoRoot) -or -not (Test-Path $FileSearchRepoRoot))
{
    throw "FileSearch repo was not found. Pass -FileSearchRepoRoot."
}
$FileSearchRepoRoot = (Resolve-Path $FileSearchRepoRoot).Path

if (Get-GitDirty -Repository $FileSearchRepoRoot)
{
    throw "FileSearch repo has uncommitted changes. Commit/stash them or run from a clean checkout so package-version patching stays isolated."
}

if ([string]::IsNullOrWhiteSpace($OutputDir))
{
    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $OutputDir = Join-Path $CSharpDbRepoRoot "artifacts\filesearch-nuget-before-after\$stamp"
}
$OutputDir = [System.IO.Path]::GetFullPath($OutputDir)
New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null
Write-Host "Output: $OutputDir"

$stampVersion = (Get-Date -Format "yyyyMMddHHmmss")
$baselineVersion = "4.0.2-ftsbaseline.$stampVersion"
$afterVersion = "4.0.2-ftsafter.$stampVersion"

$baselineCSharpDbWorktree = Join-Path $OutputDir "csharpdb-baseline-worktree"
$baselineFileSearchWorktree = Join-Path $OutputDir "filesearch-baseline-worktree"
$afterFileSearchWorktree = Join-Path $OutputDir "filesearch-after-worktree"
$summaryRows = New-Object System.Collections.Generic.List[object]
$summaryCsv = Join-Path $OutputDir "filesearch-nuget-before-after-summary.csv"
$metricCsv = Join-Path $OutputDir "filesearch-nuget-before-after-metrics.csv"
$markdownPath = Join-Path $OutputDir "filesearch-nuget-before-after.md"

try
{
    $fileSearchHead = Get-GitText -Repository $FileSearchRepoRoot -Arguments @("rev-parse", "HEAD")

    if (-not $SkipBaseline)
    {
        New-DetachedWorktree -Repository $CSharpDbRepoRoot -Destination $baselineCSharpDbWorktree -Ref $BaselineRef
        $baselineCSharpDbHead = Get-GitText -Repository $baselineCSharpDbWorktree -Arguments @("rev-parse", "HEAD")
        $baselineFeed = Join-Path $OutputDir "nuget-baseline"
        New-CSharpDbFeed -Label "baseline" -Repository $baselineCSharpDbWorktree -FeedDirectory $baselineFeed -PackageVersion $baselineVersion

        New-DetachedWorktree -Repository $FileSearchRepoRoot -Destination $baselineFileSearchWorktree -Ref "HEAD"
        Write-NuGetConfig -Repository $baselineFileSearchWorktree -FeedDirectory $baselineFeed
        Update-CSharpDbPackageReferences -Repository $baselineFileSearchWorktree -PackageVersion $baselineVersion

        $baselineRow = Invoke-FileSearchBenchmark `
            -Label "baseline" `
            -Repository $baselineFileSearchWorktree `
            -PackageVersion $baselineVersion `
            -CSharpDbHead $baselineCSharpDbHead `
            -CSharpDbDirty $false `
            -FileSearchHead $fileSearchHead `
            -RunRoot (Join-Path $OutputDir "runs\baseline")
        $summaryRows.Add($baselineRow) | Out-Null
        $summaryRows | Export-Csv -NoTypeInformation -Path $summaryCsv
        Export-MetricRows -SummaryRows $summaryRows.ToArray() -Path $metricCsv
    }

    $afterCSharpDbHead = Get-GitText -Repository $CSharpDbRepoRoot -Arguments @("rev-parse", "HEAD")
    $afterCSharpDbDirty = Get-GitDirty -Repository $CSharpDbRepoRoot
    $afterFeed = Join-Path $OutputDir "nuget-after"
    New-CSharpDbFeed -Label "after" -Repository $CSharpDbRepoRoot -FeedDirectory $afterFeed -PackageVersion $afterVersion

    New-DetachedWorktree -Repository $FileSearchRepoRoot -Destination $afterFileSearchWorktree -Ref "HEAD"
    Write-NuGetConfig -Repository $afterFileSearchWorktree -FeedDirectory $afterFeed
    Update-CSharpDbPackageReferences -Repository $afterFileSearchWorktree -PackageVersion $afterVersion

    $afterRow = Invoke-FileSearchBenchmark `
        -Label "after" `
        -Repository $afterFileSearchWorktree `
        -PackageVersion $afterVersion `
        -CSharpDbHead $afterCSharpDbHead `
        -CSharpDbDirty $afterCSharpDbDirty `
        -FileSearchHead $fileSearchHead `
        -RunRoot (Join-Path $OutputDir "runs\after")
    $summaryRows.Add($afterRow) | Out-Null
    $summaryRows | Export-Csv -NoTypeInformation -Path $summaryCsv
    Export-MetricRows -SummaryRows $summaryRows.ToArray() -Path $metricCsv
    Write-MarkdownReport -SummaryRows $summaryRows.ToArray() -MetricCsv $metricCsv -SummaryCsv $summaryCsv -Path $markdownPath
}
finally
{
    if (-not $KeepWorktrees)
    {
        Remove-DetachedWorktree -Repository $CSharpDbRepoRoot -Destination $baselineCSharpDbWorktree
        Remove-DetachedWorktree -Repository $FileSearchRepoRoot -Destination $baselineFileSearchWorktree
        Remove-DetachedWorktree -Repository $FileSearchRepoRoot -Destination $afterFileSearchWorktree
    }
}

$summaryRows | Export-Csv -NoTypeInformation -Path $summaryCsv
if ($summaryRows.Count -gt 0)
{
    Export-MetricRows -SummaryRows $summaryRows.ToArray() -Path $metricCsv
    Write-MarkdownReport -SummaryRows $summaryRows.ToArray() -MetricCsv $metricCsv -SummaryCsv $summaryCsv -Path $markdownPath
}

Write-Host "Summary: $summaryCsv"
Write-Host "Metrics: $metricCsv"
Write-Host "Report: $markdownPath"
