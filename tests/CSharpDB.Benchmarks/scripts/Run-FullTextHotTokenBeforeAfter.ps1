<#
.SYNOPSIS
Runs a focused full-text hot-token before/after benchmark against two checkouts.

.DESCRIPTION
The script creates a clean baseline worktree from -BaselineRef and generates a
small external runner for each checkout. The same runner code is compiled
against each checkout's CSharpDB.Engine project, so the baseline does not need
to contain the latest benchmark entry points.

Rows are checkpointed to CSV after each run. Baseline runs that exceed
-TimeoutSeconds are recorded as timeout rows, which is expected for large
pre-chunked hot-token corpora.

.EXAMPLE
powershell -NoProfile -ExecutionPolicy Bypass -File .\tests\CSharpDB.Benchmarks\scripts\Run-FullTextHotTokenBeforeAfter.ps1 -PostingCounts 20000 -BaselinePostingCounts 20000

.EXAMPLE
powershell -NoProfile -ExecutionPolicy Bypass -File .\tests\CSharpDB.Benchmarks\scripts\Run-FullTextHotTokenBeforeAfter.ps1 -PostingCounts 100000,500000 -BaselinePostingCounts 100000,500000 -TimeoutSeconds 180
#>
[CmdletBinding()]
param(
    [string]$RepoRoot = "",
    [string]$BaselineRef = "HEAD",
    [string]$AfterRepoRoot = "",
    [string[]]$PostingCounts = @("20000", "100000", "500000"),
    [string[]]$BaselinePostingCounts = @(),
    [int]$Iterations = 1,
    [int]$BatchSize = 500,
    [int]$TimeoutSeconds = 1800,
    [string]$Configuration = "Release",
    [string]$OutputDir = "",
    [string]$DotNet = "dotnet",
    [switch]$SkipBaseline,
    [switch]$KeepBaselineWorktree
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-DefaultRepoRoot
{
    return (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path
}

function ConvertTo-PostingCountArray
{
    param([Parameter(Mandatory = $false)][string[]]$Values = @())

    $counts = New-Object System.Collections.Generic.List[int]
    foreach ($value in @($Values))
    {
        if ([string]::IsNullOrWhiteSpace($value))
        {
            continue
        }

        foreach ($part in ($value -split ","))
        {
            if ([string]::IsNullOrWhiteSpace($part))
            {
                continue
            }

            $count = [int]::Parse($part.Trim(), [System.Globalization.CultureInfo]::InvariantCulture)
            if ($count -lt 1)
            {
                throw "Posting counts must be positive. Saw '$count'."
            }

            $counts.Add($count) | Out-Null
        }
    }

    if ($counts.Count -eq 0)
    {
        throw "At least one posting count is required."
    }

    return ,([int[]]$counts.ToArray())
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

    $started = $process.Start()
    if (-not $started)
    {
        throw "Failed to start process '$FilePath'."
    }

    $stdoutTask = $process.StandardOutput.ReadToEndAsync()
    $stderrTask = $process.StandardError.ReadToEndAsync()
    $limitMs = [Math]::Max(1, $TimeoutSeconds) * 1000
    $completed = $process.WaitForExit($limitMs)

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

function Join-ProcessArguments
{
    param([Parameter(Mandatory = $true)][string[]]$ArgumentList)

    return (($ArgumentList | ForEach-Object { ConvertTo-ProcessArgument $_ }) -join " ")
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

function New-RunnerProject
{
    param(
        [Parameter(Mandatory = $true)][string]$RunnerDir,
        [Parameter(Mandatory = $true)][string]$TargetRepo
    )

    New-Item -ItemType Directory -Force -Path $RunnerDir | Out-Null

    $engineProject = Join-Path $TargetRepo "src\CSharpDB.Engine\CSharpDB.Engine.csproj"
    if (-not (Test-Path $engineProject))
    {
        throw "CSharpDB.Engine project was not found under '$TargetRepo'."
    }

    $escapedProject = [System.Security.SecurityElement]::Escape($engineProject)
    $projectXml = @"
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net10.0</TargetFramework>
    <ImplicitUsings>enable</ImplicitUsings>
    <Nullable>enable</Nullable>
    <IsPackable>false</IsPackable>
  </PropertyGroup>
  <ItemGroup>
    <ProjectReference Include="$escapedProject" />
  </ItemGroup>
</Project>
"@

    Set-Content -Path (Join-Path $RunnerDir "FtsHotTokenRunner.csproj") -Value $projectXml -Encoding UTF8

    $program = @'
using System.Diagnostics;
using System.Text.Json;
using CSharpDB.Engine;

const string TableName = "bench_hot_fts";
const string IndexName = "fts_bench_hot_fts";
const string BodyColumn = "body";
const string HotQuery = "line";

if (args.Length != 6)
{
    Console.Error.WriteLine("Usage: <label> <targetRepo> <postingCount> <batchSize> <workDir> <resultJson>");
    return 2;
}

string label = args[0];
string targetRepo = args[1];
int postingCount = int.Parse(args[2]);
int batchSize = int.Parse(args[3]);
string workDir = args[4];
string resultJson = args[5];

Directory.CreateDirectory(workDir);
string dbPath = Path.Combine(workDir, $"{label}_{postingCount}.db");
DeleteIfExists(dbPath);
DeleteIfExists(dbPath + ".wal");

var process = Process.GetCurrentProcess();
var result = new Dictionary<string, object?>
{
    ["label"] = label,
    ["target_repo"] = targetRepo,
    ["posting_count"] = postingCount,
    ["batch_size"] = batchSize,
    ["database_path"] = dbPath,
    ["started_utc"] = DateTimeOffset.UtcNow.ToString("O"),
};

try
{
    await using var db = await Database.OpenAsync(dbPath);
    await db.ExecuteAsync($"CREATE TABLE {TableName} (id INTEGER PRIMARY KEY, body TEXT)");
    await db.EnsureFullTextIndexAsync(IndexName, TableName, new[] { BodyColumn });

    var build = Stopwatch.StartNew();
    for (int offset = 0; offset < postingCount; offset += batchSize)
    {
        await db.BeginTransactionAsync();
        try
        {
            int end = Math.Min(postingCount, offset + batchSize);
            for (int id = offset; id < end; id++)
            {
                await db.ExecuteAsync($"INSERT INTO {TableName} VALUES ({id}, 'line hot token payload_{id:D8}')");
            }

            await db.CommitAsync();
        }
        catch
        {
            await db.RollbackAsync();
            throw;
        }
    }
    build.Stop();

    long bytesAfterBuild = GetDatabaseBytes(dbPath);

    ForceFullCollection();
    var query = Stopwatch.StartNew();
    var hotHits = await db.SearchAsync(IndexName, HotQuery);
    query.Stop();
    if (hotHits.Count != postingCount)
        throw new InvalidOperationException($"Expected {postingCount} hot-token hits, found {hotHits.Count}.");

    int updateId = postingCount / 2;
    ForceFullCollection();
    var update = Stopwatch.StartNew();
    var updateResult = await db.ExecuteAsync($"UPDATE {TableName} SET body = 'cool unique_{postingCount}' WHERE id = {updateId}");
    update.Stop();
    if (updateResult.RowsAffected != 1)
        throw new InvalidOperationException($"Expected update to affect 1 row, affected {updateResult.RowsAffected}.");

    var updatedHits = await db.SearchAsync(IndexName, $"unique_{postingCount}");
    if (updatedHits.Count != 1 || updatedHits[0].RowId != updateId)
        throw new InvalidOperationException("Updated token was not reflected in the full-text index.");

    int deleteId = updateId + 1 < postingCount ? updateId + 1 : updateId - 1;
    ForceFullCollection();
    var delete = Stopwatch.StartNew();
    var deleteResult = await db.ExecuteAsync($"DELETE FROM {TableName} WHERE id = {deleteId}");
    delete.Stop();
    if (deleteResult.RowsAffected != 1)
        throw new InvalidOperationException($"Expected delete to affect 1 row, affected {deleteResult.RowsAffected}.");

    var deletedHits = await db.SearchAsync(IndexName, $"payload_{deleteId:D8}");
    if (deletedHits.Count != 0)
        throw new InvalidOperationException("Deleted token was still visible in the full-text index.");

    result["status"] = "completed";
    result["build_insert_index_ms"] = build.Elapsed.TotalMilliseconds;
    result["query_hot_token_ms"] = query.Elapsed.TotalMilliseconds;
    result["update_single_row_ms"] = update.Elapsed.TotalMilliseconds;
    result["delete_single_row_ms"] = delete.Elapsed.TotalMilliseconds;
    result["db_bytes_after_build"] = bytesAfterBuild;
    result["db_bytes_final"] = GetDatabaseBytes(dbPath);
    result["peak_working_set_bytes"] = process.PeakWorkingSet64;
    result["finished_utc"] = DateTimeOffset.UtcNow.ToString("O");
}
catch (Exception ex)
{
    result["status"] = "failed";
    result["error"] = ex.ToString();
    result["finished_utc"] = DateTimeOffset.UtcNow.ToString("O");
    await WriteResultAsync(resultJson, result);
    Console.Error.WriteLine(ex);
    return 1;
}

await WriteResultAsync(resultJson, result);
return 0;

static async Task WriteResultAsync(string path, Dictionary<string, object?> result)
{
    Directory.CreateDirectory(Path.GetDirectoryName(path)!);
    var options = new JsonSerializerOptions { WriteIndented = true };
    await File.WriteAllTextAsync(path, JsonSerializer.Serialize(result, options));
}

static long GetDatabaseBytes(string dbPath)
{
    long total = 0;
    if (File.Exists(dbPath))
        total += new FileInfo(dbPath).Length;
    if (File.Exists(dbPath + ".wal"))
        total += new FileInfo(dbPath + ".wal").Length;
    return total;
}

static void DeleteIfExists(string path)
{
    if (File.Exists(path))
        File.Delete(path);
}

static void ForceFullCollection()
{
    GC.Collect();
    GC.WaitForPendingFinalizers();
    GC.Collect();
}
'@

    Set-Content -Path (Join-Path $RunnerDir "Program.cs") -Value $program -Encoding UTF8
}

function Build-Runner
{
    param(
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][string]$TargetRepo,
        [Parameter(Mandatory = $true)][string]$OutputDir
    )

    $runnerDir = Join-Path $OutputDir "runner-$Label"
    New-RunnerProject -RunnerDir $runnerDir -TargetRepo $TargetRepo
    $project = Join-Path $runnerDir "FtsHotTokenRunner.csproj"
    Invoke-Checked -FilePath $DotNet -ArgumentList @("build", $project, "-c", $Configuration, "--nologo") -WorkingDirectory $runnerDir -TimeoutSeconds 1800 | Out-Null
    return Join-Path $runnerDir "bin\$Configuration\net10.0\FtsHotTokenRunner.dll"
}

function Convert-JsonResultToRow
{
    param(
        [Parameter(Mandatory = $true)]$Json,
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][string]$GitHead,
        [Parameter(Mandatory = $true)][bool]$GitDirty,
        [Parameter(Mandatory = $true)][int]$Iteration,
        [Parameter(Mandatory = $true)][double]$WallClockSeconds,
        [string]$ErrorText = ""
    )

    return [pscustomobject]@{
        label = $Label
        git_head = $GitHead
        git_dirty = $GitDirty
        posting_count = [int]$Json.posting_count
        iteration = $Iteration
        status = [string]$Json.status
        build_insert_index_ms = [double]$Json.build_insert_index_ms
        query_hot_token_ms = [double]$Json.query_hot_token_ms
        update_single_row_ms = [double]$Json.update_single_row_ms
        delete_single_row_ms = [double]$Json.delete_single_row_ms
        db_bytes_after_build = [long]$Json.db_bytes_after_build
        db_bytes_final = [long]$Json.db_bytes_final
        peak_working_set_bytes = [long]$Json.peak_working_set_bytes
        wall_clock_seconds = $WallClockSeconds
        error = $ErrorText
    }
}

function New-FailedRow
{
    param(
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][string]$GitHead,
        [Parameter(Mandatory = $true)][bool]$GitDirty,
        [Parameter(Mandatory = $true)][int]$PostingCount,
        [Parameter(Mandatory = $true)][int]$Iteration,
        [Parameter(Mandatory = $true)][string]$Status,
        [Parameter(Mandatory = $true)][double]$WallClockSeconds,
        [string]$ErrorText = ""
    )

    return [pscustomobject]@{
        label = $Label
        git_head = $GitHead
        git_dirty = $GitDirty
        posting_count = $PostingCount
        iteration = $Iteration
        status = $Status
        build_insert_index_ms = $null
        query_hot_token_ms = $null
        update_single_row_ms = $null
        delete_single_row_ms = $null
        db_bytes_after_build = $null
        db_bytes_final = $null
        peak_working_set_bytes = $null
        wall_clock_seconds = $WallClockSeconds
        error = $ErrorText
    }
}

function Invoke-HotTokenRun
{
    param(
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][string]$TargetRepo,
        [Parameter(Mandatory = $true)][string]$RunnerDll,
        [Parameter(Mandatory = $true)][string]$GitHead,
        [Parameter(Mandatory = $true)][bool]$GitDirty,
        [Parameter(Mandatory = $true)][int]$PostingCount,
        [Parameter(Mandatory = $true)][int]$Iteration,
        [Parameter(Mandatory = $true)][string]$OutputDir
    )

    $runDir = Join-Path $OutputDir ("runs\{0}\{1}\iter-{2}" -f $Label, $PostingCount, $Iteration)
    New-Item -ItemType Directory -Force -Path $runDir | Out-Null
    $jsonPath = Join-Path $runDir "result.json"

    Write-Host ("Running {0} postings={1} iteration={2} timeout={3}s" -f $Label, $PostingCount, $Iteration, $TimeoutSeconds)
    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    $result = Invoke-External `
        -FilePath $DotNet `
        -ArgumentList @($RunnerDll, $Label, $TargetRepo, [string]$PostingCount, [string]$BatchSize, $runDir, $jsonPath) `
        -WorkingDirectory $runDir `
        -TimeoutSeconds $TimeoutSeconds
    $stopwatch.Stop()

    Set-Content -Path (Join-Path $runDir "stdout.log") -Value $result.StandardOutput -Encoding UTF8
    Set-Content -Path (Join-Path $runDir "stderr.log") -Value $result.StandardError -Encoding UTF8

    if ($result.TimedOut)
    {
        return New-FailedRow -Label $Label -GitHead $GitHead -GitDirty $GitDirty -PostingCount $PostingCount -Iteration $Iteration -Status "timeout" -WallClockSeconds $stopwatch.Elapsed.TotalSeconds -ErrorText "Timed out after $TimeoutSeconds seconds."
    }

    if ($result.ExitCode -ne 0)
    {
        $errorText = ($result.StandardError + "`n" + $result.StandardOutput).Trim()
        return New-FailedRow -Label $Label -GitHead $GitHead -GitDirty $GitDirty -PostingCount $PostingCount -Iteration $Iteration -Status "failed" -WallClockSeconds $stopwatch.Elapsed.TotalSeconds -ErrorText $errorText
    }

    if (-not (Test-Path $jsonPath))
    {
        return New-FailedRow -Label $Label -GitHead $GitHead -GitDirty $GitDirty -PostingCount $PostingCount -Iteration $Iteration -Status "failed" -WallClockSeconds $stopwatch.Elapsed.TotalSeconds -ErrorText "Runner did not write result JSON."
    }

    $json = Get-Content -Raw -Path $jsonPath | ConvertFrom-Json
    return Convert-JsonResultToRow -Json $json -Label $Label -GitHead $GitHead -GitDirty $GitDirty -Iteration $Iteration -WallClockSeconds $stopwatch.Elapsed.TotalSeconds
}

function Get-Median
{
    param([Parameter(Mandatory = $false)][AllowEmptyCollection()][object[]]$Values = @())

    $numbers = @($Values | Where-Object { $null -ne $_ -and $_ -ne "" } | ForEach-Object { [double]$_ } | Sort-Object)
    if ($numbers.Count -eq 0)
    {
        return $null
    }

    $middle = [int][Math]::Floor($numbers.Count / 2)
    if (($numbers.Count % 2) -eq 1)
    {
        return $numbers[$middle]
    }

    return ($numbers[$middle - 1] + $numbers[$middle]) / 2.0
}

function Write-Summary
{
    param(
        [Parameter(Mandatory = $true)][object[]]$Rows,
        [Parameter(Mandatory = $true)][string]$SummaryPath
    )

    $summary = New-Object System.Collections.Generic.List[object]
    $counts = @($Rows | Select-Object -ExpandProperty posting_count -Unique | Sort-Object)
    foreach ($count in $counts)
    {
        $baselineRows = @($Rows | Where-Object { $_.label -eq "baseline" -and $_.posting_count -eq $count -and $_.status -eq "completed" })
        $afterRows = @($Rows | Where-Object { $_.label -eq "after" -and $_.posting_count -eq $count -and $_.status -eq "completed" })
        $baselineStatuses = (($Rows | Where-Object { $_.label -eq "baseline" -and $_.posting_count -eq $count } | Select-Object -ExpandProperty status -Unique) -join ";")
        $afterStatuses = (($Rows | Where-Object { $_.label -eq "after" -and $_.posting_count -eq $count } | Select-Object -ExpandProperty status -Unique) -join ";")

        $baselineBuild = Get-Median @($baselineRows | Select-Object -ExpandProperty build_insert_index_ms)
        $afterBuild = Get-Median @($afterRows | Select-Object -ExpandProperty build_insert_index_ms)
        $baselineQuery = Get-Median @($baselineRows | Select-Object -ExpandProperty query_hot_token_ms)
        $afterQuery = Get-Median @($afterRows | Select-Object -ExpandProperty query_hot_token_ms)
        $baselineUpdate = Get-Median @($baselineRows | Select-Object -ExpandProperty update_single_row_ms)
        $afterUpdate = Get-Median @($afterRows | Select-Object -ExpandProperty update_single_row_ms)
        $baselineDelete = Get-Median @($baselineRows | Select-Object -ExpandProperty delete_single_row_ms)
        $afterDelete = Get-Median @($afterRows | Select-Object -ExpandProperty delete_single_row_ms)

        $summary.Add([pscustomobject]@{
            posting_count = $count
            baseline_status = $baselineStatuses
            after_status = $afterStatuses
            baseline_build_insert_index_ms = $baselineBuild
            after_build_insert_index_ms = $afterBuild
            build_speedup = if ($baselineBuild -and $afterBuild) { $baselineBuild / $afterBuild } else { $null }
            baseline_query_hot_token_ms = $baselineQuery
            after_query_hot_token_ms = $afterQuery
            query_speedup = if ($baselineQuery -and $afterQuery) { $baselineQuery / $afterQuery } else { $null }
            baseline_update_single_row_ms = $baselineUpdate
            after_update_single_row_ms = $afterUpdate
            update_speedup = if ($baselineUpdate -and $afterUpdate) { $baselineUpdate / $afterUpdate } else { $null }
            baseline_delete_single_row_ms = $baselineDelete
            after_delete_single_row_ms = $afterDelete
            delete_speedup = if ($baselineDelete -and $afterDelete) { $baselineDelete / $afterDelete } else { $null }
        }) | Out-Null
    }

    $summary | Export-Csv -NoTypeInformation -Path $SummaryPath
}

if ([string]::IsNullOrWhiteSpace($RepoRoot))
{
    $RepoRoot = Resolve-DefaultRepoRoot
}
$RepoRoot = (Resolve-Path $RepoRoot).Path

if ([string]::IsNullOrWhiteSpace($AfterRepoRoot))
{
    $AfterRepoRoot = $RepoRoot
}
$AfterRepoRoot = (Resolve-Path $AfterRepoRoot).Path

$resolvedPostingCounts = ConvertTo-PostingCountArray -Values $PostingCounts
$resolvedBaselinePostingCounts = if ($BaselinePostingCounts.Count -eq 0)
{
    $resolvedPostingCounts
}
else
{
    ConvertTo-PostingCountArray -Values $BaselinePostingCounts
}
$PostingCounts = $resolvedPostingCounts
$BaselinePostingCounts = $resolvedBaselinePostingCounts

if ($Iterations -lt 1)
{
    throw "Iterations must be at least 1."
}
if ($BatchSize -lt 1)
{
    throw "BatchSize must be at least 1."
}

if ([string]::IsNullOrWhiteSpace($OutputDir))
{
    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $OutputDir = Join-Path $RepoRoot "artifacts\fts-hot-token-before-after\$stamp"
}
$OutputDir = [System.IO.Path]::GetFullPath($OutputDir)
New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null

$rawCsv = Join-Path $OutputDir "fulltext-hot-token-before-after-raw.csv"
$summaryCsv = Join-Path $OutputDir "fulltext-hot-token-before-after-summary.csv"

Write-Host "Output: $OutputDir"

$rows = New-Object System.Collections.Generic.List[object]
$baselineRoot = $null

try
{
    if (-not $SkipBaseline)
    {
        $baselineRoot = Join-Path $OutputDir "baseline-worktree"
        Invoke-Checked -FilePath "git" -ArgumentList @("-C", $RepoRoot, "worktree", "add", "--detach", $baselineRoot, $BaselineRef) -WorkingDirectory $RepoRoot -TimeoutSeconds 300 | Out-Null

        $baselineHead = Get-GitText -Repository $baselineRoot -Arguments @("rev-parse", "HEAD")
        $baselineDirty = Get-GitDirty -Repository $baselineRoot
        $baselineRunner = Build-Runner -Label "baseline" -TargetRepo $baselineRoot -OutputDir $OutputDir

        foreach ($count in $BaselinePostingCounts)
        {
            for ($iteration = 1; $iteration -le $Iterations; $iteration++)
            {
                $row = Invoke-HotTokenRun -Label "baseline" -TargetRepo $baselineRoot -RunnerDll $baselineRunner -GitHead $baselineHead -GitDirty $baselineDirty -PostingCount $count -Iteration $iteration -OutputDir $OutputDir
                $rows.Add($row) | Out-Null
                $rows | Export-Csv -NoTypeInformation -Path $rawCsv
                Write-Summary -Rows $rows.ToArray() -SummaryPath $summaryCsv
            }
        }
    }

    $afterHead = Get-GitText -Repository $AfterRepoRoot -Arguments @("rev-parse", "HEAD")
    $afterDirty = Get-GitDirty -Repository $AfterRepoRoot
    $afterRunner = Build-Runner -Label "after" -TargetRepo $AfterRepoRoot -OutputDir $OutputDir

    foreach ($count in $PostingCounts)
    {
        for ($iteration = 1; $iteration -le $Iterations; $iteration++)
        {
            $row = Invoke-HotTokenRun -Label "after" -TargetRepo $AfterRepoRoot -RunnerDll $afterRunner -GitHead $afterHead -GitDirty $afterDirty -PostingCount $count -Iteration $iteration -OutputDir $OutputDir
            $rows.Add($row) | Out-Null
            $rows | Export-Csv -NoTypeInformation -Path $rawCsv
            Write-Summary -Rows $rows.ToArray() -SummaryPath $summaryCsv
        }
    }
}
finally
{
    if ($baselineRoot -and -not $KeepBaselineWorktree)
    {
        try
        {
            Invoke-Checked -FilePath "git" -ArgumentList @("-C", $RepoRoot, "worktree", "remove", "--force", $baselineRoot) -WorkingDirectory $RepoRoot -TimeoutSeconds 300 | Out-Null
        }
        catch
        {
            Write-Warning "Could not remove baseline worktree '$baselineRoot': $_"
        }
    }
}

$rows | Export-Csv -NoTypeInformation -Path $rawCsv
Write-Summary -Rows $rows.ToArray() -SummaryPath $summaryCsv

Write-Host "Raw results: $rawCsv"
Write-Host "Summary: $summaryCsv"
