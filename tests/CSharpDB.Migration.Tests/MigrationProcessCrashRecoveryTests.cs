using System.Diagnostics;
using System.IO.Pipes;
using CSharpDB.Engine;
using CSharpDB.Migration.CSharpDb;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Tests;

[Collection("MigrationCrashHarnessProcess")]
public sealed class MigrationProcessCrashRecoveryTests
{
#if DEBUG
    private const string BuildConfiguration = "Debug";
#else
    private const string BuildConfiguration = "Release";
#endif

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(CSharpDbMigrationFaultPoint.BeforeRows, false)]
    [InlineData(CSharpDbMigrationFaultPoint.AfterRowsBeforeReceipt, false)]
    [InlineData(CSharpDbMigrationFaultPoint.AfterReceiptBeforeCommit, false)]
    [InlineData(CSharpDbMigrationFaultPoint.AfterCommit, true)]
    public async Task ChildProcessCrash_ResumeHasNoMissingOrDuplicateRowsAndNeverActivates(
        CSharpDbMigrationFaultPoint faultPoint,
        bool firstBatchCommitted)
    {
        using var files = new TemporaryTargetDirectory();
        MigrationCatalog catalog = await InspectAsync(Ct);
        MigrationPlan plan = ReadyPlan(catalog, batchSize: 2);
        string targetIdentity;

        await using (CSharpDbStagedMigrationTarget target =
                     await CSharpDbStagedMigrationTarget.CreateNewAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                         cancellationToken: Ct))
        {
            targetIdentity = target.TargetIdentity;
        }

        CrashResult crash = await CrashAtAsync(files.TargetPath, faultPoint, Ct);
        Assert.NotEqual(0, crash.ExitCode);
        Assert.Equal(faultPoint, crash.Point);
        Assert.Equal("syn:table:customers-lower", crash.SourceObjectId);
        Assert.Equal(0, crash.BatchOrdinal);
        Assert.True(File.Exists(files.LeasePath));

        await using (var source = new SyntheticMigrationDataSource(catalog))
        await using (CSharpDbStagedMigrationTarget resumed =
                     await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         source.SnapshotIdentity,
                         cancellationToken: Ct))
        {
            MigrationApplyResult result = await ApplyAsync(plan, catalog, source, resumed, Ct);

            Assert.Equal(MigrationApplyStatus.AwaitingValidation, result.Status);
            Assert.Equal(targetIdentity, resumed.TargetIdentity);
            Assert.Equal(firstBatchCommitted ? 10 : 11, result.BatchesWritten);
            Assert.Equal(firstBatchCommitted ? 1 : 0, result.BatchesSkipped);
            Assert.Equal(firstBatchCommitted ? 19 : 21, result.RowsWritten);
            Assert.Equal(firstBatchCommitted ? 2 : 0, result.RowsSkipped);
            await AssertReceiptSetAsync(resumed, plan, Ct);
            await using IValidationSnapshot snapshot = await resumed.OpenValidationSnapshotAsync(Ct);
            await AssertExactSyntheticRowsAsync(snapshot, catalog, plan, Ct);
        }

        Assert.False(File.Exists(files.LeasePath));

        // A second process-equivalent reopen must skip every receipt and leave
        // all row identities unchanged, proving restart idempotence.
        await using (var source = new SyntheticMigrationDataSource(catalog))
        await using (CSharpDbStagedMigrationTarget verified =
                     await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         source.SnapshotIdentity,
                         cancellationToken: Ct))
        {
            MigrationApplyResult replay = await ApplyAsync(plan, catalog, source, verified, Ct);
            Assert.Equal(targetIdentity, verified.TargetIdentity);
            Assert.Equal(0, replay.BatchesWritten);
            Assert.Equal(11, replay.BatchesSkipped);
            Assert.Equal(0, replay.RowsWritten);
            Assert.Equal(21, replay.RowsSkipped);
            await using IValidationSnapshot snapshot = await verified.OpenValidationSnapshotAsync(Ct);
            await AssertExactSyntheticRowsAsync(snapshot, catalog, plan, Ct);
        }

        Assert.Equal("awaiting-validation", await ReadLifecycleAsync(files.TargetPath, Ct));
        Assert.DoesNotContain(
            typeof(CSharpDbStagedMigrationTarget).GetMethods(),
            method => method.Name.Contains("Activate", StringComparison.OrdinalIgnoreCase) ||
                method.Name.Contains("Replace", StringComparison.OrdinalIgnoreCase));
    }

    private static async Task<CrashResult> CrashAtAsync(
        string targetPath,
        CSharpDbMigrationFaultPoint faultPoint,
        CancellationToken cancellationToken)
    {
        string pipeName = $"csharpdb-migration-crash-{Guid.NewGuid():N}";
        await using var pipe = new NamedPipeServerStream(
            pipeName,
            PipeDirection.InOut,
            maxNumberOfServerInstances: 1,
            PipeTransmissionMode.Byte,
            PipeOptions.Asynchronous);
        using Process process = CreateCrashHarnessProcess(targetPath, pipeName, faultPoint);
        if (!process.Start())
            throw new InvalidOperationException("Failed to start the migration crash harness process.");

        Task<string> stdoutTask = process.StandardOutput.ReadToEndAsync(cancellationToken);
        Task<string> stderrTask = process.StandardError.ReadToEndAsync(cancellationToken);
        bool killed = false;
        try
        {
            await pipe.WaitForConnectionAsync(cancellationToken)
                .WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
            using var reader = new StreamReader(pipe, leaveOpen: true);

            string ready = await ReadProtocolLineAsync(reader, cancellationToken);
            if (!string.Equals(ready, "READY", StringComparison.Ordinal))
                throw ProtocolFailure(ready);

            string reached = await ReadProtocolLineAsync(reader, cancellationToken);
            string[] parts = reached.Split('|');
            if (parts.Length != 4 || !string.Equals(parts[0], "REACHED", StringComparison.Ordinal) ||
                !Enum.TryParse(parts[1], ignoreCase: false, out CSharpDbMigrationFaultPoint reachedPoint) ||
                !long.TryParse(parts[3], out long batchOrdinal))
            {
                throw ProtocolFailure(reached);
            }

            process.Kill(entireProcessTree: true);
            killed = true;
            await process.WaitForExitAsync(cancellationToken)
                .WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);

            return new CrashResult(
                process.ExitCode,
                reachedPoint,
                parts[2],
                batchOrdinal,
                await stdoutTask,
                await stderrTask);
        }
        catch (Exception error)
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
                killed = true;
            }
            await process.WaitForExitAsync(CancellationToken.None)
                .WaitAsync(TimeSpan.FromSeconds(30));
            string stdout = await stdoutTask.ConfigureAwait(false);
            string stderr = await stderrTask.ConfigureAwait(false);
            throw new InvalidOperationException(
                $"Migration crash harness failed at {faultPoint}. " +
                $"ExitCode={process.ExitCode}; STDOUT={stdout}; STDERR={stderr}",
                error);
        }
        finally
        {
            if (!killed && !process.HasExited)
            {
                process.Kill(entireProcessTree: true);
                await process.WaitForExitAsync(CancellationToken.None)
                    .WaitAsync(TimeSpan.FromSeconds(30));
            }
        }
    }

    private static Process CreateCrashHarnessProcess(
        string targetPath,
        string pipeName,
        CSharpDbMigrationFaultPoint faultPoint)
    {
        string assemblyPath = FindCrashHarnessAssembly();
        string dotnetHost = Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") is { Length: > 0 } path
            ? path
            : "dotnet";
        var startInfo = new ProcessStartInfo(dotnetHost)
        {
            WorkingDirectory = Path.GetDirectoryName(assemblyPath)!,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };
        startInfo.ArgumentList.Add(assemblyPath);
        startInfo.ArgumentList.Add("--target");
        startInfo.ArgumentList.Add(targetPath);
        startInfo.ArgumentList.Add("--pipe");
        startInfo.ArgumentList.Add(pipeName);
        startInfo.ArgumentList.Add("--fault");
        startInfo.ArgumentList.Add(faultPoint.ToString());
        return new Process { StartInfo = startInfo };
    }

    private static string FindCrashHarnessAssembly()
    {
        DirectoryInfo? current = new(AppContext.BaseDirectory);
        while (current is not null)
        {
            string candidate = Path.Combine(
                current.FullName,
                "tests",
                "CSharpDB.Migration.CrashHarness",
                "bin",
                BuildConfiguration,
                "net10.0",
                "CSharpDB.Migration.CrashHarness.dll");
            if (File.Exists(candidate))
                return candidate;
            current = current.Parent;
        }

        throw new FileNotFoundException(
            $"Could not locate the {BuildConfiguration} migration crash harness assembly.");
    }

    private static async Task<string> ReadProtocolLineAsync(
        StreamReader reader,
        CancellationToken cancellationToken)
    {
        string? line = await reader.ReadLineAsync(cancellationToken)
            .AsTask()
            .WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
        if (line is null)
            throw new EndOfStreamException("Migration crash harness disconnected before reaching a fault point.");
        return line;
    }

    private static Exception ProtocolFailure(string line)
    {
        if (line.StartsWith("ERROR|", StringComparison.Ordinal))
        {
            string[] parts = line.Split('|');
            string detail = parts.Length == 3
                ? System.Text.Encoding.UTF8.GetString(Convert.FromBase64String(parts[2]))
                : line;
            return new InvalidOperationException($"Migration crash harness reported {parts.ElementAtOrDefault(1)}: {detail}");
        }

        return new InvalidDataException($"Unexpected migration crash harness protocol message '{line}'.");
    }

    private static async ValueTask<MigrationCatalog> InspectAsync(CancellationToken cancellationToken) =>
        await new SyntheticMigrationSourceInspector().InspectAsync(
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion = CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                IncludeProfile = true,
                ProfileSampleSize = 5,
            },
            cancellationToken);

    private static MigrationPlan ReadyPlan(MigrationCatalog catalog, int batchSize)
    {
        MigrationPlan plan = new MigrationPlanner().CreatePlan(
            catalog,
            new MigrationPlanningOptions { AcceptAllExclusions = true });
        return plan with { Load = plan.Load with { BatchSize = batchSize } };
    }

    private static async ValueTask<MigrationApplyResult> ApplyAsync(
        MigrationPlan plan,
        MigrationCatalog catalog,
        IMigrationDataSource source,
        IMigrationTarget target,
        CancellationToken cancellationToken) =>
        await new MigrationApplyRunner().ApplyAsync(
            new MigrationApplyRequest
            {
                Plan = plan,
                Catalog = catalog,
                Source = source,
                Target = target,
            },
            cancellationToken);

    private static async Task AssertReceiptSetAsync(
        CSharpDbStagedMigrationTarget target,
        MigrationPlan plan,
        CancellationToken cancellationToken)
    {
        var expectedRows = new Dictionary<string, long>(StringComparer.Ordinal)
        {
            ["syn:table:customers-lower"] = 3,
            ["syn:table:customers-upper"] = 4,
            ["syn:table:orders"] = 12,
            ["syn:table:reserved"] = 2,
        };
        string planDigest = MigrationArtifactSerializer.ComputePlanDigest(plan);
        foreach ((string objectId, long rowCount) in expectedRows)
        {
            var receipts = new List<MigrationBatchReceipt>();
            await foreach (MigrationBatchReceipt receipt in target.ReadReceiptsAsync(
                               planDigest,
                               objectId,
                               cancellationToken))
            {
                receipts.Add(receipt);
            }

            Assert.Equal((rowCount + plan.Load.BatchSize - 1) / plan.Load.BatchSize, receipts.Count);
            Assert.Equal(rowCount, receipts.Sum(receipt => receipt.RowCount));
            Assert.Equal(
                Enumerable.Range(0, receipts.Count).Select(value => (long)value),
                receipts.Select(receipt => receipt.BatchOrdinal));
        }
    }

    private static async Task AssertExactSyntheticRowsAsync(
        IValidationSnapshot snapshot,
        MigrationCatalog catalog,
        MigrationPlan plan,
        CancellationToken cancellationToken)
    {
        Assert.Equal(
            Enumerable.Range(1, 4).Select(value => (long)value),
            await ReadIntegerColumnAsync(
                snapshot,
                catalog,
                plan,
                "syn:table:customers-upper",
                "syn:column:customers-upper:id",
                cancellationToken));
        Assert.Equal(
            Enumerable.Range(1, 12).Select(value => (long)value),
            await ReadIntegerColumnAsync(
                snapshot,
                catalog,
                plan,
                "syn:table:orders",
                "syn:column:orders:id",
                cancellationToken));

        List<MigrationValidationRow> lower = await CollectAsync(
            snapshot.ReadRowsAsync("syn:table:customers-lower", cancellationToken));
        int lowerCodeIndex = ColumnIndex(
            catalog,
            plan,
            "syn:table:customers-lower",
            "syn:column:customers-lower:code-upper");
        Assert.Equal(
            ["alpha", "beta", "gamma"],
            lower.Select(row => row.Values[lowerCodeIndex].AsText)
                .Order(StringComparer.Ordinal));

        List<MigrationValidationRow> reserved = await CollectAsync(
            snapshot.ReadRowsAsync("syn:table:reserved", cancellationToken));
        Assert.Equal(2, reserved.Count);
        Assert.Single(reserved, row => row.Values[0].IsNull);
        Assert.Single(reserved, row =>
            row.Values[0].Type == DbType.Text &&
            string.Equals(row.Values[0].AsText, "reserved-one", StringComparison.Ordinal));
    }

    private static async Task<long[]> ReadIntegerColumnAsync(
        IValidationSnapshot snapshot,
        MigrationCatalog catalog,
        MigrationPlan plan,
        string tableObjectId,
        string columnObjectId,
        CancellationToken cancellationToken)
    {
        List<MigrationValidationRow> rows = await CollectAsync(
            snapshot.ReadRowsAsync(tableObjectId, cancellationToken));
        int index = ColumnIndex(catalog, plan, tableObjectId, columnObjectId);
        return rows.Select(row => row.Values[index].AsInteger).Order().ToArray();
    }

    private static int ColumnIndex(
        MigrationCatalog catalog,
        MigrationPlan plan,
        string tableObjectId,
        string columnObjectId)
    {
        IReadOnlySet<string> included = plan.Objects
            .Where(item => item.Included)
            .Select(item => item.SourceObjectId)
            .ToHashSet(StringComparer.Ordinal);
        string[] columns = catalog.Objects
            .Where(item => item.Kind == MigrationObjectKind.Column &&
                string.Equals(item.ParentObjectId, tableObjectId, StringComparison.Ordinal) &&
                included.Contains(item.ObjectId))
            .OrderBy(item => item.ObjectId, StringComparer.Ordinal)
            .Select(item => item.ObjectId)
            .ToArray();
        int index = Array.IndexOf(columns, columnObjectId);
        Assert.True(index >= 0, $"Column '{columnObjectId}' is not included in '{tableObjectId}'.");
        return index;
    }

    private static async Task<List<T>> CollectAsync<T>(IAsyncEnumerable<T> source)
    {
        var values = new List<T>();
        await foreach (T value in source)
            values.Add(value);
        return values;
    }

    private static async Task<string> ReadLifecycleAsync(
        string targetPath,
        CancellationToken cancellationToken)
    {
        await using Database database = await Database.OpenAsync(targetPath, cancellationToken);
        await using var result = await database.ExecuteAsync(
            "SELECT \"lifecycle_state\" FROM \"__csharpdb_migration_state\" WHERE \"singleton\" = 1",
            cancellationToken);
        Assert.True(await result.MoveNextAsync(cancellationToken));
        string lifecycle = result.Current[0].AsText;
        Assert.False(await result.MoveNextAsync(cancellationToken));
        return lifecycle;
    }

    private sealed record CrashResult(
        int ExitCode,
        CSharpDbMigrationFaultPoint Point,
        string SourceObjectId,
        long BatchOrdinal,
        string StdOut,
        string StdErr);

    private sealed class TemporaryTargetDirectory : IDisposable
    {
        public TemporaryTargetDirectory()
        {
            DirectoryPath = Path.Combine(
                Path.GetTempPath(),
                $"csharpdb-migration-process-crash-{Guid.NewGuid():N}");
            Directory.CreateDirectory(DirectoryPath);
            TargetPath = Path.Combine(DirectoryPath, "staged.csdb");
        }

        public string DirectoryPath { get; }

        public string TargetPath { get; }

        public string LeasePath => TargetPath + ".migration.lock";

        public void Dispose()
        {
            if (Directory.Exists(DirectoryPath))
                Directory.Delete(DirectoryPath, recursive: true);
        }
    }
}
