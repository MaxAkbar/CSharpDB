using System.Diagnostics;
using System.IO.Pipes;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Engine;
using CSharpDB.Migration.CSharpDb;
using CSharpDB.Migration.Validation;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Tests;

[Collection("MigrationCrashHarnessProcess")]
public sealed class MigrationProcessCrashRecoveryTests
{
    private const string FailFastScenario = "fail-fast";
    private const string AcceptedOnlyScenario = "accepted-only";
    private const string MixedScenario = "mixed";
    private const string AllRejectScenario = "all-reject";
    private const string DeterministicRuleId = "MIG-CSV-ROW-001";
    private const string RejectSourceObjectId = "syn:table:customers-lower";
    private const string RejectColumnObjectId = "syn:column:customers-lower:code-lower";

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
            Assert.Null(await resumed.ReadActivationReceiptAsync(Ct));
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
            Assert.Null(await verified.ReadActivationReceiptAsync(Ct));
        }

        Assert.Equal("awaiting-validation", await ReadLifecycleAsync(files.TargetPath, Ct));
    }

    [Theory]
    [InlineData(AcceptedOnlyScenario, CSharpDbMigrationFaultPoint.BeforeRows, false)]
    [InlineData(AcceptedOnlyScenario, CSharpDbMigrationFaultPoint.AfterRowsBeforeReceipt, false)]
    [InlineData(AcceptedOnlyScenario, CSharpDbMigrationFaultPoint.AfterRejectsBeforeReceipt, false)]
    [InlineData(AcceptedOnlyScenario, CSharpDbMigrationFaultPoint.AfterReceiptBeforeCommit, false)]
    [InlineData(AcceptedOnlyScenario, CSharpDbMigrationFaultPoint.AfterCommit, true)]
    [InlineData(MixedScenario, CSharpDbMigrationFaultPoint.BeforeRows, false)]
    [InlineData(MixedScenario, CSharpDbMigrationFaultPoint.AfterRowsBeforeReceipt, false)]
    [InlineData(MixedScenario, CSharpDbMigrationFaultPoint.AfterRejectsBeforeReceipt, false)]
    [InlineData(MixedScenario, CSharpDbMigrationFaultPoint.AfterReceiptBeforeCommit, false)]
    [InlineData(MixedScenario, CSharpDbMigrationFaultPoint.AfterCommit, true)]
    [InlineData(AllRejectScenario, CSharpDbMigrationFaultPoint.BeforeRows, false)]
    [InlineData(AllRejectScenario, CSharpDbMigrationFaultPoint.AfterRowsBeforeReceipt, false)]
    [InlineData(AllRejectScenario, CSharpDbMigrationFaultPoint.AfterRejectsBeforeReceipt, false)]
    [InlineData(AllRejectScenario, CSharpDbMigrationFaultPoint.AfterReceiptBeforeCommit, false)]
    [InlineData(AllRejectScenario, CSharpDbMigrationFaultPoint.AfterCommit, true)]
    public async Task DeterministicRejectChildProcessCrash_RecoversWholeBatchAndReplaysExactly(
        string scenario,
        CSharpDbMigrationFaultPoint faultPoint,
        bool batchCommitted)
    {
        using var files = new TemporaryTargetDirectory();
        MigrationCatalog catalog = await InspectAsync(Ct);
        MigrationPlan plan = ReadyDeterministicRejectPlan(catalog, batchSize: 3);
        MigrationTargetBatch batch = DeterministicBatch(plan, catalog, scenario);
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
            await target.ApplySchemaAsync(
                plan,
                catalog,
                MigrationSchemaStage.LoadEssential,
                Ct);
        }

        CrashResult crash = await CrashAtAsync(
            files.TargetPath,
            faultPoint,
            Ct,
            scenario);
        Assert.NotEqual(0, crash.ExitCode);
        Assert.Equal(faultPoint, crash.Point);
        Assert.Equal(RejectSourceObjectId, crash.SourceObjectId);
        Assert.Equal(0, crash.BatchOrdinal);
        Assert.True(File.Exists(files.LeasePath));

        // Inspect the recovered durable state before issuing any retry. Every
        // pre-commit boundary must be wholly absent; AfterCommit must be whole.
        await using (CSharpDbStagedMigrationTarget recovered =
                     await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                         cancellationToken: Ct))
        {
            Assert.Equal(targetIdentity, recovered.TargetIdentity);
            if (batchCommitted)
            {
                MigrationBatchReceipt receipt = Assert.IsType<MigrationBatchReceipt>(
                    await recovered.ReadReceiptAsync(
                        batch.PlanDigest,
                        batch.SourceObjectId,
                        batch.BatchOrdinal,
                        Ct));
                AssertReceiptMatchesBatch(receipt, targetIdentity, batch);
                await AssertLedgerMatchesBatchAsync(recovered, batch, Ct);
            }
            else
            {
                Assert.Null(await recovered.ReadReceiptAsync(
                    batch.PlanDigest,
                    batch.SourceObjectId,
                    batch.BatchOrdinal,
                    Ct));
                Assert.Empty(await ReadLedgerAsync(recovered, plan, Ct));
            }
        }

        Assert.False(File.Exists(files.LeasePath));
        await AssertPhysicalRowsAsync(
            files.TargetPath,
            plan,
            batch,
            batchCommitted ? batch.Rows : [],
            Ct);

        // The first call either commits the rolled-back batch or replays the
        // already-committed receipt. The second call must always be a replay.
        await using (CSharpDbStagedMigrationTarget resumed =
                     await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                         cancellationToken: Ct))
        {
            MigrationBatchReceipt first = await resumed.WriteBatchAsync(batch, Ct);
            AssertReceiptMatchesBatch(first, targetIdentity, batch);
            await AssertLedgerMatchesBatchAsync(resumed, batch, Ct);

            MigrationBatchReceipt replay = await resumed.WriteBatchAsync(batch, Ct);
            Assert.Equal(first, replay);
            await AssertLedgerMatchesBatchAsync(resumed, batch, Ct);
        }

        Assert.False(File.Exists(files.LeasePath));
        await AssertPhysicalRowsAsync(files.TargetPath, plan, batch, batch.Rows, Ct);

        // A fresh reopen must reconstruct the exact receipt/ledger state and
        // replay without duplicating either accepted or rejected outcomes.
        await using (CSharpDbStagedMigrationTarget verified =
                     await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                         cancellationToken: Ct))
        {
            MigrationBatchReceipt restored = Assert.IsType<MigrationBatchReceipt>(
                await verified.ReadReceiptAsync(
                    batch.PlanDigest,
                    batch.SourceObjectId,
                    batch.BatchOrdinal,
                    Ct));
            AssertReceiptMatchesBatch(restored, targetIdentity, batch);
            await AssertLedgerMatchesBatchAsync(verified, batch, Ct);

            MigrationBatchReceipt replay = await verified.WriteBatchAsync(batch, Ct);
            Assert.Equal(restored, replay);
            await AssertLedgerMatchesBatchAsync(verified, batch, Ct);
        }

        Assert.False(File.Exists(files.LeasePath));
        await AssertPhysicalRowsAsync(files.TargetPath, plan, batch, batch.Rows, Ct);
        Assert.Equal("loading-data", await ReadLifecycleAsync(files.TargetPath, Ct));
    }

    [Fact]
    public async Task RejectArtifactChildCrash_AfterDurablePartialTemp_RetryReclaimsAndPublishes()
    {
        using var files = new TemporaryTargetDirectory();
        MigrationCatalog catalog = await InspectAsync(Ct);
        MigrationPlan plan = ReadyDeterministicRejectPlan(catalog, batchSize: 3);
        MigrationTargetBatch batch = DeterministicBatch(plan, catalog, MixedScenario);
        await PrepareArtifactTargetAsync(files.TargetPath, plan, catalog, batch, Ct);
        string artifactPath = Path.Combine(files.DirectoryPath, "rejects.jsonl");

        ArtifactCrashResult crash = await CrashArtifactAtAsync(
            files.TargetPath,
            artifactPath,
            MigrationRejectArtifactFaultPoint.AfterTemporaryHeaderDurablyFlushed,
            Ct);

        Assert.NotEqual(0, crash.ExitCode);
        Assert.Equal(
            MigrationRejectArtifactFaultPoint.AfterTemporaryHeaderDurablyFlushed,
            crash.Point);
        Assert.False(File.Exists(artifactPath));
        string temporaryPath = Assert.Single(Directory.EnumerateFiles(
            files.DirectoryPath,
            ".csharpdb-reject-*.tmp"));
        byte[] partialBytes = await File.ReadAllBytesAsync(temporaryPath, Ct);
        Assert.Equal(
            Encoding.UTF8.GetBytes(
                MigrationRejectLedgerCodec.SerializeArtifactHeader(batch.PlanDigest) + "\n"),
            partialBytes);

        await using CSharpDbStagedMigrationTarget reopened =
            await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                files.TargetPath,
                plan,
                catalog,
                SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                cancellationToken: Ct);
        MigrationRejectArtifactWriteResult result =
            await new MigrationRejectArtifactWriter().WriteAsync(
                new MigrationRejectArtifactWriteRequest
                {
                    Plan = plan,
                    Catalog = catalog,
                    Target = reopened,
                    OutputPath = artifactPath,
                },
                Ct);

        Assert.False(result.ReusedExistingArtifact);
        Assert.Equal(1, result.RejectedRowCount);
        Assert.True(result.ArtifactBytes > partialBytes.LongLength);
        Assert.True(File.Exists(artifactPath));
        Assert.Empty(Directory.EnumerateFiles(
            files.DirectoryPath,
            ".csharpdb-reject-*.tmp"));
    }

    [Fact]
    public async Task RejectArtifactChildCrash_AfterPublishBeforeResult_RetryExactlyReuses()
    {
        using var files = new TemporaryTargetDirectory();
        MigrationCatalog catalog = await InspectAsync(Ct);
        MigrationPlan plan = ReadyDeterministicRejectPlan(catalog, batchSize: 3);
        MigrationTargetBatch batch = DeterministicBatch(plan, catalog, MixedScenario);
        await PrepareArtifactTargetAsync(files.TargetPath, plan, catalog, batch, Ct);
        string artifactPath = Path.Combine(files.DirectoryPath, "rejects.jsonl");

        ArtifactCrashResult crash = await CrashArtifactAtAsync(
            files.TargetPath,
            artifactPath,
            MigrationRejectArtifactFaultPoint.AfterPublishBeforeResult,
            Ct);

        Assert.NotEqual(0, crash.ExitCode);
        Assert.Equal(MigrationRejectArtifactFaultPoint.AfterPublishBeforeResult, crash.Point);
        Assert.True(File.Exists(artifactPath));
        Assert.Empty(Directory.EnumerateFiles(
            files.DirectoryPath,
            ".csharpdb-reject-*.tmp"));
        byte[] publishedBytes = await File.ReadAllBytesAsync(artifactPath, Ct);
        string publishedDigest = Convert.ToHexString(SHA256.HashData(publishedBytes))
            .ToLowerInvariant();

        await using CSharpDbStagedMigrationTarget reopened =
            await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                files.TargetPath,
                plan,
                catalog,
                SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                cancellationToken: Ct);
        MigrationRejectArtifactWriteResult result =
            await new MigrationRejectArtifactWriter().WriteAsync(
                new MigrationRejectArtifactWriteRequest
                {
                    Plan = plan,
                    Catalog = catalog,
                    Target = reopened,
                    OutputPath = artifactPath,
                },
                Ct);

        Assert.True(result.ReusedExistingArtifact);
        Assert.Equal(publishedDigest, result.ArtifactDigest);
        Assert.Equal(publishedBytes, await File.ReadAllBytesAsync(artifactPath, Ct));
        Assert.Empty(Directory.EnumerateFiles(
            files.DirectoryPath,
            ".csharpdb-reject-*.tmp"));
    }

    [Fact]
    public async Task CrashResumeValidateReportAndActivate_CompletesFoundationSpineExactly()
    {
        using var files = new TemporaryTargetDirectory();
        MigrationCatalog catalog = await InspectAsync(Ct);
        MigrationPlan plan = ReadyPlan(catalog, batchSize: 2);

        await using (CSharpDbStagedMigrationTarget created =
                     await CSharpDbStagedMigrationTarget.CreateNewAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                         cancellationToken: Ct))
        {
        }

        CrashResult crash = await CrashAtAsync(
            files.TargetPath,
            CSharpDbMigrationFaultPoint.AfterRowsBeforeReceipt,
            Ct);
        Assert.NotEqual(0, crash.ExitCode);

        string reportPath = Path.Combine(files.DirectoryPath, "validation.json");
        string? firstDigest;
        await using (var source = new SyntheticMigrationDataSource(catalog))
        await using (var sourceSnapshot = new MigrationDataSourceValidationSnapshot(plan, catalog, source))
        await using (CSharpDbStagedMigrationTarget resumed =
                     await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                         files.TargetPath,
                         plan,
                         catalog,
                         source.SnapshotIdentity,
                         cancellationToken: Ct))
        {
            MigrationApplyResult apply = await ApplyAsync(plan, catalog, source, resumed, Ct);
            Assert.Equal(MigrationApplyStatus.AwaitingValidation, apply.Status);

            var request = new MigrationValidationRunRequest
            {
                Plan = plan,
                Catalog = catalog,
                SourceSnapshot = sourceSnapshot,
                Target = resumed,
                Level = MigrationValidationLevel.Checksum,
                ReportOutputPath = reportPath,
                ChecksumOptions = new PartitionedChecksumValidatorOptions
                {
                    SpillRootDirectory = files.DirectoryPath,
                },
            };
            var runner = new MigrationValidationRunner();
            MigrationValidationRunResult first = await runner.ValidateAsync(request, Ct);
            MigrationValidationRunResult retry = await runner.ValidateAsync(request, Ct);

            Assert.Equal(MigrationValidationStatus.Passed, first.Report.Outcome);
            Assert.True(first.Activated);
            Assert.True(retry.Activated);
            Assert.Equal(first.ReportDigest, retry.ReportDigest);
            Assert.Equal(
                await File.ReadAllTextAsync(reportPath, Ct),
                MigrationValidationReportSerializer.Serialize(first.Report, writeIndented: true));
            firstDigest = first.ReportDigest;
        }

        Assert.Equal("activated", await ReadLifecycleAsync(files.TargetPath, Ct));
        Assert.False(string.IsNullOrWhiteSpace(firstDigest));
        Assert.Empty(Directory.GetDirectories(files.DirectoryPath));
    }

    private static async Task<CrashResult> CrashAtAsync(
        string targetPath,
        CSharpDbMigrationFaultPoint faultPoint,
        CancellationToken cancellationToken,
        string scenario = FailFastScenario)
    {
        string pipeName = $"csharpdb-migration-crash-{Guid.NewGuid():N}";
        await using var pipe = new NamedPipeServerStream(
            pipeName,
            PipeDirection.InOut,
            maxNumberOfServerInstances: 1,
            PipeTransmissionMode.Byte,
            PipeOptions.Asynchronous);
        using Process process = CreateCrashHarnessProcess(
            targetPath,
            pipeName,
            faultPoint,
            scenario);
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
                $"Migration crash harness failed in scenario '{scenario}' at {faultPoint}. " +
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

    private static async Task<ArtifactCrashResult> CrashArtifactAtAsync(
        string targetPath,
        string artifactPath,
        MigrationRejectArtifactFaultPoint faultPoint,
        CancellationToken cancellationToken)
    {
        string pipeName = $"csharpdb-reject-artifact-crash-{Guid.NewGuid():N}";
        await using var pipe = new NamedPipeServerStream(
            pipeName,
            PipeDirection.InOut,
            maxNumberOfServerInstances: 1,
            PipeTransmissionMode.Byte,
            PipeOptions.Asynchronous);
        using Process process = CreateRejectArtifactCrashHarnessProcess(
            targetPath,
            artifactPath,
            pipeName,
            faultPoint);
        if (!process.Start())
        {
            throw new InvalidOperationException(
                "Failed to start the migration reject-artifact crash harness process.");
        }

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
            if (parts.Length != 2 ||
                !string.Equals(parts[0], "ARTIFACT_REACHED", StringComparison.Ordinal) ||
                !Enum.TryParse(
                    parts[1],
                    ignoreCase: false,
                    out MigrationRejectArtifactFaultPoint reachedPoint))
            {
                throw ProtocolFailure(reached);
            }

            process.Kill(entireProcessTree: true);
            killed = true;
            await process.WaitForExitAsync(cancellationToken)
                .WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
            return new ArtifactCrashResult(
                process.ExitCode,
                reachedPoint,
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
                $"Migration reject-artifact crash harness failed at {faultPoint}. " +
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

    private static async Task PrepareArtifactTargetAsync(
        string targetPath,
        MigrationPlan plan,
        MigrationCatalog catalog,
        MigrationTargetBatch batch,
        CancellationToken cancellationToken)
    {
        await using CSharpDbStagedMigrationTarget target =
            await CSharpDbStagedMigrationTarget.CreateNewAsync(
                targetPath,
                plan,
                catalog,
                SyntheticMigrationDataSource.FixtureSnapshotIdentity,
                cancellationToken: cancellationToken);
        await target.ApplySchemaAsync(
            plan,
            catalog,
            MigrationSchemaStage.LoadEssential,
            cancellationToken);
        await target.WriteBatchAsync(batch, cancellationToken);
        foreach (MigrationSchemaStage stage in Enum.GetValues<MigrationSchemaStage>().Skip(1))
            await target.ApplySchemaAsync(plan, catalog, stage, cancellationToken);
    }

    private static Process CreateCrashHarnessProcess(
        string targetPath,
        string pipeName,
        CSharpDbMigrationFaultPoint faultPoint,
        string scenario)
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
        if (!string.Equals(scenario, FailFastScenario, StringComparison.Ordinal))
        {
            startInfo.ArgumentList.Add("--scenario");
            startInfo.ArgumentList.Add(scenario);
        }
        return new Process { StartInfo = startInfo };
    }

    private static Process CreateRejectArtifactCrashHarnessProcess(
        string targetPath,
        string artifactPath,
        string pipeName,
        MigrationRejectArtifactFaultPoint faultPoint)
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
        startInfo.ArgumentList.Add("--artifact-output");
        startInfo.ArgumentList.Add(artifactPath);
        startInfo.ArgumentList.Add("--artifact-fault");
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

    private static MigrationPlan ReadyDeterministicRejectPlan(
        MigrationCatalog catalog,
        int batchSize)
    {
        MigrationPlan plan = ReadyPlan(catalog, batchSize);
        return plan with
        {
            Load = plan.Load with
            {
                RejectMode = MigrationRejectMode.DeterministicRejects,
                RejectPolicy = new MigrationDeterministicRejectPolicy
                {
                    ContractVersion = MigrationRejectContract.DeterministicRejectsV1,
                    AllowedRuleIds = [DeterministicRuleId],
                    MaxRejectedRowsPerBatch = batchSize,
                    MaxRejectedRowsPerRun = 100,
                    MaxRawValueBytes = 1_024,
                    MaxRawValueBytesPerBatch = 8_192,
                    MaxRawValueBytesPerRun = 65_536,
                    MaxArtifactBytes = 131_072,
                },
            },
        };
    }

    private static MigrationTargetBatch DeterministicBatch(
        MigrationPlan plan,
        MigrationCatalog catalog,
        string scenario)
    {
        IReadOnlyList<MigrationTargetRow> rows;
        IReadOnlyList<MigrationRejectedRow> rejectedRows;
        switch (scenario)
        {
            case AcceptedOnlyScenario:
                rows =
                [
                    AcceptedRow(0, "zero"),
                    AcceptedRow(1, "one"),
                ];
                rejectedRows = [];
                break;
            case MixedScenario:
                rows =
                [
                    AcceptedRow(0, "zero"),
                    AcceptedRow(2, "two"),
                ];
                rejectedRows = [RejectedRow(1, "bad-one")];
                break;
            case AllRejectScenario:
                rows = [];
                rejectedRows =
                [
                    RejectedRow(0, "bad-zero"),
                    RejectedRow(1, "bad-one"),
                ];
                break;
            default:
                throw new ArgumentException(
                    $"Unknown deterministic reject crash scenario '{scenario}'.",
                    nameof(scenario));
        }

        var unsigned = new MigrationTargetBatch
        {
            PlanDigest = MigrationArtifactSerializer.ComputePlanDigest(plan),
            CatalogDigest = plan.CatalogDigest,
            SourceFingerprint = plan.Source.Fingerprint,
            SourceSnapshotIdentity = SyntheticMigrationDataSource.FixtureSnapshotIdentity,
            SourceObjectId = RejectSourceObjectId,
            ColumnObjectIds = IncludedColumnIds(catalog, plan, RejectSourceObjectId),
            BatchOrdinal = 0,
            StartCursor = null,
            NextCursor = null,
            BatchDigest = string.Empty,
            RejectContractVersion = MigrationRejectContract.DeterministicRejectsV1,
            Rows = rows,
            RejectedRows = rejectedRows,
        };
        MigrationTargetBatch rejectSealed = unsigned with
        {
            RejectDigest = MigrationRejectDigest.Compute(unsigned),
        };
        return rejectSealed with
        {
            BatchDigest = MigrationBatchDigest.Compute(rejectSealed),
        };
    }

    private static MigrationTargetRow AcceptedRow(long sourceRowOrdinal, string suffix) => new()
    {
        SourceRowOrdinal = sourceRowOrdinal,
        StableKey = suffix,
        Values =
        [
            DbValue.FromText($"lower-{suffix}"),
            DbValue.FromText($"upper-{suffix}"),
        ],
    };

    private static MigrationRejectedRow RejectedRow(
        long sourceRowOrdinal,
        string rawValue) => new()
        {
            SourceRowOrdinal = sourceRowOrdinal,
            RuleId = DeterministicRuleId,
            ColumnObjectId = RejectColumnObjectId,
            Evidence =
            [
                new MigrationRejectEvidence
                {
                    Name = MigrationRejectLedgerCodec.RawValueEvidenceName,
                    Value = rawValue,
                },
            ],
        };

    private static string[] IncludedColumnIds(
        MigrationCatalog catalog,
        MigrationPlan plan,
        string tableObjectId)
    {
        IReadOnlySet<string> included = plan.Objects
            .Where(item => item.Included)
            .Select(item => item.SourceObjectId)
            .ToHashSet(StringComparer.Ordinal);
        return catalog.Objects
            .Where(item => item.Kind == MigrationObjectKind.Column &&
                string.Equals(item.ParentObjectId, tableObjectId, StringComparison.Ordinal) &&
                included.Contains(item.ObjectId))
            .OrderBy(item => item.ObjectId, StringComparer.Ordinal)
            .Select(item => item.ObjectId)
            .ToArray();
    }

    private static void AssertReceiptMatchesBatch(
        MigrationBatchReceipt actual,
        string targetIdentity,
        MigrationTargetBatch batch)
    {
        var expected = new MigrationBatchReceipt
        {
            TargetIdentity = targetIdentity,
            PlanDigest = batch.PlanDigest,
            CatalogDigest = batch.CatalogDigest,
            SourceFingerprint = batch.SourceFingerprint,
            SourceSnapshotIdentity = batch.SourceSnapshotIdentity,
            SourceObjectId = batch.SourceObjectId,
            BatchOrdinal = batch.BatchOrdinal,
            StartCursor = batch.StartCursor,
            NextCursor = batch.NextCursor,
            BatchDigest = batch.BatchDigest,
            RejectContractVersion = batch.RejectContractVersion,
            RejectDigest = batch.RejectDigest,
            RowCount = batch.Rows.Count,
            RejectedRowCount = batch.RejectedRows.Count,
        };
        Assert.Equal(expected, actual);
    }

    private static async Task AssertLedgerMatchesBatchAsync(
        CSharpDbStagedMigrationTarget target,
        MigrationTargetBatch batch,
        CancellationToken cancellationToken)
    {
        List<MigrationRejectLedgerEntry> actual = await ReadLedgerAsync(
            target,
            batch.PlanDigest,
            cancellationToken);
        Assert.Equal(batch.RejectedRows.Count, actual.Count);
        for (int index = 0; index < batch.RejectedRows.Count; index++)
        {
            MigrationRejectedRow expectedRow = batch.RejectedRows[index];
            MigrationRejectLedgerEntry actualEntry = actual[index];
            Assert.Equal(batch.PlanDigest, actualEntry.PlanDigest);
            Assert.Equal(batch.SourceObjectId, actualEntry.SourceObjectId);
            Assert.Equal(batch.BatchOrdinal, actualEntry.BatchOrdinal);
            AssertRejectedRowEqual(expectedRow, actualEntry.RejectedRow);
            Assert.Equal(
                MigrationRejectLedgerCodec.GetRawValueByteCount(expectedRow),
                actualEntry.RawValueByteCount);
            Assert.Equal(
                MigrationRejectLedgerCodec.GetCanonicalEntryByteCount(
                    batch.SourceObjectId,
                    batch.BatchOrdinal,
                    expectedRow),
                actualEntry.CanonicalEntryByteCount);
        }
    }

    private static void AssertRejectedRowEqual(
        MigrationRejectedRow expected,
        MigrationRejectedRow actual)
    {
        Assert.Equal(expected.SourceRowOrdinal, actual.SourceRowOrdinal);
        Assert.Equal(expected.RuleId, actual.RuleId);
        Assert.Equal(expected.ColumnObjectId, actual.ColumnObjectId);
        Assert.Equal(expected.Evidence.Count, actual.Evidence.Count);
        for (int index = 0; index < expected.Evidence.Count; index++)
        {
            Assert.Equal(expected.Evidence[index].Name, actual.Evidence[index].Name);
            Assert.Equal(expected.Evidence[index].Value, actual.Evidence[index].Value);
        }
    }

    private static async Task<List<MigrationRejectLedgerEntry>> ReadLedgerAsync(
        CSharpDbStagedMigrationTarget target,
        MigrationPlan plan,
        CancellationToken cancellationToken) =>
        await ReadLedgerAsync(
            target,
            MigrationArtifactSerializer.ComputePlanDigest(plan),
            cancellationToken);

    private static async Task<List<MigrationRejectLedgerEntry>> ReadLedgerAsync(
        CSharpDbStagedMigrationTarget target,
        string planDigest,
        CancellationToken cancellationToken)
    {
        var entries = new List<MigrationRejectLedgerEntry>();
        await foreach (MigrationRejectLedgerEntry entry in target.ReadRejectLedgerAsync(
                           planDigest,
                           cancellationToken))
        {
            entries.Add(entry);
        }
        return entries;
    }

    private static async Task AssertPhysicalRowsAsync(
        string targetPath,
        MigrationPlan plan,
        MigrationTargetBatch batch,
        IReadOnlyList<MigrationTargetRow> expectedRows,
        CancellationToken cancellationToken)
    {
        string tableName = plan.Objects.Single(item =>
            string.Equals(
                item.SourceObjectId,
                batch.SourceObjectId,
                StringComparison.Ordinal)).TargetName!;
        string[] columnNames = batch.ColumnObjectIds
            .Select(columnObjectId => plan.Objects.Single(item =>
                string.Equals(
                    item.SourceObjectId,
                    columnObjectId,
                    StringComparison.Ordinal)).TargetName!)
            .ToArray();
        string projection = string.Join(", ", columnNames.Select(QuoteIdentifier));
        string sql = $"SELECT {projection} FROM {QuoteIdentifier(tableName)}";

        await using Database database = await Database.OpenAsync(targetPath, cancellationToken);
        await using var result = await database.ExecuteAsync(sql, cancellationToken);
        var actualKeys = new List<string>();
        await foreach (DbValue[] row in result.GetRowsAsync(cancellationToken))
            actualKeys.Add(PhysicalRowKey(row));

        string[] expectedKeys = expectedRows
            .Select(row => PhysicalRowKey(row.Values))
            .Order(StringComparer.Ordinal)
            .ToArray();
        actualKeys.Sort(StringComparer.Ordinal);
        Assert.Equal(expectedKeys, actualKeys);
    }

    private static string PhysicalRowKey(IEnumerable<DbValue> values) =>
        string.Join(
            "|",
            values.Select(value =>
                $"{value.Type}:{Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(value.AsText))}"));

    private static string QuoteIdentifier(string identifier) =>
        $"\"{identifier.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";

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

    private sealed record ArtifactCrashResult(
        int ExitCode,
        MigrationRejectArtifactFaultPoint Point,
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
