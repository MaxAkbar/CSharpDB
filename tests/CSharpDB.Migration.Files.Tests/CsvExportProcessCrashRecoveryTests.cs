using System.Diagnostics;
using System.Globalization;
using System.IO.Pipes;
using System.Runtime.CompilerServices;
using System.Text;
using CSharpDB.Migration.Files.Csv;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Files.Tests;

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class CsvExportCrashHarnessProcessCollection
{
    public const string Name = "CsvExportCrashHarnessProcess";
}

[Collection(CsvExportCrashHarnessProcessCollection.Name)]
public sealed class CsvExportProcessCrashRecoveryTests
{
#if DEBUG
    private const string BuildConfiguration = "Debug";
#else
    private const string BuildConfiguration = "Release";
#endif

    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(
        nameof(CsvExportCheckpointFaultPoint
            .AfterDataDurablyFlushedBeforePendingCheckpoint))]
    [InlineData(
        nameof(CsvExportCheckpointFaultPoint
            .AfterPendingCheckpointDurablyFlushedBeforeActiveReplacement))]
    [InlineData(
        nameof(CsvExportCheckpointFaultPoint
            .AfterActiveCheckpointReplacedBeforeResult))]
    public async Task CheckpointProcessCrash_RecoversOnlyActiveGenerationAndExactPrefix(
        string faultPointName)
    {
        if (!OperatingSystem.IsWindows())
            return;

        CsvExportCheckpointFaultPoint faultPoint =
            Enum.Parse<CsvExportCheckpointFaultPoint>(
                faultPointName,
                ignoreCase: false);
        using var workspace = new TemporaryDirectory();
        CheckpointFixture fixture =
            await PrepareCheckpointFixtureAsync(workspace);

        ProcessCrashResult crash = await CrashCheckpointAtAsync(
            fixture,
            faultPoint,
            Cancellation);

        Assert.NotEqual(0, crash.ExitCode);
        Assert.Equal(
            $"CSV_CHECKPOINT_REACHED|{faultPoint}",
            crash.Reached);

        bool activeReplacementCommitted =
            faultPoint == CsvExportCheckpointFaultPoint
                .AfterActiveCheckpointReplacedBeforeResult;
        bool pendingCheckpointDurable =
            faultPoint == CsvExportCheckpointFaultPoint
                .AfterPendingCheckpointDurablyFlushedBeforeActiveReplacement;

        byte[] expectedActiveCheckpointBytes =
            activeReplacementCommitted
                ? fixture.GenerationOneBytes
                : fixture.GenerationZeroBytes;
        Assert.Equal(
            expectedActiveCheckpointBytes,
            await File.ReadAllBytesAsync(
                fixture.Paths.CheckpointPath,
                Cancellation));
        Assert.Equal(
            activeReplacementCommitted ? 1 : 0,
            ReadCheckpoint(fixture.Paths.CheckpointPath).Generation);
        Assert.Equal(
            pendingCheckpointDurable,
            File.Exists(fixture.Paths.PendingCheckpointPath));
        if (pendingCheckpointDurable)
        {
            Assert.Equal(
                fixture.GenerationOneBytes,
                await File.ReadAllBytesAsync(
                    fixture.Paths.PendingCheckpointPath,
                    Cancellation));
        }
        Assert.Equal(
            fixture.HeaderAndRowBytes,
            await File.ReadAllBytesAsync(
                fixture.Paths.PreparedDataPath,
                Cancellation));

        await using (CsvExportPreparedOutputLease recovered =
                     await CsvExportPreparedOutputLease.OpenAsync(
                         fixture.DestinationPath,
                         fixture.GenerationOne.Binding,
                         Cancellation))
        {
            Assert.Equal(
                CsvExportPreparedOutputState.Recovered,
                recovered.State);
            Assert.Equal(
                activeReplacementCommitted ? 1 : 0,
                recovered.CurrentCheckpoint?.Generation);
            Assert.Equal(
                activeReplacementCommitted
                    ? fixture.HeaderAndRowBytes
                    : fixture.HeaderBytes,
                await ReadAllBytesAsync(
                    recovered.DataStream,
                    Cancellation));

        }

        await CompleteAndAssertTerminalAsync(
            fixture.RecoveryRequest,
            fixture.Paths,
            fixture.HeaderAndRowBytes,
            fixture.ExpectedExport);
    }

    [Theory]
    [InlineData(
        nameof(CsvExportCheckpointFaultPoint
            .AfterDataDurablyFlushedBeforePendingCheckpoint))]
    [InlineData(
        nameof(CsvExportCheckpointFaultPoint
            .AfterPendingCheckpointDurablyFlushedBeforeActiveReplacement))]
    [InlineData(
        nameof(CsvExportCheckpointFaultPoint
            .AfterActiveCheckpointReplacedBeforeResult))]
    public async Task FirstCheckpointProcessCrash_RerunResetsUncheckpointedDataAndCompletes(
        string faultPointName)
    {
        if (!OperatingSystem.IsWindows())
            return;

        CsvExportCheckpointFaultPoint faultPoint =
            Enum.Parse<CsvExportCheckpointFaultPoint>(
                faultPointName,
                ignoreCase: false);
        using var workspace = new TemporaryDirectory();
        FirstCheckpointFixture fixture =
            await PrepareFirstCheckpointFixtureAsync(workspace);

        ProcessCrashResult crash = await CrashCheckpointAtAsync(
            fixture.DestinationPath,
            fixture.NextCheckpointPath,
            fixture.AppendBytesPath,
            faultPoint,
            Cancellation);
        Assert.NotEqual(0, crash.ExitCode);
        Assert.Equal(
            $"CSV_CHECKPOINT_REACHED|{faultPoint}",
            crash.Reached);

        bool activeReplacementCommitted =
            faultPoint == CsvExportCheckpointFaultPoint
                .AfterActiveCheckpointReplacedBeforeResult;
        bool pendingCheckpointDurable =
            faultPoint == CsvExportCheckpointFaultPoint
                .AfterPendingCheckpointDurablyFlushedBeforeActiveReplacement;

        Assert.Equal(
            activeReplacementCommitted,
            File.Exists(fixture.Paths.CheckpointPath));
        if (activeReplacementCommitted)
        {
            Assert.Equal(
                fixture.GenerationZeroBytes,
                await File.ReadAllBytesAsync(
                    fixture.Paths.CheckpointPath,
                    Cancellation));
        }
        Assert.Equal(
            pendingCheckpointDurable,
            File.Exists(fixture.Paths.PendingCheckpointPath));
        if (pendingCheckpointDurable)
        {
            Assert.Equal(
                fixture.GenerationZeroBytes,
                await File.ReadAllBytesAsync(
                    fixture.Paths.PendingCheckpointPath,
                    Cancellation));
        }
        Assert.Equal(
            fixture.HeaderBytes,
            await File.ReadAllBytesAsync(
                fixture.Paths.PreparedDataPath,
                Cancellation));

        await using (CsvExportPreparedOutputLease observed =
                     await CsvExportPreparedOutputLease.OpenAsync(
                         fixture.DestinationPath,
                         fixture.GenerationZero.Binding,
                         Cancellation))
        {
            Assert.Equal(
                activeReplacementCommitted
                    ? CsvExportPreparedOutputState.Recovered
                    : CsvExportPreparedOutputState.UncheckpointedData,
                observed.State);
            Assert.Equal(
                activeReplacementCommitted ? (long?)0 : null,
                observed.CurrentCheckpoint?.Generation);
        }

        await CompleteAndAssertTerminalAsync(
            fixture.RecoveryRequest,
            fixture.Paths,
            fixture.ExpectedDataBytes,
            fixture.ExpectedExport);
    }

    [Theory]
    [InlineData(
        nameof(CsvExportCheckpointFaultPoint
            .AfterDataDurablyFlushedBeforePendingCheckpoint))]
    [InlineData(
        nameof(CsvExportCheckpointFaultPoint
            .AfterPendingCheckpointDurablyFlushedBeforeActiveReplacement))]
    [InlineData(
        nameof(CsvExportCheckpointFaultPoint
            .AfterActiveCheckpointReplacedBeforeResult))]
    public async Task TerminalCheckpointProcessCrash_RerunCompletesIdenticalPreparedData(
        string faultPointName)
    {
        if (!OperatingSystem.IsWindows())
            return;

        CsvExportCheckpointFaultPoint faultPoint =
            Enum.Parse<CsvExportCheckpointFaultPoint>(
                faultPointName,
                ignoreCase: false);
        using var workspace = new TemporaryDirectory();
        TerminalCheckpointFixture fixture =
            await PrepareTerminalCheckpointFixtureAsync(workspace);

        ProcessCrashResult crash = await CrashCheckpointAtAsync(
            fixture.DestinationPath,
            fixture.NextCheckpointPath,
            fixture.EmptyAppendBytesPath,
            faultPoint,
            Cancellation);
        Assert.NotEqual(0, crash.ExitCode);
        Assert.Equal(
            $"CSV_CHECKPOINT_REACHED|{faultPoint}",
            crash.Reached);

        bool activeReplacementCommitted =
            faultPoint == CsvExportCheckpointFaultPoint
                .AfterActiveCheckpointReplacedBeforeResult;
        bool pendingCheckpointDurable =
            faultPoint == CsvExportCheckpointFaultPoint
                .AfterPendingCheckpointDurablyFlushedBeforeActiveReplacement;
        Assert.Equal(
            activeReplacementCommitted
                ? fixture.TerminalCheckpointBytes
                : fixture.WritingCheckpointBytes,
            await File.ReadAllBytesAsync(
                fixture.Paths.CheckpointPath,
                Cancellation));
        Assert.Equal(
            pendingCheckpointDurable,
            File.Exists(fixture.Paths.PendingCheckpointPath));
        if (pendingCheckpointDurable)
        {
            Assert.Equal(
                fixture.TerminalCheckpointBytes,
                await File.ReadAllBytesAsync(
                    fixture.Paths.PendingCheckpointPath,
                    Cancellation));
        }
        Assert.Equal(
            fixture.ExpectedDataBytes,
            await File.ReadAllBytesAsync(
                fixture.Paths.PreparedDataPath,
                Cancellation));

        await CompleteAndAssertTerminalAsync(
            fixture.RecoveryRequest,
            fixture.Paths,
            fixture.ExpectedDataBytes,
            fixture.ExpectedExport);
    }

    [Theory]
    [InlineData(
        nameof(CsvExportPublicationFaultPoint.BeforeDataNamespaceCommit))]
    [InlineData(
        nameof(CsvExportPublicationFaultPoint
            .AfterDataNamespaceCommitBeforeManifest))]
    [InlineData(
        nameof(CsvExportPublicationFaultPoint
            .BeforeManifestNamespaceCommit))]
    [InlineData(
        nameof(CsvExportPublicationFaultPoint
            .AfterManifestNamespaceCommitBeforeResult))]
    public async Task PublicationProcessCrash_RetryProducesOnlyExactManifestLastPair(
        string faultPointName)
    {
        if (!OperatingSystem.IsWindows())
            return;

        CsvExportPublicationFaultPoint faultPoint =
            Enum.Parse<CsvExportPublicationFaultPoint>(
                faultPointName,
                ignoreCase: false);
        using var workspace = new TemporaryDirectory();
        PublicationFixture fixture =
            await PreparePublicationFixtureAsync(workspace);

        ProcessCrashResult crash = await CrashPublicationAtAsync(
            fixture,
            faultPoint,
            Cancellation);

        Assert.NotEqual(0, crash.ExitCode);
        Assert.Equal(
            $"CSV_PUBLICATION_REACHED|{faultPoint}",
            crash.Reached);

        bool dataCommitted =
            faultPoint !=
            CsvExportPublicationFaultPoint.BeforeDataNamespaceCommit;
        bool manifestCommitted =
            faultPoint ==
            CsvExportPublicationFaultPoint
                .AfterManifestNamespaceCommitBeforeResult;

        Assert.Equal(
            dataCommitted,
            File.Exists(fixture.DestinationPath));
        Assert.Equal(
            manifestCommitted,
            File.Exists(fixture.ManifestPath));
        Assert.False(
            File.Exists(fixture.ManifestPath) &&
            !File.Exists(fixture.DestinationPath));

        if (dataCommitted)
        {
            Assert.Equal(
                fixture.PreparedDataBytes,
                await File.ReadAllBytesAsync(
                    fixture.DestinationPath,
                    Cancellation));
        }
        if (manifestCommitted)
        {
            Assert.Equal(
                fixture.Export.CanonicalManifestBytes,
                await File.ReadAllBytesAsync(
                    fixture.ManifestPath,
                    Cancellation));
        }

        string[] crashStaging = PublicationStagingFiles(workspace.Root);
        switch (faultPoint)
        {
            case CsvExportPublicationFaultPoint.BeforeDataNamespaceCommit:
                Assert.Single(crashStaging);
                Assert.EndsWith(
                    ".publish.data.next",
                    crashStaging[0],
                    StringComparison.OrdinalIgnoreCase);
                break;
            case CsvExportPublicationFaultPoint
                .AfterDataNamespaceCommitBeforeManifest:
                Assert.Empty(crashStaging);
                break;
            case CsvExportPublicationFaultPoint.BeforeManifestNamespaceCommit:
                Assert.Single(crashStaging);
                Assert.EndsWith(
                    ".publish.manifest.next",
                    crashStaging[0],
                    StringComparison.OrdinalIgnoreCase);
                break;
            case CsvExportPublicationFaultPoint
                .AfterManifestNamespaceCommitBeforeResult:
                Assert.Empty(crashStaging);
                break;
            default:
                throw new ArgumentOutOfRangeException(
                    nameof(faultPoint),
                    faultPoint,
                    "Unknown CSV publication fault point.");
        }

        CsvExportPublicationResult recovered =
            await new CsvExportPreparedOutputPublisher()
                .PublishCompletedAsync(
                    new CsvExportPublicationRequest
                    {
                        DestinationPath = fixture.DestinationPath,
                        ManifestPath = fixture.ManifestPath,
                        ExpectedManifestDigest =
                            fixture.Export.ManifestDigest,
                    },
                    Cancellation);

        Assert.Equal(dataCommitted, recovered.ReusedData);
        Assert.Equal(manifestCommitted, recovered.ReusedManifest);
        Assert.Equal(
            fixture.PreparedDataBytes,
            await File.ReadAllBytesAsync(
                fixture.DestinationPath,
                Cancellation));
        Assert.Equal(
            fixture.Export.CanonicalManifestBytes,
            await File.ReadAllBytesAsync(
                fixture.ManifestPath,
                Cancellation));
        Assert.Equal(
            fixture.Export.ManifestDigest,
            recovered.ManifestDigest);
        Assert.Empty(PublicationStagingFiles(workspace.Root));
        Assert.True(File.Exists(fixture.Paths.PreparedDataPath));
        Assert.True(File.Exists(fixture.Paths.CheckpointPath));
    }

    [Theory]
    [InlineData(
        nameof(CsvExportPublicationFaultPoint
            .BeforeManifestNamespaceCommit))]
    [InlineData(
        nameof(CsvExportPublicationFaultPoint
            .AfterManifestNamespaceCommitBeforeResult))]
    public async Task ExactCsvOnlyProcessCrash_ManifestBranchRetriesIdempotently(
        string faultPointName)
    {
        if (!OperatingSystem.IsWindows())
            return;

        CsvExportPublicationFaultPoint faultPoint =
            Enum.Parse<CsvExportPublicationFaultPoint>(
                faultPointName,
                ignoreCase: false);
        using var workspace = new TemporaryDirectory();
        PublicationFixture fixture =
            await PreparePublicationFixtureAsync(workspace);

        ProcessCrashResult csvOnlySetup =
            await CrashPublicationAtAsync(
                fixture,
                CsvExportPublicationFaultPoint
                    .AfterDataNamespaceCommitBeforeManifest,
                Cancellation);
        Assert.NotEqual(0, csvOnlySetup.ExitCode);
        Assert.Equal(
            fixture.PreparedDataBytes,
            await File.ReadAllBytesAsync(
                fixture.DestinationPath,
                Cancellation));
        Assert.False(File.Exists(fixture.ManifestPath));
        Assert.Empty(PublicationStagingFiles(workspace.Root));

        ProcessCrashResult crash = await CrashPublicationAtAsync(
            fixture,
            faultPoint,
            Cancellation);
        Assert.NotEqual(0, crash.ExitCode);
        Assert.Equal(
            $"CSV_PUBLICATION_REACHED|{faultPoint}",
            crash.Reached);

        bool manifestCommitted =
            faultPoint ==
            CsvExportPublicationFaultPoint
                .AfterManifestNamespaceCommitBeforeResult;
        Assert.Equal(
            fixture.PreparedDataBytes,
            await File.ReadAllBytesAsync(
                fixture.DestinationPath,
                Cancellation));
        Assert.Equal(
            manifestCommitted,
            File.Exists(fixture.ManifestPath));
        if (manifestCommitted)
        {
            Assert.Equal(
                fixture.Export.CanonicalManifestBytes,
                await File.ReadAllBytesAsync(
                    fixture.ManifestPath,
                    Cancellation));
            Assert.Empty(PublicationStagingFiles(workspace.Root));
        }
        else
        {
            string staging =
                Assert.Single(PublicationStagingFiles(workspace.Root));
            Assert.EndsWith(
                ".publish.manifest.next",
                staging,
                StringComparison.OrdinalIgnoreCase);
        }

        CsvExportPublicationResult recovered =
            await new CsvExportPreparedOutputPublisher()
                .PublishCompletedAsync(
                    PublicationRequest(fixture),
                    Cancellation);
        Assert.True(recovered.ReusedData);
        Assert.Equal(manifestCommitted, recovered.ReusedManifest);
        Assert.Equal(
            fixture.PreparedDataBytes,
            await File.ReadAllBytesAsync(
                fixture.DestinationPath,
                Cancellation));
        Assert.Equal(
            fixture.Export.CanonicalManifestBytes,
            await File.ReadAllBytesAsync(
                fixture.ManifestPath,
                Cancellation));
        Assert.Empty(PublicationStagingFiles(workspace.Root));

        CsvExportPublicationResult exactPairRetry =
            await new CsvExportPreparedOutputPublisher()
                .PublishCompletedAsync(
                    PublicationRequest(fixture),
                    Cancellation);
        Assert.True(exactPairRetry.ReusedData);
        Assert.True(exactPairRetry.ReusedManifest);
        Assert.Equal(
            fixture.Export.ManifestDigest,
            exactPairRetry.ManifestDigest);
    }

    private static async Task<CheckpointFixture>
        PrepareCheckpointFixtureAsync(TemporaryDirectory workspace)
    {
        CsvExportRow row = Row(
            17,
            DbValue.FromInteger(1),
            DbValue.FromText("alpha"));
        string destinationPath = workspace.PathFor("checkpoint.csv");
        CsvResumableExportRequest headerRequest = Request(
            destinationPath,
            (boundary, token) => RowsThenThrow(
                [row],
                countBeforeFailure: 0,
                boundary,
                token));
        await Assert.ThrowsAsync<InjectedSourceException>(
            () => new CsvStreamingExporter()
                .WriteResumableAsync(headerRequest, Cancellation)
                .AsTask());

        (
            _,
            CsvExportPreparedOutputPaths paths,
            _
        ) = CsvExportPreparedOutputLease.BindPaths(
            destinationPath,
            allowExistingDestination: true);
        CsvExportCheckpoint generationZero =
            ReadCheckpoint(paths.CheckpointPath);
        byte[] generationZeroBytes = await File.ReadAllBytesAsync(
            paths.CheckpointPath,
            Cancellation);
        byte[] headerBytes = await File.ReadAllBytesAsync(
            paths.PreparedDataPath,
            Cancellation);
        Assert.Equal(0, generationZero.Generation);
        Assert.Equal(0, generationZero.Progress.CompletedRowCount);
        Assert.Equal(
            generationZero.Progress.DataPrefixByteLength,
            headerBytes.LongLength);

        string controlDestination = workspace.PathFor("control.csv");
        CsvResumableExportRequest generationOneRequest = Request(
            controlDestination,
            (boundary, token) => RowsThenThrow(
                [row],
                countBeforeFailure: 1,
                boundary,
                token));
        await Assert.ThrowsAsync<InjectedSourceException>(
            () => new CsvStreamingExporter()
                .WriteResumableAsync(
                    generationOneRequest,
                    Cancellation)
                .AsTask());
        (
            _,
            CsvExportPreparedOutputPaths controlPaths,
            _
        ) = CsvExportPreparedOutputLease.BindPaths(
            controlDestination,
            allowExistingDestination: true);
        byte[] generationOneBytes = await File.ReadAllBytesAsync(
            controlPaths.CheckpointPath,
            Cancellation);
        CsvExportCheckpoint generationOne =
            CsvExportCheckpointSerializer.Deserialize(
                generationOneBytes);
        byte[] headerAndRowBytes = await File.ReadAllBytesAsync(
            controlPaths.PreparedDataPath,
            Cancellation);

        Assert.Equal(1, generationOne.Generation);
        Assert.Equal(1, generationOne.Progress.CompletedRowCount);
        Assert.Equal(row.RowId, generationOne.Progress.LastCompletedRowId);
        Assert.Equal(
            generationZero.BindingDigest,
            generationOne.BindingDigest);
        Assert.True(headerAndRowBytes.AsSpan().StartsWith(headerBytes));

        byte[] rowBytes =
            headerAndRowBytes[headerBytes.Length..];
        Assert.NotEmpty(rowBytes);
        string nextCheckpointPath =
            workspace.PathFor("generation-one.checkpoint");
        string appendBytesPath =
            workspace.PathFor("generation-one.append");
        await File.WriteAllBytesAsync(
            nextCheckpointPath,
            generationOneBytes,
            Cancellation);
        await File.WriteAllBytesAsync(
            appendBytesPath,
            rowBytes,
            Cancellation);
        CsvResumableExportRequest recoveryRequest = Request(
            destinationPath,
            (boundary, token) => Rows([row], boundary, token));
        string referenceDestination =
            workspace.PathFor("checkpoint-reference.csv");
        CsvStreamingExportResult expectedExport =
            await new CsvStreamingExporter().WriteResumableAsync(
                Request(
                    referenceDestination,
                    (boundary, token) => Rows([row], boundary, token)),
                Cancellation);

        return new CheckpointFixture(
            destinationPath,
            paths,
            recoveryRequest,
            expectedExport,
            generationZeroBytes,
            generationOne,
            generationOneBytes,
            nextCheckpointPath,
            appendBytesPath,
            headerBytes,
            headerAndRowBytes);
    }

    private static async Task<FirstCheckpointFixture>
        PrepareFirstCheckpointFixtureAsync(TemporaryDirectory workspace)
    {
        CsvExportRow row = Row(
            17,
            DbValue.FromInteger(1),
            DbValue.FromText("alpha"));
        string controlDestination =
            workspace.PathFor("first-control.csv");
        CsvResumableExportRequest controlRequest = Request(
            controlDestination,
            (boundary, token) => RowsThenThrow(
                [row],
                countBeforeFailure: 0,
                boundary,
                token));
        await Assert.ThrowsAsync<InjectedSourceException>(
            () => new CsvStreamingExporter()
                .WriteResumableAsync(controlRequest, Cancellation)
                .AsTask());
        (
            _,
            CsvExportPreparedOutputPaths controlPaths,
            _
        ) = CsvExportPreparedOutputLease.BindPaths(
            controlDestination,
            allowExistingDestination: true);
        byte[] generationZeroBytes = await File.ReadAllBytesAsync(
            controlPaths.CheckpointPath,
            Cancellation);
        CsvExportCheckpoint generationZero =
            CsvExportCheckpointSerializer.Deserialize(
                generationZeroBytes);
        byte[] headerBytes = await File.ReadAllBytesAsync(
            controlPaths.PreparedDataPath,
            Cancellation);
        Assert.Equal(0, generationZero.Generation);
        Assert.Equal(CsvExportCheckpointPhase.Writing, generationZero.Phase);
        Assert.Equal(0, generationZero.Progress.CompletedRowCount);

        string destinationPath =
            workspace.PathFor("first-checkpoint.csv");
        (
            _,
            CsvExportPreparedOutputPaths paths,
            _
        ) = CsvExportPreparedOutputLease.BindPaths(
            destinationPath,
            allowExistingDestination: true);
        Assert.False(File.Exists(paths.PreparedDataPath));
        Assert.False(File.Exists(paths.CheckpointPath));
        Assert.False(File.Exists(paths.PendingCheckpointPath));

        string nextCheckpointPath =
            workspace.PathFor("first-generation-zero.checkpoint");
        string appendBytesPath =
            workspace.PathFor("first-generation-zero.append");
        await File.WriteAllBytesAsync(
            nextCheckpointPath,
            generationZeroBytes,
            Cancellation);
        await File.WriteAllBytesAsync(
            appendBytesPath,
            headerBytes,
            Cancellation);
        CsvResumableExportRequest recoveryRequest = Request(
            destinationPath,
            (boundary, token) => Rows([row], boundary, token));
        string referenceDestination =
            workspace.PathFor("first-checkpoint-reference.csv");
        CsvStreamingExportResult expectedExport =
            await new CsvStreamingExporter().WriteResumableAsync(
                Request(
                    referenceDestination,
                    (boundary, token) => Rows([row], boundary, token)),
                Cancellation);
        (
            _,
            CsvExportPreparedOutputPaths referencePaths,
            _
        ) = CsvExportPreparedOutputLease.BindPaths(
            referenceDestination,
            allowExistingDestination: true);
        byte[] expectedDataBytes = await File.ReadAllBytesAsync(
            referencePaths.PreparedDataPath,
            Cancellation);
        Assert.Equal(
            "id,note\r\n1,alpha\r\n",
            Encoding.UTF8.GetString(expectedDataBytes));

        return new FirstCheckpointFixture(
            destinationPath,
            paths,
            recoveryRequest,
            expectedExport,
            generationZero,
            generationZeroBytes,
            nextCheckpointPath,
            appendBytesPath,
            headerBytes,
            expectedDataBytes);
    }

    private static async Task<TerminalCheckpointFixture>
        PrepareTerminalCheckpointFixtureAsync(
            TemporaryDirectory workspace)
    {
        CsvExportRow row = Row(
            17,
            DbValue.FromInteger(1),
            DbValue.FromText("alpha"));
        string destinationPath =
            workspace.PathFor("terminal-checkpoint.csv");
        CsvResumableExportRequest interruptedRequest = Request(
            destinationPath,
            (boundary, token) => RowsThenThrow(
                [row],
                countBeforeFailure: 1,
                boundary,
                token));
        await Assert.ThrowsAsync<InjectedSourceException>(
            () => new CsvStreamingExporter()
                .WriteResumableAsync(
                    interruptedRequest,
                    Cancellation)
                .AsTask());
        (
            _,
            CsvExportPreparedOutputPaths paths,
            _
        ) = CsvExportPreparedOutputLease.BindPaths(
            destinationPath,
            allowExistingDestination: true);
        byte[] writingCheckpointBytes =
            await File.ReadAllBytesAsync(
                paths.CheckpointPath,
                Cancellation);
        CsvExportCheckpoint writingCheckpoint =
            CsvExportCheckpointSerializer.Deserialize(
                writingCheckpointBytes);
        byte[] writingDataBytes = await File.ReadAllBytesAsync(
            paths.PreparedDataPath,
            Cancellation);
        Assert.Equal(1, writingCheckpoint.Generation);
        Assert.Equal(
            CsvExportCheckpointPhase.Writing,
            writingCheckpoint.Phase);

        string controlDestination =
            workspace.PathFor("terminal-control.csv");
        CsvResumableExportRequest controlRequest = Request(
            controlDestination,
            (boundary, token) => Rows([row], boundary, token));
        CsvStreamingExportResult expectedExport =
            await new CsvStreamingExporter().WriteResumableAsync(
                controlRequest,
                Cancellation);
        (
            _,
            CsvExportPreparedOutputPaths controlPaths,
            _
        ) = CsvExportPreparedOutputLease.BindPaths(
            controlDestination,
            allowExistingDestination: true);
        byte[] terminalCheckpointBytes =
            await File.ReadAllBytesAsync(
                controlPaths.CheckpointPath,
                Cancellation);
        CsvExportCheckpoint terminalCheckpoint =
            CsvExportCheckpointSerializer.Deserialize(
                terminalCheckpointBytes);
        byte[] expectedDataBytes = await File.ReadAllBytesAsync(
            controlPaths.PreparedDataPath,
            Cancellation);
        Assert.Equal(2, terminalCheckpoint.Generation);
        Assert.Equal(
            CsvExportCheckpointPhase.DataComplete,
            terminalCheckpoint.Phase);
        Assert.NotNull(terminalCheckpoint.Completion);
        Assert.Equal(
            expectedExport.ManifestDigest,
            terminalCheckpoint.Completion.ManifestDigest);
        Assert.Equal(
            writingCheckpoint.BindingDigest,
            terminalCheckpoint.BindingDigest);
        Assert.Equal(
            writingCheckpoint.Progress,
            terminalCheckpoint.Progress);
        Assert.Equal(writingDataBytes, expectedDataBytes);

        string nextCheckpointPath =
            workspace.PathFor("terminal-generation.checkpoint");
        string emptyAppendBytesPath =
            workspace.PathFor("terminal-generation.append");
        await File.WriteAllBytesAsync(
            nextCheckpointPath,
            terminalCheckpointBytes,
            Cancellation);
        await File.WriteAllBytesAsync(
            emptyAppendBytesPath,
            [],
            Cancellation);
        CsvResumableExportRequest recoveryRequest = Request(
            destinationPath,
            (boundary, token) => Rows([row], boundary, token));

        return new TerminalCheckpointFixture(
            destinationPath,
            paths,
            recoveryRequest,
            writingCheckpointBytes,
            terminalCheckpointBytes,
            nextCheckpointPath,
            emptyAppendBytesPath,
            expectedExport,
            expectedDataBytes);
    }

    private static async Task<PublicationFixture>
        PreparePublicationFixtureAsync(TemporaryDirectory workspace)
    {
        string destinationPath = workspace.PathFor("publication.csv");
        string manifestPath =
            workspace.PathFor("publication.manifest.json");
        CsvExportRow[] rows =
        [
            Row(
                -7,
                DbValue.FromInteger(1),
                DbValue.FromText("alpha")),
            Row(
                4,
                DbValue.FromInteger(2),
                DbValue.FromText("beta")),
        ];
        CsvResumableExportRequest request = Request(
            destinationPath,
            (boundary, token) => Rows(rows, boundary, token));
        CsvStreamingExportResult export =
            await new CsvStreamingExporter().WriteResumableAsync(
                request,
                Cancellation);
        (
            _,
            CsvExportPreparedOutputPaths paths,
            _
        ) = CsvExportPreparedOutputLease.BindPaths(
            destinationPath,
            allowExistingDestination: true);
        byte[] preparedDataBytes = await File.ReadAllBytesAsync(
            paths.PreparedDataPath,
            Cancellation);

        Assert.Equal(
            "id,note\r\n1,alpha\r\n2,beta\r\n",
            Encoding.UTF8.GetString(preparedDataBytes));
        Assert.False(File.Exists(destinationPath));
        Assert.False(File.Exists(manifestPath));
        Assert.Empty(PublicationStagingFiles(workspace.Root));
        return new PublicationFixture(
            destinationPath,
            manifestPath,
            paths,
            export,
            preparedDataBytes);
    }

    private static async Task<ProcessCrashResult> CrashCheckpointAtAsync(
        CheckpointFixture fixture,
        CsvExportCheckpointFaultPoint faultPoint,
        CancellationToken cancellationToken) =>
        await CrashCheckpointAtAsync(
            fixture.DestinationPath,
            fixture.NextCheckpointPath,
            fixture.AppendBytesPath,
            faultPoint,
            cancellationToken);

    private static async Task<ProcessCrashResult> CrashCheckpointAtAsync(
        string destinationPath,
        string nextCheckpointPath,
        string appendBytesPath,
        CsvExportCheckpointFaultPoint faultPoint,
        CancellationToken cancellationToken) =>
        await CrashHarnessAtAsync(
            [
                "--csv-checkpoint-destination",
                destinationPath,
                "--csv-next-checkpoint",
                nextCheckpointPath,
                "--csv-append-bytes",
                appendBytesPath,
                "--csv-checkpoint-fault",
                faultPoint.ToString(),
            ],
            $"CSV_CHECKPOINT_REACHED|{faultPoint}",
            cancellationToken);

    private static async Task<ProcessCrashResult> CrashPublicationAtAsync(
        PublicationFixture fixture,
        CsvExportPublicationFaultPoint faultPoint,
        CancellationToken cancellationToken) =>
        await CrashHarnessAtAsync(
            [
                "--csv-publication-destination",
                fixture.DestinationPath,
                "--csv-publication-manifest",
                fixture.ManifestPath,
                "--csv-publication-manifest-digest",
                fixture.Export.ManifestDigest,
                "--csv-publication-fault",
                faultPoint.ToString(),
            ],
            $"CSV_PUBLICATION_REACHED|{faultPoint}",
            cancellationToken);

    private static async Task<ProcessCrashResult> CrashHarnessAtAsync(
        IReadOnlyList<string> arguments,
        string expectedReached,
        CancellationToken cancellationToken)
    {
        string pipeName =
            $"csharpdb-csv-export-crash-{Guid.NewGuid():N}";
        await using var pipe = new NamedPipeServerStream(
            pipeName,
            PipeDirection.InOut,
            maxNumberOfServerInstances: 1,
            PipeTransmissionMode.Byte,
            PipeOptions.Asynchronous);
        using Process process = CreateCrashHarnessProcess(
            arguments,
            pipeName);
        if (!process.Start())
        {
            throw new InvalidOperationException(
                "Failed to start the CSV export crash harness process.");
        }

        Task<string> stdoutTask =
            process.StandardOutput.ReadToEndAsync();
        Task<string> stderrTask =
            process.StandardError.ReadToEndAsync();
        bool killed = false;
        try
        {
            await pipe.WaitForConnectionAsync(cancellationToken)
                .WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
            using var reader = new StreamReader(pipe, leaveOpen: true);
            string ready = await ReadProtocolLineAsync(
                reader,
                cancellationToken);
            if (!string.Equals(ready, "READY", StringComparison.Ordinal))
                throw ProtocolFailure(ready);

            string reached = await ReadProtocolLineAsync(
                reader,
                cancellationToken);
            if (!string.Equals(
                    reached,
                    expectedReached,
                    StringComparison.Ordinal))
            {
                throw ProtocolFailure(reached);
            }

            process.Kill(entireProcessTree: true);
            killed = true;
            await process.WaitForExitAsync(cancellationToken)
                .WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
            return new ProcessCrashResult(
                process.ExitCode,
                reached,
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
                $"CSV export crash harness failed while waiting for " +
                $"'{expectedReached}'. ExitCode={process.ExitCode}; " +
                $"STDOUT={stdout}; STDERR={stderr}",
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
        IReadOnlyList<string> arguments,
        string pipeName)
    {
        string assemblyPath = FindCrashHarnessAssembly();
        string dotnetHost =
            Environment.GetEnvironmentVariable("DOTNET_HOST_PATH")
            is { Length: > 0 } path
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
        foreach (string argument in arguments)
            startInfo.ArgumentList.Add(argument);
        startInfo.ArgumentList.Add("--pipe");
        startInfo.ArgumentList.Add(pipeName);
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
            $"Could not locate the {BuildConfiguration} migration " +
            "crash harness assembly.");
    }

    private static async Task<string> ReadProtocolLineAsync(
        StreamReader reader,
        CancellationToken cancellationToken)
    {
        string? line = await reader.ReadLineAsync(cancellationToken)
            .AsTask()
            .WaitAsync(TimeSpan.FromSeconds(30), cancellationToken);
        if (line is null)
        {
            throw new EndOfStreamException(
                "CSV export crash harness disconnected before reaching " +
                "a fault point.");
        }
        return line;
    }

    private static Exception ProtocolFailure(string line)
    {
        if (line.StartsWith("ERROR|", StringComparison.Ordinal))
        {
            string[] parts = line.Split('|');
            string detail = parts.Length == 3
                ? Encoding.UTF8.GetString(
                    Convert.FromBase64String(parts[2]))
                : line;
            return new InvalidOperationException(
                $"CSV export crash harness reported " +
                $"{parts.ElementAtOrDefault(1)}: {detail}");
        }

        return new InvalidDataException(
            $"Unexpected CSV export crash harness protocol message " +
            $"'{line}'.");
    }

    private static CsvExportPublicationRequest PublicationRequest(
        PublicationFixture fixture) => new()
        {
            DestinationPath = fixture.DestinationPath,
            ManifestPath = fixture.ManifestPath,
            ExpectedManifestDigest = fixture.Export.ManifestDigest,
        };

    private static async Task CompleteAndAssertTerminalAsync(
        CsvResumableExportRequest recoveryRequest,
        CsvExportPreparedOutputPaths paths,
        byte[] expectedDataBytes,
        CsvStreamingExportResult? expectedExport)
    {
        CsvStreamingExportResult recovered =
            await new CsvStreamingExporter().WriteResumableAsync(
                recoveryRequest,
                Cancellation);
        byte[] activeCheckpointBytes =
            await File.ReadAllBytesAsync(
                paths.CheckpointPath,
                Cancellation);
        CsvExportCheckpoint terminal =
            CsvExportCheckpointSerializer.Deserialize(
                activeCheckpointBytes);

        Assert.Equal(
            CsvExportCheckpointPhase.DataComplete,
            terminal.Phase);
        Assert.Equal(2, terminal.Generation);
        Assert.NotNull(terminal.Completion);
        Assert.Equal(
            recovered.ManifestDigest,
            terminal.Completion.ManifestDigest);
        Assert.Equal(
            expectedDataBytes,
            await File.ReadAllBytesAsync(
                paths.PreparedDataPath,
                Cancellation));
        Assert.False(File.Exists(paths.PendingCheckpointPath));
        if (expectedExport is not null)
        {
            Assert.Equal(
                expectedExport.ManifestDigest,
                recovered.ManifestDigest);
            Assert.Equal(
                expectedExport.CanonicalManifestBytes,
                recovered.CanonicalManifestBytes);
        }

        await using CsvExportPreparedOutputLease fresh =
            await CsvExportPreparedOutputLease.OpenAsync(
                recoveryRequest.DestinationPath,
                terminal.Binding,
                Cancellation);
        Assert.Equal(
            CsvExportPreparedOutputState.Recovered,
            fresh.State);
        Assert.Equal(
            CsvExportCheckpointPhase.DataComplete,
            fresh.CurrentCheckpoint?.Phase);
        Assert.Equal(
            activeCheckpointBytes,
            await File.ReadAllBytesAsync(
                paths.CheckpointPath,
                Cancellation));
        Assert.Equal(
            expectedDataBytes,
            await ReadAllBytesAsync(
                fresh.DataStream,
                Cancellation));
    }

    private static CsvResumableExportRequest Request(
        string destinationPath,
        Func<
            long?,
            CancellationToken,
            IAsyncEnumerable<CsvExportRow>> openRows)
    {
        CsvExportSourceManifest source = Source('a');
        return new CsvResumableExportRequest
        {
            DestinationPath = destinationPath,
            Profile = CsvExportProfile.LosslessV1,
            Source = source,
            SourceSnapshotIdentity = SnapshotIdentity(source),
            Table = new TableSchema
            {
                TableName = "crash_recovery",
                Columns =
                [
                    new ColumnDefinition
                    {
                        Name = "id",
                        Type = DbType.Integer,
                        Nullable = false,
                    },
                    new ColumnDefinition
                    {
                        Name = "note",
                        Type = DbType.Text,
                        Nullable = true,
                    },
                ],
            },
            OpenRows = openRows,
            MaxDataBytes = 1L << 20,
            MaximumDecodedBlobBytes =
                CsvExportContracts.MaximumSupportedDecodedBlobBytes,
            CheckpointRowInterval = 1,
        };
    }

    private static CsvExportSourceManifest Source(char digestValue) => new()
    {
        Kind = CsvExportContracts.SourceKind,
        Version = "4.3.0",
        SnapshotByteLength = 4096,
        SnapshotDigest = new CsvExportHashManifest
        {
            Algorithm = CsvExportHashManifest.Sha256Algorithm,
            Value = new string(digestValue, 64),
        },
    };

    private static string SnapshotIdentity(
        CsvExportSourceManifest source) =>
        CsvExportCheckpointContracts.RetainedSnapshotIdentityPrefix +
        source.SnapshotByteLength.ToString(CultureInfo.InvariantCulture) +
        ":sha256:" +
        source.SnapshotDigest.Value;

    private static CsvExportCheckpoint ReadCheckpoint(string path) =>
        CsvExportCheckpointSerializer.Deserialize(
            File.ReadAllBytes(path));

    private static CsvExportRow Row(
        long rowId,
        params DbValue[] values) => new(rowId, values);

    private static async IAsyncEnumerable<CsvExportRow> Rows(
        IReadOnlyList<CsvExportRow> rows,
        long? afterRowIdExclusive,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (CsvExportRow row in rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (afterRowIdExclusive is null ||
                row.RowId > afterRowIdExclusive.Value)
            {
                yield return row;
                await Task.Yield();
            }
        }
    }

    private static async IAsyncEnumerable<CsvExportRow> RowsThenThrow(
        IReadOnlyList<CsvExportRow> rows,
        int countBeforeFailure,
        long? afterRowIdExclusive,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        int yielded = 0;
        foreach (CsvExportRow row in rows)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (afterRowIdExclusive is not null &&
                row.RowId <= afterRowIdExclusive.Value)
            {
                continue;
            }
            if (yielded == countBeforeFailure)
                break;

            yielded++;
            yield return row;
            await Task.Yield();
        }

        throw new InjectedSourceException();
    }

    private static async Task<byte[]> ReadAllBytesAsync(
        Stream stream,
        CancellationToken cancellationToken)
    {
        long position = stream.Position;
        try
        {
            stream.Position = 0;
            using var bytes = new MemoryStream();
            await stream.CopyToAsync(bytes, cancellationToken);
            return bytes.ToArray();
        }
        finally
        {
            stream.Position = position;
        }
    }

    private static string[] PublicationStagingFiles(string root) =>
        Directory.GetFiles(root)
            .Where(path =>
                Path.GetFileName(path).Contains(
                    ".publish.",
                    StringComparison.OrdinalIgnoreCase) &&
                path.EndsWith(
                    ".next",
                    StringComparison.OrdinalIgnoreCase))
            .Order(StringComparer.OrdinalIgnoreCase)
            .ToArray();

    private sealed record CheckpointFixture(
        string DestinationPath,
        CsvExportPreparedOutputPaths Paths,
        CsvResumableExportRequest RecoveryRequest,
        CsvStreamingExportResult ExpectedExport,
        byte[] GenerationZeroBytes,
        CsvExportCheckpoint GenerationOne,
        byte[] GenerationOneBytes,
        string NextCheckpointPath,
        string AppendBytesPath,
        byte[] HeaderBytes,
        byte[] HeaderAndRowBytes);

    private sealed record FirstCheckpointFixture(
        string DestinationPath,
        CsvExportPreparedOutputPaths Paths,
        CsvResumableExportRequest RecoveryRequest,
        CsvStreamingExportResult ExpectedExport,
        CsvExportCheckpoint GenerationZero,
        byte[] GenerationZeroBytes,
        string NextCheckpointPath,
        string AppendBytesPath,
        byte[] HeaderBytes,
        byte[] ExpectedDataBytes);

    private sealed record TerminalCheckpointFixture(
        string DestinationPath,
        CsvExportPreparedOutputPaths Paths,
        CsvResumableExportRequest RecoveryRequest,
        byte[] WritingCheckpointBytes,
        byte[] TerminalCheckpointBytes,
        string NextCheckpointPath,
        string EmptyAppendBytesPath,
        CsvStreamingExportResult ExpectedExport,
        byte[] ExpectedDataBytes);

    private sealed record PublicationFixture(
        string DestinationPath,
        string ManifestPath,
        CsvExportPreparedOutputPaths Paths,
        CsvStreamingExportResult Export,
        byte[] PreparedDataBytes);

    private sealed record ProcessCrashResult(
        int ExitCode,
        string Reached,
        string StandardOutput,
        string StandardError);

    private sealed class InjectedSourceException : Exception;

    private sealed class TemporaryDirectory : IDisposable
    {
        public TemporaryDirectory()
        {
            Root = Path.GetFullPath(Path.Combine(
                Path.GetTempPath(),
                "csharpdb-csv-export-crash-tests",
                Guid.NewGuid().ToString("N")));
            Directory.CreateDirectory(Root);
        }

        public string Root { get; }

        public string PathFor(string leaf) =>
            Path.Combine(Root, leaf);

        public void Dispose()
        {
            if (Directory.Exists(Root))
                Directory.Delete(Root, recursive: true);
        }
    }
}
