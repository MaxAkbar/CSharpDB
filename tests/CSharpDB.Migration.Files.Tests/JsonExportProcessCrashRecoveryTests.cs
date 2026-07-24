using System.Diagnostics;
using System.Globalization;
using System.IO.Pipes;
using System.Runtime.CompilerServices;
using System.Text;
using CSharpDB.Migration.Files.Json;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Files.Tests;

[Collection(CsvExportCrashHarnessProcessCollection.Name)]
public sealed class JsonExportProcessCrashRecoveryTests
{
#if DEBUG
    private const string BuildConfiguration = "Debug";
#else
    private const string BuildConfiguration = "Release";
#endif

    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    public static TheoryData<JsonExportFraming, string>
        CheckpointCrashCases =>
        new()
        {
            {
                JsonExportFraming.RootArray,
                nameof(JsonExportCheckpointFaultPoint
                    .AfterDataDurablyFlushedBeforePendingCheckpoint)
            },
            {
                JsonExportFraming.RootArray,
                nameof(JsonExportCheckpointFaultPoint
                    .AfterPendingCheckpointDurablyFlushedBeforeActiveReplacement)
            },
            {
                JsonExportFraming.RootArray,
                nameof(JsonExportCheckpointFaultPoint
                    .AfterActiveCheckpointReplacedBeforeResult)
            },
            {
                JsonExportFraming.Ndjson,
                nameof(JsonExportCheckpointFaultPoint
                    .AfterDataDurablyFlushedBeforePendingCheckpoint)
            },
            {
                JsonExportFraming.Ndjson,
                nameof(JsonExportCheckpointFaultPoint
                    .AfterPendingCheckpointDurablyFlushedBeforeActiveReplacement)
            },
            {
                JsonExportFraming.Ndjson,
                nameof(JsonExportCheckpointFaultPoint
                    .AfterActiveCheckpointReplacedBeforeResult)
            },
        };

    public static TheoryData<JsonExportFraming, bool, string>
        PublicationCrashCases
    {
        get
        {
            var cases = new TheoryData<JsonExportFraming, bool, string>();
            foreach (JsonExportFraming framing in
                     new[]
                     {
                         JsonExportFraming.RootArray,
                         JsonExportFraming.Ndjson,
                     })
            {
                AddPublicationCases(cases, framing, empty: false);
            }
            AddPublicationCases(
                cases,
                JsonExportFraming.Ndjson,
                empty: true);
            return cases;
        }
    }

    [Theory]
    [MemberData(nameof(CheckpointCrashCases))]
    public async Task MidStreamCheckpointProcessCrash_PublicRerunPublishesExactPair(
        JsonExportFraming framing,
        string faultPointName)
    {
        if (!OperatingSystem.IsWindows())
            return;

        JsonExportCheckpointFaultPoint faultPoint =
            Enum.Parse<JsonExportCheckpointFaultPoint>(
                faultPointName,
                ignoreCase: false);
        using var workspace = new TemporaryDirectory();
        CheckpointFixture fixture =
            await PrepareCheckpointFixtureAsync(workspace, framing);

        ProcessCrashResult crash = await CrashCheckpointAtAsync(
            fixture,
            faultPoint,
            Cancellation);

        Assert.NotEqual(0, crash.ExitCode);
        Assert.Equal(
            $"JSON_CHECKPOINT_REACHED|{faultPoint}",
            crash.Reached);

        bool activeReplacementCommitted =
            faultPoint ==
            JsonExportCheckpointFaultPoint
                .AfterActiveCheckpointReplacedBeforeResult;
        bool pendingCheckpointDurable =
            faultPoint ==
            JsonExportCheckpointFaultPoint
                .AfterPendingCheckpointDurablyFlushedBeforeActiveReplacement;
        Assert.Equal(
            activeReplacementCommitted
                ? fixture.GenerationOneBytes
                : fixture.GenerationZeroBytes,
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
            fixture.GenerationOneDataBytes,
            await File.ReadAllBytesAsync(
                fixture.Paths.PreparedDataPath,
                Cancellation));
        Assert.False(File.Exists(fixture.DestinationPath));
        Assert.False(File.Exists(fixture.ManifestPath));

        JsonExportPublicationResult recovered =
            await new JsonStreamingExporter()
                .WriteResumableAndPublishAsync(
                    fixture.RecoveryRequest,
                    fixture.ManifestPath,
                    Cancellation);

        Assert.False(recovered.ReusedData);
        Assert.False(recovered.ReusedManifest);
        Assert.Equal(
            fixture.ExpectedExport.ManifestDigest,
            recovered.ManifestDigest);
        Assert.Equal(
            fixture.ExpectedExport.CanonicalManifestBytes,
            recovered.CanonicalManifestBytes);
        Assert.Equal(
            fixture.ExpectedDataBytes,
            await File.ReadAllBytesAsync(
                fixture.DestinationPath,
                Cancellation));
        Assert.Equal(
            fixture.ExpectedExport.CanonicalManifestBytes,
            await File.ReadAllBytesAsync(
                fixture.ManifestPath,
                Cancellation));
        Assert.False(
            File.Exists(fixture.ManifestPath) &&
            !File.Exists(fixture.DestinationPath));
        Assert.Equal(
            fixture.ExpectedDataBytes,
            await File.ReadAllBytesAsync(
                fixture.Paths.PreparedDataPath,
                Cancellation));
        JsonExportCheckpoint terminal =
            ReadCheckpoint(fixture.Paths.CheckpointPath);
        Assert.Equal(JsonExportCheckpointPhase.DataComplete, terminal.Phase);
        Assert.Equal(2, terminal.Generation);
        Assert.Equal(
            fixture.ExpectedExport.ManifestDigest,
            terminal.Completion?.ManifestDigest);
        Assert.False(File.Exists(fixture.Paths.PendingCheckpointPath));
        Assert.Empty(PublicationStagingFiles(workspace.Root));
    }

    [Theory]
    [MemberData(nameof(PublicationCrashCases))]
    public async Task PublicationProcessCrash_PublicRerunConvergesAndPreservesJournal(
        JsonExportFraming framing,
        bool empty,
        string faultPointName)
    {
        if (!OperatingSystem.IsWindows())
            return;

        JsonExportPublicationFaultPoint faultPoint =
            Enum.Parse<JsonExportPublicationFaultPoint>(
                faultPointName,
                ignoreCase: false);
        using var workspace = new TemporaryDirectory();
        PublicationFixture fixture =
            await PreparePublicationFixtureAsync(
                workspace,
                framing,
                empty);
        byte[] preparedBefore =
            await File.ReadAllBytesAsync(
                fixture.Paths.PreparedDataPath,
                Cancellation);
        byte[] checkpointBefore =
            await File.ReadAllBytesAsync(
                fixture.Paths.CheckpointPath,
                Cancellation);

        ProcessCrashResult crash = await CrashPublicationAtAsync(
            fixture,
            faultPoint,
            Cancellation);

        Assert.NotEqual(0, crash.ExitCode);
        Assert.Equal(
            $"JSON_PUBLICATION_REACHED|{faultPoint}",
            crash.Reached);

        bool dataCommitted =
            faultPoint is
                JsonExportPublicationFaultPoint
                    .AfterDataNamespaceCommitBeforeManifest or
                JsonExportPublicationFaultPoint
                    .BeforeManifestNamespaceCommit or
                JsonExportPublicationFaultPoint
                    .AfterManifestNamespaceCommitBeforeResult;
        bool manifestCommitted =
            faultPoint ==
            JsonExportPublicationFaultPoint
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
                fixture.DataBytes,
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
        Assert.Equal(
            preparedBefore,
            await File.ReadAllBytesAsync(
                fixture.Paths.PreparedDataPath,
                Cancellation));
        Assert.Equal(
            checkpointBefore,
            await File.ReadAllBytesAsync(
                fixture.Paths.CheckpointPath,
                Cancellation));
        AssertCrashStaging(
            workspace.Root,
            faultPoint);

        JsonExportPublicationResult recovered =
            await new JsonExportPublisher()
                .PublishCompletedAsync(
                    PublicationRequest(fixture),
                    Cancellation);

        Assert.Equal(dataCommitted, recovered.ReusedData);
        Assert.Equal(manifestCommitted, recovered.ReusedManifest);
        Assert.Equal(
            fixture.Export.ManifestDigest,
            recovered.ManifestDigest);
        Assert.Equal(
            fixture.Export.CanonicalManifestBytes,
            recovered.CanonicalManifestBytes);
        Assert.Equal(
            fixture.DataBytes,
            await File.ReadAllBytesAsync(
                fixture.DestinationPath,
                Cancellation));
        Assert.Equal(
            fixture.Export.CanonicalManifestBytes,
            await File.ReadAllBytesAsync(
                fixture.ManifestPath,
                Cancellation));
        Assert.False(
            File.Exists(fixture.ManifestPath) &&
            !File.Exists(fixture.DestinationPath));
        Assert.Equal(
            preparedBefore,
            await File.ReadAllBytesAsync(
                fixture.Paths.PreparedDataPath,
                Cancellation));
        Assert.Equal(
            checkpointBefore,
            await File.ReadAllBytesAsync(
                fixture.Paths.CheckpointPath,
                Cancellation));
        Assert.False(File.Exists(fixture.Paths.PendingCheckpointPath));
        Assert.Empty(PublicationStagingFiles(workspace.Root));

        JsonExportPublicationResult exactRetry =
            await new JsonExportPublisher()
                .PublishCompletedAsync(
                    PublicationRequest(fixture),
                    Cancellation);
        Assert.True(exactRetry.ReusedData);
        Assert.True(exactRetry.ReusedManifest);
    }

    private static void AddPublicationCases(
        TheoryData<JsonExportFraming, bool, string> cases,
        JsonExportFraming framing,
        bool empty)
    {
        cases.Add(
            framing,
            empty,
            nameof(JsonExportPublicationFaultPoint
                .BeforeDataNamespaceCommit));
        cases.Add(
            framing,
            empty,
            nameof(JsonExportPublicationFaultPoint
                .AfterManifestAbsenceCheckBeforeDataCommit));
        cases.Add(
            framing,
            empty,
            nameof(JsonExportPublicationFaultPoint
                .AfterDataNamespaceCommitBeforeManifest));
        cases.Add(
            framing,
            empty,
            nameof(JsonExportPublicationFaultPoint
                .BeforeManifestNamespaceCommit));
        cases.Add(
            framing,
            empty,
            nameof(JsonExportPublicationFaultPoint
                .AfterManifestNamespaceCommitBeforeResult));
    }

    private static async Task<CheckpointFixture>
        PrepareCheckpointFixtureAsync(
        TemporaryDirectory workspace,
        JsonExportFraming framing)
    {
        JsonExportRow row = Row(17, 1);
        string extension =
            framing == JsonExportFraming.Ndjson
                ? ".ndjson"
                : ".json";
        string destinationPath =
            workspace.PathFor("checkpoint" + extension);
        string manifestPath =
            workspace.PathFor("checkpoint.manifest.json");
        JsonResumableExportRequest generationZeroRequest =
            Request(
                destinationPath,
                framing,
                (boundary, token) => RowsThenThrow(
                    [row],
                    countBeforeFailure: 0,
                    boundary,
                    token));
        await Assert.ThrowsAsync<InjectedSourceException>(
            () => new JsonStreamingExporter()
                .WriteResumableAsync(
                    generationZeroRequest,
                    Cancellation)
                .AsTask());
        (
            _,
            JsonExportPreparedOutputPaths paths
        ) = JsonExportPreparedOutputLease.BindPaths(
            destinationPath);
        byte[] generationZeroBytes =
            await File.ReadAllBytesAsync(
                paths.CheckpointPath,
                Cancellation);
        JsonExportCheckpoint generationZero =
            JsonExportCheckpointSerializer.Deserialize(
                generationZeroBytes);
        byte[] generationZeroDataBytes =
            await File.ReadAllBytesAsync(
                paths.PreparedDataPath,
                Cancellation);
        Assert.Equal(0, generationZero.Generation);
        Assert.Equal(JsonExportCheckpointPhase.Writing, generationZero.Phase);
        Assert.Equal(0, generationZero.Progress.CompletedRowCount);
        Assert.Equal(
            framing == JsonExportFraming.RootArray
                ? "["u8.ToArray()
                : [],
            generationZeroDataBytes);

        string controlDestination =
            workspace.PathFor("checkpoint-control" + extension);
        JsonResumableExportRequest generationOneRequest =
            Request(
                controlDestination,
                framing,
                (boundary, token) => RowsThenThrow(
                    [row],
                    countBeforeFailure: 1,
                    boundary,
                    token));
        await Assert.ThrowsAsync<InjectedSourceException>(
            () => new JsonStreamingExporter()
                .WriteResumableAsync(
                    generationOneRequest,
                    Cancellation)
                .AsTask());
        (
            _,
            JsonExportPreparedOutputPaths controlPaths
        ) = JsonExportPreparedOutputLease.BindPaths(
            controlDestination);
        byte[] generationOneBytes =
            await File.ReadAllBytesAsync(
                controlPaths.CheckpointPath,
                Cancellation);
        JsonExportCheckpoint generationOne =
            JsonExportCheckpointSerializer.Deserialize(
                generationOneBytes);
        byte[] generationOneDataBytes =
            await File.ReadAllBytesAsync(
                controlPaths.PreparedDataPath,
                Cancellation);
        Assert.Equal(1, generationOne.Generation);
        Assert.Equal(JsonExportCheckpointPhase.Writing, generationOne.Phase);
        Assert.Equal(1, generationOne.Progress.CompletedRowCount);
        Assert.Equal(row.RowId, generationOne.Progress.LastCompletedRowId);
        Assert.Equal(
            generationZero.BindingDigest,
            generationOne.BindingDigest);
        Assert.True(
            generationOneDataBytes.AsSpan()
                .StartsWith(generationZeroDataBytes));

        byte[] appendBytes =
            generationOneDataBytes[generationZeroDataBytes.Length..];
        Assert.NotEmpty(appendBytes);
        string nextCheckpointPath =
            workspace.PathFor("generation-one.checkpoint.input");
        string appendBytesPath =
            workspace.PathFor("generation-one.data.input");
        await File.WriteAllBytesAsync(
            nextCheckpointPath,
            generationOneBytes,
            Cancellation);
        await File.WriteAllBytesAsync(
            appendBytesPath,
            appendBytes,
            Cancellation);

        JsonResumableExportRequest recoveryRequest =
            Request(
                destinationPath,
                framing,
                (boundary, token) => Rows(
                    [row],
                    boundary,
                    token));
        string referenceDestination =
            workspace.PathFor("checkpoint-reference" + extension);
        JsonStreamingExportResult expectedExport =
            await new JsonStreamingExporter()
                .WriteResumableAsync(
                    Request(
                        referenceDestination,
                        framing,
                        (boundary, token) => Rows(
                            [row],
                            boundary,
                            token)),
                    Cancellation);
        (
            _,
            JsonExportPreparedOutputPaths referencePaths
        ) = JsonExportPreparedOutputLease.BindPaths(
            referenceDestination);
        byte[] expectedDataBytes =
            await File.ReadAllBytesAsync(
                referencePaths.PreparedDataPath,
                Cancellation);
        Assert.Equal(
            framing == JsonExportFraming.RootArray
                ? "[{\"id\":1}]\n"
                : "{\"id\":1}\n",
            Encoding.UTF8.GetString(expectedDataBytes));

        return new CheckpointFixture(
            destinationPath,
            manifestPath,
            paths,
            recoveryRequest,
            expectedExport,
            generationZeroBytes,
            generationOneBytes,
            nextCheckpointPath,
            appendBytesPath,
            generationOneDataBytes,
            expectedDataBytes);
    }

    private static async Task<PublicationFixture>
        PreparePublicationFixtureAsync(
        TemporaryDirectory workspace,
        JsonExportFraming framing,
        bool empty)
    {
        string extension =
            framing == JsonExportFraming.Ndjson
                ? ".ndjson"
                : ".json";
        string stem =
            $"publication-{framing}-{empty}";
        string destinationPath =
            workspace.PathFor(stem + extension);
        string manifestPath =
            workspace.PathFor(stem + ".manifest.json");
        JsonExportRow[] rows =
            empty
                ? []
                :
                [
                    Row(-7, 1),
                    Row(4, 2),
                ];
        JsonResumableExportRequest request =
            Request(
                destinationPath,
                framing,
                (boundary, token) => Rows(
                    rows,
                    boundary,
                    token));
        JsonStreamingExportResult export =
            await new JsonStreamingExporter()
                .WriteResumableAsync(
                    request,
                    Cancellation);
        (
            _,
            JsonExportPreparedOutputPaths paths
        ) = JsonExportPreparedOutputLease.BindPaths(
            destinationPath);
        byte[] dataBytes =
            await File.ReadAllBytesAsync(
                paths.PreparedDataPath,
                Cancellation);
        string expectedText =
            empty
                ? string.Empty
                : framing == JsonExportFraming.RootArray
                    ? "[{\"id\":1},{\"id\":2}]\n"
                    : "{\"id\":1}\n{\"id\":2}\n";
        Assert.Equal(
            expectedText,
            Encoding.UTF8.GetString(dataBytes));
        Assert.Equal(
            JsonExportCheckpointPhase.DataComplete,
            ReadCheckpoint(paths.CheckpointPath).Phase);
        Assert.False(File.Exists(destinationPath));
        Assert.False(File.Exists(manifestPath));
        Assert.Empty(PublicationStagingFiles(workspace.Root));
        return new PublicationFixture(
            destinationPath,
            manifestPath,
            paths,
            export,
            dataBytes);
    }

    private static async Task<ProcessCrashResult>
        CrashCheckpointAtAsync(
        CheckpointFixture fixture,
        JsonExportCheckpointFaultPoint faultPoint,
        CancellationToken cancellationToken) =>
        await CrashHarnessAtAsync(
            [
                "--json-checkpoint-destination",
                fixture.DestinationPath,
                "--json-next-checkpoint",
                fixture.NextCheckpointPath,
                "--json-append-bytes",
                fixture.AppendBytesPath,
                "--json-checkpoint-fault",
                faultPoint.ToString(),
            ],
            $"JSON_CHECKPOINT_REACHED|{faultPoint}",
            cancellationToken);

    private static async Task<ProcessCrashResult>
        CrashPublicationAtAsync(
        PublicationFixture fixture,
        JsonExportPublicationFaultPoint faultPoint,
        CancellationToken cancellationToken) =>
        await CrashHarnessAtAsync(
            [
                "--json-publication-destination",
                fixture.DestinationPath,
                "--json-publication-manifest",
                fixture.ManifestPath,
                "--json-publication-manifest-digest",
                fixture.Export.ManifestDigest,
                "--json-publication-fault",
                faultPoint.ToString(),
            ],
            $"JSON_PUBLICATION_REACHED|{faultPoint}",
            cancellationToken);

    private static async Task<ProcessCrashResult>
        CrashHarnessAtAsync(
        IReadOnlyList<string> arguments,
        string expectedReached,
        CancellationToken cancellationToken)
    {
        string pipeName =
            $"csharpdb-json-export-crash-{Guid.NewGuid():N}";
        await using var pipe = new NamedPipeServerStream(
            pipeName,
            PipeDirection.InOut,
            maxNumberOfServerInstances: 1,
            PipeTransmissionMode.Byte,
            PipeOptions.Asynchronous);
        using Process process =
            CreateCrashHarnessProcess(
                arguments,
                pipeName);
        if (!process.Start())
        {
            throw new InvalidOperationException(
                "Failed to start the JSON export crash harness process.");
        }

        Task<string> stdoutTask =
            process.StandardOutput.ReadToEndAsync();
        Task<string> stderrTask =
            process.StandardError.ReadToEndAsync();
        bool killed = false;
        try
        {
            await pipe.WaitForConnectionAsync(cancellationToken)
                .WaitAsync(
                    TimeSpan.FromSeconds(30),
                    cancellationToken);
            using var reader =
                new StreamReader(
                    pipe,
                    leaveOpen: true);
            string ready =
                await ReadProtocolLineAsync(
                    reader,
                    cancellationToken);
            if (!string.Equals(
                    ready,
                    "READY",
                    StringComparison.Ordinal))
            {
                throw ProtocolFailure(ready);
            }

            string reached =
                await ReadProtocolLineAsync(
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
                .WaitAsync(
                    TimeSpan.FromSeconds(30),
                    cancellationToken);
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
            string stdout =
                await stdoutTask.ConfigureAwait(false);
            string stderr =
                await stderrTask.ConfigureAwait(false);
            throw new InvalidOperationException(
                $"JSON export crash harness failed while waiting for " +
                $"'{expectedReached}'. ExitCode={process.ExitCode}; " +
                $"STDOUT={stdout}; STDERR={stderr}",
                error);
        }
        finally
        {
            if (!killed &&
                !process.HasExited)
            {
                process.Kill(entireProcessTree: true);
                await process.WaitForExitAsync(
                        CancellationToken.None)
                    .WaitAsync(TimeSpan.FromSeconds(30));
            }
        }
    }

    private static Process CreateCrashHarnessProcess(
        IReadOnlyList<string> arguments,
        string pipeName)
    {
        string assemblyPath =
            FindCrashHarnessAssembly();
        string dotnetHost =
            Environment.GetEnvironmentVariable(
                "DOTNET_HOST_PATH")
            is { Length: > 0 } path
                ? path
                : "dotnet";
        var startInfo =
            new ProcessStartInfo(dotnetHost)
            {
                WorkingDirectory =
                    Path.GetDirectoryName(
                        assemblyPath)!,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };
        startInfo.ArgumentList.Add(
            assemblyPath);
        foreach (string argument in arguments)
            startInfo.ArgumentList.Add(argument);
        startInfo.ArgumentList.Add("--pipe");
        startInfo.ArgumentList.Add(pipeName);
        return new Process
        {
            StartInfo = startInfo,
        };
    }

    private static string FindCrashHarnessAssembly()
    {
        DirectoryInfo? current =
            new(AppContext.BaseDirectory);
        while (current is not null)
        {
            string candidate =
                Path.Combine(
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

    private static async Task<string>
        ReadProtocolLineAsync(
        StreamReader reader,
        CancellationToken cancellationToken)
    {
        string? line =
            await reader.ReadLineAsync(
                    cancellationToken)
                .AsTask()
                .WaitAsync(
                    TimeSpan.FromSeconds(30),
                    cancellationToken);
        if (line is null)
        {
            throw new EndOfStreamException(
                "JSON export crash harness disconnected before reaching " +
                "a fault point.");
        }
        return line;
    }

    private static Exception ProtocolFailure(
        string line)
    {
        if (line.StartsWith(
                "ERROR|",
                StringComparison.Ordinal))
        {
            string[] parts =
                line.Split('|');
            string detail =
                parts.Length == 3
                    ? Encoding.UTF8.GetString(
                        Convert.FromBase64String(
                            parts[2]))
                    : line;
            return new InvalidOperationException(
                $"JSON export crash harness reported " +
                $"{parts.ElementAtOrDefault(1)}: {detail}");
        }

        return new InvalidDataException(
            $"Unexpected JSON export crash harness protocol message " +
            $"'{line}'.");
    }

    private static JsonPreparedExportPublicationRequest
        PublicationRequest(
        PublicationFixture fixture) =>
        new()
        {
            DestinationPath =
                fixture.DestinationPath,
            ManifestPath =
                fixture.ManifestPath,
            ExpectedManifestDigest =
                fixture.Export.ManifestDigest,
        };

    private static JsonResumableExportRequest Request(
        string destinationPath,
        JsonExportFraming framing,
        Func<
            long?,
            CancellationToken,
            IAsyncEnumerable<JsonExportRow>> openRows)
    {
        JsonExportSourceManifest source =
            new()
            {
                Kind =
                    JsonExportContracts.SourceKind,
                Version = "4.3.0",
                SnapshotByteLength = 4_096,
                SnapshotDigest = Hash('a'),
            };
        return new JsonResumableExportRequest
        {
            DestinationPath =
                destinationPath,
            Profile =
                JsonExportProfile.LosslessV1,
            Framing = framing,
            Source = source,
            SourceSnapshotIdentity =
                JsonExportCheckpointContracts
                    .RetainedSnapshotIdentityPrefix +
                source.SnapshotByteLength.ToString(
                    CultureInfo.InvariantCulture) +
                ":sha256:" +
                source.SnapshotDigest.Value,
            Table =
                new TableSchema
                {
                    TableName =
                        "crash_recovery",
                    Columns =
                    [
                        new ColumnDefinition
                        {
                            Name = "id",
                            Type = DbType.Integer,
                            Nullable = false,
                        },
                    ],
                },
            OpenRows = openRows,
            MaxDataBytes = 1L << 20,
            MaximumDecodedBlobBytes =
                JsonExportContracts
                    .MaximumSupportedDecodedBlobBytes,
            CheckpointRowInterval = 1,
        };
    }

    private static JsonExportHashManifest Hash(
        char value) =>
        new()
        {
            Algorithm =
                JsonExportHashManifest.Sha256Algorithm,
            Value = new string(value, 64),
        };

    private static JsonExportCheckpoint ReadCheckpoint(
        string path) =>
        JsonExportCheckpointSerializer.Deserialize(
            File.ReadAllBytes(path));

    private static JsonExportRow Row(
        long rowId,
        long value) =>
        new(
            rowId,
            new[]
            {
                DbValue.FromInteger(value),
            });

    private static async IAsyncEnumerable<JsonExportRow>
        Rows(
        IReadOnlyList<JsonExportRow> rows,
        long? afterRowIdExclusive,
        [EnumeratorCancellation]
        CancellationToken cancellationToken)
    {
        foreach (JsonExportRow row in rows)
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

    private static async IAsyncEnumerable<JsonExportRow>
        RowsThenThrow(
        IReadOnlyList<JsonExportRow> rows,
        int countBeforeFailure,
        long? afterRowIdExclusive,
        [EnumeratorCancellation]
        CancellationToken cancellationToken)
    {
        int yielded = 0;
        foreach (JsonExportRow row in rows)
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

    private static void AssertCrashStaging(
        string root,
        JsonExportPublicationFaultPoint faultPoint)
    {
        string[] staging =
            PublicationStagingFiles(root);
        switch (faultPoint)
        {
            case JsonExportPublicationFaultPoint
                .BeforeDataNamespaceCommit:
            case JsonExportPublicationFaultPoint
                .AfterManifestAbsenceCheckBeforeDataCommit:
                Assert.Equal(2, staging.Length);
                Assert.Contains(
                    staging,
                    path => path.EndsWith(
                        ".publish.data.next",
                        StringComparison.OrdinalIgnoreCase));
                Assert.Contains(
                    staging,
                    path => path.EndsWith(
                        ".publish.manifest.next",
                        StringComparison.OrdinalIgnoreCase));
                break;
            case JsonExportPublicationFaultPoint
                .AfterDataNamespaceCommitBeforeManifest:
            case JsonExportPublicationFaultPoint
                .BeforeManifestNamespaceCommit:
                string manifestStage =
                    Assert.Single(staging);
                Assert.EndsWith(
                    ".publish.manifest.next",
                    manifestStage,
                    StringComparison.OrdinalIgnoreCase);
                break;
            case JsonExportPublicationFaultPoint
                .AfterManifestNamespaceCommitBeforeResult:
                Assert.Empty(staging);
                break;
            default:
                throw new ArgumentOutOfRangeException(
                    nameof(faultPoint),
                    faultPoint,
                    "Unknown JSON publication fault point.");
        }
    }

    private static string[] PublicationStagingFiles(
        string root) =>
        Directory.GetFileSystemEntries(
                root,
                ".csharpdb-json-export-*.publish.*.next")
            .Order(StringComparer.OrdinalIgnoreCase)
            .ToArray();

    private sealed record CheckpointFixture(
        string DestinationPath,
        string ManifestPath,
        JsonExportPreparedOutputPaths Paths,
        JsonResumableExportRequest RecoveryRequest,
        JsonStreamingExportResult ExpectedExport,
        byte[] GenerationZeroBytes,
        byte[] GenerationOneBytes,
        string NextCheckpointPath,
        string AppendBytesPath,
        byte[] GenerationOneDataBytes,
        byte[] ExpectedDataBytes);

    private sealed record PublicationFixture(
        string DestinationPath,
        string ManifestPath,
        JsonExportPreparedOutputPaths Paths,
        JsonStreamingExportResult Export,
        byte[] DataBytes);

    private sealed record ProcessCrashResult(
        int ExitCode,
        string Reached,
        string StandardOutput,
        string StandardError);

    private sealed class InjectedSourceException :
        Exception;

    private sealed class TemporaryDirectory :
        IDisposable
    {
        public TemporaryDirectory()
        {
            Root =
                Path.GetFullPath(
                    Path.Combine(
                        Path.GetTempPath(),
                        "csharpdb-json-export-crash-tests",
                        Guid.NewGuid().ToString("N")));
            Directory.CreateDirectory(Root);
        }

        public string Root { get; }

        public string PathFor(
            string leaf) =>
            Path.Combine(Root, leaf);

        public void Dispose()
        {
            if (Directory.Exists(Root))
            {
                Directory.Delete(
                    Root,
                    recursive: true);
            }
        }
    }
}
