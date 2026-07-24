using System.Globalization;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Migration.Files.Json;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonResumableExporterTests
{
    private static readonly CancellationToken Cancellation =
        TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(JsonExportFraming.RootArray)]
    [InlineData(JsonExportFraming.Ndjson)]
    public async Task PublicWriteResumableAsync_UsesDurableLeaseAndRequalifiesCompletedSource(
        JsonExportFraming framing)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace =
            new TemporaryDirectory();
        string destination =
            workspace.PathFor(
                framing ==
                JsonExportFraming.RootArray
                    ? "items.json"
                    : "items.ndjson");
        JsonExportRow[] rows =
        [
            Row(-8, 1),
            Row(4, 2),
        ];
        var firstSource =
            new RecordingRowSource(rows);
        JsonResumableExportRequest firstRequest =
            Request(
                framing,
                firstSource.Open,
                checkpointRowInterval: 1) with
            {
                DestinationPath = destination,
            };

        JsonStreamingExportResult first =
            await new JsonStreamingExporter()
                .WriteResumableAsync(
                    firstRequest,
                    Cancellation);

        string parent =
            Path.GetDirectoryName(
                destination)!;
        string prepared =
            Assert.Single(
                Directory.GetFiles(
                    parent,
                    ".csharpdb-json-export-*.prepared"));
        _ = Assert.Single(
            Directory.GetFiles(
                parent,
                ".csharpdb-json-export-*.checkpoint"));
        Assert.False(
            File.Exists(destination));
        byte[] preparedBytes =
            await File.ReadAllBytesAsync(
                prepared,
                Cancellation);

        var reopenedSource =
            new RecordingRowSource(rows);
        JsonStreamingExportResult reopened =
            await new JsonStreamingExporter()
                .WriteResumableAsync(
                    firstRequest with
                    {
                        OpenRows =
                            reopenedSource.Open,
                    },
                    Cancellation);

        Assert.Equal(
            first.ManifestDigest,
            reopened.ManifestDigest);
        Assert.Equal(
            [null],
            reopenedSource.Boundaries);
        Assert.Equal(
            preparedBytes,
            await File.ReadAllBytesAsync(
                prepared,
                Cancellation));
        Assert.False(
            File.Exists(destination));
    }

    [Fact]
    public async Task PublicWriteResumableAndPublishAsync_ReusesExactPairAfterSourceRequalification()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace =
            new TemporaryDirectory();
        string destination =
            workspace.PathFor("items.json");
        string manifest =
            workspace.PathFor(
                "items.manifest.json");
        JsonExportRow[] rows =
        [
            Row(-8, 1),
            Row(4, 2),
        ];
        var firstSource =
            new RecordingRowSource(rows);
        JsonResumableExportRequest request =
            Request(
                JsonExportFraming.RootArray,
                firstSource.Open,
                checkpointRowInterval: 1) with
            {
                DestinationPath =
                    destination,
            };

        JsonExportPublicationResult first =
            await new JsonStreamingExporter()
                .WriteResumableAndPublishAsync(
                    request,
                    manifest,
                    Cancellation);

        Assert.False(first.ReusedData);
        Assert.False(first.ReusedManifest);
        Assert.Equal(
            first.CanonicalManifestBytes,
            await File.ReadAllBytesAsync(
                manifest,
                Cancellation));
        Assert.Equal(
            first.Manifest.Content.DataDigest.Value,
            PhysicalDigest(
                await File.ReadAllBytesAsync(
                    destination,
                    Cancellation)));

        var reopenedSource =
            new RecordingRowSource(rows);
        JsonExportPublicationResult reopened =
            await new JsonStreamingExporter()
                .WriteResumableAndPublishAsync(
                    request with
                    {
                        OpenRows =
                            reopenedSource.Open,
                    },
                    manifest,
                    Cancellation);

        Assert.True(reopened.ReusedData);
        Assert.True(reopened.ReusedManifest);
        Assert.Equal(
            first.ManifestDigest,
            reopened.ManifestDigest);
        Assert.Equal(
            [null],
            reopenedSource.Boundaries);
        Assert.Empty(
            Directory.GetFiles(
                Path.GetDirectoryName(
                    destination)!,
                ".csharpdb-json-export-*.publish.*.next"));
    }

    [Fact]
    public async Task PublicWriteResumableAndPublishAsync_RejectsJournalAliasBeforeSourceOrFiles()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace =
            new TemporaryDirectory();
        string destination =
            workspace.PathFor("alias.json");
        (
            _,
            JsonExportPreparedOutputPaths paths
        ) = JsonExportPreparedOutputLease
            .BindPaths(destination);
        var source =
            new RecordingRowSource(
                [Row(1, 1)]);
        JsonResumableExportRequest request =
            Request(
                JsonExportFraming.RootArray,
                source.Open) with
            {
                DestinationPath =
                    destination,
            };

        await Assert.ThrowsAsync<ArgumentException>(
            () => new JsonStreamingExporter()
                .WriteResumableAndPublishAsync(
                    request,
                    paths.PendingCheckpointPath,
                    Cancellation)
                .AsTask());

        Assert.Empty(source.Boundaries);
        Assert.Empty(
            Directory.EnumerateFileSystemEntries(
                Path.GetDirectoryName(
                    destination)!));
    }

    [Fact]
    public async Task PublicWriteResumableAndPublishAsync_RepairsDataOnlyAfterSourceRequalification()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace =
            new TemporaryDirectory();
        string destination =
            workspace.PathFor("data-only.json");
        string manifest =
            workspace.PathFor(
                "data-only.manifest.json");
        JsonExportRow[] rows =
        [
            Row(-8, 1),
            Row(4, 2),
        ];
        JsonResumableExportRequest request =
            Request(
                JsonExportFraming.RootArray,
                new RecordingRowSource(rows)
                    .Open,
                checkpointRowInterval: 1) with
            {
                DestinationPath =
                    destination,
            };
        JsonStreamingExportResult completed =
            await new JsonStreamingExporter()
                .WriteResumableAsync(
                    request,
                    Cancellation);
        var injector =
            new ThrowOncePublicationFaultInjector(
                JsonExportPublicationFaultPoint
                    .AfterDataNamespaceCommitBeforeManifest);

        await Assert.ThrowsAsync<
            InjectedPublicationException>(
            () => new JsonExportPublisher(
                    injector)
                .PublishCompletedAsync(
                    new JsonPreparedExportPublicationRequest
                    {
                        DestinationPath =
                            destination,
                        ManifestPath =
                            manifest,
                        ExpectedManifestDigest =
                            completed.ManifestDigest,
                    },
                    Cancellation)
                .AsTask());
        Assert.True(File.Exists(destination));
        Assert.False(File.Exists(manifest));

        var reopenedSource =
            new RecordingRowSource(rows);
        JsonExportPublicationResult recovered =
            await new JsonStreamingExporter()
                .WriteResumableAndPublishAsync(
                    request with
                    {
                        OpenRows =
                            reopenedSource.Open,
                    },
                    manifest,
                    Cancellation);

        Assert.True(recovered.ReusedData);
        Assert.False(recovered.ReusedManifest);
        Assert.Equal(
            [null],
            reopenedSource.Boundaries);
        Assert.Equal(
            recovered.CanonicalManifestBytes,
            await File.ReadAllBytesAsync(
                manifest,
                Cancellation));
    }

    [Theory]
    [InlineData(JsonExportFraming.RootArray)]
    [InlineData(JsonExportFraming.Ndjson)]
    public async Task FreshExportPersistsInitialPeriodicAndTerminalCheckpoints(
        JsonExportFraming framing)
    {
        JsonExportRow[] rows =
        [
            Row(-8, 1),
            Row(0, 2),
            Row(11, 3),
        ];
        var source = new RecordingRowSource(rows);
        await using var session =
            FakeSession.New();
        JsonResumableExportRequest request =
            Request(
                framing,
                source.Open,
                checkpointRowInterval: 2);

        JsonStreamingExportResult result =
            await new JsonStreamingExporter()
                .WriteResumableCoreAsync(
                    request,
                    session,
                    Cancellation);

        byte[] expected =
            Encoding.UTF8.GetBytes(
                framing ==
                    JsonExportFraming.RootArray
                    ? "[{\"id\":1},{\"id\":2},{\"id\":3}]\n"
                    : "{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n");
        Assert.Equal(
            expected,
            session.Bytes);
        Assert.Equal(
            PhysicalDigest(expected),
            result.Manifest.Content
                .DataDigest.Value);
        Assert.Equal(
            3,
            result.Manifest.Content.RowCount);
        Assert.Equal(
            [0L, 1L, 2L],
            session.Persisted.Select(
                static checkpoint =>
                    checkpoint.Generation));
        Assert.Equal(
            [
                JsonExportCheckpointPhase.Writing,
                JsonExportCheckpointPhase.Writing,
                JsonExportCheckpointPhase.DataComplete,
            ],
            session.Persisted.Select(
                static checkpoint =>
                    checkpoint.Phase));
        Assert.Equal(
            [0L, 2L, 3L],
            session.Persisted.Select(
                static checkpoint =>
                    checkpoint.Progress
                        .CompletedRowCount));
        Assert.Equal(
            framing ==
                JsonExportFraming.RootArray
                ? 1
                : 0,
            session.Persisted[0]
                .Progress
                .DataPrefixByteLength);
        Assert.Equal(
            result.ManifestDigest,
            session.Persisted[^1]
                .Completion!
                .ManifestDigest);
        Assert.Equal(
            [null],
            source.Boundaries);
    }

    [Theory]
    [InlineData(JsonExportFraming.RootArray, "[]\n")]
    [InlineData(JsonExportFraming.Ndjson, "")]
    public async Task EmptyExportPersistsGenerationZeroThenCompletion(
        JsonExportFraming framing,
        string expectedText)
    {
        var source =
            new RecordingRowSource([]);
        await using var session =
            FakeSession.New();

        JsonStreamingExportResult result =
            await new JsonStreamingExporter()
                .WriteResumableCoreAsync(
                    Request(
                        framing,
                        source.Open),
                    session,
                    Cancellation);

        Assert.Equal(
            expectedText,
            Encoding.UTF8.GetString(
                session.Bytes));
        Assert.Equal(
            [0L, 1L],
            session.Persisted.Select(
                static checkpoint =>
                    checkpoint.Generation));
        Assert.Equal(
            [
                JsonExportCheckpointPhase.Writing,
                JsonExportCheckpointPhase.DataComplete,
            ],
            session.Persisted.Select(
                static checkpoint =>
                    checkpoint.Phase));
        Assert.All(
            session.Persisted,
            static checkpoint =>
                Assert.Equal(
                    0,
                    checkpoint.Progress
                        .CompletedRowCount));
        Assert.Equal(
            0,
            result.Manifest.Content.RowCount);
    }

    [Fact]
    public async Task UncheckpointedPrivateBytesAreResetBeforeGenerationZero()
    {
        var source =
            new RecordingRowSource(
                [Row(4, 9)]);
        await using var session =
            FakeSession.Uncheckpointed(
                "untrusted-tail"u8.ToArray());

        await new JsonStreamingExporter()
            .WriteResumableCoreAsync(
                Request(
                    JsonExportFraming.RootArray,
                    source.Open),
                session,
                Cancellation);

        Assert.Equal(1, session.ResetCount);
        Assert.Equal(
            "[{\"id\":9}]\n",
            Encoding.UTF8.GetString(
                session.Bytes));
        Assert.Equal(
            0,
            session.Persisted[0]
                .Generation);
    }

    [Theory]
    [InlineData(JsonExportFraming.RootArray)]
    [InlineData(JsonExportFraming.Ndjson)]
    public async Task WritingCheckpointReplaysOnlyItsRowsThenContinuesAfterBoundary(
        JsonExportFraming framing)
    {
        JsonExportRow[] rows =
        [
            Row(long.MinValue, 1),
            Row(-2, 2),
            Row(7, 3),
            Row(long.MaxValue, 4),
        ];
        await using CompletedFixture fixture =
            await CreateFixtureAsync(
                framing,
                rows,
                checkpointRowInterval: 2);
        JsonExportCheckpoint writing =
            fixture.Session.Persisted[1];
        byte[] prefix =
            fixture.Session
                .PersistedBytes[1];
        var source =
            new RecordingRowSource(rows);
        await using var recovered =
            FakeSession.Recovered(
                writing,
                prefix);

        JsonStreamingExportResult resumed =
            await new JsonStreamingExporter()
                .WriteResumableCoreAsync(
                    Request(
                        framing,
                        source.Open,
                        checkpointRowInterval: 2),
                    recovered,
                    Cancellation);

        Assert.Equal(
            fixture.Session.Bytes,
            recovered.Bytes);
        Assert.Equal(
            fixture.Result.ManifestDigest,
            resumed.ManifestDigest);
        Assert.Equal(
            [null, -2L],
            source.Boundaries);
        Assert.Equal(
            [2, 2],
            source.YieldCounts);
        Assert.Equal(
            [2L, 3L],
            recovered.Persisted.Select(
                static checkpoint =>
                    checkpoint.Generation));
    }

    [Theory]
    [InlineData(JsonExportFraming.RootArray)]
    [InlineData(JsonExportFraming.Ndjson)]
    public async Task DataCompleteReopenRequalifiesSourceWithoutPersisting(
        JsonExportFraming framing)
    {
        JsonExportRow[] rows =
        [
            Row(-1, 1),
            Row(5, 2),
        ];
        await using CompletedFixture fixture =
            await CreateFixtureAsync(
                framing,
                rows,
                checkpointRowInterval: 1);
        JsonExportCheckpoint complete =
            fixture.Session.Persisted[^1];
        var source =
            new RecordingRowSource(rows);
        await using var recovered =
            FakeSession.Recovered(
                complete,
                fixture.Session.Bytes);

        JsonStreamingExportResult reopened =
            await new JsonStreamingExporter()
                .WriteResumableCoreAsync(
                    Request(
                        framing,
                        source.Open,
                        checkpointRowInterval: 1),
                    recovered,
                    Cancellation);

        Assert.Empty(recovered.Persisted);
        Assert.Equal(
            [null],
            source.Boundaries);
        Assert.Equal(
            fixture.Result.ManifestDigest,
            reopened.ManifestDigest);
        Assert.Equal(
            fixture.Session.Bytes,
            recovered.Bytes);
    }

    [Fact]
    public async Task RecoveredSessionRejectsExtraOrMissingPreparedBytes()
    {
        JsonExportRow[] rows =
        [
            Row(1, 1),
            Row(2, 2),
        ];
        await using CompletedFixture fixture =
            await CreateFixtureAsync(
                JsonExportFraming.RootArray,
                rows,
                checkpointRowInterval: 1);
        JsonExportCheckpoint writing =
            fixture.Session.Persisted[1];
        byte[] exact =
            fixture.Session
                .PersistedBytes[1];

        await using var extra =
            FakeSession.Recovered(
                writing,
                [.. exact, (byte)'x']);
        await Assert.ThrowsAsync<InvalidDataException>(
            () =>
                new JsonStreamingExporter()
                    .WriteResumableCoreAsync(
                        Request(
                            JsonExportFraming.RootArray,
                            new RecordingRowSource(rows)
                                .Open),
                        extra,
                        Cancellation)
                    .AsTask());

        await using var shortData =
            FakeSession.Recovered(
                writing,
                exact[..^1]);
        await Assert.ThrowsAsync<InvalidDataException>(
            () =>
                new JsonStreamingExporter()
                    .WriteResumableCoreAsync(
                        Request(
                            JsonExportFraming.RootArray,
                            new RecordingRowSource(rows)
                                .Open),
                        shortData,
                        Cancellation)
                    .AsTask());
    }

    [Fact]
    public async Task CancellationAfterDurableCheckpointLeavesThatCheckpointAuthoritative()
    {
        using var cancellation =
            CancellationTokenSource
                .CreateLinkedTokenSource(
                    Cancellation);
        var source =
            new RecordingRowSource(
                [Row(1, 1)]);
        await using var session =
            FakeSession.New();
        session.AfterPersist =
            call =>
            {
                if (call == 1)
                    cancellation.Cancel();
            };

        await Assert.ThrowsAnyAsync<
            OperationCanceledException>(
            () =>
                new JsonStreamingExporter()
                    .WriteResumableCoreAsync(
                        Request(
                            JsonExportFraming.RootArray,
                            source.Open),
                        session,
                        cancellation.Token)
                    .AsTask());

        JsonExportCheckpoint active =
            Assert.Single(
                session.Persisted);
        Assert.Equal(0, active.Generation);
        Assert.Equal(
            JsonExportCheckpointPhase.Writing,
            active.Phase);
        Assert.Equal(
            "[",
            Encoding.UTF8.GetString(
                session.Bytes));
    }

    [Fact]
    public async Task InvalidRequestAndDataLimitFailWithoutTerminalAuthority()
    {
        var source =
            new RecordingRowSource(
                [Row(1, 1)]);
        await using var invalidInterval =
            FakeSession.New();
        await Assert.ThrowsAsync<
            ArgumentOutOfRangeException>(
            () =>
                new JsonStreamingExporter()
                    .WriteResumableCoreAsync(
                        Request(
                            JsonExportFraming.RootArray,
                            source.Open) with
                        {
                            CheckpointRowInterval = 0,
                        },
                        invalidInterval,
                        Cancellation)
                    .AsTask());
        Assert.Empty(
            invalidInterval.Persisted);

        await using var tooSmall =
            FakeSession.New();
        await Assert.ThrowsAsync<
            InvalidDataException>(
            () =>
                new JsonStreamingExporter()
                    .WriteResumableCoreAsync(
                        Request(
                            JsonExportFraming.RootArray,
                            source.Open) with
                        {
                            MaxDataBytes = 3,
                        },
                        tooSmall,
                        Cancellation)
                    .AsTask());
        Assert.DoesNotContain(
            tooSmall.Persisted,
            static checkpoint =>
                checkpoint.Phase ==
                JsonExportCheckpointPhase
                    .DataComplete);
    }

    private static async Task<CompletedFixture>
        CreateFixtureAsync(
        JsonExportFraming framing,
        IReadOnlyList<JsonExportRow> rows,
        long checkpointRowInterval)
    {
        var source =
            new RecordingRowSource(rows);
        var session =
            FakeSession.New();
        JsonStreamingExportResult result =
            await new JsonStreamingExporter()
                .WriteResumableCoreAsync(
                    Request(
                        framing,
                        source.Open,
                        checkpointRowInterval),
                    session,
                    Cancellation);
        return new CompletedFixture(
            result,
            session);
    }

    private static JsonResumableExportRequest
        Request(
        JsonExportFraming framing,
        Func<
            long?,
            CancellationToken,
            IAsyncEnumerable<JsonExportRow>>
            openRows,
        long checkpointRowInterval = 10_000)
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
                @"C:\qualified\future.json",
            Profile =
                JsonExportProfile.LosslessV1,
            Framing = framing,
            Source = source,
            SourceSnapshotIdentity =
                JsonExportCheckpointContracts
                    .RetainedSnapshotIdentityPrefix +
                source.SnapshotByteLength
                    .ToString(
                        CultureInfo.InvariantCulture) +
                ":sha256:" +
                source.SnapshotDigest.Value,
            Table = new TableSchema
            {
                TableName = "items",
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
            CheckpointRowInterval =
                checkpointRowInterval,
        };
    }

    private static JsonExportRow Row(
        long rowId,
        long value) =>
        new(
            rowId,
            new[]
            {
                DbValue.FromInteger(value),
            });

    private static JsonExportHashManifest Hash(
        char value) =>
        new()
        {
            Algorithm =
                JsonExportHashManifest
                    .Sha256Algorithm,
            Value = new string(value, 64),
        };

    private static string PhysicalDigest(
        ReadOnlySpan<byte> bytes) =>
        Convert.ToHexString(
                SHA256.HashData(bytes))
            .ToLowerInvariant();

    private sealed record CompletedFixture(
        JsonStreamingExportResult Result,
        FakeSession Session) :
        IAsyncDisposable
    {
        public ValueTask DisposeAsync() =>
            Session.DisposeAsync();
    }

    private sealed class RecordingRowSource
    {
        private readonly IReadOnlyList<
            JsonExportRow> rows;

        internal RecordingRowSource(
            IReadOnlyList<JsonExportRow> rows)
        {
            this.rows = rows;
        }

        internal List<long?> Boundaries { get; } =
            [];

        internal List<int> YieldCounts { get; } =
            [];

        internal IAsyncEnumerable<JsonExportRow>
            Open(
            long? boundary,
            CancellationToken cancellationToken)
        {
            int call = Boundaries.Count;
            Boundaries.Add(boundary);
            YieldCounts.Add(0);
            return Enumerate(
                call,
                boundary,
                cancellationToken);
        }

        private async IAsyncEnumerable<
            JsonExportRow> Enumerate(
            int call,
            long? boundary,
            [EnumeratorCancellation]
            CancellationToken cancellationToken)
        {
            foreach (JsonExportRow row in rows)
            {
                cancellationToken
                    .ThrowIfCancellationRequested();
                if (boundary is long value &&
                    row.RowId <= value)
                {
                    continue;
                }

                YieldCounts[call]++;
                yield return row;
                await Task.Yield();
            }
        }
    }

    private sealed class
        ThrowOncePublicationFaultInjector(
        JsonExportPublicationFaultPoint point) :
        IJsonExportPublicationFaultInjector
    {
        private int thrown;

        public ValueTask InjectAsync(
            JsonExportPublicationFaultPoint observed,
            CancellationToken cancellationToken)
        {
            cancellationToken
                .ThrowIfCancellationRequested();
            if (observed == point &&
                Interlocked.Exchange(
                    ref thrown,
                    1) == 0)
            {
                throw new
                    InjectedPublicationException();
            }

            return ValueTask.CompletedTask;
        }
    }

    private sealed class
        InjectedPublicationException :
        Exception
    {
    }

    private sealed class TemporaryDirectory :
        IDisposable
    {
        internal TemporaryDirectory()
        {
            Root =
                Path.Combine(
                    Path.GetTempPath(),
                    "csharpdb-json-resumable-export-tests",
                    Guid.NewGuid()
                        .ToString("N"));
            Directory.CreateDirectory(
                Root);
        }

        private string Root { get; }

        internal string PathFor(
            string leaf) =>
            Path.Combine(
                Root,
                leaf);

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

    private sealed class FakeSession :
        IJsonExportPreparedOutputSession
    {
        private readonly MemoryStream data;
        private readonly JsonExportCheckpoint?
            openingCheckpoint;
        private int persistCalls;

        private FakeSession(
            JsonExportPreparedOutputState state,
            JsonExportCheckpoint? checkpoint,
            byte[] bytes)
        {
            State = state;
            openingCheckpoint = checkpoint;
            data = new MemoryStream();
            data.Write(bytes);
            data.Position = data.Length;
        }

        internal static FakeSession New() =>
            new(
                JsonExportPreparedOutputState.New,
                checkpoint: null,
                []);

        internal static FakeSession
            Uncheckpointed(
            byte[] bytes) =>
            new(
                JsonExportPreparedOutputState
                    .UncheckpointedData,
                checkpoint: null,
                bytes);

        internal static FakeSession Recovered(
            JsonExportCheckpoint checkpoint,
            byte[] bytes) =>
            new(
                JsonExportPreparedOutputState
                    .Recovered,
                checkpoint,
                bytes);

        public JsonExportPreparedOutputState
            State
        { get; }

        public JsonExportCheckpoint?
            CurrentCheckpoint =>
            openingCheckpoint;

        public Stream DataStream => data;

        internal byte[] Bytes =>
            data.ToArray();

        internal List<JsonExportCheckpoint>
            Persisted
        { get; } = [];

        internal List<byte[]>
            PersistedBytes
        { get; } = [];

        internal int ResetCount { get; private set; }

        internal Action<int>? AfterPersist { get; set; }

        public ValueTask ResetUncheckpointedAsync(
            CancellationToken cancellationToken)
        {
            cancellationToken
                .ThrowIfCancellationRequested();
            ResetCount++;
            data.SetLength(0);
            data.Position = 0;
            return ValueTask.CompletedTask;
        }

        public ValueTask PersistCheckpointAsync(
            JsonExportCheckpoint checkpoint,
            CancellationToken cancellationToken)
        {
            cancellationToken
                .ThrowIfCancellationRequested();
            _ = JsonExportCheckpointSerializer
                .ComputeCheckpointDigest(
                    checkpoint);
            JsonExportCheckpoint? previous =
                Persisted.Count == 0
                    ? openingCheckpoint
                    : Persisted[^1];
            if (previous is not null)
            {
                JsonExportCheckpointFraming
                    .ValidateTransition(
                        previous,
                        checkpoint);
            }
            Assert.Equal(
                checkpoint.Progress
                    .DataPrefixByteLength,
                data.Length);
            Assert.Equal(
                data.Length,
                data.Position);
            Assert.Equal(
                checkpoint.Progress
                    .DataPrefixDigest.Value,
                PhysicalDigest(
                    data.ToArray()));

            persistCalls++;
            Persisted.Add(checkpoint);
            PersistedBytes.Add(
                data.ToArray());

            // This is deliberately after the in-memory commit. It models the
            // lease cutoff where cancellation can no longer undo authority.
            AfterPersist?.Invoke(
                persistCalls);
            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            data.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
