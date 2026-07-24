using System.Runtime.CompilerServices;
using CSharpDB.Migration.Files.Json;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonResumableExporterReplayTests
{
    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("short")]
    [InlineData("extra")]
    [InlineData("wrong-position")]
    public async Task RecoveredStreamMustBeExactCheckpointPrefix(
        string failureKind)
    {
        WritingFixture fixture =
            await CreateWritingFixtureAsync(
                JsonExportFraming.RootArray);
        byte[] prepared =
            failureKind switch
            {
                "short" =>
                    fixture.PreparedBytes[..^1],
                "extra" =>
                    [.. fixture.PreparedBytes, 0x7f],
                "wrong-position" =>
                    fixture.PreparedBytes,
                _ => throw new
                    ArgumentOutOfRangeException(
                        nameof(failureKind)),
            };
        long position =
            failureKind == "wrong-position"
                ? 0
                : prepared.LongLength;
        int sourceOpens = 0;
        JsonResumableExportRequest request =
            fixture.Request with
            {
                OpenRows = (_, token) =>
                {
                    sourceOpens++;
                    return Rows(
                        fixture.Rows,
                        token);
                },
            };
        var session =
            RecoveredSession(
                fixture.Checkpoint,
                prepared,
                position);

        await Assert.ThrowsAsync<InvalidDataException>(
            () => new JsonStreamingExporter()
                .WriteResumableCoreAsync(
                    request,
                    session,
                    Cancellation)
                .AsTask());

        Assert.Equal(0, sourceOpens);
        Assert.Empty(session.Persisted);
        Assert.Same(
            fixture.Checkpoint,
            session.CurrentCheckpoint);
    }

    [Theory]
    [InlineData(false, true, true)]
    [InlineData(true, false, true)]
    [InlineData(true, true, false)]
    public async Task RecoveredStreamRequiresReadWriteAndSeek(
        bool canRead,
        bool canWrite,
        bool canSeek)
    {
        WritingFixture fixture =
            await CreateWritingFixtureAsync(
                JsonExportFraming.Ndjson);
        var stream =
            new CapabilityMemoryStream(
                fixture.PreparedBytes,
                canRead,
                canWrite,
                canSeek);
        var session =
            new TestSession(
                JsonExportPreparedOutputState.Recovered,
                fixture.Checkpoint,
                stream);
        int sourceOpens = 0;

        await Assert.ThrowsAsync<InvalidDataException>(
            () => new JsonStreamingExporter()
                .WriteResumableCoreAsync(
                    fixture.Request with
                    {
                        OpenRows = (_, token) =>
                        {
                            sourceOpens++;
                            return Rows(
                                fixture.Rows,
                                token);
                        },
                    },
                    session,
                    Cancellation)
                .AsTask());

        Assert.Equal(0, sourceOpens);
        Assert.Empty(session.Persisted);
    }

    [Theory]
    [InlineData("max-data")]
    [InlineData("framing")]
    [InlineData("table")]
    [InlineData("source")]
    public async Task BindingMismatchFailsBeforeOpeningReplay(
        string driftKind)
    {
        WritingFixture fixture =
            await CreateWritingFixtureAsync(
                JsonExportFraming.RootArray);
        int sourceOpens = 0;
        JsonResumableExportRequest changed =
            driftKind switch
            {
                "max-data" =>
                    fixture.Request with
                    {
                        MaxDataBytes =
                            fixture.Request
                                .MaxDataBytes +
                            1,
                    },
                "framing" =>
                    fixture.Request with
                    {
                        Framing =
                            JsonExportFraming.Ndjson,
                    },
                "table" =>
                    fixture.Request with
                    {
                        Table = Schema(
                            "changed",
                            Column(
                                "id",
                                DbType.Integer,
                                nullable: false),
                            Column(
                                "note",
                                DbType.Text),
                            Column(
                                "other",
                                DbType.Integer)),
                    },
                "source" =>
                    ChangeSource(
                        fixture.Request),
                _ => throw new
                    ArgumentOutOfRangeException(
                        nameof(driftKind)),
            };
        changed =
            changed with
            {
                OpenRows = (_, token) =>
                {
                    sourceOpens++;
                    return Rows(
                        fixture.Rows,
                        token);
                },
            };
        var session =
            RecoveredSession(
                fixture.Checkpoint,
                fixture.PreparedBytes);

        await Assert.ThrowsAsync<InvalidDataException>(
            () => new JsonStreamingExporter()
                .WriteResumableCoreAsync(
                    changed,
                    session,
                    Cancellation)
                .AsTask());

        Assert.Equal(0, sourceOpens);
        Assert.Empty(session.Persisted);
    }

    [Theory]
    [InlineData("fewer")]
    [InlineData("logical-drift")]
    [InlineData("decreasing-ids")]
    public async Task InvalidReplayCannotReachContinuationOrPersist(
        string failureKind)
    {
        WritingFixture fixture =
            await CreateWritingFixtureAsync(
                JsonExportFraming.RootArray);
        JsonExportRow[] replay =
            failureKind switch
            {
                "fewer" =>
                    [fixture.Rows[0]],
                "logical-drift" =>
                [
                    fixture.Rows[0],
                    Row(
                        fixture.Rows[1].RowId,
                        DbValue.FromInteger(2),
                        DbValue.FromText(
                            "changed")),
                ],
                "decreasing-ids" =>
                [
                    fixture.Rows[0],
                    Row(
                        fixture.Rows[0].RowId -
                            1,
                        DbValue.FromInteger(2),
                        DbValue.FromText(
                            "second")),
                ],
                _ => throw new
                    ArgumentOutOfRangeException(
                        nameof(failureKind)),
            };
        var boundaries =
            new List<long?>();
        JsonResumableExportRequest request =
            fixture.Request with
            {
                OpenRows = (boundary, token) =>
                {
                    boundaries.Add(boundary);
                    if (boundary is not null)
                    {
                        throw new InvalidOperationException(
                            "Continuation opened after invalid replay.");
                    }
                    return Rows(
                        replay,
                        token);
                },
            };
        var session =
            RecoveredSession(
                fixture.Checkpoint,
                fixture.PreparedBytes);

        await Assert.ThrowsAsync<InvalidDataException>(
            () => new JsonStreamingExporter()
                .WriteResumableCoreAsync(
                    request,
                    session,
                    Cancellation)
                .AsTask());

        Assert.Equal(
            new long?[] { null },
            boundaries);
        Assert.Empty(session.Persisted);
    }

    [Fact]
    public async Task ReplayLastIdMustMatchCheckpointBoundary()
    {
        WritingFixture fixture =
            await CreateWritingFixtureAsync(
                JsonExportFraming.Ndjson);
        JsonExportCheckpoint changed =
            fixture.Checkpoint with
            {
                Progress =
                    fixture.Checkpoint.Progress with
                    {
                        LastCompletedRowId =
                            fixture.Checkpoint
                                .Progress
                                .LastCompletedRowId +
                            1,
                    },
            };
        var boundaries =
            new List<long?>();
        var session =
            RecoveredSession(
                changed,
                fixture.PreparedBytes);

        await Assert.ThrowsAsync<InvalidDataException>(
            () => new JsonStreamingExporter()
                .WriteResumableCoreAsync(
                    fixture.Request with
                    {
                        OpenRows = (boundary, token) =>
                        {
                            boundaries.Add(boundary);
                            return Rows(
                                fixture.Rows,
                                token);
                        },
                    },
                    session,
                    Cancellation)
                .AsTask());

        Assert.Equal(
            new long?[] { null },
            boundaries);
        Assert.Empty(session.Persisted);
    }

    [Fact]
    public async Task RootWritingReplayDoesNotProbeNextRow()
    {
        WritingFixture fixture =
            await CreateWritingFixtureAsync(
                JsonExportFraming.RootArray);
        bool replayTailProbed = false;
        var boundaries =
            new List<long?>();
        JsonResumableExportRequest request =
            fixture.Request with
            {
                OpenRows = (boundary, token) =>
                {
                    boundaries.Add(boundary);
                    return boundary is null
                        ? RowsThenFailIfProbed(
                            fixture.Rows[..2],
                            () =>
                                replayTailProbed =
                                    true,
                            token)
                        : Rows(
                            [],
                            token);
                },
            };
        var session =
            RecoveredSession(
                fixture.Checkpoint,
                fixture.PreparedBytes);

        JsonStreamingExportResult result =
            await new JsonStreamingExporter()
                .WriteResumableCoreAsync(
                    request,
                    session,
                    Cancellation);

        Assert.False(replayTailProbed);
        Assert.Equal(
            new long?[]
            {
                null,
                fixture.Checkpoint
                    .Progress
                    .LastCompletedRowId,
            },
            boundaries);
        Assert.Equal(
            2,
            result.Manifest.Content.RowCount);
        Assert.Single(session.Persisted);
        Assert.Equal(
            JsonExportCheckpointPhase.DataComplete,
            session.Persisted[0].Phase);
    }

    [Theory]
    [InlineData("extra-row")]
    [InlineData("post-boundary-failure")]
    public async Task DataCompleteReplayRequiresSourceEof(
        string failureKind)
    {
        CompletedFixture fixture =
            await CreateCompletedFixtureAsync(
                JsonExportFraming.Ndjson);
        IAsyncEnumerable<JsonExportRow> replay =
            failureKind switch
            {
                "extra-row" =>
                    Rows(
                        [
                            .. fixture.Rows,
                            Row(
                                11,
                                DbValue.FromInteger(3),
                                DbValue.FromText(
                                    "extra")),
                        ],
                        Cancellation),
                "post-boundary-failure" =>
                    RowsThenThrow(
                        fixture.Rows,
                        fixture.Rows.Length,
                        Cancellation),
                _ => throw new
                    ArgumentOutOfRangeException(
                        nameof(failureKind)),
            };
        int sourceOpens = 0;
        var session =
            RecoveredSession(
                fixture.Checkpoint,
                fixture.PreparedBytes);

        Exception? error =
            await Record.ExceptionAsync(
                () => new JsonStreamingExporter()
                    .WriteResumableCoreAsync(
                        fixture.Request with
                        {
                            OpenRows =
                                (boundary, _) =>
                                {
                                    sourceOpens++;
                                    Assert.Null(
                                        boundary);
                                    return replay;
                                },
                        },
                        session,
                        Cancellation)
                    .AsTask());

        Assert.NotNull(error);
        if (failureKind == "extra-row")
        {
            Assert.IsType<InvalidDataException>(
                error);
        }
        else
        {
            Assert.IsType<InjectedReplayException>(
                error);
        }
        Assert.Equal(1, sourceOpens);
        Assert.Empty(session.Persisted);
    }

    [Fact]
    public async Task RootDataCompletePhysicalBytesIncludeExactClose()
    {
        CompletedFixture fixture =
            await CreateCompletedFixtureAsync(
                JsonExportFraming.RootArray);
        byte[] tampered =
            fixture.PreparedBytes.ToArray();
        Assert.Equal(
            (byte)']',
            tampered[^2]);
        Assert.Equal(
            (byte)'\n',
            tampered[^1]);
        tampered[^2] = (byte)'}';
        var session =
            RecoveredSession(
                fixture.Checkpoint,
                tampered);

        InvalidDataException error =
            await Assert.ThrowsAsync<InvalidDataException>(
                () => new JsonStreamingExporter()
                    .WriteResumableCoreAsync(
                        fixture.Request,
                        session,
                        Cancellation)
                    .AsTask());

        Assert.Contains(
            "prepared JSON prefix",
            error.Message,
            StringComparison.OrdinalIgnoreCase);
        Assert.Empty(session.Persisted);
    }

    [Theory]
    [InlineData(JsonExportFraming.RootArray)]
    [InlineData(JsonExportFraming.Ndjson)]
    public async Task ContinuationMustStartStrictlyAfterBoundary(
        JsonExportFraming framing)
    {
        WritingFixture fixture =
            await CreateWritingFixtureAsync(
                framing);
        long boundary =
            fixture.Checkpoint.Progress
                .LastCompletedRowId!.Value;
        var boundaries =
            new List<long?>();
        JsonResumableExportRequest request =
            fixture.Request with
            {
                OpenRows = (requested, token) =>
                {
                    boundaries.Add(requested);
                    return requested is null
                        ? Rows(
                            fixture.Rows[..2],
                            token)
                        : Rows(
                            [
                                Row(
                                    boundary,
                                    DbValue
                                        .FromInteger(
                                            99),
                                    DbValue.FromText(
                                        "at-boundary")),
                            ],
                            token);
                },
            };
        var session =
            RecoveredSession(
                fixture.Checkpoint,
                fixture.PreparedBytes);

        await Assert.ThrowsAsync<InvalidDataException>(
            () => new JsonStreamingExporter()
                .WriteResumableCoreAsync(
                    request,
                    session,
                    Cancellation)
                .AsTask());

        Assert.Equal(
            new long?[] { null, boundary },
            boundaries);
        Assert.Empty(session.Persisted);
    }

    [Theory]
    [InlineData(JsonExportFraming.RootArray)]
    [InlineData(JsonExportFraming.Ndjson)]
    public async Task SeededPrefixHashRejectsSameLengthTamper(
        JsonExportFraming framing)
    {
        WritingFixture fixture =
            await CreateWritingFixtureAsync(
                framing);
        byte[] tampered =
            fixture.PreparedBytes.ToArray();
        int offset =
            Math.Clamp(
                tampered.Length / 2,
                1,
                tampered.Length - 2);
        tampered[offset] ^=
            0x01;
        var boundaries =
            new List<long?>();
        var session =
            RecoveredSession(
                fixture.Checkpoint,
                tampered);

        InvalidDataException error =
            await Assert.ThrowsAsync<InvalidDataException>(
                () => new JsonStreamingExporter()
                    .WriteResumableCoreAsync(
                        fixture.Request with
                        {
                            OpenRows =
                                (boundary, token) =>
                                {
                                    boundaries.Add(
                                        boundary);
                                    return Rows(
                                        fixture.Rows,
                                        token);
                                },
                        },
                        session,
                        Cancellation)
                    .AsTask());

        Assert.Contains(
            "prepared JSON prefix",
            error.Message,
            StringComparison.OrdinalIgnoreCase);
        Assert.Equal(
            new long?[] { null },
            boundaries);
        Assert.Empty(session.Persisted);
    }

    [Fact]
    public async Task ExhaustedGenerationCannotAdvanceAuthority()
    {
        WritingFixture fixture =
            await CreateWritingFixtureAsync(
                JsonExportFraming.RootArray);
        JsonExportCheckpoint exhausted =
            fixture.Checkpoint with
            {
                Generation = long.MaxValue,
            };
        var boundaries =
            new List<long?>();
        var session =
            RecoveredSession(
                exhausted,
                fixture.PreparedBytes);
        JsonResumableExportRequest request =
            fixture.Request with
            {
                CheckpointRowInterval = 1,
                OpenRows = (boundary, token) =>
                {
                    boundaries.Add(boundary);
                    return boundary is null
                        ? Rows(
                            fixture.Rows[..2],
                            token)
                        : Rows(
                            [fixture.Rows[2]],
                            token);
                },
            };

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => new JsonStreamingExporter()
                .WriteResumableCoreAsync(
                    request,
                    session,
                    Cancellation)
                .AsTask());

        Assert.Same(
            exhausted,
            session.CurrentCheckpoint);
        Assert.Empty(session.Persisted);
        Assert.True(
            session.DataStream.Length >
            exhausted.Progress
                .DataPrefixByteLength);
        Assert.Equal(
            new long?[]
            {
                null,
                exhausted.Progress
                    .LastCompletedRowId,
            },
            boundaries);
    }

    [Fact]
    public async Task CancellationBeforePersistLeavesPriorAuthority()
    {
        WritingFixture fixture =
            await CreateWritingFixtureAsync(
                JsonExportFraming.Ndjson);
        using var cancellation =
            new CancellationTokenSource();
        var session =
            RecoveredSession(
                fixture.Checkpoint,
                fixture.PreparedBytes);
        session.PersistAction =
            (_, _, token) =>
            {
                cancellation.Cancel();
                token.ThrowIfCancellationRequested();
                return ValueTask.CompletedTask;
            };
        JsonResumableExportRequest request =
            ContinuationRequest(
                fixture,
                cancellation.Token);

        await Assert.ThrowsAnyAsync<
            OperationCanceledException>(
            () => new JsonStreamingExporter()
                .WriteResumableCoreAsync(
                    request,
                    session,
                    cancellation.Token)
                .AsTask());

        Assert.Same(
            fixture.Checkpoint,
            session.CurrentCheckpoint);
        Assert.Empty(session.Persisted);
        Assert.Equal(1, session.PersistAttempts);
        Assert.True(
            session.DataStream.Length >
            fixture.Checkpoint.Progress
                .DataPrefixByteLength);
    }

    [Fact]
    public async Task CancellationAfterPersistRetainsNewAuthority()
    {
        WritingFixture fixture =
            await CreateWritingFixtureAsync(
                JsonExportFraming.Ndjson);
        using var cancellation =
            new CancellationTokenSource();
        var session =
            RecoveredSession(
                fixture.Checkpoint,
                fixture.PreparedBytes);
        session.PersistAction =
            (owner, checkpoint, _) =>
            {
                owner.Commit(checkpoint);
                cancellation.Cancel();
                return ValueTask.CompletedTask;
            };
        JsonResumableExportRequest request =
            fixture.Request with
            {
                CheckpointRowInterval = 1,
                OpenRows = (boundary, token) =>
                    boundary is null
                        ? Rows(
                            fixture.Rows[..2],
                            token)
                        : RowThenObserveCancellation(
                            fixture.Rows[2],
                            token),
            };

        await Assert.ThrowsAnyAsync<
            OperationCanceledException>(
            () => new JsonStreamingExporter()
                .WriteResumableCoreAsync(
                    request,
                    session,
                    cancellation.Token)
                .AsTask());

        JsonExportCheckpoint persisted =
            Assert.Single(session.Persisted);
        Assert.Same(
            persisted,
            session.CurrentCheckpoint);
        Assert.Equal(
            fixture.Checkpoint.Generation + 1,
            persisted.Generation);
        Assert.Equal(
            3,
            persisted.Progress.CompletedRowCount);
        Assert.Equal(
            fixture.Rows[2].RowId,
            persisted.Progress.LastCompletedRowId);
    }

    private static JsonResumableExportRequest
        ContinuationRequest(
        WritingFixture fixture,
        CancellationToken cancellationToken) =>
        fixture.Request with
        {
            CheckpointRowInterval = 1,
            OpenRows = (boundary, _) =>
                boundary is null
                    ? Rows(
                        fixture.Rows[..2],
                        cancellationToken)
                    : Rows(
                        [fixture.Rows[2]],
                        cancellationToken),
        };

    private static async Task<WritingFixture>
        CreateWritingFixtureAsync(
        JsonExportFraming framing)
    {
        JsonExportRow[] rows =
            StandardRows();
        JsonResumableExportRequest request =
            Request(
                framing,
                rows,
                checkpointRowInterval: 2);
        var session =
            new TestSession(
                JsonExportPreparedOutputState.New,
                checkpoint: null,
                new MemoryStream());
        JsonResumableExportRequest interrupted =
            request with
            {
                OpenRows = (_, token) =>
                    RowsThenThrow(
                        rows,
                        countBeforeFailure: 2,
                        token),
            };

        await Assert.ThrowsAsync<
            InjectedReplayException>(
            () => new JsonStreamingExporter()
                .WriteResumableCoreAsync(
                    interrupted,
                    session,
                    Cancellation)
                .AsTask());

        JsonExportCheckpoint checkpoint =
            session.CurrentCheckpoint
            ?? throw new InvalidOperationException(
                "Fixture did not persist a checkpoint.");
        Assert.Equal(
            JsonExportCheckpointPhase.Writing,
            checkpoint.Phase);
        Assert.Equal(
            2,
            checkpoint.Progress.CompletedRowCount);
        byte[] prepared =
            ReadBytes(session.DataStream);
        Assert.Equal(
            checkpoint.Progress
                .DataPrefixByteLength,
            prepared.LongLength);
        return new WritingFixture(
            request,
            rows,
            checkpoint,
            prepared);
    }

    private static async Task<CompletedFixture>
        CreateCompletedFixtureAsync(
        JsonExportFraming framing)
    {
        JsonExportRow[] rows =
            StandardRows()[..2];
        JsonResumableExportRequest request =
            Request(
                framing,
                rows,
                checkpointRowInterval: 2);
        var session =
            new TestSession(
                JsonExportPreparedOutputState.New,
                checkpoint: null,
                new MemoryStream());

        _ = await new JsonStreamingExporter()
            .WriteResumableCoreAsync(
                request,
                session,
                Cancellation);

        JsonExportCheckpoint checkpoint =
            session.CurrentCheckpoint
            ?? throw new InvalidOperationException(
                "Fixture did not persist a checkpoint.");
        Assert.Equal(
            JsonExportCheckpointPhase.DataComplete,
            checkpoint.Phase);
        return new CompletedFixture(
            request,
            rows,
            checkpoint,
            ReadBytes(session.DataStream));
    }

    private static TestSession RecoveredSession(
        JsonExportCheckpoint checkpoint,
        byte[] prepared,
        long? position = null)
    {
        var stream = new MemoryStream();
        stream.Write(prepared);
        stream.Position =
            position ??
            stream.Length;
        return new TestSession(
            JsonExportPreparedOutputState.Recovered,
            checkpoint,
            stream);
    }

    private static JsonResumableExportRequest Request(
        JsonExportFraming framing,
        IReadOnlyList<JsonExportRow> rows,
        long checkpointRowInterval)
    {
        JsonExportSourceManifest source =
            Source('a');
        return new JsonResumableExportRequest
        {
            DestinationPath =
                @"C:\qualified\future.json",
            Profile =
                JsonExportProfile.LosslessV1,
            Framing = framing,
            Source = source,
            SourceSnapshotIdentity =
                SnapshotIdentity(source),
            Table = Schema(
                "items",
                Column(
                    "id",
                    DbType.Integer,
                    nullable: false),
                Column(
                    "note",
                    DbType.Text)),
            OpenRows = (boundary, token) =>
                RowsAfter(
                    rows,
                    boundary,
                    token),
            MaxDataBytes = 1L << 20,
            MaximumDecodedBlobBytes =
                JsonExportContracts
                    .MaximumSupportedDecodedBlobBytes,
            CheckpointRowInterval =
                checkpointRowInterval,
        };
    }

    private static JsonResumableExportRequest
        ChangeSource(
        JsonResumableExportRequest request)
    {
        JsonExportSourceManifest changed =
            Source('b');
        return request with
        {
            Source = changed,
            SourceSnapshotIdentity =
                SnapshotIdentity(changed),
        };
    }

    private static JsonExportSourceManifest Source(
        char digestCharacter) =>
        new()
        {
            Kind = JsonExportContracts.SourceKind,
            Version = "4.3.0",
            SnapshotByteLength = 4_096,
            SnapshotDigest =
                new JsonExportHashManifest
                {
                    Algorithm =
                        JsonExportHashManifest
                            .Sha256Algorithm,
                    Value =
                        new string(
                            digestCharacter,
                            64),
                },
        };

    private static string SnapshotIdentity(
        JsonExportSourceManifest source) =>
        JsonExportCheckpointContracts
            .RetainedSnapshotIdentityPrefix +
        source.SnapshotByteLength +
        ":" +
        JsonExportHashManifest
            .Sha256Algorithm +
        ":" +
        source.SnapshotDigest.Value;

    private static JsonExportRow[] StandardRows() =>
    [
        Row(
            -5,
            DbValue.FromInteger(1),
            DbValue.FromText("first")),
        Row(
            3,
            DbValue.FromInteger(2),
            DbValue.FromText("second")),
        Row(
            8,
            DbValue.FromInteger(3),
            DbValue.FromText("third")),
        Row(
            13,
            DbValue.FromInteger(4),
            DbValue.FromText("fourth")),
    ];

    private static TableSchema Schema(
        string name,
        params ColumnDefinition[] columns) =>
        new()
        {
            TableName = name,
            Columns = columns,
        };

    private static ColumnDefinition Column(
        string name,
        DbType type,
        bool nullable = true) =>
        new()
        {
            Name = name,
            Type = type,
            Nullable = nullable,
        };

    private static JsonExportRow Row(
        long rowId,
        params DbValue[] values) =>
        new(
            rowId,
            values);

    private static byte[] ReadBytes(
        Stream stream)
    {
        long position = stream.Position;
        stream.Position = 0;
        using var copy =
            new MemoryStream();
        stream.CopyTo(copy);
        stream.Position = position;
        return copy.ToArray();
    }

    private static async IAsyncEnumerable<JsonExportRow>
        Rows(
        IReadOnlyList<JsonExportRow> rows,
        [EnumeratorCancellation]
        CancellationToken cancellationToken =
            default)
    {
        foreach (JsonExportRow row in rows)
        {
            cancellationToken
                .ThrowIfCancellationRequested();
            yield return row;
            await Task.Yield();
        }
    }

    private static async IAsyncEnumerable<JsonExportRow>
        RowsAfter(
        IReadOnlyList<JsonExportRow> rows,
        long? boundary,
        [EnumeratorCancellation]
        CancellationToken cancellationToken =
            default)
    {
        foreach (JsonExportRow row in rows)
        {
            cancellationToken
                .ThrowIfCancellationRequested();
            if (boundary is null ||
                row.RowId > boundary.Value)
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
        [EnumeratorCancellation]
        CancellationToken cancellationToken =
            default)
    {
        for (int index = 0;
             index < countBeforeFailure;
             index++)
        {
            cancellationToken
                .ThrowIfCancellationRequested();
            yield return rows[index];
            await Task.Yield();
        }

        throw new InjectedReplayException();
    }

    private static async IAsyncEnumerable<JsonExportRow>
        RowsThenFailIfProbed(
        IReadOnlyList<JsonExportRow> rows,
        Action onProbe,
        [EnumeratorCancellation]
        CancellationToken cancellationToken =
            default)
    {
        foreach (JsonExportRow row in rows)
        {
            cancellationToken
                .ThrowIfCancellationRequested();
            yield return row;
            await Task.Yield();
        }

        onProbe();
        throw new InjectedReplayException();
    }

    private static async IAsyncEnumerable<JsonExportRow>
        RowThenObserveCancellation(
        JsonExportRow row,
        [EnumeratorCancellation]
        CancellationToken cancellationToken =
            default)
    {
        cancellationToken
            .ThrowIfCancellationRequested();
        yield return row;
        await Task.Yield();
        cancellationToken
            .ThrowIfCancellationRequested();
    }

    private sealed record WritingFixture(
        JsonResumableExportRequest Request,
        JsonExportRow[] Rows,
        JsonExportCheckpoint Checkpoint,
        byte[] PreparedBytes);

    private sealed record CompletedFixture(
        JsonResumableExportRequest Request,
        JsonExportRow[] Rows,
        JsonExportCheckpoint Checkpoint,
        byte[] PreparedBytes);

    private sealed class InjectedReplayException :
        Exception;

    private sealed class TestSession :
        IJsonExportPreparedOutputSession
    {
        internal TestSession(
            JsonExportPreparedOutputState state,
            JsonExportCheckpoint? checkpoint,
            Stream dataStream)
        {
            State = state;
            CurrentCheckpoint = checkpoint;
            DataStream = dataStream;
        }

        public JsonExportPreparedOutputState State
        {
            get;
        }

        public JsonExportCheckpoint? CurrentCheckpoint
        {
            get;
            private set;
        }

        public Stream DataStream { get; }

        internal List<JsonExportCheckpoint>
            Persisted { get; } = [];

        internal int PersistAttempts
        {
            get;
            private set;
        }

        internal Func<
            TestSession,
            JsonExportCheckpoint,
            CancellationToken,
            ValueTask>? PersistAction
        {
            get;
            set;
        }

        public ValueTask ResetUncheckpointedAsync(
            CancellationToken cancellationToken)
        {
            cancellationToken
                .ThrowIfCancellationRequested();
            DataStream.SetLength(0);
            DataStream.Position = 0;
            return ValueTask.CompletedTask;
        }

        public ValueTask PersistCheckpointAsync(
            JsonExportCheckpoint checkpoint,
            CancellationToken cancellationToken)
        {
            PersistAttempts++;
            return PersistAction is null
                ? Commit(checkpoint)
                : PersistAction(
                    this,
                    checkpoint,
                    cancellationToken);
        }

        internal ValueTask Commit(
            JsonExportCheckpoint checkpoint)
        {
            Persisted.Add(checkpoint);
            CurrentCheckpoint = checkpoint;
            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsync() =>
            ValueTask.CompletedTask;
    }

    private sealed class CapabilityMemoryStream :
        MemoryStream
    {
        private bool canRead = true;
        private bool canWrite = true;
        private bool canSeek = true;

        internal CapabilityMemoryStream(
            byte[] bytes,
            bool canRead,
            bool canWrite,
            bool canSeek)
        {
            base.Write(bytes);
            Position = Length;
            this.canRead = canRead;
            this.canWrite = canWrite;
            this.canSeek = canSeek;
        }

        public override bool CanRead =>
            canRead;

        public override bool CanWrite =>
            canWrite;

        public override bool CanSeek =>
            canSeek;
    }
}
