using System.Globalization;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Migration.Files.Csv;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Files.Tests;

public sealed class CsvResumableExporterTests
{
    private static readonly CancellationToken Cancellation =
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task NonWindowsPlatform_IsExplicitlyUnsupported()
    {
        if (OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        TableSchema schema = Schema(
            "unsupported",
            Column("id", DbType.Integer, nullable: false));
        CsvResumableExportRequest request = Request(
            workspace.PathFor("unsupported.csv"),
            schema,
            (_, token) => Rows([], cancellationToken: token));

        await Assert.ThrowsAsync<PlatformNotSupportedException>(
            () => new CsvStreamingExporter()
                .WriteResumableAsync(request, Cancellation)
                .AsTask());
    }

    [Fact]
    public async Task NewEmptyExport_CheckpointsHeaderThenCompletesWithExactManifest()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        TableSchema schema = Schema(
            "empty",
            Column("id", DbType.Integer, nullable: false));
        string destinationPath = workspace.PathFor("empty.csv");
        CsvResumableExportRequest prototype = Request(
            destinationPath,
            schema,
            (_, token) => Rows([], cancellationToken: token));
        CsvExportPreparedOutputPaths paths =
            await CapturePathsAsync(prototype);
        CsvExportCheckpoint? headerCheckpoint = null;
        CsvResumableExportRequest request = prototype with
        {
            OpenRows = (boundary, token) =>
            {
                Assert.Null(boundary);
                headerCheckpoint = ReadCheckpoint(paths.CheckpointPath);
                return Rows([], cancellationToken: token);
            },
        };

        CsvStreamingExportResult actual =
            await new CsvStreamingExporter().WriteResumableAsync(
                request,
                Cancellation);
        (CsvStreamingExportResult expected, byte[] expectedBytes) =
            await RestartOnlyExportAsync(request, []);

        Assert.NotNull(headerCheckpoint);
        Assert.Equal(0, headerCheckpoint.Generation);
        Assert.Equal(
            CsvExportCheckpointPhase.Writing,
            headerCheckpoint.Phase);
        Assert.Equal(0, headerCheckpoint.Progress.CompletedRowCount);
        Assert.Equal("id\r\n", Encoding.UTF8.GetString(expectedBytes));
        Assert.Equal(
            expectedBytes,
            await File.ReadAllBytesAsync(
                paths.PreparedDataPath,
                Cancellation));
        AssertEquivalent(expected, actual);

        CsvExportCheckpoint completed =
            ReadCheckpoint(paths.CheckpointPath);
        Assert.Equal(1, completed.Generation);
        Assert.Equal(
            CsvExportCheckpointPhase.DataComplete,
            completed.Phase);
        Assert.Equal(0, completed.Progress.CompletedRowCount);
        Assert.Equal(
            actual.ManifestDigest,
            completed.Completion!.ManifestDigest);
        Assert.False(File.Exists(destinationPath));
    }

    [Fact]
    public async Task NonemptyPreparedDataWithoutCheckpoint_IsResetBeforeExport()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        TableSchema schema = Schema(
            "uncheckpointed",
            Column("id", DbType.Integer, nullable: false));
        CsvExportRow[] rows =
        [
            Row(-3, DbValue.FromInteger(10)),
            Row(8, DbValue.FromInteger(20)),
        ];
        string destinationPath = workspace.PathFor("uncheckpointed.csv");
        CsvResumableExportRequest request = Request(
            destinationPath,
            schema,
            (_, token) => Rows(rows, cancellationToken: token),
            checkpointRowInterval: 1);
        CsvExportPreparedOutputPaths paths =
            await CapturePathsAsync(request);
        byte[] uncheckpointedBytes =
            "not,authoritative\r\ncomplete,but,uncheckpointed\r\n"u8.ToArray();
        await File.WriteAllBytesAsync(
            paths.PreparedDataPath,
            uncheckpointedBytes,
            Cancellation);
        Assert.False(File.Exists(paths.CheckpointPath));

        CsvStreamingExportResult actual =
            await new CsvStreamingExporter().WriteResumableAsync(
                request,
                Cancellation);
        (CsvStreamingExportResult expected, byte[] expectedBytes) =
            await RestartOnlyExportAsync(request, rows);

        byte[] preparedBytes = await File.ReadAllBytesAsync(
            paths.PreparedDataPath,
            Cancellation);
        Assert.Equal(expectedBytes, preparedBytes);
        Assert.False(
            preparedBytes.AsSpan().StartsWith(uncheckpointedBytes));
        AssertEquivalent(expected, actual);
        Assert.Equal(
            CsvExportCheckpointPhase.DataComplete,
            ReadCheckpoint(paths.CheckpointPath).Phase);
        Assert.False(File.Exists(destinationPath));
    }

    [Fact]
    public async Task MultiRowExport_PersistsIntervalsAndMatchesRestartOnlyExporter()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        TableSchema schema = Schema(
            "orders",
            Column("id", DbType.Integer, nullable: false),
            Column("note", DbType.Text));
        CsvExportRow[] rows =
        [
            Row(-8, DbValue.FromInteger(1), DbValue.FromText("alpha")),
            Row(-2, DbValue.FromInteger(2), DbValue.FromText("beta")),
            Row(4, DbValue.FromInteger(3), DbValue.FromText("gamma")),
            Row(9, DbValue.FromInteger(4), DbValue.Null),
            Row(15, DbValue.FromInteger(5), DbValue.FromText("omega")),
        ];
        string destinationPath = workspace.PathFor("orders.csv");
        CsvResumableExportRequest prototype = Request(
            destinationPath,
            schema,
            (_, token) => Rows(rows, cancellationToken: token),
            checkpointRowInterval: 2);
        CsvExportPreparedOutputPaths paths =
            await CapturePathsAsync(prototype);
        var observed = new List<CsvExportCheckpoint>();
        CsvResumableExportRequest request = prototype with
        {
            OpenRows = (boundary, token) =>
            {
                Assert.Null(boundary);
                observed.Add(ReadCheckpoint(paths.CheckpointPath));
                return ObserveIntervalCheckpoints(
                    rows,
                    paths.CheckpointPath,
                    interval: 2,
                    observed,
                    token);
            },
        };

        CsvStreamingExportResult actual =
            await new CsvStreamingExporter().WriteResumableAsync(
                request,
                Cancellation);
        (CsvStreamingExportResult expected, byte[] expectedBytes) =
            await RestartOnlyExportAsync(request, rows);

        Assert.Equal([0L, 1L, 2L], observed.Select(
            static checkpoint => checkpoint.Generation));
        Assert.All(
            observed,
            static checkpoint => Assert.Equal(
                CsvExportCheckpointPhase.Writing,
                checkpoint.Phase));
        Assert.Equal([0L, 2L, 4L], observed.Select(
            static checkpoint =>
                checkpoint.Progress.CompletedRowCount));

        CsvExportCheckpoint completed =
            ReadCheckpoint(paths.CheckpointPath);
        Assert.Equal(3, completed.Generation);
        Assert.Equal(
            CsvExportCheckpointPhase.DataComplete,
            completed.Phase);
        Assert.Equal(rows.LongLength, completed.Progress.CompletedRowCount);
        Assert.Equal(rows[^1].RowId, completed.Progress.LastCompletedRowId);
        Assert.Equal(
            expectedBytes,
            await File.ReadAllBytesAsync(
                paths.PreparedDataPath,
                Cancellation));
        AssertEquivalent(expected, actual);
        Assert.False(File.Exists(destinationPath));
    }

    [Fact]
    public async Task ExactCheckpointIntervalAtEof_UsesDistinctDataCompleteGeneration()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        TableSchema schema = Schema(
            "exact_interval",
            Column("value", DbType.Integer, nullable: false));
        CsvExportRow[] rows =
        [
            Row(-8, DbValue.FromInteger(1)),
            Row(-2, DbValue.FromInteger(2)),
            Row(4, DbValue.FromInteger(3)),
            Row(9, DbValue.FromInteger(4)),
        ];
        CsvResumableExportRequest prototype = Request(
            workspace.PathFor("exact-interval.csv"),
            schema,
            (_, token) => Rows(rows, cancellationToken: token),
            checkpointRowInterval: 2);
        CsvExportPreparedOutputPaths paths =
            await CapturePathsAsync(prototype);
        var observed = new List<CsvExportCheckpoint>();
        CsvResumableExportRequest request = prototype with
        {
            OpenRows = (boundary, token) =>
            {
                Assert.Null(boundary);
                observed.Add(ReadCheckpoint(paths.CheckpointPath));
                return ObserveIntervalCheckpoints(
                    rows,
                    paths.CheckpointPath,
                    interval: 2,
                    observed,
                    token);
            },
        };

        CsvStreamingExportResult actual =
            await new CsvStreamingExporter().WriteResumableAsync(
                request,
                Cancellation);
        (CsvStreamingExportResult expected, byte[] expectedBytes) =
            await RestartOnlyExportAsync(request, rows);

        Assert.Equal([0L, 1L, 2L], observed.Select(
            static checkpoint => checkpoint.Generation));
        CsvExportCheckpoint finalWriting = observed[^1];
        Assert.Equal(
            CsvExportCheckpointPhase.Writing,
            finalWriting.Phase);
        Assert.Equal(rows.LongLength, finalWriting.Progress.CompletedRowCount);

        CsvExportCheckpoint completed =
            ReadCheckpoint(paths.CheckpointPath);
        Assert.Equal(3, completed.Generation);
        Assert.Equal(
            CsvExportCheckpointPhase.DataComplete,
            completed.Phase);
        Assert.Equal(finalWriting.Progress, completed.Progress);
        Assert.NotNull(completed.Completion);
        Assert.Equal(
            expectedBytes,
            await File.ReadAllBytesAsync(
                paths.PreparedDataPath,
                Cancellation));
        AssertEquivalent(expected, actual);
    }

    [Fact]
    public async Task InterruptedTail_ResumesWithReplayAndExclusiveContinuation()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        TableSchema schema = Schema(
            "resume",
            Column("id", DbType.Integer, nullable: false),
            Column("note", DbType.Text));
        CsvExportRow[] rows =
        [
            Row(-9, DbValue.FromInteger(1), DbValue.FromText("first")),
            Row(-2, DbValue.FromInteger(2), DbValue.FromText("second")),
            Row(4, DbValue.FromInteger(3), DbValue.FromText("third")),
            Row(11, DbValue.FromInteger(4), DbValue.FromText("fourth")),
        ];
        CsvResumableExportRequest request = Request(
            workspace.PathFor("resume.csv"),
            schema,
            (_, token) => Rows(rows, cancellationToken: token),
            checkpointRowInterval: 2);
        CsvExportPreparedOutputPaths paths =
            await CapturePathsAsync(request);

        CsvResumableExportRequest interrupted = request with
        {
            OpenRows = (_, token) => RowsThenThrow(
                rows,
                countBeforeFailure: 2,
                token),
        };
        await Assert.ThrowsAsync<InjectedExportException>(
            () => new CsvStreamingExporter()
                .WriteResumableAsync(interrupted, Cancellation)
                .AsTask());

        CsvExportCheckpoint durable =
            ReadCheckpoint(paths.CheckpointPath);
        Assert.Equal(1, durable.Generation);
        Assert.Equal(2, durable.Progress.CompletedRowCount);
        Assert.Equal(-2, durable.Progress.LastCompletedRowId);

        await using (var tail = new FileStream(
                         paths.PreparedDataPath,
                         FileMode.Append,
                         FileAccess.Write,
                         FileShare.None,
                         bufferSize: 4096,
                         FileOptions.WriteThrough))
        {
            await tail.WriteAsync("partial,row"u8.ToArray(), Cancellation);
            tail.Flush(flushToDisk: true);
        }

        var source = new TrackingRowSource(rows);
        CsvResumableExportRequest resumed = request with
        {
            OpenRows = source.OpenRows,
        };
        CsvStreamingExportResult actual =
            await new CsvStreamingExporter().WriteResumableAsync(
                resumed,
                Cancellation);
        (CsvStreamingExportResult expected, byte[] expectedBytes) =
            await RestartOnlyExportAsync(request, rows);

        Assert.Equal(
            new long?[] { null, -2 },
            source.Opens.Select(static open => open.Boundary));
        Assert.Equal(
            [-9L, -2L],
            source.Opens[0].YieldedRowIds);
        Assert.Equal(
            [4L, 11L],
            source.Opens[1].YieldedRowIds);
        Assert.Equal(1, source.MaximumConcurrentEnumerators);
        Assert.Equal(
            expectedBytes,
            await File.ReadAllBytesAsync(
                paths.PreparedDataPath,
                Cancellation));
        AssertEquivalent(expected, actual);
    }

    [Fact]
    public async Task CompleteRowPastCheckpoint_IsTruncatedAndReenumerated()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        TableSchema schema = Schema(
            "complete_tail",
            Column("id", DbType.Integer, nullable: false),
            Column("note", DbType.Text, nullable: false));
        CsvExportRow[] rows =
        [
            Row(-9, DbValue.FromInteger(1), DbValue.FromText("first")),
            Row(-2, DbValue.FromInteger(2), DbValue.FromText("second")),
            Row(4, DbValue.FromInteger(3), DbValue.FromText("third")),
            Row(11, DbValue.FromInteger(4), DbValue.FromText("fourth")),
        ];
        CsvResumableExportRequest request = Request(
            workspace.PathFor("complete-tail.csv"),
            schema,
            (_, token) => Rows(rows, cancellationToken: token),
            checkpointRowInterval: 2);
        CsvExportPreparedOutputPaths paths =
            await CapturePathsAsync(request);

        await CreateInterruptedCheckpointAsync(
            request,
            rows,
            countBeforeFailure: 3);

        CsvExportCheckpoint durable =
            ReadCheckpoint(paths.CheckpointPath);
        Assert.Equal(2, durable.Progress.CompletedRowCount);
        byte[] bytesWithCompleteTail = await File.ReadAllBytesAsync(
            paths.PreparedDataPath,
            Cancellation);
        Assert.True(
            bytesWithCompleteTail.LongLength >
            durable.Progress.DataPrefixByteLength);
        Assert.Equal(
            "\r\n"u8.ToArray(),
            bytesWithCompleteTail[^2..]);

        var source = new TrackingRowSource(rows);
        CsvStreamingExportResult actual =
            await new CsvStreamingExporter().WriteResumableAsync(
                request with { OpenRows = source.OpenRows },
                Cancellation);
        (CsvStreamingExportResult expected, byte[] expectedBytes) =
            await RestartOnlyExportAsync(request, rows);

        Assert.Equal(
            new long?[] { null, -2 },
            source.Opens.Select(static open => open.Boundary));
        Assert.Equal(
            [-9L, -2L],
            source.Opens[0].YieldedRowIds);
        Assert.Equal(
            [4L, 11L],
            source.Opens[1].YieldedRowIds);
        Assert.Equal(
            expectedBytes,
            await File.ReadAllBytesAsync(
                paths.PreparedDataPath,
                Cancellation));
        AssertEquivalent(expected, actual);
    }

    [Fact]
    public async Task RecoveredDataComplete_VerifiesSourceAndReturnsWithoutWrites()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        TableSchema schema = Schema(
            "complete",
            Column("id", DbType.Integer, nullable: false));
        CsvExportRow[] rows =
        [
            Row(-1, DbValue.FromInteger(10)),
            Row(5, DbValue.FromInteger(20)),
        ];
        CsvResumableExportRequest request = Request(
            workspace.PathFor("complete.csv"),
            schema,
            (_, token) => Rows(rows, cancellationToken: token),
            checkpointRowInterval: 1);
        CsvExportPreparedOutputPaths paths =
            await CapturePathsAsync(request);

        CsvStreamingExportResult first =
            await new CsvStreamingExporter().WriteResumableAsync(
                request,
                Cancellation);
        byte[] dataBefore = await File.ReadAllBytesAsync(
            paths.PreparedDataPath,
            Cancellation);
        byte[] checkpointBefore = await File.ReadAllBytesAsync(
            paths.CheckpointPath,
            Cancellation);
        var source = new TrackingRowSource(rows);

        CsvStreamingExportResult recovered =
            await new CsvStreamingExporter().WriteResumableAsync(
                request with { OpenRows = source.OpenRows },
                Cancellation);

        Assert.Single(source.Opens);
        Assert.Null(source.Opens[0].Boundary);
        Assert.Equal(
            rows.Select(static row => row.RowId),
            source.Opens[0].YieldedRowIds);
        Assert.Equal(
            dataBefore,
            await File.ReadAllBytesAsync(
                paths.PreparedDataPath,
                Cancellation));
        Assert.Equal(
            checkpointBefore,
            await File.ReadAllBytesAsync(
                paths.CheckpointPath,
                Cancellation));
        AssertEquivalent(first, recovered);
    }

    [Theory]
    [InlineData("extra-row")]
    [InlineData("post-boundary-throw")]
    public async Task RecoveredDataComplete_RejectsSourceBeyondCheckpoint(
        string failureKind)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        TableSchema schema = Schema(
            "complete_eof",
            Column("id", DbType.Integer, nullable: false));
        CsvExportRow[] rows =
        [
            Row(-1, DbValue.FromInteger(10)),
            Row(5, DbValue.FromInteger(20)),
        ];
        CsvResumableExportRequest request = Request(
            workspace.PathFor($"complete-eof-{failureKind}.csv"),
            schema,
            (_, token) => Rows(rows, cancellationToken: token),
            checkpointRowInterval: 1);
        CsvExportPreparedOutputPaths paths =
            await CapturePathsAsync(request);
        await new CsvStreamingExporter().WriteResumableAsync(
            request,
            Cancellation);
        byte[] dataBefore = await File.ReadAllBytesAsync(
            paths.PreparedDataPath,
            Cancellation);
        byte[] checkpointBefore = await File.ReadAllBytesAsync(
            paths.CheckpointPath,
            Cancellation);
        var boundaries = new List<long?>();
        CsvResumableExportRequest invalid = request with
        {
            OpenRows = (boundary, token) =>
            {
                boundaries.Add(boundary);
                return failureKind switch
                {
                    "extra-row" => Rows(
                        [
                            .. rows,
                            Row(9, DbValue.FromInteger(30)),
                        ],
                        cancellationToken: token),
                    "post-boundary-throw" => RowsThenThrow(
                        rows,
                        rows.Length,
                        token),
                    _ => throw new ArgumentOutOfRangeException(
                        nameof(failureKind)),
                };
            },
        };

        Exception? error = await Record.ExceptionAsync(
            () => new CsvStreamingExporter()
                .WriteResumableAsync(invalid, Cancellation)
                .AsTask());

        Assert.NotNull(error);
        if (failureKind == "extra-row")
            Assert.IsType<InvalidDataException>(error);
        else
            Assert.IsType<InjectedExportException>(error);
        Assert.Equal(new long?[] { null }, boundaries);
        Assert.Equal(
            dataBefore,
            await File.ReadAllBytesAsync(
                paths.PreparedDataPath,
                Cancellation));
        Assert.Equal(
            checkpointBefore,
            await File.ReadAllBytesAsync(
                paths.CheckpointPath,
                Cancellation));
    }

    [Theory]
    [InlineData("logical-drift")]
    [InlineData("wrong-boundary")]
    [InlineData("missing-row")]
    public async Task InvalidReplay_IsRejectedWithoutAdvancingCheckpoint(
        string failureKind)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        TableSchema schema = Schema(
            "replay",
            Column("id", DbType.Integer, nullable: false),
            Column("note", DbType.Text));
        CsvExportRow[] rows =
        [
            Row(-5, DbValue.FromInteger(1), DbValue.FromText("alpha")),
            Row(3, DbValue.FromInteger(2), DbValue.FromText("beta")),
            Row(8, DbValue.FromInteger(3), DbValue.FromText("gamma")),
        ];
        CsvResumableExportRequest request = Request(
            workspace.PathFor($"replay-{failureKind}.csv"),
            schema,
            (_, token) => Rows(rows, cancellationToken: token),
            checkpointRowInterval: 2);
        CsvExportPreparedOutputPaths paths =
            await CapturePathsAsync(request);
        await CreateInterruptedCheckpointAsync(
            request,
            rows,
            countBeforeFailure: 2);
        byte[] checkpointBefore = await File.ReadAllBytesAsync(
            paths.CheckpointPath,
            Cancellation);
        byte[] dataBefore = await File.ReadAllBytesAsync(
            paths.PreparedDataPath,
            Cancellation);

        CsvExportRow[] replayRows = failureKind switch
        {
            "logical-drift" =>
            [
                rows[0],
                Row(
                    rows[1].RowId,
                    DbValue.FromInteger(2),
                    DbValue.FromText("changed")),
                rows[2],
            ],
            "wrong-boundary" =>
            [
                rows[0],
                Row(
                    4,
                    DbValue.FromInteger(2),
                    DbValue.FromText("beta")),
                rows[2],
            ],
            "missing-row" => [rows[0]],
            _ => throw new ArgumentOutOfRangeException(nameof(failureKind)),
        };
        var source = new TrackingRowSource(
            replayRows,
            static (boundary, available) =>
            {
                if (boundary is not null)
                {
                    throw new InvalidOperationException(
                        "Continuation must not open after invalid replay.");
                }
                return available;
            });

        await Assert.ThrowsAsync<InvalidDataException>(
            () => new CsvStreamingExporter()
                .WriteResumableAsync(
                    request with { OpenRows = source.OpenRows },
                    Cancellation)
                .AsTask());

        Assert.Single(source.Opens);
        Assert.Null(source.Opens[0].Boundary);
        Assert.Equal(
            checkpointBefore,
            await File.ReadAllBytesAsync(
                paths.CheckpointPath,
                Cancellation));
        Assert.Equal(
            dataBefore,
            await File.ReadAllBytesAsync(
                paths.PreparedDataPath,
                Cancellation));
    }

    [Theory]
    [InlineData("at-boundary")]
    [InlineData("out-of-order")]
    public async Task InvalidContinuation_IsRejectedWithoutAdvancingCheckpoint(
        string failureKind)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        TableSchema schema = Schema(
            "continuation",
            Column("value", DbType.Integer, nullable: false));
        CsvExportRow[] rows =
        [
            Row(-7, DbValue.FromInteger(1)),
            Row(-2, DbValue.FromInteger(2)),
            Row(4, DbValue.FromInteger(3)),
            Row(9, DbValue.FromInteger(4)),
        ];
        CsvResumableExportRequest request = Request(
            workspace.PathFor($"continuation-{failureKind}.csv"),
            schema,
            (_, token) => Rows(rows, cancellationToken: token),
            checkpointRowInterval: 2);
        CsvExportPreparedOutputPaths paths =
            await CapturePathsAsync(request);
        await CreateInterruptedCheckpointAsync(
            request,
            rows,
            countBeforeFailure: 2);
        byte[] checkpointBefore = await File.ReadAllBytesAsync(
            paths.CheckpointPath,
            Cancellation);
        CsvExportRow[] invalidContinuation = failureKind switch
        {
            "at-boundary" =>
            [
                Row(-2, DbValue.FromInteger(99)),
            ],
            "out-of-order" =>
            [
                rows[2],
                Row(3, DbValue.FromInteger(99)),
            ],
            _ => throw new ArgumentOutOfRangeException(nameof(failureKind)),
        };
        var source = new TrackingRowSource(
            rows,
            (boundary, available) =>
                boundary is null
                    ? available
                    : invalidContinuation);

        await Assert.ThrowsAsync<InvalidDataException>(
            () => new CsvStreamingExporter()
                .WriteResumableAsync(
                    request with { OpenRows = source.OpenRows },
                    Cancellation)
                .AsTask());

        Assert.Equal(
            new long?[] { null, -2 },
            source.Opens.Select(static open => open.Boundary));
        Assert.Equal(
            checkpointBefore,
            await File.ReadAllBytesAsync(
                paths.CheckpointPath,
                Cancellation));
        Assert.Equal(
            2,
            ReadCheckpoint(paths.CheckpointPath)
                .Progress.CompletedRowCount);
    }

    [Theory]
    [InlineData("profile")]
    [InlineData("source")]
    [InlineData("table")]
    [InlineData("max-data")]
    [InlineData("blob-limit")]
    public async Task BindingDrift_IsRejectedBeforeOpeningSource(
        string driftKind)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        TableSchema schema = Schema(
            "binding",
            Column("id", DbType.Integer, nullable: false));
        CsvExportRow[] rows =
        [
            Row(1, DbValue.FromInteger(10)),
        ];
        CsvResumableExportRequest original = Request(
            workspace.PathFor($"binding-{driftKind}.csv"),
            schema,
            (_, token) => Rows(rows, cancellationToken: token),
            checkpointRowInterval: 1);
        CsvExportPreparedOutputPaths paths =
            await CapturePathsAsync(original);
        await new CsvStreamingExporter().WriteResumableAsync(
            original,
            Cancellation);
        byte[] checkpointBefore = await File.ReadAllBytesAsync(
            paths.CheckpointPath,
            Cancellation);
        CsvExportSourceManifest changedSource = Source('b');
        int sourceOpenCount = 0;
        CsvResumableExportRequest changed = driftKind switch
        {
            "profile" => original with
            {
                Profile = CsvExportProfile.SpreadsheetSafeLossyV1,
            },
            "source" => original with
            {
                Source = changedSource,
                SourceSnapshotIdentity =
                    SnapshotIdentity(changedSource),
            },
            "table" => original with
            {
                Table = Schema(
                    "binding-changed",
                    Column("id", DbType.Integer, nullable: false)),
            },
            "max-data" => original with
            {
                MaxDataBytes = original.MaxDataBytes + 1,
            },
            "blob-limit" => original with
            {
                MaximumDecodedBlobBytes =
                    original.MaximumDecodedBlobBytes - 1,
            },
            _ => throw new ArgumentOutOfRangeException(nameof(driftKind)),
        };
        changed = changed with
        {
            OpenRows = (_, token) =>
            {
                sourceOpenCount++;
                return Rows(rows, cancellationToken: token);
            },
        };

        await Assert.ThrowsAsync<InvalidDataException>(
            () => new CsvStreamingExporter()
                .WriteResumableAsync(changed, Cancellation)
                .AsTask());

        Assert.Equal(0, sourceOpenCount);
        Assert.Equal(
            checkpointBefore,
            await File.ReadAllBytesAsync(
                paths.CheckpointPath,
                Cancellation));
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(-1L)]
    public async Task CheckpointRowInterval_MustBePositive(long interval)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        int sourceOpenCount = 0;
        CsvResumableExportRequest request = Request(
            workspace.PathFor($"interval-{interval}.csv"),
            Schema(
                "interval",
                Column("id", DbType.Integer, nullable: false)),
            (_, token) =>
            {
                sourceOpenCount++;
                return Rows([], cancellationToken: token);
            },
            checkpointRowInterval: interval);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => new CsvStreamingExporter()
                .WriteResumableAsync(request, Cancellation)
                .AsTask());

        Assert.Equal(0, sourceOpenCount);
        Assert.Empty(Directory.EnumerateFileSystemEntries(workspace.Root));
    }

    [Fact]
    public async Task ExhaustedCheckpointGeneration_FailsWithoutAdvancingAuthority()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        TableSchema schema = Schema(
            "generation_exhausted",
            Column("value", DbType.Integer, nullable: false));
        CsvExportRow[] rows =
        [
            Row(-5, DbValue.FromInteger(1)),
            Row(2, DbValue.FromInteger(2)),
            Row(8, DbValue.FromInteger(3)),
        ];
        string destinationPath =
            workspace.PathFor("generation-exhausted.csv");
        CsvResumableExportRequest request = Request(
            destinationPath,
            schema,
            (boundary, token) => Rows(
                rows,
                afterRowIdExclusive: boundary,
                cancellationToken: token),
            checkpointRowInterval: 2);
        CsvExportPreparedOutputPaths paths =
            await CapturePathsAsync(request);
        await CreateInterruptedCheckpointAsync(
            request,
            rows,
            countBeforeFailure: 2);
        CsvExportCheckpoint durable =
            ReadCheckpoint(paths.CheckpointPath);
        Assert.Equal(
            CsvExportCheckpointPhase.Writing,
            durable.Phase);

        CsvExportCheckpoint exhausted = durable with
        {
            Generation = long.MaxValue,
        };
        byte[] exhaustedBytes =
            CsvExportCheckpointSerializer.Serialize(exhausted);
        await File.WriteAllBytesAsync(
            paths.CheckpointPath,
            exhaustedBytes,
            Cancellation);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => new CsvStreamingExporter()
                .WriteResumableAsync(request, Cancellation)
                .AsTask());

        Assert.Equal(
            exhaustedBytes,
            await File.ReadAllBytesAsync(
                paths.CheckpointPath,
                Cancellation));
        Assert.True(
            new FileInfo(paths.PreparedDataPath).Length >
            exhausted.Progress.DataPrefixByteLength);
        Assert.False(File.Exists(destinationPath));

        await using CsvExportPreparedOutputLease recovered =
            await CsvExportPreparedOutputLease.OpenAsync(
                destinationPath,
                CreateBinding(request),
                Cancellation);
        Assert.Equal(
            CsvExportPreparedOutputState.Recovered,
            recovered.State);
        Assert.Equal(
            long.MaxValue,
            recovered.CurrentCheckpoint!.Generation);
        Assert.Equal(
            exhausted.Progress.DataPrefixByteLength,
            recovered.DataStream.Length);
    }

    [Fact]
    public async Task CancellationAfterInterval_PreservesLastDurableCheckpoint()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        TableSchema schema = Schema(
            "cancel",
            Column("id", DbType.Integer, nullable: false));
        CsvExportRow[] rows =
        [
            Row(1, DbValue.FromInteger(1)),
            Row(2, DbValue.FromInteger(2)),
            Row(3, DbValue.FromInteger(3)),
        ];
        using var cancellation = new CancellationTokenSource();
        CsvResumableExportRequest prototype = Request(
            workspace.PathFor("cancel.csv"),
            schema,
            (_, token) => Rows(rows, cancellationToken: token),
            checkpointRowInterval: 2);
        CsvExportPreparedOutputPaths paths =
            await CapturePathsAsync(prototype);
        CsvResumableExportRequest request = prototype with
        {
            OpenRows = (_, token) => RowsThenCancel(
                rows,
                countBeforeCancellation: 2,
                cancellation,
                token),
        };

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => new CsvStreamingExporter()
                .WriteResumableAsync(request, cancellation.Token)
                .AsTask());

        CsvExportCheckpoint checkpoint =
            ReadCheckpoint(paths.CheckpointPath);
        Assert.Equal(1, checkpoint.Generation);
        Assert.Equal(
            CsvExportCheckpointPhase.Writing,
            checkpoint.Phase);
        Assert.Equal(2, checkpoint.Progress.CompletedRowCount);
        Assert.Equal(2, checkpoint.Progress.LastCompletedRowId);
        Assert.Equal(
            checkpoint.Progress.DataPrefixByteLength,
            new FileInfo(paths.PreparedDataPath).Length);
    }

    [Fact]
    public async Task LargeReplay_UsesSeparateBoundedReplayAndContinuationStreams()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        const int totalRows = 2048;
        const int durableRows = 1536;
        CsvExportRow[] rows = Enumerable
            .Range(0, totalRows)
            .Select(static index => Row(
                index - 1024L,
                DbValue.FromInteger(index)))
            .ToArray();
        CsvResumableExportRequest request = Request(
            workspace.PathFor("large-replay.csv"),
            Schema(
                "large_replay",
                Column("value", DbType.Integer, nullable: false)),
            (_, token) => Rows(rows, cancellationToken: token),
            checkpointRowInterval: 512);
        CsvExportPreparedOutputPaths paths =
            await CapturePathsAsync(request);
        await CreateInterruptedCheckpointAsync(
            request,
            rows,
            durableRows);
        Assert.Equal(
            durableRows,
            ReadCheckpoint(paths.CheckpointPath)
                .Progress.CompletedRowCount);
        var source = new TrackingRowSource(rows);

        CsvStreamingExportResult result =
            await new CsvStreamingExporter().WriteResumableAsync(
                request with { OpenRows = source.OpenRows },
                Cancellation);

        Assert.Equal(2, source.Opens.Count);
        Assert.Null(source.Opens[0].Boundary);
        Assert.Equal(
            rows[durableRows - 1].RowId,
            source.Opens[1].Boundary);
        Assert.Equal(durableRows, source.Opens[0].YieldedRowIds.Count);
        Assert.Equal(
            totalRows - durableRows,
            source.Opens[1].YieldedRowIds.Count);
        Assert.Equal(1, source.MaximumConcurrentEnumerators);
        Assert.Equal(totalRows, result.Manifest.Content.RowCount);
        Assert.Equal(
            CsvExportCheckpointPhase.DataComplete,
            ReadCheckpoint(paths.CheckpointPath).Phase);
    }

    private static async Task<CsvExportPreparedOutputPaths> CapturePathsAsync(
        CsvResumableExportRequest request)
    {
        await using CsvExportPreparedOutputLease lease =
            await CsvExportPreparedOutputLease.OpenAsync(
                request.DestinationPath,
                CreateBinding(request),
                Cancellation);
        Assert.Equal(CsvExportPreparedOutputState.New, lease.State);
        return lease.Paths;
    }

    private static async Task CreateInterruptedCheckpointAsync(
        CsvResumableExportRequest request,
        IReadOnlyList<CsvExportRow> rows,
        int countBeforeFailure)
    {
        CsvResumableExportRequest interrupted = request with
        {
            OpenRows = (_, token) => RowsThenThrow(
                rows,
                countBeforeFailure,
                token),
        };
        await Assert.ThrowsAsync<InjectedExportException>(
            () => new CsvStreamingExporter()
                .WriteResumableAsync(interrupted, Cancellation)
                .AsTask());
    }

    private static CsvExportCheckpoint ReadCheckpoint(string path) =>
        CsvExportCheckpointSerializer.Deserialize(File.ReadAllBytes(path));

    private static async Task<(CsvStreamingExportResult Result, byte[] Bytes)>
        RestartOnlyExportAsync(
            CsvResumableExportRequest request,
            IReadOnlyList<CsvExportRow> rows)
    {
        await using var destination = new MemoryStream();
        CsvStreamingExportResult result =
            await new CsvStreamingExporter().WriteAsync(
                destination,
                new CsvStreamingExportRequest
                {
                    Profile = request.Profile,
                    Source = request.Source,
                    Table = request.Table,
                    Rows = Rows(rows),
                    MaxDataBytes = request.MaxDataBytes,
                    MaximumDecodedBlobBytes =
                        request.MaximumDecodedBlobBytes,
                },
                Cancellation);
        return (result, destination.ToArray());
    }

    private static void AssertEquivalent(
        CsvStreamingExportResult expected,
        CsvStreamingExportResult actual)
    {
        Assert.Equal(
            expected.CanonicalManifestBytes,
            actual.CanonicalManifestBytes);
        Assert.Equal(expected.ManifestDigest, actual.ManifestDigest);
    }

    private static CsvResumableExportRequest Request(
        string destinationPath,
        TableSchema schema,
        Func<long?, CancellationToken, IAsyncEnumerable<CsvExportRow>>
            openRows,
        CsvExportProfile profile = CsvExportProfile.LosslessV1,
        CsvExportSourceManifest? source = null,
        long maxDataBytes = 1L << 30,
        int maximumDecodedBlobBytes =
            CsvExportContracts.MaximumSupportedDecodedBlobBytes,
        long checkpointRowInterval = 10_000)
    {
        CsvExportSourceManifest resolvedSource = source ?? Source('a');
        return new CsvResumableExportRequest
        {
            DestinationPath = destinationPath,
            Profile = profile,
            Source = resolvedSource,
            SourceSnapshotIdentity =
                SnapshotIdentity(resolvedSource),
            Table = schema,
            OpenRows = openRows,
            MaxDataBytes = maxDataBytes,
            MaximumDecodedBlobBytes = maximumDecodedBlobBytes,
            CheckpointRowInterval = checkpointRowInterval,
        };
    }

    private static CsvExportCheckpointBinding CreateBinding(
        CsvResumableExportRequest request)
    {
        CsvExportColumnManifest[] columns = request.Table.Columns
            .Select((column, ordinal) => ManifestColumn(
                column,
                ordinal,
                request.MaximumDecodedBlobBytes))
            .ToArray();
        return new CsvExportCheckpointBinding
        {
            Profile = request.Profile,
            Source = request.Source,
            SourceSnapshotIdentity = request.SourceSnapshotIdentity,
            Table = new CsvExportTableManifest
            {
                Name = request.Table.TableName,
                SchemaContract = CsvExportContracts.Schema,
                SchemaDigest =
                    CsvExportManifestSerializer.ComputeSchemaDigest(columns),
                RowOrder = CsvExportContracts.RowOrder,
                Columns = columns,
            },
            Csv = FixedFormat(),
            MaxDataBytes = request.MaxDataBytes,
            MaximumDecodedBlobBytes =
                request.MaximumDecodedBlobBytes,
        };
    }

    private static CsvExportColumnManifest ManifestColumn(
        ColumnDefinition column,
        int ordinal,
        int maximumDecodedBlobBytes)
    {
        CsvExportDatabaseType databaseType = column.Type switch
        {
            DbType.Integer => CsvExportDatabaseType.Integer,
            DbType.Real => CsvExportDatabaseType.Real,
            DbType.Text => CsvExportDatabaseType.Text,
            DbType.Blob => CsvExportDatabaseType.Blob,
            _ => throw new ArgumentOutOfRangeException(nameof(column)),
        };
        return new CsvExportColumnManifest
        {
            Ordinal = ordinal,
            SourceName = column.Name,
            Header = column.Name,
            DatabaseType = databaseType,
            Nullable = column.Nullable,
            ValueEncoding = databaseType switch
            {
                CsvExportDatabaseType.Integer =>
                    CsvExportContracts.IntegerValueEncoding,
                CsvExportDatabaseType.Real =>
                    CsvExportContracts.RealValueEncoding,
                CsvExportDatabaseType.Text =>
                    CsvExportContracts.TextValueEncoding,
                CsvExportDatabaseType.Blob =>
                    CsvExportContracts.BlobValueEncoding,
                _ => throw new ArgumentOutOfRangeException(nameof(column)),
            },
            MaximumDecodedBytes =
                databaseType == CsvExportDatabaseType.Blob
                    ? maximumDecodedBlobBytes
                    : 0,
        };
    }

    private static CsvExportFormatManifest FixedFormat() => new()
    {
        Encoding = CsvExportContracts.Encoding,
        HasByteOrderMark = false,
        Culture = CsvExportContracts.Culture,
        Delimiter = ",",
        Quote = '"',
        Newline = CsvExportContracts.Newline,
        HasHeaderRecord = true,
        HasFinalNewline = true,
        NullToken = CsvExportContracts.NullToken,
        NullTokenMatchesQuotedFields = false,
        TextEscape = CsvExportContracts.TextEscape,
    };

    private static CsvExportSourceManifest Source(char digestValue) => new()
    {
        Kind = CsvExportContracts.SourceKind,
        Version = "4.3.0",
        SnapshotByteLength = 4096,
        SnapshotDigest = Hash(digestValue),
    };

    private static string SnapshotIdentity(CsvExportSourceManifest source) =>
        CsvExportCheckpointContracts.RetainedSnapshotIdentityPrefix +
        source.SnapshotByteLength.ToString(CultureInfo.InvariantCulture) +
        ":sha256:" +
        source.SnapshotDigest.Value;

    private static CsvExportHashManifest Hash(char value) => new()
    {
        Algorithm = CsvExportHashManifest.Sha256Algorithm,
        Value = new string(value, 64),
    };

    private static CsvExportRow Row(
        long rowId,
        params DbValue[] values) => new(rowId, values);

    private static TableSchema Schema(
        string tableName,
        params ColumnDefinition[] columns) => new()
        {
            TableName = tableName,
            Columns = columns,
        };

    private static ColumnDefinition Column(
        string name,
        DbType type,
        bool nullable = true) => new()
        {
            Name = name,
            Type = type,
            Nullable = nullable,
        };

    private static async IAsyncEnumerable<CsvExportRow> Rows(
        IReadOnlyList<CsvExportRow> rows,
        long? afterRowIdExclusive = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
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
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        for (int index = 0; index < countBeforeFailure; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return rows[index];
            await Task.Yield();
        }

        throw new InjectedExportException();
    }

    private static async IAsyncEnumerable<CsvExportRow> RowsThenCancel(
        IReadOnlyList<CsvExportRow> rows,
        int countBeforeCancellation,
        CancellationTokenSource cancellation,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        for (int index = 0; index < countBeforeCancellation; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return rows[index];
            await Task.Yield();
        }

        cancellation.Cancel();
        cancellationToken.ThrowIfCancellationRequested();
    }

    private static async IAsyncEnumerable<CsvExportRow>
        ObserveIntervalCheckpoints(
            IReadOnlyList<CsvExportRow> rows,
            string checkpointPath,
            int interval,
            ICollection<CsvExportCheckpoint> observed,
            [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        for (int index = 0; index < rows.Count; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return rows[index];
            await Task.Yield();
            if ((index + 1) % interval == 0)
                observed.Add(ReadCheckpoint(checkpointPath));
        }
    }

    private sealed class TrackingRowSource
    {
        private readonly IReadOnlyList<CsvExportRow> rows;
        private readonly Func<
            long?,
            IReadOnlyList<CsvExportRow>,
            IReadOnlyList<CsvExportRow>> selector;
        private int activeEnumerators;

        public TrackingRowSource(
            IReadOnlyList<CsvExportRow> rows,
            Func<
                long?,
                IReadOnlyList<CsvExportRow>,
                IReadOnlyList<CsvExportRow>>? selector = null)
        {
            this.rows = rows;
            this.selector = selector ?? DefaultSelection;
        }

        public List<SourceOpen> Opens { get; } = [];

        public int MaximumConcurrentEnumerators { get; private set; }

        public IAsyncEnumerable<CsvExportRow> OpenRows(
            long? afterRowIdExclusive,
            CancellationToken cancellationToken)
        {
            IReadOnlyList<CsvExportRow> selected =
                selector(afterRowIdExclusive, rows);
            var sourceOpen = new SourceOpen(afterRowIdExclusive);
            Opens.Add(sourceOpen);
            return Enumerate(sourceOpen, selected, cancellationToken);
        }

        private async IAsyncEnumerable<CsvExportRow> Enumerate(
            SourceOpen sourceOpen,
            IReadOnlyList<CsvExportRow> selected,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            activeEnumerators++;
            MaximumConcurrentEnumerators = Math.Max(
                MaximumConcurrentEnumerators,
                activeEnumerators);
            try
            {
                foreach (CsvExportRow row in selected)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    sourceOpen.YieldedRowIds.Add(row.RowId);
                    yield return row;
                    await Task.Yield();
                }
            }
            finally
            {
                activeEnumerators--;
            }
        }

        private static IReadOnlyList<CsvExportRow> DefaultSelection(
            long? boundary,
            IReadOnlyList<CsvExportRow> available) =>
            boundary is null
                ? available
                : available
                    .Where(row => row.RowId > boundary.Value)
                    .ToArray();
    }

    private sealed class SourceOpen(long? boundary)
    {
        public long? Boundary { get; } = boundary;

        public List<long> YieldedRowIds { get; } = [];
    }

    private sealed class InjectedExportException : Exception
    {
    }

    private sealed class TemporaryDirectory : IDisposable
    {
        public TemporaryDirectory()
        {
            Root = Path.GetFullPath(Path.Combine(
                Path.GetTempPath(),
                "csharpdb-resumable-export-tests",
                Guid.NewGuid().ToString("N")));
            Directory.CreateDirectory(Root);
        }

        public string Root { get; }

        public string PathFor(string leaf) => Path.Combine(Root, leaf);

        public void Dispose()
        {
            if (Directory.Exists(Root))
                Directory.Delete(Root, recursive: true);
        }
    }
}
