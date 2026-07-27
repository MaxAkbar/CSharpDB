using System.Globalization;
using System.Runtime.CompilerServices;
using System.Security.AccessControl;
using System.Security.Principal;
using System.Text;
using CSharpDB.Migration.Files.Csv;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Files.Tests;

public sealed class CsvExportPreparedOutputPublisherTests
{
    private static readonly CancellationToken Cancellation =
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task FreshPublish_WritesExactCsvBeforeCanonicalManifest()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PreparedExport prepared = await PrepareCompletedAsync(
            workspace,
            "fresh");
        var injector = new RecordingFaultInjector(
            (point, _) =>
            {
                switch (point)
                {
                    case CsvExportPublicationFaultPoint
                        .BeforeDataNamespaceCommit:
                        Assert.False(File.Exists(prepared.DestinationPath));
                        Assert.False(File.Exists(prepared.ManifestPath));
                        break;
                    case CsvExportPublicationFaultPoint
                        .AfterDataNamespaceCommitBeforeManifest:
                    case CsvExportPublicationFaultPoint
                        .BeforeManifestNamespaceCommit:
                        Assert.True(File.Exists(prepared.DestinationPath));
                        Assert.False(File.Exists(prepared.ManifestPath));
                        break;
                    case CsvExportPublicationFaultPoint
                        .AfterManifestNamespaceCommitBeforeResult:
                        Assert.True(File.Exists(prepared.DestinationPath));
                        Assert.True(File.Exists(prepared.ManifestPath));
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(
                            nameof(point),
                            point,
                            "Unknown CSV publication fault point.");
                }
            });

        CsvExportPublicationResult result =
            await new CsvExportPreparedOutputPublisher(injector)
                .PublishCompletedAsync(
                    PublicationRequest(prepared),
                    Cancellation);

        Assert.Equal(
            Enum.GetValues<CsvExportPublicationFaultPoint>(),
            injector.ObservedPoints);
        Assert.False(result.ReusedData);
        Assert.False(result.ReusedManifest);
        Assert.Equal(
            "id,note\r\n1,alpha\r\n2,beta\r\n",
            Encoding.UTF8.GetString(prepared.PreparedDataBytes));
        Assert.Equal(
            prepared.PreparedDataBytes,
            await File.ReadAllBytesAsync(
                prepared.DestinationPath,
                Cancellation));
        Assert.Equal(
            prepared.Export.CanonicalManifestBytes,
            await File.ReadAllBytesAsync(
                prepared.ManifestPath,
                Cancellation));
        Assert.Equal(prepared.Export.ManifestDigest, result.ManifestDigest);
        Assert.Equal(
            prepared.Export.CanonicalManifestBytes,
            result.CanonicalManifestBytes);
        Assert.True(File.Exists(prepared.Paths.PreparedDataPath));
        Assert.True(File.Exists(prepared.Paths.CheckpointPath));
    }

    [Fact]
    public async Task WriteResumableAndPublishAsync_PublishesExactPair()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        string destinationPath = workspace.PathFor("integrated.csv");
        string manifestPath = workspace.PathFor("integrated.manifest.json");
        CsvResumableExportRequest request = Request(destinationPath);

        CsvExportPublicationResult result =
            await new CsvStreamingExporter()
                .WriteResumableAndPublishAsync(
                    request,
                    manifestPath,
                    Cancellation);
        (
            _,
            CsvExportPreparedOutputPaths paths,
            _
        ) = CsvExportPreparedOutputLease.BindPaths(
            destinationPath,
            allowExistingDestination: true);

        byte[] preparedBytes = await File.ReadAllBytesAsync(
            paths.PreparedDataPath,
            Cancellation);
        Assert.Equal(
            "id,note\r\n1,alpha\r\n2,beta\r\n",
            Encoding.UTF8.GetString(preparedBytes));
        Assert.Equal(
            preparedBytes,
            await File.ReadAllBytesAsync(destinationPath, Cancellation));
        Assert.Equal(
            result.CanonicalManifestBytes,
            await File.ReadAllBytesAsync(manifestPath, Cancellation));
        Assert.False(result.ReusedData);
        Assert.False(result.ReusedManifest);
    }

    [Fact]
    public async Task FaultAfterDataCommit_RetryReusesCsvAndPublishesManifest()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PreparedExport prepared = await PrepareCompletedAsync(
            workspace,
            "after-data");
        var injector = new ThrowOnceFaultInjector(
            CsvExportPublicationFaultPoint
                .AfterDataNamespaceCommitBeforeManifest);

        InjectedPublicationException failure =
            await Assert.ThrowsAsync<InjectedPublicationException>(
                () => new CsvExportPreparedOutputPublisher(injector)
                    .PublishCompletedAsync(
                        PublicationRequest(prepared),
                        Cancellation)
                    .AsTask());

        Assert.Equal(
            CsvExportPublicationFaultPoint
                .AfterDataNamespaceCommitBeforeManifest,
            failure.Point);
        Assert.Equal(
            prepared.PreparedDataBytes,
            await File.ReadAllBytesAsync(
                prepared.DestinationPath,
                Cancellation));
        Assert.False(File.Exists(prepared.ManifestPath));

        CsvExportPublicationResult recovered =
            await new CsvExportPreparedOutputPublisher()
                .PublishCompletedAsync(
                    PublicationRequest(prepared),
                    Cancellation);

        Assert.True(recovered.ReusedData);
        Assert.False(recovered.ReusedManifest);
        await AssertExactFinalPairAsync(prepared);
    }

    [Fact]
    public async Task CancellationAfterDataCommit_IsIgnoredThroughManifestCommit()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PreparedExport prepared = await PrepareCompletedAsync(
            workspace,
            "cancel-after-data");
        using var cancellation = new CancellationTokenSource();
        var injector = new RecordingFaultInjector(
            (point, _) =>
            {
                if (point == CsvExportPublicationFaultPoint
                    .AfterDataNamespaceCommitBeforeManifest)
                {
                    cancellation.Cancel();
                }
            });

        CsvExportPublicationResult result =
            await new CsvExportPreparedOutputPublisher(injector)
                .PublishCompletedAsync(
                    PublicationRequest(prepared),
                    cancellation.Token);

        Assert.False(result.ReusedData);
        Assert.False(result.ReusedManifest);
        Assert.True(cancellation.IsCancellationRequested);
        await AssertExactFinalPairAsync(prepared);
    }

    [Fact]
    public async Task FaultAfterManifestCommit_RetryReusesExactPair()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PreparedExport prepared = await PrepareCompletedAsync(
            workspace,
            "after-manifest");
        var injector = new ThrowOnceFaultInjector(
            CsvExportPublicationFaultPoint
                .AfterManifestNamespaceCommitBeforeResult);

        InjectedPublicationException failure =
            await Assert.ThrowsAsync<InjectedPublicationException>(
                () => new CsvExportPreparedOutputPublisher(injector)
                    .PublishCompletedAsync(
                        PublicationRequest(prepared),
                        Cancellation)
                    .AsTask());

        Assert.Equal(
            CsvExportPublicationFaultPoint
                .AfterManifestNamespaceCommitBeforeResult,
            failure.Point);
        await AssertExactFinalPairAsync(prepared);

        CsvExportPublicationResult recovered =
            await new CsvExportPreparedOutputPublisher()
                .PublishCompletedAsync(
                    PublicationRequest(prepared),
                    Cancellation);

        Assert.True(recovered.ReusedData);
        Assert.True(recovered.ReusedManifest);
        await AssertExactFinalPairAsync(prepared);
    }

    [Fact]
    public async Task ExactPair_RetryIsIdempotent()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PreparedExport prepared = await PrepareCompletedAsync(
            workspace,
            "idempotent");
        CsvExportPreparedOutputPublisher publisher = new();

        CsvExportPublicationResult first =
            await publisher.PublishCompletedAsync(
                PublicationRequest(prepared),
                Cancellation);
        byte[] firstData = await File.ReadAllBytesAsync(
            prepared.DestinationPath,
            Cancellation);
        byte[] firstManifest = await File.ReadAllBytesAsync(
            prepared.ManifestPath,
            Cancellation);

        CsvExportPublicationResult second =
            await publisher.PublishCompletedAsync(
                PublicationRequest(prepared),
                Cancellation);

        Assert.False(first.ReusedData);
        Assert.False(first.ReusedManifest);
        Assert.True(second.ReusedData);
        Assert.True(second.ReusedManifest);
        Assert.Equal(
            firstData,
            await File.ReadAllBytesAsync(
                prepared.DestinationPath,
                Cancellation));
        Assert.Equal(
            firstManifest,
            await File.ReadAllBytesAsync(
                prepared.ManifestPath,
                Cancellation));
    }

    [Fact]
    public async Task ExactManifestWithoutData_IsRejectedWithoutLateDataRepair()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PreparedExport prepared = await PrepareCompletedAsync(
            workspace,
            "manifest-only");
        await WritePrivateFileAsync(
            prepared.ManifestPath,
            prepared.Export.CanonicalManifestBytes,
            Cancellation);

        await Assert.ThrowsAsync<InvalidDataException>(
            () => new CsvExportPreparedOutputPublisher()
                .PublishCompletedAsync(
                    PublicationRequest(prepared),
                    Cancellation)
                .AsTask());

        Assert.False(File.Exists(prepared.DestinationPath));
        Assert.Equal(
            prepared.Export.CanonicalManifestBytes,
            await File.ReadAllBytesAsync(
                prepared.ManifestPath,
                Cancellation));
    }

    [Fact]
    public async Task ManifestAppearingAtDataBarrier_PreventsLateDataPublication()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PreparedExport prepared = await PrepareCompletedAsync(
            workspace,
            "manifest-race");
        var injector = new AsyncFaultInjector(
            async (point, token) =>
            {
                if (point == CsvExportPublicationFaultPoint
                    .BeforeDataNamespaceCommit)
                {
                    await WritePrivateFileAsync(
                        prepared.ManifestPath,
                        prepared.Export.CanonicalManifestBytes,
                        token);
                }
            });

        await Assert.ThrowsAsync<InvalidDataException>(
            () => new CsvExportPreparedOutputPublisher(injector)
                .PublishCompletedAsync(
                    PublicationRequest(prepared),
                    Cancellation)
                .AsTask());

        Assert.False(File.Exists(prepared.DestinationPath));
        Assert.Equal(
            prepared.Export.CanonicalManifestBytes,
            await File.ReadAllBytesAsync(
                prepared.ManifestPath,
                Cancellation));
    }

    [Fact]
    public async Task DifferentFinalCsv_IsRejectedWithoutManifestOrOverwrite()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PreparedExport prepared = await PrepareCompletedAsync(
            workspace,
            "different-data");
        byte[] different = "different,data\r\n"u8.ToArray();
        await WritePrivateFileAsync(
            prepared.DestinationPath,
            different,
            Cancellation);

        await Assert.ThrowsAsync<IOException>(
            () => new CsvExportPreparedOutputPublisher()
                .PublishCompletedAsync(
                    PublicationRequest(prepared),
                    Cancellation)
                .AsTask());

        Assert.Equal(
            different,
            await File.ReadAllBytesAsync(
                prepared.DestinationPath,
                Cancellation));
        Assert.False(File.Exists(prepared.ManifestPath));
    }

    [Fact]
    public async Task DifferentFinalManifest_IsRejectedWithoutOverwrite()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PreparedExport prepared = await PrepareCompletedAsync(
            workspace,
            "different-manifest");
        byte[] differentManifest = """{"different":true}"""u8.ToArray();
        await WritePrivateFileAsync(
            prepared.DestinationPath,
            prepared.PreparedDataBytes,
            Cancellation);
        await WritePrivateFileAsync(
            prepared.ManifestPath,
            differentManifest,
            Cancellation);

        await Assert.ThrowsAsync<IOException>(
            () => new CsvExportPreparedOutputPublisher()
                .PublishCompletedAsync(
                    PublicationRequest(prepared),
                    Cancellation)
                .AsTask());

        Assert.Equal(
            prepared.PreparedDataBytes,
            await File.ReadAllBytesAsync(
                prepared.DestinationPath,
                Cancellation));
        Assert.Equal(
            differentManifest,
            await File.ReadAllBytesAsync(
                prepared.ManifestPath,
                Cancellation));
    }

    [Fact]
    public async Task TamperedPreparedData_IsRejectedBeforeFinalVisibility()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PreparedExport prepared = await PrepareCompletedAsync(
            workspace,
            "tampered-prepared");
        await using (var stream = new FileStream(
                         prepared.Paths.PreparedDataPath,
                         FileMode.Open,
                         FileAccess.ReadWrite,
                         FileShare.None,
                         bufferSize: 4096,
                         FileOptions.Asynchronous |
                             FileOptions.WriteThrough))
        {
            int original = stream.ReadByte();
            Assert.NotEqual(-1, original);
            stream.Position = 0;
            stream.WriteByte((byte)(original ^ 0x01));
            stream.Flush(flushToDisk: true);
        }

        await Assert.ThrowsAsync<InvalidDataException>(
            () => new CsvExportPreparedOutputPublisher()
                .PublishCompletedAsync(
                    PublicationRequest(prepared),
                    Cancellation)
                .AsTask());

        Assert.False(File.Exists(prepared.DestinationPath));
        Assert.False(File.Exists(prepared.ManifestPath));
    }

    [Fact]
    public async Task DifferentExpectedManifestDigest_IsRejectedBeforeVisibility()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PreparedExport prepared = await PrepareCompletedAsync(
            workspace,
            "digest-mismatch");

        await Assert.ThrowsAsync<InvalidDataException>(
            () => new CsvExportPreparedOutputPublisher()
                .PublishCompletedAsync(
                    PublicationRequest(prepared) with
                    {
                        ExpectedManifestDigest = new string('0', 64),
                    },
                    Cancellation)
                .AsTask());

        Assert.False(File.Exists(prepared.DestinationPath));
        Assert.False(File.Exists(prepared.ManifestPath));
    }

    [Fact]
    public async Task WritingCheckpoint_IsNotReadyForPublication()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        string destinationPath = workspace.PathFor("writing.csv");
        string manifestPath = workspace.PathFor("writing.manifest.json");
        CsvResumableExportRequest request = Request(destinationPath) with
        {
            CheckpointRowInterval = 1,
            OpenRows = (_, token) => RowsThenThrow(token),
        };

        await Assert.ThrowsAsync<InjectedSourceException>(
            () => new CsvStreamingExporter()
                .WriteResumableAsync(request, Cancellation)
                .AsTask());

        await Assert.ThrowsAsync<InvalidDataException>(
            () => new CsvExportPreparedOutputPublisher()
                .PublishCompletedAsync(
                    new CsvExportPublicationRequest
                    {
                        DestinationPath = destinationPath,
                        ManifestPath = manifestPath,
                        ExpectedManifestDigest = new string('0', 64),
                    },
                    Cancellation)
                .AsTask());

        Assert.False(File.Exists(destinationPath));
        Assert.False(File.Exists(manifestPath));
    }

    [Fact]
    public async Task ActiveDataCompleteCheckpoint_OutranksStalePending()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PreparedExport prepared = await PrepareCompletedAsync(
            workspace,
            "stale-pending");
        CsvExportCheckpoint active = CsvExportCheckpointSerializer.Deserialize(
            await File.ReadAllBytesAsync(
                prepared.Paths.CheckpointPath,
                Cancellation));
        CsvExportCheckpoint stale = active with
        {
            Generation = checked(active.Generation + 100),
        };
        byte[] staleBytes = CsvExportCheckpointSerializer.Serialize(stale);
        await WritePrivateFileAsync(
            prepared.Paths.PendingCheckpointPath,
            staleBytes,
            Cancellation);

        CsvExportPublicationResult result =
            await new CsvExportPreparedOutputPublisher()
                .PublishCompletedAsync(
                    PublicationRequest(prepared),
                    Cancellation);

        Assert.False(result.ReusedData);
        Assert.False(result.ReusedManifest);
        Assert.Equal(
            staleBytes,
            await File.ReadAllBytesAsync(
                prepared.Paths.PendingCheckpointPath,
                Cancellation));
        await AssertExactFinalPairAsync(prepared);
    }

    [Fact]
    public async Task PreCanceledPublish_CreatesNoFinalOrTemporaryFiles()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PreparedExport prepared = await PrepareCompletedAsync(
            workspace,
            "pre-canceled");
        IReadOnlyDictionary<string, byte[]> before =
            await SnapshotFilesAsync(workspace.Root);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => new CsvExportPreparedOutputPublisher()
                .PublishCompletedAsync(
                    PublicationRequest(prepared),
                    cancellation.Token)
                .AsTask());

        IReadOnlyDictionary<string, byte[]> after =
            await SnapshotFilesAsync(workspace.Root);
        Assert.Equal(before.Keys.Order(), after.Keys.Order());
        foreach ((string path, byte[] bytes) in before)
            Assert.Equal(bytes, after[path]);
        Assert.False(File.Exists(prepared.DestinationPath));
        Assert.False(File.Exists(prepared.ManifestPath));
    }

    [Fact]
    public async Task PublicationPaths_RejectAliasesAndNonSiblingManifest()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        PreparedExport prepared = await PrepareCompletedAsync(
            workspace,
            "path-validation");
        string otherDirectory = workspace.PathFor("other");
        Directory.CreateDirectory(otherDirectory);
        string unnormalizedManifest =
            workspace.Root +
            Path.DirectorySeparatorChar +
            "." +
            Path.DirectorySeparatorChar +
            "aliased.manifest.json";
        string[] invalidManifestPaths =
        [
            prepared.DestinationPath,
            prepared.Paths.PreparedDataPath,
            Path.Combine(otherDirectory, "outside.manifest.json"),
            unnormalizedManifest,
            workspace.PathFor("ALIAS~1.JSON"),
            Path.Combine(
                workspace.Root,
                "PARENT~1",
                "aliased.manifest.json"),
        ];
        byte[] preparedBefore = await File.ReadAllBytesAsync(
            prepared.Paths.PreparedDataPath,
            Cancellation);
        byte[] checkpointBefore = await File.ReadAllBytesAsync(
            prepared.Paths.CheckpointPath,
            Cancellation);

        foreach (string invalidManifestPath in invalidManifestPaths)
        {
            await Assert.ThrowsAsync<ArgumentException>(
                () => new CsvExportPreparedOutputPublisher()
                    .PublishCompletedAsync(
                        PublicationRequest(prepared) with
                        {
                            ManifestPath = invalidManifestPath,
                        },
                        Cancellation)
                    .AsTask());
        }

        Assert.False(File.Exists(prepared.DestinationPath));
        Assert.False(File.Exists(prepared.ManifestPath));
        Assert.Equal(
            preparedBefore,
            await File.ReadAllBytesAsync(
                prepared.Paths.PreparedDataPath,
                Cancellation));
        Assert.Equal(
            checkpointBefore,
            await File.ReadAllBytesAsync(
                prepared.Paths.CheckpointPath,
                Cancellation));
    }

    private static async Task<PreparedExport> PrepareCompletedAsync(
        TemporaryDirectory workspace,
        string stem)
    {
        string destinationPath = workspace.PathFor(stem + ".csv");
        string manifestPath = workspace.PathFor(stem + ".manifest.json");
        CsvResumableExportRequest request = Request(destinationPath);
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
        Assert.False(File.Exists(destinationPath));
        Assert.False(File.Exists(manifestPath));
        return new PreparedExport(
            destinationPath,
            manifestPath,
            request,
            paths,
            export,
            preparedDataBytes);
    }

    private static CsvExportPublicationRequest PublicationRequest(
        PreparedExport prepared) => new()
        {
            DestinationPath = prepared.DestinationPath,
            ManifestPath = prepared.ManifestPath,
            ExpectedManifestDigest = prepared.Export.ManifestDigest,
        };

    private static async Task AssertExactFinalPairAsync(
        PreparedExport prepared)
    {
        Assert.Equal(
            prepared.PreparedDataBytes,
            await File.ReadAllBytesAsync(
                prepared.DestinationPath,
                Cancellation));
        Assert.Equal(
            prepared.Export.CanonicalManifestBytes,
            await File.ReadAllBytesAsync(
                prepared.ManifestPath,
                Cancellation));
    }

    private static CsvResumableExportRequest Request(string destinationPath)
    {
        CsvExportSourceManifest source = Source('a');
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
        return new CsvResumableExportRequest
        {
            DestinationPath = destinationPath,
            Profile = CsvExportProfile.LosslessV1,
            Source = source,
            SourceSnapshotIdentity = SnapshotIdentity(source),
            Table = new TableSchema
            {
                TableName = "publication",
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
            OpenRows = (boundary, token) =>
                Rows(rows, boundary, token),
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

    private static string SnapshotIdentity(CsvExportSourceManifest source) =>
        CsvExportCheckpointContracts.RetainedSnapshotIdentityPrefix +
        source.SnapshotByteLength.ToString(CultureInfo.InvariantCulture) +
        ":sha256:" +
        source.SnapshotDigest.Value;

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
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        yield return Row(
            -7,
            DbValue.FromInteger(1),
            DbValue.FromText("alpha"));
        await Task.Yield();
        throw new InjectedSourceException();
    }

    private static async Task WritePrivateFileAsync(
        string path,
        byte[] bytes,
        CancellationToken cancellationToken)
    {
        if (!OperatingSystem.IsWindows())
        {
            await File.WriteAllBytesAsync(path, bytes, cancellationToken);
            return;
        }

        using WindowsIdentity identity =
            WindowsIdentity.GetCurrent(TokenAccessLevels.Query);
        SecurityIdentifier owner = identity.User ??
            throw new InvalidOperationException(
                "The current Windows test identity has no security identifier.");
        var security = new FileSecurity();
        security.SetOwner(owner);
        security.SetAccessRuleProtection(
            isProtected: true,
            preserveInheritance: false);
        security.AddAccessRule(new FileSystemAccessRule(
            owner,
            FileSystemRights.FullControl,
            AccessControlType.Allow));
        await using FileStream stream = FileSystemAclExtensions.Create(
            new FileInfo(path),
            FileMode.CreateNew,
            FileSystemRights.FullControl,
            FileShare.None,
            bufferSize: 4096,
            FileOptions.Asynchronous | FileOptions.WriteThrough,
            security);
        await stream.WriteAsync(bytes, cancellationToken);
        stream.Flush(flushToDisk: true);
    }

    private static async Task<IReadOnlyDictionary<string, byte[]>>
        SnapshotFilesAsync(string root)
    {
        var files = new Dictionary<string, byte[]>(
            StringComparer.OrdinalIgnoreCase);
        foreach (string path in Directory.GetFiles(root))
        {
            files.Add(
                Path.GetFileName(path),
                await File.ReadAllBytesAsync(path, Cancellation));
        }
        return files;
    }

    private sealed record PreparedExport(
        string DestinationPath,
        string ManifestPath,
        CsvResumableExportRequest Request,
        CsvExportPreparedOutputPaths Paths,
        CsvStreamingExportResult Export,
        byte[] PreparedDataBytes);

    private sealed class RecordingFaultInjector(
        Action<CsvExportPublicationFaultPoint, CancellationToken> callback)
        : ICsvExportPublicationFaultInjector
    {
        public List<CsvExportPublicationFaultPoint> ObservedPoints { get; } = [];

        public ValueTask InjectAsync(
            CsvExportPublicationFaultPoint point,
            CancellationToken cancellationToken)
        {
            ObservedPoints.Add(point);
            callback(point, cancellationToken);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class AsyncFaultInjector(
        Func<
            CsvExportPublicationFaultPoint,
            CancellationToken,
            ValueTask> callback)
        : ICsvExportPublicationFaultInjector
    {
        public ValueTask InjectAsync(
            CsvExportPublicationFaultPoint point,
            CancellationToken cancellationToken) =>
            callback(point, cancellationToken);
    }

    private sealed class ThrowOnceFaultInjector(
        CsvExportPublicationFaultPoint faultPoint)
        : ICsvExportPublicationFaultInjector
    {
        private bool thrown;

        public ValueTask InjectAsync(
            CsvExportPublicationFaultPoint point,
            CancellationToken cancellationToken)
        {
            if (point == faultPoint && !thrown)
            {
                thrown = true;
                throw new InjectedPublicationException(point);
            }

            return ValueTask.CompletedTask;
        }
    }

    private sealed class InjectedPublicationException(
        CsvExportPublicationFaultPoint point) : Exception
    {
        public CsvExportPublicationFaultPoint Point { get; } = point;
    }

    private sealed class InjectedSourceException : Exception
    {
    }

    private sealed class TemporaryDirectory : IDisposable
    {
        public TemporaryDirectory()
        {
            Root = Path.GetFullPath(Path.Combine(
                Path.GetTempPath(),
                "csharpdb-export-publication-tests",
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
