using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Principal;
using CSharpDB.Migration.Files.Json;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonExportPreparedOutputPublisherTests
{
    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task FreshPreparedOutput_PublishesExactPairAndPreservesJournal()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace =
            new TemporaryWorkspace();
        PreparedExport prepared =
            await PrepareCompletedAsync(
                workspace,
                "fresh");
        byte[] checkpointBefore =
            await File.ReadAllBytesAsync(
                prepared.Paths.CheckpointPath,
                Cancellation);

        JsonExportPublicationResult result =
            await new JsonExportPublisher()
                .PublishCompletedAsync(
                    PublicationRequest(
                        prepared),
                    Cancellation);

        Assert.False(result.ReusedData);
        Assert.False(result.ReusedManifest);
        Assert.Equal(
            prepared.DataBytes,
            await File.ReadAllBytesAsync(
                prepared.DestinationPath,
                Cancellation));
        Assert.Equal(
            prepared.Export
                .CanonicalManifestBytes,
            await File.ReadAllBytesAsync(
                prepared.ManifestPath,
                Cancellation));
        Assert.Equal(
            prepared.DataBytes,
            await File.ReadAllBytesAsync(
                prepared.Paths
                    .PreparedDataPath,
                Cancellation));
        Assert.Equal(
            checkpointBefore,
            await File.ReadAllBytesAsync(
                prepared.Paths
                    .CheckpointPath,
                Cancellation));
        Assert.Empty(
            PublicationStagingFiles(
                workspace.Root));
    }

    [Theory]
    [InlineData(JsonExportFraming.RootArray, false)]
    [InlineData(JsonExportFraming.Ndjson, false)]
    [InlineData(JsonExportFraming.Ndjson, true)]
    public async Task PreparedPublication_CoversFramingEmptyNdjsonAndExactRetry(
        JsonExportFraming framing,
        bool empty)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace =
            new TemporaryWorkspace();
        PreparedExport prepared =
            await PrepareCompletedAsync(
                workspace,
                $"framing-{framing}-{empty}",
                framing,
                empty);
        if (framing ==
                JsonExportFraming.Ndjson &&
            empty)
        {
            Assert.Empty(
                prepared.DataBytes);
        }

        JsonExportPublisher publisher =
            new();
        JsonExportPublicationResult first =
            await publisher
                .PublishCompletedAsync(
                    PublicationRequest(
                        prepared),
                    Cancellation);
        JsonExportPublicationResult retry =
            await publisher
                .PublishCompletedAsync(
                    PublicationRequest(
                        prepared),
                    Cancellation);

        Assert.False(first.ReusedData);
        Assert.False(first.ReusedManifest);
        Assert.True(retry.ReusedData);
        Assert.True(retry.ReusedManifest);
        Assert.Equal(
            prepared.DataBytes,
            await File.ReadAllBytesAsync(
                prepared.DestinationPath,
                Cancellation));
        Assert.Equal(
            prepared.Export
                .CanonicalManifestBytes,
            await File.ReadAllBytesAsync(
                prepared.ManifestPath,
                Cancellation));
        Assert.Equal(
            prepared.DataBytes,
            await File.ReadAllBytesAsync(
                prepared.Paths
                    .PreparedDataPath,
                Cancellation));
    }

    [Fact]
    public async Task LiveLeasePublication_DoesNotDisposePreparedAuthority()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace =
            new TemporaryWorkspace();
        PreparedExport prepared =
            await PrepareCompletedAsync(
                workspace,
                "live-lease");
        await using JsonExportPreparedOutputLease lease =
            await JsonExportPreparedOutputLease
                .OpenForPublicationAsync(
                    prepared.DestinationPath,
                    prepared.Export.ManifestDigest,
                    Cancellation);

        JsonExportPublicationResult result =
            await new JsonExportPublisher()
                .PublishCompletedAsync(
                    PublicationRequest(
                        prepared),
                    lease,
                    Cancellation);

        Assert.False(result.ReusedData);
        Assert.False(result.ReusedManifest);
        await using
            JsonExportPreparedOutputPublicationQualification
                qualification =
                    await lease
                        .QualifyForPublicationAsync(
                            prepared.Export
                                .ManifestDigest,
                            Cancellation);
        Assert.Same(
            lease.DataStream,
            qualification.DataStream);
        Assert.Equal(
            prepared.DataBytes.LongLength,
            qualification.DataStream.Length);
    }

    [Fact]
    public async Task DataOnlyRetry_ReusesDataAndPublishesManifest()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace =
            new TemporaryWorkspace();
        PreparedExport prepared =
            await PrepareCompletedAsync(
                workspace,
                "data-only");
        byte[] checkpointBefore =
            await File.ReadAllBytesAsync(
                prepared.Paths.CheckpointPath,
                Cancellation);
        var injector =
            new ThrowOncePublicationFaultInjector(
                JsonExportPublicationFaultPoint
                    .AfterDataNamespaceCommitBeforeManifest);

        await Assert.ThrowsAsync<
            PreparedInjectedPublicationException>(
            () => new JsonExportPublisher(
                    injector)
                .PublishCompletedAsync(
                    PublicationRequest(
                        prepared),
                    Cancellation)
                .AsTask());

        Assert.Equal(
            prepared.DataBytes,
            await File.ReadAllBytesAsync(
                prepared.DestinationPath,
                Cancellation));
        Assert.False(
            File.Exists(
                prepared.ManifestPath));

        JsonExportPublicationResult recovered =
            await new JsonExportPublisher()
                .PublishCompletedAsync(
                    PublicationRequest(
                        prepared),
                    Cancellation);

        Assert.True(recovered.ReusedData);
        Assert.False(recovered.ReusedManifest);
        Assert.Equal(
            prepared.Export
                .CanonicalManifestBytes,
            await File.ReadAllBytesAsync(
                prepared.ManifestPath,
                Cancellation));
        Assert.Equal(
            checkpointBefore,
            await File.ReadAllBytesAsync(
                prepared.Paths.CheckpointPath,
                Cancellation));
    }

    [Fact]
    public async Task ExactPairRetry_InspectsFinalsBeforeUnsafeStaging()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace =
            new TemporaryWorkspace();
        PreparedExport prepared =
            await PrepareCompletedAsync(
                workspace,
                "exact-pair");
        JsonExportPublisher publisher = new();
        _ = await publisher
            .PublishCompletedAsync(
                PublicationRequest(
                    prepared),
                Cancellation);
        JsonExportPublicationFileSystem
            .PublicationPaths publication =
            JsonExportPublicationFileSystem
                .PublicationPaths.Bind(
                    prepared.DestinationPath,
                    prepared.ManifestPath);
        Directory.CreateDirectory(
            publication.DataStagingPath);

        JsonExportPublicationResult reused =
            await publisher
                .PublishCompletedAsync(
                    PublicationRequest(
                        prepared),
                    Cancellation);

        Assert.True(reused.ReusedData);
        Assert.True(reused.ReusedManifest);
        Assert.True(
            Directory.Exists(
                publication.DataStagingPath));
        Assert.Equal(
            prepared.DataBytes,
            await File.ReadAllBytesAsync(
                prepared.DestinationPath,
                Cancellation));
    }

    [Fact]
    public async Task ManifestOnlyFinal_IsRejectedBeforePreparedCopy()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace =
            new TemporaryWorkspace();
        PreparedExport prepared =
            await PrepareCompletedAsync(
                workspace,
                "manifest-only");
        await WritePrivateFileAsync(
            prepared.ManifestPath,
            prepared.Export
                .CanonicalManifestBytes);

        await Assert.ThrowsAsync<
            InvalidDataException>(
            () => new JsonExportPublisher()
                .PublishCompletedAsync(
                    PublicationRequest(
                        prepared),
                    Cancellation)
                .AsTask());

        Assert.False(
            File.Exists(
                prepared.DestinationPath));
        Assert.Equal(
            prepared.DataBytes,
            await File.ReadAllBytesAsync(
                prepared.Paths
                    .PreparedDataPath,
                Cancellation));
        Assert.Empty(
            PublicationStagingFiles(
                workspace.Root));
    }

    [Fact]
    public async Task DifferentFinalData_IsRejectedWithoutManifestOrStaging()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace =
            new TemporaryWorkspace();
        PreparedExport prepared =
            await PrepareCompletedAsync(
                workspace,
                "different-data");
        byte[] different =
            """{"different":true}"""u8
                .ToArray();
        await WritePrivateFileAsync(
            prepared.DestinationPath,
            different);

        await Assert.ThrowsAsync<IOException>(
            () => new JsonExportPublisher()
                .PublishCompletedAsync(
                    PublicationRequest(
                        prepared),
                    Cancellation)
                .AsTask());

        Assert.Equal(
            different,
            await File.ReadAllBytesAsync(
                prepared.DestinationPath,
                Cancellation));
        Assert.False(
            File.Exists(
                prepared.ManifestPath));
        Assert.Empty(
            PublicationStagingFiles(
                workspace.Root));
    }

    [Fact]
    public async Task DigestMismatchAndJournalAlias_FailBeforeVisibility()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace =
            new TemporaryWorkspace();
        PreparedExport prepared =
            await PrepareCompletedAsync(
                workspace,
                "preflight");

        await Assert.ThrowsAsync<
            InvalidDataException>(
            () => new JsonExportPublisher()
                .PublishCompletedAsync(
                    PublicationRequest(
                        prepared) with
                    {
                        ExpectedManifestDigest =
                            new string(
                                '0',
                                64),
                    },
                    Cancellation)
                .AsTask());
        byte[] pendingSentinel =
            "must-not-be-reclaimed"u8
                .ToArray();
        await WritePrivateFileAsync(
            prepared.Paths
                .PendingCheckpointPath,
            pendingSentinel);
        foreach (string journalAlias in
                 new[]
                 {
                     prepared.Paths
                         .PreparedDataPath,
                     prepared.Paths
                         .CheckpointPath,
                     prepared.Paths
                         .PendingCheckpointPath,
                 })
        {
            await Assert.ThrowsAsync<
                ArgumentException>(
                () => new JsonExportPublisher()
                    .PublishCompletedAsync(
                        PublicationRequest(
                            prepared) with
                        {
                            ManifestPath =
                                journalAlias,
                        },
                        Cancellation)
                    .AsTask());
        }

        Assert.False(
            File.Exists(
                prepared.DestinationPath));
        Assert.False(
            File.Exists(
                prepared.ManifestPath));
        Assert.Equal(
            prepared.DataBytes,
            await File.ReadAllBytesAsync(
                prepared.Paths
                    .PreparedDataPath,
                Cancellation));
        Assert.Equal(
            pendingSentinel,
            await File.ReadAllBytesAsync(
                prepared.Paths
                    .PendingCheckpointPath,
                Cancellation));
        Assert.Empty(
            PublicationStagingFiles(
                workspace.Root));
    }

    [Fact]
    public async Task PublicStandalonePublisher_InvalidArgumentsCreateNothing()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace =
            new TemporaryWorkspace();
        string destination =
            workspace.PathFor("invalid.json");
        string manifest =
            workspace.PathFor(
                "invalid.manifest.json");
        JsonExportPublisher publisher =
            new();
        var valid =
            new JsonPreparedExportPublicationRequest
            {
                DestinationPath =
                    destination,
                ManifestPath =
                    manifest,
                ExpectedManifestDigest =
                    new string('0', 64),
            };

        await Assert.ThrowsAsync<
            ArgumentNullException>(
            () => publisher
                .PublishCompletedAsync(
                    null!,
                    Cancellation)
                .AsTask());
        await Assert.ThrowsAsync<
            ArgumentException>(
            () => publisher
                .PublishCompletedAsync(
                    valid with
                    {
                        DestinationPath = " ",
                    },
                    Cancellation)
                .AsTask());
        await Assert.ThrowsAsync<
            ArgumentException>(
            () => publisher
                .PublishCompletedAsync(
                    valid with
                    {
                        ManifestPath = " ",
                    },
                    Cancellation)
                .AsTask());
        await Assert.ThrowsAsync<
            ArgumentException>(
            () => publisher
                .PublishCompletedAsync(
                    valid with
                    {
                        ExpectedManifestDigest =
                            new string('A', 64),
                    },
                    Cancellation)
                .AsTask());

        Assert.Empty(
            Directory.EnumerateFileSystemEntries(
                workspace.Root));
    }

    [Fact]
    public async Task PublicStandalonePublisher_PreCanceledRequestDoesNotMutatePreparedAuthority()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace =
            new TemporaryWorkspace();
        PreparedExport prepared =
            await PrepareCompletedAsync(
                workspace,
                "pre-canceled");
        byte[] preparedBefore =
            await File.ReadAllBytesAsync(
                prepared.Paths
                    .PreparedDataPath,
                Cancellation);
        byte[] checkpointBefore =
            await File.ReadAllBytesAsync(
                prepared.Paths
                    .CheckpointPath,
                Cancellation);
        using var canceled =
            new CancellationTokenSource();
        canceled.Cancel();

        await Assert.ThrowsAnyAsync<
            OperationCanceledException>(
            () => new JsonExportPublisher()
                .PublishCompletedAsync(
                    PublicationRequest(
                        prepared),
                    canceled.Token)
                .AsTask());

        Assert.False(
            File.Exists(
                prepared.DestinationPath));
        Assert.False(
            File.Exists(
                prepared.ManifestPath));
        Assert.Equal(
            preparedBefore,
            await File.ReadAllBytesAsync(
                prepared.Paths
                    .PreparedDataPath,
                Cancellation));
        Assert.Equal(
            checkpointBefore,
            await File.ReadAllBytesAsync(
                prepared.Paths
                    .CheckpointPath,
                Cancellation));
        Assert.Empty(
            PublicationStagingFiles(
                workspace.Root));
    }

    private static async Task<PreparedExport>
        PrepareCompletedAsync(
        TemporaryWorkspace workspace,
        string stem,
        JsonExportFraming framing =
            JsonExportFraming.RootArray,
        bool empty = false)
    {
        string destination =
            workspace.PathFor(
                stem +
                (framing ==
                 JsonExportFraming.Ndjson
                    ? ".ndjson"
                    : ".json"));
        string manifest =
            workspace.PathFor(
                stem + ".manifest.json");
        JsonResumableExportRequest request =
            Request(
                destination,
                framing,
                empty);
        JsonStreamingExportResult export =
            await new JsonStreamingExporter()
                .WriteResumableAsync(
                    request,
                    Cancellation);
        (
            _,
            JsonExportPreparedOutputPaths paths
        ) = JsonExportPreparedOutputLease
            .BindPaths(
                destination);
        byte[] data =
            await File.ReadAllBytesAsync(
                paths.PreparedDataPath,
                Cancellation);
        Assert.False(
            File.Exists(destination));
        Assert.False(
            File.Exists(manifest));
        return new PreparedExport(
            destination,
            manifest,
            paths,
            export,
            data);
    }

    private static JsonPreparedExportPublicationRequest
        PublicationRequest(
        PreparedExport prepared) =>
        new()
        {
            DestinationPath =
                prepared.DestinationPath,
            ManifestPath =
                prepared.ManifestPath,
            ExpectedManifestDigest =
                prepared.Export.ManifestDigest,
        };

    private static JsonResumableExportRequest
        Request(
        string destinationPath,
        JsonExportFraming framing =
            JsonExportFraming.RootArray,
        bool empty = false)
    {
        JsonExportSourceManifest source =
            new()
            {
                Kind =
                    JsonExportContracts
                        .SourceKind,
                Version = "4.3.0",
                SnapshotByteLength =
                    4_096,
                SnapshotDigest =
                    Hash('a'),
            };
        JsonExportRow[] rows =
            empty
                ? []
                :
                [
                    Row(-7, 1),
                    Row(4, 2),
                ];
        return new JsonResumableExportRequest
        {
            DestinationPath =
                destinationPath,
            Profile =
                JsonExportProfile
                    .LosslessV1,
            Framing =
                framing,
            Source = source,
            SourceSnapshotIdentity =
                JsonExportCheckpointContracts
                    .RetainedSnapshotIdentityPrefix +
                source.SnapshotByteLength
                    .ToString(
                        CultureInfo
                            .InvariantCulture) +
                ":sha256:" +
                source.SnapshotDigest.Value,
            Table =
                new TableSchema
                {
                    TableName =
                        "publication",
                    Columns =
                    [
                        new ColumnDefinition
                        {
                            Name = "id",
                            Type =
                                DbType.Integer,
                            Nullable = false,
                        },
                    ],
                },
            OpenRows =
                (boundary, token) =>
                    Rows(
                        rows,
                        boundary,
                        token),
            MaxDataBytes = 1L << 20,
            CheckpointRowInterval = 1,
        };
    }

    private static JsonExportHashManifest Hash(
        char value) =>
        new()
        {
            Algorithm =
                JsonExportHashManifest
                    .Sha256Algorithm,
            Value = new string(
                value,
                64),
        };

    private static JsonExportRow Row(
        long rowId,
        long value) =>
        new(
            rowId,
            new[]
            {
                DbValue.FromInteger(
                    value),
            });

    private static async IAsyncEnumerable<
        JsonExportRow> Rows(
        IReadOnlyList<JsonExportRow> rows,
        long? afterRowIdExclusive,
        [EnumeratorCancellation]
        CancellationToken cancellationToken)
    {
        foreach (JsonExportRow row in rows)
        {
            cancellationToken
                .ThrowIfCancellationRequested();
            if (afterRowIdExclusive is null ||
                row.RowId >
                afterRowIdExclusive.Value)
            {
                yield return row;
                await Task.Yield();
            }
        }
    }

    [SupportedOSPlatform("windows")]
    private static async Task
        WritePrivateFileAsync(
        string path,
        ReadOnlyMemory<byte> bytes)
    {
        using WindowsIdentity identity =
            WindowsIdentity.GetCurrent(
                TokenAccessLevels.Query);
        SecurityIdentifier owner =
            identity.User ??
            throw new IOException(
                "The current Windows identity has no SID.");
        var security =
            new FileSecurity();
        security.SetOwner(owner);
        security.SetAccessRuleProtection(
            isProtected: true,
            preserveInheritance: false);
        security.AddAccessRule(
            new FileSystemAccessRule(
                owner,
                FileSystemRights
                    .FullControl,
                AccessControlType.Allow));
        await using FileStream stream =
            FileSystemAclExtensions.Create(
                new FileInfo(path),
                FileMode.CreateNew,
                FileSystemRights
                    .FullControl,
                FileShare.None,
                4_096,
                FileOptions.Asynchronous |
                FileOptions.WriteThrough,
                security);
        await stream.WriteAsync(
            bytes,
            Cancellation);
        stream.Flush(
            flushToDisk: true);
    }

    private static string[]
        PublicationStagingFiles(
        string root) =>
        Directory.GetFileSystemEntries(
            root,
            ".csharpdb-json-export-*.publish.*.next");

    private sealed record PreparedExport(
        string DestinationPath,
        string ManifestPath,
        JsonExportPreparedOutputPaths Paths,
        JsonStreamingExportResult Export,
        byte[] DataBytes);

    private sealed class
        ThrowOncePublicationFaultInjector(
        JsonExportPublicationFaultPoint point) :
        IJsonExportPublicationFaultInjector
    {
        private int thrown;

        public ValueTask InjectAsync(
            JsonExportPublicationFaultPoint
                observed,
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
                    PreparedInjectedPublicationException(
                        observed);
            }
            return ValueTask.CompletedTask;
        }
    }

    private sealed class
        PreparedInjectedPublicationException(
        JsonExportPublicationFaultPoint point) :
        Exception(
            $"Injected JSON publication failure at {point}.")
    {
    }

    private sealed class TemporaryWorkspace :
        IDisposable
    {
        internal TemporaryWorkspace()
        {
            Root =
                Path.Combine(
                    Path.GetTempPath(),
                    "csharpdb-json-prepared-publisher-tests",
                    Guid.NewGuid()
                        .ToString("N"));
            Directory.CreateDirectory(
                Root);
        }

        internal string Root { get; }

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
}
