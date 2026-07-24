using System.Globalization;
using System.Security.AccessControl;
using System.Security.Cryptography;
using System.Security.Principal;
using System.Text;
using CSharpDB.Migration.Files.Json;

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonExportPreparedOutputLeaseTests
{
    private static readonly CancellationToken Cancellation =
        TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(JsonExportFraming.RootArray)]
    [InlineData(JsonExportFraming.Ndjson)]
    public async Task NewLease_PersistsCheckpoint_AndReopensRecovered(
        JsonExportFraming framing)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        string destinationPath = workspace.PathFor(
            framing == JsonExportFraming.RootArray
                ? "items.json"
                : "items.ndjson");
        JsonExportCheckpointBinding binding =
            CreateBinding(framing);
        (byte[] prefix, JsonExportCheckpoint checkpoint) =
            CreateInitialCheckpoint(binding);
        JsonExportPreparedOutputPaths paths;

        await using (JsonExportPreparedOutputLease lease =
                     await JsonExportPreparedOutputLease.OpenAsync(
                         destinationPath,
                         binding,
                         Cancellation))
        {
            Assert.Equal(
                JsonExportPreparedOutputLeaseState.New,
                lease.State);
            Assert.Null(lease.CurrentCheckpoint);
            Assert.False(File.Exists(destinationPath));

            paths = lease.Paths;
            Assert.Matches(
                @"^\.csharpdb-json-export-[0-9a-f]{32}\.prepared$",
                Path.GetFileName(paths.PreparedDataPath));
            Assert.Equal(
                Path.GetDirectoryName(destinationPath),
                Path.GetDirectoryName(paths.PreparedDataPath));

            await lease.DataStream.WriteAsync(
                prefix,
                Cancellation);
            await lease.PersistCheckpointAsync(
                checkpoint,
                Cancellation);

            Assert.NotSame(checkpoint, lease.CurrentCheckpoint);
            Assert.Equal(0, lease.CurrentCheckpoint!.Generation);
            Assert.True(File.Exists(paths.PreparedDataPath));
            Assert.True(File.Exists(paths.CheckpointPath));
            Assert.False(File.Exists(paths.PendingCheckpointPath));
            Assert.False(File.Exists(destinationPath));
        }

        await using JsonExportPreparedOutputLease recovered =
            await JsonExportPreparedOutputLease.OpenAsync(
                destinationPath,
                binding,
                Cancellation);

        Assert.Equal(
            JsonExportPreparedOutputLeaseState.Recovered,
            recovered.State);
        Assert.Equal(
            JsonExportCheckpointSerializer.Serialize(checkpoint),
            JsonExportCheckpointSerializer.Serialize(
                recovered.CurrentCheckpoint!));
        Assert.Equal(prefix.LongLength, recovered.DataStream.Length);
        Assert.Equal(prefix.LongLength, recovered.DataStream.Position);
        Assert.False(File.Exists(destinationPath));
    }

    [Fact]
    public async Task OpenAsync_HoldsExclusivePreparedOutputLease()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        string destinationPath =
            workspace.PathFor("exclusive.json");
        JsonExportCheckpointBinding binding =
            CreateBinding(JsonExportFraming.RootArray);

        await using JsonExportPreparedOutputLease first =
            await JsonExportPreparedOutputLease.OpenAsync(
                destinationPath,
                binding,
                Cancellation);

        await Assert.ThrowsAsync<IOException>(
            () => JsonExportPreparedOutputLease.OpenAsync(
                    destinationPath,
                    binding,
                    Cancellation)
                .AsTask());
    }

    [Fact]
    public async Task UncheckpointedData_IsBlockedUntilExplicitReset()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        string destinationPath =
            workspace.PathFor("uncheckpointed.json");
        JsonExportCheckpointBinding binding =
            CreateBinding(JsonExportFraming.RootArray);
        byte[] bytes = "[{\"i\":1"u8.ToArray();

        await using (JsonExportPreparedOutputLease created =
                     await JsonExportPreparedOutputLease.OpenAsync(
                         destinationPath,
                         binding,
                         Cancellation))
        {
            await created.DataStream.WriteAsync(
                bytes,
                Cancellation);
        }

        await using JsonExportPreparedOutputLease blocked =
            await JsonExportPreparedOutputLease.OpenAsync(
                destinationPath,
                binding,
                Cancellation);

        Assert.Equal(
            JsonExportPreparedOutputLeaseState.UncheckpointedData,
            blocked.State);
        Assert.Null(blocked.CurrentCheckpoint);
        Assert.Throws<InvalidOperationException>(
            () => blocked.DataStream);

        await blocked.ResetUncheckpointedAsync(Cancellation);

        Assert.Equal(
            JsonExportPreparedOutputLeaseState.New,
            blocked.State);
        Assert.Equal(0, blocked.DataStream.Length);
        Assert.Equal(0, blocked.DataStream.Position);
        Assert.False(File.Exists(destinationPath));
    }

    [Theory]
    [InlineData(JsonExportFraming.RootArray)]
    [InlineData(JsonExportFraming.Ndjson)]
    public async Task Recovery_VerifiesPrefixThenDurablyTruncatesTail(
        JsonExportFraming framing)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        string destinationPath = workspace.PathFor(
            framing == JsonExportFraming.RootArray
                ? "tail.json"
                : "tail.ndjson");
        JsonExportCheckpointBinding binding =
            CreateBinding(framing);
        (byte[] prefix, JsonExportCheckpoint checkpoint) =
            CreateInitialCheckpoint(binding);
        JsonExportPreparedOutputPaths paths =
            await PersistInitialCheckpointAsync(
                destinationPath,
                binding,
                prefix,
                checkpoint);
        byte[] tail = "{\"partial\":"u8.ToArray();

        await using (var append = new FileStream(
                         paths.PreparedDataPath,
                         FileMode.Append,
                         FileAccess.Write,
                         FileShare.None))
        {
            await append.WriteAsync(tail, Cancellation);
            append.Flush(flushToDisk: true);
        }

        await using JsonExportPreparedOutputLease recovered =
            await JsonExportPreparedOutputLease.OpenAsync(
                destinationPath,
                binding,
                Cancellation);

        Assert.Equal(
            JsonExportPreparedOutputLeaseState.Recovered,
            recovered.State);
        Assert.Equal(prefix.LongLength, recovered.DataStream.Length);
        Assert.Equal(prefix.LongLength, recovered.DataStream.Position);
        recovered.DataStream.Position = 0;
        byte[] actual = new byte[prefix.Length];
        await recovered.DataStream.ReadExactlyAsync(
            actual,
            Cancellation);
        Assert.Equal(
            prefix,
            actual);
    }

    [Fact]
    public async Task Recovery_RejectsTamperedPrefixWithoutTruncatingTail()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        string destinationPath =
            workspace.PathFor("tampered.json");
        JsonExportCheckpointBinding binding =
            CreateBinding(JsonExportFraming.RootArray);
        (byte[] prefix, JsonExportCheckpoint checkpoint) =
            CreateInitialCheckpoint(binding);
        JsonExportPreparedOutputPaths paths =
            await PersistInitialCheckpointAsync(
                destinationPath,
                binding,
                prefix,
                checkpoint);
        byte[] tail = "unfinished"u8.ToArray();

        await using (var stream = new FileStream(
                         paths.PreparedDataPath,
                         FileMode.Open,
                         FileAccess.Write,
                         FileShare.None))
        {
            stream.Position = 0;
            await stream.WriteAsync("{"u8.ToArray(), Cancellation);
            stream.Position = stream.Length;
            await stream.WriteAsync(tail, Cancellation);
            stream.Flush(flushToDisk: true);
        }

        await Assert.ThrowsAsync<InvalidDataException>(
            () => JsonExportPreparedOutputLease.OpenAsync(
                    destinationPath,
                    binding,
                    Cancellation)
                .AsTask());

        byte[] unchanged = await File.ReadAllBytesAsync(
            paths.PreparedDataPath,
            Cancellation);
        Assert.Equal(prefix.Length + tail.Length, unchanged.Length);
        Assert.Equal((byte)'{', unchanged[0]);
    }

    [Fact]
    public async Task Recovery_RejectsCheckpointForDifferentBinding()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        string destinationPath =
            workspace.PathFor("binding.json");
        JsonExportCheckpointBinding original =
            CreateBinding(
                JsonExportFraming.RootArray,
                tableName: "items");
        (byte[] prefix, JsonExportCheckpoint checkpoint) =
            CreateInitialCheckpoint(original);
        _ = await PersistInitialCheckpointAsync(
            destinationPath,
            original,
            prefix,
            checkpoint);
        JsonExportCheckpointBinding different =
            CreateBinding(
                JsonExportFraming.RootArray,
                tableName: "other_items");

        await Assert.ThrowsAsync<InvalidDataException>(
            () => JsonExportPreparedOutputLease.OpenAsync(
                    destinationPath,
                    different,
                    Cancellation)
                .AsTask());
    }

    [Fact]
    public async Task OpenAsync_KeepsActiveAuthorityAndReclaimsStalePending()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        string destinationPath =
            workspace.PathFor("stale-pending.json");
        JsonExportCheckpointBinding binding =
            CreateBinding(JsonExportFraming.RootArray);
        (byte[] prefix, JsonExportCheckpoint checkpoint) =
            CreateInitialCheckpoint(binding);
        JsonExportPreparedOutputPaths paths =
            await PersistInitialCheckpointAsync(
                destinationPath,
                binding,
                prefix,
                checkpoint);
        await WritePrivateFileAsync(
            paths.PendingCheckpointPath,
            JsonExportCheckpointSerializer.Serialize(
                checkpoint),
            Cancellation);
        Assert.True(File.Exists(paths.PendingCheckpointPath));

        await using JsonExportPreparedOutputLease recovered =
            await JsonExportPreparedOutputLease.OpenAsync(
                destinationPath,
                binding,
                Cancellation);

        Assert.Equal(
            JsonExportPreparedOutputLeaseState.Recovered,
            recovered.State);
        Assert.Equal(0, recovered.CurrentCheckpoint!.Generation);
        Assert.Equal(prefix.LongLength, recovered.DataStream.Length);
        Assert.False(File.Exists(paths.PendingCheckpointPath));
    }

    [Fact]
    public async Task Recovery_ReconstructsCheckpointedEmptyNdjsonData()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        string destinationPath =
            workspace.PathFor("empty.ndjson");
        JsonExportCheckpointBinding binding =
            CreateBinding(JsonExportFraming.Ndjson);
        (byte[] prefix, JsonExportCheckpoint checkpoint) =
            CreateInitialCheckpoint(binding);
        JsonExportPreparedOutputPaths paths =
            await PersistInitialCheckpointAsync(
                destinationPath,
                binding,
                prefix,
                checkpoint);
        File.Delete(paths.PreparedDataPath);

        await using JsonExportPreparedOutputLease recovered =
            await JsonExportPreparedOutputLease.OpenAsync(
                destinationPath,
                binding,
                Cancellation);

        Assert.Equal(
            JsonExportPreparedOutputLeaseState.Recovered,
            recovered.State);
        Assert.Equal(0, recovered.CurrentCheckpoint!.Generation);
        Assert.Equal(0, recovered.DataStream.Length);
        Assert.True(File.Exists(paths.PreparedDataPath));
    }

    [Fact]
    public async Task PersistCheckpoint_EmitsDurableFaultBoundariesInOrder()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        string destinationPath =
            workspace.PathFor("faults.json");
        JsonExportCheckpointBinding binding =
            CreateBinding(JsonExportFraming.RootArray);
        (byte[] prefix, JsonExportCheckpoint checkpoint) =
            CreateInitialCheckpoint(binding);
        var injector =
            new RecordingCheckpointFaultInjector();

        await using JsonExportPreparedOutputLease lease =
            await JsonExportPreparedOutputLease
                .OpenWithCheckpointFaultInjectorAsync(
                    destinationPath,
                    binding,
                    injector,
                    Cancellation);
        await lease.DataStream.WriteAsync(prefix, Cancellation);

        await lease.PersistCheckpointAsync(
            checkpoint,
            Cancellation);

        Assert.Equal(
            Enum.GetValues<JsonExportCheckpointFaultPoint>(),
            injector.ObservedPoints);
        Assert.False(
            injector.ObservedCancellationCapability[1]);
        Assert.False(
            injector.ObservedCancellationCapability[2]);
        Assert.Equal(0, lease.CurrentCheckpoint!.Generation);
    }

    [Fact]
    public async Task PostActivationFailure_PoisonsLeaseAndReopensAuthority()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var workspace = new TemporaryDirectory();
        string destinationPath =
            workspace.PathFor("post-activation-failure.json");
        JsonExportCheckpointBinding binding =
            CreateBinding(JsonExportFraming.RootArray);
        (byte[] prefix, JsonExportCheckpoint checkpoint) =
            CreateInitialCheckpoint(binding);
        var injector =
            new ThrowingCheckpointFaultInjector(
                JsonExportCheckpointFaultPoint
                    .AfterActiveCheckpointReplacedBeforeResult);

        await using (JsonExportPreparedOutputLease poisoned =
                     await JsonExportPreparedOutputLease
                         .OpenWithCheckpointFaultInjectorAsync(
                             destinationPath,
                             binding,
                             injector,
                             Cancellation))
        {
            await poisoned.DataStream.WriteAsync(
                prefix,
                Cancellation);
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => poisoned.PersistCheckpointAsync(
                        checkpoint,
                        Cancellation)
                    .AsTask());

            Assert.Throws<ObjectDisposedException>(
                () => poisoned.DataStream);
            await Assert.ThrowsAsync<ObjectDisposedException>(
                () => poisoned.PersistCheckpointAsync(
                        checkpoint,
                        Cancellation)
                    .AsTask());
        }

        await using JsonExportPreparedOutputLease recovered =
            await JsonExportPreparedOutputLease.OpenAsync(
                destinationPath,
                binding,
                Cancellation);
        Assert.Equal(
            JsonExportPreparedOutputLeaseState.Recovered,
            recovered.State);
        Assert.Equal(0, recovered.CurrentCheckpoint!.Generation);
        Assert.Equal(prefix.LongLength, recovered.DataStream.Length);
    }

    [Fact]
    public void BindPaths_DistinguishesExactDestinationCase()
    {
        using var workspace = new TemporaryDirectory();
        string lower =
            workspace.PathFor("items.json");
        string upper =
            workspace.PathFor("Items.json");

        (string lowerDestination, JsonExportPreparedOutputPaths lowerPaths) =
            JsonExportPreparedOutputLease.BindPaths(lower);
        (string upperDestination, JsonExportPreparedOutputPaths upperPaths) =
            JsonExportPreparedOutputLease.BindPaths(upper);

        Assert.Equal(lower, lowerDestination);
        Assert.Equal(upper, upperDestination);
        Assert.NotEqual(
            lowerPaths.PreparedDataPath,
            upperPaths.PreparedDataPath);
        Assert.NotEqual(
            lowerPaths.CheckpointPath,
            upperPaths.CheckpointPath);
    }

    private static async Task<JsonExportPreparedOutputPaths>
        PersistInitialCheckpointAsync(
        string destinationPath,
        JsonExportCheckpointBinding binding,
        byte[] prefix,
        JsonExportCheckpoint checkpoint)
    {
        await using JsonExportPreparedOutputLease lease =
            await JsonExportPreparedOutputLease.OpenAsync(
                destinationPath,
                binding,
                Cancellation);
        await lease.DataStream.WriteAsync(prefix, Cancellation);
        await lease.PersistCheckpointAsync(
            checkpoint,
            Cancellation);
        return lease.Paths;
    }

    private static (
        byte[] Prefix,
        JsonExportCheckpoint Checkpoint)
        CreateInitialCheckpoint(
        JsonExportCheckpointBinding binding)
    {
        byte[] prefix =
            binding.Json.Framing ==
            JsonExportFraming.RootArray
                ? "["u8.ToArray()
                : [];
        using var logical =
            new JsonExportOrderedContentDigest();
        JsonExportCheckpointProgress progress =
            new()
            {
                CompletedRowCount = 0,
                LastCompletedRowId = null,
                DataPrefixByteLength =
                    prefix.LongLength,
                DataPrefixDigest =
                    HashBytes(prefix),
                LogicalPrefixAggregation =
                    JsonExportCheckpointContracts
                        .LogicalPrefixAggregation,
                SourceLogicalRowHashPrefixDigest =
                    logical.GetCurrentPrefixDigest(),
                ExportedLogicalRowHashPrefixDigest =
                    logical.GetCurrentPrefixDigest(),
            };
        return (
            prefix,
            new JsonExportCheckpoint
            {
                Generation = 0,
                Phase =
                    JsonExportCheckpointPhase.Writing,
                Binding = binding,
                BindingDigest =
                    JsonExportCheckpointSerializer
                        .ComputeBindingDigest(binding),
                Progress = progress,
                Completion = null,
            });
    }

    private static JsonExportCheckpointBinding
        CreateBinding(
        JsonExportFraming framing,
        string tableName = "items")
    {
        JsonExportColumnManifest[] columns =
        [
            new JsonExportColumnManifest
            {
                Ordinal = 0,
                SourceName = "i",
                PropertyName = "i",
                DatabaseType =
                    JsonExportDatabaseType.Integer,
                Nullable = false,
                ValueEncoding =
                    JsonExportContracts
                        .IntegerValueEncoding,
                MaximumDecodedBytes = 0,
            },
        ];
        JsonExportSourceManifest source =
            new()
            {
                Kind =
                    JsonExportContracts.SourceKind,
                Version = "4.3.0",
                SnapshotByteLength = 128,
                SnapshotDigest = Hash('a'),
            };

        return new JsonExportCheckpointBinding
        {
            Profile = JsonExportProfile.LosslessV1,
            Source = source,
            SourceSnapshotIdentity =
                JsonExportCheckpointContracts
                    .RetainedSnapshotIdentityPrefix +
                source.SnapshotByteLength.ToString(
                    CultureInfo.InvariantCulture) +
                ":" +
                JsonExportHashManifest
                    .Sha256Algorithm +
                ":" +
                source.SnapshotDigest.Value,
            Table =
                new JsonExportTableManifest
                {
                    Name = tableName,
                    SchemaContract =
                        JsonExportContracts.Schema,
                    SchemaDigest =
                        JsonExportManifestSerializer
                            .ComputeSchemaDigest(
                                columns),
                    RowOrder =
                        JsonExportContracts.RowOrder,
                    Columns = columns,
                },
            Json =
                new JsonExportFormatManifest
                {
                    Encoding =
                        JsonExportContracts.Encoding,
                    HasByteOrderMark = false,
                    Culture =
                        JsonExportContracts.Culture,
                    Framing = framing,
                    Compact = true,
                    PropertyOrder =
                        JsonExportContracts
                            .PropertyOrder,
                    Newline =
                        JsonExportContracts.Newline,
                    HasFinalNewline = true,
                    NullEncoding =
                        JsonExportContracts
                            .NullEncoding,
                    TextEscape =
                        JsonExportContracts.TextEscape,
                    MaxDataBytes = 1L << 20,
                    MaximumDecodedBlobBytes =
                        JsonExportContracts
                            .MaximumSupportedDecodedBlobBytes,
                    MaximumValueBytes =
                        JsonInputContracts
                            .MaximumValueBytes,
                    MaximumStringBytes =
                        JsonInputContracts
                            .MaximumStringBytes,
                    MaximumPropertyNameBytes =
                        JsonInputContracts
                            .MaximumPropertyNameBytes,
                    MaximumPropertiesPerObject =
                        JsonInputContracts
                            .MaximumPropertiesPerObject,
                },
        };
    }

    private static JsonExportHashManifest Hash(
        char value) =>
        new()
        {
            Algorithm =
                JsonExportHashManifest
                    .Sha256Algorithm,
            Value = new string(value, 64),
        };

    private static JsonExportHashManifest HashBytes(
        ReadOnlySpan<byte> bytes) =>
        new()
        {
            Algorithm =
                JsonExportHashManifest
                    .Sha256Algorithm,
            Value =
                Convert.ToHexString(
                        SHA256.HashData(bytes))
                    .ToLowerInvariant(),
        };

    private static async Task WritePrivateFileAsync(
        string path,
        byte[] bytes,
        CancellationToken cancellationToken)
    {
        if (!OperatingSystem.IsWindows())
        {
            throw new PlatformNotSupportedException(
                "Private prepared-output test files require Windows ACLs.");
        }

        using WindowsIdentity identity =
            WindowsIdentity.GetCurrent(
                TokenAccessLevels.Query);
        SecurityIdentifier owner =
            identity.User ??
            throw new InvalidOperationException(
                "The current Windows test identity has no security identifier.");
        var security = new FileSecurity();
        security.SetOwner(owner);
        security.SetAccessRuleProtection(
            isProtected: true,
            preserveInheritance: false);
        security.AddAccessRule(
            new FileSystemAccessRule(
                owner,
                FileSystemRights.FullControl,
                AccessControlType.Allow));
        await using FileStream stream =
            FileSystemAclExtensions.Create(
                new FileInfo(path),
                FileMode.CreateNew,
                FileSystemRights.FullControl,
                FileShare.None,
                bufferSize: 4096,
                FileOptions.Asynchronous |
                FileOptions.WriteThrough,
                security);
        await stream.WriteAsync(
            bytes,
            cancellationToken);
        stream.Flush(flushToDisk: true);
    }

    private sealed class TemporaryDirectory :
        IDisposable
    {
        public TemporaryDirectory()
        {
            Root =
                Path.Combine(
                    Path.GetTempPath(),
                    "csharpdb-json-export-lease-tests",
                    Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(Root);
        }

        private string Root { get; }

        public string PathFor(string leaf) =>
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

    private sealed class RecordingCheckpointFaultInjector :
        IJsonExportCheckpointFaultInjector
    {
        public List<JsonExportCheckpointFaultPoint>
            ObservedPoints
        {
            get;
        } =
            [];

        public List<bool>
            ObservedCancellationCapability
        {
            get;
        } =
            [];

        public ValueTask InjectAsync(
            JsonExportCheckpointFaultPoint point,
            CancellationToken cancellationToken)
        {
            cancellationToken
                .ThrowIfCancellationRequested();
            ObservedPoints.Add(point);
            ObservedCancellationCapability.Add(
                cancellationToken.CanBeCanceled);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class ThrowingCheckpointFaultInjector(
        JsonExportCheckpointFaultPoint faultPoint) :
        IJsonExportCheckpointFaultInjector
    {
        public ValueTask InjectAsync(
            JsonExportCheckpointFaultPoint point,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (point == faultPoint)
            {
                throw new InvalidOperationException(
                    "Injected JSON checkpoint fault.");
            }

            return ValueTask.CompletedTask;
        }
    }
}
