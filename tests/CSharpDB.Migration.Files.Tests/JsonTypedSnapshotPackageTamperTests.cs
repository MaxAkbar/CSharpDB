using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CSharpDB.Migration;
using CSharpDB.Migration.Files.Json;

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonTypedSnapshotPackageTamperTests
{
    private const int HeaderSize = 112;
    private const int VersionOffset = 8;
    private const int HeaderSizeOffset = 12;
    private const int ManifestLengthOffset = 16;
    private const int IntentLengthOffset = 20;
    private const int FlagsOffset = 24;
    private const int ReservedOffset = 28;
    private const int SnapshotLengthOffset = 32;
    private const int ManifestHashOffset = 40;
    private const int IntentHashOffset = 72;
    private const int ReservedTailOffset = 104;

    private static string TargetVersion =>
        CSharpDbCapabilityCatalogLoader.CurrentTargetVersion;

    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task TrustedPinWinsBeforeMalformedManifestIntentOrWorkspaceUse()
    {
        using var workspace = new TemporaryDirectory();
        PackageOrigin origin =
            await CreatePackageAsync(workspace, "trusted-pin");
        byte[] bytes =
            await File.ReadAllBytesAsync(
                origin.PackagePath,
                Cancellation);
        int manifestOffset = HeaderSize;
        int intentOffset =
            HeaderSize + ManifestLength(bytes);
        bytes[manifestOffset] ^= 0x40;
        bytes[intentOffset] ^= 0x20;
        await File.WriteAllBytesAsync(
            origin.PackagePath,
            bytes,
            Cancellation);
        string missingWorkspace =
            workspace.PathFor(
                Path.Combine("missing", "workspace"));
        string wrongPin =
            "sha256:" + new string('0', 64);
        Assert.NotEqual(
            origin.Manifest.ManifestDigest,
            wrongPin);

        JsonSnapshotPackageException error =
            await Assert.ThrowsAsync<
                JsonSnapshotPackageException>(
                async () =>
                {
                    await using JsonTypedSnapshotPackageSession
                        session =
                            await JsonTypedSnapshotPackage
                                .OpenAsync(
                                    origin.PackagePath,
                                    new JsonSnapshotPackageOpenOptions
                                    {
                                        WorkspacePath =
                                            missingWorkspace,
                                        MaxSourceBytes =
                                            1024 * 1024,
                                        ExpectedManifestDigest =
                                            wrongPin,
                                    },
                                    Cancellation);
                });

        Assert.Equal(
            JsonSnapshotPackageRules.IntegrityMismatch,
            error.RuleId);
        Assert.False(
            Directory.Exists(missingWorkspace));
        Assert.Equal(
            bytes,
            await File.ReadAllBytesAsync(
                origin.PackagePath,
                Cancellation));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task SingleByteManifestIntentOrRawTamperIsRejected(
        int section)
    {
        using var workspace = new TemporaryDirectory();
        PackageOrigin origin =
            await CreatePackageAsync(
                workspace,
                "section-" + section);
        byte[] bytes =
            await File.ReadAllBytesAsync(
                origin.PackagePath,
                Cancellation);
        int manifestLength = ManifestLength(bytes);
        int intentLength = IntentLength(bytes);
        int offset = section switch
        {
            0 => HeaderSize + manifestLength / 2,
            1 => HeaderSize +
                 manifestLength +
                 intentLength / 2,
            2 => HeaderSize +
                 manifestLength +
                 intentLength,
            _ => throw new ArgumentOutOfRangeException(
                nameof(section)),
        };
        bytes[offset] ^= 0x01;
        await File.WriteAllBytesAsync(
            origin.PackagePath,
            bytes,
            Cancellation);

        await AssertOpenFailsAndPreservesAsync(
            workspace,
            origin.PackagePath,
            JsonSnapshotPackageRules.IntegrityMismatch);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    public async Task UnsupportedHeaderAndReservedFieldsAreRejected(
        int mutation)
    {
        using var workspace = new TemporaryDirectory();
        PackageOrigin origin =
            await CreatePackageAsync(
                workspace,
                "header-" + mutation);
        byte[] bytes =
            await File.ReadAllBytesAsync(
                origin.PackagePath,
                Cancellation);
        switch (mutation)
        {
            case 0:
                bytes[0] ^= 0x80;
                break;
            case 1:
                BinaryPrimitives.WriteUInt32BigEndian(
                    bytes.AsSpan(
                        VersionOffset,
                        sizeof(uint)),
                    1);
                break;
            case 2:
                BinaryPrimitives.WriteUInt32BigEndian(
                    bytes.AsSpan(
                        HeaderSizeOffset,
                        sizeof(uint)),
                    HeaderSize - 1);
                break;
            case 3:
                BinaryPrimitives.WriteUInt32BigEndian(
                    bytes.AsSpan(
                        FlagsOffset,
                        sizeof(uint)),
                    1);
                break;
            case 4:
                BinaryPrimitives.WriteUInt32BigEndian(
                    bytes.AsSpan(
                        ReservedOffset,
                        sizeof(uint)),
                    1);
                break;
            case 5:
                bytes[ReservedTailOffset] = 1;
                break;
            default:
                throw new ArgumentOutOfRangeException(
                    nameof(mutation));
        }
        await File.WriteAllBytesAsync(
            origin.PackagePath,
            bytes,
            Cancellation);

        await AssertOpenFailsAndPreservesAsync(
            workspace,
            origin.PackagePath,
            JsonSnapshotPackageRules.InvalidFormat);
    }

    [Theory]
    [InlineData(
        0,
        JsonSnapshotPackageRules.IntegrityMismatch)]
    [InlineData(
        1,
        JsonSnapshotPackageRules.SizeLimitExceeded)]
    [InlineData(
        2,
        JsonSnapshotPackageRules.SizeLimitExceeded)]
    [InlineData(
        3,
        JsonSnapshotPackageRules.InvalidFormat)]
    [InlineData(
        4,
        JsonSnapshotPackageRules.InvalidFormat)]
    public async Task IntentHeaderAndPhysicalGeometryTamperIsRejected(
        int mutation,
        string expectedRule)
    {
        using var workspace = new TemporaryDirectory();
        PackageOrigin origin =
            await CreatePackageAsync(
                workspace,
                "geometry-" + mutation);
        byte[] bytes =
            await File.ReadAllBytesAsync(
                origin.PackagePath,
                Cancellation);
        switch (mutation)
        {
            case 0:
                bytes[IntentHashOffset] ^= 0x01;
                break;
            case 1:
                BinaryPrimitives.WriteUInt32BigEndian(
                    bytes.AsSpan(
                        IntentLengthOffset,
                        sizeof(uint)),
                    0);
                break;
            case 2:
                BinaryPrimitives.WriteUInt32BigEndian(
                    bytes.AsSpan(
                        IntentLengthOffset,
                        sizeof(uint)),
                    uint.MaxValue);
                break;
            case 3:
                bytes = bytes[..^1];
                break;
            case 4:
                Array.Resize(ref bytes, bytes.Length + 1);
                bytes[^1] = 0x7f;
                break;
            default:
                throw new ArgumentOutOfRangeException(
                    nameof(mutation));
        }
        await File.WriteAllBytesAsync(
            origin.PackagePath,
            bytes,
            Cancellation);

        await AssertOpenFailsAndPreservesAsync(
            workspace,
            origin.PackagePath,
            expectedRule);
    }

    [Fact]
    public async Task OpenAcceptsExactSourceLimitAndRejectsOneUnder()
    {
        using var workspace = new TemporaryDirectory();
        PackageOrigin origin =
            await CreatePackageAsync(workspace, "source-limit");

        await using (JsonTypedSnapshotPackageSession session =
            await JsonTypedSnapshotPackage.OpenAsync(
                origin.PackagePath,
                new JsonSnapshotPackageOpenOptions
                {
                    WorkspacePath = workspace.Root,
                    MaxSourceBytes =
                        origin.Manifest.ContentLength,
                    ExpectedManifestDigest =
                        origin.Manifest.ManifestDigest,
                },
                Cancellation))
        {
            Assert.Equal(
                origin.Manifest.ContentLength,
                session.Manifest.ContentLength);
        }
        Assert.Empty(
            Directory.EnumerateDirectories(
                workspace.Root));

        JsonSnapshotPackageException error =
            await Assert.ThrowsAsync<
                JsonSnapshotPackageException>(
                async () =>
                {
                    await using JsonTypedSnapshotPackageSession
                        session =
                            await JsonTypedSnapshotPackage
                                .OpenAsync(
                                    origin.PackagePath,
                                    new JsonSnapshotPackageOpenOptions
                                    {
                                        WorkspacePath =
                                            workspace.Root,
                                        MaxSourceBytes =
                                            origin.Manifest
                                                .ContentLength -
                                            1,
                                    },
                                    Cancellation);
                });
        Assert.Equal(
            JsonSnapshotPackageRules.SizeLimitExceeded,
            error.RuleId);
        Assert.Empty(
            Directory.EnumerateDirectories(
                workspace.Root));
    }

    [Fact]
    public async Task ExistingDestinationIsNeverOverwrittenOrLeaked()
    {
        using var workspace = new TemporaryDirectory();
        await using WritableOrigin origin =
            await WritableOrigin.CreateAsync(
                workspace,
                "existing");
        string packagePath = workspace.PathFor(
            "existing" +
            JsonTypedSnapshotPackage.FileExtension);
        byte[] existing = [0xde, 0xad, 0xbe, 0xef];
        await File.WriteAllBytesAsync(
            packagePath,
            existing,
            Cancellation);

        await Assert.ThrowsAsync<IOException>(
            async () =>
                await JsonTypedSnapshotPackage.WriteAsync(
                    packagePath,
                    origin.Snapshot,
                    origin.Schema,
                    TargetVersion,
                    Cancellation));

        Assert.Equal(
            existing,
            await File.ReadAllBytesAsync(
                packagePath,
                Cancellation));
        Assert.Empty(
            Directory.EnumerateFiles(
                workspace.Root,
                ".csdbjson-v2-*.tmp"));
        await origin.Snapshot.VerifyIntegrityAsync(
            Cancellation);
    }

    [Fact]
    public async Task PreCanceledWriteLeavesNoDestinationOrTemporaryFile()
    {
        using var workspace = new TemporaryDirectory();
        await using WritableOrigin origin =
            await WritableOrigin.CreateAsync(
                workspace,
                "cancel-write");
        string packagePath = workspace.PathFor(
            "cancel-write" +
            JsonTypedSnapshotPackage.FileExtension);
        using var canceled =
            new CancellationTokenSource();
        canceled.Cancel();

        await Assert.ThrowsAnyAsync<
            OperationCanceledException>(
                async () =>
                    await JsonTypedSnapshotPackage
                        .WriteAsync(
                            packagePath,
                            origin.Snapshot,
                            origin.Schema,
                            TargetVersion,
                            canceled.Token));

        Assert.False(File.Exists(packagePath));
        Assert.Empty(
            Directory.EnumerateFiles(
                workspace.Root,
                ".csdbjson-v2-*.tmp"));
        await origin.Snapshot.VerifyIntegrityAsync(
            Cancellation);
    }

    [Fact]
    public async Task PreCanceledOpenLeavesPackageAndWorkspaceUnchanged()
    {
        using var workspace = new TemporaryDirectory();
        PackageOrigin origin =
            await CreatePackageAsync(
                workspace,
                "cancel-open");
        byte[] packageBytes =
            await File.ReadAllBytesAsync(
                origin.PackagePath,
                Cancellation);
        string openWorkspace =
            workspace.CreateDirectory(
                "open-workspace");
        using var canceled =
            new CancellationTokenSource();
        canceled.Cancel();

        await Assert.ThrowsAnyAsync<
            OperationCanceledException>(
                async () =>
                {
                    await using JsonTypedSnapshotPackageSession
                        session =
                            await JsonTypedSnapshotPackage
                                .OpenAsync(
                                    origin.PackagePath,
                                    new JsonSnapshotPackageOpenOptions
                                    {
                                        WorkspacePath =
                                            openWorkspace,
                                        MaxSourceBytes =
                                            1024 * 1024,
                                        ExpectedManifestDigest =
                                            origin.Manifest
                                                .ManifestDigest,
                                    },
                                    canceled.Token);
                });

        Assert.Equal(
            packageBytes,
            await File.ReadAllBytesAsync(
                origin.PackagePath,
                Cancellation));
        Assert.Empty(
            Directory.EnumerateFileSystemEntries(
                openWorkspace));
    }

    [Fact]
    public async Task RepeatedDisposeAsyncIsIdempotent()
    {
        using var workspace = new TemporaryDirectory();
        PackageOrigin origin =
            await CreatePackageAsync(
                workspace,
                "dispose");
        JsonTypedSnapshotPackageSession session =
            await JsonTypedSnapshotPackage.OpenAsync(
                origin.PackagePath,
                new JsonSnapshotPackageOpenOptions
                {
                    WorkspacePath = workspace.Root,
                    MaxSourceBytes = 1024 * 1024,
                    ExpectedManifestDigest =
                        origin.Manifest.ManifestDigest,
                },
                Cancellation);
        Assert.Single(
            Directory.EnumerateDirectories(
                workspace.Root));

        await Task.WhenAll(
            session.DisposeAsync().AsTask(),
            session.DisposeAsync().AsTask());
        await session.DisposeAsync();

        Assert.Empty(
            Directory.EnumerateDirectories(
                workspace.Root));
        Assert.True(File.Exists(origin.PackagePath));
    }

    [Fact]
    public async Task DirectoryInputIsRejectedAsUnsafePath()
    {
        using var workspace = new TemporaryDirectory();
        string directoryPath =
            workspace.CreateDirectory(
                "not-a-package");

        JsonSnapshotPackageException error =
            await Assert.ThrowsAsync<
                JsonSnapshotPackageException>(
                async () =>
                {
                    await using JsonTypedSnapshotPackageSession
                        session =
                            await JsonTypedSnapshotPackage
                                .OpenAsync(
                                    directoryPath,
                                    new JsonSnapshotPackageOpenOptions
                                    {
                                        WorkspacePath =
                                            workspace.Root,
                                        MaxSourceBytes =
                                            1024 * 1024,
                                    },
                                    Cancellation);
                });
        Assert.Equal(
            JsonSnapshotPackageRules.UnsafePath,
            error.RuleId);
    }

    [Theory]
    [InlineData(
        0,
        JsonSnapshotPackageRules.PolicyMismatch)]
    [InlineData(
        1,
        JsonSnapshotPackageRules.PolicyMismatch)]
    [InlineData(
        2,
        JsonSnapshotPackageRules.PolicyMismatch)]
    [InlineData(
        3,
        JsonSnapshotPackageRules.IntegrityMismatch)]
    [InlineData(
        4,
        JsonSnapshotPackageRules.PolicyMismatch)]
    [InlineData(
        5,
        JsonSnapshotPackageRules.PolicyMismatch)]
    [InlineData(
        6,
        JsonSnapshotPackageRules.IntegrityMismatch)]
    public async Task ResignedSemanticTamperIsRejected(
        int mutation,
        string expectedRule)
    {
        using var workspace = new TemporaryDirectory();
        PackageOrigin origin =
            await CreatePackageAsync(
                workspace,
                "semantic-" + mutation);
        byte[] manifestBytes =
            ReadManifest(
                await File.ReadAllBytesAsync(
                    origin.PackagePath,
                    Cancellation));
        JsonTypedSnapshotPackageManifestPayload payload =
            JsonTypedSnapshotPackageManifestSerializer
                .Deserialize(manifestBytes);

        payload = mutation switch
        {
            0 => payload with
            {
                Contracts = payload.Contracts with
                {
                    TypedSchema =
                        "csharpdb-json-typed-table-schema-v999",
                },
            },
            1 => payload with
            {
                TypedIntent = payload.TypedIntent with
                {
                    MaxDecodedBinaryBytes =
                        payload.TypedIntent
                            .MaxDecodedBinaryBytes + 1,
                },
            },
            2 => payload with
            {
                TypedIntent = payload.TypedIntent with
                {
                    ColumnCount =
                        payload.TypedIntent.ColumnCount + 1,
                },
            },
            3 => payload with
            {
                Source = payload.Source with
                {
                    OptionsDigest = ZeroPrefixedDigest(),
                },
            },
            4 => payload with
            {
                Catalog = payload.Catalog with
                {
                    Digest = new string('0', 64),
                },
            },
            5 => payload with
            {
                Inference = payload.Inference with
                {
                    ColumnOverrides =
                    [
                        new JsonSnapshotPackageColumnOverrideManifest
                        {
                            ColumnIndex = 0,
                            ExpectedPropertyName = "typed",
                            LogicalType =
                                JsonTableColumnLogicalType.Text,
                            Nullable = false,
                            MissingPolicy =
                                JsonMissingPropertyPolicy.Reject,
                        },
                        .. payload.Inference.ColumnOverrides,
                    ],
                },
            },
            6 => ChangedSnapshotDigest(payload),
            _ => throw new ArgumentOutOfRangeException(
                nameof(mutation)),
        };
        byte[] replacement =
            JsonTypedSnapshotPackageManifestSerializer
                .Serialize(payload);
        await ReplaceManifestAsync(
            origin.PackagePath,
            replacement);

        await AssertOpenFailsAndPreservesAsync(
            workspace,
            origin.PackagePath,
            expectedRule);
    }

    [Fact]
    public async Task NullOrdinaryOverrideIsStableInvalidFormat()
    {
        using var workspace = new TemporaryDirectory();
        PackageOrigin origin =
            await CreatePackageAsync(
                workspace,
                "null-override");
        byte[] originalManifest =
            ReadManifest(
                await File.ReadAllBytesAsync(
                    origin.PackagePath,
                    Cancellation));
        string json = Encoding.UTF8.GetString(
            originalManifest);
        const string marker = "\"columnOverrides\":[";
        int offset = json.IndexOf(
            marker,
            StringComparison.Ordinal);
        Assert.True(offset >= 0);
        json = json.Insert(
            offset + marker.Length,
            "null,");
        json = RecomputeInnerDigest(json);
        await ReplaceManifestAsync(
            origin.PackagePath,
            Encoding.UTF8.GetBytes(json));

        await AssertOpenFailsAndPreservesAsync(
            workspace,
            origin.PackagePath,
            JsonSnapshotPackageRules.InvalidFormat);
    }

    private static JsonTypedSnapshotPackageManifestPayload
        ChangedSnapshotDigest(
            JsonTypedSnapshotPackageManifestPayload payload)
    {
        string digest = ZeroPrefixedDigest();
        return payload with
        {
            Snapshot = payload.Snapshot with
            {
                ContentDigest = digest,
                SnapshotIdentity =
                    $"{JsonSourceSnapshot.IdentityAlgorithm}:{digest}:bytes:{payload.Snapshot.ContentLength}",
            },
        };
    }

    private static async Task<PackageOrigin>
        CreatePackageAsync(
            TemporaryDirectory workspace,
            string name)
    {
        await using WritableOrigin origin =
            await WritableOrigin.CreateAsync(
                workspace,
                name);
        string packagePath = workspace.PathFor(
            name +
            JsonTypedSnapshotPackage.FileExtension);
        JsonTypedSnapshotPackageManifest manifest =
            await JsonTypedSnapshotPackage.WriteAsync(
                packagePath,
                origin.Snapshot,
                origin.Schema,
                TargetVersion,
                Cancellation);
        return new PackageOrigin(packagePath, manifest);
    }

    private static async Task
        AssertOpenFailsAndPreservesAsync(
            TemporaryDirectory workspace,
            string packagePath,
            string expectedRule)
    {
        byte[] before =
            await File.ReadAllBytesAsync(
                packagePath,
                Cancellation);
        JsonSnapshotPackageException error =
            await Assert.ThrowsAsync<
                JsonSnapshotPackageException>(
                async () =>
                {
                    await using JsonTypedSnapshotPackageSession
                        session =
                            await JsonTypedSnapshotPackage
                                .OpenAsync(
                                    packagePath,
                                    new JsonSnapshotPackageOpenOptions
                                    {
                                        WorkspacePath =
                                            workspace.Root,
                                        MaxSourceBytes =
                                            1024 * 1024,
                                    },
                                    Cancellation);
                });
        Assert.Equal(expectedRule, error.RuleId);
        Assert.Equal(
            before,
            await File.ReadAllBytesAsync(
                packagePath,
                Cancellation));
        Assert.Empty(
            Directory.EnumerateDirectories(
                workspace.Root));
    }

    private static byte[] ReadManifest(
        ReadOnlySpan<byte> packageBytes) =>
        packageBytes.Slice(
                HeaderSize,
                ManifestLength(packageBytes))
            .ToArray();

    private static async Task ReplaceManifestAsync(
        string packagePath,
        byte[] replacementManifest)
    {
        byte[] original =
            await File.ReadAllBytesAsync(
                packagePath,
                Cancellation);
        int oldManifestLength = ManifestLength(original);
        int intentLength = IntentLength(original);
        int oldIntentOffset =
            HeaderSize + oldManifestLength;
        int oldSnapshotOffset =
            oldIntentOffset + intentLength;
        ReadOnlySpan<byte> intent = original.AsSpan(
            oldIntentOffset,
            intentLength);
        ReadOnlySpan<byte> snapshot =
            original.AsSpan(oldSnapshotOffset);
        byte[] header =
            original.AsSpan(0, HeaderSize).ToArray();
        BinaryPrimitives.WriteUInt32BigEndian(
            header.AsSpan(
                ManifestLengthOffset,
                sizeof(uint)),
            checked((uint)replacementManifest.Length));
        SHA256.HashData(replacementManifest).CopyTo(
            header.AsSpan(
                ManifestHashOffset,
                SHA256.HashSizeInBytes));
        byte[] replacement = new byte[checked(
            HeaderSize +
            replacementManifest.Length +
            intent.Length +
            snapshot.Length)];
        header.CopyTo(replacement, 0);
        replacementManifest.CopyTo(
            replacement,
            HeaderSize);
        intent.CopyTo(
            replacement.AsSpan(
                HeaderSize +
                replacementManifest.Length));
        snapshot.CopyTo(
            replacement.AsSpan(
                HeaderSize +
                replacementManifest.Length +
                intent.Length));
        await File.WriteAllBytesAsync(
            packagePath,
            replacement,
            Cancellation);
    }

    private static string RecomputeInnerDigest(
        string envelope)
    {
        string currentDigest = EnvelopeDigest(envelope);
        string digestProperty =
            $"\"digest\":\"{currentDigest}\",";
        int digestPropertyOffset = envelope.IndexOf(
            digestProperty,
            StringComparison.Ordinal);
        Assert.True(digestPropertyOffset > 0);
        string digestInput = envelope.Remove(
            digestPropertyOffset,
            digestProperty.Length);
        string replacementDigest =
            Convert.ToHexString(
                    SHA256.HashData(
                        Encoding.UTF8.GetBytes(
                            digestInput)))
                .ToLowerInvariant();
        int valueOffset =
            digestPropertyOffset +
            "\"digest\":\"".Length;
        return envelope
            .Remove(valueOffset, currentDigest.Length)
            .Insert(valueOffset, replacementDigest);
    }

    private static string EnvelopeDigest(string envelope)
    {
        const string marker = "\"digest\":\"";
        int offset = envelope.IndexOf(
            marker,
            StringComparison.Ordinal);
        Assert.True(offset > 0);
        return envelope.Substring(
            offset + marker.Length,
            64);
    }

    private static string ZeroPrefixedDigest() =>
        "sha256:" + new string('0', 64);

    private static int ManifestLength(
        ReadOnlySpan<byte> packageBytes) =>
        checked((int)
            BinaryPrimitives.ReadUInt32BigEndian(
                packageBytes.Slice(
                    ManifestLengthOffset,
                    sizeof(uint))));

    private static int IntentLength(
        ReadOnlySpan<byte> packageBytes) =>
        checked((int)
            BinaryPrimitives.ReadUInt32BigEndian(
                packageBytes.Slice(
                    IntentLengthOffset,
                    sizeof(uint))));

    private sealed record PackageOrigin(
        string PackagePath,
        JsonTypedSnapshotPackageManifest Manifest);

    private sealed class WritableOrigin :
        IAsyncDisposable
    {
        private readonly string sidecarPath;
        private bool disposed;

        private WritableOrigin(
            string sidecarPath,
            JsonSourceSnapshot snapshot,
            JsonTypedTableSchemaInferenceResult schema)
        {
            this.sidecarPath = sidecarPath;
            Snapshot = snapshot;
            Schema = schema;
        }

        internal JsonSourceSnapshot Snapshot { get; }

        internal JsonTypedTableSchemaInferenceResult Schema
        {
            get;
        }

        internal static async Task<WritableOrigin>
            CreateAsync(
                TemporaryDirectory workspace,
                string name)
        {
            string sidecarPath = workspace.PathFor(
                name +
                JsonTypedIntentSidecar.FileExtension);
            JsonSourceSnapshot? snapshot = null;
            try
            {
                snapshot =
                    await JsonSourceSnapshot.CreateAsync(
                        new MemoryStream(
                            Encoding.UTF8.GetBytes(
                                """
                                [
                                  {"typed":"1","ordinary":"alpha"},
                                  {"typed":"2","ordinary":"beta"}
                                ]
                                """)),
                        new JsonSourceSnapshotOptions
                        {
                            WorkspacePath =
                                workspace.Root,
                            MaxSourceBytes =
                                1024 * 1024,
                        },
                        Cancellation);
                JsonSourceBinding binding =
                    await JsonSourceBinding.CreateAsync(
                        snapshot,
                        new JsonStreamingReaderOptions
                        {
                            Framing =
                                JsonInputFraming.RootArray,
                            MaxValueBytes = 256 * 1024,
                            MaxDepth = 32,
                            MaxPropertiesPerObject = 256,
                            MaxArrayElements = 1024,
                            MaxTotalNodes = 2048,
                            MaxPropertyNameBytes = 8192,
                            MaxStringBytes = 128 * 1024,
                            MaxNumberBytes = 8192,
                            LeaveOpen = true,
                        },
                        "typed-package-tamper/" + name,
                        Cancellation);
                JsonTypedIntentManifest intent =
                    await JsonTypedIntentSidecar.WriteAsync(
                        sidecarPath,
                        binding,
                        new JsonTypedIntentOptions
                        {
                            Columns =
                            [
                                new JsonTypedColumnIntent
                                {
                                    ColumnIndex = 0,
                                    ExpectedPropertyName =
                                        "typed",
                                    Codec =
                                        JsonTypedValueCodec
                                            .Int64String,
                                    Nullable = false,
                                },
                            ],
                            MaxDecodedBinaryBytes = 1024,
                            MaxDecimalDigits = 64,
                        },
                        Cancellation);
                JsonTypedTableSchemaInferenceResult schema =
                    await JsonTypedTableSchemaInferer
                        .InferAsync(
                            binding,
                            snapshot,
                            intent,
                            maxProfileRecords: 100,
                            new JsonTableSchemaInferenceOptions
                            {
                                ColumnOverrides =
                                [
                                    new JsonTableColumnSchemaOverride
                                    {
                                        ColumnIndex = 1,
                                        ExpectedPropertyName =
                                            "ordinary",
                                        LogicalType =
                                            JsonTableColumnLogicalType
                                                .Text,
                                        Nullable = false,
                                    },
                                ],
                            },
                            Cancellation);
                return new WritableOrigin(
                    sidecarPath,
                    snapshot,
                    schema);
            }
            catch
            {
                if (snapshot is not null)
                    await snapshot.DisposeAsync();
                if (File.Exists(sidecarPath))
                    File.Delete(sidecarPath);
                throw;
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (disposed)
                return;
            disposed = true;
            await Snapshot.DisposeAsync();
            if (File.Exists(sidecarPath))
                File.Delete(sidecarPath);
        }
    }

    private sealed class TemporaryDirectory : IDisposable
    {
        internal TemporaryDirectory()
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                "csharpdb-json-typed-package-tamper-" +
                Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(Root);
        }

        internal string Root { get; }

        internal string PathFor(string name) =>
            Path.Combine(Root, name);

        internal string CreateDirectory(string name)
        {
            string path = PathFor(name);
            Directory.CreateDirectory(path);
            return path;
        }

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
