using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CSharpDB.Migration;
using CSharpDB.Migration.Files.Json;

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonSnapshotPackageSemanticTamperTests
{
    private const int HeaderSize = 64;
    private const int VersionOffset = 8;
    private const int HeaderSizeOffset = 12;
    private const int ManifestLengthOffset = 16;
    private const int FlagsOffset = 20;
    private const int SnapshotLengthOffset = 24;
    private const int ManifestHashOffset = 32;

    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    private static string TargetVersion =>
        CSharpDbCapabilityCatalogLoader.CurrentTargetVersion;

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
        JsonSnapshotPackageRules.PolicyMismatch)]
    [InlineData(
        4,
        JsonSnapshotPackageRules.PolicyMismatch)]
    [InlineData(
        5,
        JsonSnapshotPackageRules.PolicyMismatch)]
    [InlineData(
        6,
        JsonSnapshotPackageRules.PolicyMismatch)]
    [InlineData(
        7,
        JsonSnapshotPackageRules.PolicyMismatch)]
    [InlineData(
        8,
        JsonSnapshotPackageRules.PolicyMismatch)]
    [InlineData(
        9,
        JsonSnapshotPackageRules.IntegrityMismatch)]
    public async Task SupportedSemanticTamperWithValidInnerAndOuterDigestsIsRejected(
        int mutation,
        string expectedRule)
    {
        using var workspace = new TemporaryWorkspace();
        string packagePath =
            await CreatePackageAsync(workspace);
        string json = Encoding.UTF8.GetString(
            await ReadManifestAsync(packagePath));
        using JsonDocument document =
            JsonDocument.Parse(json);
        JsonElement payload =
            document.RootElement.GetProperty("payload");

        switch (mutation)
        {
            case 0:
                json = ReplaceOnce(
                    json,
                    $"\"schema\":\"{JsonTableSchemaInferenceResult.AlgorithmId}\"",
                    "\"schema\":\"csharpdb-json-table-schema-v999\"");
                break;

            case 1:
                json = ReplaceOnce(
                    json,
                    "\"framing\":\"rootArray\"",
                    "\"framing\":\"multipleValues\"");
                break;

            case 2:
                json = ReplaceOnce(
                    json,
                    "\"maxDepth\":32",
                    "\"maxDepth\":31");
                break;

            case 3:
                json = ReplaceOnce(
                    json,
                    "\"maxProfileRecords\":100",
                    "\"maxProfileRecords\":1");
                break;

            case 4:
                json = ReplaceOnce(
                    json,
                    "\"tableName\":\"json_data\"",
                    "\"tableName\":\"tampered_table\"");
                break;

            case 5:
                json = ReplaceOnce(
                    json,
                    "\"expectedPropertyName\":\"id\"",
                    "\"expectedPropertyName\":\"identifier\"");
                break;

            case 6:
                {
                    string optionsDigest = payload
                        .GetProperty("source")
                        .GetProperty("optionsDigest")
                        .GetString()!;
                    json = ReplaceOnce(
                        json,
                        $"\"optionsDigest\":\"{optionsDigest}\"",
                        $"\"optionsDigest\":\"{ZeroPrefixedDigest()}\"");
                    break;
                }

            case 7:
                {
                    string fingerprint = payload
                        .GetProperty("source")
                        .GetProperty("fingerprint")
                        .GetString()!;
                    json = ReplaceOnce(
                        json,
                        $"\"fingerprint\":\"{fingerprint}\"",
                        $"\"fingerprint\":\"{ZeroPrefixedDigest()}\"");
                    break;
                }

            case 8:
                {
                    string catalogDigest = payload
                        .GetProperty("catalog")
                        .GetProperty("digest")
                        .GetString()!;
                    json = ReplaceOnce(
                        json,
                        $"\"digest\":\"{catalogDigest}\"",
                        $"\"digest\":\"{new string('0', 64)}\"");
                    break;
                }

            case 9:
                {
                    JsonElement snapshot =
                        payload.GetProperty("snapshot");
                    string oldDigest = snapshot
                        .GetProperty("contentDigest")
                        .GetString()!;
                    string oldIdentity = snapshot
                        .GetProperty("snapshotIdentity")
                        .GetString()!;
                    long contentLength = snapshot
                        .GetProperty("contentLength")
                        .GetInt64();
                    string newDigest = ZeroPrefixedDigest();
                    string newIdentity =
                        $"{JsonSourceSnapshot.IdentityAlgorithm}:{newDigest}:bytes:{contentLength}";
                    json = ReplaceOnce(
                        json,
                        $"\"contentDigest\":\"{oldDigest}\"",
                        $"\"contentDigest\":\"{newDigest}\"");
                    json = ReplaceOnce(
                        json,
                        $"\"snapshotIdentity\":\"{oldIdentity}\"",
                        $"\"snapshotIdentity\":\"{newIdentity}\"");
                    break;
                }

            default:
                throw new ArgumentOutOfRangeException(
                    nameof(mutation));
        }

        json = RecomputeInnerDigest(json);
        await ReplaceManifestAsync(
            packagePath,
            Encoding.UTF8.GetBytes(json));
        await AssertOpenFailsAndPreservesAsync(
            workspace,
            packagePath,
            expectedRule);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task UnsupportedMagicVersionHeaderSizeOrFlagsAreRejected(
        int mutation)
    {
        using var workspace = new TemporaryWorkspace();
        string packagePath =
            await CreatePackageAsync(workspace);
        byte[] bytes = await File.ReadAllBytesAsync(
            packagePath,
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
                    2);
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
            default:
                throw new ArgumentOutOfRangeException(
                    nameof(mutation));
        }

        await File.WriteAllBytesAsync(
            packagePath,
            bytes,
            Cancellation);
        await AssertOpenFailsAndPreservesAsync(
            workspace,
            packagePath,
            JsonSnapshotPackageRules.InvalidFormat);
    }

    [Theory]
    [InlineData(
        0,
        JsonSnapshotPackageRules.SizeLimitExceeded)]
    [InlineData(
        1,
        JsonSnapshotPackageRules.SizeLimitExceeded)]
    [InlineData(
        2,
        JsonSnapshotPackageRules.InvalidFormat)]
    [InlineData(
        3,
        JsonSnapshotPackageRules.SizeLimitExceeded)]
    [InlineData(
        4,
        JsonSnapshotPackageRules.InvalidFormat)]
    public async Task ExtremeOrInconsistentSectionGeometryIsRejected(
        int mutation,
        string expectedRule)
    {
        using var workspace = new TemporaryWorkspace();
        string packagePath =
            await CreatePackageAsync(workspace);
        byte[] bytes = await File.ReadAllBytesAsync(
            packagePath,
            Cancellation);
        ulong snapshotLength =
            BinaryPrimitives.ReadUInt64BigEndian(
                bytes.AsSpan(
                    SnapshotLengthOffset,
                    sizeof(ulong)));
        switch (mutation)
        {
            case 0:
                BinaryPrimitives.WriteUInt32BigEndian(
                    bytes.AsSpan(
                        ManifestLengthOffset,
                        sizeof(uint)),
                    0);
                break;
            case 1:
                BinaryPrimitives.WriteUInt32BigEndian(
                    bytes.AsSpan(
                        ManifestLengthOffset,
                        sizeof(uint)),
                    uint.MaxValue);
                break;
            case 2:
                BinaryPrimitives.WriteUInt64BigEndian(
                    bytes.AsSpan(
                        SnapshotLengthOffset,
                        sizeof(ulong)),
                    0);
                break;
            case 3:
                BinaryPrimitives.WriteUInt64BigEndian(
                    bytes.AsSpan(
                        SnapshotLengthOffset,
                        sizeof(ulong)),
                    ulong.MaxValue);
                break;
            case 4:
                BinaryPrimitives.WriteUInt64BigEndian(
                    bytes.AsSpan(
                        SnapshotLengthOffset,
                        sizeof(ulong)),
                    checked(snapshotLength + 1));
                break;
            default:
                throw new ArgumentOutOfRangeException(
                    nameof(mutation));
        }

        await File.WriteAllBytesAsync(
            packagePath,
            bytes,
            Cancellation);
        await AssertOpenFailsAndPreservesAsync(
            workspace,
            packagePath,
            expectedRule);
    }

    private static async ValueTask<string>
        CreatePackageAsync(TemporaryWorkspace workspace)
    {
        string packagePath = workspace.PathFor(
            "semantic" +
            JsonSnapshotPackage.FileExtension);
        JsonSourceSnapshot snapshot =
            await JsonSourceSnapshot.CreateAsync(
                new MemoryStream(
                    Encoding.UTF8.GetBytes(
                        """
                        [
                          {"id":1,"name":"alpha"},
                          {"id":2,"name":"beta"}
                        ]
                        """)),
                new JsonSourceSnapshotOptions
                {
                    WorkspacePath = workspace.Root,
                    MaxSourceBytes = 1024 * 1024,
                },
                Cancellation);
        await using (snapshot)
        {
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
                        MaxArrayElements = 1_024,
                        MaxTotalNodes = 2_048,
                        MaxPropertyNameBytes =
                            8 * 1_024,
                        MaxStringBytes = 128 * 1_024,
                        MaxNumberBytes = 8 * 1_024,
                    },
                    logicalSourceIdentity:
                        "semantic/package-source",
                    cancellationToken: Cancellation);
            JsonTableSchemaInferenceResult schema =
                await JsonTableSchemaInferer.InferAsync(
                    binding,
                    snapshot,
                    maxProfileRecords: 100,
                    new JsonTableSchemaInferenceOptions
                    {
                        ColumnOverrides =
                        [
                            new JsonTableColumnSchemaOverride
                            {
                                ColumnIndex = 0,
                                ExpectedPropertyName = "id",
                                LogicalType =
                                    JsonTableColumnLogicalType
                                        .SignedInteger,
                                Nullable = false,
                            },
                        ],
                    },
                    Cancellation);
            await JsonSnapshotPackage.WriteAsync(
                packagePath,
                snapshot,
                schema,
                TargetVersion,
                Cancellation);
        }

        return packagePath;
    }

    private static async ValueTask<byte[]>
        ReadManifestAsync(string packagePath)
    {
        byte[] package =
            await File.ReadAllBytesAsync(
                packagePath,
                Cancellation);
        int manifestLength =
            ManifestLength(package);
        return package
            .AsSpan(
                HeaderSize,
                manifestLength)
            .ToArray();
    }

    private static async ValueTask ReplaceManifestAsync(
        string packagePath,
        byte[] replacementManifest)
    {
        byte[] original =
            await File.ReadAllBytesAsync(
                packagePath,
                Cancellation);
        int rawOffset =
            RawSnapshotOffset(original);
        ReadOnlySpan<byte> rawSnapshot =
            original.AsSpan(rawOffset);
        byte[] header =
            original.AsSpan(0, HeaderSize)
                .ToArray();
        BinaryPrimitives.WriteUInt32BigEndian(
            header.AsSpan(
                ManifestLengthOffset,
                sizeof(uint)),
            checked((uint)
                replacementManifest.Length));
        SHA256.HashData(replacementManifest)
            .CopyTo(
                header.AsSpan(
                    ManifestHashOffset,
                    SHA256.HashSizeInBytes));

        byte[] replacement =
            new byte[checked(
                HeaderSize +
                replacementManifest.Length +
                rawSnapshot.Length)];
        header.CopyTo(replacement, 0);
        replacementManifest.CopyTo(
            replacement,
            HeaderSize);
        rawSnapshot.CopyTo(
            replacement.AsSpan(
                HeaderSize +
                replacementManifest.Length));
        await File.WriteAllBytesAsync(
            packagePath,
            replacement,
            Cancellation);
    }

    private static string RecomputeInnerDigest(
        string envelope)
    {
        string currentDigest =
            EnvelopeDigest(envelope);
        string digestProperty =
            $"\"digest\":\"{currentDigest}\",";
        int digestPropertyOffset =
            envelope.IndexOf(
                digestProperty,
                StringComparison.Ordinal);
        Assert.True(digestPropertyOffset > 0);
        string digestInput = envelope.Remove(
            digestPropertyOffset,
            digestProperty.Length);
        using JsonDocument document =
            JsonDocument.Parse(digestInput);
        JsonElement root = document.RootElement;
        Assert.Equal(
            JsonSnapshotPackage.Format,
            root.GetProperty("format").GetString());
        Assert.Equal(
            "sha256",
            root.GetProperty("digestAlgorithm")
                .GetString());
        Assert.Equal(
            JsonValueKind.Object,
            root.GetProperty("payload")
                .ValueKind);

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
            .Remove(
                valueOffset,
                currentDigest.Length)
            .Insert(
                valueOffset,
                replacementDigest);
    }

    private static string EnvelopeDigest(
        string envelope)
    {
        const string marker = "\"digest\":\"";
        int offset = envelope.IndexOf(
            marker,
            StringComparison.Ordinal);
        Assert.True(offset > 0);
        int valueOffset = offset + marker.Length;
        Assert.True(
            envelope.Length >= valueOffset + 64);
        return envelope.Substring(
            valueOffset,
            64);
    }

    private static string ReplaceOnce(
        string value,
        string oldValue,
        string newValue)
    {
        int offset = value.IndexOf(
            oldValue,
            StringComparison.Ordinal);
        Assert.True(
            offset >= 0,
            $"Expected manifest token was not found: {oldValue}");
        Assert.Equal(
            -1,
            value.IndexOf(
                oldValue,
                offset + oldValue.Length,
                StringComparison.Ordinal));
        return value
            .Remove(offset, oldValue.Length)
            .Insert(offset, newValue);
    }

    private static string ZeroPrefixedDigest() =>
        "sha256:" + new string('0', 64);

    private static async Task
        AssertOpenFailsAndPreservesAsync(
            TemporaryWorkspace workspace,
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
                    await using JsonSnapshotPackageSession
                        session =
                            await JsonSnapshotPackage
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

    private static int ManifestLength(
        ReadOnlySpan<byte> packageBytes) =>
        checked((int)
            BinaryPrimitives.ReadUInt32BigEndian(
                packageBytes.Slice(
                    ManifestLengthOffset,
                    sizeof(uint))));

    private static int RawSnapshotOffset(
        ReadOnlySpan<byte> packageBytes) =>
        checked(
            HeaderSize +
            ManifestLength(packageBytes));

    private sealed class TemporaryWorkspace :
        IDisposable
    {
        internal TemporaryWorkspace()
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                "csharpdb-json-package-semantic-tests-" +
                Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(Root);
        }

        internal string Root { get; }

        internal string PathFor(string fileName) =>
            Path.Combine(Root, fileName);

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
