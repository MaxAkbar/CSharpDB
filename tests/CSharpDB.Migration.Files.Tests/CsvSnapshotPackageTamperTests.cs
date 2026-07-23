using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Nodes;
using CSharpDB.Migration;
using CSharpDB.Migration.Files.Csv;

namespace CSharpDB.Migration.Files.Tests;

public sealed class CsvSnapshotPackageTamperTests
{
    private const int HeaderSize = 64;
    private const int VersionOffset = 8;
    private const int ManifestLengthOffset = 16;
    private const int FlagsOffset = 20;
    private const int SnapshotLengthOffset = 24;
    private const int ManifestHashOffset = 32;

    [Fact]
    public async Task ExistingDestinationIsNeverOverwritten()
    {
        using var workspace = new TemporaryWorkspace();
        string packagePath = workspace.PathFor("retained.csdbcsv");
        (CsvSourceSnapshot snapshot, CsvSchemaInferenceResult schema) =
            await CreateSourceAsync(workspace, "id,name\n1,alpha\n2,beta\n");
        await using (snapshot)
        {
            await CsvSnapshotPackage.WriteAsync(
                packagePath,
                snapshot,
                schema,
                TargetVersion,
                Cancellation);
            byte[] original = await File.ReadAllBytesAsync(packagePath, Cancellation);

            await Assert.ThrowsAsync<IOException>(async () =>
                await CsvSnapshotPackage.WriteAsync(
                    packagePath,
                    snapshot,
                    schema,
                    TargetVersion,
                    Cancellation));

            Assert.Equal(original, await File.ReadAllBytesAsync(packagePath, Cancellation));
            Assert.Empty(workspace.PackageTempFiles());
        }
    }

    [Fact]
    public async Task PreCanceledWriteLeavesNoDestinationOrTemporaryFile()
    {
        using var workspace = new TemporaryWorkspace();
        string packagePath = workspace.PathFor("canceled.csdbcsv");
        (CsvSourceSnapshot snapshot, CsvSchemaInferenceResult schema) =
            await CreateSourceAsync(workspace, "id\n1\n");
        await using (snapshot)
        using (var canceled = new CancellationTokenSource())
        {
            await canceled.CancelAsync();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await CsvSnapshotPackage.WriteAsync(
                    packagePath,
                    snapshot,
                    schema,
                    TargetVersion,
                    canceled.Token));

            Assert.False(File.Exists(packagePath));
            Assert.Empty(workspace.PackageTempFiles());
        }
    }

    [Fact]
    public async Task ManifestBitFlipIsRejectedWithoutRemovingPackage()
    {
        using var workspace = new TemporaryWorkspace();
        string packagePath = await CreatePackageAsync(workspace);
        byte[] bytes = await File.ReadAllBytesAsync(packagePath, Cancellation);
        int manifestLength = ManifestLength(bytes);
        Assert.True(manifestLength > 0);
        bytes[HeaderSize + (manifestLength / 2)] ^= 0x01;
        await File.WriteAllBytesAsync(packagePath, bytes, Cancellation);

        await AssertOpenFailsAndPreservesAsync(
            workspace,
            packagePath,
            CsvSnapshotPackageRules.IntegrityMismatch);
        Assert.Equal(bytes, await File.ReadAllBytesAsync(packagePath, Cancellation));
    }

    [Fact]
    public async Task RawSnapshotBitFlipIsRejected()
    {
        using var workspace = new TemporaryWorkspace();
        string packagePath = await CreatePackageAsync(workspace);
        byte[] bytes = await File.ReadAllBytesAsync(packagePath, Cancellation);
        int rawOffset = RawSnapshotOffset(bytes);
        Assert.True(rawOffset < bytes.Length);
        bytes[rawOffset] ^= 0x01;
        await File.WriteAllBytesAsync(packagePath, bytes, Cancellation);

        await AssertOpenFailsAndPreservesAsync(
            workspace,
            packagePath,
            CsvSnapshotPackageRules.IntegrityMismatch);
    }

    [Fact]
    public async Task TruncatedPackageIsRejected()
    {
        using var workspace = new TemporaryWorkspace();
        string packagePath = await CreatePackageAsync(workspace);
        using (var package = new FileStream(packagePath, FileMode.Open, FileAccess.Write, FileShare.None))
            package.SetLength(package.Length - 1);

        await AssertOpenFailsAndPreservesAsync(
            workspace,
            packagePath,
            CsvSnapshotPackageRules.InvalidFormat);
    }

    [Fact]
    public async Task AppendedPackageByteIsRejected()
    {
        using var workspace = new TemporaryWorkspace();
        string packagePath = await CreatePackageAsync(workspace);
        await using (var package = new FileStream(
            packagePath,
            FileMode.Append,
            FileAccess.Write,
            FileShare.None,
            bufferSize: 4096,
            useAsync: true))
        {
            await package.WriteAsync(new byte[] { 0xA5 }, Cancellation);
        }

        await AssertOpenFailsAndPreservesAsync(
            workspace,
            packagePath,
            CsvSnapshotPackageRules.InvalidFormat);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task UnsupportedMagicVersionOrFlagsAreRejected(int mutation)
    {
        using var workspace = new TemporaryWorkspace();
        string packagePath = await CreatePackageAsync(workspace);
        byte[] bytes = await File.ReadAllBytesAsync(packagePath, Cancellation);
        switch (mutation)
        {
            case 0:
                bytes[0] ^= 0x80;
                break;
            case 1:
                BinaryPrimitives.WriteUInt32BigEndian(bytes.AsSpan(VersionOffset, 4), 2);
                break;
            case 2:
                BinaryPrimitives.WriteUInt32BigEndian(bytes.AsSpan(FlagsOffset, 4), 1);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(mutation));
        }

        await File.WriteAllBytesAsync(packagePath, bytes, Cancellation);
        await AssertOpenFailsAndPreservesAsync(
            workspace,
            packagePath,
            CsvSnapshotPackageRules.InvalidFormat);
    }

    [Theory]
    [InlineData(0, CsvSnapshotPackageRules.SizeLimitExceeded)]
    [InlineData(1, CsvSnapshotPackageRules.SizeLimitExceeded)]
    [InlineData(2, CsvSnapshotPackageRules.InvalidFormat)]
    [InlineData(3, CsvSnapshotPackageRules.SizeLimitExceeded)]
    public async Task ExtremeSectionLengthsAreRejected(int mutation, string expectedRule)
    {
        using var workspace = new TemporaryWorkspace();
        string packagePath = await CreatePackageAsync(workspace);
        byte[] bytes = await File.ReadAllBytesAsync(packagePath, Cancellation);
        switch (mutation)
        {
            case 0:
                BinaryPrimitives.WriteUInt32BigEndian(
                    bytes.AsSpan(ManifestLengthOffset, 4),
                    0);
                break;
            case 1:
                BinaryPrimitives.WriteUInt32BigEndian(
                    bytes.AsSpan(ManifestLengthOffset, 4),
                    uint.MaxValue);
                break;
            case 2:
                BinaryPrimitives.WriteUInt64BigEndian(
                    bytes.AsSpan(SnapshotLengthOffset, 8),
                    0);
                break;
            case 3:
                BinaryPrimitives.WriteUInt64BigEndian(
                    bytes.AsSpan(SnapshotLengthOffset, 8),
                    ulong.MaxValue);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(mutation));
        }

        await File.WriteAllBytesAsync(packagePath, bytes, Cancellation);
        await AssertOpenFailsAndPreservesAsync(workspace, packagePath, expectedRule);
    }

    [Fact]
    public async Task OpenHonorsConfiguredMaximumSourceBytes()
    {
        using var workspace = new TemporaryWorkspace();
        string packagePath = await CreatePackageAsync(workspace);
        byte[] bytes = await File.ReadAllBytesAsync(packagePath, Cancellation);
        long snapshotLength = checked((long)BinaryPrimitives.ReadUInt64BigEndian(
            bytes.AsSpan(SnapshotLengthOffset, 8)));
        Assert.True(snapshotLength > 0);

        await AssertOpenFailsAndPreservesAsync(
            workspace,
            packagePath,
            CsvSnapshotPackageRules.SizeLimitExceeded,
            maxSourceBytes: snapshotLength - 1);
    }

    [Fact]
    public async Task SymbolicLinkPackagePathIsRejectedWhereSupported()
    {
        using var workspace = new TemporaryWorkspace();
        string packagePath = await CreatePackageAsync(workspace);
        string linkPath = workspace.PathFor("retained-link.csdbcsv");
        try
        {
            File.CreateSymbolicLink(linkPath, packagePath);
        }
        catch (Exception exception) when (
            exception is PlatformNotSupportedException or
                UnauthorizedAccessException or
                IOException)
        {
            return;
        }

        if ((File.GetAttributes(linkPath) & FileAttributes.ReparsePoint) == 0)
            return;

        CsvSnapshotPackageException error = await Assert.ThrowsAsync<CsvSnapshotPackageException>(
            async () =>
            {
                await using CsvSnapshotPackageSession session = await CsvSnapshotPackage.OpenAsync(
                    linkPath,
                    OpenOptions(workspace),
                    Cancellation);
            });
        Assert.Equal(CsvSnapshotPackageRules.UnsafePath, error.RuleId);
        Assert.True(File.Exists(packagePath));
    }

    [Fact]
    public async Task ManifestDoesNotExposeSourcePathOrRawLogicalIdentity()
    {
        using var workspace = new TemporaryWorkspace();
        const string sourceFileName = "sensitive-original-source-name.csv";
        const string logicalIdentity = "private-logical-source-customer-42";
        string sourcePath = workspace.PathFor(sourceFileName);
        await File.WriteAllTextAsync(sourcePath, "id,name\n1,alpha\n", Encoding.UTF8, Cancellation);
        string packagePath = workspace.PathFor("private.csdbcsv");

        CsvSourceSnapshot snapshot = await CsvSourceSnapshot.CreateFromFileAsync(
            sourcePath,
            new CsvSourceSnapshotOptions { WorkspacePath = workspace.Root },
            Cancellation);
        await using (snapshot)
        {
            CsvSchemaInferenceResult schema = await InferAsync(snapshot, logicalIdentity);
            await CsvSnapshotPackage.WriteAsync(
                packagePath,
                snapshot,
                schema,
                TargetVersion,
                Cancellation);
        }

        byte[] packageBytes = await File.ReadAllBytesAsync(packagePath, Cancellation);
        int manifestLength = ManifestLength(packageBytes);
        string manifest = Encoding.UTF8.GetString(
            packageBytes,
            HeaderSize,
            manifestLength);
        Assert.DoesNotContain(sourcePath, manifest);
        Assert.DoesNotContain(sourceFileName, manifest);
        Assert.DoesNotContain(logicalIdentity, manifest);
        Assert.Contains("csv-logical:sha256:", manifest);
    }

    [Fact]
    public async Task CanonicalManifestUsesLiteralUtf8AndStableEscapesAndReopens()
    {
        using var workspace = new TemporaryWorkspace();
        const string header = "naïve \"名\"\tfield";
        const string tableName = "café_日本";
        const string csv = "\"naïve \"\"名\"\"\tfield\",value\n1,alpha\n2,beta\n";
        var schemaOptions = new CsvSchemaInferenceOptions
        {
            TableName = tableName,
            ColumnOverrides =
            [
                new CsvColumnSchemaOverride
                {
                    ColumnIndex = 0,
                    ExpectedHeader = header,
                    LogicalType = CsvColumnLogicalType.SignedInteger,
                    Nullable = false,
                },
            ],
        };
        string packagePath = workspace.PathFor("canonical-unicode.csdbcsv");
        (CsvSourceSnapshot snapshot, CsvSchemaInferenceResult schema) =
            await CreateSourceAsync(workspace, csv, schemaOptions);
        await using (snapshot)
        {
            await CsvSnapshotPackage.WriteAsync(
                packagePath,
                snapshot,
                schema,
                TargetVersion,
                Cancellation);
        }

        byte[] manifest = await ReadManifestAsync(packagePath);
        AssertContainsBytes(manifest, Encoding.UTF8.GetBytes("\"tableName\":\"café_日本\""));
        AssertContainsBytes(
            manifest,
            Encoding.UTF8.GetBytes("\"expectedHeader\":\"naïve \\\"名\\\"\\tfield\""));
        AssertContainsBytes(manifest, Encoding.UTF8.GetBytes("\"quote\":\"\\\"\""));

        await using CsvSnapshotPackageSession session = await CsvSnapshotPackage.OpenAsync(
            packagePath,
            OpenOptions(workspace),
            Cancellation);
        Assert.Equal(tableName, session.Schema.TableName);
        Assert.Equal(header, session.Schema.Columns[0].OriginalHeader);
    }

    [Fact]
    public async Task FailedOpenNeverDeletesOrRewritesRetainedPackage()
    {
        using var workspace = new TemporaryWorkspace();
        string packagePath = await CreatePackageAsync(workspace);
        byte[] corrupt = await File.ReadAllBytesAsync(packagePath, Cancellation);
        corrupt[ManifestHashOffset] ^= 0x01;
        await File.WriteAllBytesAsync(packagePath, corrupt, Cancellation);

        await AssertOpenFailsAndPreservesAsync(
            workspace,
            packagePath,
            CsvSnapshotPackageRules.IntegrityMismatch);

        Assert.Equal(corrupt, await File.ReadAllBytesAsync(packagePath, Cancellation));
    }

    [Fact]
    public async Task DuplicateManifestPropertyPassesOuterHashButIsRejected()
    {
        using var workspace = new TemporaryWorkspace();
        string packagePath = await CreatePackageAsync(workspace);
        string json = Encoding.UTF8.GetString(await ReadManifestAsync(packagePath));
        int insertion = json.IndexOf(",\"digestAlgorithm\"", StringComparison.Ordinal);
        Assert.True(insertion > 0);
        json = json.Insert(
            insertion,
            $",\"format\":\"{CsvSnapshotPackage.Format}\"");

        await ReplaceManifestAsync(packagePath, Encoding.UTF8.GetBytes(json));
        await AssertOpenFailsAndPreservesAsync(
            workspace,
            packagePath,
            CsvSnapshotPackageRules.InvalidFormat);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task UnknownOrMisCasedManifestPropertyIsRejected(bool misCaseKnownProperty)
    {
        using var workspace = new TemporaryWorkspace();
        string packagePath = await CreatePackageAsync(workspace);
        string json = Encoding.UTF8.GetString(await ReadManifestAsync(packagePath));
        json = misCaseKnownProperty
            ? ReplaceOnce(json, "\"contracts\":", "\"Contracts\":")
            : ReplaceOnce(json, "\"payload\":{", "\"payload\":{\"unexpectedMember\":true,");

        await ReplaceManifestAsync(packagePath, Encoding.UTF8.GetBytes(json));
        await AssertOpenFailsAndPreservesAsync(
            workspace,
            packagePath,
            CsvSnapshotPackageRules.InvalidFormat);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ValidButNoncanonicalManifestJsonIsRejected(bool reorderEnvelope)
    {
        using var workspace = new TemporaryWorkspace();
        string packagePath = await CreatePackageAsync(workspace);
        byte[] manifest;
        if (reorderEnvelope)
        {
            JsonObject source = await ReadEnvelopeAsync(packagePath);
            var reordered = new JsonObject
            {
                ["digestAlgorithm"] = source["digestAlgorithm"]!.DeepClone(),
                ["format"] = source["format"]!.DeepClone(),
                ["digest"] = source["digest"]!.DeepClone(),
                ["payload"] = source["payload"]!.DeepClone(),
            };
            manifest = SerializeCompact(reordered);
        }
        else
        {
            byte[] canonical = await ReadManifestAsync(packagePath);
            manifest = new byte[canonical.Length + 1];
            manifest[0] = canonical[0];
            manifest[1] = (byte)' ';
            canonical.AsSpan(1).CopyTo(manifest.AsSpan(2));
        }

        await ReplaceManifestAsync(packagePath, manifest);
        await AssertOpenFailsAndPreservesAsync(
            workspace,
            packagePath,
            CsvSnapshotPackageRules.InvalidFormat);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task InvalidManifestEncodingIsRejectedWithValidOuterGeometry(int mutation)
    {
        using var workspace = new TemporaryWorkspace();
        string packagePath = await CreatePackageAsync(workspace);
        byte[] canonical = await ReadManifestAsync(packagePath);
        byte[] prefix = mutation switch
        {
            0 => Encoding.UTF8.Preamble.ToArray(),
            1 => [0x00],
            2 => [0xFF],
            _ => throw new ArgumentOutOfRangeException(nameof(mutation)),
        };
        byte[] manifest = new byte[prefix.Length + canonical.Length];
        prefix.CopyTo(manifest, 0);
        canonical.CopyTo(manifest, prefix.Length);

        await ReplaceManifestAsync(packagePath, manifest);
        await AssertOpenFailsAndPreservesAsync(
            workspace,
            packagePath,
            CsvSnapshotPackageRules.InvalidFormat);
    }

    [Fact]
    public async Task IntegerEncodedEnumIsRejectedAfterOuterHashVerification()
    {
        using var workspace = new TemporaryWorkspace();
        string packagePath = await CreatePackageWithOverridesAsync(workspace);
        string json = Encoding.UTF8.GetString(await ReadManifestAsync(packagePath));
        json = ReplaceOnce(
            json,
            "\"logicalType\":\"signedInteger\"",
            "\"logicalType\":1");

        await ReplaceManifestAsync(packagePath, Encoding.UTF8.GetBytes(json));
        await AssertOpenFailsAndPreservesAsync(
            workspace,
            packagePath,
            CsvSnapshotPackageRules.InvalidFormat);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task UppercaseOrIncorrectInnerDigestIsRejected(bool uppercase)
    {
        using var workspace = new TemporaryWorkspace();
        string packagePath = await CreatePackageAsync(workspace);
        string json = Encoding.UTF8.GetString(await ReadManifestAsync(packagePath));
        string digest = EnvelopeDigest(json);
        string replacement = uppercase
            ? digest.ToUpperInvariant()
            : (digest[0] == '0' ? "1" : "0") + digest[1..];
        json = ReplaceOnce(json, digest, replacement);

        await ReplaceManifestAsync(packagePath, Encoding.UTF8.GetBytes(json));
        await AssertOpenFailsAndPreservesAsync(
            workspace,
            packagePath,
            CsvSnapshotPackageRules.InvalidFormat);
    }

    [Fact]
    public async Task ExplicitNullWithValidInnerAndOuterDigestsIsNoncanonical()
    {
        using var workspace = new TemporaryWorkspace();
        string packagePath = await CreatePackageAsync(workspace);
        string json = Encoding.UTF8.GetString(await ReadManifestAsync(packagePath));
        Assert.DoesNotContain("\"nullToken\":", json);
        json = ReplaceOnce(
            json,
            "\"nullTokenMatchesQuotedFields\":",
            "\"nullToken\":null,\"nullTokenMatchesQuotedFields\":");

        await ReplaceManifestAsync(packagePath, Encoding.UTF8.GetBytes(json));
        await AssertOpenFailsAndPreservesAsync(
            workspace,
            packagePath,
            CsvSnapshotPackageRules.InvalidFormat);
    }

    [Fact]
    public async Task OversizedRetainedTextWithValidDigestsIsRejectedBeforeWorkspaceCreation()
    {
        using var workspace = new TemporaryWorkspace();
        string packagePath = await CreatePackageAsync(workspace);
        string json = Encoding.UTF8.GetString(await ReadManifestAsync(packagePath));
        string oversizedNullToken = new('n', (1024 * 1024) + 1);
        json = ReplaceOnce(
            json,
            "\"nullTokenMatchesQuotedFields\":",
            $"\"nullToken\":\"{oversizedNullToken}\",\"nullTokenMatchesQuotedFields\":");
        json = RecomputeInnerDigest(json);
        await ReplaceManifestAsync(packagePath, Encoding.UTF8.GetBytes(json));
        Assert.Empty(Directory.EnumerateDirectories(workspace.Root));

        await AssertOpenFailsAndPreservesAsync(
            workspace,
            packagePath,
            CsvSnapshotPackageRules.InvalidFormat);

        Assert.Empty(Directory.EnumerateDirectories(workspace.Root));
    }

    [Fact]
    public async Task InnerDigestHelperMatchesCanonicalPackageDigest()
    {
        using var workspace = new TemporaryWorkspace();
        string packagePath = await CreatePackageAsync(workspace);
        string json = Encoding.UTF8.GetString(await ReadManifestAsync(packagePath));

        string resigned = RecomputeInnerDigest(json);

        Assert.Equal(json, resigned);
    }

    [Fact]
    public async Task UnorderedOverridesWithValidInnerAndOuterDigestsAreRejected()
    {
        using var workspace = new TemporaryWorkspace();
        string packagePath = await CreatePackageWithOverridesAsync(workspace);
        string json = Encoding.UTF8.GetString(await ReadManifestAsync(packagePath));
        const string ordered =
            "{\"index\":0,\"logicalType\":\"signedInteger\",\"nullable\":false}," +
            "{\"index\":1,\"logicalType\":\"text\"}";
        const string unordered =
            "{\"index\":1,\"logicalType\":\"text\"}," +
            "{\"index\":0,\"logicalType\":\"signedInteger\",\"nullable\":false}";
        json = ReplaceOnce(json, ordered, unordered);

        json = RecomputeInnerDigest(json);
        await ReplaceManifestAsync(packagePath, Encoding.UTF8.GetBytes(json));
        await AssertOpenFailsAndPreservesAsync(
            workspace,
            packagePath,
            CsvSnapshotPackageRules.InvalidFormat);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    public async Task SemanticTamperWithValidInnerAndOuterDigestsIsRejected(int mutation)
    {
        using var workspace = new TemporaryWorkspace();
        string packagePath = await CreatePackageAsync(workspace);
        string json = Encoding.UTF8.GetString(await ReadManifestAsync(packagePath));
        switch (mutation)
        {
            case 0:
                json = ReplaceOnce(
                    json,
                    "\"schema\":\"csharpdb-csv-schema-v1\"",
                    "\"schema\":\"csharpdb-csv-schema-v999\"");
                break;
            case 1:
                using (JsonDocument document = JsonDocument.Parse(json))
                {
                    string catalogDigest = document.RootElement
                        .GetProperty("payload")
                        .GetProperty("catalog")
                        .GetProperty("digest")
                        .GetString()!;
                    json = ReplaceOnce(json, catalogDigest, new string('0', 64));
                }
                break;
            case 2:
                json = ReplaceOnce(
                    json,
                    "\"newlinePolicy\":\"common-auto\"",
                    "\"newlinePolicy\":\"lf-only\"");
                break;
            case 3:
                json = ReplaceOnce(
                    json,
                    "\"maxFieldCharacters\":16777216",
                    "\"maxFieldCharacters\":2147483647");
                break;
            case 4:
                json = ReplaceOnce(
                    json,
                    "\"maxDataRecords\":100",
                    "\"maxDataRecords\":2147483647");
                break;
            case 5:
                json = ReplaceOnce(
                    json,
                    "\"maxProfileCharacters\":67108864",
                    "\"maxProfileCharacters\":9223372036854775807");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(mutation));
        }

        json = RecomputeInnerDigest(json);
        await ReplaceManifestAsync(packagePath, Encoding.UTF8.GetBytes(json));
        await AssertOpenFailsAndPreservesAsync(
            workspace,
            packagePath,
            CsvSnapshotPackageRules.PolicyMismatch);
    }

    private static CancellationToken Cancellation => TestContext.Current.CancellationToken;

    private static string TargetVersion =>
        CSharpDbCapabilityCatalogLoader.CurrentTargetVersion;

    private static async ValueTask<string> CreatePackageAsync(TemporaryWorkspace workspace)
    {
        string packagePath = workspace.PathFor($"retained-{Guid.NewGuid():N}.csdbcsv");
        (CsvSourceSnapshot snapshot, CsvSchemaInferenceResult schema) =
            await CreateSourceAsync(workspace, "id,name\n1,alpha\n2,beta\n");
        await using (snapshot)
        {
            await CsvSnapshotPackage.WriteAsync(
                packagePath,
                snapshot,
                schema,
                TargetVersion,
                Cancellation);
        }

        return packagePath;
    }

    private static async ValueTask<string> CreatePackageWithOverridesAsync(
        TemporaryWorkspace workspace)
    {
        string packagePath = workspace.PathFor($"overrides-{Guid.NewGuid():N}.csdbcsv");
        var schemaOptions = new CsvSchemaInferenceOptions
        {
            ColumnOverrides =
            [
                new CsvColumnSchemaOverride
                {
                    ColumnIndex = 1,
                    LogicalType = CsvColumnLogicalType.Text,
                },
                new CsvColumnSchemaOverride
                {
                    ColumnIndex = 0,
                    LogicalType = CsvColumnLogicalType.SignedInteger,
                    Nullable = false,
                },
            ],
        };
        (CsvSourceSnapshot snapshot, CsvSchemaInferenceResult schema) =
            await CreateSourceAsync(
                workspace,
                "id,name\n1,alpha\n2,beta\n",
                schemaOptions);
        await using (snapshot)
        {
            await CsvSnapshotPackage.WriteAsync(
                packagePath,
                snapshot,
                schema,
                TargetVersion,
                Cancellation);
        }

        return packagePath;
    }

    private static async ValueTask<(CsvSourceSnapshot Snapshot, CsvSchemaInferenceResult Schema)>
        CreateSourceAsync(
            TemporaryWorkspace workspace,
            string csv,
            CsvSchemaInferenceOptions? schemaOptions = null)
    {
        CsvSourceSnapshot snapshot = await CsvSourceSnapshot.CreateAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(csv)),
            new CsvSourceSnapshotOptions { WorkspacePath = workspace.Root },
            Cancellation);
        try
        {
            CsvSchemaInferenceResult schema = await InferAsync(
                snapshot,
                schemaOptions: schemaOptions);
            return (snapshot, schema);
        }
        catch
        {
            await snapshot.DisposeAsync();
            throw;
        }
    }

    private static async ValueTask<CsvSchemaInferenceResult> InferAsync(
        CsvSourceSnapshot snapshot,
        string? logicalIdentity = null,
        CsvSchemaInferenceOptions? schemaOptions = null)
    {
        var readerOptions = new CsvReaderOptions();
        CsvFormatInspection inspection = await CsvFormatInspector.InspectAsync(
            snapshot,
            readerOptions,
            new CsvInspectionOptions { DelimiterCandidates = [readerOptions.Delimiter] },
            Cancellation);
        CsvSourceBinding binding = await CsvSourceBinding.CreateAsync(
            snapshot,
            inspection,
            logicalIdentity,
            Cancellation);
        return await CsvSchemaInferer.InferAsync(
            binding,
            snapshot,
            maxDataRecords: 100,
            options: schemaOptions,
            cancellationToken: Cancellation);
    }

    private static async ValueTask<byte[]> ReadManifestAsync(string packagePath)
    {
        byte[] package = await File.ReadAllBytesAsync(packagePath, Cancellation);
        int manifestLength = ManifestLength(package);
        return package.AsSpan(HeaderSize, manifestLength).ToArray();
    }

    private static async ValueTask<JsonObject> ReadEnvelopeAsync(string packagePath)
    {
        byte[] manifest = await ReadManifestAsync(packagePath);
        JsonNode? node = JsonNode.Parse(Encoding.UTF8.GetString(manifest));
        Assert.NotNull(node);
        return Assert.IsType<JsonObject>(node);
    }

    private static async ValueTask ReplaceManifestAsync(
        string packagePath,
        byte[] replacementManifest)
    {
        byte[] original = await File.ReadAllBytesAsync(packagePath, Cancellation);
        int rawOffset = RawSnapshotOffset(original);
        ReadOnlySpan<byte> rawSnapshot = original.AsSpan(rawOffset);
        byte[] header = original.AsSpan(0, HeaderSize).ToArray();
        BinaryPrimitives.WriteUInt32BigEndian(
            header.AsSpan(ManifestLengthOffset, 4),
            checked((uint)replacementManifest.Length));
        SHA256.HashData(replacementManifest).CopyTo(
            header.AsSpan(ManifestHashOffset, SHA256.HashSizeInBytes));

        byte[] replacement = new byte[checked(
            HeaderSize + replacementManifest.Length + rawSnapshot.Length)];
        header.CopyTo(replacement, 0);
        replacementManifest.CopyTo(replacement, HeaderSize);
        rawSnapshot.CopyTo(replacement.AsSpan(HeaderSize + replacementManifest.Length));
        await File.WriteAllBytesAsync(packagePath, replacement, Cancellation);
    }

    private static string RecomputeInnerDigest(string envelope)
    {
        string currentDigest = EnvelopeDigest(envelope);
        string digestProperty = $"\"digest\":\"{currentDigest}\",";
        int digestPropertyOffset = envelope.IndexOf(digestProperty, StringComparison.Ordinal);
        Assert.True(digestPropertyOffset > 0);
        string digestInput = envelope.Remove(digestPropertyOffset, digestProperty.Length);
        using JsonDocument document = JsonDocument.Parse(digestInput);
        JsonElement root = document.RootElement;
        Assert.Equal(CsvSnapshotPackage.Format, root.GetProperty("format").GetString());
        Assert.Equal("sha256", root.GetProperty("digestAlgorithm").GetString());
        Assert.Equal(JsonValueKind.Object, root.GetProperty("payload").ValueKind);

        string replacementDigest = Convert.ToHexString(
                SHA256.HashData(Encoding.UTF8.GetBytes(digestInput)))
            .ToLowerInvariant();
        return envelope.Remove(
                digestPropertyOffset + "\"digest\":\"".Length,
                currentDigest.Length)
            .Insert(
                digestPropertyOffset + "\"digest\":\"".Length,
                replacementDigest);
    }

    private static byte[] SerializeCompact(JsonNode node) => Encoding.UTF8.GetBytes(
        node.ToJsonString(new JsonSerializerOptions(JsonSerializerDefaults.Web)
        {
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            WriteIndented = false,
        }));

    private static string EnvelopeDigest(string envelope)
    {
        const string marker = "\"digest\":\"";
        int offset = envelope.IndexOf(marker, StringComparison.Ordinal);
        Assert.True(offset > 0);
        int valueOffset = offset + marker.Length;
        Assert.True(envelope.Length >= valueOffset + 64);
        return envelope.Substring(valueOffset, 64);
    }

    private static string ReplaceOnce(string value, string oldValue, string newValue)
    {
        int offset = value.IndexOf(oldValue, StringComparison.Ordinal);
        Assert.True(offset >= 0, $"Expected manifest token was not found: {oldValue}");
        Assert.Equal(-1, value.IndexOf(oldValue, offset + oldValue.Length, StringComparison.Ordinal));
        return value.Remove(offset, oldValue.Length).Insert(offset, newValue);
    }

    private static void AssertContainsBytes(
        ReadOnlySpan<byte> value,
        ReadOnlySpan<byte> expected) =>
        Assert.True(value.IndexOf(expected) >= 0, $"Missing UTF-8 bytes: {Encoding.UTF8.GetString(expected)}");

    private static async Task<CsvSnapshotPackageException> AssertOpenFailsAndPreservesAsync(
        TemporaryWorkspace workspace,
        string packagePath,
        string expectedRule,
        long? maxSourceBytes = null)
    {
        long lengthBefore = new FileInfo(packagePath).Length;
        CsvSnapshotPackageException error = await Assert.ThrowsAsync<CsvSnapshotPackageException>(
            async () =>
            {
                await using CsvSnapshotPackageSession session = await CsvSnapshotPackage.OpenAsync(
                    packagePath,
                    OpenOptions(workspace, maxSourceBytes),
                    Cancellation);
            });

        Assert.True(
            string.Equals(expectedRule, error.RuleId, StringComparison.Ordinal),
            error.InnerException?.ToString() ?? error.ToString());
        Assert.True(File.Exists(packagePath));
        Assert.Equal(lengthBefore, new FileInfo(packagePath).Length);
        return error;
    }

    private static CsvSnapshotPackageOpenOptions OpenOptions(
        TemporaryWorkspace workspace,
        long? maxSourceBytes = null) => new()
        {
            WorkspacePath = workspace.Root,
            MaxSourceBytes = maxSourceBytes ?? 1024 * 1024,
        };

    private static int ManifestLength(ReadOnlySpan<byte> packageBytes) => checked(
        (int)BinaryPrimitives.ReadUInt32BigEndian(
            packageBytes.Slice(ManifestLengthOffset, 4)));

    private static int RawSnapshotOffset(ReadOnlySpan<byte> packageBytes) => checked(
        HeaderSize + ManifestLength(packageBytes));

    private sealed class TemporaryWorkspace : IDisposable
    {
        internal TemporaryWorkspace()
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                $"csharpdb-csv-package-tests-{Guid.NewGuid():N}");
            Directory.CreateDirectory(Root);
        }

        internal string Root { get; }

        internal string PathFor(string fileName) => Path.Combine(Root, fileName);

        internal string[] PackageTempFiles() =>
            Directory.GetFiles(Root, ".csdbcsv-*.tmp", SearchOption.TopDirectoryOnly);

        public void Dispose()
        {
            if (Directory.Exists(Root))
                Directory.Delete(Root, recursive: true);
        }
    }
}
