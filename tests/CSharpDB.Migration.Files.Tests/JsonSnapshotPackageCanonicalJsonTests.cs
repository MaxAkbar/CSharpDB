using System.Buffers.Binary;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Nodes;
using CSharpDB.Migration;
using CSharpDB.Migration.Files.Json;

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonSnapshotPackageCanonicalJsonTests
{
    private const int HeaderSize = 64;
    private const int ManifestLengthOffset = 16;
    private const int ManifestHashOffset = 32;

    private static readonly MethodInfo SerializeMethod =
        typeof(JsonSnapshotPackage).Assembly
            .GetType(
                "CSharpDB.Migration.Files.Json.JsonSnapshotPackageCanonicalJson",
                throwOnError: true)!
            .GetMethod(
                "Serialize",
                BindingFlags.Static |
                BindingFlags.NonPublic,
                binder: null,
                types: [typeof(JsonElement)],
                modifiers: null)!;

    private static string TargetVersion =>
        CSharpDbCapabilityCatalogLoader.CurrentTargetVersion;

    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    [Fact]
    public void CanonicalBytesAreDeterministicWithLiteralUnicodeAndLowercaseEscapes()
    {
        string value =
            string.Concat(
                Enumerable.Range(0, 32)
                    .Select(index => (char)index)) +
            "\"\\café_日本😀";
        JsonElement element =
            JsonSerializer.SerializeToElement(
                new OrderedValue(value));
        const string expected =
            "{\"value\":\"" +
            "\\u0000\\u0001\\u0002\\u0003\\u0004\\u0005\\u0006\\u0007" +
            "\\b\\t\\n\\u000b\\f\\r\\u000e\\u000f" +
            "\\u0010\\u0011\\u0012\\u0013\\u0014\\u0015\\u0016\\u0017" +
            "\\u0018\\u0019\\u001a\\u001b\\u001c\\u001d\\u001e\\u001f" +
            "\\\"\\\\café_日本😀\"}";

        byte[] first = Serialize(element);
        byte[] repeated = Serialize(element);
        using JsonDocument reparsed =
            JsonDocument.Parse(first);
        byte[] roundTripped =
            Serialize(reparsed.RootElement);

        Assert.Equal(
            Encoding.UTF8.GetBytes(expected),
            first);
        Assert.Equal(first, repeated);
        Assert.Equal(first, roundTripped);
        Assert.False(
            first.AsSpan().StartsWith(
                Encoding.UTF8.Preamble));
        Assert.Equal(
            value,
            reparsed.RootElement
                .GetProperty("value")
                .GetString());
    }

    [Fact]
    public async Task PackageManifestUsesCanonicalLiteralUnicodeAndReopens()
    {
        using var workspace = new TemporaryWorkspace();
        const string propertyName =
            "naïve \"名\"\tfield";
        const string tableName =
            "café_日本_\u000b_\u001f";
        const string json =
            """
            [
              {"naïve \"名\"\tfield":1,"value":"alpha"},
              {"naïve \"名\"\tfield":2,"value":"beta"}
            ]
            """;
        var options =
            new JsonTableSchemaInferenceOptions
            {
                TableName = tableName,
                ColumnOverrides =
                [
                    new JsonTableColumnSchemaOverride
                    {
                        ColumnIndex = 0,
                        ExpectedPropertyName =
                            propertyName,
                        LogicalType =
                            JsonTableColumnLogicalType
                                .SignedInteger,
                        Nullable = false,
                    },
                ],
            };
        PackageOrigin origin = await CreatePackageAsync(
            workspace,
            "canonical-unicode",
            json,
            logicalIdentity:
                "canonical/unicode-source",
            options);

        byte[] manifest =
            await ReadManifestAsync(origin.PackagePath);
        Assert.False(
            manifest.AsSpan().StartsWith(
                Encoding.UTF8.Preamble));
        AssertContainsBytes(
            manifest,
            Encoding.UTF8.GetBytes(
                "\"tableName\":\"café_日本_\\u000b_\\u001f\""));
        AssertContainsBytes(
            manifest,
            Encoding.UTF8.GetBytes(
                "\"expectedPropertyName\":\"naïve \\\"名\\\"\\tfield\""));
        string text = Encoding.UTF8.GetString(manifest);
        Assert.DoesNotContain(
            "\\u00e9",
            text,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            "\\u65e5",
            text,
            StringComparison.OrdinalIgnoreCase);

        await using JsonSnapshotPackageSession session =
            await JsonSnapshotPackage.OpenAsync(
                origin.PackagePath,
                OpenOptions(workspace),
                Cancellation);
        Assert.Equal(
            tableName,
            session.Schema.TableName);
        Assert.Equal(
            propertyName,
            session.Schema.Columns[0]
                .OriginalPropertyName);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task DuplicateUnknownOrMisCasedManifestPropertiesAreRejected(
        int mutation)
    {
        using var workspace = new TemporaryWorkspace();
        PackageOrigin origin = await CreatePackageAsync(
            workspace,
            "member-shape");
        string json = Encoding.UTF8.GetString(
            await ReadManifestAsync(origin.PackagePath));
        json = mutation switch
        {
            0 => json.Insert(
                json.IndexOf(
                    ",\"digestAlgorithm\"",
                    StringComparison.Ordinal),
                $",\"format\":\"{JsonSnapshotPackage.Format}\""),
            1 => ReplaceOnce(
                json,
                "\"payload\":{",
                "\"payload\":{\"unexpectedMember\":true,"),
            2 => ReplaceOnce(
                json,
                "\"contracts\":",
                "\"Contracts\":"),
            _ => throw new ArgumentOutOfRangeException(
                nameof(mutation)),
        };

        await ReplaceManifestAsync(
            origin.PackagePath,
            Encoding.UTF8.GetBytes(json));
        await AssertOpenFailsAndPreservesAsync(
            workspace,
            origin.PackagePath);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task ReorderedWhitespaceCommentOrTrailingCommaManifestIsRejected(
        int mutation)
    {
        using var workspace = new TemporaryWorkspace();
        PackageOrigin origin = await CreatePackageAsync(
            workspace,
            "noncanonical-json");
        byte[] canonical =
            await ReadManifestAsync(origin.PackagePath);
        byte[] replacement;
        switch (mutation)
        {
            case 0:
                JsonObject source =
                    await ReadEnvelopeAsync(
                        origin.PackagePath);
                var reordered =
                    new JsonObject
                    {
                        ["digestAlgorithm"] =
                            source["digestAlgorithm"]!
                                .DeepClone(),
                        ["format"] =
                            source["format"]!.DeepClone(),
                        ["digest"] =
                            source["digest"]!.DeepClone(),
                        ["payload"] =
                            source["payload"]!.DeepClone(),
                    };
                replacement = SerializeCompact(reordered);
                break;

            case 1:
                replacement = new byte[
                    canonical.Length + 1];
                replacement[0] = canonical[0];
                replacement[1] = (byte)' ';
                canonical.AsSpan(1).CopyTo(
                    replacement.AsSpan(2));
                break;

            case 2:
                replacement =
                [
                    (byte)'{',
                    .. "/*comment*/"u8,
                    .. canonical.AsSpan(1).ToArray(),
                ];
                break;

            case 3:
                Assert.Equal(
                    (byte)'}',
                    canonical[^1]);
                replacement =
                [
                    .. canonical.AsSpan(
                        0,
                        canonical.Length - 1).ToArray(),
                    (byte)',',
                    (byte)'}',
                ];
                break;

            default:
                throw new ArgumentOutOfRangeException(
                    nameof(mutation));
        }

        await ReplaceManifestAsync(
            origin.PackagePath,
            replacement);
        await AssertOpenFailsAndPreservesAsync(
            workspace,
            origin.PackagePath);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task BomNulOrInvalidUtf8ManifestIsRejectedWithValidOuterHash(
        int mutation)
    {
        using var workspace = new TemporaryWorkspace();
        PackageOrigin origin = await CreatePackageAsync(
            workspace,
            "invalid-encoding");
        byte[] canonical =
            await ReadManifestAsync(origin.PackagePath);
        byte[] prefix = mutation switch
        {
            0 => Encoding.UTF8.Preamble.ToArray(),
            1 => [0x00],
            2 => [0xFF],
            _ => throw new ArgumentOutOfRangeException(
                nameof(mutation)),
        };
        byte[] replacement = new byte[
            prefix.Length + canonical.Length];
        prefix.CopyTo(replacement, 0);
        canonical.CopyTo(
            replacement,
            prefix.Length);

        await ReplaceManifestAsync(
            origin.PackagePath,
            replacement);
        await AssertOpenFailsAndPreservesAsync(
            workspace,
            origin.PackagePath);
    }

    [Fact]
    public async Task IntegerEncodedEnumIsRejectedAfterOuterHashVerification()
    {
        using var workspace = new TemporaryWorkspace();
        PackageOrigin origin = await CreatePackageAsync(
            workspace,
            "integer-enum");
        string json = Encoding.UTF8.GetString(
            await ReadManifestAsync(origin.PackagePath));
        json = ReplaceOnce(
            json,
            "\"logicalType\":\"signedInteger\"",
            "\"logicalType\":1");

        await ReplaceManifestAsync(
            origin.PackagePath,
            Encoding.UTF8.GetBytes(json));
        await AssertOpenFailsAndPreservesAsync(
            workspace,
            origin.PackagePath);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task IncorrectOrUppercaseInnerDigestIsRejected(
        bool uppercase)
    {
        using var workspace = new TemporaryWorkspace();
        PackageOrigin origin = await CreatePackageAsync(
            workspace,
            "inner-digest");
        string json = Encoding.UTF8.GetString(
            await ReadManifestAsync(origin.PackagePath));
        string digest = EnvelopeDigest(json);
        string replacement = uppercase
            ? digest.ToUpperInvariant()
            : (digest[0] == '0' ? "1" : "0") +
              digest[1..];
        json = ReplaceOnce(
            json,
            digest,
            replacement);

        await ReplaceManifestAsync(
            origin.PackagePath,
            Encoding.UTF8.GetBytes(json));
        await AssertOpenFailsAndPreservesAsync(
            workspace,
            origin.PackagePath);
    }

    [Fact]
    public async Task ManifestDoesNotExposePathLogicalIdentityOrRawSourceValues()
    {
        using var workspace = new TemporaryWorkspace();
        const string sourceFileName =
            "sensitive-original-source-name.json";
        const string logicalIdentity =
            "private-logical-source-customer-42";
        const string rawValue =
            "raw-customer-value-do-not-retain-93";
        string sourcePath = workspace.PathFor(
            sourceFileName);
        string packagePath = workspace.PathFor(
            "private" + JsonSnapshotPackage.FileExtension);
        await File.WriteAllTextAsync(
            sourcePath,
            $$"""[{"id":1,"value":"{{rawValue}}"}]""",
            new UTF8Encoding(false, true),
            Cancellation);
        JsonSourceSnapshot snapshot =
            await JsonSourceSnapshot.CreateFromFileAsync(
                sourcePath,
                new JsonSourceSnapshotOptions
                {
                    WorkspacePath = workspace.Root,
                },
                Cancellation);
        await using (snapshot)
        {
            JsonTableSchemaInferenceResult schema =
                await InferAsync(
                    snapshot,
                    logicalIdentity,
                    options: null);
            await JsonSnapshotPackage.WriteAsync(
                packagePath,
                snapshot,
                schema,
                TargetVersion,
                Cancellation);
        }

        string manifest = Encoding.UTF8.GetString(
            await ReadManifestAsync(packagePath));
        Assert.DoesNotContain(
            sourcePath,
            manifest,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            sourceFileName,
            manifest,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            logicalIdentity,
            manifest,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            rawValue,
            manifest,
            StringComparison.Ordinal);
        Assert.Contains(
            "json-logical:sha256:",
            manifest,
            StringComparison.Ordinal);
    }

    private static byte[] Serialize(JsonElement element) =>
        Assert.IsType<byte[]>(
            SerializeMethod.Invoke(
                obj: null,
                parameters: [element]));

    private static async ValueTask<PackageOrigin>
        CreatePackageAsync(
            TemporaryWorkspace workspace,
            string name,
            string json =
                """
                [
                  {"id":1,"name":"alpha"},
                  {"id":2,"name":"beta"}
                ]
                """,
            string logicalIdentity =
                "canonical/package-source",
            JsonTableSchemaInferenceOptions? options =
                null)
    {
        string sourcePath = workspace.PathFor(
            name + ".json");
        string packagePath = workspace.PathFor(
            name + JsonSnapshotPackage.FileExtension);
        await File.WriteAllTextAsync(
            sourcePath,
            json,
            new UTF8Encoding(false, true),
            Cancellation);
        JsonSourceSnapshot snapshot =
            await JsonSourceSnapshot.CreateFromFileAsync(
                sourcePath,
                new JsonSourceSnapshotOptions
                {
                    WorkspacePath = workspace.Root,
                    MaxSourceBytes = 1024 * 1024,
                },
                Cancellation);
        try
        {
            JsonTableSchemaInferenceOptions effective =
                options ??
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
                };
            JsonTableSchemaInferenceResult schema =
                await InferAsync(
                    snapshot,
                    logicalIdentity,
                    effective);
            JsonSnapshotPackageManifest manifest =
                await JsonSnapshotPackage.WriteAsync(
                    packagePath,
                    snapshot,
                    schema,
                    TargetVersion,
                    Cancellation);
            return new PackageOrigin(
                sourcePath,
                packagePath,
                manifest);
        }
        finally
        {
            await snapshot.DisposeAsync();
        }
    }

    private static async ValueTask<
        JsonTableSchemaInferenceResult> InferAsync(
            JsonSourceSnapshot snapshot,
            string logicalIdentity,
            JsonTableSchemaInferenceOptions? options)
    {
        JsonSourceBinding binding =
            await JsonSourceBinding.CreateAsync(
                snapshot,
                new JsonStreamingReaderOptions
                {
                    Framing = JsonInputFraming.RootArray,
                    MaxValueBytes = 256 * 1024,
                    MaxDepth = 32,
                    MaxPropertiesPerObject = 256,
                    MaxArrayElements = 1_024,
                    MaxTotalNodes = 2_048,
                    MaxPropertyNameBytes = 8 * 1_024,
                    MaxStringBytes = 128 * 1_024,
                    MaxNumberBytes = 8 * 1_024,
                },
                logicalIdentity,
                Cancellation);
        return await JsonTableSchemaInferer.InferAsync(
            binding,
            snapshot,
            maxProfileRecords: 100,
            options,
            Cancellation);
    }

    private static async ValueTask<byte[]>
        ReadManifestAsync(string packagePath)
    {
        byte[] package = await File.ReadAllBytesAsync(
            packagePath,
            Cancellation);
        int manifestLength = ManifestLength(package);
        return package
            .AsSpan(HeaderSize, manifestLength)
            .ToArray();
    }

    private static async ValueTask<JsonObject>
        ReadEnvelopeAsync(string packagePath)
    {
        byte[] manifest =
            await ReadManifestAsync(packagePath);
        JsonNode? node = JsonNode.Parse(
            Encoding.UTF8.GetString(manifest));
        Assert.NotNull(node);
        return Assert.IsType<JsonObject>(node);
    }

    private static async ValueTask ReplaceManifestAsync(
        string packagePath,
        byte[] replacementManifest)
    {
        byte[] original =
            await File.ReadAllBytesAsync(
                packagePath,
                Cancellation);
        int rawOffset = RawSnapshotOffset(original);
        ReadOnlySpan<byte> rawSnapshot =
            original.AsSpan(rawOffset);
        byte[] header =
            original.AsSpan(0, HeaderSize).ToArray();
        BinaryPrimitives.WriteUInt32BigEndian(
            header.AsSpan(
                ManifestLengthOffset,
                sizeof(uint)),
            checked((uint)replacementManifest.Length));
        SHA256.HashData(replacementManifest)
            .CopyTo(
                header.AsSpan(
                    ManifestHashOffset,
                    SHA256.HashSizeInBytes));

        byte[] replacement = new byte[checked(
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

    private static async Task
        AssertOpenFailsAndPreservesAsync(
            TemporaryWorkspace workspace,
            string packagePath)
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
                                    OpenOptions(workspace),
                                    Cancellation);
                });

        Assert.Equal(
            JsonSnapshotPackageRules.InvalidFormat,
            error.RuleId);
        Assert.Equal(
            before,
            await File.ReadAllBytesAsync(
                packagePath,
                Cancellation));
        Assert.Empty(
            Directory.EnumerateDirectories(
                workspace.Root));
    }

    private static JsonSnapshotPackageOpenOptions
        OpenOptions(TemporaryWorkspace workspace) =>
        new()
        {
            WorkspacePath = workspace.Root,
            MaxSourceBytes = 1024 * 1024,
        };

    private static byte[] SerializeCompact(
        JsonNode node) =>
        Encoding.UTF8.GetBytes(
            node.ToJsonString(
                new JsonSerializerOptions(
                    JsonSerializerDefaults.Web)
                {
                    Encoder =
                        JavaScriptEncoder
                            .UnsafeRelaxedJsonEscaping,
                    WriteIndented = false,
                }));

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
        return envelope.Substring(valueOffset, 64);
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

    private static void AssertContainsBytes(
        ReadOnlySpan<byte> value,
        ReadOnlySpan<byte> expected) =>
        Assert.True(
            value.IndexOf(expected) >= 0,
            "Missing UTF-8 bytes: " +
            Encoding.UTF8.GetString(expected));

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

    private sealed record OrderedValue(
        [property:
            System.Text.Json.Serialization
                .JsonPropertyName("value")]
        string Value);

    private sealed record PackageOrigin(
        string SourcePath,
        string PackagePath,
        JsonSnapshotPackageManifest Manifest);

    private sealed class TemporaryWorkspace :
        IDisposable
    {
        internal TemporaryWorkspace()
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                "csharpdb-json-package-canonical-tests-" +
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
