using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using CSharpDB.Migration.Files.Json;

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonTypedIntentSidecarTests
{
    private const int MaximumColumns = 16_384;
    private const int MaximumDecodedBinaryBytes = 12 * 1024 * 1024;
    private const int MaximumDecimalDigits = 16 * 1024 * 1024;
    private const int MaximumManifestBytes = 4 * 1024 * 1024;
    private const int MaximumPayloadTextCharacters = 1024 * 1024;

    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task AllCodecsProduceDeterministicCanonicalBytesAndDigests()
    {
        using var workspace = new TemporaryDirectory();
        await using BoundSource source = await CreateSourceAsync(
            workspace,
            """
            [
              {
                "binary":"AA==",
                "decimalText":"1.25",
                "decimalNumber":1.25,
                "guid":"00112233-4455-6677-8899-aabbccddeeff",
                "date":"2026-07-23",
                "time":"08:09:10.1234567",
                "dateTime":"2026-07-23 08:09:10.1234567",
                "dateTimeOffset":"2026-07-23 08:09:10.1234567-07:00",
                "signed":"-1",
                "unsigned":"18446744073709551615"
              }
            ]
            """,
            logicalSourceIdentity: "typed/all-codecs");
        JsonTypedColumnIntent[] declarations = AllCodecDeclarations();
        var options = new JsonTypedIntentOptions
        {
            Columns = declarations,
            MaxDecodedBinaryBytes = MaximumDecodedBinaryBytes,
            MaxDecimalDigits = 1_000_000,
        };
        string firstPath = workspace.PathFor(
            "first" + JsonTypedIntentSidecar.FileExtension);
        string secondPath = workspace.PathFor(
            "second" + JsonTypedIntentSidecar.FileExtension);

        JsonTypedIntentManifest first =
            await JsonTypedIntentSidecar.WriteAsync(
                firstPath,
                source.Binding,
                options,
                Cancellation);
        JsonTypedIntentManifest second =
            await JsonTypedIntentSidecar.WriteAsync(
                secondPath,
                source.Binding,
                options,
                Cancellation);
        byte[] firstBytes = await File.ReadAllBytesAsync(
            firstPath,
            Cancellation);
        byte[] secondBytes = await File.ReadAllBytesAsync(
            secondPath,
            Cancellation);

        Assert.Equal(
            "csharpdb-json-table-intent/v1",
            JsonTypedIntentSidecar.Format);
        Assert.Equal(firstBytes, secondBytes);
        Assert.Equal(first.ManifestDigest, second.ManifestDigest);
        Assert.Equal(
            Digest(firstBytes),
            first.ManifestDigest);
        Assert.Equal(
            firstBytes,
            first.ToCanonicalUtf8Bytes());
        Assert.Equal(
            source.Binding.Source.Identity,
            first.SourceIdentity);
        Assert.Equal(
            source.Binding.Source.Fingerprint,
            first.SourceFingerprint);
        Assert.Equal(
            source.Binding.OptionsDigest,
            first.OptionsDigest);
        Assert.Equal(
            source.Snapshot.SnapshotIdentity,
            first.SnapshotIdentity);
        Assert.Equal(
            MaximumDecodedBinaryBytes,
            first.MaxDecodedBinaryBytes);
        Assert.Equal(1_000_000, first.MaxDecimalDigits);
        Assert.Equal(
            declarations.Length,
            first.Columns.Count);
        AssertColumnsEqual(
            declarations,
            first.Columns);

        using JsonDocument document =
            JsonDocument.Parse(firstBytes);
        JsonElement root = document.RootElement;
        Assert.Equal(
            ["format", "digestAlgorithm", "digest", "payload"],
            root.EnumerateObject()
                .Select(item => item.Name)
                .ToArray());
        Assert.Equal(
            ["contracts", "source", "limits", "columns"],
            root.GetProperty("payload")
                .EnumerateObject()
                .Select(item => item.Name)
                .ToArray());
        Assert.Equal(
            [
                "sourceBinding",
                "readerOptions",
                "propertyNameComparison",
                "typedValue",
                "textCodec",
            ],
            root.GetProperty("payload")
                .GetProperty("contracts")
                .EnumerateObject()
                .Select(item => item.Name)
                .ToArray());
        Assert.Equal(
            [
                "snapshotIdentity",
                "contentDigest",
                "contentLength",
                "identity",
                "fingerprint",
                "optionsDigest",
            ],
            root.GetProperty("payload")
                .GetProperty("source")
                .EnumerateObject()
                .Select(item => item.Name)
                .ToArray());
        Assert.Equal(
            64,
            root.GetProperty("digest")
                .GetString()!
                .Length);

        JsonTypedIntentManifest parsed =
            JsonTypedIntentSidecar.Parse(
                firstBytes,
                source.Binding,
                first.ManifestDigest);
        JsonTypedIntentManifest opened =
            await JsonTypedIntentSidecar.OpenAsync(
                firstPath,
                source.Binding,
                new JsonTypedIntentOpenOptions
                {
                    ExpectedManifestDigest =
                        first.ManifestDigest,
                },
                Cancellation);
        Assert.Equal(first.ManifestDigest, parsed.ManifestDigest);
        Assert.Equal(first.ManifestDigest, opened.ManifestDigest);
        AssertColumnsEqual(declarations, parsed.Columns);
        AssertColumnsEqual(declarations, opened.Columns);
    }

    [Fact]
    public async Task SparseBlankUnicodeAndCaseDistinctNamesRoundTripOrdinally()
    {
        using var workspace = new TemporaryDirectory();
        await using BoundSource source = await CreateSourceAsync(
            workspace,
            """[{"":"AA==","Name":"1","name":"2","Å":"3","Å":"4"}]""",
            logicalSourceIdentity: "typed/ordinal-names");
        JsonTypedColumnIntent[] declarations =
        [
            Intent(
                0,
                string.Empty,
                JsonTypedValueCodec.BinaryBase64,
                nullable: null),
            Intent(
                4,
                "Name",
                JsonTypedValueCodec.Int64String,
                nullable: true,
                missingPolicy:
                    JsonMissingPropertyPolicy.AsNull),
            Intent(
                9,
                "name",
                JsonTypedValueCodec.UInt64String,
                nullable: false),
            Intent(
                20,
                "\u00c5",
                JsonTypedValueCodec.DecimalString,
                precision: 10,
                scale: 2),
            Intent(
                21,
                "A\u030a",
                JsonTypedValueCodec.DecimalNumber,
                precision: 10,
                scale: 2),
        ];
        string path = workspace.PathFor(
            "names" + JsonTypedIntentSidecar.FileExtension);

        JsonTypedIntentManifest written =
            await JsonTypedIntentSidecar.WriteAsync(
                path,
                source.Binding,
                new JsonTypedIntentOptions
                {
                    Columns = declarations,
                },
                Cancellation);
        JsonTypedIntentManifest reopened =
            await JsonTypedIntentSidecar.OpenAsync(
                path,
                source.Binding,
                cancellationToken: Cancellation);

        AssertColumnsEqual(declarations, written.Columns);
        AssertColumnsEqual(declarations, reopened.Columns);
        Assert.NotEqual(
            reopened.Columns[1].ExpectedPropertyName,
            reopened.Columns[2].ExpectedPropertyName);
        Assert.NotEqual(
            reopened.Columns[3].ExpectedPropertyName,
            reopened.Columns[4].ExpectedPropertyName);
    }

    [Fact]
    public async Task PublicModelsAndCanonicalByteArraysAreDefensiveCopies()
    {
        using var workspace = new TemporaryDirectory();
        await using BoundSource source = await CreateSourceAsync(
            workspace,
            """[{"value":"AA=="}]""",
            logicalSourceIdentity: "typed/defensive-copy");
        JsonTypedColumnIntent[] callerColumns =
        [
            Intent(
                7,
                "value",
                JsonTypedValueCodec.BinaryBase64,
                nullable: true),
        ];
        var options = new JsonTypedIntentOptions
        {
            Columns = callerColumns,
        };
        string path = workspace.PathFor(
            "copy" + JsonTypedIntentSidecar.FileExtension);
        JsonTypedIntentManifest manifest =
            await JsonTypedIntentSidecar.WriteAsync(
                path,
                source.Binding,
                options,
                Cancellation);
        byte[] expected = manifest.ToCanonicalUtf8Bytes();

        callerColumns[0] = Intent(
            99,
            "changed",
            JsonTypedValueCodec.GuidD);
        byte[] firstCopy = manifest.ToCanonicalUtf8Bytes();
        firstCopy[0] ^= 0xff;
        byte[] callerBuffer =
            manifest.ToCanonicalUtf8Bytes();
        JsonTypedIntentManifest parsed =
            JsonTypedIntentSidecar.Parse(
                callerBuffer,
                source.Binding);
        byte[] parsedExpected =
            parsed.ToCanonicalUtf8Bytes();
        callerBuffer.AsSpan().Fill(0);

        Assert.Equal(expected, manifest.ToCanonicalUtf8Bytes());
        Assert.Equal(
            parsedExpected,
            parsed.ToCanonicalUtf8Bytes());
        Assert.Equal(7, manifest.Columns[0].ColumnIndex);
        Assert.Equal(
            "value",
            manifest.Columns[0].ExpectedPropertyName);
        Assert.Equal(
            JsonTypedValueCodec.BinaryBase64,
            manifest.Columns[0].Codec);
    }

    [Fact]
    public async Task InvalidColumnAndLimitRelationshipsFailBeforePublication()
    {
        using var workspace = new TemporaryDirectory();
        await using BoundSource source = await CreateSourceAsync(
            workspace,
            """[{"value":"1"}]""",
            logicalSourceIdentity: "typed/invalid-policy");
        var cases = new List<JsonTypedIntentOptions>
        {
            new(),
            Options(
                Intent(
                    -1,
                    "value",
                    JsonTypedValueCodec.Int64String)),
            Options(
                Intent(
                    MaximumColumns,
                    "value",
                    JsonTypedValueCodec.Int64String)),
            Options(
                Intent(
                    2,
                    "first",
                    JsonTypedValueCodec.Int64String),
                Intent(
                    2,
                    "second",
                    JsonTypedValueCodec.UInt64String)),
            Options(
                Intent(
                    2,
                    "later",
                    JsonTypedValueCodec.Int64String),
                Intent(
                    1,
                    "earlier",
                    JsonTypedValueCodec.UInt64String)),
            Options(
                Intent(
                    1,
                    "duplicate",
                    JsonTypedValueCodec.Int64String),
                Intent(
                    2,
                    "duplicate",
                    JsonTypedValueCodec.UInt64String)),
            Options(
                Intent(
                    0,
                    "value",
                    JsonTypedValueCodec.Int64String,
                    nullable: false,
                    missingPolicy:
                        JsonMissingPropertyPolicy.AsNull)),
            Options(
                Intent(
                    0,
                    "value",
                    JsonTypedValueCodec.DecimalString)),
            Options(
                Intent(
                    0,
                    "value",
                    JsonTypedValueCodec.DecimalNumber,
                    precision: 10)),
            Options(
                Intent(
                    0,
                    "value",
                    JsonTypedValueCodec.DecimalString,
                    precision: 0,
                    scale: 0)),
            Options(
                Intent(
                    0,
                    "value",
                    JsonTypedValueCodec.DecimalString,
                    precision: 10,
                    scale: -1)),
            Options(
                Intent(
                    0,
                    "value",
                    JsonTypedValueCodec.DecimalString,
                    precision: 10,
                    scale: 11)),
            Options(
                Intent(
                    0,
                    "value",
                    JsonTypedValueCodec.GuidD,
                    precision: 10,
                    scale: 0)),
            OptionsWithLimits(
                maxDecodedBinaryBytes: 0,
                maxDecimalDigits: 10),
            OptionsWithLimits(
                maxDecodedBinaryBytes:
                    MaximumDecodedBinaryBytes + 1,
                maxDecimalDigits: 10),
            OptionsWithLimits(
                maxDecodedBinaryBytes: 1,
                maxDecimalDigits: 0),
            OptionsWithLimits(
                maxDecodedBinaryBytes: 1,
                maxDecimalDigits:
                    MaximumDecimalDigits + 1),
            Options(
                Intent(
                    0,
                    "value",
                    (JsonTypedValueCodec)999)),
            Options(
                new JsonTypedColumnIntent
                {
                    ColumnIndex = 0,
                    ExpectedPropertyName = "value",
                    Codec =
                        JsonTypedValueCodec.Int64String,
                    MissingPolicy =
                        (JsonMissingPropertyPolicy)999,
                }),
            Options(
                Intent(
                    0,
                    "\ud800",
                    JsonTypedValueCodec.Int64String)),
        };

        for (int index = 0; index < cases.Count; index++)
        {
            string path = workspace.PathFor(
                $"invalid-{index}" +
                JsonTypedIntentSidecar.FileExtension);
            await Assert.ThrowsAnyAsync<ArgumentException>(
                    async () =>
                        await JsonTypedIntentSidecar
                            .WriteAsync(
                                path,
                                source.Binding,
                                cases[index],
                                Cancellation));

            Assert.False(File.Exists(path));
        }
    }

    [Fact]
    public async Task MaximumColumnCountIsAcceptedAndOneOverIsRejected()
    {
        using var workspace = new TemporaryDirectory();
        await using BoundSource source = await CreateSourceAsync(
            workspace,
            """[{"value":"1"}]""",
            logicalSourceIdentity: "typed/column-limit");
        JsonTypedColumnIntent[] exact =
            Enumerable.Range(0, MaximumColumns)
                .Select(index => Intent(
                    index,
                    "column-" +
                    index.ToString(
                        System.Globalization
                            .CultureInfo.InvariantCulture),
                    JsonTypedValueCodec.Int64String))
                .ToArray();
        string exactPath = workspace.PathFor(
            "exact-columns" +
            JsonTypedIntentSidecar.FileExtension);

        JsonTypedIntentManifest accepted =
            await JsonTypedIntentSidecar.WriteAsync(
                exactPath,
                source.Binding,
                new JsonTypedIntentOptions
                {
                    Columns = exact,
                },
                Cancellation);

        Assert.Equal(MaximumColumns, accepted.Columns.Count);
        Assert.True(
            new FileInfo(exactPath).Length <=
            MaximumManifestBytes);

        JsonTypedColumnIntent[] over =
        [
            .. exact,
            Intent(
                MaximumColumns,
                "one-over",
                JsonTypedValueCodec.Int64String),
        ];
        string overPath = workspace.PathFor(
            "over-columns" +
            JsonTypedIntentSidecar.FileExtension);
        await Assert.ThrowsAnyAsync<ArgumentException>(
                async () =>
                    await JsonTypedIntentSidecar.WriteAsync(
                        overPath,
                        source.Binding,
                        new JsonTypedIntentOptions
                        {
                            Columns = over,
                        },
                        Cancellation));

        Assert.False(File.Exists(overPath));
    }

    [Fact]
    public async Task AggregateTextAndManifestInputLimitsFailClosed()
    {
        using var workspace = new TemporaryDirectory();
        await using BoundSource source = await CreateSourceAsync(
            workspace,
            """[{"value":"1"}]""",
            logicalSourceIdentity: "typed/manifest-limits");
        JsonTypedColumnIntent[] textOverColumns =
            Enumerable.Range(0, MaximumColumns)
                .Select(index => Intent(
                    index,
                    index.ToString(
                            "D5",
                            System.Globalization
                                .CultureInfo.InvariantCulture) +
                    new string('x', 60),
                    JsonTypedValueCodec.Int64String))
                .ToArray();
        Assert.True(
            textOverColumns.Sum(
                item =>
                    item.ExpectedPropertyName.Length) >
            MaximumPayloadTextCharacters);
        string path = workspace.PathFor(
            "text-over" +
            JsonTypedIntentSidecar.FileExtension);
        await Assert.ThrowsAnyAsync<ArgumentException>(
                async () =>
                    await JsonTypedIntentSidecar.WriteAsync(
                        path,
                        source.Binding,
                        new JsonTypedIntentOptions
                        {
                            Columns = textOverColumns,
                        },
                        Cancellation));

        Assert.False(File.Exists(path));

        JsonTypedIntentException byteError =
            Assert.Throws<JsonTypedIntentException>(
                () => JsonTypedIntentSidecar.Parse(
                    new byte[MaximumManifestBytes + 1],
                    source.Binding));
        Assert.Equal(
            JsonTypedIntentRules.SizeLimitExceeded,
            byteError.RuleId);
    }

    [Fact]
    public async Task ExactSourceBindingIsRequiredIncludingSafeLogicalIdentity()
    {
        using var workspace = new TemporaryDirectory();
        const string json = """[{"value":"1"}]""";
        await using BoundSource first = await CreateSourceAsync(
            workspace,
            json,
            logicalSourceIdentity: "tenant/first");
        JsonSourceBinding changedIdentity =
            await JsonSourceBinding.CreateAsync(
                first.Snapshot,
                BaseReaderOptions(),
                logicalSourceIdentity: "tenant/second",
                Cancellation);
        await using BoundSource changedBytes =
            await CreateSourceAsync(
                workspace,
                """[{"value":"2"}]""",
                logicalSourceIdentity: "tenant/first");
        string path = workspace.PathFor(
            "bound" + JsonTypedIntentSidecar.FileExtension);
        JsonTypedIntentManifest manifest =
            await JsonTypedIntentSidecar.WriteAsync(
                path,
                first.Binding,
                Options(
                    Intent(
                        0,
                        "value",
                        JsonTypedValueCodec.Int64String)),
                Cancellation);
        byte[] bytes = manifest.ToCanonicalUtf8Bytes();

        JsonTypedIntentManifest exact =
            JsonTypedIntentSidecar.Parse(
                bytes,
                first.Binding);
        Assert.Equal(
            first.Binding.Source.Identity,
            exact.SourceIdentity);
        Assert.StartsWith(
            "json-logical:sha256:",
            exact.SourceIdentity,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "tenant/first",
            Encoding.UTF8.GetString(bytes),
            StringComparison.Ordinal);

        AssertSourceMismatch(bytes, changedIdentity);
        AssertSourceMismatch(bytes, changedBytes.Binding);
    }

    [Fact]
    public async Task EveryReaderPolicyFieldParticipatesInSourceBinding()
    {
        using var workspace = new TemporaryDirectory();
        await using BoundSource source = await CreateSourceAsync(
            workspace,
            """[{"value":"1"}]""",
            logicalSourceIdentity: "typed/options-binding");
        JsonTypedIntentManifest manifest =
            await JsonTypedIntentSidecar.WriteAsync(
                workspace.PathFor(
                    "options" +
                    JsonTypedIntentSidecar.FileExtension),
                source.Binding,
                Options(
                    Intent(
                        0,
                        "value",
                        JsonTypedValueCodec.Int64String)),
                Cancellation);
        byte[] bytes = manifest.ToCanonicalUtf8Bytes();
        JsonStreamingReaderOptions baseline =
            BaseReaderOptions();
        JsonStreamingReaderOptions[] changedOptions =
        [
            baseline with
            {
                Framing =
                    JsonInputFraming.MultipleValues,
            },
            baseline with
            {
                MaxValueBytes =
                    baseline.MaxValueBytes + 1,
            },
            baseline with
            {
                MaxDepth = baseline.MaxDepth + 1,
            },
            baseline with
            {
                MaxPropertiesPerObject =
                    baseline.MaxPropertiesPerObject + 1,
            },
            baseline with
            {
                MaxArrayElements =
                    baseline.MaxArrayElements + 1,
            },
            baseline with
            {
                MaxTotalNodes =
                    baseline.MaxTotalNodes + 1,
            },
            baseline with
            {
                MaxPropertyNameBytes =
                    baseline.MaxPropertyNameBytes + 1,
            },
            baseline with
            {
                MaxStringBytes =
                    baseline.MaxStringBytes + 1,
            },
            baseline with
            {
                MaxNumberBytes =
                    baseline.MaxNumberBytes + 1,
            },
        ];

        foreach (JsonStreamingReaderOptions changed in
                 changedOptions)
        {
            JsonSourceBinding drifted =
                await JsonSourceBinding.CreateAsync(
                    source.Snapshot,
                    changed,
                    logicalSourceIdentity:
                        "typed/options-binding",
                    Cancellation);
            AssertSourceMismatch(bytes, drifted);
        }

        JsonSourceBinding leaveOpenOnly =
            await JsonSourceBinding.CreateAsync(
                source.Snapshot,
                baseline with
                {
                    LeaveOpen = !baseline.LeaveOpen,
                },
                logicalSourceIdentity:
                    "typed/options-binding",
                Cancellation);
        JsonTypedIntentManifest accepted =
            JsonTypedIntentSidecar.Parse(
                bytes,
                leaveOpenOnly);
        Assert.Equal(
            manifest.ManifestDigest,
            accepted.ManifestDigest);
    }

    [Fact]
    public async Task ResignedSourceAndContractTamperStillFailsPolicyBinding()
    {
        using var workspace = new TemporaryDirectory();
        await using BoundSource source = await CreateSourceAsync(
            workspace,
            """[{"value":"1"}]""",
            logicalSourceIdentity: "typed/semantic-tamper");
        JsonTypedIntentManifest manifest =
            await JsonTypedIntentSidecar.WriteAsync(
                workspace.PathFor(
                    "semantic" +
                    JsonTypedIntentSidecar.FileExtension),
                source.Binding,
                Options(
                    Intent(
                        0,
                        "value",
                        JsonTypedValueCodec.Int64String)),
                Cancellation);
        byte[] canonical = manifest.ToCanonicalUtf8Bytes();
        const string zeroDigest =
            "sha256:0000000000000000000000000000000000000000000000000000000000000000";
        byte[][] sourceMutations =
        [
            MutateAndResign(
                canonical,
                payload =>
                {
                    JsonObject sourceNode = Source(payload);
                    sourceNode["contentDigest"] =
                        zeroDigest;
                    sourceNode["snapshotIdentity"] =
                        "json-snapshot-v1:" +
                        zeroDigest +
                        ":bytes:" +
                        source.Binding.ContentLength.ToString(
                            System.Globalization
                                .CultureInfo.InvariantCulture);
                }),
            MutateAndResign(
                canonical,
                payload =>
                    Source(payload)["identity"] =
                        "json-logical:sha256:" +
                        new string('0', 64)),
            MutateAndResign(
                canonical,
                payload =>
                    Source(payload)["fingerprint"] =
                        "sha256:" +
                        new string('0', 64)),
            MutateAndResign(
                canonical,
                payload =>
                    Source(payload)["optionsDigest"] =
                        "sha256:" +
                        new string('0', 64)),
        ];

        foreach (byte[] mutation in sourceMutations)
            AssertSourceMismatch(mutation, source.Binding);

        byte[] contractMutation = MutateAndResign(
            canonical,
            payload =>
                payload["contracts"]!
                    .AsObject()["typedValue"] =
                    "csharpdb-json-typed-value/v999");
        JsonTypedIntentException policy =
            Assert.Throws<JsonTypedIntentException>(
                () => JsonTypedIntentSidecar.Parse(
                    contractMutation,
                    source.Binding));
        Assert.Equal(
            JsonTypedIntentRules.PolicyMismatch,
            policy.RuleId);

        byte[] declarationMutation = MutateAndResign(
            canonical,
            payload =>
            {
                JsonObject column = payload["columns"]!
                    .AsArray()[0]!
                    .AsObject();
                column["precision"] = 1;
                column["scale"] = 0;
            });
        JsonTypedIntentException declaration =
            Assert.Throws<JsonTypedIntentException>(
                () => JsonTypedIntentSidecar.Parse(
                    declarationMutation,
                    source.Binding));
        Assert.Equal(
            JsonTypedIntentRules.PolicyMismatch,
            declaration.RuleId);

        byte[] limitMutation = MutateAndResign(
            canonical,
            payload =>
                payload["limits"]!
                    .AsObject()[
                        "maxDecodedBinaryBytes"] = 0);
        JsonTypedIntentException limit =
            Assert.Throws<JsonTypedIntentException>(
                () => JsonTypedIntentSidecar.Parse(
                    limitMutation,
                    source.Binding));
        Assert.Equal(
            JsonTypedIntentRules.SizeLimitExceeded,
            limit.RuleId);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(6)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(9)]
    [InlineData(10)]
    [InlineData(11)]
    public async Task StrictCanonicalManifestAdversariesAreRejected(
        int mutation)
    {
        using var workspace = new TemporaryDirectory();
        await using BoundSource source = await CreateSourceAsync(
            workspace,
            """[{"value":"1"}]""",
            logicalSourceIdentity: "typed/canonical-adversary");
        JsonTypedIntentManifest manifest =
            await JsonTypedIntentSidecar.WriteAsync(
                workspace.PathFor(
                    "canonical" +
                    JsonTypedIntentSidecar.FileExtension),
                source.Binding,
                Options(
                    Intent(
                        0,
                        "value",
                        JsonTypedValueCodec.Int64String)),
                Cancellation);
        byte[] canonical = manifest.ToCanonicalUtf8Bytes();
        byte[] adversarial = CreateCanonicalAdversary(
            canonical,
            mutation);

        JsonTypedIntentException error =
            Assert.Throws<JsonTypedIntentException>(
                () => JsonTypedIntentSidecar.Parse(
                    adversarial,
                    source.Binding));

        Assert.Equal(
            JsonTypedIntentRules.InvalidFormat,
            error.RuleId);
    }

    [Fact]
    public async Task PayloadDigestAndExpectedOuterDigestAreIndependentChecks()
    {
        using var workspace = new TemporaryDirectory();
        await using BoundSource source = await CreateSourceAsync(
            workspace,
            """[{"value":"1"}]""",
            logicalSourceIdentity: "typed/digest-pins");
        JsonTypedIntentManifest manifest =
            await JsonTypedIntentSidecar.WriteAsync(
                workspace.PathFor(
                    "digests" +
                    JsonTypedIntentSidecar.FileExtension),
                source.Binding,
                Options(
                    Intent(
                        0,
                        "value",
                        JsonTypedValueCodec.Int64String)),
                Cancellation);
        byte[] canonical = manifest.ToCanonicalUtf8Bytes();
        byte[] stalePayloadDigest =
            Encoding.UTF8.GetBytes(
                Encoding.UTF8.GetString(canonical)
                    .Replace(
                        "\"expectedPropertyName\":\"value\"",
                        "\"expectedPropertyName\":\"other\"",
                        StringComparison.Ordinal));
        JsonTypedIntentException stale =
            Assert.Throws<JsonTypedIntentException>(
                () => JsonTypedIntentSidecar.Parse(
                    stalePayloadDigest,
                    source.Binding));
        Assert.Equal(
            JsonTypedIntentRules.IntegrityMismatch,
            stale.RuleId);

        byte[] innerTamper = canonical.ToArray();
        int digestIndex = Encoding.UTF8
            .GetString(innerTamper)
            .IndexOf(
                "\"digest\":\"",
                StringComparison.Ordinal) +
            "\"digest\":\"".Length;
        innerTamper[digestIndex] =
            innerTamper[digestIndex] == (byte)'0'
                ? (byte)'1'
                : (byte)'0';

        JsonTypedIntentException inner =
            Assert.Throws<JsonTypedIntentException>(
                () => JsonTypedIntentSidecar.Parse(
                    innerTamper,
                    source.Binding));
        Assert.Equal(
            JsonTypedIntentRules.IntegrityMismatch,
            inner.RuleId);

        foreach (string malformedDigest in
                 new[]
                 {
                     new string('a', 63),
                     new string('A', 64),
                 })
        {
            JsonObject malformed =
                JsonNode.Parse(canonical)!.AsObject();
            malformed["digest"] = malformedDigest;
            JsonTypedIntentException format =
                Assert.Throws<JsonTypedIntentException>(
                    () => JsonTypedIntentSidecar.Parse(
                        SerializeCanonical(malformed),
                        source.Binding));
            Assert.Equal(
                JsonTypedIntentRules.InvalidFormat,
                format.RuleId);
        }

        string wrongPin =
            "sha256:" + new string('0', 64);
        JsonTypedIntentException outer =
            Assert.Throws<JsonTypedIntentException>(
                () => JsonTypedIntentSidecar.Parse(
                    canonical,
                    source.Binding,
                    wrongPin));
        Assert.Equal(
            JsonTypedIntentRules.IntegrityMismatch,
            outer.RuleId);

        JsonTypedIntentManifest accepted =
            JsonTypedIntentSidecar.Parse(
                canonical,
                source.Binding,
                manifest.ManifestDigest);
        Assert.Equal(
            manifest.ManifestDigest,
            accepted.ManifestDigest);

        Assert.Throws<ArgumentException>(
            () => JsonTypedIntentSidecar.Parse(
                canonical,
                source.Binding,
                "SHA256:" + new string('0', 64)));
    }

    [Fact]
    public async Task PrivacyKeepsRawIdentitySourceValuesAndCredentialMaterialOut()
    {
        using var workspace = new TemporaryDirectory();
        const string rawLogicalIdentity =
            "https://operator:password@example.test/source";
        const string sensitiveSourceValue =
            "authorization: bearer secret-value";
        await using BoundSource source = await CreateSourceAsync(
            workspace,
            $$"""[{"password":"{{sensitiveSourceValue}}"}]""",
            rawLogicalIdentity);
        string path = workspace.PathFor(
            "privacy" +
            JsonTypedIntentSidecar.FileExtension);
        JsonTypedIntentManifest manifest =
            await JsonTypedIntentSidecar.WriteAsync(
                path,
                source.Binding,
                Options(
                    Intent(
                        0,
                        "password",
                        JsonTypedValueCodec.GuidD)),
                Cancellation);
        string text = Encoding.UTF8.GetString(
            manifest.ToCanonicalUtf8Bytes());

        Assert.DoesNotContain(
            rawLogicalIdentity,
            text,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            sensitiveSourceValue,
            text,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            workspace.Root,
            text,
            StringComparison.OrdinalIgnoreCase);
        Assert.Contains(
            "\"expectedPropertyName\":\"password\"",
            text,
            StringComparison.Ordinal);
        Assert.StartsWith(
            "json-logical:sha256:",
            manifest.SourceIdentity,
            StringComparison.Ordinal);

        string unsafePath = workspace.PathFor(
            "unsafe" +
            JsonTypedIntentSidecar.FileExtension);
        await Assert.ThrowsAnyAsync<ArgumentException>(
                async () =>
                    await JsonTypedIntentSidecar.WriteAsync(
                        unsafePath,
                        source.Binding,
                        Options(
                            Intent(
                                0,
                                "password=not-retainable",
                                JsonTypedValueCodec.GuidD)),
                        Cancellation));
        Assert.False(File.Exists(unsafePath));
    }

    [Fact]
    public async Task StructuralErrorsDoNotEchoInjectedCredentialMaterial()
    {
        using var workspace = new TemporaryDirectory();
        await using BoundSource source = await CreateSourceAsync(
            workspace,
            """[{"value":"1"}]""",
            logicalSourceIdentity: "typed/error-privacy");
        JsonTypedIntentManifest manifest =
            await JsonTypedIntentSidecar.WriteAsync(
                workspace.PathFor(
                    "privacy-errors" +
                    JsonTypedIntentSidecar.FileExtension),
                source.Binding,
                Options(
                    Intent(
                        0,
                        "value",
                        JsonTypedValueCodec.Int64String)),
                Cancellation);
        byte[] canonical = manifest.ToCanonicalUtf8Bytes();
        const string injectedFormat =
            "password=should-never-appear";
        const string injectedProperty =
            "authorization: bearer should-never-appear";
        string text = Encoding.UTF8.GetString(canonical);
        byte[][] adversaries =
        [
            Encoding.UTF8.GetBytes(
                text.Replace(
                    JsonTypedIntentSidecar.Format,
                    injectedFormat,
                    StringComparison.Ordinal)),
            Encoding.UTF8.GetBytes(
                text.Replace(
                    "\"format\":",
                    "\"" + injectedProperty + "\":0,\"format\":",
                    StringComparison.Ordinal)
                    .Replace(
                        "\"format\":\"csharpdb-json-table-intent/v1\"",
                        "\"format\":\"csharpdb-json-table-intent/v1\",\"format\":\"csharpdb-json-table-intent/v1\"",
                        StringComparison.Ordinal)),
        ];

        foreach (byte[] adversarial in adversaries)
        {
            JsonTypedIntentException error =
                Assert.Throws<JsonTypedIntentException>(
                    () => JsonTypedIntentSidecar.Parse(
                        adversarial,
                        source.Binding));
            Assert.Equal(
                JsonTypedIntentRules.InvalidFormat,
                error.RuleId);
            string rendered = error.ToString();
            Assert.DoesNotContain(
                injectedFormat,
                rendered,
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                injectedProperty,
                rendered,
                StringComparison.Ordinal);
            Assert.DoesNotContain(
                "should-never-appear",
                rendered,
                StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task PublicationIsNoOverwriteAndPreservesExistingBytes()
    {
        using var workspace = new TemporaryDirectory();
        await using BoundSource source = await CreateSourceAsync(
            workspace,
            """[{"value":"1"}]""",
            logicalSourceIdentity: "typed/no-overwrite");
        string path = workspace.PathFor(
            "existing" +
            JsonTypedIntentSidecar.FileExtension);
        byte[] sentinel = "do-not-overwrite"u8.ToArray();
        await File.WriteAllBytesAsync(
            path,
            sentinel,
            Cancellation);

        await Assert.ThrowsAnyAsync<IOException>(
            async () =>
                await JsonTypedIntentSidecar.WriteAsync(
                    path,
                    source.Binding,
                    Options(
                        Intent(
                            0,
                            "value",
                            JsonTypedValueCodec.Int64String)),
                    Cancellation));

        Assert.Equal(
            sentinel,
            await File.ReadAllBytesAsync(
                path,
                Cancellation));
    }

    [Fact]
    public async Task PreCanceledPublicationLeavesNoDestinationOrTemporaryFile()
    {
        using var workspace = new TemporaryDirectory();
        await using BoundSource source = await CreateSourceAsync(
            workspace,
            """[{"value":"1"}]""",
            logicalSourceIdentity: "typed/cancel");
        string path = workspace.PathFor(
            "cancel" +
            JsonTypedIntentSidecar.FileExtension);
        string[] before = Directory.GetFileSystemEntries(
            workspace.Root);
        using var cancellation =
            new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () =>
                await JsonTypedIntentSidecar.WriteAsync(
                    path,
                    source.Binding,
                    Options(
                        Intent(
                            0,
                            "value",
                            JsonTypedValueCodec.Int64String)),
                    cancellation.Token));

        Assert.False(File.Exists(path));
        Assert.Equal(
            before.Order(StringComparer.Ordinal),
            Directory.GetFileSystemEntries(
                    workspace.Root)
                .Order(StringComparer.Ordinal));
    }

    private static byte[] CreateCanonicalAdversary(
        byte[] canonical,
        int mutation)
    {
        string text = Encoding.UTF8.GetString(canonical);
        return mutation switch
        {
            0 => [.. Encoding.UTF8.Preamble, .. canonical],
            1 => [.. canonical, (byte)' '],
            2 => Encoding.UTF8.GetBytes(
                text.Insert(1, "/*comment*/")),
            3 => Encoding.UTF8.GetBytes(
                text[..^1] + ",}"),
            4 => Encoding.UTF8.GetBytes(
                text.Replace(
                    "\"format\":",
                    "\"Format\":",
                    StringComparison.Ordinal)),
            5 => Encoding.UTF8.GetBytes(
                text.Replace(
                    "\"digestAlgorithm\":",
                    "\"unknown\":0,\"digestAlgorithm\":",
                    StringComparison.Ordinal)),
            6 => Encoding.UTF8.GetBytes(
                text.Replace(
                    "\"format\":\"csharpdb-json-table-intent/v1\"",
                    "\"format\":\"csharpdb-json-table-intent/v1\",\"format\":\"csharpdb-json-table-intent/v1\"",
                    StringComparison.Ordinal)),
            7 => Encoding.UTF8.GetBytes(
                "{\"digestAlgorithm\":\"sha256\"," +
                text[1..text.IndexOf(
                    ",\"digestAlgorithm\"",
                    StringComparison.Ordinal)] +
                text[text.IndexOf(
                    ",\"digestAlgorithm\"",
                    StringComparison.Ordinal)..]),
            8 => Encoding.UTF8.GetBytes(
                text.Replace(
                    "\"codec\":\"int64String\"",
                    "\"codec\":8",
                    StringComparison.Ordinal)),
            9 => Encoding.UTF8.GetBytes(
                text.Replace(
                    "\"columns\":[",
                    "\"columns\":null,\"ignored\":[",
                    StringComparison.Ordinal)),
            10 => [(byte)0xff, .. canonical],
            11 => [(byte)0, .. canonical],
            _ => throw new ArgumentOutOfRangeException(
                nameof(mutation)),
        };
    }

    private static byte[] MutateAndResign(
        byte[] canonical,
        Action<JsonObject> mutatePayload)
    {
        JsonObject envelope =
            JsonNode.Parse(canonical)!.AsObject();
        JsonObject payload =
            envelope["payload"]!.AsObject();
        mutatePayload(payload);

        var digestInput = new JsonObject
        {
            ["format"] =
                envelope["format"]!.DeepClone(),
            ["digestAlgorithm"] =
                envelope["digestAlgorithm"]!.DeepClone(),
            ["payload"] = payload.DeepClone(),
        };
        byte[] digestBytes =
            SerializeCanonical(digestInput);
        envelope["digest"] =
            Convert.ToHexString(
                    SHA256.HashData(digestBytes))
                .ToLowerInvariant();
        return SerializeCanonical(envelope);
    }

    private static JsonObject Source(
        JsonObject payload) =>
        payload["source"]!.AsObject();

    private static byte[] SerializeCanonical(
        JsonNode node)
    {
        using JsonDocument document =
            JsonDocument.Parse(node.ToJsonString());
        return JsonSnapshotPackageCanonicalJson.Serialize(
            document.RootElement);
    }

    private static void AssertSourceMismatch(
        byte[] bytes,
        JsonSourceBinding binding)
    {
        JsonTypedIntentException error =
            Assert.Throws<JsonTypedIntentException>(
                () => JsonTypedIntentSidecar.Parse(
                    bytes,
                    binding));
        Assert.Equal(
            JsonTypedIntentRules.SourceMismatch,
            error.RuleId);
    }

    private static JsonTypedIntentOptions Options(
        params JsonTypedColumnIntent[] columns) =>
        new()
        {
            Columns = columns,
            MaxDecodedBinaryBytes =
                MaximumDecodedBinaryBytes,
            MaxDecimalDigits = 1_000_000,
        };

    private static JsonTypedIntentOptions OptionsWithLimits(
        int maxDecodedBinaryBytes,
        int maxDecimalDigits) =>
        new()
        {
            Columns =
            [
                Intent(
                    0,
                    "value",
                    JsonTypedValueCodec.BinaryBase64),
            ],
            MaxDecodedBinaryBytes =
                maxDecodedBinaryBytes,
            MaxDecimalDigits = maxDecimalDigits,
        };

    private static JsonTypedColumnIntent Intent(
        int columnIndex,
        string propertyName,
        JsonTypedValueCodec codec,
        bool? nullable = null,
        JsonMissingPropertyPolicy missingPolicy =
            JsonMissingPropertyPolicy.Reject,
        int? precision = null,
        int? scale = null) =>
        new()
        {
            ColumnIndex = columnIndex,
            ExpectedPropertyName = propertyName,
            Codec = codec,
            Nullable = nullable,
            MissingPolicy = missingPolicy,
            Precision = precision,
            Scale = scale,
        };

    private static JsonTypedColumnIntent[]
        AllCodecDeclarations() =>
    [
        Intent(
            0,
            "binary",
            JsonTypedValueCodec.BinaryBase64,
            nullable: true),
        Intent(
            2,
            "decimalText",
            JsonTypedValueCodec.DecimalString,
            nullable: false,
            precision: 38,
            scale: 18),
        Intent(
            4,
            "decimalNumber",
            JsonTypedValueCodec.DecimalNumber,
            nullable: true,
            missingPolicy:
                JsonMissingPropertyPolicy.AsNull,
            precision: 200,
            scale: 40),
        Intent(
            6,
            "guid",
            JsonTypedValueCodec.GuidD),
        Intent(
            8,
            "date",
            JsonTypedValueCodec.DateCSharpDbText),
        Intent(
            10,
            "time",
            JsonTypedValueCodec.TimeCSharpDbText),
        Intent(
            12,
            "dateTime",
            JsonTypedValueCodec.DateTimeCSharpDbText),
        Intent(
            14,
            "dateTimeOffset",
            JsonTypedValueCodec.DateTimeOffsetCSharpDbText),
        Intent(
            16,
            "signed",
            JsonTypedValueCodec.Int64String),
        Intent(
            18,
            "unsigned",
            JsonTypedValueCodec.UInt64String),
    ];

    private static void AssertColumnsEqual(
        IReadOnlyList<JsonTypedColumnIntent> expected,
        IReadOnlyList<JsonTypedColumnIntent> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        for (int index = 0; index < expected.Count; index++)
        {
            Assert.Equal(
                expected[index].ColumnIndex,
                actual[index].ColumnIndex);
            Assert.Equal(
                expected[index].ExpectedPropertyName,
                actual[index].ExpectedPropertyName);
            Assert.Equal(
                expected[index].Codec,
                actual[index].Codec);
            Assert.Equal(
                expected[index].Nullable,
                actual[index].Nullable);
            Assert.Equal(
                expected[index].MissingPolicy,
                actual[index].MissingPolicy);
            Assert.Equal(
                expected[index].Precision,
                actual[index].Precision);
            Assert.Equal(
                expected[index].Scale,
                actual[index].Scale);
        }
    }

    private static string Digest(
        ReadOnlySpan<byte> bytes) =>
        "sha256:" +
        Convert.ToHexString(SHA256.HashData(bytes))
            .ToLowerInvariant();

    private static JsonStreamingReaderOptions
        BaseReaderOptions() =>
        new()
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
            LeaveOpen = false,
        };

    private static async ValueTask<BoundSource>
        CreateSourceAsync(
            TemporaryDirectory workspace,
            string json,
            string logicalSourceIdentity)
    {
        using var stream = new MemoryStream(
            Encoding.UTF8.GetBytes(json),
            writable: false);
        JsonSourceSnapshot snapshot =
            await JsonSourceSnapshot.CreateAsync(
                stream,
                new JsonSourceSnapshotOptions
                {
                    WorkspacePath = workspace.Root,
                    MaxSourceBytes = 4 * 1024 * 1024,
                    LeaveOpen = true,
                },
                Cancellation);
        try
        {
            JsonSourceBinding binding =
                await JsonSourceBinding.CreateAsync(
                    snapshot,
                    BaseReaderOptions(),
                    logicalSourceIdentity,
                    Cancellation);
            return new BoundSource(snapshot, binding);
        }
        catch
        {
            await snapshot.DisposeAsync();
            throw;
        }
    }

    private sealed class BoundSource(
        JsonSourceSnapshot snapshot,
        JsonSourceBinding binding)
        : IAsyncDisposable
    {
        internal JsonSourceSnapshot Snapshot { get; } =
            snapshot;

        internal JsonSourceBinding Binding { get; } =
            binding;

        public ValueTask DisposeAsync() =>
            Snapshot.DisposeAsync();
    }

    private sealed class TemporaryDirectory : IDisposable
    {
        internal TemporaryDirectory()
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                "csharpdb-json-intent-tests-" +
                Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(Root);
        }

        internal string Root { get; }

        internal string PathFor(string name) =>
            Path.Combine(Root, name);

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
