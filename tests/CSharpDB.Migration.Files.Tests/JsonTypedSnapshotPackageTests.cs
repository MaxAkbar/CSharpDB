using System.Buffers.Binary;
using System.Text;
using System.Text.Json;
using CSharpDB.Migration;
using CSharpDB.Migration.Files.Json;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Files.Tests;

public sealed class JsonTypedSnapshotPackageTests
{
    private static string TargetVersion =>
        CSharpDbCapabilityCatalogLoader.CurrentTargetVersion;

    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(JsonInputFraming.RootArray)]
    [InlineData(JsonInputFraming.MultipleValues)]
    public async Task DeterministicPackageReopensExactIntentAfterOriginalsDisappear(
        JsonInputFraming framing)
    {
        using var workspace = new TemporaryDirectory();
        await using TypedFixture origin =
            await TypedFixture.CreateAsync(
                workspace,
                "deterministic",
                framing,
                Frame(
                    framing,
                    """
                    {"binary":"AQIDBA==","amount":"12345678901234567890.123456789012345678","id":"9223372036854775807","ordinary":"alpha"}
                    """,
                    """
                    {"binary":"BQY=","amount":"0.000000000000000001","id":"-9223372036854775808","ordinary":"bravo"}
                    """),
                new JsonTypedIntentOptions
                {
                    Columns =
                    [
                        Intent(
                            0,
                            "binary",
                            JsonTypedValueCodec.BinaryBase64,
                            nullable: false),
                        Intent(
                            1,
                            "amount",
                            JsonTypedValueCodec.DecimalString,
                            nullable: false,
                            precision: 38,
                            scale: 18),
                        Intent(
                            2,
                            "id",
                            JsonTypedValueCodec.Int64String,
                            nullable: false),
                    ],
                    MaxDecodedBinaryBytes = 1024,
                    MaxDecimalDigits = 128,
                },
                new JsonTableSchemaInferenceOptions
                {
                    TableName = "typed_orders",
                    ColumnOverrides =
                    [
                        new JsonTableColumnSchemaOverride
                        {
                            ColumnIndex = 3,
                            ExpectedPropertyName = "ordinary",
                            LogicalType =
                                JsonTableColumnLogicalType.Text,
                            Nullable = false,
                        },
                    ],
                });
        string firstPath = workspace.PathFor(
            "first" + JsonTypedSnapshotPackage.FileExtension);
        string secondPath = workspace.PathFor(
            "second" + JsonTypedSnapshotPackage.FileExtension);

        JsonTypedSnapshotPackageManifest first =
            await JsonTypedSnapshotPackage.WriteAsync(
                firstPath,
                origin.Snapshot,
                origin.Schema,
                TargetVersion,
                Cancellation);
        JsonTypedSnapshotPackageManifest second =
            await JsonTypedSnapshotPackage.WriteAsync(
                secondPath,
                origin.Snapshot,
                origin.Schema,
                TargetVersion,
                Cancellation);

        await origin.Snapshot.VerifyIntegrityAsync(Cancellation);
        byte[] expectedIntent =
            origin.Intent.ToCanonicalUtf8Bytes();
        byte[] firstBytes =
            await File.ReadAllBytesAsync(firstPath, Cancellation);
        byte[] secondBytes =
            await File.ReadAllBytesAsync(secondPath, Cancellation);
        Assert.Equal(firstBytes, secondBytes);
        Assert.Equal("CSDBJSN2"u8.ToArray(), firstBytes[..8]);
        Assert.Equal(
            "csharpdb-json-snapshot-package/v2",
            JsonTypedSnapshotPackage.Format);
        Assert.Equal(
            JsonSnapshotPackage.FileExtension,
            JsonTypedSnapshotPackage.FileExtension);
        AssertManifestEquivalent(first, second);
        Assert.Equal(
            origin.Intent.ManifestDigest,
            first.IntentManifestDigest);
        int manifestLength = checked((int)
            BinaryPrimitives.ReadUInt32BigEndian(
                firstBytes.AsSpan(16, sizeof(uint))));
        int intentLength = checked((int)
            BinaryPrimitives.ReadUInt32BigEndian(
                firstBytes.AsSpan(20, sizeof(uint))));
        using (JsonDocument document = JsonDocument.Parse(
            firstBytes.AsMemory(112, manifestLength)))
        {
            JsonElement overrides = document.RootElement
                .GetProperty("payload")
                .GetProperty("inference")
                .GetProperty("columnOverrides");
            JsonElement retained = Assert.Single(
                overrides.EnumerateArray());
            Assert.Equal(
                3,
                retained.GetProperty("columnIndex")
                    .GetInt32());
        }
        Assert.Equal(
            expectedIntent,
            firstBytes.AsSpan(
                112 + manifestLength,
                intentLength).ToArray());

        await origin.DisposeAsync();
        await File.WriteAllTextAsync(
            origin.SourcePath,
            """[{"binary":"changed"}]""",
            StrictUtf8,
            Cancellation);
        await File.WriteAllTextAsync(
            origin.SidecarPath,
            """{"changed":true}""",
            StrictUtf8,
            Cancellation);

        await using (JsonTypedSnapshotPackageSession session =
            await OpenAsync(
                firstPath,
                workspace.Root,
                first.ManifestDigest))
        {
            AssertManifestEquivalent(first, session.Manifest);
            Assert.Equal(
                expectedIntent,
                session.IntentManifest.ToCanonicalUtf8Bytes());
            Assert.Equal(
                first.IntentManifestDigest,
                session.IntentManifest.ManifestDigest);
            Assert.Equal(
                first.CatalogDigest,
                MigrationArtifactSerializer.ComputeCatalogDigest(
                    session.Catalog));
            Assert.Equal("typed_orders", session.Schema.TableName);
            Assert.NotNull(session.Schema.Columns[0].Intent);
            Assert.NotNull(session.Schema.Columns[1].Intent);
            Assert.NotNull(session.Schema.Columns[2].Intent);
            Assert.Null(session.Schema.Columns[3].Intent);
            Assert.Equal(
                JsonTableColumnLogicalType.Text,
                session.Schema.Columns[3]
                    .RepresentationSchema.LogicalType);
            await AssertTypedRowsAsync(session.DataSource);
        }

        File.Delete(origin.SourcePath);
        File.Delete(origin.SidecarPath);
        await using (JsonTypedSnapshotPackageSession session =
            await OpenAsync(
                secondPath,
                workspace.Root,
                second.ManifestDigest))
        {
            Assert.Equal(
                expectedIntent,
                session.IntentManifest.ToCanonicalUtf8Bytes());
            await AssertTypedRowsAsync(session.DataSource);
        }

        Assert.Equal(
            firstBytes,
            await File.ReadAllBytesAsync(firstPath, Cancellation));
        Assert.Equal(
            secondBytes,
            await File.ReadAllBytesAsync(secondPath, Cancellation));
        Assert.Empty(Directory.EnumerateDirectories(workspace.Root));
    }

    [Fact]
    public async Task ReopenRetainsOrdinaryOverridesAndResynthesizesTypedColumns()
    {
        using var workspace = new TemporaryDirectory();
        await using TypedFixture origin =
            await TypedFixture.CreateAsync(
                workspace,
                "overrides",
                JsonInputFraming.RootArray,
                """
                [
                  {"typed":"1","ordinary":"alpha"},
                  {"typed":"2"}
                ]
                """,
                new JsonTypedIntentOptions
                {
                    Columns =
                    [
                        Intent(
                            0,
                            "typed",
                            JsonTypedValueCodec.Int64String,
                            nullable: false),
                    ],
                },
                new JsonTableSchemaInferenceOptions
                {
                    TableName = "override_table",
                    ColumnOverrides =
                    [
                        new JsonTableColumnSchemaOverride
                        {
                            ColumnIndex = 1,
                            ExpectedPropertyName = "ordinary",
                            LogicalType =
                                JsonTableColumnLogicalType.Text,
                            Nullable = true,
                            MissingPolicy =
                                JsonMissingPropertyPolicy.AsNull,
                        },
                    ],
                });
        string packagePath = workspace.PathFor(
            "overrides" +
            JsonTypedSnapshotPackage.FileExtension);
        JsonTypedSnapshotPackageManifest manifest =
            await JsonTypedSnapshotPackage.WriteAsync(
                packagePath,
                origin.Snapshot,
                origin.Schema,
                TargetVersion,
                Cancellation);
        await origin.DisposeAsync();
        File.Delete(origin.SidecarPath);
        File.Delete(origin.SourcePath);

        await using JsonTypedSnapshotPackageSession session =
            await OpenAsync(
                packagePath,
                workspace.Root,
                manifest.ManifestDigest);
        JsonTypedTableColumnSchema typed =
            session.Schema.Columns[0];
        JsonTypedTableColumnSchema ordinary =
            session.Schema.Columns[1];
        Assert.Equal(
            JsonTypedValueCodec.Int64String,
            Assert.IsType<JsonTypedColumnIntent>(
                typed.Intent).Codec);
        Assert.Equal(
            JsonTableColumnLogicalType.Text,
            typed.RepresentationSchema.LogicalType);
        Assert.False(typed.Nullable);
        Assert.Null(ordinary.Intent);
        Assert.Equal(
            JsonTableColumnLogicalType.Text,
            ordinary.RepresentationSchema.LogicalType);
        Assert.True(ordinary.Nullable);
        Assert.Equal(
            JsonMissingPropertyPolicy.AsNull,
            ordinary.MissingPolicy);

        List<MigrationDataBatch> batches = await CollectAsync(
            session.DataSource.ReadAsync(
                Request(
                    session.DataSource,
                    [
                        JsonMigrationObjectIds.Column(0),
                        JsonMigrationObjectIds.Column(1),
                    ],
                    batchSize: 10),
                Cancellation));
        MigrationDataRow[] rows = batches
            .SelectMany(batch => batch.Rows)
            .ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal("1", rows[0].Values[0].CanonicalText);
        Assert.Equal("alpha", rows[0].Values[1].CanonicalText);
        Assert.Equal("2", rows[1].Values[0].CanonicalText);
        Assert.Equal(
            MigrationSourceValueKind.Null,
            rows[1].Values[1].Kind);
    }

    [Fact]
    public async Task ReopenedPackageAppliesRemainingCodecsAndDeterministicReject()
    {
        using var workspace = new TemporaryDirectory();
        await using TypedFixture origin =
            await TypedFixture.CreateAsync(
                workspace,
                "remaining-codecs",
                JsonInputFraming.RootArray,
                """
                [
                  {
                    "decimalNumber":123.45,
                    "guid":"00112233-4455-6677-8899-aabbccddeeff",
                    "date":"2024-02-29",
                    "time":"08:09:10.1234567",
                    "dateTime":"2026-07-23 08:09:10.1234567",
                    "dateTimeOffset":"2026-07-23 08:09:10.1234567-07:00",
                    "uint64":"18446744073709551615",
                    "ordinary":"accepted"
                  },
                  {
                    "decimalNumber":1.23,
                    "guid":"00112233-4455-6677-8899-AABBCCDDEEFF",
                    "date":"2024-02-29",
                    "time":"08:09:10.1234567",
                    "dateTime":"2026-07-23 08:09:10.1234567",
                    "dateTimeOffset":"2026-07-23 08:09:10.1234567-07:00",
                    "uint64":"1",
                    "ordinary":"rejected"
                  }
                ]
                """,
                new JsonTypedIntentOptions
                {
                    Columns =
                    [
                        Intent(
                            0,
                            "decimalNumber",
                            JsonTypedValueCodec.DecimalNumber,
                            nullable: false,
                            precision: 10,
                            scale: 2),
                        Intent(
                            1,
                            "guid",
                            JsonTypedValueCodec.GuidD,
                            nullable: false),
                        Intent(
                            2,
                            "date",
                            JsonTypedValueCodec.DateCSharpDbText,
                            nullable: false),
                        Intent(
                            3,
                            "time",
                            JsonTypedValueCodec.TimeCSharpDbText,
                            nullable: false),
                        Intent(
                            4,
                            "dateTime",
                            JsonTypedValueCodec
                                .DateTimeCSharpDbText,
                            nullable: false),
                        Intent(
                            5,
                            "dateTimeOffset",
                            JsonTypedValueCodec
                                .DateTimeOffsetCSharpDbText,
                            nullable: false),
                        Intent(
                            6,
                            "uint64",
                            JsonTypedValueCodec.UInt64String,
                            nullable: false),
                    ],
                },
                new JsonTableSchemaInferenceOptions
                {
                    ColumnOverrides =
                    [
                        new JsonTableColumnSchemaOverride
                        {
                            ColumnIndex = 7,
                            ExpectedPropertyName = "ordinary",
                            LogicalType =
                                JsonTableColumnLogicalType.Text,
                            Nullable = false,
                        },
                    ],
                });
        string packagePath = workspace.PathFor(
            "remaining-codecs" +
            JsonTypedSnapshotPackage.FileExtension);
        JsonTypedSnapshotPackageManifest manifest =
            await JsonTypedSnapshotPackage.WriteAsync(
                packagePath,
                origin.Snapshot,
                origin.Schema,
                TargetVersion,
                Cancellation);
        await origin.DisposeAsync();
        File.Delete(origin.SourcePath);
        File.Delete(origin.SidecarPath);

        await using JsonTypedSnapshotPackageSession session =
            await OpenAsync(
                packagePath,
                workspace.Root,
                manifest.ManifestDigest);
        MigrationPlan plan = ReadyRejectPlan(
            session.Catalog,
            batchSize: 10);
        await using var target =
            new JsonMigrationDataSourceIntegrationTests
                .ReceiptMigrationTarget();
        MigrationApplyResult applied =
            await new MigrationApplyRunner().ApplyAsync(
                new MigrationApplyRequest
                {
                    Plan = plan,
                    Catalog = session.Catalog,
                    Source = session.DataSource,
                    Target = target,
                },
                Cancellation);

        Assert.Equal(1, applied.RowsWritten);
        Assert.Equal(1, applied.RejectedRowsWritten);
        MigrationTargetBatch batch =
            Assert.Single(target.Batches);
        MigrationTargetRow accepted =
            Assert.Single(batch.Rows);
        Dictionary<string, DbValue> values =
            batch.ColumnObjectIds
                .Zip(accepted.Values)
                .ToDictionary(
                    item => item.First,
                    item => item.Second,
                    StringComparer.Ordinal);
        Assert.Equal(
            123.45m,
            values[JsonMigrationObjectIds.Column(0)]
                .AsDecimal);
        Assert.Equal(
            "00112233-4455-6677-8899-aabbccddeeff",
            values[JsonMigrationObjectIds.Column(1)]
                .AsText);
        Assert.Equal(
            "2024-02-29",
            values[JsonMigrationObjectIds.Column(2)]
                .AsText);
        Assert.Equal(
            "08:09:10.1234567",
            values[JsonMigrationObjectIds.Column(3)]
                .AsText);
        Assert.Equal(
            "2026-07-23 08:09:10.1234567",
            values[JsonMigrationObjectIds.Column(4)]
                .AsText);
        Assert.Equal(
            "2026-07-23 15:09:10.1234567+00:00",
            values[JsonMigrationObjectIds.Column(5)]
                .AsText);
        Assert.Equal(
            "18446744073709551615",
            values[JsonMigrationObjectIds.Column(6)]
                .AsText);
        Assert.Equal(
            "accepted",
            values[JsonMigrationObjectIds.Column(7)]
                .AsText);
        MigrationRejectedRow rejected =
            Assert.Single(batch.RejectedRows);
        Assert.Equal(1, rejected.SourceRowOrdinal);
        Assert.Equal(
            JsonMigrationDataRules.TypedValueInvalid,
            rejected.RuleId);
        Assert.Equal(
            JsonMigrationObjectIds.Column(1),
            rejected.ColumnObjectId);
    }

    [Theory]
    [InlineData(JsonInputFraming.RootArray)]
    [InlineData(JsonInputFraming.MultipleValues)]
    public async Task IndependentReopensPreserveTypedCatalogCursorsAndSuffixes(
        JsonInputFraming framing)
    {
        using var workspace = new TemporaryDirectory();
        await using TypedFixture origin =
            await TypedFixture.CreateAsync(
                workspace,
                "cursor",
                framing,
                Frame(
                    framing,
                    """{"value":"1"}""",
                    """{"value":"2"}""",
                    """{"value":"3"}""",
                    """{"value":"4"}""",
                    """{"value":"5"}"""),
                OneIntentOptions());
        string packagePath = workspace.PathFor(
            "cursor" +
            JsonTypedSnapshotPackage.FileExtension);
        JsonTypedSnapshotPackageManifest manifest =
            await JsonTypedSnapshotPackage.WriteAsync(
                packagePath,
                origin.Snapshot,
                origin.Schema,
                TargetVersion,
                Cancellation);
        await origin.DisposeAsync();
        File.Delete(origin.SidecarPath);
        File.Delete(origin.SourcePath);

        List<MigrationDataBatch> expected;
        string resumeCursor;
        await using (JsonTypedSnapshotPackageSession first =
            await OpenAsync(
                packagePath,
                workspace.Root,
                manifest.ManifestDigest))
        {
            MigrationReadRequest request = Request(
                first.DataSource,
                [JsonMigrationObjectIds.Column(0)],
                batchSize: 2);
            expected = await CollectAsync(
                first.DataSource.ReadAsync(
                    request,
                    Cancellation));
            Assert.Equal(
                [2, 2, 1],
                expected.Select(batch => batch.Rows.Count));
            resumeCursor = Assert.IsType<string>(
                expected[0].NextCursor);
            Assert.StartsWith(
                JsonMigrationDataSource.TypedCursorAlgorithmId +
                "/",
                resumeCursor,
                StringComparison.Ordinal);
        }

        Assert.Empty(Directory.EnumerateDirectories(workspace.Root));
        await using (JsonTypedSnapshotPackageSession second =
            await OpenAsync(
                packagePath,
                workspace.Root,
                manifest.ManifestDigest))
        {
            Assert.Equal(
                manifest.CatalogDigest,
                MigrationArtifactSerializer.ComputeCatalogDigest(
                    second.Catalog));
            Assert.Equal(
                manifest.IntentManifestDigest,
                second.IntentManifest.ManifestDigest);
            MigrationReadRequest request = Request(
                second.DataSource,
                [JsonMigrationObjectIds.Column(0)],
                batchSize: 2);
            List<MigrationDataBatch> replay =
                await CollectAsync(
                    second.DataSource.ReadAsync(
                        request,
                        Cancellation));
            AssertBatchSequenceEqual(expected, replay);
            List<MigrationDataBatch> resumed =
                await CollectAsync(
                    second.DataSource.ReadAsync(
                        request with
                        {
                            ResumeCursor = resumeCursor,
                        },
                        Cancellation));
            AssertBatchSequenceEqual(
                expected.Skip(1).ToArray(),
                resumed);
        }
    }

    [Fact]
    public async Task SessionsOwnPrivateCopiesAfterPackageRemoval()
    {
        using var workspace = new TemporaryDirectory();
        await using TypedFixture origin =
            await TypedFixture.CreateAsync(
                workspace,
                "ownership",
                JsonInputFraming.RootArray,
                """
                [
                  {"value":"1"},
                  {"value":"2"},
                  {"value":"3"}
                ]
                """,
                OneIntentOptions());
        string packagePath = workspace.PathFor(
            "ownership" +
            JsonTypedSnapshotPackage.FileExtension);
        JsonTypedSnapshotPackageManifest manifest =
            await JsonTypedSnapshotPackage.WriteAsync(
                packagePath,
                origin.Snapshot,
                origin.Schema,
                TargetVersion,
                Cancellation);
        await origin.DisposeAsync();
        File.Delete(origin.SourcePath);
        File.Delete(origin.SidecarPath);

        Task<JsonTypedSnapshotPackageSession> firstOpen =
            OpenAsync(
                    packagePath,
                    workspace.Root,
                    manifest.ManifestDigest)
                .AsTask();
        Task<JsonTypedSnapshotPackageSession> secondOpen =
            OpenAsync(
                    packagePath,
                    workspace.Root,
                    manifest.ManifestDigest)
                .AsTask();
        JsonTypedSnapshotPackageSession[] sessions =
            await Task.WhenAll(firstOpen, secondOpen);
        JsonTypedSnapshotPackageSession first = sessions[0];
        JsonTypedSnapshotPackageSession second = sessions[1];
        bool firstDisposed = false;
        bool secondDisposed = false;
        try
        {
            Assert.Equal(
                2,
                Directory.EnumerateDirectories(
                    workspace.Root).Count());
            File.Delete(packagePath);

            List<MigrationDataBatch>[] reads =
                await Task.WhenAll(
                    ReadAllAsync(first.DataSource),
                    ReadAllAsync(second.DataSource));
            AssertBatchSequenceEqual(reads[0], reads[1]);

            await first.DisposeAsync();
            firstDisposed = true;
            Assert.Single(
                Directory.EnumerateDirectories(
                    workspace.Root));

            List<MigrationDataBatch> replay =
                await ReadAllAsync(second.DataSource);
            AssertBatchSequenceEqual(reads[1], replay);

            await second.DisposeAsync();
            secondDisposed = true;
            Assert.Empty(
                Directory.EnumerateDirectories(
                    workspace.Root));
        }
        finally
        {
            if (!firstDisposed)
                await first.DisposeAsync();
            if (!secondDisposed)
                await second.DisposeAsync();
        }
    }

    [Fact]
    public async Task PackageAndCursorV1AndV2AreMutuallyIsolated()
    {
        using var workspace = new TemporaryDirectory();
        await using TypedFixture origin =
            await TypedFixture.CreateAsync(
                workspace,
                "isolation",
                JsonInputFraming.RootArray,
                """
                [
                  {"value":"1"},
                  {"value":"2"},
                  {"value":"3"}
                ]
                """,
                OneIntentOptions());
        string v1Path = workspace.PathFor(
            "ordinary" + JsonSnapshotPackage.FileExtension);
        string v2Path = workspace.PathFor(
            "typed" +
            JsonTypedSnapshotPackage.FileExtension);
        JsonTableSchemaInferenceResult ordinarySchema =
            await JsonTableSchemaInferer.InferAsync(
                origin.Binding,
                origin.Snapshot,
                maxProfileRecords: 100,
                cancellationToken: Cancellation);
        JsonSnapshotPackageManifest v1Manifest =
            await JsonSnapshotPackage.WriteAsync(
                v1Path,
                origin.Snapshot,
                ordinarySchema,
                TargetVersion,
                Cancellation);
        JsonTypedSnapshotPackageManifest v2Manifest =
            await JsonTypedSnapshotPackage.WriteAsync(
                v2Path,
                origin.Snapshot,
                origin.Schema,
                TargetVersion,
                Cancellation);
        await origin.DisposeAsync();

        Assert.Equal(
            "CSDBJSN1"u8.ToArray(),
            (await File.ReadAllBytesAsync(
                v1Path,
                Cancellation))[..8]);
        Assert.Equal(
            "CSDBJSN2"u8.ToArray(),
            (await File.ReadAllBytesAsync(
                v2Path,
                Cancellation))[..8]);
        JsonSnapshotPackageException typedAsV1 =
            await Assert.ThrowsAsync<JsonSnapshotPackageException>(
                async () => await JsonSnapshotPackage.OpenAsync(
                    v2Path,
                    cancellationToken: Cancellation));
        Assert.Equal(
            JsonSnapshotPackageRules.InvalidFormat,
            typedAsV1.RuleId);
        JsonSnapshotPackageException ordinaryAsV2 =
            await Assert.ThrowsAsync<JsonSnapshotPackageException>(
                async () =>
                    await JsonTypedSnapshotPackage.OpenAsync(
                        v1Path,
                        cancellationToken: Cancellation));
        Assert.Equal(
            JsonSnapshotPackageRules.InvalidFormat,
            ordinaryAsV2.RuleId);

        await using JsonSnapshotPackageSession v1 =
            await JsonSnapshotPackage.OpenAsync(
                v1Path,
                new JsonSnapshotPackageOpenOptions
                {
                    WorkspacePath = workspace.Root,
                    ExpectedManifestDigest =
                        v1Manifest.ManifestDigest,
                    MaxSourceBytes = 1024 * 1024,
                },
                Cancellation);
        await using JsonTypedSnapshotPackageSession v2 =
            await OpenAsync(
                v2Path,
                workspace.Root,
                v2Manifest.ManifestDigest);
        MigrationReadRequest v1Request = Request(
            v1.DataSource,
            [JsonMigrationObjectIds.Column(0)],
            batchSize: 1);
        MigrationReadRequest v2Request = Request(
            v2.DataSource,
            [JsonMigrationObjectIds.Column(0)],
            batchSize: 1);
        string v1Cursor = Assert.IsType<string>(
            (await CollectAsync(
                v1.DataSource.ReadAsync(
                    v1Request,
                    Cancellation)))[0].NextCursor);
        string v2Cursor = Assert.IsType<string>(
            (await CollectAsync(
                v2.DataSource.ReadAsync(
                    v2Request,
                    Cancellation)))[0].NextCursor);
        Assert.StartsWith(
            JsonMigrationDataSource.CursorAlgorithmId + "/",
            v1Cursor,
            StringComparison.Ordinal);
        Assert.StartsWith(
            JsonMigrationDataSource.TypedCursorAlgorithmId + "/",
            v2Cursor,
            StringComparison.Ordinal);
        await Assert.ThrowsAsync<InvalidDataException>(
            async () => await CollectAsync(
                v1.DataSource.ReadAsync(
                    v1Request with
                    {
                        ResumeCursor = v2Cursor,
                    },
                    Cancellation)));
        await Assert.ThrowsAsync<InvalidDataException>(
            async () => await CollectAsync(
                v2.DataSource.ReadAsync(
                    v2Request with
                    {
                        ResumeCursor = v1Cursor,
                    },
                    Cancellation)));
    }

    private static readonly UTF8Encoding StrictUtf8 =
        new(false, true);

    private static JsonTypedColumnIntent Intent(
        int columnIndex,
        string expectedPropertyName,
        JsonTypedValueCodec codec,
        bool? nullable = null,
        int? precision = null,
        int? scale = null) =>
        new()
        {
            ColumnIndex = columnIndex,
            ExpectedPropertyName = expectedPropertyName,
            Codec = codec,
            Nullable = nullable,
            Precision = precision,
            Scale = scale,
        };

    private static JsonTypedIntentOptions OneIntentOptions() =>
        new()
        {
            Columns =
            [
                Intent(
                    0,
                    "value",
                    JsonTypedValueCodec.Int64String,
                    nullable: false),
            ],
        };

    private static async ValueTask<
        JsonTypedSnapshotPackageSession> OpenAsync(
            string path,
            string workspacePath,
            string? expectedManifestDigest = null) =>
        await JsonTypedSnapshotPackage.OpenAsync(
            path,
            new JsonSnapshotPackageOpenOptions
            {
                WorkspacePath = workspacePath,
                MaxSourceBytes = 1024 * 1024,
                ExpectedManifestDigest =
                    expectedManifestDigest,
            },
            Cancellation);

    private static MigrationReadRequest Request(
        JsonMigrationDataSource source,
        IReadOnlyList<string> columns,
        int batchSize) =>
        new()
        {
            SourceObjectId = JsonMigrationObjectIds.Table,
            ColumnObjectIds = columns,
            BatchSize = batchSize,
            MaxBatchBytes = 1024 * 1024,
            MaxValueBytes = 256 * 1024,
            SnapshotToken = source.SnapshotIdentity,
        };

    private static async Task<List<MigrationDataBatch>>
        ReadAllAsync(JsonMigrationDataSource source) =>
        await CollectAsync(
            source.ReadAsync(
                Request(
                    source,
                    [JsonMigrationObjectIds.Column(0)],
                    batchSize: 2),
                Cancellation));

    private static async Task AssertTypedRowsAsync(
        JsonMigrationDataSource source)
    {
        List<MigrationDataBatch> batches = await CollectAsync(
            source.ReadAsync(
                Request(
                    source,
                    [
                        JsonMigrationObjectIds.Column(0),
                        JsonMigrationObjectIds.Column(1),
                        JsonMigrationObjectIds.Column(2),
                        JsonMigrationObjectIds.Column(3),
                    ],
                    batchSize: 1),
                Cancellation));
        MigrationDataRow[] rows = batches
            .SelectMany(batch => batch.Rows)
            .ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal(
            [1, 2, 3, 4],
            rows[0].Values[0].BinaryValue.ToArray());
        Assert.Equal(
            "12345678901234567890.123456789012345678",
            rows[0].Values[1].CanonicalText);
        Assert.Equal(
            "9223372036854775807",
            rows[0].Values[2].CanonicalText);
        Assert.Equal(
            "alpha",
            rows[0].Values[3].CanonicalText);
        Assert.Equal(
            [5, 6],
            rows[1].Values[0].BinaryValue.ToArray());
        Assert.Equal(
            "0.000000000000000001",
            rows[1].Values[1].CanonicalText);
        Assert.Equal(
            "-9223372036854775808",
            rows[1].Values[2].CanonicalText);
        Assert.Equal(
            "bravo",
            rows[1].Values[3].CanonicalText);
    }

    private static MigrationPlan ReadyRejectPlan(
        MigrationCatalog catalog,
        int batchSize)
    {
        MigrationPlan draft =
            new MigrationPlanner().CreatePlan(catalog);
        return draft with
        {
            AcceptedExclusionObjectIds = draft.Objects
                .Where(item => !item.Included)
                .Select(item => item.SourceObjectId)
                .OrderBy(
                    item => item,
                    StringComparer.Ordinal)
                .ToArray(),
            Load = draft.Load with
            {
                BatchSize = batchSize,
                RejectMode =
                    MigrationRejectMode
                        .DeterministicRejects,
                RejectPolicy =
                    new MigrationDeterministicRejectPolicy
                    {
                        ContractVersion =
                            MigrationRejectContract
                                .DeterministicRejectsV1,
                        AllowedRuleIds =
                        [
                            JsonMigrationDataRules
                                .TypedValueInvalid,
                        ],
                        MaxRejectedRowsPerBatch =
                            batchSize,
                        MaxRejectedRowsPerRun = 1000,
                        MaxRawValueBytes = 4096,
                        MaxRawValueBytesPerBatch =
                            64 * 1024,
                        MaxRawValueBytesPerRun =
                            1024 * 1024,
                        MaxArtifactBytes =
                            16 * 1024 * 1024,
                    },
            },
        };
    }

    private static void AssertManifestEquivalent(
        JsonTypedSnapshotPackageManifest expected,
        JsonTypedSnapshotPackageManifest actual)
    {
        Assert.Equal(
            expected.ManifestDigest,
            actual.ManifestDigest);
        Assert.Equal(
            expected.IntentManifestDigest,
            actual.IntentManifestDigest);
        Assert.Equal(
            expected.SnapshotIdentity,
            actual.SnapshotIdentity);
        Assert.Equal(
            expected.ContentDigest,
            actual.ContentDigest);
        Assert.Equal(
            expected.ContentLength,
            actual.ContentLength);
        Assert.Equal(expected.Source, actual.Source);
        Assert.Equal(
            expected.OptionsDigest,
            actual.OptionsDigest);
        Assert.Equal(
            expected.TargetCSharpDbVersion,
            actual.TargetCSharpDbVersion);
        Assert.Equal(
            expected.CatalogDigest,
            actual.CatalogDigest);
    }

    private static void AssertBatchSequenceEqual(
        IReadOnlyList<MigrationDataBatch> expected,
        IReadOnlyList<MigrationDataBatch> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        for (int index = 0; index < expected.Count; index++)
        {
            MigrationDataBatch left = expected[index];
            MigrationDataBatch right = actual[index];
            Assert.Equal(left.SourceObjectId, right.SourceObjectId);
            Assert.Equal(
                left.SnapshotIdentity,
                right.SnapshotIdentity);
            Assert.Equal(
                left.ColumnObjectIds,
                right.ColumnObjectIds);
            Assert.Equal(left.BatchOrdinal, right.BatchOrdinal);
            Assert.Equal(left.StartCursor, right.StartCursor);
            Assert.Equal(left.NextCursor, right.NextCursor);
            Assert.Equal(left.Rows.Count, right.Rows.Count);
            for (int rowIndex = 0;
                 rowIndex < left.Rows.Count;
                 rowIndex++)
            {
                MigrationDataRow leftRow =
                    left.Rows[rowIndex];
                MigrationDataRow rightRow =
                    right.Rows[rowIndex];
                Assert.Equal(
                    leftRow.StableKey,
                    rightRow.StableKey);
                Assert.Equal(
                    leftRow.Values.Count,
                    rightRow.Values.Count);
                for (int valueIndex = 0;
                     valueIndex < leftRow.Values.Count;
                     valueIndex++)
                {
                    MigrationSourceValue leftValue =
                        leftRow.Values[valueIndex];
                    MigrationSourceValue rightValue =
                        rightRow.Values[valueIndex];
                    Assert.Equal(
                        leftValue.Kind,
                        rightValue.Kind);
                    Assert.Equal(
                        leftValue.CanonicalText,
                        rightValue.CanonicalText);
                    Assert.Equal(
                        leftValue.BinaryValue.ToArray(),
                        rightValue.BinaryValue.ToArray());
                }
            }
            Assert.Equal(
                left.RejectedRows.Select(row =>
                    (
                        row.SourceRowOrdinal,
                        row.RuleId,
                        row.ColumnObjectId)),
                right.RejectedRows.Select(row =>
                    (
                        row.SourceRowOrdinal,
                        row.RuleId,
                        row.ColumnObjectId)));
        }
    }

    private static async Task<List<T>> CollectAsync<T>(
        IAsyncEnumerable<T> source)
    {
        var result = new List<T>();
        await foreach (T item in source.WithCancellation(
                           Cancellation))
        {
            result.Add(item);
        }

        return result;
    }

    private static string Frame(
        JsonInputFraming framing,
        params string[] values) =>
        framing switch
        {
            JsonInputFraming.RootArray =>
                "[\n" +
                string.Join(",\n", values) +
                "\n]",
            JsonInputFraming.MultipleValues =>
                string.Join("\n", values) + "\n",
            _ => throw new ArgumentOutOfRangeException(
                nameof(framing)),
        };

    private sealed class TypedFixture : IAsyncDisposable
    {
        private bool disposed;

        private TypedFixture(
            string sourcePath,
            string sidecarPath,
            JsonSourceSnapshot snapshot,
            JsonSourceBinding binding,
            JsonTypedIntentManifest intent,
            JsonTypedTableSchemaInferenceResult schema)
        {
            SourcePath = sourcePath;
            SidecarPath = sidecarPath;
            Snapshot = snapshot;
            Binding = binding;
            Intent = intent;
            Schema = schema;
        }

        internal string SourcePath { get; }

        internal string SidecarPath { get; }

        internal JsonSourceSnapshot Snapshot { get; }

        internal JsonSourceBinding Binding { get; }

        internal JsonTypedIntentManifest Intent { get; }

        internal JsonTypedTableSchemaInferenceResult Schema
        {
            get;
        }

        internal static async Task<TypedFixture> CreateAsync(
            TemporaryDirectory workspace,
            string name,
            JsonInputFraming framing,
            string json,
            JsonTypedIntentOptions intentOptions,
            JsonTableSchemaInferenceOptions?
                inferenceOptions = null)
        {
            string sourcePath =
                workspace.PathFor(name + ".json");
            string sidecarPath =
                workspace.PathFor(
                    name +
                    JsonTypedIntentSidecar.FileExtension);
            await File.WriteAllTextAsync(
                sourcePath,
                json,
                StrictUtf8,
                Cancellation);
            JsonSourceSnapshot? snapshot = null;
            try
            {
                snapshot =
                    await JsonSourceSnapshot.CreateFromFileAsync(
                        sourcePath,
                        new JsonSourceSnapshotOptions
                        {
                            WorkspacePath = workspace.Root,
                            MaxSourceBytes = 1024 * 1024,
                        },
                        Cancellation);
                JsonSourceBinding binding =
                    await JsonSourceBinding.CreateAsync(
                        snapshot,
                        new JsonStreamingReaderOptions
                        {
                            Framing = framing,
                            MaxValueBytes = 256 * 1024,
                            MaxDepth = 32,
                            MaxPropertiesPerObject = 256,
                            MaxArrayElements = 1024,
                            MaxTotalNodes = 2048,
                            MaxPropertyNameBytes = 8 * 1024,
                            MaxStringBytes = 128 * 1024,
                            MaxNumberBytes = 8 * 1024,
                            LeaveOpen = true,
                        },
                        "typed-package/" + name,
                        Cancellation);
                JsonTypedIntentManifest intent =
                    await JsonTypedIntentSidecar.WriteAsync(
                        sidecarPath,
                        binding,
                        intentOptions,
                        Cancellation);
                JsonTypedTableSchemaInferenceResult schema =
                    await JsonTypedTableSchemaInferer.InferAsync(
                        binding,
                        snapshot,
                        intent,
                        maxProfileRecords: 100,
                        inferenceOptions,
                        Cancellation);
                return new TypedFixture(
                    sourcePath,
                    sidecarPath,
                    snapshot,
                    binding,
                    intent,
                    schema);
            }
            catch
            {
                if (snapshot is not null)
                    await snapshot.DisposeAsync();
                throw;
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (disposed)
                return;
            disposed = true;
            await Snapshot.DisposeAsync();
        }
    }

    private sealed class TemporaryDirectory : IDisposable
    {
        internal TemporaryDirectory()
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                "csharpdb-json-typed-package-tests-" +
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
