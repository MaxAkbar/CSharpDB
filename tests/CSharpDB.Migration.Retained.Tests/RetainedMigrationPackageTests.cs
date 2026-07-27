using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Migration;
using CSharpDB.Migration.Retained;

namespace CSharpDB.Migration.Retained.Tests;

public sealed class RetainedMigrationPackageTests
{
    private static CancellationToken Ct =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task
        CaptureFactoryRoundTripsEveryScalarKindAndBindsContent()
    {
        using var temporary =
            new RetainedTestDirectory();
        string packagePath =
            temporary.PathFor(
                "all-values.csdbsqlserver");
        string[] columns =
            Enumerable.Range(0, 15)
                .Select(index =>
                    $"column:{index:D2}")
                .ToArray();
        MigrationSourceValue[] expected =
        [
            Value(MigrationSourceValueKind.Null),
            Value(MigrationSourceValueKind.Boolean, "true"),
            Value(MigrationSourceValueKind.SignedInteger, "-42"),
            Value(MigrationSourceValueKind.UnsignedInteger, "42"),
            Value(MigrationSourceValueKind.Decimal, "12.340"),
            Value(MigrationSourceValueKind.FloatingPoint, "1.25"),
            Value(MigrationSourceValueKind.Text, "left\0雪😀"),
            Binary(0x00, 0xff, 0x2a),
            Value(MigrationSourceValueKind.Guid, "00112233-4455-6677-8899-aabbccddeeff"),
            Value(MigrationSourceValueKind.Date, "2026-07-25"),
            Value(MigrationSourceValueKind.Time, "12:34:56.789"),
            Value(MigrationSourceValueKind.DateTime, "2026-07-25T12:34:56.789"),
            Value(MigrationSourceValueKind.DateTimeOffset, "2026-07-25T12:34:56.789-07:00"),
            Value(MigrationSourceValueKind.Json, "{\"a\":1}"),
            Value(MigrationSourceValueKind.Native, "geography:POINT(1 2)"),
        ];

        RetainedMigrationPackageWriteResult result =
            await RetainedMigrationPackageWriter.WriteAsync(
                new RetainedMigrationPackageCaptureRequest
                {
                    OutputPath = packagePath,
                    Tables =
                    [
                        Table(
                            "table:all",
                            columns,
                            [columns[0]],
                            Rows(
                                new MigrationDataRow
                                {
                                    StableKey = "pk:1",
                                    Values = expected,
                                })),
                    ],
                    CatalogFactory =
                        (summary, _) =>
                        {
                            Assert.Equal(
                                RetainedMigrationPackageContract
                                    .ContentDigestAlgorithm,
                                summary.DigestAlgorithm);
                            Assert.Single(summary.Tables);
                            return ValueTask.FromResult(
                                new RetainedMigrationCatalogBinding
                                {
                                    Catalog = Catalog(
                                        "table:all",
                                        columns,
                                        summary.ContentDigest),
                                    SnapshotIdentity =
                                        "sqlserver-snapshot:" +
                                        summary.ContentDigest,
                                });
                        },
                },
                Ct);

        Assert.Equal(
            RetainedMigrationPackageContract.Format,
            result.Manifest.Format);
        Assert.Equal(
            result.ContentSummary.ContentDigest,
            result.Manifest.ContentDigest);
        Assert.Equal(
            1,
            result.RowCounts["table:all"]);

        await using RetainedMigrationPackageSession
            session =
            await OpenAsync(
                packagePath,
                result.PackageDigest,
                temporary.Root);
        Assert.Equal(
            result.ContentSummary.ContentDigest,
            session.Catalog.Source.Fingerprint);
        Assert.Equal(
            result.Manifest.SnapshotIdentity,
            session.DataSource.SnapshotIdentity);
        Assert.Equal(
            result.Manifest.CatalogDigest,
            session.DataSource.CatalogDigest);

        List<MigrationDataBatch> batches =
            await ReadAsync(
                session.DataSource,
                Request(
                    "table:all",
                    columns,
                    session.Manifest.SnapshotIdentity,
                    batchSize: 10));
        MigrationDataRow row =
            Assert.Single(
                Assert.Single(batches).Rows);
        Assert.Equal("pk:1", row.StableKey);
        Assert.Equal(expected.Length, row.Values.Count);
        for (int index = 0;
             index < expected.Length;
             index++)
        {
            AssertValue(
                expected[index],
                row.Values[index]);
        }
    }

    [Fact]
    public async Task
        RequestedOrderBatchingAndResumeAreDeterministic()
    {
        using var temporary =
            new RetainedTestDirectory();
        string packagePath =
            temporary.PathFor("rows.csdbsqlserver");
        string[] columns =
            ["column:id", "column:name"];
        RetainedMigrationPackageWriteResult result =
            await WriteAsync(
                packagePath,
                "table:people",
                columns,
                Enumerable.Range(1, 5)
                    .Select(index =>
                        new MigrationDataRow
                        {
                            StableKey =
                                $"id:{index}",
                            Values =
                            [
                                Value(
                                    MigrationSourceValueKind
                                        .SignedInteger,
                                    index.ToString(
                                        System.Globalization
                                            .CultureInfo
                                            .InvariantCulture)),
                                Value(
                                    MigrationSourceValueKind
                                        .Text,
                                    $"name-{index}"),
                            ],
                        })
                    .ToArray());
        await using RetainedMigrationPackageSession
            session =
            await OpenAsync(
                packagePath,
                result.PackageDigest,
                temporary.Root);
        string[] requested =
            [columns[1], columns[0]];
        MigrationReadRequest request =
            Request(
                "table:people",
                requested,
                session.Manifest.SnapshotIdentity,
                batchSize: 2);

        List<MigrationDataBatch> batches =
            await ReadAsync(
                session.DataSource,
                request);
        Assert.Equal(
            [2, 2, 1],
            batches.Select(batch =>
                    batch.Rows.Count)
                .ToArray());
        Assert.Equal(
            [0L, 1L, 2L],
            batches.Select(batch =>
                    batch.BatchOrdinal)
                .ToArray());
        Assert.Equal(
            requested,
            batches[0].ColumnObjectIds);
        Assert.Equal(
            "name-1",
            batches[0].Rows[0]
                .Values[0].CanonicalText);
        Assert.Equal(
            "1",
            batches[0].Rows[0]
                .Values[1].CanonicalText);
        Assert.Null(batches[^1].NextCursor);

        string cursor =
            Assert.IsType<string>(
                batches[0].NextCursor);
        List<MigrationDataBatch> resumed =
            await ReadAsync(
                session.DataSource,
                request with
                {
                    ResumeCursor = cursor,
                });
        Assert.Equal(
            [1L, 2L],
            resumed.Select(batch =>
                    batch.BatchOrdinal)
                .ToArray());
        Assert.Equal(
            ["id:3", "id:4", "id:5"],
                resumed.SelectMany(batch =>
                    batch.Rows)
                .Select(row =>
                    row.StableKey ??
                    string.Empty)
                .ToArray());
        Assert.Equal(
            cursor,
            resumed[0].StartCursor);
    }

    [Fact]
    public async Task
        ResumeCursorRejectsTamperedStaleAndNonBoundaryPositions()
    {
        using var temporary =
            new RetainedTestDirectory();
        string packagePath =
            temporary.PathFor("cursor.csdbsqlserver");
        string[] columns =
            ["column:id", "column:name"];
        RetainedMigrationPackageWriteResult result =
            await WriteAsync(
                packagePath,
                "table:people",
                columns,
                Enumerable.Range(1, 5)
                    .Select(index =>
                        Row(index))
                    .ToArray());
        await using RetainedMigrationPackageSession
            session =
            await OpenAsync(
                packagePath,
                result.PackageDigest,
                temporary.Root);
        MigrationReadRequest request =
            Request(
                "table:people",
                columns,
                session.Manifest.SnapshotIdentity,
                batchSize: 2);
        List<MigrationDataBatch> batches =
            await ReadAsync(
                session.DataSource,
                request);
        string cursor =
            Assert.IsType<string>(
                batches[0].NextCursor);

        char replacement =
            cursor[^1] == '0'
                ? '1'
                : '0';
        string tampered =
            cursor[..^1] + replacement;
        await Assert.ThrowsAsync<
            InvalidDataException>(
            () => ReadAsync(
                session.DataSource,
                request with
                {
                    ResumeCursor = tampered,
                }));

        await Assert.ThrowsAsync<
            InvalidDataException>(
            () => ReadAsync(
                session.DataSource,
                request with
                {
                    BatchSize = 3,
                    ResumeCursor = cursor,
                }));

        string scope =
            RetainedMigrationCursorCodec
                .ComputeScope(
                    result.PackageDigest,
                    session.Manifest.CatalogDigest,
                    session.Catalog.Source.Identity,
                    session.Catalog.Source.Fingerprint,
                    session.Manifest.SnapshotIdentity,
                    request.SourceObjectId,
                    request.ColumnObjectIds,
                    request.BatchSize,
                    request.MaxBatchBytes,
                    request.MaxValueBytes,
                    request.RejectContractVersion);
        RetainedMigrationCursorCodec.Position
            position =
            RetainedMigrationCursorCodec.Parse(
                cursor,
                scope);
        string nonBoundary =
            RetainedMigrationCursorCodec.Encode(
                position.RowOrdinal,
                position.RelativeOffset + 1,
                position.BatchOrdinal,
                scope);
        await Assert.ThrowsAsync<
            InvalidDataException>(
            () => ReadAsync(
                session.DataSource,
                request with
                {
                    ResumeCursor =
                        nonBoundary,
                }));
    }

    [Fact]
    public async Task
        OpenRejectsAFieldBeyondTheDeclaredRowBeforeAllocatingIt()
    {
        using var temporary =
            new RetainedTestDirectory();
        string packagePath =
            temporary.PathFor(
                "oversized-field.package");
        RetainedMigrationPackageWriteResult result =
            await WriteAsync(
                packagePath,
                "table:people",
                ["column:id", "column:name"],
                [Row(1)]);
        byte[] package =
            await File.ReadAllBytesAsync(
                packagePath,
                Ct);
        int rowOffset =
            FindAscii(package, "ROW1");
        Assert.True(rowOffset >= 0);
        int payloadOffset =
            checked(
                rowOffset +
                RetainedMigrationBinaryCodec
                    .RowHeaderBytes);
        Assert.Equal(
            1,
            package[payloadOffset]);
        int stableKeyLength =
            BinaryPrimitives
                .ReadInt32BigEndian(
                    package.AsSpan(
                        payloadOffset + 1,
                        sizeof(int)));
        int firstValueLengthOffset =
            checked(
                payloadOffset +
                1 +
                sizeof(int) +
                stableKeyLength +
                sizeof(int) +
                1);
        BinaryPrimitives.WriteInt32BigEndian(
            package.AsSpan(
                firstValueLengthOffset,
                sizeof(int)),
            64 * 1024 * 1024);
        await File.WriteAllBytesAsync(
            packagePath,
            package,
            Ct);

        RetainedMigrationPackageException failure =
            await Assert.ThrowsAsync<
                RetainedMigrationPackageException>(
                async () =>
                {
                    await using
                        RetainedMigrationPackageSession
                            _ =
                            await OpenAsync(
                                packagePath,
                                Digest(package),
                                temporary.Root);
                });

        Assert.Contains(
            "declared payload",
            failure.Message,
            StringComparison.OrdinalIgnoreCase);
        Assert.Empty(
            Directory.EnumerateDirectories(
                temporary.Root));
        Assert.NotEqual(
            result.PackageDigest,
            Digest(package));
    }

    [Fact]
    public void
        ManifestLimitIsEnforcedWhileTheManifestIsBuilt()
    {
        MigrationCatalog catalog =
            Catalog(
                "table:people",
                ["column:id"],
                FixedFingerprint);

        RetainedMigrationPackageLimitException
            failure =
            Assert.Throws<
                RetainedMigrationPackageLimitException>(
                () =>
                    RetainedMigrationBinaryCodec
                        .BuildManifest(
                            catalog,
                            new string(
                                'x',
                                1024 * 1024),
                            new string('a', 64),
                            FixedSnapshot,
                            FixedFingerprint,
                            [],
                            maximumBytes: 256));

        Assert.Contains(
            "manifest",
            failure.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task
        IdenticalInputsProduceIdenticalPackages()
    {
        using var temporary =
            new RetainedTestDirectory();
        string[] columns =
            ["column:id", "column:name"];
        MigrationDataRow[] rows =
            Enumerable.Range(1, 3)
                .Select(Row)
                .ToArray();
        string firstPath =
            temporary.PathFor("first.package");
        string secondPath =
            temporary.PathFor("second.package");

        RetainedMigrationPackageWriteResult first =
            await WriteAsync(
                firstPath,
                "table:people",
                columns,
                rows);
        RetainedMigrationPackageWriteResult second =
            await WriteAsync(
                secondPath,
                "table:people",
                columns,
                rows);

        Assert.Equal(
            first.PackageDigest,
            second.PackageDigest);
        Assert.Equal(
            first.ContentSummary.ContentDigest,
            second.ContentSummary.ContentDigest);
        Assert.Equal(
            await File.ReadAllBytesAsync(
                firstPath,
                Ct),
            await File.ReadAllBytesAsync(
                secondPath,
                Ct));
    }

    [Fact]
    public async Task
        WholeDigestCatalogDigestAndTableSectionTamperAreRejected()
    {
        using var temporary =
            new RetainedTestDirectory();
        string[] columns =
            ["column:id", "column:name"];

        string mismatchPath =
            temporary.PathFor("mismatch.package");
        RetainedMigrationPackageWriteResult mismatch =
            await WriteAsync(
                mismatchPath,
                "table:people",
                columns,
                [Row(1)]);
        RetainedMigrationPackageException
            mismatchFailure =
            await Assert.ThrowsAsync<
                RetainedMigrationPackageException>(
                async () =>
                {
                    await using
                        RetainedMigrationPackageSession
                            _ =
                            await OpenAsync(
                                mismatchPath,
                                "sha256:" +
                                new string('0', 64),
                                temporary.Root);
                });
        Assert.Contains(
            "whole-package",
            mismatchFailure.Message,
            StringComparison.OrdinalIgnoreCase);

        string catalogPath =
            temporary.PathFor("catalog.package");
        RetainedMigrationPackageWriteResult catalog =
            await WriteAsync(
                catalogPath,
                "table:people",
                columns,
                [Row(1)]);
        byte[] catalogBytes =
            await File.ReadAllBytesAsync(
                catalogPath,
                Ct);
        int catalogDigestOffset =
            FindAscii(
                catalogBytes,
                catalog.Manifest.CatalogDigest);
        Assert.True(catalogDigestOffset >= 0);
        int catalogHexOffset =
            catalogDigestOffset;
        catalogBytes[catalogHexOffset] =
            catalogBytes[catalogHexOffset] ==
                (byte)'0'
                ? (byte)'1'
                : (byte)'0';
        await File.WriteAllBytesAsync(
            catalogPath,
            catalogBytes,
            Ct);
        string changedCatalogDigest =
            Digest(catalogBytes);
        RetainedMigrationPackageException
            catalogFailure =
            await Assert.ThrowsAsync<
                RetainedMigrationPackageException>(
                async () =>
                {
                    await using
                        RetainedMigrationPackageSession
                            _ =
                            await OpenAsync(
                                catalogPath,
                                changedCatalogDigest,
                                temporary.Root);
                });
        Assert.Contains(
            "catalog digest",
            catalogFailure.Message,
            StringComparison.OrdinalIgnoreCase);

        string rowPath =
            temporary.PathFor("row.package");
        _ = await WriteAsync(
            rowPath,
            "table:people",
            columns,
            [Row(1)]);
        byte[] rowBytes =
            await File.ReadAllBytesAsync(
                rowPath,
                Ct);
        rowBytes[^1] ^= 0x01;
        await File.WriteAllBytesAsync(
            rowPath,
            rowBytes,
            Ct);
        RetainedMigrationPackageException rowFailure =
            await Assert.ThrowsAsync<
                RetainedMigrationPackageException>(
                async () =>
                {
                    await using
                        RetainedMigrationPackageSession
                            _ =
                            await OpenAsync(
                                rowPath,
                                Digest(rowBytes),
                                temporary.Root);
                });
        Assert.Contains(
            "section digest",
            rowFailure.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task
        WriterBoundsFailAtomicallyWithoutPublishingOutput()
    {
        using var temporary =
            new RetainedTestDirectory();
        string packagePath =
            temporary.PathFor("bounded.package");
        string[] columns = ["column:id"];
        await Assert.ThrowsAsync<
            RetainedMigrationPackageLimitException>(
            async () =>
            {
                _ = await RetainedMigrationPackageWriter
                    .WriteAsync(
                        new RetainedMigrationPackageWriteRequest
                        {
                            OutputPath =
                                packagePath,
                            Catalog = Catalog(
                                "table:bounded",
                                columns,
                                FixedFingerprint),
                            SnapshotIdentity =
                                FixedSnapshot,
                            Tables =
                            [
                                Table(
                                    "table:bounded",
                                    columns,
                                    columns,
                                    Rows(
                                        new MigrationDataRow
                                        {
                                            StableKey = "1",
                                            Values =
                                            [
                                                Binary(
                                                    0x01,
                                                    0x02,
                                                    0x03),
                                            ],
                                        })),
                            ],
                            Options = new()
                            {
                                MaxValueBytes = 2,
                                MaxRowBytes = 128,
                            },
                        },
                        Ct);
            });
        Assert.False(
            File.Exists(packagePath));
        Assert.Empty(
            Directory.EnumerateFiles(
                temporary.Root,
                "*.tmp"));
    }

    [Fact]
    public async Task
        WriterReportsPlaintextCleanupFailureBeforePublishing()
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var temporary =
            new RetainedTestDirectory();
        string packagePath =
            temporary.PathFor(
                "cleanup-failure.package");
        FileStream? bodyLease = null;
        try
        {
            AggregateException failure =
                await Assert.ThrowsAsync<
                    AggregateException>(
                    async () =>
                    {
                        _ = await
                            RetainedMigrationPackageWriter
                                .WriteAsync(
                                    new RetainedMigrationPackageCaptureRequest
                                    {
                                        OutputPath =
                                            packagePath,
                                        Tables =
                                        [
                                            Table(
                                                "table:people",
                                                ["column:id"],
                                                ["column:id"],
                                                Rows(
                                                    new MigrationDataRow
                                                    {
                                                        StableKey =
                                                            "id:1",
                                                        Values =
                                                        [
                                                            Value(
                                                                MigrationSourceValueKind
                                                                    .SignedInteger,
                                                                "1"),
                                                        ],
                                                    })),
                                        ],
                                        CatalogFactory =
                                            (summary, _) =>
                                            {
                                                string bodyPath =
                                                    Assert.Single(
                                                        Directory
                                                            .EnumerateFiles(
                                                                temporary.Root,
                                                                "*.rows.tmp"));
                                                bodyLease =
                                                    new FileStream(
                                                        bodyPath,
                                                        FileMode.Open,
                                                        FileAccess.Read,
                                                        FileShare.Read);
                                                return ValueTask
                                                    .FromResult(
                                                        new RetainedMigrationCatalogBinding
                                                        {
                                                            Catalog =
                                                                Catalog(
                                                                    "table:people",
                                                                    ["column:id"],
                                                                    summary
                                                                        .ContentDigest),
                                                            SnapshotIdentity =
                                                                FixedSnapshot,
                                                        });
                                            },
                                    },
                                    Ct);
                    });

            Assert.Contains(
                failure.Flatten()
                    .InnerExceptions,
                exception =>
                    exception.Message.Contains(
                        "plaintext",
                        StringComparison
                            .OrdinalIgnoreCase));
            Assert.False(
                File.Exists(packagePath));
            Assert.Single(
                Directory.EnumerateFiles(
                    temporary.Root,
                    "*.rows.tmp"));
        }
        finally
        {
            if (bodyLease is not null)
            {
                await bodyLease.DisposeAsync();
            }
        }
    }

    [Fact]
    public async Task
        SessionCleanupRefusesUnexpectedWorkspaceEntries()
    {
        using var temporary =
            new RetainedTestDirectory();
        string packagePath =
            temporary.PathFor(
                "cleanup-path.package");
        RetainedMigrationPackageWriteResult result =
            await WriteAsync(
                packagePath,
                "table:people",
                ["column:id", "column:name"],
                [Row(1)]);
        RetainedMigrationPackageSession session =
            await OpenAsync(
                packagePath,
                result.PackageDigest,
                temporary.Root);
        string workspace =
            Assert.Single(
                Directory.EnumerateDirectories(
                    temporary.Root,
                    "csharpdb-retained-*"));
        string unexpectedPath =
            Path.Combine(
                workspace,
                "do-not-delete.txt");
        await File.WriteAllTextAsync(
            unexpectedPath,
            "sentinel",
            Ct);

        RetainedMigrationPackageException failure =
            await Assert.ThrowsAsync<
                RetainedMigrationPackageException>(
                () => session.DisposeAsync()
                    .AsTask());

        Assert.Contains(
            "workspace",
            failure.Message,
            StringComparison.OrdinalIgnoreCase);
        Assert.True(
            File.Exists(unexpectedPath));
    }

    [Fact]
    public async Task
        OpenRejectsManifestBeforeHostileAllocation()
    {
        using var temporary =
            new RetainedTestDirectory();
        string packagePath =
            temporary.PathFor("manifest.package");
        RetainedMigrationPackageWriteResult result =
            await WriteAsync(
                packagePath,
                "table:people",
                ["column:id", "column:name"],
                [Row(1)]);

        RetainedMigrationPackageLimitException failure =
            await Assert.ThrowsAsync<
                RetainedMigrationPackageLimitException>(
                async () =>
                {
                    await using
                        RetainedMigrationPackageSession
                            _ =
                            await RetainedMigrationPackageSession
                                .OpenAsync(
                                    packagePath,
                                    new RetainedMigrationPackageOpenOptions
                                    {
                                        ExpectedPackageDigest =
                                            result.PackageDigest,
                                        WorkspacePath =
                                            temporary.Root,
                                        MaxCatalogBytes = 64,
                                        MaxManifestBytes = 64,
                                    },
                                    Ct);
                });
        Assert.Contains(
            "manifest",
            failure.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    private static async ValueTask<
        RetainedMigrationPackageWriteResult>
        WriteAsync(
        string packagePath,
        string tableObjectId,
        string[] columns,
        MigrationDataRow[] rows) =>
        await RetainedMigrationPackageWriter
            .WriteAsync(
                new RetainedMigrationPackageWriteRequest
                {
                    OutputPath = packagePath,
                    Catalog = Catalog(
                        tableObjectId,
                        columns,
                        FixedFingerprint),
                    SnapshotIdentity =
                        FixedSnapshot,
                    Tables =
                    [
                        Table(
                            tableObjectId,
                            columns,
                            [columns[0]],
                            Rows(rows)),
                    ],
                },
                Ct);

    private static RetainedMigrationTableWrite Table(
        string tableObjectId,
        IReadOnlyList<string> columns,
        IReadOnlyList<string> orderingKeys,
        IAsyncEnumerable<MigrationDataRow> rows) =>
        new()
        {
            Descriptor =
                new RetainedMigrationTableDescriptor
                {
                    SourceObjectId =
                        tableObjectId,
                    ColumnObjectIds =
                        columns,
                    OrderingKeyColumnObjectIds =
                        orderingKeys,
                },
            Rows = rows,
        };

    private static async IAsyncEnumerable<
        MigrationDataRow> Rows(
        params MigrationDataRow[] rows)
    {
        await Task.Yield();
        foreach (MigrationDataRow row in rows)
            yield return row;
    }

    private static MigrationCatalog Catalog(
        string tableObjectId,
        IReadOnlyList<string> columns,
        string fingerprint)
    {
        var objects =
            new List<MigrationCatalogObject>
            {
                new()
                {
                    ObjectId = "database:test",
                    Kind =
                        MigrationObjectKind.Database,
                    SourceName = "test",
                },
                new()
                {
                    ObjectId = "namespace:dbo",
                    Kind =
                        MigrationObjectKind.Namespace,
                    ParentObjectId =
                        "database:test",
                    SourceName = "dbo",
                },
                new()
                {
                    ObjectId = tableObjectId,
                    Kind =
                        MigrationObjectKind.Table,
                    ParentObjectId =
                        "namespace:dbo",
                    SourceNamespace = "dbo",
                    SourceName = "items",
                },
            };
        for (int index = 0;
             index < columns.Count;
             index++)
        {
            objects.Add(
                new MigrationCatalogObject
                {
                    ObjectId = columns[index],
                    Kind =
                        MigrationObjectKind.Column,
                    ParentObjectId =
                        tableObjectId,
                    SourceNamespace = "dbo",
                    SourceName =
                        $"value_{index:D2}",
                    NativeType = "nvarchar",
                });
        }

        return new MigrationCatalog
        {
            TargetCSharpDbVersion = "4.3.0",
            Source =
                new MigrationSourceIdentity
                {
                    Kind =
                        MigrationSourceKind.SqlServer,
                    Identity =
                        "sqlserver:test-endpoint:test-database",
                    Fingerprint =
                        fingerprint,
                    ProviderVersion = "test",
                    SourceVersion = "test",
                    Consistency =
                        new MigrationConsistencyStrategy
                        {
                            Kind =
                                MigrationConsistencyKind
                                    .Snapshot,
                            Description =
                                "Retained test snapshot.",
                        },
                },
            Objects = objects,
        };
    }

    private static MigrationDataRow Row(
        int index) =>
        new()
        {
            StableKey = $"id:{index}",
            Values =
            [
                Value(
                    MigrationSourceValueKind
                        .SignedInteger,
                    index.ToString(
                        System.Globalization
                            .CultureInfo.InvariantCulture)),
                Value(
                    MigrationSourceValueKind.Text,
                    $"name-{index}"),
            ],
        };

    private static MigrationSourceValue Value(
        MigrationSourceValueKind kind,
        string? text = null) =>
        new()
        {
            Kind = kind,
            CanonicalText = text,
        };

    private static MigrationSourceValue Binary(
        params byte[] bytes) =>
        new()
        {
            Kind =
                MigrationSourceValueKind.Binary,
            BinaryValue = bytes,
        };

    private static void AssertValue(
        MigrationSourceValue expected,
        MigrationSourceValue actual)
    {
        Assert.Equal(
            expected.Kind,
            actual.Kind);
        Assert.Equal(
            expected.CanonicalText,
            actual.CanonicalText);
        Assert.Equal(
            expected.BinaryValue.ToArray(),
            actual.BinaryValue.ToArray());
    }

    private static MigrationReadRequest Request(
        string tableObjectId,
        IReadOnlyList<string> columns,
        string snapshotIdentity,
        int batchSize) =>
        new()
        {
            SourceObjectId = tableObjectId,
            ColumnObjectIds = columns,
            BatchSize = batchSize,
            MaxBatchBytes = 1024 * 1024,
            MaxValueBytes = 512 * 1024,
            SnapshotToken = snapshotIdentity,
        };

    private static async Task<
        List<MigrationDataBatch>> ReadAsync(
        IMigrationDataSource dataSource,
        MigrationReadRequest request)
    {
        var batches =
            new List<MigrationDataBatch>();
        await foreach (
            MigrationDataBatch batch in
            dataSource.ReadAsync(
                request,
                Ct))
        {
            batches.Add(batch);
        }
        return batches;
    }

    private static ValueTask<
        RetainedMigrationPackageSession> OpenAsync(
        string packagePath,
        string packageDigest,
        string workspacePath) =>
        RetainedMigrationPackageSession.OpenAsync(
            packagePath,
            new RetainedMigrationPackageOpenOptions
            {
                ExpectedPackageDigest =
                    packageDigest,
                WorkspacePath = workspacePath,
            },
            Ct);

    private static string Digest(
        byte[] bytes) =>
        "sha256:" +
        Convert.ToHexString(
                SHA256.HashData(bytes))
            .ToLowerInvariant();

    private static int FindAscii(
        byte[] bytes,
        string text)
    {
        byte[] needle =
            Encoding.ASCII.GetBytes(text);
        return bytes.AsSpan().IndexOf(needle);
    }

    private const string FixedFingerprint =
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    private const string FixedSnapshot =
        "sqlserver-snapshot:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    private sealed class RetainedTestDirectory :
        IDisposable
    {
        public RetainedTestDirectory()
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                "csharpdb-retained-tests-" +
                Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(Root);
        }

        public string Root { get; }

        public string PathFor(string name) =>
            Path.Combine(Root, name);

        public void Dispose()
        {
            if (!Directory.Exists(Root))
                return;
            foreach (string directory in
                     Directory.EnumerateDirectories(
                         Root))
            {
                foreach (string file in
                         Directory.EnumerateFiles(
                             directory))
                {
                    File.Delete(file);
                }
                Directory.Delete(
                    directory,
                    recursive: false);
            }
            foreach (string file in
                     Directory.EnumerateFiles(Root))
            {
                File.Delete(file);
            }
            Directory.Delete(
                Root,
                recursive: false);
        }
    }
}
