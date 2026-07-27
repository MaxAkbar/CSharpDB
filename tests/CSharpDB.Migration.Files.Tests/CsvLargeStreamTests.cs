using System.Globalization;
using System.Text;
using CSharpDB.Migration;
using CSharpDB.Migration.Files.Csv;

namespace CSharpDB.Migration.Files.Tests;

public sealed class CsvLargeStreamTests
{
    private const int RowCount = 50_000;
    private const int BatchSize = 128;
    private const long MaxBatchBytes = 15 * 512;
    private const int MaxValueBytes = 128;
    private const long MaxSourceBytes = 16L * 1024 * 1024;

    [Fact]
    public async Task RetainedPackageStreamsLargeSourceWithinFixedBoundsAndCleansWorkspaces()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        using var temporary = new TemporaryDirectory();
        string sourcePath = Path.Combine(temporary.Root, "large.csv");
        string packagePath = Path.Combine(temporary.Root, "large.csdbcsv");
        string workspacePath = Path.Combine(temporary.Root, "workspace");
        Directory.CreateDirectory(workspacePath);
        await WriteSourceAsync(sourcePath, cancellationToken);
        long sourceLength = new FileInfo(sourcePath).Length;

        CsvSnapshotPackageManifest manifest;
        await using (CsvSourceSnapshot snapshot = await CsvSourceSnapshot.CreateFromFileAsync(
            sourcePath,
            new CsvSourceSnapshotOptions
            {
                WorkspacePath = workspacePath,
                MaxSourceBytes = MaxSourceBytes,
            },
            cancellationToken))
        {
            Assert.Single(Directory.EnumerateDirectories(workspacePath));
            CsvFormatInspection inspection = await CsvFormatInspector.InspectAsync(
                snapshot,
                new CsvReaderOptions(),
                new CsvInspectionOptions { DelimiterCandidates = [","] },
                cancellationToken);
            CsvSourceBinding binding = await CsvSourceBinding.CreateAsync(
                snapshot,
                inspection,
                "large-stream/rows-50000",
                cancellationToken);
            CsvSchemaInferenceResult schema = await CsvSchemaInferer.InferAsync(
                binding,
                snapshot,
                maxDataRecords: 512,
                cancellationToken: cancellationToken);

            Assert.Equal(MigrationCoverageKind.Sample, schema.Coverage.Kind);
            Assert.Equal(512, schema.RecordsExamined);
            manifest = await CsvSnapshotPackage.WriteAsync(
                packagePath,
                snapshot,
                schema,
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                cancellationToken);
        }

        Assert.Equal(sourceLength, manifest.ContentLength);
        Assert.True(File.Exists(packagePath));
        AssertOwnedArtifactsCleaned(temporary.Root, workspacePath);
        File.Delete(sourcePath);
        Assert.False(File.Exists(sourcePath));

        await using (CsvSnapshotPackageSession session = await CsvSnapshotPackage.OpenAsync(
            packagePath,
            new CsvSnapshotPackageOpenOptions
            {
                WorkspacePath = workspacePath,
                MaxSourceBytes = MaxSourceBytes,
                ExpectedManifestDigest = manifest.ManifestDigest,
            },
            cancellationToken))
        {
            Assert.Single(Directory.EnumerateDirectories(workspacePath));
            Assert.Equal(manifest.ManifestDigest, session.Manifest.ManifestDigest);
            string[] projection =
            [
                CsvMigrationObjectIds.Column(2),
                CsvMigrationObjectIds.Column(0),
                CsvMigrationObjectIds.Column(1),
                CsvMigrationObjectIds.Column(3),
            ];
            var request = new MigrationReadRequest
            {
                SourceObjectId = CsvMigrationObjectIds.Table,
                ColumnObjectIds = projection,
                BatchSize = BatchSize,
                MaxBatchBytes = MaxBatchBytes,
                MaxValueBytes = MaxValueBytes,
                SnapshotToken = session.DataSource.SnapshotIdentity,
            };

            long expectedRow = 0;
            long expectedBatchOrdinal = 0;
            string? previousNextCursor = null;
            int previousRowCount = 0;
            long previousBatchBytes = 0;
            bool sawRowBound = false;
            bool sawByteBound = false;
            await foreach (MigrationDataBatch batch in session.DataSource
                               .ReadAsync(request, cancellationToken)
                               .WithCancellation(cancellationToken))
            {
                Assert.Equal(CsvMigrationObjectIds.Table, batch.SourceObjectId);
                Assert.Equal(session.DataSource.SnapshotIdentity, batch.SnapshotIdentity);
                Assert.Equal(projection, batch.ColumnObjectIds);
                Assert.Equal(expectedBatchOrdinal, batch.BatchOrdinal);
                Assert.NotEmpty(batch.Rows);
                Assert.InRange(batch.Rows.Count, 1, BatchSize);
                if (expectedBatchOrdinal == 0)
                {
                    Assert.Null(batch.StartCursor);
                }
                else
                {
                    Assert.NotNull(previousNextCursor);
                    Assert.Equal(previousNextCursor, batch.StartCursor);
                }

                long batchBytes = 0;
                long firstRowBytes = 0;
                for (int rowIndex = 0; rowIndex < batch.Rows.Count; rowIndex++)
                {
                    MigrationDataRow row = batch.Rows[rowIndex];
                    Assert.Null(row.StableKey);
                    Assert.Equal(4, row.Values.Count);
                    string ordinal = expectedRow.ToString(CultureInfo.InvariantCulture);
                    AssertValue(
                        row.Values[0],
                        MigrationSourceValueKind.Text,
                        $"note {ordinal} \"quoted\"\nline {ordinal}");
                    AssertValue(row.Values[1], MigrationSourceValueKind.SignedInteger, ordinal);
                    AssertValue(
                        row.Values[2],
                        MigrationSourceValueKind.Text,
                        $"row-{expectedRow:D5}");
                    AssertValue(row.Values[3], MigrationSourceValueKind.Text, string.Empty);

                    long rowBytes = 0;
                    foreach (MigrationSourceValue value in row.Values)
                    {
                        long valueBytes = CanonicalValueBytes(value);
                        Assert.InRange(valueBytes, 1, MaxValueBytes);
                        rowBytes = checked(rowBytes + CanonicalBatchBytes(value, valueBytes));
                    }

                    if (rowIndex == 0)
                        firstRowBytes = rowBytes;
                    batchBytes = checked(batchBytes + rowBytes);
                    expectedRow++;
                }

                Assert.InRange(batchBytes, 1, MaxBatchBytes);
                if (expectedBatchOrdinal > 0)
                {
                    bool rowBound = previousRowCount == BatchSize;
                    bool byteBound = previousBatchBytes + firstRowBytes > MaxBatchBytes;
                    Assert.True(rowBound || byteBound);
                    sawRowBound |= rowBound;
                    sawByteBound |= byteBound;
                }

                previousRowCount = batch.Rows.Count;
                previousBatchBytes = batchBytes;
                previousNextCursor = batch.NextCursor;
                expectedBatchOrdinal++;
            }

            Assert.Equal(RowCount, expectedRow);
            Assert.True(expectedBatchOrdinal > 0);
            Assert.True(sawRowBound);
            Assert.True(sawByteBound);
            Assert.Null(previousNextCursor);
        }

        Assert.True(File.Exists(packagePath));
        AssertOwnedArtifactsCleaned(temporary.Root, workspacePath);
    }

    private static async ValueTask WriteSourceAsync(
        string sourcePath,
        CancellationToken cancellationToken)
    {
        await using var stream = new FileStream(
            sourcePath,
            FileMode.CreateNew,
            FileAccess.Write,
            FileShare.None,
            128 * 1024,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        await using var writer = new StreamWriter(
            stream,
            new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true),
            128 * 1024,
            leaveOpen: false)
        {
            NewLine = "\n",
        };
        await writer.WriteAsync("id,name,note,tail\n".AsMemory(), cancellationToken);
        for (int row = 0; row < RowCount; row++)
        {
            string record = string.Create(
                CultureInfo.InvariantCulture,
                $"{row},row-{row:D5},\"note {row} \"\"quoted\"\"\nline {row}\",\n");
            await writer.WriteAsync(record.AsMemory(), cancellationToken);
        }

        await writer.FlushAsync(cancellationToken);
    }

    private static void AssertValue(
        MigrationSourceValue value,
        MigrationSourceValueKind expectedKind,
        string expectedText)
    {
        Assert.Equal(expectedKind, value.Kind);
        Assert.Equal(expectedText, value.CanonicalText);
        Assert.True(value.BinaryValue.IsEmpty);
    }

    private static long CanonicalValueBytes(MigrationSourceValue value)
    {
        if (value.Kind == MigrationSourceValueKind.Null)
            return 1;
        return checked(5L + Encoding.UTF8.GetByteCount(Assert.IsType<string>(value.CanonicalText)));
    }

    private static long CanonicalBatchBytes(MigrationSourceValue value, long valueBytes) =>
        value.Kind switch
        {
            MigrationSourceValueKind.Boolean or
            MigrationSourceValueKind.SignedInteger or
            MigrationSourceValueKind.UnsignedInteger or
            MigrationSourceValueKind.Decimal or
            MigrationSourceValueKind.FloatingPoint => Math.Max(9L, valueBytes),
            _ => valueBytes,
        };

    private static void AssertOwnedArtifactsCleaned(string root, string workspacePath)
    {
        Assert.Empty(Directory.EnumerateFileSystemEntries(workspacePath));
        Assert.Empty(Directory.GetFiles(root, ".csdbcsv-*.tmp", SearchOption.TopDirectoryOnly));
    }

    private sealed class TemporaryDirectory : IDisposable
    {
        internal TemporaryDirectory()
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                $"csharpdb-csv-large-stream-tests-{Guid.NewGuid():N}");
            Directory.CreateDirectory(Root);
        }

        internal string Root { get; }

        public void Dispose()
        {
            if (Directory.Exists(Root))
                Directory.Delete(Root, recursive: true);
        }
    }
}
