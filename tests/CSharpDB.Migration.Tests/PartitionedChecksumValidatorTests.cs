using System.Runtime.CompilerServices;
using CSharpDB.Migration.Canonicalization;
using CSharpDB.Migration.Validation;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Tests;

public sealed class PartitionedChecksumValidatorTests
{
    [Fact]
    public async Task UnkeyedValidationIsOrderIndependentAndPreservesDuplicates()
    {
        string root = CreateRoot();
        try
        {
            CanonicalRowContract contract = UnkeyedContract();
            MigrationValidationRow[] source =
            [
                Row(DbValue.FromInteger(3)),
                Row(DbValue.FromInteger(1)),
                Row(DbValue.FromInteger(2)),
                Row(DbValue.FromInteger(1)),
            ];
            MigrationValidationRow[] target =
            [
                Row(DbValue.FromInteger(1)),
                Row(DbValue.FromInteger(2)),
                Row(DbValue.FromInteger(1)),
                Row(DbValue.FromInteger(3)),
            ];

            PartitionedChecksumValidationResult result = await new PartitionedChecksumValidator().ValidateAsync(
                contract,
                Rows(source),
                Rows(target),
                Options(root),
                TestContext.Current.CancellationToken);

            Assert.Equal(MigrationValidationStatus.Passed, result.Status);
            Assert.Equal(4, result.SourceRowCount);
            Assert.Equal(result.SourceChecksum, result.TargetChecksum);
            Assert.Equal(256, result.Partitions.Count);
            Assert.All(result.Partitions, partition => Assert.Equal(MigrationValidationStatus.Passed, partition.Status));
            Assert.True(result.PeakSpillBytes > 0);
            Assert.Empty(Directory.GetDirectories(root));
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task UnkeyedValidationReportsMultiplicityDifferenceWithoutRawValues()
    {
        string root = CreateRoot();
        try
        {
            PartitionedChecksumValidationResult result = await new PartitionedChecksumValidator().ValidateAsync(
                UnkeyedContract(),
                Rows([Row(DbValue.FromInteger(7)), Row(DbValue.FromInteger(7)), Row(DbValue.FromInteger(7))]),
                Rows([Row(DbValue.FromInteger(7)), Row(DbValue.FromInteger(7))]),
                Options(root),
                TestContext.Current.CancellationToken);

            Assert.Equal(MigrationValidationStatus.Different, result.Status);
            MigrationValidationPartitionEvidence different = Assert.Single(
                result.Partitions,
                item => item.Status == MigrationValidationStatus.Different);
            MigrationValidationMismatchEvidence mismatch = Assert.Single(different.Mismatches);
            Assert.Equal(MigrationValidationMismatchKind.SourceOnly, mismatch.Kind);
            Assert.Null(mismatch.KeyHash);
            Assert.NotNull(mismatch.SourceRowHash);
            Assert.Equal(64, mismatch.SourceRowHash!.Length);
            Assert.Equal(1, mismatch.SourceMultiplicity);
            Assert.Equal(0, mismatch.TargetMultiplicity);
            Assert.Empty(Directory.GetDirectories(root));
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task KeyedValidationLocalizesChangedRowByHashedKey()
    {
        string root = CreateRoot();
        try
        {
            CanonicalRowContract contract = KeyedContract();
            PartitionedChecksumValidationResult result = await new PartitionedChecksumValidator().ValidateAsync(
                contract,
                Rows([Row(DbValue.FromInteger(1), DbValue.FromText("before"))]),
                Rows([Row(DbValue.FromInteger(1), DbValue.FromText("after"))]),
                Options(root),
                TestContext.Current.CancellationToken);

            MigrationValidationPartitionEvidence partition = Assert.Single(
                result.Partitions,
                item => item.Status == MigrationValidationStatus.Different);
            MigrationValidationMismatchEvidence mismatch = Assert.Single(partition.Mismatches);
            Assert.Equal(MigrationValidationMismatchKind.Changed, mismatch.Kind);
            Assert.Equal(64, mismatch.KeyHash!.Length);
            Assert.Equal(64, mismatch.SourceRowHash!.Length);
            Assert.Equal(64, mismatch.TargetRowHash!.Length);
            Assert.NotEqual(mismatch.SourceRowHash, mismatch.TargetRowHash);
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task DuplicateCanonicalPrimaryKeyIsRejectedAndWorkspaceIsCleaned()
    {
        string root = CreateRoot();
        try
        {
            InvalidDataException error = await Assert.ThrowsAsync<InvalidDataException>(
                async () => await new PartitionedChecksumValidator().ValidateAsync(
                    KeyedContract(),
                    Rows(
                    [
                        Row(DbValue.FromInteger(1), DbValue.FromText("same")),
                        Row(DbValue.FromInteger(1), DbValue.FromText("same")),
                    ]),
                    Rows([Row(DbValue.FromInteger(1), DbValue.FromText("same"))]),
                    Options(root),
                    TestContext.Current.CancellationToken));

            Assert.Contains("primary key", error.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Empty(Directory.GetDirectories(root));
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task SpillLimitFailureAndCancellationBothCleanWorkspace()
    {
        string root = CreateRoot();
        try
        {
            PartitionedChecksumValidatorOptions tiny = Options(root) with { MaxSpillBytes = 100 };
            await Assert.ThrowsAsync<IOException>(
                async () => await new PartitionedChecksumValidator().ValidateAsync(
                    UnkeyedContract(),
                    Rows([Row(DbValue.FromInteger(1)), Row(DbValue.FromInteger(2))]),
                    Rows([]),
                    tiny,
                    TestContext.Current.CancellationToken));
            Assert.Empty(Directory.GetDirectories(root));

            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(
                TestContext.Current.CancellationToken);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await new PartitionedChecksumValidator().ValidateAsync(
                    UnkeyedContract(),
                    CancelingRows(cancellation, cancellation.Token),
                    Rows([]),
                    Options(root),
                    cancellation.Token));
            Assert.Empty(Directory.GetDirectories(root));
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    [Fact]
    public async Task LargeStreamingValidationRemainsBoundedAndOrderIndependent()
    {
        const int rowCount = 50_000;
        string root = CreateRoot();
        try
        {
            PartitionedChecksumValidatorOptions options = Options(root) with
            {
                SortMemoryBudgetBytes = 64 * 1024,
                MaxSpillBytes = 64 * 1024 * 1024,
                MergeFanIn = 4,
                MaxOpenFiles = 5,
                // The fixed per-partition buffers keep this practical even
                // with a deliberately small file-handle cap.
                MaxOpenPartitionWriters = 8,
            };

            PartitionedChecksumValidationResult result = await new PartitionedChecksumValidator()
                .ValidateAsync(
                    UnkeyedContract(),
                    GeneratedRows(rowCount, reverse: false, TestContext.Current.CancellationToken),
                    GeneratedRows(rowCount, reverse: true, TestContext.Current.CancellationToken),
                    options,
                    TestContext.Current.CancellationToken);

            Assert.Equal(MigrationValidationStatus.Passed, result.Status);
            Assert.Equal(rowCount, result.SourceRowCount);
            Assert.Equal(rowCount, result.TargetRowCount);
            Assert.Equal(result.SourceChecksum, result.TargetChecksum);
            Assert.InRange(result.PeakSpillBytes, 1, options.MaxSpillBytes);
            Assert.Empty(Directory.GetDirectories(root));
        }
        finally
        {
            DeleteRoot(root);
        }
    }

    private static CanonicalRowContract UnkeyedContract() => new()
    {
        SourceObjectId = "table:values",
        TargetObjectId = "values",
        ObjectContractDigest = new string('1', 64),
        Fields =
        [
            new CanonicalFieldContract
            {
                SourceColumnObjectId = "column:value",
                TargetColumnName = "value",
                StoredType = DbType.Integer,
                CanonicalType = CanonicalType.Int64,
            },
        ],
    };

    private static CanonicalRowContract KeyedContract() => new()
    {
        SourceObjectId = "table:keyed",
        TargetObjectId = "keyed",
        ObjectContractDigest = new string('2', 64),
        KeyFieldOrdinals = [0],
        Fields =
        [
            new CanonicalFieldContract
            {
                SourceColumnObjectId = "column:id",
                TargetColumnName = "id",
                StoredType = DbType.Integer,
                CanonicalType = CanonicalType.Int64,
            },
            new CanonicalFieldContract
            {
                SourceColumnObjectId = "column:value",
                TargetColumnName = "value",
                StoredType = DbType.Text,
                CanonicalType = CanonicalType.Text,
            },
        ],
    };

    private static MigrationValidationRow Row(params DbValue[] values) => new() { Values = values };

    private static PartitionedChecksumValidatorOptions Options(string root) => new()
    {
        SpillRootDirectory = root,
        SortMemoryBudgetBytes = ValidationHashRecord.SerializedLength * 2,
        MaxSpillBytes = 16 * 1024 * 1024,
        MergeFanIn = 2,
        MaxOpenFiles = 3,
        MaxOpenPartitionWriters = 2,
        MaxMismatchDetailsPerPartition = 10,
    };

    private static async IAsyncEnumerable<MigrationValidationRow> Rows(
        IEnumerable<MigrationValidationRow> rows)
    {
        foreach (MigrationValidationRow row in rows)
        {
            yield return row;
            await Task.Yield();
        }
    }

    private static async IAsyncEnumerable<MigrationValidationRow> CancelingRows(
        CancellationTokenSource cancellation,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        yield return Row(DbValue.FromInteger(1));
        cancellation.Cancel();
        await Task.Yield();
        cancellationToken.ThrowIfCancellationRequested();
        yield return Row(DbValue.FromInteger(2));
    }

    private static async IAsyncEnumerable<MigrationValidationRow> GeneratedRows(
        int count,
        bool reverse,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await Task.CompletedTask;
        for (int index = 0; index < count; index++)
        {
            if ((index & 1023) == 0)
                cancellationToken.ThrowIfCancellationRequested();
            int value = reverse ? count - index - 1 : index;
            yield return Row(DbValue.FromInteger(value % 10_000));
        }
    }

    private static string CreateRoot()
    {
        string path = Path.Combine(Path.GetTempPath(), $"csharpdb-checksum-tests-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteRoot(string root)
    {
        if (Directory.Exists(root))
            Directory.Delete(root, recursive: true);
    }
}
