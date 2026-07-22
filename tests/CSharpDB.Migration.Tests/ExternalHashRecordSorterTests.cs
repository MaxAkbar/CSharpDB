using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using CSharpDB.Migration.Validation;

namespace CSharpDB.Migration.Tests;

public sealed class ExternalHashRecordSorterTests
{
    [Fact]
    public void ValidationHashRecord_ValidatesWidthAndUsesUnsignedBytewiseOrder()
    {
        byte[] lower = new byte[ValidationHashRecord.HashLength];
        byte[] higher = new byte[ValidationHashRecord.HashLength];
        lower[0] = 0x7F;
        higher[0] = 0x80;

        var lowerRecord = new ValidationHashRecord(lower, new byte[ValidationHashRecord.HashLength]);
        var higherRecord = new ValidationHashRecord(higher, new byte[ValidationHashRecord.HashLength]);

        Assert.True(lowerRecord.CompareTo(higherRecord) < 0);
        Assert.Equal(ValidationHashRecord.SerializedLength, lowerRecord.ToArray().Length);
        Assert.Throws<ArgumentException>(() => new ValidationHashRecord(new byte[31], new byte[32]));
        Assert.Throws<ArgumentException>(() => ValidationHashRecord.FromBytes(new byte[65]));
    }

    [Fact]
    public async Task SortAsync_UsesMultipleBoundedMergePassesAndPreservesDuplicates()
    {
        string root = CreateTemporaryRoot();
        try
        {
            await using var workspace = new ValidationSpillWorkspace(root);
            var sorter = new ExternalHashRecordSorter(
                workspace,
                new ExternalHashRecordSorterOptions
                {
                    // Two sort buffers fit exactly one fixed-width record.
                    MemoryBudgetBytes = ValidationHashRecord.SerializedLength * 2,
                    MergeFanIn = 8,
                    MaxOpenFiles = 3,
                });

            List<byte[]> input =
            [
                Record(0x80, 2),
                Record(0x01, 9),
                Record(0x01, 3),
                Record(0xFF, 0),
                Record(0x00, 8),
                Record(0x7F, 4),
                Record(0x01, 3),
                Record(0x00, 1),
                Record(0x80, 1),
                Record(0x02, 0),
                Record(0x01, 3),
            ];

            ExternalHashRecordSortResult result = await sorter.SortAsync(
                EnumerateAsync(input),
                TestContext.Current.CancellationToken);

            var expected = input.Select(item => (byte[])item.Clone()).ToList();
            expected.Sort(static (left, right) => left.AsSpan().SequenceCompareTo(right));
            List<byte[]> actual = await ReadAllAsync(result);

            Assert.Equal(input.Count, result.RecordCount);
            Assert.Equal(expected.Count, actual.Count);
            for (int index = 0; index < expected.Count; index++)
                Assert.Equal(expected[index], actual[index]);
            Assert.Equal(3, actual.Count(item => item.AsSpan().SequenceEqual(Record(0x01, 3))));

            long expectedFinalBytes = 32 + (input.Count * ValidationHashRecord.SerializedLength);
            Assert.Equal(expectedFinalBytes, workspace.LiveSpillBytes);
            Assert.True(workspace.MaximumSpillBytes > workspace.LiveSpillBytes);
            Assert.Single(Directory.GetFiles(workspace.DirectoryPath, "run-*.bin"));

            byte[] header = new byte[32];
            await using (var stream = File.OpenRead(result.SpillFilePath))
                await stream.ReadExactlyAsync(header, TestContext.Current.CancellationToken);
            Assert.Equal("CSHRSORT"u8.ToArray(), header.AsSpan(0, 8).ToArray());
            Assert.Equal(1U, BinaryPrimitives.ReadUInt32BigEndian(header.AsSpan(8, 4)));
            Assert.Equal(32U, BinaryPrimitives.ReadUInt32BigEndian(header.AsSpan(12, 4)));
            Assert.Equal(64U, BinaryPrimitives.ReadUInt32BigEndian(header.AsSpan(16, 4)));
            Assert.Equal(0U, BinaryPrimitives.ReadUInt32BigEndian(header.AsSpan(20, 4)));
            Assert.Equal((ulong)input.Count, BinaryPrimitives.ReadUInt64BigEndian(header.AsSpan(24, 8)));
        }
        finally
        {
            DeleteTemporaryRoot(root);
        }
    }

    [Theory]
    [InlineData(63)]
    [InlineData(65)]
    public async Task SortAsync_RejectsMalformedRecordsAndCleansCompletedRuns(int malformedLength)
    {
        string root = CreateTemporaryRoot();
        try
        {
            await using var workspace = new ValidationSpillWorkspace(root);
            var sorter = new ExternalHashRecordSorter(
                workspace,
                new ExternalHashRecordSorterOptions
                {
                    MemoryBudgetBytes = ValidationHashRecord.SerializedLength * 2,
                    MergeFanIn = 2,
                    MaxOpenFiles = 3,
                });

            byte[][] input = [Record(1, 1), new byte[malformedLength]];
            InvalidDataException exception = await Assert.ThrowsAsync<InvalidDataException>(
                () => sorter.SortAsync(EnumerateAsync(input), TestContext.Current.CancellationToken));

            Assert.Contains("exactly 64 bytes", exception.Message, StringComparison.Ordinal);
            Assert.Equal(0, workspace.LiveSpillBytes);
            Assert.Empty(Directory.GetFiles(workspace.DirectoryPath, "run-*.bin"));
        }
        finally
        {
            DeleteTemporaryRoot(root);
        }
    }

    [Fact]
    public async Task SortAsync_CancellationCleansSpillFiles()
    {
        string root = CreateTemporaryRoot();
        try
        {
            await using var workspace = new ValidationSpillWorkspace(root);
            var sorter = new ExternalHashRecordSorter(
                workspace,
                new ExternalHashRecordSorterOptions
                {
                    MemoryBudgetBytes = ValidationHashRecord.SerializedLength * 2,
                    MergeFanIn = 2,
                    MaxOpenFiles = 3,
                });
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(
                TestContext.Current.CancellationToken);

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => sorter.SortAsync(
                    EnumerateAndCancelAsync(cancellation, cancellation.Token),
                    cancellation.Token));

            Assert.Equal(0, workspace.LiveSpillBytes);
            Assert.Empty(Directory.GetFiles(workspace.DirectoryPath, "run-*.bin"));
        }
        finally
        {
            DeleteTemporaryRoot(root);
        }
    }

    [Fact]
    public async Task ResultReader_RejectsNonzeroReservedHeaderBytes()
    {
        string root = CreateTemporaryRoot();
        try
        {
            await using var workspace = new ValidationSpillWorkspace(root);
            var sorter = new ExternalHashRecordSorter(workspace);
            ExternalHashRecordSortResult result = await sorter.SortAsync(
                EnumerateAsync([Record(1, 2)]),
                TestContext.Current.CancellationToken);

            await using (var stream = new FileStream(result.SpillFilePath, FileMode.Open, FileAccess.Write))
            {
                stream.Position = 20;
                await stream.WriteAsync(new byte[] { 0, 0, 0, 1 }, TestContext.Current.CancellationToken);
            }

            InvalidDataException exception = await Assert.ThrowsAsync<InvalidDataException>(
                async () => await ReadAllAsync(result));
            Assert.Contains("reserved", exception.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            DeleteTemporaryRoot(root);
        }
    }

    [Fact]
    public async Task Workspace_HardLimitRejectsAndDeletesTheRunThatWouldExceedIt()
    {
        string root = CreateTemporaryRoot();
        try
        {
            // One single-record run occupies 32 header bytes + 64 record bytes.
            await using var workspace = new ValidationSpillWorkspace(
                root,
                maximumLiveSpillBytes: 100);
            var sorter = new ExternalHashRecordSorter(
                workspace,
                new ExternalHashRecordSorterOptions
                {
                    MemoryBudgetBytes = ValidationHashRecord.SerializedLength * 2,
                    MergeFanIn = 2,
                    MaxOpenFiles = 3,
                });

            IOException exception = await Assert.ThrowsAsync<IOException>(
                () => sorter.SortAsync(
                    EnumerateAsync([Record(2, 2), Record(1, 1)]),
                    TestContext.Current.CancellationToken));

            Assert.Contains("spill limit", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(100, workspace.MaximumLiveSpillBytes);
            Assert.Equal(0, workspace.LiveSpillBytes);
            Assert.InRange(workspace.MaximumSpillBytes, 1, 100);
            Assert.Empty(Directory.GetFiles(workspace.DirectoryPath, "run-*.bin"));
        }
        finally
        {
            DeleteTemporaryRoot(root);
        }
    }

    [Fact]
    public async Task SortAsync_EmptyInputProducesAValidEmptyRun()
    {
        string root = CreateTemporaryRoot();
        try
        {
            await using var workspace = new ValidationSpillWorkspace(root);
            var sorter = new ExternalHashRecordSorter(workspace);

            ExternalHashRecordSortResult result = await sorter.SortAsync(
                EnumerateAsync([]),
                TestContext.Current.CancellationToken);

            Assert.Equal(0, result.RecordCount);
            Assert.Empty(await ReadAllAsync(result));
            Assert.Equal(32, workspace.LiveSpillBytes);
            Assert.Single(Directory.GetFiles(workspace.DirectoryPath, "run-*.bin"));
        }
        finally
        {
            DeleteTemporaryRoot(root);
        }
    }

    [Fact]
    public async Task Workspace_RefusesRecursiveCleanupWhenOwnershipMarkerChanges()
    {
        string root = CreateTemporaryRoot();
        try
        {
            var workspace = new ValidationSpillWorkspace(root);
            string workspacePath = workspace.DirectoryPath;
            string ownershipMarker = Assert.Single(Directory.GetFiles(workspacePath, ".*owner"));
            await File.WriteAllTextAsync(
                ownershipMarker,
                "not-the-owner-token",
                TestContext.Current.CancellationToken);

            IOException exception = await Assert.ThrowsAsync<IOException>(
                () => workspace.DisposeAsync().AsTask());

            Assert.Contains("ownership", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.True(Directory.Exists(workspacePath));
        }
        finally
        {
            DeleteTemporaryRoot(root);
        }
    }

    [Fact]
    public async Task Workspace_IsUniqueRejectsNonChildPathsAndDeletesOnlyItsDirectory()
    {
        string root = CreateTemporaryRoot();
        string? firstPath = null;
        string? secondPath = null;
        try
        {
            await using (var first = new ValidationSpillWorkspace(root))
            await using (var second = new ValidationSpillWorkspace(root))
            {
                firstPath = first.DirectoryPath;
                secondPath = second.DirectoryPath;
                Assert.NotEqual(firstPath, secondPath);
                Assert.Equal(first.DirectoryPath, Path.GetDirectoryName(first.GetImmediateChildPath("run.bin")));
                Assert.Throws<ArgumentException>(() => first.GetImmediateChildPath("../outside.bin"));
                Assert.Throws<ArgumentException>(() => first.GetImmediateChildPath(Path.Combine(root, "outside.bin")));
            }

            Assert.False(Directory.Exists(firstPath));
            Assert.False(Directory.Exists(secondPath));
            Assert.True(Directory.Exists(root));
        }
        finally
        {
            DeleteTemporaryRoot(root);
        }
    }

    private static byte[] Record(byte firstByte, byte finalByte)
    {
        var record = new byte[ValidationHashRecord.SerializedLength];
        record[0] = firstByte;
        record[^1] = finalByte;
        return record;
    }

    private static async IAsyncEnumerable<ReadOnlyMemory<byte>> EnumerateAsync(
        IEnumerable<byte[]> records)
    {
        foreach (byte[] record in records)
        {
            yield return record;
            await Task.Yield();
        }
    }

    private static async IAsyncEnumerable<ReadOnlyMemory<byte>> EnumerateAndCancelAsync(
        CancellationTokenSource cancellation,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        yield return Record(2, 2);
        cancellation.Cancel();
        await Task.Yield();
        cancellationToken.ThrowIfCancellationRequested();
        yield return Record(1, 1);
    }

    private static async Task<List<byte[]>> ReadAllAsync(ExternalHashRecordSortResult result)
    {
        var records = new List<byte[]>();
        await foreach (ValidationHashRecord record in result.ReadRecordsAsync(
            TestContext.Current.CancellationToken))
        {
            records.Add(record.ToArray());
        }

        return records;
    }

    private static string CreateTemporaryRoot()
    {
        string root = Path.Combine(Path.GetTempPath(), $"csharpdb-validation-sort-{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);
        return root;
    }

    private static void DeleteTemporaryRoot(string root)
    {
        if (Directory.Exists(root))
            Directory.Delete(root, recursive: true);
    }
}
