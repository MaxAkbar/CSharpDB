using CSharpDB.Engine;
using CSharpDB.Storage.Diagnostics;
using CSharpDB.Storage.Diagnostics.Internal;

namespace CSharpDB.Tests;

public sealed class StorageDiagnosticsCancellationTests
{
    [Fact]
    public async Task PublicInspectors_PreCanceledFastPathsThrowCallerToken()
    {
        string missingPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_diag_missing_{Guid.NewGuid():N}.db");
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        OperationCanceledException database = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            _ = await DatabaseInspector.InspectAsync(missingPath, ct: cancellation.Token);
        });
        OperationCanceledException page = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            _ = await DatabaseInspector.InspectPageAsync(missingPath, 0, ct: cancellation.Token);
        });
        OperationCanceledException wal = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            _ = await WalInspector.InspectAsync(missingPath, ct: cancellation.Token);
        });
        OperationCanceledException index = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            _ = await IndexInspector.CheckAsync(missingPath, ct: cancellation.Token);
        });

        Assert.Equal(cancellation.Token, database.CancellationToken);
        Assert.Equal(cancellation.Token, page.CancellationToken);
        Assert.Equal(cancellation.Token, wal.CancellationToken);
        Assert.Equal(cancellation.Token, index.CancellationToken);
    }

    [Fact]
    public void ParsePage_PreCanceledDensePageThrowsCallerToken()
    {
        byte[] page = CreateSingleCellLeafPage();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        OperationCanceledException exception = Assert.ThrowsAny<OperationCanceledException>(() =>
            InspectorEngine.ParsePage(1, page, captureLeafPayload: true, ct: cancellation.Token));

        Assert.Equal(cancellation.Token, exception.CancellationToken);
    }

    [Fact]
    public void IssueCopy_CancellationAfterFirstReadStopsTraversalAndPreservesCallerToken()
    {
        using var cancellation = new CancellationTokenSource();
        IReadOnlyList<IntegrityIssue> source = new CancelAfterFirstReadIssueList(
            [
                new IntegrityIssue
                {
                    Code = "FIRST",
                    Severity = InspectSeverity.Warning,
                    Message = "first",
                },
                new IntegrityIssue
                {
                    Code = "SECOND",
                    Severity = InspectSeverity.Error,
                    Message = "second",
                },
            ],
            cancellation);

        OperationCanceledException exception = Assert.ThrowsAny<OperationCanceledException>(() =>
            InspectorEngine.CopyIssues(source, cancellation.Token));

        Assert.Equal(cancellation.Token, exception.CancellationToken);
    }

    [Fact]
    public void WalkBTree_CancellationAfterFirstLookupStopsTraversal()
    {
        using var cancellation = new CancellationTokenSource();
        var root = new InspectorEngine.ParsedPage
        {
            PageId = 1,
            BaseOffset = 0,
            PageType = PageConstants.PageTypeInterior,
            CellCount = 0,
            CellContentStart = PageConstants.PageSize,
            RightChildOrNextLeaf = 2,
            FreeSpaceBytes = PageConstants.PageSize - PageConstants.SlottedPageHeaderSize,
            CellOffsets = [],
            LeafCells = [],
            InteriorCells = [],
            ChildPageReferences = [2],
        };
        IReadOnlyDictionary<uint, InspectorEngine.ParsedPage> pages =
            new CancelOnFirstLookupDictionary(
                new Dictionary<uint, InspectorEngine.ParsedPage> { [1] = root },
                cancellation);

        OperationCanceledException exception = Assert.ThrowsAny<OperationCanceledException>(() =>
            InspectorEngine.WalkBTree(
                1,
                pages,
                physicalPageCount: 3,
                issues: [],
                scope: "cancellation-test",
                ct: cancellation.Token));

        Assert.Equal(cancellation.Token, exception.CancellationToken);
    }

    [Fact]
    public void OverflowValidation_PreCanceledTraversalThrowsCallerToken()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        OperationCanceledException exception = Assert.ThrowsAny<OperationCanceledException>(() =>
            InspectorEngine.ValidateOverflowReferences(
                new Dictionary<uint, InspectorEngine.ParsedPage>(),
                physicalPageCount: 0,
                issues: [],
                ct: cancellation.Token));

        Assert.Equal(cancellation.Token, exception.CancellationToken);
    }

    [Fact]
    public void HexDumpAndChecksum_PreCanceledThrowCallerToken()
    {
        byte[] bytes = new byte[PageConstants.PageSize];
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        OperationCanceledException hex = Assert.ThrowsAny<OperationCanceledException>(() =>
            InspectorEngine.BuildHexDump(bytes, cancellation.Token));
        OperationCanceledException checksum = Assert.ThrowsAny<OperationCanceledException>(() =>
            InspectorEngine.Checksum(bytes, cancellation.Token));

        Assert.Equal(cancellation.Token, hex.CancellationToken);
        Assert.Equal(cancellation.Token, checksum.CancellationToken);
    }

    [Fact]
    public void WalPendingReset_ReplacesMapWithoutClearingAndHonorsCancellation()
    {
        var pending = new Dictionary<uint, byte[]>
        {
            [7] = [1, 2, 3],
        };

        Dictionary<uint, byte[]> replacement = InspectorEngine.ResetPendingWalPages(
            pending,
            TestContext.Current.CancellationToken);

        Assert.NotSame(pending, replacement);
        Assert.Empty(replacement);
        Assert.Equal(new byte[] { 1, 2, 3 }, Assert.Single(pending).Value);

        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        OperationCanceledException exception = Assert.ThrowsAny<OperationCanceledException>(() =>
            InspectorEngine.ResetPendingWalPages(pending, cancellation.Token));

        Assert.Equal(cancellation.Token, exception.CancellationToken);
        Assert.Equal(new byte[] { 1, 2, 3 }, Assert.Single(pending).Value);
    }

    [Fact]
    public async Task DatabaseInspector_IncludePagesPreservesPageOrderingAndHistogram()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempDbPath();

        try
        {
            await using (var database = await Database.OpenAsync(dbPath, ct))
            {
                await database.ExecuteAsync(
                    "CREATE TABLE ordered_pages (id INTEGER PRIMARY KEY, body TEXT)",
                    ct);
                await database.ExecuteAsync(
                    $"INSERT INTO ordered_pages VALUES (1, '{new string('x', 12_000)}')",
                    ct);
            }

            DatabaseInspectReport report = await DatabaseInspector.InspectAsync(
                dbPath,
                new DatabaseInspectOptions { IncludePages = true },
                ct);
            IReadOnlyList<PageReport> pages = Assert.IsAssignableFrom<IReadOnlyList<PageReport>>(report.Pages);

            Assert.Equal(report.PageCountScanned, pages.Count);
            Assert.Equal(pages.OrderBy(static page => page.PageId).Select(static page => page.PageId),
                pages.Select(static page => page.PageId));

            Dictionary<string, int> expectedHistogram = pages
                .GroupBy(static page => page.PageTypeName, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(static group => group.Key, static group => group.Count(), StringComparer.OrdinalIgnoreCase);
            Assert.Equal(expectedHistogram.Count, report.PageTypeHistogram.Count);
            foreach ((string pageType, int count) in expectedHistogram)
                Assert.Equal(count, report.PageTypeHistogram[pageType]);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task IndexInspector_MultipleIndexesRemainAlphabeticalAndFilterable()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempDbPath();

        try
        {
            await using (var database = await Database.OpenAsync(dbPath, ct))
            {
                await database.ExecuteAsync(
                    "CREATE TABLE ordered_indexes (id INTEGER PRIMARY KEY, a INTEGER, z INTEGER)",
                    ct);
                await database.ExecuteAsync("CREATE INDEX z_ordered_index ON ordered_indexes (z)", ct);
                await database.ExecuteAsync("CREATE INDEX a_ordered_index ON ordered_indexes (a)", ct);
            }

            IndexInspectReport all = await IndexInspector.CheckAsync(dbPath, ct: ct);
            Assert.Equal(
                new[] { "a_ordered_index", "z_ordered_index" },
                all.Indexes.Select(static item => item.IndexName));

            IndexInspectReport filtered = await IndexInspector.CheckAsync(
                dbPath,
                indexName: "Z_ORDERED_INDEX",
                ct: ct);
            Assert.Equal("z_ordered_index", Assert.Single(filtered.Indexes).IndexName);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    private static byte[] CreateSingleCellLeafPage()
    {
        var page = new byte[PageConstants.PageSize];
        const int cellOffset = PageConstants.PageSize - 9;
        page[PageConstants.PageTypeOffset] = PageConstants.PageTypeLeaf;
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(
            page.AsSpan(PageConstants.CellCountOffset, sizeof(ushort)),
            1);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(
            page.AsSpan(PageConstants.FreeSpaceStartOffset, sizeof(ushort)),
            cellOffset);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(
            page.AsSpan(PageConstants.SlottedPageHeaderSize, sizeof(ushort)),
            cellOffset);
        page[cellOffset] = 8;
        return page;
    }

    private static string NewTempDbPath() =>
        Path.Combine(Path.GetTempPath(), $"csharpdb_diag_cancel_{Guid.NewGuid():N}.db");

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    private sealed class CancelOnFirstLookupDictionary(
        IReadOnlyDictionary<uint, InspectorEngine.ParsedPage> inner,
        CancellationTokenSource cancellation)
        : IReadOnlyDictionary<uint, InspectorEngine.ParsedPage>
    {
        private int _lookupCount;

        public InspectorEngine.ParsedPage this[uint key] => inner[key];
        public IEnumerable<uint> Keys => inner.Keys;
        public IEnumerable<InspectorEngine.ParsedPage> Values => inner.Values;
        public int Count => inner.Count;

        public bool ContainsKey(uint key) => inner.ContainsKey(key);

        public IEnumerator<KeyValuePair<uint, InspectorEngine.ParsedPage>> GetEnumerator() =>
            inner.GetEnumerator();

        public bool TryGetValue(uint key, out InspectorEngine.ParsedPage value)
        {
            bool found = inner.TryGetValue(key, out InspectorEngine.ParsedPage? foundValue);
            value = foundValue!;
            if (Interlocked.Increment(ref _lookupCount) == 1)
                cancellation.Cancel();
            return found;
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    private sealed class CancelAfterFirstReadIssueList(
        IReadOnlyList<IntegrityIssue> inner,
        CancellationTokenSource cancellation)
        : IReadOnlyList<IntegrityIssue>
    {
        private int _readCount;

        public IntegrityIssue this[int index]
        {
            get
            {
                IntegrityIssue issue = inner[index];
                if (Interlocked.Increment(ref _readCount) == 1)
                    cancellation.Cancel();
                return issue;
            }
        }

        public int Count => inner.Count;

        public IEnumerator<IntegrityIssue> GetEnumerator() => inner.GetEnumerator();

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
