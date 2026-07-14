using System.Buffers.Binary;
using CSharpDB.Engine;
using CSharpDB.Storage.Diagnostics;

namespace CSharpDB.Tests;

public sealed class StorageDiagnosticsTests
{
    [Fact]
    public async Task DatabaseInspector_InspectAsync_BadMagicReportsError()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempDbPath();

        try
        {
            await File.WriteAllBytesAsync(dbPath, new byte[PageConstants.PageSize], ct);

            DatabaseInspectReport report = await DatabaseInspector.InspectAsync(dbPath, ct: ct);

            Assert.Contains(report.Issues, i => i.Code == "DB_HEADER_BAD_MAGIC" && i.Severity == InspectSeverity.Error);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task DatabaseInspector_InspectPageAsync_OutOfRangeReportsNotFound()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempDbPath();

        try
        {
            await using (var db = await Database.OpenAsync(dbPath, ct))
            {
                await db.ExecuteAsync("CREATE TABLE t (id INTEGER PRIMARY KEY)", ct);
            }

            PageInspectReport report = await DatabaseInspector.InspectPageAsync(dbPath, 9_999, includeHex: false, ct);

            Assert.False(report.Exists);
            Assert.Contains(report.Issues, i => i.Code == "PAGE_NOT_FOUND" && i.Severity == InspectSeverity.Error);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task DatabaseInspector_InspectPageAsync_InteriorOverflowFlagReportsError()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempDbPath();

        try
        {
            var databaseBytes = new byte[PageConstants.PageSize * 2];
            int pageBase = PageConstants.PageSize;
            ulong encodedPayloadSize = PageConstants.LeafCellOverflowFlag | 12UL;
            int cellOffset = PageConstants.PageSize - Varint.SizeOf(encodedPayloadSize) - 12;

            databaseBytes[pageBase + PageConstants.PageTypeOffset] = PageConstants.PageTypeInterior;
            BinaryPrimitives.WriteUInt16LittleEndian(
                databaseBytes.AsSpan(pageBase + PageConstants.CellCountOffset, 2),
                1);
            BinaryPrimitives.WriteUInt16LittleEndian(
                databaseBytes.AsSpan(pageBase + PageConstants.FreeSpaceStartOffset, 2),
                checked((ushort)cellOffset));
            BinaryPrimitives.WriteUInt16LittleEndian(
                databaseBytes.AsSpan(pageBase + PageConstants.SlottedPageHeaderSize, 2),
                checked((ushort)cellOffset));

            int headerBytes = Varint.Write(
                databaseBytes.AsSpan(pageBase + cellOffset),
                encodedPayloadSize);
            BinaryPrimitives.WriteUInt32LittleEndian(
                databaseBytes.AsSpan(pageBase + cellOffset + headerBytes, 4),
                PageConstants.NullPageId);
            BinaryPrimitives.WriteInt64LittleEndian(
                databaseBytes.AsSpan(pageBase + cellOffset + headerBytes + 4, 8),
                42);

            await File.WriteAllBytesAsync(dbPath, databaseBytes, ct);

            PageInspectReport report = await DatabaseInspector.InspectPageAsync(
                dbPath,
                pageId: 1,
                includeHex: false,
                ct);

            Assert.Contains(
                report.Issues,
                issue => issue.Code == "INTERIOR_CELL_OVERFLOW_FLAG_INVALID" &&
                         issue.Severity == InspectSeverity.Error);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task DatabaseInspector_FormatVersionOne_RemainsSupported()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempDbPath();

        try
        {
            var databaseBytes = new byte[PageConstants.PageSize];
            PageConstants.MagicBytes.CopyTo(databaseBytes, PageConstants.MagicOffset);
            BinaryPrimitives.WriteInt32LittleEndian(
                databaseBytes.AsSpan(PageConstants.VersionOffset, 4),
                PageConstants.MinimumSupportedFormatVersion);
            BinaryPrimitives.WriteInt32LittleEndian(
                databaseBytes.AsSpan(PageConstants.PageSizeOffset, 4),
                PageConstants.PageSize);
            BinaryPrimitives.WriteUInt32LittleEndian(
                databaseBytes.AsSpan(PageConstants.PageCountOffset, 4),
                1);

            int pageBase = PageConstants.FileHeaderSize;
            databaseBytes[pageBase + PageConstants.PageTypeOffset] = PageConstants.PageTypeLeaf;
            BinaryPrimitives.WriteUInt16LittleEndian(
                databaseBytes.AsSpan(pageBase + PageConstants.FreeSpaceStartOffset, 2),
                PageConstants.PageSize);

            await File.WriteAllBytesAsync(dbPath, databaseBytes, ct);

            DatabaseInspectReport report = await DatabaseInspector.InspectAsync(dbPath, ct: ct);

            Assert.Equal(PageConstants.MinimumSupportedFormatVersion, report.Header.Version);
            Assert.True(report.Header.VersionValid);
            Assert.DoesNotContain(report.Issues, issue => issue.Code == "DB_HEADER_BAD_VERSION");
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task StorageInspectors_EmptyDatabase_DoNotWarnOnStatsCatalogSentinels()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempDbPath();

        try
        {
            await using (var db = await Database.OpenAsync(dbPath, ct))
            {
            }

            DatabaseInspectReport dbReport = await DatabaseInspector.InspectAsync(dbPath, ct: ct);
            IndexInspectReport indexReport = await IndexInspector.CheckAsync(dbPath, ct: ct);

            Assert.DoesNotContain(dbReport.Issues, i => i.Code == "CATALOG_TABLE_SCHEMA_DECODE_FAILED");
            Assert.DoesNotContain(indexReport.Issues, i => i.Code == "CATALOG_TABLE_SCHEMA_DECODE_FAILED");
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task DatabaseInspector_LargeRowOverflowPages_AreParsedAsHealthy()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempDbPath();

        try
        {
            await using (var db = await Database.OpenAsync(dbPath, ct))
            {
                await db.ExecuteAsync("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)", ct);
                await db.ExecuteAsync(
                    $"INSERT INTO docs VALUES (1, '{new string('x', 10_000)}')",
                    ct);
            }

            DatabaseInspectReport report = await DatabaseInspector.InspectAsync(dbPath, ct: ct);

            Assert.True(report.PageTypeHistogram.TryGetValue("overflow", out int overflowPageCount));
            Assert.True(overflowPageCount >= 3);
            Assert.DoesNotContain(report.Issues, issue => issue.Severity == InspectSeverity.Error);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task DatabaseInspector_OverflowChainOutsideDatabaseReportsError()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempDbPath();

        try
        {
            await using (var db = await Database.OpenAsync(dbPath, ct))
            {
                await db.ExecuteAsync("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)", ct);
                await db.ExecuteAsync(
                    $"INSERT INTO docs VALUES (1, '{new string('x', 10_000)}')",
                    ct);
            }

            byte[] databaseBytes = await File.ReadAllBytesAsync(dbPath, ct);
            int physicalPageCount = databaseBytes.Length / PageConstants.PageSize;
            int overflowPageId = Enumerable.Range(1, physicalPageCount - 1)
                .First(pageId =>
                    databaseBytes[pageId * PageConstants.PageSize + PageConstants.PageTypeOffset] ==
                    PageConstants.PageTypeOverflow);
            BinaryPrimitives.WriteUInt32LittleEndian(
                databaseBytes.AsSpan(
                    overflowPageId * PageConstants.PageSize + PageConstants.OverflowNextOffset,
                    sizeof(uint)),
                uint.MaxValue);
            await File.WriteAllBytesAsync(dbPath, databaseBytes, ct);

            DatabaseInspectReport report = await DatabaseInspector.InspectAsync(dbPath, ct: ct);

            Assert.Contains(report.Issues, issue => issue.Code == "OVERFLOW_NEXT_PAGE_OOB");
            Assert.Contains(report.Issues, issue => issue.Code == "OVERFLOW_CHAIN_PAGE_OOB");
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task WalInspector_InspectAsync_TrailingPartialFrameReportsWarning()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempDbPath();
        string walPath = dbPath + ".wal";

        try
        {
            byte[] header = new byte[PageConstants.WalHeaderSize];
            PageConstants.WalMagic.AsSpan().CopyTo(header.AsSpan(0, 4));
            BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(4, 4), 1);
            BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(8, 4), PageConstants.PageSize);
            BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(12, 4), 1);
            BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(16, 4), 1234);
            BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(20, 4), 5678);

            byte[] trailing = new byte[77];
            byte[] walBytes = new byte[header.Length + trailing.Length];
            Buffer.BlockCopy(header, 0, walBytes, 0, header.Length);
            Buffer.BlockCopy(trailing, 0, walBytes, header.Length, trailing.Length);
            await File.WriteAllBytesAsync(walPath, walBytes, ct);

            WalInspectReport report = await WalInspector.InspectAsync(dbPath, ct: ct);

            Assert.True(report.Exists);
            Assert.Equal(77, report.TrailingBytes);
            Assert.Contains(report.Issues, i => i.Code == "WAL_TRAILING_PARTIAL_FRAME" && i.Severity == InspectSeverity.Warning);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(walPath);
        }
    }

    [Fact]
    public async Task IndexInspector_CheckAsync_ReportsHealthyIndex()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempDbPath();

        try
        {
            await using (var db = await Database.OpenAsync(dbPath, ct))
            {
                await db.ExecuteAsync("CREATE TABLE users (id INTEGER PRIMARY KEY, score INTEGER)", ct);
                await db.ExecuteAsync("CREATE INDEX idx_users_score ON users (score)", ct);
            }

            IndexInspectReport report = await IndexInspector.CheckAsync(dbPath, ct: ct);
            IndexCheckItem item = Assert.Single(report.Indexes);

            Assert.Equal("idx_users_score", item.IndexName);
            Assert.True(item.RootPageValid);
            Assert.True(item.TableExists);
            Assert.True(item.ColumnsExistInTable);
            Assert.True(item.RootTreeReachable);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task StorageInspectors_LiveDatabaseWithCommittedWal_ReportCommittedView()
    {
        var ct = TestContext.Current.CancellationToken;
        string dbPath = NewTempDbPath();

        try
        {
            await using var db = await Database.OpenAsync(dbPath, ct);
            await db.ExecuteAsync("CREATE TABLE customers (id INTEGER PRIMARY KEY, score INTEGER)", ct);
            await db.ExecuteAsync("INSERT INTO customers VALUES (1, 10)", ct);
            await db.ExecuteAsync("INSERT INTO customers VALUES (2, 20)", ct);
            await db.ExecuteAsync("CREATE INDEX idx_customers_score ON customers (score)", ct);

            DatabaseInspectReport dbReport = await DatabaseInspector.InspectAsync(dbPath, ct: ct);
            IndexInspectReport indexReport = await IndexInspector.CheckAsync(dbPath, ct: ct);

            Assert.DoesNotContain(dbReport.Issues, static issue => issue.Code == "DB_PAGE_COUNT_MISMATCH");
            Assert.DoesNotContain(dbReport.Issues, static issue => issue.Code == "BTREE_CHILD_OUT_OF_RANGE");
            Assert.DoesNotContain(indexReport.Issues, static issue => issue.Code == "DB_PAGE_COUNT_MISMATCH");
            Assert.DoesNotContain(indexReport.Issues, static issue => issue.Code == "BTREE_CHILD_OUT_OF_RANGE");

            IndexCheckItem item = Assert.Single(indexReport.Indexes);
            Assert.Equal("idx_customers_score", item.IndexName);
            Assert.True(item.RootPageValid);
            Assert.True(item.TableExists);
            Assert.True(item.ColumnsExistInTable);
            Assert.True(item.RootTreeReachable);
        }
        finally
        {
            DeleteIfExists(dbPath);
            DeleteIfExists(dbPath + ".wal");
        }
    }

    private static string NewTempDbPath()
    {
        return Path.Combine(Path.GetTempPath(), $"csharpdb_diag_test_{Guid.NewGuid():N}.db");
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}
