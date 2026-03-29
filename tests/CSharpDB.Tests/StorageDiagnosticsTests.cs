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
