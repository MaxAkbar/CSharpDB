using CSharpDB.ImportExport.Models;
using CSharpDB.ImportExport.TableArchives;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public class TableArchiveTests
{
    [Fact]
    public async Task Archive_RoundtripsSchemaAndRows()
    {
        var ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"customers_{Guid.NewGuid():N}.csdbtable");
        var schema = new TableSchema
        {
            TableName = "customers",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false, IsPrimaryKey = true, IsIdentity = true },
                new ColumnDefinition { Name = "name", Type = DbType.Text, Nullable = false, Collation = "NOCASE" },
                new ColumnDefinition { Name = "balance", Type = DbType.Real, Nullable = true },
                new ColumnDefinition { Name = "payload", Type = DbType.Blob, Nullable = true },
            ],
            NextRowId = 12,
        };
        var rows = new List<DbValue[]>
        {
            new[] { DbValue.FromInteger(1), DbValue.FromText("O'Reilly"), DbValue.FromReal(10.5), DbValue.FromBlob(new byte[] { 0x01, 0x02, 0xff }) },
            new[] { DbValue.FromInteger(2), DbValue.FromText("Nulls"), DbValue.Null, DbValue.Null },
        };

        try
        {
            var manifest = await TableArchiveWriter.WriteAsync(path, schema, TableArchiveWriter.ToAsyncRows(rows, ct), ct);
            Assert.Equal(2, manifest.RowCount);
            Assert.Equal("customers", manifest.SourceTableName);
            Assert.Equal(TableArchiveManifest.CurrentFormatVersion, manifest.FormatVersion);
            Assert.Equal("CSDBTBL3"u8.ToArray(), File.ReadAllBytes(path).Take(8).ToArray());
            var index = Assert.Single(manifest.Indexes);
            Assert.Equal("primary-key", index.Kind);
            Assert.Equal("id", index.ColumnName);
            Assert.Equal(2, index.EntryCount);

            TableSchema restoredSchema = await TableArchiveReader.ReadTableSchemaAsync(path, ct: ct);
            Assert.Equal("customers", restoredSchema.TableName);
            Assert.Equal(4, restoredSchema.Columns.Count);
            Assert.True(restoredSchema.Columns[0].IsPrimaryKey);
            Assert.True(restoredSchema.Columns[0].IsIdentity);
            Assert.Equal("NOCASE", restoredSchema.Columns[1].Collation);
            Assert.Equal(12, restoredSchema.NextRowId);

            var restoredRows = new List<DbValue[]>();
            await foreach (DbValue[] row in TableArchiveReader.ReadRowsAsync(path, ct))
                restoredRows.Add(row);

            Assert.Equal(rows.Count, restoredRows.Count);
            Assert.Equal("O'Reilly", restoredRows[0][1].AsText);
            Assert.Equal(10.5, restoredRows[0][2].AsReal);
            Assert.Equal(new byte[] { 0x01, 0x02, 0xff }, restoredRows[0][3].AsBlob);
            Assert.True(restoredRows[1][2].IsNull);
            Assert.True(restoredRows[1][3].IsNull);

            Assert.True(await TableArchiveReader.HasIntegerPrimaryKeyIndexAsync(path, ct));
            var lookup = await TableArchiveReader.LookupIntegerPrimaryKeyAsync(path, 2, ct);
            Assert.True(lookup.IsIndexed);
            Assert.NotNull(lookup.Row);
            Assert.Equal("Nulls", lookup.Row![1].AsText);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Archive_PrimaryKeyIndexSupportsMultipleBTreeLevels()
    {
        var ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"indexed_{Guid.NewGuid():N}.csdbtable");
        var schema = new TableSchema
        {
            TableName = "large_customers",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false, IsPrimaryKey = true },
                new ColumnDefinition { Name = "name", Type = DbType.Text, Nullable = false },
            ],
            NextRowId = 65001,
        };

        try
        {
            await TableArchiveWriter.WriteAsync(path, schema, GenerateRows(65000, ct), ct);

            Assert.True(await TableArchiveReader.HasIntegerPrimaryKeyIndexAsync(path, ct));

            var first = await TableArchiveReader.LookupIntegerPrimaryKeyAsync(path, 1, ct);
            var middle = await TableArchiveReader.LookupIntegerPrimaryKeyAsync(path, 32768, ct);
            var last = await TableArchiveReader.LookupIntegerPrimaryKeyAsync(path, 65000, ct);
            var missing = await TableArchiveReader.LookupIntegerPrimaryKeyAsync(path, 70000, ct);

            Assert.Equal("Customer 1", first.Row![1].AsText);
            Assert.Equal("Customer 32768", middle.Row![1].AsText);
            Assert.Equal("Customer 65000", last.Row![1].AsText);
            Assert.True(missing.IsIndexed);
            Assert.Null(missing.Row);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    [Fact]
    public async Task Archive_SupportsEmptyTables()
    {
        var ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"empty_{Guid.NewGuid():N}.csdbtable");
        var schema = new TableSchema
        {
            TableName = "empty_table",
            Columns =
            [
                new ColumnDefinition { Name = "id", Type = DbType.Integer, Nullable = false },
            ],
            NextRowId = 1,
        };

        try
        {
            var manifest = await TableArchiveWriter.WriteAsync(path, schema, TableArchiveWriter.ToAsyncRows(Array.Empty<DbValue[]>(), ct), ct);
            Assert.Equal(0, manifest.RowCount);

            var rows = new List<DbValue[]>();
            await foreach (DbValue[] row in TableArchiveReader.ReadRowsAsync(path, ct))
                rows.Add(row);

            Assert.Empty(rows);
        }
        finally
        {
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    private static async IAsyncEnumerable<DbValue[]> GenerateRows(
        int count,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        for (int i = 1; i <= count; i++)
        {
            ct.ThrowIfCancellationRequested();
            yield return [DbValue.FromInteger(i), DbValue.FromText($"Customer {i}")];
            if ((i & 1023) == 0)
                await Task.Yield();
        }
    }
}
