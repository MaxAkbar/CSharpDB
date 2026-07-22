using CSharpDB.ImportExport.Models;
using CSharpDB.ImportExport.TableArchives;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class TableArchiveBoundedIndexTests
{
    [Fact]
    public async Task Archive_AboveIndexEntryLimit_UsesCompleteScanFallback()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(
            Path.GetTempPath(),
            $"bounded_archive_index_{Guid.NewGuid():N}.csdbtable");
        int rowCount = TableArchiveWriter.MaximumInMemoryPrimaryKeyIndexEntries + 1;
        var schema = new TableSchema
        {
            TableName = "bounded_index_items",
            Columns =
            [
                new ColumnDefinition
                {
                    Name = "id",
                    Type = DbType.Integer,
                    Nullable = false,
                    IsPrimaryKey = true,
                },
            ],
            NextRowId = rowCount + 1L,
        };

        try
        {
            TableArchiveManifest manifest = await TableArchiveWriter.WriteAsync(
                path,
                schema,
                GenerateRows(rowCount, ct),
                ct);

            Assert.Equal(rowCount, manifest.RowCount);
            Assert.Empty(manifest.Indexes);
            Assert.False(await TableArchiveReader.HasIntegerPrimaryKeyIndexAsync(path, ct));

            var lookup = await TableArchiveReader.LookupIntegerPrimaryKeyAsync(path, rowCount, ct);
            Assert.False(lookup.IsIndexed);
            Assert.Null(lookup.Row);

            long rowsRead = 0;
            long lastKey = 0;
            await foreach (DbValue[] row in TableArchiveReader.ReadRowsAsync(path, ct))
            {
                rowsRead++;
                lastKey = Assert.Single(row).AsInteger;
            }

            Assert.Equal(rowCount, rowsRead);
            Assert.Equal(rowCount, lastKey);
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
        for (int id = 1; id <= count; id++)
        {
            ct.ThrowIfCancellationRequested();
            yield return [DbValue.FromInteger(id)];
            if (id % 4096 == 0)
                await Task.Yield();
        }
    }
}
