using System.Reflection;
using CSharpDB.Admin.ImportExport.Contracts;
using CSharpDB.Admin.ImportExport.Services;
using CSharpDB.Client;
using CSharpDB.ImportExport.Models;
using CSharpDB.ImportExport.TableArchives;
using ClientColumnDefinition = CSharpDB.Client.Models.ColumnDefinition;
using ClientDbType = CSharpDB.Client.Models.DbType;
using ClientIndexSchema = CSharpDB.Client.Models.IndexSchema;
using ClientTableBrowseResult = CSharpDB.Client.Models.TableBrowseResult;
using ClientTableSchema = CSharpDB.Client.Models.TableSchema;

namespace CSharpDB.Tests;

public sealed class TableArchiveFallbackExportTests
{
    [Fact]
    public async Task ExportTableAsync_FallbackIncludesOnlySourceTableIndexesWithFullMetadata()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"fallback_indexes_{Guid.NewGuid():N}.csdbtable");
        ClientTableSchema schema = CreateSchema();
        ICSharpDbClient client = CreateClient(
            schema,
            [
                new ClientIndexSchema
                {
                    IndexName = "ux_items_code_region",
                    TableName = "ITEMS",
                    Columns = ["code", "region"],
                    ColumnCollations = ["NOCASE", null],
                    IsUnique = true,
                },
                new ClientIndexSchema
                {
                    IndexName = "ix_other_code",
                    TableName = "other",
                    Columns = ["code"],
                    ColumnCollations = [null],
                },
            ]);

        try
        {
            var service = new TableImportExportService(client, new TableArchiveDownloadStore());
            TableExportResult result = await service.ExportTableAsync(
                new TableExportRequest
                {
                    TableName = "items",
                    Destination = TableExportDestination.ServerPath,
                    ServerPath = path,
                },
                ct: ct);

            Assert.Equal(1, result.RowCount);
            TableArchiveSchema archiveSchema = await TableArchiveReader.ReadArchiveSchemaAsync(path, ct);
            Assert.Equal(41, archiveSchema.NextRowId);
            TableArchiveSecondaryIndex index = Assert.Single(archiveSchema.SecondaryIndexes!);
            Assert.Equal("ux_items_code_region", index.Name);
            Assert.Equal(["code", "region"], index.Columns);
            Assert.Equal(["NOCASE", null], index.ColumnCollations);
            Assert.True(index.IsUnique);

            TableArchiveManifest manifest = await TableArchiveReader.ReadManifestAsync(path, ct);
            Assert.Equal(TableArchiveManifest.SchemaFidelityFormatVersion, manifest.FormatVersion);
        }
        finally
        {
            await client.DisposeAsync();
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    private static ClientTableSchema CreateSchema() => new()
    {
        TableName = "items",
        Columns =
        [
            new ClientColumnDefinition
            {
                Name = "id",
                Type = ClientDbType.Integer,
                Nullable = false,
                IsPrimaryKey = true,
            },
            new ClientColumnDefinition
            {
                Name = "code",
                Type = ClientDbType.Text,
                Nullable = false,
            },
            new ClientColumnDefinition
            {
                Name = "region",
                Type = ClientDbType.Text,
                Nullable = false,
            },
        ],
        NextRowId = 41,
    };

    private static ICSharpDbClient CreateClient(
        ClientTableSchema schema,
        IReadOnlyList<ClientIndexSchema> indexes)
    {
        ICSharpDbClient client = DispatchProxy.Create<ICSharpDbClient, FallbackClientProxy>();
        var proxy = (FallbackClientProxy)client;
        proxy.Schema = schema;
        proxy.Indexes = indexes;
        return client;
    }

    public class FallbackClientProxy : DispatchProxy
    {
        public required ClientTableSchema Schema { get; set; }
        public required IReadOnlyList<ClientIndexSchema> Indexes { get; set; }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) => targetMethod?.Name switch
        {
            "get_DataSource" => "fallback",
            "GetTableSchemaAsync" => Task.FromResult<ClientTableSchema?>(Schema),
            "GetIndexesAsync" => Task.FromResult(Indexes),
            "BrowseTableAsync" => Task.FromResult(new ClientTableBrowseResult
            {
                TableName = Schema.TableName,
                Schema = Schema,
                Rows = [[1L, "A", "West"]],
                TotalRows = 1,
                Page = (int)args![1]!,
                PageSize = (int)args[2]!,
            }),
            "DisposeAsync" => ValueTask.CompletedTask,
            _ => throw new NotSupportedException(targetMethod?.Name),
        };
    }
}
