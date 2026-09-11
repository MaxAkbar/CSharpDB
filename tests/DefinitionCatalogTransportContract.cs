using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.ImportExport.TableArchives;

internal static class DefinitionCatalogTransportContract
{
    internal static async Task<IReadOnlyList<DefinitionCatalogRecord>> SeedAsync(string path, CancellationToken ct)
    {
        await using var client = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = path });
        await TableArchiveWriter.WriteAsync(path + ".archive.csdbtable", new CSharpDB.Primitives.TableSchema
        {
            TableName = "OriginalOrders",
            Columns = [new() { Name = "Id", Type = CSharpDB.Primitives.DbType.Integer, IsPrimaryKey = true, Nullable = false }, new() { Name = "ParentId", Type = CSharpDB.Primitives.DbType.Integer }],
            ForeignKeys = [new() { ConstraintName = "archive_parent", ColumnName = "ParentId", ReferencedTableName = "OriginalOrders", ReferencedColumnName = "Id", SupportingIndexName = "__archive_parent" }],
        }, TableArchiveWriter.ToAsyncRows([], ct), ct);
        var result = await client.ExecuteSqlAsync("""
            CREATE TABLE Orders (Id INTEGER PRIMARY KEY, CustomerId INTEGER, Amount INTEGER DEFAULT 0 CHECK (Amount >= 0));
            CREATE VIEW buyer_view AS SELECT CustomerId FROM Orders;
            CREATE INDEX ix_amount ON Orders (Amount);
            CREATE TRIGGER orders_audit AFTER UPDATE ON Orders BEGIN INSERT INTO Orders (Id, Amount) VALUES (NEW.Id + 1000, NEW.Amount); END;
            CREATE TABLE __forms (id TEXT, name TEXT, table_name TEXT, definition_json TEXT);
            INSERT INTO __forms VALUES ('f1','Order entry','Orders','{"tableName":"Orders","fields":[{"fieldName":"CustomerId"}]}');
            CREATE TABLE __reports (id TEXT, name TEXT, definition_json TEXT);
            CREATE TABLE __report_definition_chunks (storage_id TEXT, chunk_ordinal INTEGER, chunk_text TEXT);
            INSERT INTO __reports VALUES ('r1','Buyer report','#chunked:report1');
            INSERT INTO __report_definition_chunks VALUES ('report1',0,'{"source":{"kind":"View","name":"buyer_view"},'),('report1',1,'"fields":[{"boundFieldName":"CustomerId"}]}');
            CREATE TABLE _etl_pipelines (name TEXT, current_revision INTEGER);
            CREATE TABLE _etl_pipeline_versions (name TEXT, revision INTEGER, package_json TEXT);
            INSERT INTO _etl_pipelines VALUES ('buyer export',2);
            INSERT INTO _etl_pipeline_versions VALUES ('buyer export',1,'{"old":true}'),('buyer export',2,'{"source":{"tableName":"Orders"},"destination":{"tableName":"Orders"}}');
            CREATE TABLE __code_modules (module_id TEXT, name TEXT, owner_id TEXT, owner_kind TEXT, source TEXT);
            INSERT INTO __code_modules VALUES ('m1','Form behavior','f1','Form','throw new System.Exception("NEVER RUN");');
            CREATE TABLE __data_model_diagrams (id TEXT, name TEXT, diagram_json TEXT);
            INSERT INTO __data_model_diagrams VALUES ('model1','Sales model','{"Version":5,"Nodes":[{"Name":"Orders","Columns":[{"Name":"CustomerId"}]}],"PendingOperations":[{"Id":"change1","TableName":"Orders","ColumnName":"CustomerId","NewColumnName":"BuyerId","Description":"Rename buyer column","Kind":5}]}');
            """, ct);
        Xunit.Assert.Null(result.Error);
        Xunit.Assert.Null((await client.ExecuteSqlAsync($"CREATE EXTERNAL TABLE archived_orders FROM '{Path.GetFileName(path + ".archive.csdbtable").Replace("'", "''", StringComparison.Ordinal)}'", ct)).Error);
        for (int i = 0; i < 70; i++) await client.UpsertSavedQueryAsync("query_" + i, "SELECT CustomerId FROM Orders -- " + i, ct);
        await client.UpsertSavedQueryAsync("large", "SELECT '" + new string('λ', 33000) + "' AS text", ct);
        await client.UpsertSavedQueryAsync("__designer_layout:hidden", "{}", ct);
        return await ReadAllAsync(client, ct);
    }

    internal static async Task<IReadOnlyList<DefinitionCatalogRecord>> ReadAllAsync(ICSharpDbClient client, CancellationToken ct)
    {
        var reader = Xunit.Assert.IsAssignableFrom<ICSharpDbDefinitionCatalogReader>(client);
        var records = new List<DefinitionCatalogRecord>(); string? token = null, version = null;
        do
        {
            var page = await reader.ReadDefinitionCatalogAsync(token, 7, ct);
            Xunit.Assert.InRange(page.Records.Count, 0, 7); Xunit.Assert.Empty(page.Diagnostics);
            Xunit.Assert.True(JsonSerializer.SerializeToUtf8Bytes(page).Length < 4 * 1024 * 1024);
            version ??= page.CatalogVersion; Xunit.Assert.Equal(version, page.CatalogVersion);
            records.AddRange(page.Records); token = page.ContinuationToken;
        } while (token is not null);
        Xunit.Assert.DoesNotContain(records, r => r.Name == "__designer_layout:hidden");
        Xunit.Assert.Contains(records, r => r.Kind == "Form"); Xunit.Assert.Contains(records, r => r.Kind == "Trigger");
        Xunit.Assert.Contains(records, r => r.Kind == "Module" && r.Source.Contains("NEVER RUN"));
        Xunit.Assert.Contains(records, r => r.Kind == "Report" && r.Source.Contains("boundFieldName"));
        Xunit.Assert.Contains(records, r => r.Kind == "Data model" && r.Id == "DataModel:model1");
        Xunit.Assert.Contains(records, r => r.Kind == "Proposed change" && r.Id == "ModelChange:model1:change1" && r.OwnerName == "Sales model");
        Xunit.Assert.Contains(records, r => r.Kind == "External table" && r.Name == "archived_orders");
        Xunit.Assert.Contains(records, r => r.Kind == "Archive relationship" && r.Source.Contains("OriginalOrders"));
        Xunit.Assert.DoesNotContain(records, r => r.Kind == "Pipeline" && r.Source.Contains("old"));
        return records;
    }

    internal static async Task VerifyAsync(ICSharpDbClient client, IReadOnlyList<DefinitionCatalogRecord> expected, CancellationToken ct)
    {
        var actual = await ReadAllAsync(client, ct);
        Xunit.Assert.Equal(expected, actual);
        await Xunit.Assert.ThrowsAnyAsync<OperationCanceledException>(() => ((ICSharpDbDefinitionCatalogReader)client).ReadDefinitionCatalogAsync(ct: new CancellationToken(true)));
        var result = await client.ExecuteSqlAsync("SELECT COUNT(*) FROM Orders", ct); Xunit.Assert.Null(result.Error);
    }
}
