using System.Buffers.Binary;
using System.Text;
using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.DevOps;
using CSharpDB.ImportExport.TableArchives;
using CSharpDB.Primitives;

namespace CSharpDB.DevOps.Tests;

public sealed class DataModelDefinitionCatalogTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static string Literal(string value) => "'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";

    [Fact]
    public async Task SavedDiagramsAndProposals_AreReadOnlyPagedAndRefreshable()
    {
        string path = Path.Combine(Path.GetTempPath(), $"model-catalog-{Guid.NewGuid():N}.db");
        int calls = 0;
        var functions = DbFunctionRegistry.Create(b => b.AddScalar("InspectionProbe", 0, (_, _) => { calls++; return DbValue.FromInteger(1); }));
        try
        {
            await using (var emptyClient = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = path }))
                Assert.Empty((await DefinitionCatalogService.ReadAsync(emptyClient, ct: Ct)).Definitions);
            await using (var database = await CSharpDB.Engine.Database.OpenAsync(path, ct: Ct))
                Assert.Empty(database.GetTableNames());
            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions
            { DataSource = path, DirectDatabaseOptions = new CSharpDB.Engine.DatabaseOptions { Functions = functions } });
            Assert.Empty(await client.GetTableNamesAsync(Ct));
            string source = JsonSerializer.Serialize(new
            {
                Version = 5,
                Nodes = new[] { new { Name = "Orders", Columns = new[] { new { Name = "Id" } } } },
                PendingOperations = new[] { new { Id = "rename-id", TableName = "Orders", ColumnName = "Id", NewColumnName = "OrderId", Description = "Rename order identity", ExpressionSql = "InspectionProbe()", Kind = 5 } },
                Notes = new string('λ', 24000),
            });
            Assert.Null((await client.ExecuteSqlAsync($"""
                CREATE TABLE Orders (Id INTEGER PRIMARY KEY);
                CREATE TABLE __data_model_diagrams (id TEXT, name TEXT, diagram_json TEXT);
                INSERT INTO __data_model_diagrams VALUES ('model-1', 'Fulfillment', {Literal(source)});
                """, Ct)).Error);
            await client.UpsertSavedQueryAsync("__designer_layout:query-layout", "{}", Ct);
            var before = (await client.GetTableNamesAsync(Ct)).Order().ToArray();
            var internalBefore = await InternalTablesAsync(client);
            var catalog = await DefinitionCatalogService.ReadAsync(client, ct: Ct);
            Assert.Empty(catalog.Diagnostics);
            var diagram = Assert.Single(catalog.Definitions, d => d.Kind == "Data model");
            Assert.Equal("DataModel:model-1", diagram.Id);
            Assert.Equal(source, diagram.Source);
            var proposal = Assert.Single(catalog.Definitions, d => d.Kind == "Proposed change");
            Assert.Equal("ModelChange:model-1:rename-id", proposal.Id);
            Assert.Equal("Fulfillment", proposal.OwnerName);
            Assert.Equal("modelChange", proposal.Format);
            using (var metadata = JsonDocument.Parse(proposal.MetadataJson!))
                Assert.Equal(diagram.Id, metadata.RootElement.GetProperty("modelId").GetString());
            Assert.DoesNotContain(catalog.Definitions, d => d.Name.Contains("query-layout", StringComparison.Ordinal));
            Assert.Equal(before, (await client.GetTableNamesAsync(Ct)).Order().ToArray());
            Assert.Equal(internalBefore, await InternalTablesAsync(client));
            Assert.Equal(0, calls);
            Assert.Null((await client.ExecuteSqlAsync("UPDATE __data_model_diagrams SET name = 'Renamed model'", Ct)).Error);
            var refreshed = await DefinitionCatalogService.ReadAsync(client, ct: Ct);
            Assert.NotEqual(catalog.Version, refreshed.Version);
            Assert.Equal(diagram.Id, Assert.Single(refreshed.Definitions, d => d.Kind == "Data model").Id);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => DefinitionCatalogService.ReadAsync(client, ct: new CancellationToken(true)));
        }
        finally { DeleteDatabase(path); }
    }

    [Fact]
    public async Task MalformedDiagramOrProposal_PreservesSearchableSourceAndOtherModels()
    {
        string path = Path.Combine(Path.GetTempPath(), $"model-invalid-{Guid.NewGuid():N}.db");
        try
        {
            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = path });
            Assert.Null((await client.ExecuteSqlAsync("""
                CREATE TABLE __data_model_diagrams (id TEXT, name TEXT, diagram_json TEXT);
                INSERT INTO __data_model_diagrams VALUES
                  ('bad', 'Damaged diagram', '{invalid'),
                  ('draft', 'Damaged proposal', '{"pendingOperations":[null,{"id":"p","tableName":"Orders"},{"id":"p","tableName":"Orders"}]}'),
                  ('good', 'Healthy diagram', '{"nodes":[]}');
                """, Ct)).Error);
            var catalog = await DefinitionCatalogService.ReadAsync(client, ct: Ct);
            Assert.Equal(3, catalog.Definitions.Count(d => d.Kind == "Data model"));
            Assert.Equal(2, catalog.Definitions.Count(d => d.Kind == "Proposed change"));
            Assert.Contains(catalog.Diagnostics, d => d.Source == "Damaged diagram");
            Assert.Contains(catalog.Diagnostics, d => d.Message.Contains("duplicate identity", StringComparison.Ordinal));
            Assert.Equal(catalog.Definitions.Count, catalog.Definitions.Select(d => d.Id).Distinct().Count());
        }
        finally { DeleteDatabase(path); }
    }

    [Fact]
    public async Task RegisteredArchives_UseMetadataAndKeepSourceIdentitiesSeparate()
    {
        string path = Path.Combine(Path.GetTempPath(), $"archive-catalog-{Guid.NewGuid():N}.db");
        string archivePath = path + ".csdbtable";
        try
        {
            var schema = ArchiveSchema();
            await TableArchiveWriter.WriteAsync(archivePath, schema, TableArchiveWriter.ToAsyncRows([[DbValue.FromInteger(1), DbValue.FromInteger(1)]], Ct), Ct);
            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = path });
            Assert.Null((await client.ExecuteSqlAsync($"CREATE EXTERNAL TABLE archived_orders FROM {Literal(Path.GetFileName(archivePath))}", Ct)).Error);
            // The definition-only reader must not validate or visit row payload bytes.
            byte[] archive = await File.ReadAllBytesAsync(archivePath, Ct);
            long rowOffset = BinaryPrimitives.ReadInt64LittleEndian(archive.AsSpan(36));
            archive[(int)rowOffset] ^= 0x01;
            await File.WriteAllBytesAsync(archivePath, archive, Ct);
            var before = (await client.GetTableNamesAsync(Ct)).Order().ToArray();
            var catalog = await DefinitionCatalogService.ReadAsync(client, ct: Ct);
            Assert.Empty(catalog.Diagnostics);
            var external = Assert.Single(catalog.Definitions, d => d.Kind == "External table");
            Assert.Equal("ExternalTable:archived_orders", external.Id);
            Assert.DoesNotContain(schema.SchemaId.ToString(), external.Source, StringComparison.OrdinalIgnoreCase);
            using var source = JsonDocument.Parse(external.Source);
            Assert.Equal("archived_orders", source.RootElement.GetProperty("tableName").GetString());
            Assert.Equal(2, source.RootElement.GetProperty("columns").GetArrayLength());
            var relationship = Assert.Single(catalog.Definitions, d => d.Kind == "Archive relationship");
            Assert.Equal("archived_orders", relationship.OwnerName);
            using var reference = JsonDocument.Parse(relationship.Source);
            Assert.Equal("SourceOrders", reference.RootElement.GetProperty("referencedTableName").GetString());
            using var metadata = JsonDocument.Parse(relationship.MetadataJson!);
            Assert.Equal("archived_orders", Assert.Single(metadata.RootElement.GetProperty("referencedTableCandidates").EnumerateArray()).GetString());
            Assert.False(metadata.RootElement.GetProperty("dataPayloadIntegrityVerified").GetBoolean());
            Assert.Equal(before, (await client.GetTableNamesAsync(Ct)).Order().ToArray());
            await Assert.ThrowsAsync<InvalidDataException>(async () => await TableArchiveReader.ReadMetadataAsync(archivePath, Ct));
        }
        finally { DeleteDatabase(path); if (File.Exists(archivePath)) File.Delete(archivePath); }
    }

    [Fact]
    public async Task MissingAndCorruptArchives_RemainDiscoverableWithCoverageDiagnostics()
    {
        string path = Path.Combine(Path.GetTempPath(), $"archive-missing-{Guid.NewGuid():N}.db");
        string archivePath = path + ".csdbtable";
        try
        {
            await TableArchiveWriter.WriteAsync(archivePath, ArchiveSchema(), TableArchiveWriter.ToAsyncRows([], Ct), Ct);
            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = path });
            Assert.Null((await client.ExecuteSqlAsync($"CREATE EXTERNAL TABLE missing_archive FROM {Literal(archivePath)}; CREATE EXTERNAL TABLE corrupt_archive FROM {Literal(archivePath)}", Ct)).Error);
            Assert.Null((await client.ExecuteSqlAsync($"UPDATE __external_tables SET path = {Literal(archivePath + ".missing")} WHERE name = 'missing_archive'", Ct)).Error);
            await File.WriteAllTextAsync(archivePath, "invalid archive", Ct);
            var catalog = await DefinitionCatalogService.ReadAsync(client, ct: Ct);
            Assert.Equal(2, catalog.Definitions.Count(d => d.Kind == "External table"));
            Assert.Contains(catalog.Diagnostics, d => d.Source == "missing_archive");
            Assert.Contains(catalog.Diagnostics, d => d.Source == "corrupt_archive");
            Assert.DoesNotContain(catalog.Definitions, d => d.Kind == "Archive relationship");
        }
        finally { DeleteDatabase(path); if (File.Exists(archivePath)) File.Delete(archivePath); }
    }

    [Fact]
    public async Task DefinitionMetadata_ReadsNoRowsOrIndexPages_AndStillValidatesSchemaIntegrity()
    {
        using var destination = new MemoryStream();
        await TableArchiveWriter.WriteAsync(destination, ArchiveSchema(), TableArchiveWriter.ToAsyncRows([[DbValue.FromInteger(1), DbValue.FromInteger(1)]], Ct), Ct);
        byte[] bytes = destination.ToArray();
        using (var guarded = new MetadataOnlyStream(bytes))
        {
            var metadata = await TableArchiveReader.ReadDefinitionMetadataAsync(guarded, Ct);
            Assert.Equal("SourceOrders", metadata.Schema.TableName);
            Assert.True(guarded.BytesRead < bytes.Length);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await TableArchiveReader.ReadDefinitionMetadataAsync(guarded, new CancellationToken(true)));
        }
        long schemaOffset = BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(12));
        int schemaLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(20));
        int nameOffset = Encoding.UTF8.GetString(bytes, (int)schemaOffset, schemaLength).IndexOf("SourceOrders", StringComparison.Ordinal);
        Assert.True(nameOffset >= 0);
        bytes[(int)schemaOffset + nameOffset] = (byte)'X';
        using var corrupt = new MemoryStream(bytes);
        await Assert.ThrowsAsync<InvalidDataException>(async () => await TableArchiveReader.ReadDefinitionMetadataAsync(corrupt, Ct));
    }

    private static TableSchema ArchiveSchema()
    {
        Guid tableId = Guid.NewGuid(), id = Guid.NewGuid(), parentId = Guid.NewGuid(), keyId = Guid.NewGuid();
        return new()
        {
            SchemaId = tableId, TableName = "SourceOrders",
            Columns =
            [
                new() { SchemaId = id, Name = "Id", Type = DbType.Integer, IsPrimaryKey = true, Nullable = false },
                new() { SchemaId = parentId, Name = "ParentId", Type = DbType.Integer },
            ],
            KeyConstraints = [new() { SchemaId = keyId, ConstraintName = "pk_orders", Kind = KeyConstraintKind.PrimaryKey, Columns = ["Id"] }],
            ForeignKeys = [new()
            {
                SchemaId = Guid.NewGuid(), ConstraintName = "fk_parent", ColumnName = "ParentId", ColumnSchemaIds = [parentId],
                ReferencedTableName = "SourceOrders", ReferencedTableSchemaId = tableId,
                ReferencedColumnName = "Id", ReferencedColumnSchemaIds = [id], ReferencedKeySchemaId = keyId, SupportingIndexName = "__fk_parent",
            }],
        };
    }

    private static void DeleteDatabase(string path)
    {
        foreach (string file in new[] { path, path + ".wal", path + ".shm" })
            if (File.Exists(file)) File.Delete(file);
    }

    private static async Task<string[]> InternalTablesAsync(ICSharpDbClient client)
    {
        var result = await client.ExecuteSqlAsync("SELECT table_name FROM sys.internal_tables ORDER BY table_name", Ct);
        Assert.Null(result.Error);
        return result.Rows!.Select(row => (string)row[0]!).ToArray();
    }

    private sealed class MetadataOnlyStream(byte[] bytes) : MemoryStream(bytes, writable: false)
    {
        public long BytesRead { get; private set; }
        private readonly (long Start, long End)[] _allowed =
        [
            (0, 76),
            (BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(12)), BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(12)) + BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(20))),
            (BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(24)), BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(24)) + BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(32))),
            (BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(60)), BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(60)) + 48),
        ];
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
        {
            Assert.Contains(_allowed, range => Position >= range.Start && Position + buffer.Length <= range.End);
            BytesRead += buffer.Length;
            return base.ReadAsync(buffer, ct);
        }
    }
}
