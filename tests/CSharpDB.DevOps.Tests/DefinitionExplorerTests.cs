using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.Engine;
using CSharpDB.Sql;

namespace CSharpDB.DevOps.Tests;

public sealed class DefinitionExplorerTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    internal static DefinitionCatalogRecord Definition(string kind, string name, string source, string format = "sql", string? owner = null) => new()
    { Id = kind + ":" + name, Kind = kind, Name = name, Source = source, Format = format, OwnerName = owner, SourceHash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(source))) };
    internal static DefinitionCatalogRecord Table(string name, params string[] columns) => Definition("Table", name,
        JsonSerializer.Serialize(new TableSchema { TableName = name, Columns = columns.Select(c => new ColumnDefinition { Name = c, Type = DbType.Integer }).ToArray() }, Json), "table");
    private static DefinitionCatalogSnapshot Catalog(params DefinitionCatalogRecord[] records) => new(Guid.NewGuid().ToString(), DateTimeOffset.UtcNow, records, []);

    [Fact]
    public void Matching_WholeWordsCaseHighlightsAndLocations()
    {
        string source = "id ID order_id id2\r\n'id' -- id\nαid idβ id\u0301 id$ id";
        var exact = DefinitionTextSearch.Find(source, "id", true, true, Ct);
        Assert.Equal(4, exact.Count);
        Assert.Equal(new DefinitionTextMatch(21, 2, 2, 2), exact[1]);
        Assert.All(exact, m => Assert.Equal("id", source.Substring(m.Start, m.Length)));
        Assert.Equal(5, DefinitionTextSearch.Find(source, "id", false, true, Ct).Count);
        Assert.Empty(DefinitionTextSearch.Find(source, "", ct: Ct));
        Assert.Single(DefinitionTextSearch.Search([Definition("View", "needle", "SELECT 1"), Definition("Procedure", "p", "needle")], "needle", "View", ct: Ct));
        Assert.Equal(5, DefinitionTextSearch.Find("a\rb\r\nc\nd", "c", ct: Ct)[0].Start);
        Assert.Equal(3, DefinitionTextSearch.Find("a\rb\r\nc\nd", "c", ct: Ct)[0].Line);
    }

    private static SqlDependencyRelation? Resolve(string name) => name.ToLowerInvariant() switch
    { "orders" => new("Orders", ["Id", "CustomerId", "Amount"]), "customers" => new("Customers", ["Id", "Name"]), _ => null };

    [Theory]
    [InlineData("SELECT o.CustomerId AS customer FROM Orders o JOIN Customers c ON o.CustomerId = c.Id WHERE o.Amount > 10", "Orders", "Amount", "Predicate")]
    [InlineData("SELECT c.Name FROM Customers c WHERE EXISTS (SELECT 1 FROM Orders o WHERE o.CustomerId = c.Id)", "Customers", "Id", "Predicate")]
    [InlineData("UPDATE Orders SET Amount = Amount + 1 WHERE CustomerId = @id", "Orders", "Amount", "Write")]
    [InlineData("DELETE FROM Orders WHERE Amount < 0", "Orders", "Amount", "Predicate")]
    [InlineData("INSERT INTO Orders VALUES (1, 2, 3)", "Orders", null, "Positional insert")]
    [InlineData("INSERT INTO Orders (Id, CustomerId) VALUES (1, 2)", "Orders", "CustomerId", "Write")]
    [InlineData("SELECT SUM(Amount) OVER (PARTITION BY CustomerId ORDER BY Id) AS total FROM Orders", "Orders", "CustomerId", "Window")]
    [InlineData("SELECT * FROM Orders", "Orders", "Amount", "Projection")]
    public void Sql_ResolvesScopedReadsWritesAndShapes(string sql, string table, string? column, string usage)
    {
        var result = new SqlDependencyAnalyzer().Analyze(sql, Resolve, ct: Ct);
        Assert.Empty(result.Diagnostics);
        Assert.Contains(result.References, r => r.Relation == table && r.Column == column && r.Usage == usage);
    }

    [Fact]
    public void Cte_OnlyReferencedOutputLineageEscapesScope()
    {
        var result = new SqlDependencyAnalyzer().Analyze("WITH x (a, b) AS (SELECT CustomerId, Amount FROM Orders) SELECT a AS result FROM x", Resolve, ct: Ct);
        Assert.Empty(result.Diagnostics);
        Assert.Contains(result.References, r => r.Column == "CustomerId" && r.OutputColumn == "result");
        Assert.DoesNotContain(result.References, r => r.Column == "Amount");
        Assert.Equal(["result"], result.OutputColumns);
    }

    [Fact]
    public void Sql_DoesNotBindCommentsStringsOrAmbiguousNames()
    {
        var analyzer = new SqlDependencyAnalyzer();
        var result = analyzer.Analyze("SELECT 'Orders.Amount' AS text FROM Customers -- Orders.CustomerId", Resolve, ct: Ct);
        Assert.DoesNotContain(result.References, r => r.Relation == "Orders");
        Assert.Contains(analyzer.Analyze("SELECT Id FROM Orders o JOIN Customers c ON o.CustomerId = c.Id", Resolve, ct: Ct).Diagnostics, d => d.Message.Contains("Ambiguous"));
        Assert.NotEmpty(analyzer.Analyze("SELECT FROM broken", Resolve, ct: Ct).Diagnostics);
        Assert.NotEmpty(analyzer.Analyze("SELECT missing FROM Orders", Resolve, ct: Ct).Diagnostics);
    }

    [Fact]
    public void Lineage_FollowsRenamedOutputsAndPredicatesAcrossNativeDefinitions()
    {
        var analysis = new DefinitionDependencyService().Analyze(Catalog(
            Table("Orders", "Id", "CustomerId", "Amount"), Table("Archive", "Buyer"),
            Definition("View", "buyers", "SELECT CustomerId AS buyer, Amount FROM Orders WHERE Amount > 0"),
            Definition("View", "ids", "SELECT Id FROM Orders"),
            Definition("Saved query", "q", "SELECT buyer FROM buyers"),
            Definition("Report", "r", """{"source":{"kind":"SavedQuery","name":"q"},"fields":[{"boundFieldName":"buyer"}]}""", "report"),
            Definition("Form", "f", """{"tableName":"Orders","fields":[{"fieldName":"CustomerId"}],"actions":[{"kind":"RunSql","value":"q"}]}""", "form", "Orders"),
            Definition("Pipeline", "p", """{"source":{"kind":"SqlQuery","queryText":"SELECT buyer FROM buyers"},"transforms":[{"kind":"Rename","renameMappings":[{"source":"buyer","target":"Buyer"}]}],"destination":{"tableName":"Archive"},"incremental":{"watermarkColumn":"buyer"}}""", "pipeline")), Ct);
        Assert.Empty(analysis.Diagnostics);
        foreach (string field in new[] { "CustomerId", "Amount" })
        {
            var report = ColumnImpactService.Assess(analysis, "Orders", field, ColumnChangeKind.Drop, Ct);
            foreach (string id in new[] { "View:buyers", "Saved query:q", "Report:r", "Pipeline:p" }) Assert.Contains(report.Findings, f => f.ObjectId == id && f.Category is ColumnImpactCategory.ConfirmedUsage or ColumnImpactCategory.IndirectEffects);
            Assert.DoesNotContain(report.Findings, f => f.ObjectId == "View:ids" && f.Category == ColumnImpactCategory.ConfirmedUsage);
            Assert.Contains(report.Findings, f => f.ObjectId == "View:ids" && f.Category == ColumnImpactCategory.EngineRestriction);
        }
        Assert.Contains(analysis.Dependencies, e => e.SourceId == "Pipeline:p" && e.TargetId == "Table:Archive" && e.TargetColumn == "Buyer" && e.Usage == "Write");
        Assert.DoesNotContain(ColumnImpactService.Assess(analysis, "Orders", "CustomerId", ColumnChangeKind.Nullability, Ct).Findings, f => f.Category == ColumnImpactCategory.EngineRestriction);
    }

    [Fact]
    public void CatalogCache_RebindsAfterSchemaAndRouteChangeAndKeepsMalformedCoverage()
    {
        var service = new DefinitionDependencyService();
        var sql = Definition("View", "v", "SELECT Amount FROM Orders");
        Assert.Empty(service.Analyze(Catalog(Table("Orders", "Amount"), sql), Ct).Diagnostics);
        Assert.NotEmpty(service.Analyze(Catalog(Table("Orders", "Total"), sql), Ct).Diagnostics);
        var newRoute = service.Analyze(Catalog(Table("Different", "Amount"), sql, Definition("Form", "bad", "{invalid", "form")), Ct);
        Assert.Empty(newRoute.Dependencies);
        Assert.Equal(3, newRoute.Catalog.Definitions.Count);
        Assert.Contains(newRoute.Diagnostics, d => d.ObjectId == "Form:bad");
        var cycle = service.Analyze(Catalog(Definition("View", "a", "SELECT * FROM b"), Definition("View", "b", "SELECT * FROM a")), Ct);
        Assert.NotEmpty(cycle.Diagnostics);
        Assert.NotEmpty(service.Analyze(cycle.Catalog, Ct).Diagnostics);
    }

    [Fact]
    public void NativeBindings_InspectChildrenLookupsTransformsAndExternalSources()
    {
        var analysis = new DefinitionDependencyService().Analyze(Catalog(Table("Orders", "Id", "CustomerId", "Amount"), Table("Customers", "Id", "Name"), Table("Export", "Buyer", "DoubleAmount"),
            Definition("Form", "entry", """{"tableName":"Customers","controls":[{"binding":{"fieldName":"Name"},"props":{"lookupTable":"Orders","valueField":"Id","displayFields":["Amount","CustomerId"]}},{"props":{"childTabs":[{"childTable":"Orders","foreignKeyField":"CustomerId","parentKeyField":"Id","visibleColumns":["Amount"]}]}}]}""", "form", "Customers"),
            Definition("Pipeline", "etl", """{"source":{"kind":"SqlQuery","queryText":"SELECT CustomerId, Amount FROM Orders"},"transforms":[{"kind":"Filter","filterExpression":"Amount > 0"},{"kind":"Derive","derivedColumns":[{"name":"DoubleAmount","expression":"Amount * 2"}]},{"kind":"Rename","renameMappings":[{"source":"CustomerId","target":"Buyer"}]},{"kind":"Select","selectColumns":["Buyer","DoubleAmount"]},{"kind":"Cast","castMappings":[{"column":"Buyer","targetType":0}]},{"kind":"Deduplicate","deduplicateKeys":["Buyer"]}],"destination":{"tableName":"Export"},"incremental":{"watermarkColumn":"CustomerId"}}""", "pipeline")), Ct);
        Assert.Empty(analysis.Diagnostics);
        Assert.Contains(analysis.Dependencies, e => e.SourceId == "Form:entry" && e.TargetColumn == "Amount" && e.Location.EndsWith("displayFields"));
        Assert.Contains(analysis.Dependencies, e => e.SourceId == "Form:entry" && e.TargetColumn == "CustomerId" && e.Usage == "Child binding");
        Assert.Contains(analysis.Dependencies, e => e.SourceId == "Pipeline:etl" && e.TargetColumn == "Amount" && e.OutputColumn == "DoubleAmount");
        Assert.Contains(analysis.Dependencies, e => e.SourceId == "Pipeline:etl" && e.Location.Contains("watermarkColumn"));
        Assert.DoesNotContain(analysis.Dependencies, e => e.TargetId == "Table:Export" && e.TargetColumn == "Amount");
        var external = new DefinitionDependencyService().Analyze(Catalog(Table("Orders", "Id"), Definition("Pipeline", "external", """{"source":{"kind":"SqlQuery","queryText":"SELECT Id FROM Orders","connectionString":"external"},"destination":{"kind":"CsvFile","path":"output.csv"}}""", "pipeline")), Ct);
        Assert.Empty(external.Dependencies); Assert.Equal(2, external.Diagnostics.Count);
    }

    [Fact]
    public void LargeCatalog_SearchAnalysisCacheAndCancellation()
    {
        const int count = 10000;
        var definitions = Enumerable.Range(0, count).Select(i => Definition("Saved query", $"q{i:D5}", $"SELECT CustomerId AS buyer FROM Orders WHERE Amount > {i}")).Prepend(Table("Orders", "CustomerId", "Amount")).ToArray();
        var service = new DefinitionDependencyService(); var catalog = Catalog(definitions);
        var timer = Stopwatch.StartNew(); var analysis = service.Analyze(catalog, Ct); var cold = timer.Elapsed;
        timer.Restart(); var results = DefinitionTextSearch.Search(definitions, "CustomerId", ct: Ct); var search = timer.Elapsed;
        timer.Restart(); service.Analyze(catalog, Ct); var warm = timer.Elapsed;
        TestContext.Current.TestOutputHelper!.WriteLine($"10,001 definitions: analysis {cold.TotalMilliseconds:F0} ms; search {search.TotalMilliseconds:F0} ms; refresh analysis {warm.TotalMilliseconds:F0} ms; {analysis.Dependencies.Count} edges.");
        Assert.Equal(count + 1, results.Count); Assert.Empty(analysis.Diagnostics);
        Assert.Equal(count, analysis.Dependencies.Count(e => e.Usage == "Projection"));
        using var cancelled = new CancellationTokenSource(); cancelled.Cancel();
        Assert.Throws<OperationCanceledException>(() => service.Analyze(catalog, cancelled.Token));
        Assert.Throws<OperationCanceledException>(() => DefinitionTextSearch.Search(definitions, "a", ct: cancelled.Token));
    }

    [Fact]
    public async Task EmbeddedInspection_IsReadOnlyAndPaginatesWithoutExecutingDefinitions()
    {
        string path = Path.Combine(Path.GetTempPath(), "definition-test-" + Guid.NewGuid().ToString("N") + ".db");
        try
        {
            await using (var client = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = path }))
            {
                var empty = await DefinitionCatalogService.ReadAsync(client, ct: Ct);
                Assert.Empty(empty.Definitions);
            }
            await using (var db = await Database.OpenAsync(path, ct: Ct)) Assert.Empty(db.GetTableNames());
            await using (var client = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = path }))
            {
                Assert.Null((await client.ExecuteSqlAsync("CREATE TABLE Orders (Id INTEGER PRIMARY KEY, Amount INTEGER DEFAULT 0 CHECK (Amount >= 0)); CREATE VIEW totals AS SELECT Amount FROM Orders; CREATE INDEX ix_amount ON Orders (Amount);", Ct)).Error);
                await client.UpsertSavedQueryAsync("danger", "DROP TABLE Orders; -- " + new string('λ', 24000), Ct);
                var reader = Assert.IsAssignableFrom<ICSharpDbDefinitionCatalogReader>(client);
                var page = await reader.ReadDefinitionCatalogAsync(pageSize: 1, ct: Ct);
                Assert.Single(page.Records); Assert.NotNull(page.ContinuationToken);
                var all = await DefinitionCatalogService.ReadAsync(client, ct: Ct);
                Assert.Empty(all.Diagnostics);
                Assert.Contains(all.Definitions, d => d.Kind == "Index"); Assert.Contains(all.Definitions, d => d.Kind == "Default"); Assert.Contains(all.Definitions, d => d.Kind == "Check");
                Assert.Equal(24022, all.Definitions.Single(d => d.Name == "danger").Source.Length);
                Assert.NotNull(await client.GetTableSchemaAsync("Orders", Ct));
                await client.UpsertSavedQueryAsync("new", "SELECT Id FROM Orders", Ct);
                Assert.Equal(page.CatalogVersion, (await reader.ReadDefinitionCatalogAsync(page.ContinuationToken, 1, Ct)).CatalogVersion);
                Assert.NotEqual(page.CatalogVersion, (await reader.ReadDefinitionCatalogAsync(ct: Ct)).CatalogVersion);
                await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => reader.ReadDefinitionCatalogAsync(pageSize: 0, ct: Ct));
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => reader.ReadDefinitionCatalogAsync(ct: new CancellationToken(true)));
            }
        }
        finally { foreach (string file in new[] { path, path + ".wal", path + ".shm" }) if (File.Exists(file)) File.Delete(file); }
    }

    [Fact]
    public async Task Impact_MatchesActualEngineRestrictionsAndInspectionNeverInvokesCallback()
    {
        string path = Path.Combine(Path.GetTempPath(), $"impact-engine-{Guid.NewGuid():N}.db"); int calls = 0;
        var functions = CSharpDB.Primitives.DbFunctionRegistry.Create(b => b.AddScalar("InspectionProbe", 0, (_, _) => { calls++; return CSharpDB.Primitives.DbValue.FromInteger(1); }));
        try
        {
            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = path, DirectDatabaseOptions = new DatabaseOptions { Functions = functions } });
            Assert.Null((await client.ExecuteSqlAsync("CREATE TABLE T (Id INTEGER PRIMARY KEY, Value INTEGER); CREATE VIEW V AS SELECT Id, InspectionProbe() AS probe FROM T", Ct)).Error);
            await client.CreateProcedureAsync(new() { Name = "danger", BodySql = "DROP TABLE T", IsEnabled = false }, Ct);
            await client.UpsertSavedQueryAsync("callback", "SELECT InspectionProbe() FROM T", Ct);
            calls = 0;
            var catalog = await DefinitionCatalogService.ReadAsync(client, ct: Ct);
            var analysis = new DefinitionDependencyService().Analyze(catalog, Ct);
            Assert.Equal(0, calls); Assert.Contains(catalog.Definitions, d => d.Kind == "Procedure" && !d.IsEnabled);
            Assert.Contains(ColumnImpactService.Assess(analysis, "T", "Value", ColumnChangeKind.Rename, Ct).Findings, f => f.Category == ColumnImpactCategory.EngineRestriction);
            Assert.NotNull((await client.ExecuteSqlAsync("ALTER TABLE T RENAME COLUMN Value TO Total", Ct)).Error);
            Assert.DoesNotContain(ColumnImpactService.Assess(analysis, "T", "Value", ColumnChangeKind.Nullability, Ct).Findings, f => f.Category == ColumnImpactCategory.EngineRestriction);
            Assert.Null((await client.ExecuteSqlAsync("ALTER TABLE T ALTER COLUMN Value SET NOT NULL; DROP VIEW V; CREATE INDEX ix_value ON T (Value)", Ct)).Error);
            analysis = new DefinitionDependencyService().Analyze(await DefinitionCatalogService.ReadAsync(client, ct: Ct), Ct);
            Assert.Contains(ColumnImpactService.Assess(analysis, "T", "Value", ColumnChangeKind.Drop, Ct).Findings, f => f.Category == ColumnImpactCategory.EngineRestriction && f.ObjectId.Contains("ix_value"));
            Assert.NotNull((await client.ExecuteSqlAsync("ALTER TABLE T DROP COLUMN Value", Ct)).Error);
            Assert.DoesNotContain(ColumnImpactService.Assess(analysis, "T", "Id", ColumnChangeKind.Rename, Ct).Findings, f => f.Category == ColumnImpactCategory.EngineRestriction);
            Assert.Null((await client.ExecuteSqlAsync("ALTER TABLE T RENAME COLUMN Id TO KeyId", Ct)).Error);
        }
        finally { foreach (string file in new[] { path, path + ".wal", path + ".shm" }) if (File.Exists(file)) File.Delete(file); }
    }

    [Fact]
    public async Task LargePersistedCatalog_PagesCompletelyAndRefreshes()
    {
        string path = Path.Combine(Path.GetTempPath(), $"catalog-scale-{Guid.NewGuid():N}.db");
        try
        {
            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = path });
            await client.UpsertSavedQueryAsync("initial", "SELECT 1", Ct);
            for (int batch = 0; batch < 20; batch++)
            {
                string values = string.Join(",", Enumerable.Range(batch * 500, 500).Select(i => $"('catalog_{i}', 'SELECT {i} AS value', '2026-09-09', '2026-09-09')"));
                Assert.Null((await client.ExecuteSqlAsync("INSERT INTO __saved_queries (name, sql_text, created_utc, updated_utc) VALUES " + values, Ct)).Error);
            }
            var timer = Stopwatch.StartNew(); var catalog = await DefinitionCatalogService.ReadAsync(client, ct: Ct); var elapsed = timer.Elapsed;
            Assert.Equal(10001, catalog.Definitions.Count); Assert.Empty(catalog.Diagnostics);
            Assert.Equal(10001, catalog.Definitions.Select(d => d.Id).Distinct().Count());
            TestContext.Current.TestOutputHelper!.WriteLine($"10,001 persisted definitions: complete paged catalog read {elapsed.TotalMilliseconds:F0} ms.");
            await client.UpsertSavedQueryAsync("initial", "SELECT 2", Ct);
            var refreshed = await DefinitionCatalogService.ReadAsync(client, ct: Ct); Assert.NotEqual(catalog.Version, refreshed.Version);
            Assert.Equal("SELECT 2", refreshed.Definitions.Single(d => d.Name == "initial").Source);
        }
        finally { foreach (string file in new[] { path, path + ".wal", path + ".shm" }) if (File.Exists(file)) File.Delete(file); }
    }

    [Fact]
    public async Task ConstantColumnCheck_StillCarriesEngineColumnDependency()
    {
        string path = Path.Combine(Path.GetTempPath(), $"constant-check-{Guid.NewGuid():N}.db");
        try
        {
            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = path });
            Assert.Null((await client.ExecuteSqlAsync("CREATE TABLE T (Id INTEGER, Value INTEGER CHECK (1 = 1))", Ct)).Error);
            var analysis = new DefinitionDependencyService().Analyze(await DefinitionCatalogService.ReadAsync(client, ct: Ct), Ct);
            Assert.Contains(ColumnImpactService.Assess(analysis, "T", "Value", ColumnChangeKind.Drop, Ct).Findings, f => f.Category == ColumnImpactCategory.EngineRestriction);
            Assert.NotNull((await client.ExecuteSqlAsync("ALTER TABLE T DROP COLUMN Value", Ct)).Error);
        }
        finally { foreach (string file in new[] { path, path + ".wal", path + ".shm" }) if (File.Exists(file)) File.Delete(file); }
    }

    [Fact]
    public async Task CorruptReportChunks_ProduceCoverageDiagnosticWithoutLosingHealthyReport()
    {
        string path = Path.Combine(Path.GetTempPath(), $"report-chunks-{Guid.NewGuid():N}.db");
        try
        {
            await using var client = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = path });
            Assert.Null((await client.ExecuteSqlAsync("""
                CREATE TABLE __reports (id TEXT, name TEXT, definition_json TEXT);
                CREATE TABLE __report_definition_chunks (storage_id TEXT, chunk_ordinal INTEGER, chunk_text TEXT);
                INSERT INTO __reports VALUES ('1','broken','#chunked:broken'),('2','healthy','{"source":{"name":"T"}}');
                INSERT INTO __report_definition_chunks VALUES ('broken',1,'{}');
                """, Ct)).Error);
            var catalog = await DefinitionCatalogService.ReadAsync(client, ct: Ct);
            Assert.Contains(catalog.Definitions, d => d.Name == "healthy");
            Assert.Contains(catalog.Diagnostics, d => d.Source == "broken");
        }
        finally { foreach (string file in new[] { path, path + ".wal", path + ".shm" }) if (File.Exists(file)) File.Delete(file); }
    }
}
