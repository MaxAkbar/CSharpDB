using CSharpDB.Admin.Models;
using CSharpDB.Admin.Services;
using CSharpDB.Client;
using CSharpDB.DevOps;

namespace CSharpDB.Admin.Forms.Tests.Admin;

public sealed class CompareDeployAdminServiceTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void ParseKeyColumns_TrimsCsv()
    {
        Assert.Equal(["id", "tenant_id"], CompareDeployAdminService.ParseKeyColumns(" id, tenant_id ,,"));
    }

    [Fact]
    public void IsWritableEndpoint_ReturnsFalseForArchives()
    {
        Assert.True(CompareDeployAdminService.IsWritableEndpoint(CompareDeployEndpointSpec.CurrentDatabase));
        Assert.True(CompareDeployAdminService.IsWritableEndpoint(new CompareDeployEndpointSpec(CompareDeployEndpointKind.DatabaseFile, "target.db")));
        Assert.False(CompareDeployAdminService.IsWritableEndpoint(new CompareDeployEndpointSpec(CompareDeployEndpointKind.TableArchive, "target.csdbtable")));
    }

    [Fact]
    public async Task CompareSchemaAsync_RendersAndAppliesAddedColumnScript()
    {
        string sourcePath = await CreateSourceDatabaseAsync(
            "devops_source",
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT);");
        await using TestDatabaseScope target = await TestDatabaseScope.CreateAsync("devops_target");

        try
        {
            await target.ExecuteAsync("CREATE TABLE customers (id INTEGER PRIMARY KEY);");
            var service = new CompareDeployAdminService(target.Client);

            var result = await service.CompareSchemaAsync(sourcePath, "customers", Ct);

            SchemaDiffChange change = Assert.Single(result.Report.Changes);
            Assert.Equal(SchemaObjectKind.Column, change.ObjectKind);
            Assert.Equal(SchemaChangeKind.Added, change.ChangeKind);
            string script = service.RenderSchemaScript(result.Report);
            Assert.Contains("ALTER TABLE customers ADD COLUMN name TEXT;", script, StringComparison.Ordinal);

            await service.ApplyScriptAsync(script, Ct);

            var schema = await target.Client.GetTableSchemaAsync("customers", Ct);
            Assert.Contains(schema!.Columns, column => column.Name == "name");
        }
        finally
        {
            DeleteDatabaseFiles(sourcePath);
        }
    }

    [Fact]
    public async Task CompareDataAsync_RendersAndAppliesDataSyncScript()
    {
        string sourcePath = await CreateSourceDatabaseAsync(
            "devops_data_source",
            "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT);",
            "INSERT INTO customers (id, name) VALUES (1, 'Ada'), (2, 'Grace');");
        await using TestDatabaseScope target = await TestDatabaseScope.CreateAsync("devops_data_target");

        try
        {
            await target.ExecuteAsync("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT);");
            await target.ExecuteAsync("INSERT INTO customers (id, name) VALUES (1, 'Ada Lovelace'), (3, 'Target Only');");
            var service = new CompareDeployAdminService(target.Client);

            var result = await service.CompareDataAsync(sourcePath, "customers", keyColumns: string.Empty, maxPreviewRows: 100, ct: Ct);

            Assert.Equal(1, result.Report.Summary.SourceOnlyRows);
            Assert.Equal(1, result.Report.Summary.TargetOnlyRows);
            Assert.Equal(1, result.Report.Summary.ChangedRows);
            string script = service.RenderDataScript(result.Report);
            Assert.Contains("INSERT INTO customers", script, StringComparison.Ordinal);
            Assert.Contains("UPDATE customers SET name = 'Ada'", script, StringComparison.Ordinal);
            Assert.Contains("DELETE FROM customers WHERE id = 3;", script, StringComparison.Ordinal);

            await service.ApplyScriptAsync(script, Ct);

            IReadOnlyList<Dictionary<string, object?>> rows = await target.QueryRowsAsync("SELECT id, name FROM customers ORDER BY id;");
            Assert.Equal(2, rows.Count);
            Assert.Equal(1L, rows[0]["id"]);
            Assert.Equal("Ada", rows[0]["name"]);
            Assert.Equal(2L, rows[1]["id"]);
            Assert.Equal("Grace", rows[1]["name"]);
        }
        finally
        {
            DeleteDatabaseFiles(sourcePath);
        }
    }

    [Fact]
    public async Task CompareSchemaAsync_CanUseCurrentDatabaseAsSourceAndDatabaseFileAsTarget()
    {
        await using TestDatabaseScope current = await TestDatabaseScope.CreateAsync("devops_current_source");
        await current.ExecuteAsync("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT);");
        string targetPath = await CreateSourceDatabaseAsync(
            "devops_external_target",
            "CREATE TABLE customers (id INTEGER PRIMARY KEY);");

        try
        {
            var service = new CompareDeployAdminService(current.Client);
            var target = new CompareDeployEndpointSpec(CompareDeployEndpointKind.DatabaseFile, targetPath);

            var result = await service.CompareSchemaAsync(
                CompareDeployEndpointSpec.CurrentDatabase,
                target,
                "customers",
                Ct);

            SchemaDiffChange change = Assert.Single(result.Report.Changes);
            Assert.Equal(SchemaObjectKind.Column, change.ObjectKind);
            Assert.Equal(SchemaChangeKind.Added, change.ChangeKind);

            string script = service.RenderSchemaScript(result.Report);
            await service.ApplyScriptAsync(script, target, Ct);

            await using ICSharpDbClient targetClient = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = targetPath });
            var schema = await targetClient.GetTableSchemaAsync("customers", Ct);
            Assert.Contains(schema!.Columns, column => column.Name == "name");
        }
        finally
        {
            DeleteDatabaseFiles(targetPath);
        }
    }

    [Fact]
    public async Task ScriptSchemaAsync_ScriptsWholeDatabaseFromSelectedEndpoint()
    {
        await using TestDatabaseScope current = await TestDatabaseScope.CreateAsync("devops_script_source");
        await current.ExecuteAsync("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT);");
        await current.ExecuteAsync("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER);");
        await current.ExecuteAsync("CREATE INDEX idx_orders_customer ON orders (customer_id);");
        var service = new CompareDeployAdminService(current.Client);

        CompareDeployRunResult<string> result = await service.ScriptSchemaAsync(
            CompareDeployEndpointSpec.CurrentDatabase,
            new SchemaScriptOptions { Scope = SchemaScriptScope.WholeDatabase },
            Ct);

        Assert.Contains("CREATE TABLE customers", result.Report, StringComparison.Ordinal);
        Assert.Contains("CREATE TABLE orders", result.Report, StringComparison.Ordinal);
        Assert.Contains("CREATE INDEX idx_orders_customer", result.Report, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GetSchemaObjectsAsync_ReturnsSelectableObjectsWithParents()
    {
        await using TestDatabaseScope current = await TestDatabaseScope.CreateAsync("devops_script_objects");
        await current.ExecuteAsync("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER);");
        await current.ExecuteAsync("CREATE INDEX idx_orders_customer ON orders (customer_id);");
        var service = new CompareDeployAdminService(current.Client);

        IReadOnlyList<CompareDeploySchemaObjectOption> objects = await service.GetSchemaObjectsAsync(
            CompareDeployEndpointSpec.CurrentDatabase,
            Ct);

        Assert.Contains(objects, option => option.Kind == SchemaObjectKind.Table && option.Name == "orders");
        Assert.Contains(objects, option => option.Kind == SchemaObjectKind.Index && option.Name == "idx_orders_customer" && option.ParentName == "orders");
    }

    private static async Task<string> CreateSourceDatabaseAsync(string name, params string[] statements)
    {
        string databasePath = Path.Combine(Path.GetTempPath(), $"{name}_{Guid.NewGuid():N}.db");
        await using ICSharpDbClient client = CSharpDbClient.Create(new CSharpDbClientOptions
        {
            DataSource = databasePath,
        });

        await client.GetInfoAsync(Ct);
        foreach (string sql in statements)
        {
            var result = await client.ExecuteSqlAsync(sql, Ct);
            Assert.Null(result.Error);
        }

        return databasePath;
    }

    private static void DeleteDatabaseFiles(string databasePath)
    {
        TryDelete(databasePath);
        TryDelete(databasePath + ".wal");
    }

    private static void TryDelete(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}
