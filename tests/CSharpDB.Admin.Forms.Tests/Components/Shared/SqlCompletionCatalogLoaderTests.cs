using System.Reflection;
using CSharpDB.Admin.Components.Tabs;
using CSharpDB.Admin.Helpers;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Tests.Components.Shared;

public sealed class SqlCompletionCatalogLoaderTests
{
    [Fact]
    public async Task LoadsTableViewAndEverySystemCatalog_WithWorkingWhereCompletions()
    {
        await using var db = await TestDatabaseScope.CreateAsync("sql_completion");
        var ct = TestContext.Current.CancellationToken;
        await db.Client.ExecuteSqlAsync("CREATE TABLE Customers (Id INTEGER, Email TEXT)", ct);
        await db.Client.ExecuteSqlAsync("CREATE VIEW \"Customer emails\" AS SELECT Email AS Address FROM Customers", ct);
        var sources = QueryTab.BuildCompletionSources(["Customers"], ["Customer emails"]);
        var columns = await SqlCompletionCatalogLoader.LoadColumnsAsync(db.Client, sources, ct);

        Assert.Contains(columns["Customers"], c => c.Name == "Email");
        Assert.Contains(columns["Customer emails"], c => c.Name == "Address");
        foreach (var catalog in DbSystemCatalogRegistry.Catalogs)
        {
            Assert.NotEmpty(columns[catalog.Name]);
            var alias = DbSystemCatalogRegistry.GetUnderscoredAlias(catalog.Name);
            Assert.Equal(columns[catalog.Name].Select(c => c.Name), columns[alias].Select(c => c.Name));
        }
        var completionCatalog = new SqlCompletionCatalog { Sources = sources, ColumnsBySource = columns };
        foreach (var (sql, expected) in new[]
        {
            ("SELECT * FROM Customers WHERE ", "Email"),
            ("SELECT * FROM \"Customer emails\" WHERE ", "Address"),
            ("SELECT * FROM sys.tables WHERE ", "table_name"),
            ("SELECT * FROM sys_tables WHERE ", "table_name"),
        })
            Assert.Contains(SqlCompletionProvider.GetCompletions(sql, sql.Length, completionCatalog).Suggestions,
                s => s.Label == expected && s.Kind == SqlCompletionSuggestionKind.Column);
    }

    [Fact]
    public async Task FailedSource_PreservesOtherColumns_AndAliasesReuseTheZeroRowRequest()
    {
        var client = DispatchProxy.Create<ICSharpDbClient, ClientProxy>();
        var proxy = (ClientProxy)(object)client;
        var columns = await SqlCompletionCatalogLoader.LoadColumnsAsync(client,
        [new("broken", SqlCompletionSourceKind.Table), new("Customers", SqlCompletionSourceKind.Table),
         new("sys_tables", SqlCompletionSourceKind.SystemCatalog), new("sys.tables", SqlCompletionSourceKind.SystemCatalog),
         new("View\"name", SqlCompletionSourceKind.View)], TestContext.Current.CancellationToken);
        Assert.Empty(columns["broken"]);
        Assert.Contains(columns["Customers"], c => c.Name == "Email");
        Assert.Equal(new[] { "SELECT * FROM \"sys\".\"tables\" LIMIT 0", "SELECT * FROM \"View\"\"name\" LIMIT 0" }, proxy.Queries);
    }

    public class ClientProxy : DispatchProxy
    {
        public List<string> Queries { get; } = [];
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            if (targetMethod?.Name == nameof(ICSharpDbClient.GetTableSchemaAsync))
                return (string)args![0]! == "broken"
                    ? Task.FromException<CSharpDB.Client.Models.TableSchema?>(new InvalidOperationException("stale schema"))
                    : Task.FromResult<CSharpDB.Client.Models.TableSchema?>(new()
                    {
                        TableName = "Customers", Columns = [new() { Name = "Email", Type = CSharpDB.Client.Models.DbType.Text }],
                    });
            if (targetMethod?.Name == nameof(ICSharpDbClient.ExecuteSqlAsync))
            {
                Queries.Add((string)args![0]!);
                return Task.FromResult(new SqlExecutionResult { IsQuery = true, ColumnNames = ["name"], Rows = [] });
            }
            throw new NotSupportedException(targetMethod?.Name);
        }
    }
}
