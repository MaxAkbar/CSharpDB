using CSharpDB.DevOps;

namespace CSharpDB.CodeModules.Tests;

public sealed class ModuleDefinitionAnalyzerTests
{
    [Fact]
    public void StaticInspection_BindsConstantsAndNeverExecutesStaticInitializer()
    {
        string source = """
            using CSharpDB.Client;
            using System.Threading.Tasks;
            public class SavedCode
            {
                static SavedCode() => throw new System.Exception("Must never execute");
                const string Sql = "SELECT CustomerId " + "FROM Orders";
                public async Task Run(ICSharpDbClient client) { await client.ExecuteSqlAsync(Sql); }
            }
            """;
        var result = new ModuleDefinitionAnalyzer().Inspect(source, TestContext.Current.CancellationToken);
        Assert.Contains(result.References, r => r.Kind == "sql" && r.Text == "SELECT CustomerId FROM Orders" && r.Confirmed && r.Line == 7);
        Assert.Empty(result.Diagnostics);
    }

    [Fact]
    public void StaticInspection_FlagsDynamicAndUnboundCallsAndMalformedSource()
    {
        var analyzer = new ModuleDefinitionAnalyzer(); var ct = TestContext.Current.CancellationToken;
        var result = analyzer.Inspect("""
            using CSharpDB.Client;
            public class SavedCode {
                public void Run(ICSharpDbClient client, string sql) {
                    client.ExecuteSqlAsync(sql);
                    unknown.ExecuteSqlAsync("SELECT Id FROM Orders");
                }
            }
            """, ct);
        Assert.Contains(result.Diagnostics, d => d.Contains("runtime-computed"));
        Assert.Contains(result.References, r => r.Text == "SELECT Id FROM Orders" && !r.Confirmed);
        Assert.NotEmpty(analyzer.Inspect("class {", ct).Diagnostics);
        Assert.ThrowsAny<OperationCanceledException>(() => analyzer.Inspect("class X {}", new CancellationToken(true)));
    }

    [Fact]
    public void StaticInspection_CachesByDefinitionHash()
    {
        var module = new CountingAnalyzer(); var service = new DefinitionDependencyService(module);
        CSharpDB.Client.Models.DefinitionCatalogRecord Definition(string source) => new() { Id = "m", Kind = "Module", Name = "module", Format = "module", Source = source, MetadataJson = "{}" };
        var first = new DefinitionCatalogSnapshot("1", DateTimeOffset.UtcNow, [Definition("class One {}")], []);
        service.Analyze(first, TestContext.Current.CancellationToken);
        service.Analyze(first with { Version = "2" }, TestContext.Current.CancellationToken);
        Assert.Equal(1, module.Calls);
        service.Analyze(first with { Definitions = [Definition("class Two {}")] }, TestContext.Current.CancellationToken);
        Assert.Equal(2, module.Calls);
    }
    private sealed class CountingAnalyzer : IDefinitionModuleAnalyzer
    {
        public int Calls;
        public ModuleDefinitionInspection Inspect(string source, CancellationToken ct = default) { Calls++; return new([], []); }
    }
}
