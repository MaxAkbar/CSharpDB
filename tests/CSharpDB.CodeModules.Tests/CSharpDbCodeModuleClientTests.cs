using CSharpDB.CodeModules.Trust;

namespace CSharpDB.CodeModules.Tests;

public sealed class CSharpDbCodeModuleClientTests
{
    [Fact]
    public async Task ModuleCrud_PersistsMetadataAndStableHashes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using TestDatabaseScope db = await TestDatabaseScope.CreateAsync();
        var client = new CSharpDbCodeModuleClient(db.Client, new InMemoryCodeModuleTrustStore());

        CodeModuleDefinition saved = await client.UpsertAsync(new CodeModuleDefinition(
            "form:customers",
            "Customers",
            CodeModuleKind.Form,
            "public class Customers { }\r\n",
            OwnerKind: "Form",
            OwnerId: "customers-form",
            TypeName: "Customers"), ct);

        Assert.Equal("form:customers", saved.ModuleId);
        Assert.Equal(CodeModuleKind.Form, saved.Kind);
        Assert.NotNull(saved.SourceHash);
        Assert.Equal("public class Customers { }\n", saved.Source);

        CodeModuleDefinition? loaded = await client.GetAsync("form:customers", ct);
        Assert.NotNull(loaded);
        Assert.Equal(saved.SourceHash, loaded!.SourceHash);

        IReadOnlyList<CodeModuleSummary> modules = await client.ListAsync(ct);
        CodeModuleSummary summary = Assert.Single(modules);
        Assert.Equal("customers-form", summary.OwnerId);
    }

    [Fact]
    public async Task ExportImport_RoundTripsAndDetectsBothChangedConflict()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using TestDatabaseScope sourceDb = await TestDatabaseScope.CreateAsync("code_modules_export");
        var source = new CSharpDbCodeModuleClient(sourceDb.Client, new InMemoryCodeModuleTrustStore());
        await source.UpsertAsync(new CodeModuleDefinition(
            "module:helpers",
            "Helpers",
            CodeModuleKind.Standard,
            "public static class Helpers { public static int A() => 1; }\n"), ct);

        string workspace = Path.Combine(Path.GetTempPath(), $"csharpdb_code_ws_{Guid.NewGuid():N}");
        CodeModuleExportResult export = await source.ExportAsync(workspace, ct);
        Assert.Equal(1, export.ModuleCount);
        string exportedSourcePath = Directory.EnumerateFiles(export.WorkspaceDirectory, "*.cs", SearchOption.AllDirectories).Single();

        await source.UpsertAsync(new CodeModuleDefinition(
            "module:helpers",
            "Helpers",
            CodeModuleKind.Standard,
            "public static class Helpers { public static int A() => 2; }\n"), ct);
        await File.WriteAllTextAsync(exportedSourcePath, "public static class Helpers { public static int A() => 3; }\n", ct);

        CodeModuleImportResult conflict = await source.ImportAsync(workspace, ct);
        CodeModuleImportChange change = Assert.Single(conflict.Changes);
        Assert.Equal(CodeModuleImportChangeKind.Conflict, change.Kind);

        await using TestDatabaseScope targetDb = await TestDatabaseScope.CreateAsync("code_modules_import");
        var target = new CSharpDbCodeModuleClient(targetDb.Client, new InMemoryCodeModuleTrustStore());
        CodeModuleImportResult imported = await target.ImportAsync(workspace, ct);
        Assert.Equal(CodeModuleImportChangeKind.Added, Assert.Single(imported.Changes).Kind);
        Assert.NotNull(await target.GetAsync("module:helpers", ct));

        Directory.Delete(workspace, recursive: true);
    }

    [Fact]
    public async Task Build_ReturnsAssemblyBytesAndStructuredDiagnostics()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using TestDatabaseScope db = await TestDatabaseScope.CreateAsync();
        var client = new CSharpDbCodeModuleClient(db.Client, new InMemoryCodeModuleTrustStore());
        await client.UpsertAsync(new CodeModuleDefinition(
            "form:orders",
            "Orders",
            CodeModuleKind.Form,
            """
            using CSharpDB.CodeModules.Runtime;

            namespace TestModules;

            public sealed class OrdersModule : FormCodeModule
            {
                public void BeforeUpdate(FormBeforeEventContext context)
                {
                    Me.Status = "Ready";
                }
            }
            """,
            TypeName: "TestModules.OrdersModule"), ct);

        CodeModuleBuildResult build = await client.BuildAsync(ct);

        Assert.True(build.Succeeded);
        Assert.NotNull(build.AssemblyBytes);
        Assert.NotEmpty(build.AssemblyBytes!);

        await client.UpsertAsync(new CodeModuleDefinition(
            "form:orders",
            "Orders",
            CodeModuleKind.Form,
            "public sealed class Broken {",
            TypeName: "Broken"), ct);

        CodeModuleBuildResult failed = await client.BuildAsync(ct);
        Assert.False(failed.Succeeded);
        CodeModuleDiagnostic diagnostic = failed.Diagnostics.First(d => d.Severity == CodeModuleDiagnosticSeverity.Error);
        Assert.Equal("form:orders", diagnostic.ModuleId);
        Assert.True(diagnostic.Line > 0);
    }

    [Fact]
    public async Task Trust_IsLocalAndInvalidatesWhenSourceChanges()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using TestDatabaseScope db = await TestDatabaseScope.CreateAsync();
        var trust = new InMemoryCodeModuleTrustStore();
        var client = new CSharpDbCodeModuleClient(db.Client, trust);
        await client.UpsertAsync(new CodeModuleDefinition(
            "module:helpers",
            "Helpers",
            CodeModuleKind.Standard,
            "public static class Helpers { public static int A() => 1; }\n"), ct);

        CodeModuleBuildResult build = await client.BuildAsync(ct);
        await client.TrustAsync(ct);

        Assert.True((await client.GetTrustStateAsync(build.ModuleSetHash, ct)).IsTrusted);

        await client.UpsertAsync(new CodeModuleDefinition(
            "module:helpers",
            "Helpers",
            CodeModuleKind.Standard,
            "public static class Helpers { public static int A() => 2; }\n"), ct);

        Assert.False((await client.GetTrustStateAsync(ct: ct)).IsTrusted);
    }
}
