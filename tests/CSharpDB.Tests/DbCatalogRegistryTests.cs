using CSharpDB.Engine;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class DbCatalogRegistryTests
{
    [Fact]
    public void InternalTableRegistry_RecognizesOnlyRegisteredNamesAndPrefixes()
    {
        Assert.NotEmpty(DbInternalTableRegistry.Descriptors);
        Assert.DoesNotContain(
            DbInternalTableRegistry.Descriptors,
            static descriptor => descriptor.Pattern == "_");

        Assert.True(DbInternalTableRegistry.TryGet("__data_model_diagrams", out var diagrams));
        Assert.Equal("Data Modeler", diagrams.Owner);

        Assert.False(DbInternalTableRegistry.TryGet("__unregistered_feature_data", out _));
        Assert.False(DbInternalTableRegistry.IsInternalTable("_unknown"));
        Assert.False(DbInternalTableRegistry.IsInternalTable("__unknown"));
        Assert.False(DbInternalTableRegistry.IsInternalTable("customers"));
        Assert.False(DbInternalTableRegistry.IsInternalTable(null));

        Assert.True(DbInternalTableRegistry.IsReservedInternalTableName("__unknown"));
        Assert.True(DbInternalTableRegistry.IsReservedInternalTableName("__data_model_diagrams"));
        Assert.False(DbInternalTableRegistry.IsReservedInternalTableName("_unknown"));
        Assert.False(DbInternalTableRegistry.IsReservedInternalTableName("customers"));
        Assert.False(DbInternalTableRegistry.IsReservedInternalTableName(null));
    }

    [Theory]
    [InlineData("_col_orders", "Collections")]
    [InlineData("__procedures", "Stored Procedures")]
    [InlineData("__saved_queries", "Saved Queries")]
    [InlineData("__external_tables", "External Tables")]
    [InlineData("__data_model_diagrams", "Data Modeler")]
    [InlineData("__documentation_annotations", "Database Documenter")]
    [InlineData("__validation_rules", "Data Hygiene")]
    [InlineData("__forms", "Admin Forms")]
    [InlineData("__reports", "Admin Reports")]
    [InlineData("__report_definition_chunks", "Admin Reports")]
    [InlineData("__code_modules", "Code Modules")]
    [InlineData("__code_module_builds", "Code Modules")]
    [InlineData("_etl_pipeline_versions", "ETL Pipelines")]
    [InlineData("_shard_migration_checkpoints", "Sharding")]
    [InlineData("__migrate_orders_a1b2c3d4", "Foreign Key Migration")]
    [InlineData("__csharpdb_restore_stage_v1_abc123", "Archive Restore")]
    [InlineData("__csharpdb_migration_receipts", "CSharpDB Migration")]
    [InlineData("__EFMigrationsHistory", "Entity Framework Core")]
    [InlineData("__efmigrationslock", "Entity Framework Core")]
    public void InternalTableRegistry_MapsKnownFeatureStorage(string tableName, string expectedOwner)
    {
        Assert.True(DbInternalTableRegistry.TryGet(tableName, out var descriptor));
        Assert.Equal(expectedOwner, descriptor.Owner);
    }

    [Theory]
    [InlineData("_col_orders", true, false)]
    [InlineData("__procedures", true, false)]
    [InlineData("__saved_queries", true, false)]
    [InlineData("__external_tables", true, true)]
    [InlineData("__data_model_diagrams", true, true)]
    [InlineData("__documentation_annotations", true, true)]
    [InlineData("__validation_rules", false, true)]
    [InlineData("__forms", false, false)]
    [InlineData("_etl_runs", false, false)]
    [InlineData("__migrate_orders_a1b2c3d4", false, false)]
    [InlineData("__unknown_internal_table", false, false)]
    public void InternalTableRegistry_PreservesExistingVisibilityPolicies(
        string tableName,
        bool hiddenFromClientMetadata,
        bool hiddenFromSystemCatalog)
    {
        Assert.Equal(
            hiddenFromClientMetadata,
            DbInternalTableRegistry.IsHiddenFromClientMetadata(tableName));
        Assert.Equal(
            hiddenFromSystemCatalog,
            DbInternalTableRegistry.IsHiddenFromSystemCatalog(tableName));
    }

    [Theory]
    [InlineData("_col_orders", "Collections")]
    [InlineData("__procedures", "Procedures")]
    [InlineData("__saved_queries", "sys.saved_queries")]
    [InlineData("__external_tables", "sys.external_tables")]
    [InlineData("__data_model_diagrams", "sys.diagrams")]
    [InlineData("__validation_rules", "sys.validation_rules")]
    public void InternalTableRegistry_MapsLogicalReplacements(
        string tableName,
        string expectedReplacement)
    {
        Assert.True(DbInternalTableRegistry.TryGet(tableName, out var descriptor));
        Assert.Equal(expectedReplacement, descriptor.LogicalReplacement);
    }

    [Fact]
    public void InternalTableRegistry_ResolvesCollectionNamesToLogicalCollections()
    {
        Assert.Equal(
            "collection:scanner_sessions",
            DbInternalTableRegistry.ResolveLogicalReplacement("_col_scanner_sessions"));
        Assert.Equal(
            "sys.diagrams",
            DbInternalTableRegistry.ResolveLogicalReplacement("__data_model_diagrams"));
        Assert.Null(DbInternalTableRegistry.ResolveLogicalReplacement("__unknown_internal_table"));
        Assert.Null(DbInternalTableRegistry.ResolveLogicalReplacement("customers"));
    }

    [Fact]
    public void SystemCatalogRegistry_ContainsUniqueCanonicalCatalogsAndAliases()
    {
        Assert.Equal(22, DbSystemCatalogRegistry.Descriptors.Count);
        Assert.Equal(22, DbSystemCatalogRegistry.CanonicalNames.Count);
        Assert.Equal(22, DbSystemCatalogRegistry.UnderscoredAliases.Count);

        Assert.Equal(
            DbSystemCatalogRegistry.Descriptors.Count,
            DbSystemCatalogRegistry.Descriptors
                .Select(static descriptor => descriptor.Name)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .Count());

        Assert.Contains(
            DbSystemCatalogRegistry.Descriptors,
            static descriptor => descriptor.Name == "sys.internal_tables"
                                 && descriptor.DefaultSql == "SELECT * FROM sys.internal_tables ORDER BY table_name;");
    }

    [Fact]
    public void SystemCatalogRegistry_NormalizesCanonicalNamesAndUnderscoredAliases()
    {
        foreach (DbSystemCatalogDescriptor descriptor in DbSystemCatalogRegistry.Descriptors)
        {
            Assert.True(DbSystemCatalogRegistry.TryNormalize(descriptor.Name, out string canonical));
            Assert.Equal(descriptor.Name, canonical);

            string alias = DbSystemCatalogRegistry.GetUnderscoredAlias(descriptor.Name);
            Assert.True(DbSystemCatalogRegistry.TryNormalize(alias.ToUpperInvariant(), out string fromAlias));
            Assert.Equal(descriptor.Name, fromAlias);

            Assert.True(DbSystemCatalogRegistry.TryGet(alias, out var resolved));
            Assert.Same(descriptor, resolved);
        }

        Assert.False(DbSystemCatalogRegistry.TryNormalize("sys.unknown", out string unknown));
        Assert.Equal(string.Empty, unknown);
        Assert.False(DbSystemCatalogRegistry.TryNormalize(null, out _));
    }

    [Fact]
    public void ReaderSession_SystemCatalogDetectionMatchesRegistry()
    {
        foreach (DbSystemCatalogDescriptor descriptor in DbSystemCatalogRegistry.Descriptors)
        {
            Assert.True(Database.ReaderSession.IsSystemCatalogTable(descriptor.Name));
            Assert.True(Database.ReaderSession.IsSystemCatalogTable(descriptor.Name.ToUpperInvariant()));

            string alias = DbSystemCatalogRegistry.GetUnderscoredAlias(descriptor.Name);
            Assert.True(Database.ReaderSession.IsSystemCatalogTable(alias));
            Assert.True(Database.ReaderSession.IsSystemCatalogTable(alias.ToUpperInvariant()));
        }

        Assert.False(Database.ReaderSession.IsSystemCatalogTable("sys.unknown"));
        Assert.False(Database.ReaderSession.IsSystemCatalogTable("application_table"));
    }

    [Theory]
    [InlineData("sys.foreign_keys")]
    [InlineData("sys.key_constraints")]
    [InlineData("sys.check_constraints")]
    [InlineData("sys.functions")]
    [InlineData("sys.temp_tables")]
    [InlineData("sys.temp_columns")]
    [InlineData("sys.internal_tables")]
    public void SystemCatalogRegistry_IncludesRelationshipAndDiagnosticCatalogs(string catalogName)
    {
        Assert.True(DbSystemCatalogRegistry.TryGet(catalogName, out var descriptor));
        Assert.Equal(catalogName, descriptor.Name);
        Assert.Contains(catalogName, descriptor.DefaultSql, StringComparison.OrdinalIgnoreCase);
    }
}
