using System.Text.Json;
using CSharpDB.Client.Models;

namespace CSharpDB.DevOps.Tests;

public sealed class DataModelDefinitionAnalysisTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private static DefinitionCatalogRecord Table(string name, params string[] columns) => DefinitionExplorerTests.Table(name, columns);
    private static DefinitionCatalogRecord Native(string kind, string name, object source, string format, string? owner = null)
        => DefinitionExplorerTests.Definition(kind, name, JsonSerializer.Serialize(source), format, owner);
    private static DefinitionCatalogRecord Model(object source, string name = "Sales")
        => Native("Data model", name, source, "dataModel") with { Id = "DataModel:" + name };
    private static DefinitionCatalogRecord Change(string id, object source, string model = "Sales")
        => Native("Proposed change", id, source, "modelChange", model) with
        { Id = "ModelChange:" + model + ":" + id, MetadataJson = JsonSerializer.Serialize(new { modelId = "DataModel:" + model, modelName = model }) };
    private static DefinitionAnalysis Analyze(params DefinitionCatalogRecord[] definitions)
        => new DefinitionDependencyService().Analyze(new("v1", DateTimeOffset.UtcNow, definitions, []), Ct);

    [Fact]
    public void SavedMembership_IsDiscoverableInBothDirections_AndNeverAnEngineRestriction()
    {
        var model = Model(new { Nodes = new[] { new { Name = "Customers", Columns = new[] { new { Name = "Id" }, new { Name = "Name" } } } } });
        var analysis = Analyze(Table("Customers", "Id", "Name"), model);
        Assert.Empty(analysis.Diagnostics);
        Assert.Contains(analysis.Dependencies, e => e.SourceId == model.Id && e.TargetId == "Table:Customers" && e.TargetColumn == "Id"
            && e.Relationship == DependencyRelationshipKind.ModelMembership && e.Location.Contains("columns[0]"));
        Assert.Contains(analysis.Dependencies.Where(e => e.TargetId == "Table:Customers"), e => e.SourceId == model.Id);
        foreach (var change in Enum.GetValues<ColumnChangeKind>())
        {
            var report = ColumnImpactService.Assess(analysis, "Customers", "Id", change, Ct);
            Assert.Contains(report.Findings, f => f.ObjectId == model.Id && f.Category == ColumnImpactCategory.ModelMembership);
            Assert.DoesNotContain(report.Findings, f => f.ObjectId == model.Id && f.Category is ColumnImpactCategory.ConfirmedUsage or ColumnImpactCategory.IndirectEffects or ColumnImpactCategory.EngineRestriction);
        }
    }

    [Fact]
    public void NonExecutableMembership_DoesNotExtendExecutableLineage()
    {
        var model = Model(new { Nodes = new[] { new { Name = "Customers", Columns = new[] { new { Name = "Id" } } } } });
        var analysis = Analyze(Table("Customers", "Id", "Name"), model);
        var unrelated = DefinitionExplorerTests.Definition("View", "NotExecutable", "SELECT 1");
        analysis = analysis with
        {
            Catalog = analysis.Catalog with { Definitions = [.. analysis.Catalog.Definitions, unrelated] },
            Dependencies = [.. analysis.Dependencies, new(unrelated.Id, model.Id, null, null, "Read", "definition")],
        };
        var report = ColumnImpactService.Assess(analysis, "Customers", "Id", ColumnChangeKind.Drop, Ct);
        Assert.DoesNotContain(report.Findings, f => f.ObjectId == unrelated.Id);
    }

    [Fact]
    public void StableIdentities_MapSavedAndStagedLabelsToPersistedNames()
    {
        Guid tableId = Guid.NewGuid(), columnId = Guid.NewGuid();
        var table = Native("Table", "CurrentCustomers", new TableSchema
        {
            SchemaId = tableId, TableName = "CurrentCustomers",
            Columns = [new() { SchemaId = columnId, Name = "CustomerId", Type = DbType.Integer }, new() { Name = "Id", Type = DbType.Integer }],
        }, "table");
        var operation = new { Id = "rename", Kind = "RenameColumn", TableName = "OldCustomers", ColumnName = "Id", NewColumnName = "PersonId" };
        var model = Model(new
        {
            Nodes = new[] { new { SchemaId = tableId, Name = "OldCustomers", Columns = new[] { new { SchemaId = columnId, Name = "Id" } } } },
            PendingOperations = new[] { operation },
        });
        var analysis = Analyze(table, model, Change("rename", operation));
        Assert.Empty(analysis.Diagnostics);
        Assert.Contains(analysis.Dependencies, e => e.SourceId == model.Id && e.TargetId == table.Id && e.TargetColumn == "CustomerId");
        Assert.Contains(analysis.Dependencies, e => e.SourceId == "ModelChange:Sales:rename" && e.TargetColumn == "CustomerId" && e.Relationship == DependencyRelationshipKind.ProposedChange);
        Assert.DoesNotContain(analysis.Dependencies, e => e.TargetColumn == "Id");
    }

    [Fact]
    public void RecreatedSameNamedObjects_DoNotOverrideSavedIdentities()
    {
        var missingTable = Model(new { Nodes = new[] { new { Name = "Customers", SchemaId = Guid.NewGuid(), Columns = new[] { new { Name = "Id" } } } } });
        var missingColumn = Model(new { Nodes = new[] { new { Name = "Customers", Columns = new[] { new { Name = "Id", SchemaId = Guid.NewGuid() } } } } }, "Other");
        var analysis = Analyze(Table("Customers", "Id", "Name"), missingTable, missingColumn);
        Assert.DoesNotContain(analysis.Dependencies, e => e.SourceId == missingTable.Id);
        Assert.DoesNotContain(analysis.Dependencies, e => e.SourceId == missingColumn.Id && e.TargetColumn == "Id");
        Assert.Equal(2, analysis.Diagnostics.Count);
        Assert.False(ColumnImpactService.Assess(analysis, "Customers", "Id", ColumnChangeKind.Rename, Ct).CoverageComplete);
    }

    [Fact]
    public void OrderedDraftChanges_ResolveRenamesAndNewColumnsWithoutInventingLiveUsage()
    {
        object[] operations =
        [
            new { Id = "table", Kind = 2, TableName = "Customers", NewTableName = "People" },
            new { Id = "column", Kind = 5, TableName = "People", ColumnName = "Id", NewColumnName = "PersonId" },
            new { Id = "add", Kind = 3, TableName = "People", ColumnName = "Label" },
            new { Id = "check", Kind = 17, TableName = "People", ExpressionSql = "PersonId > 0 AND Label IS NOT NULL" },
        ];
        var model = Model(new { Nodes = new[] { new { Name = "Customers", Columns = new[] { new { Name = "Id" } } } }, PendingOperations = operations });
        var analysis = Analyze([Table("Customers", "Id", "Name"), model, .. operations.Select((op, i) => Change(new[] { "table", "column", "add", "check" }[i], op))]);
        Assert.Empty(analysis.Diagnostics);
        var checkEdges = analysis.Dependencies.Where(e => e.SourceId == "ModelChange:Sales:check").ToArray();
        Assert.Contains(checkEdges, e => e.TargetColumn == "Id");
        Assert.Contains(checkEdges, e => e.TargetColumn == "Label");
        Assert.All(checkEdges, e => Assert.Equal(DependencyRelationshipKind.ProposedChange, e.Relationship));
        Assert.Contains(ColumnImpactService.Assess(analysis, "Customers", "Id", ColumnChangeKind.Drop, Ct).Findings,
            f => f.ObjectId == "ModelChange:Sales:check" && f.Category == ColumnImpactCategory.ProposedChange);
    }

    [Fact]
    public void ProposedNewTable_WithSameNameAsLiveTable_IsNeverBoundToLiveObject()
    {
        var create = new { Id = "create", Kind = "CreateTable", TableName = "Customers", Columns = new[] { new { Name = "Id" } } };
        var model = Model(new
        {
            Nodes = new[] { new { Name = "Customers", IsDraft = true, Columns = new[] { new { Name = "Id" } } } },
            PendingOperations = new[] { create },
        });
        var analysis = Analyze(Table("Customers", "Id", "Name"), model, Change("create", create));
        Assert.Empty(analysis.Diagnostics);
        Assert.Contains(analysis.Dependencies, e => e.SourceId == model.Id && e.TargetId == "ModelChange:Sales:create" && e.Relationship == DependencyRelationshipKind.ProposedChange);
        Assert.DoesNotContain(analysis.Dependencies, e => e.TargetId == "Table:Customers");
        Assert.Empty(ColumnImpactService.Assess(analysis, "Customers", "Id", ColumnChangeKind.Drop, Ct).Findings);
    }

    [Fact]
    public void ProposedRelationships_RecordBothEndpointsIncludingCompositeColumns()
    {
        var model = Model(new
        {
            Nodes = new[]
            {
                new { Name = "Orders", Columns = new[] { new { Name = "CustomerId" }, new { Name = "Region" } } },
                new { Name = "Customers", Columns = new[] { new { Name = "Id" }, new { Name = "Region" } } },
            },
            Relationships = new[] { new { Kind = "Draft", LeftTable = "Orders", RightTable = "Customers", ColumnPairs = new[]
                { new { ChildColumn = "CustomerId", ParentColumn = "Id" }, new { ChildColumn = "Region", ParentColumn = "Region" } } } },
        });
        var analysis = Analyze(Table("Orders", "CustomerId", "Region"), Table("Customers", "Id", "Region"), model);
        Assert.Empty(analysis.Diagnostics);
        Assert.Equal(4, analysis.Dependencies.Count(e => e.Relationship == DependencyRelationshipKind.ProposedChange));
        Assert.Contains(ColumnImpactService.Assess(analysis, "Customers", "Region", ColumnChangeKind.Type, Ct).Findings,
            f => f.ObjectId == model.Id && f.Category == ColumnImpactCategory.ProposedChange);
    }

    [Fact]
    public void ColumnSpecificProposal_DoesNotAffectUnrelatedColumns()
    {
        var operation = new { Id = "rename", Kind = "RenameColumn", TableName = "Orders", ColumnName = "Amount", NewColumnName = "Total" };
        var model = Model(new { Nodes = new[] { new { Name = "Orders", Columns = new[] { new { Name = "Id" }, new { Name = "Amount" } } } }, PendingOperations = new[] { operation } });
        var change = Change("rename", operation);
        var analysis = Analyze(Table("Orders", "Id", "Amount"), model, change);
        Assert.Empty(analysis.Diagnostics);
        foreach (var kind in Enum.GetValues<ColumnChangeKind>())
        {
            Assert.DoesNotContain(ColumnImpactService.Assess(analysis, "Orders", "Id", kind, Ct).Findings, f => f.ObjectId == change.Id);
            Assert.Contains(ColumnImpactService.Assess(analysis, "Orders", "Amount", kind, Ct).Findings, f => f.ObjectId == change.Id && f.Category == ColumnImpactCategory.ProposedChange);
            Assert.Contains(ColumnImpactService.Assess(analysis, "Orders", "Id", kind, Ct).Findings, f => f.ObjectId == model.Id && f.Category == ColumnImpactCategory.ModelMembership);
        }
    }

    [Theory]
    [InlineData("RenameTable")]
    [InlineData("DropTable")]
    public void TableWideProposal_IsRelevantToEveryColumn(string kind)
    {
        var operation = new { Id = "table", Kind = kind, TableName = "Orders", NewTableName = "History" };
        var model = Model(new { Nodes = new[] { new { Name = "Orders", Columns = new[] { new { Name = "Id" }, new { Name = "Amount" } } } }, PendingOperations = new[] { operation } });
        var change = Change("table", operation);
        var analysis = Analyze(Table("Orders", "Id", "Amount"), model, change);
        foreach (string column in new[] { "Id", "Amount" })
            Assert.Contains(ColumnImpactService.Assess(analysis, "Orders", column, ColumnChangeKind.Drop, Ct).Findings, f => f.ObjectId == change.Id && f.Category == ColumnImpactCategory.ProposedChange);
    }

    private static DefinitionCatalogRecord External(string name, params string[] columns)
        => Table(name, columns) with { Id = "ExternalTable:" + name, Kind = "External table", Format = "externalTable" };
    private static DefinitionCatalogRecord ArchiveRelationship(params string[] candidates)
        => Native("Archive relationship", "fk", new { columnNames = new[] { "CustomerId" }, referencedTableName = "Customers", referencedColumnNames = new[] { "Id" } }, "archiveForeignKey", "ArchiveOrders")
            with { MetadataJson = JsonSerializer.Serialize(new { archivePath = "orders.cta", referencedTableCandidates = candidates }, Json) };

    [Fact]
    public void ArchiveRelationships_ResolveExternalAliasesAndRemainSeparateFromLiveRestrictions()
    {
        var foreignKey = ArchiveRelationship("ArchiveCustomers");
        var sql = DefinitionExplorerTests.Definition("Saved query", "History", "SELECT CustomerId FROM ArchiveOrders");
        var analysis = Analyze(Table("Customers", "Id", "Name"), External("ArchiveOrders", "CustomerId"), External("ArchiveCustomers", "Id"), foreignKey, sql);
        Assert.Empty(analysis.Diagnostics);
        Assert.Contains(analysis.Dependencies, e => e.SourceId == foreignKey.Id && e.TargetId == "ExternalTable:ArchiveCustomers" && e.TargetColumn == "Id");
        Assert.All(analysis.Dependencies.Where(e => e.SourceId == foreignKey.Id), e => Assert.Equal(DependencyRelationshipKind.ExternalArchive, e.Relationship));
        Assert.All(analysis.Dependencies.Where(e => e.SourceId == sql.Id), e => Assert.Equal(DependencyRelationshipKind.Usage, e.Relationship));
        Assert.Empty(ColumnImpactService.Assess(analysis, "Customers", "Id", ColumnChangeKind.Drop, Ct).Findings);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void MissingOrAmbiguousArchiveTargets_NeverBindToSameNamedLocalTables(bool ambiguous)
    {
        var foreignKey = ambiguous ? ArchiveRelationship("ArchiveCustomers", "OtherArchive") : ArchiveRelationship();
        var analysis = Analyze(Table("Customers", "Id", "Name"), External("ArchiveOrders", "CustomerId"), External("ArchiveCustomers", "Id"), External("OtherArchive", "Id"), foreignKey);
        Assert.Contains(analysis.Diagnostics, d => d.Message.Contains("not bound to a local table"));
        Assert.Single(analysis.Dependencies);
        Assert.Equal("ExternalTable:ArchiveOrders", analysis.Dependencies[0].TargetId);
    }

    [Fact]
    public void MalformedSavedModel_DoesNotHideHealthySqlOrOtherModels()
    {
        var healthy = Model(new { Nodes = new[] { new { Name = "Customers", Columns = new[] { new { Name = "Id" } } } } });
        var broken = healthy with { Id = "DataModel:broken", Source = "{ broken" };
        var query = DefinitionExplorerTests.Definition("Saved query", "Active", "SELECT Id FROM Customers");
        var analysis = Analyze(Table("Customers", "Id", "Name"), broken, healthy, query);
        Assert.Contains(analysis.Diagnostics, d => d.ObjectId == broken.Id);
        Assert.Contains(analysis.Dependencies, e => e.SourceId == query.Id && e.Relationship == DependencyRelationshipKind.Usage);
        Assert.Contains(analysis.Dependencies, e => e.SourceId == healthy.Id && e.Relationship == DependencyRelationshipKind.ModelMembership);
    }

    [Fact]
    public void ChangeCatalogOrder_DoesNotChangeAnalysis()
    {
        var operation = new { Id = "rename", Kind = "RenameColumn", TableName = "Customers", ColumnName = "Id", NewColumnName = "CustomerId" };
        var model = Model(new { Nodes = new[] { new { Name = "Customers", Columns = new[] { new { Name = "Id" } } } }, PendingOperations = new[] { operation } });
        var change = Change("rename", operation);
        var analysis = Analyze(change, model, Table("Customers", "Id", "Name"));
        Assert.Empty(analysis.Diagnostics);
        Assert.Contains(analysis.Dependencies, e => e.SourceId == change.Id && e.TargetColumn == "Id" && e.Relationship == DependencyRelationshipKind.ProposedChange);
        var containment = Assert.Single(analysis.Dependencies, e => e.SourceId == model.Id && e.TargetId == change.Id);
        Assert.Equal(DependencyRelationshipKind.ProposedChange, containment.Relationship);
        Assert.Equal("Saved proposed change", containment.Usage);
        Assert.Equal("$.pendingOperations[0]", containment.Location);
        Assert.Contains(analysis.Dependencies.Where(e => e.TargetId == change.Id), e => e.SourceId == model.Id);
    }
}
