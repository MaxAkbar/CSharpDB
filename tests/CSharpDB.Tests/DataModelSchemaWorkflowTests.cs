using System.Reflection;
using CSharpDB.Admin.Helpers;
using CSharpDB.Admin.Models;
using CSharpDB.Admin.Services;
using CSharpDB.Client;
using CSharpDB.Client.Models;

namespace CSharpDB.Tests;

public sealed class DataModelSchemaWorkflowTests : IAsyncLifetime
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"modeler_schema_{Guid.NewGuid():N}.db");
    private ICSharpDbClient _client = null!;
    private DataModelService _service = null!;
    private CancellationToken Ct => TestContext.Current.CancellationToken;
    public async ValueTask InitializeAsync()
    {
        _client = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = _path }); _service = new(_client);
        await Sql("CREATE TABLE parents (tenant INTEGER NOT NULL, id INTEGER NOT NULL, label VARCHAR(80), CONSTRAINT pk_parent PRIMARY KEY (tenant,id));");
        await Sql("CREATE TABLE children (id INTEGER PRIMARY KEY, tenant INTEGER, parent_id INTEGER, amount DECIMAL(12,2) DEFAULT 0, code TEXT, CONSTRAINT fk_parent FOREIGN KEY (tenant,parent_id) REFERENCES parents(tenant,id), CONSTRAINT ck_amount CHECK (amount >= 0));");
    }
    public async ValueTask DisposeAsync()
    {
        await _client.DisposeAsync(); if (File.Exists(_path)) File.Delete(_path); if (File.Exists(_path + ".wal")) File.Delete(_path + ".wal");
    }
    private async Task Sql(string sql) => Assert.Null((await _client.ExecuteSqlAsync(sql, Ct)).Error);
    private Task<DataModelState> Model() => _service.BuildSelectionAsync(["parents", "children"], ct: Ct);
    private static DataModelPendingOperation Op(DataModelPendingOperationKind kind, string column = "code") => new() { Kind = kind, TableName = "children", ColumnName = column };
    private static DataModelPendingOperation DefaultOperation(string sql) => new() { Kind = DataModelPendingOperationKind.SetDefault, TableName = "children", ColumnName = "code", ExpressionSql = sql };

    [Fact]
    public async Task CompositeMetadataIncludesOrderedMappingsConstraintsAndTypes()
    {
        var state = await Model(); var parent = state.Nodes.Single(node => node.Name == "parents"); var child = state.Nodes.Single(node => node.Name == "children");
        var relationship = Assert.Single(state.Relationships);
        Assert.Equal(["tenant", "parent_id"], relationship.ColumnPairs.Select(pair => pair.ChildColumn));
        Assert.Equal(["tenant", "id"], relationship.ColumnPairs.Select(pair => pair.ParentColumn));
        Assert.Equal(DataModelCardinality.ZeroOrOne, relationship.ReferencedEndCardinality);
        Assert.Equal(DataModelCardinality.ZeroOrMany, relationship.ReferencingEndCardinality);
        Assert.All(parent.Columns.Where(column => column.IsPrimaryKey), column => Assert.False(column.IsUnique));
        Assert.Equal("DECIMAL(12,2)", child.Columns.Single(column => column.Name == "amount").TypeLabel);
        Assert.Single(child.Checks); Assert.Single(parent.Keys); Assert.NotEqual(Guid.Empty, relationship.SchemaId);
        Assert.Contains(child.Indexes, index => index.IsEngineManaged);
        Assert.Contains("parent_id", DataModelCanvasMetrics.GetVisibleColumns(state, child).Select(column => column.Name));
        Assert.Equal(2, _service.ToQueryDesignerState(state).Joins.Count);
        string preview = _service.BuildPreviewSql(state);
        Assert.Contains("PRIMARY KEY (tenant, id)", preview); Assert.Contains("FOREIGN KEY (tenant, parent_id)", preview); Assert.Contains("CHECK", preview);
    }

    [Fact]
    public async Task CompositeUniquenessUsesWholeKeyAndDistinctConstraintsGetDistinctConnectors()
    {
        await Sql("CREATE UNIQUE INDEX uq_child_pair ON children(tenant,parent_id);");
        await Sql("ALTER TABLE children ADD CONSTRAINT fk_parent_again FOREIGN KEY (tenant,parent_id) REFERENCES parents(tenant,id);");
        var state = await Model();
        Assert.Equal(2, state.Relationships.Select(r => r.Id).Distinct().Count());
        Assert.All(state.Relationships, r => Assert.Equal(DataModelCardinality.ZeroOrOne, r.ReferencingEndCardinality));
    }

    [Fact]
    public async Task FailureAfterFirstStepRollsBackEverythingAndKeepsPendingIntent()
    {
        await Sql("INSERT INTO children (id,code) VALUES (1,NULL);");
        var state = await Model();
        state.PendingOperations.Add(new() { Kind = DataModelPendingOperationKind.CreateIndex, TableName = "children", IndexName = "idx_rollback", ColumnNames = ["code"] });
        state.PendingOperations.Add(Op(DataModelPendingOperationKind.SetNotNull));
        var plan = await _service.ReviewChangesAsync(state, Ct); Assert.True(plan.CanApply, string.Join(";", plan.Errors));
        await Assert.ThrowsAsync<CSharpDB.Primitives.CSharpDbException>(() => _service.ApplyReviewedChangesAsync(state, plan, Ct));
        Assert.DoesNotContain(await _client.GetIndexesAsync(Ct), index => index.IndexName == "idx_rollback");
        Assert.True((await _client.GetTableSchemaAsync("children", Ct))!.Columns.Single(column => column.Name == "code").Nullable);
        Assert.Equal(2, state.PendingOperations.Count);
    }

    [Fact]
    public async Task ReviewIsReadOnlyAndStaleOrChangedPlansAreRejected()
    {
        var state = await Model(); state.PendingOperations.Add(DefaultOperation("'new'"));
        var plan = await _service.ReviewChangesAsync(state, Ct); Assert.True(plan.CanApply);
        Assert.Null((await _client.GetTableSchemaAsync("children", Ct))!.Columns.Single(column => column.Name == "code").DefaultSql);
        state.PendingOperations[0].ExpressionSql = "'other'";
        await Assert.ThrowsAsync<InvalidOperationException>(() => _service.ApplyReviewedChangesAsync(state, plan, Ct));
        state.PendingOperations[0].ExpressionSql = "'new'";
        await Sql("ALTER TABLE parents ADD COLUMN changed TEXT;");
        await Assert.ThrowsAsync<InvalidOperationException>(() => _service.ApplyReviewedChangesAsync(state, plan, Ct));
        Assert.Single(state.PendingOperations);
    }

    [Fact]
    public async Task NativeCompositeForeignKeyAndIndexChangesApplyAtomically()
    {
        var state = await Model();
        state.PendingOperations.Add(new() { Kind = DataModelPendingOperationKind.DropForeignKey, TableName = "children", ConstraintName = "fk_parent" });
        state.PendingOperations.Add(new() { Kind = DataModelPendingOperationKind.AddForeignKey, TableName = "children", ConstraintName = "fk_replaced", ColumnNames = ["tenant", "parent_id"], ReferencedTableName = "parents", ReferencedColumnNames = ["tenant", "id"], OnDelete = "SET NULL" });
        state.PendingOperations.Add(new() { Kind = DataModelPendingOperationKind.CreateIndex, TableName = "children", IndexName = "idx_code", ColumnNames = ["code"], ColumnCollations = ["NOCASE"] });
        var plan = await _service.ReviewChangesAsync(state, Ct); Assert.True(plan.CanApply, string.Join(";", plan.Errors));
        Assert.DoesNotContain(plan.Steps, step => step.Sql.StartsWith("--"));
        Assert.True((await _service.ApplyReviewedChangesAsync(state, plan, Ct)).Succeeded);
        Assert.Equal("fk_replaced", Assert.Single((await _client.GetTableSchemaAsync("children", Ct))!.ForeignKeys).ConstraintName);
        Assert.Empty(state.PendingOperations);
    }

    [Fact]
    public async Task StableIdentitiesPreserveMembershipAndRoutesAcrossExternalRename()
    {
        var state = await Model(); var group = DataModelGroups.Create(state, ["parents", "children"])!;
        state.Nodes[0].X = 731; state.Relationships[0].ConnectorLayout = new() { Waypoints = [new() { X = 600, Y = 180 }] };
        Guid identity = state.Nodes[0].SchemaId; string oldName = state.Nodes[0].Name;
        await _service.SaveDiagramAsync("Identity", state, Ct);
        await Sql($"ALTER TABLE {oldName} RENAME TO renamed;");
        var loaded = (await _service.LoadDiagramAsync("Identity", Ct))!;
        var renamed = loaded.Nodes.Single(node => node.SchemaId == identity);
        Assert.Equal("renamed", renamed.Name); Assert.Equal(731, renamed.X); Assert.Equal(group.Id, renamed.GroupId);
        Assert.NotNull(Assert.Single(loaded.Relationships).ConnectorLayout);
    }

    [Fact]
    public async Task DiagramSaveFailureReportsCommittedSchemaAndBlocksSavedPlanReplay()
    {
        var state = await Model(); state.PendingOperations.Add(DefaultOperation("'new'"));
        await _service.SaveDiagramAsync("Save failure", state, Ct);
        var wrapper = DispatchProxy.Create<ICSharpDbClient, ClientProxy>();
        var proxy = (ClientProxy)(object)wrapper; proxy.Inner = _client;
        proxy.FailSave = true;
        var service = new DataModelService(wrapper);
        var result = await service.ApplyReviewedChangesAsync(state, await service.ReviewChangesAsync(state, Ct), Ct);
        Assert.True(result.Succeeded); Assert.False(result.DiagramSaved); Assert.Empty(state.PendingOperations);
        Assert.Equal("'new'", (await _client.GetTableSchemaAsync("children", Ct))!.Columns.Single(column => column.Name == "code").DefaultSql);
        var oldSaved = (await _service.LoadDiagramAsync("Save failure", Ct))!;
        Assert.False((await _service.ReviewChangesAsync(oldSaved, Ct)).CanApply);
    }

    [Fact]
    public async Task UnsupportedTransactionsDisableReviewWithoutApplyingAnything()
    {
        var state = await Model(); state.PendingOperations.Add(Op(DataModelPendingOperationKind.SetNotNull));
        var wrapper = DispatchProxy.Create<ICSharpDbClient, ClientProxy>();
        var proxy = (ClientProxy)(object)wrapper; proxy.Inner = _client; proxy.NoTransactions = true;
        var review = await new DataModelService(wrapper).ReviewChangesAsync(state, Ct);
        Assert.False(review.CanApply); Assert.Contains(review.Errors, error => error.Contains("unavailable"));
    }

    [Fact]
    public async Task ExplicitChecksCanRunAndCancelWithoutChangingSchema()
    {
        var state = await Model(); string baseline = state.SchemaFingerprint!;
        Assert.All(await _service.CheckDataAsync(state, "children", Ct), check => Assert.Equal(0, check.Violations));
        using var cancelled = new CancellationTokenSource(); cancelled.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => _service.CheckDataAsync(state, "children", cancelled.Token));
        Assert.Equal(baseline, (await Model()).SchemaFingerprint);
    }

    [Fact]
    public async Task VisualHistoryDoesNotChangeSqlAndRoundTripsRichMetadata()
    {
        var state = await Model(); var history = new DataModelHistory(); history.Reset(state);
        string sql = _service.BuildPreviewSql(state);
        DataModelGroups.Create(state, ["parents", "children"]); history.Record(state);
        DataModelGroups.Move(state, state.Groups[0].Id, 100, 50); history.Record(state);
        var beforeMove = history.Undo()!; Assert.Equal(sql, _service.BuildPreviewSql(beforeMove)); Assert.Single(beforeMove.Groups);
        var original = history.Undo()!; Assert.Empty(original.Groups); Assert.NotEmpty(original.Nodes.Single(n => n.Name == "children").Checks);
        Assert.Single(history.Redo()!.Groups); history.Reset(state); Assert.False(history.CanUndo); Assert.False(history.CanRedo);
    }

    [Fact]
    public async Task ColumnKeysChecksAndIndexEditorsGenerateExecutableNativeSql()
    {
        await Sql("CREATE TABLE edits (a INTEGER, b TEXT, c DECIMAL(12,2), d BLOB);");
        async Task Apply(params DataModelPendingOperation[] operations)
        {
            var state = await _service.BuildSelectionAsync(["edits"], ct: Ct);
            foreach (var operation in operations) { operation.TableName = "edits"; state.PendingOperations.Add(operation); }
            var plan = await _service.ReviewChangesAsync(state, Ct);
            Assert.True(plan.CanApply, string.Join(";", plan.Errors));
            Assert.True((await _service.ApplyReviewedChangesAsync(state, plan, Ct)).Succeeded);
        }
        await Apply(new DataModelPendingOperation { Kind = DataModelPendingOperationKind.AddColumn, ColumnName = "label", ColumnType = "VARCHAR(50)", ExpressionSql = "'new'", Collation = "NOCASE", ConstraintName = "ck_label", CheckExpressionSql = "label <> ''" });
        var schema = (await _client.GetTableSchemaAsync("edits", Ct))!;
        Assert.Equal("label", schema.CheckConstraints.Single(check => check.ConstraintName == "ck_label").ColumnName);
        await Apply(new DataModelPendingOperation { Kind = DataModelPendingOperationKind.DropConstraint, ConstraintName = "ck_label" });
        await Apply(new DataModelPendingOperation { Kind = DataModelPendingOperationKind.AlterColumnType, ColumnName = "label", ColumnType = "VARCHAR(100)" });
        await Apply(new DataModelPendingOperation { Kind = DataModelPendingOperationKind.SetDefault, ColumnName = "b", ExpressionSql = "'ready'" },
            new() { Kind = DataModelPendingOperationKind.SetNotNull, ColumnName = "b" },
            new() { Kind = DataModelPendingOperationKind.SetCollation, ColumnName = "b", Collation = "NOCASE_AI" });
        await Apply(new DataModelPendingOperation { Kind = DataModelPendingOperationKind.DropDefault, ColumnName = "b" },
            new() { Kind = DataModelPendingOperationKind.DropNotNull, ColumnName = "b" },
            new() { Kind = DataModelPendingOperationKind.DropCollation, ColumnName = "b" });
        await Apply(new DataModelPendingOperation { Kind = DataModelPendingOperationKind.AddPrimaryKey, ColumnNames = ["a", "b"], ConstraintName = "pk_edits" },
            new() { Kind = DataModelPendingOperationKind.AddUniqueKey, ColumnNames = ["c", "d"], ConstraintName = "uq_edits" },
            new() { Kind = DataModelPendingOperationKind.AddCheck, ConstraintName = "ck_a", ExpressionSql = "a >= 0" },
            new() { Kind = DataModelPendingOperationKind.CreateIndex, IndexName = "idx_edits", ColumnNames = ["c", "d"] });
        await Apply(new DataModelPendingOperation { Kind = DataModelPendingOperationKind.DropIndex, IndexName = "idx_edits" },
            new() { Kind = DataModelPendingOperationKind.DropConstraint, ConstraintName = "uq_edits" },
            new() { Kind = DataModelPendingOperationKind.DropConstraint, ConstraintName = "ck_a" },
            new() { Kind = DataModelPendingOperationKind.DropPrimaryKey });
        await Apply(new DataModelPendingOperation { Kind = DataModelPendingOperationKind.RenameColumn, ColumnName = "label", NewColumnName = "caption" });
        await Apply(new DataModelPendingOperation { Kind = DataModelPendingOperationKind.DropColumn, ColumnName = "caption" });
        Assert.DoesNotContain((await _client.GetTableSchemaAsync("edits", Ct))!.Columns, column => column.Name == "caption");
    }

    [Fact]
    public async Task RefreshUsesCurrentVisualStateAndRebasesPendingIntentWithoutApplyingIt()
    {
        var state = await Model(); await _service.SaveDiagramAsync("Refresh", state, Ct);
        state.Nodes[0].X = 999; state.Scale = .75; state.ViewportY = 74;
        var group = DataModelGroups.Create(state, ["parents", "children"])!;
        state.PendingOperations.Add(DefaultOperation("'new'"));
        await Sql("ALTER TABLE parents ADD COLUMN note TEXT;");
        var refreshed = (await _service.RefreshModelAsync(state, Ct))!;
        Assert.Equal(999, refreshed.Nodes[0].X); Assert.Equal(.75, refreshed.Scale); Assert.Equal(74, refreshed.ViewportY);
        Assert.Equal(group.Id, Assert.Single(refreshed.Groups).Id); Assert.Single(refreshed.PendingOperations);
        Assert.NotEqual(state.SchemaFingerprint, refreshed.SchemaFingerprint);
        Assert.True((await _service.ReviewChangesAsync(refreshed, Ct)).CanApply);
        Assert.Null((await _client.GetTableSchemaAsync("children", Ct))!.Columns.Single(column => column.Name == "code").DefaultSql);
    }

    [Fact]
    public async Task UnsupportedDefinitionsAreBlockedAndPendingCandidateChecksReportDuplicates()
    {
        var state = await Model();
        Assert.Throws<InvalidOperationException>(() => DataModelService.ValidateForStaging(state, DefaultOperation("UPPER('x')")));
        Assert.Throws<InvalidOperationException>(() => DataModelService.ValidateForStaging(state, new() { Kind = DataModelPendingOperationKind.AddColumn, TableName = "children", ColumnName = "rv", ColumnType = "ROWVERSION" }));
        Assert.Throws<InvalidOperationException>(() => DataModelService.ValidateForStaging(state, new() { Kind = DataModelPendingOperationKind.AddForeignKey, TableName = "children", ColumnNames = ["parent_id"], ReferencedTableName = "parents", ReferencedColumnNames = ["id"] }));
        Assert.Throws<InvalidOperationException>(() => SchemaColumnRules.NormalizeType("TEXT); DROP TABLE children; --"));
        await Sql("INSERT INTO children (id,code) VALUES (1,'repeat'),(2,'repeat');");
        state.PendingOperations.Add(new() { Kind = DataModelPendingOperationKind.AddUniqueKey, TableName = "children", ConstraintName = "uq_candidate", ColumnNames = ["code"] });
        var checks = await _service.CheckDataAsync(state, "children", Ct);
        Assert.Contains(checks, check => check.Description.Contains("uq_candidate") && check.Violations == 1);
    }

    [Theory]
    [InlineData(1)] [InlineData(2)] [InlineData(3)] [InlineData(4)]
    public async Task PriorJsonVersionsRetainMembershipPositionsViewportAndPendingIntent(int version)
    {
        var state = await Model(); state.Nodes[0].X = 123.5; state.Scale = .8; state.ViewportX = 211;
        state.PendingOperations.Add(DefaultOperation("'new'"));
        var json = System.Text.Json.Nodes.JsonNode.Parse(DataModelGraphBuilder.SerializeState(state))!;
        json["Version"] = version;
        var loaded = DataModelGraphBuilder.DeserializeState(json.ToJsonString())!;
        Assert.Equal(5, loaded.Version); Assert.Equal(123.5, loaded.Nodes[0].X); Assert.Equal(.8, loaded.Scale); Assert.Equal(211, loaded.ViewportX);
        Assert.Equal("'new'", Assert.Single(loaded.PendingOperations).ExpressionSql); Assert.Equal(2, loaded.Nodes.Count);
    }

    [Fact]
    public async Task RemovingCardsDoesNotChangeReviewedSqlOrSchemaIntent()
    {
        var state = await Model();
        state.PendingOperations.Add(new() { Kind = DataModelPendingOperationKind.AddForeignKey, TableName = "children", ConstraintName = "fk_visible_independent", ColumnNames = ["tenant", "parent_id"], ReferencedTableName = "parents", ReferencedColumnNames = ["tenant", "id"] });
        var plan = await _service.ReviewChangesAsync(state, Ct); Assert.True(plan.CanApply);
        state.Nodes.Clear(); state.Groups.Clear(); state.Relationships.Clear();
        var after = await _service.ReviewChangesAsync(state, Ct);
        Assert.True(after.CanApply, string.Join(";", after.Errors));
        Assert.Equal(plan.Steps, after.Steps); Assert.True(DataModelService.MatchesReview(state, plan));
    }

    [Fact]
    public async Task DraftCardinalityDoesNotTreatCompositePrimaryKeyMembersAsIndividuallyUnique()
    {
        var state = await Model();
        var child = state.Nodes.Single(node => node.Name == "children");
        child.Columns.Single(column => column.Name == "tenant").IsPrimaryKey = true;
        child.Keys = [new() { Kind = KeyConstraintKind.PrimaryKey, Columns = ["id", "tenant"] }];
        state.Relationships = [new() { Kind = DataModelRelationshipKind.Draft, LeftTable = "children", LeftColumn = "tenant", RightTable = "parents", RightColumn = "tenant" }];
        DataModelGraphBuilder.UpdateDraftCardinalities(state);
        Assert.Equal(DataModelCardinality.ZeroOrMany, state.Relationships[0].ReferencingEndCardinality);
        state.Relationships[0].ColumnPairs = [new("id", "id"), new("tenant", "tenant")];
        DataModelGraphBuilder.UpdateDraftCardinalities(state);
        Assert.Equal(DataModelCardinality.ZeroOrOne, state.Relationships[0].ReferencingEndCardinality);
    }

    [Fact]
    public async Task ReviewedPlansCannotBeAppliedToAnotherRouteClient()
    {
        var state = await Model(); state.PendingOperations.Add(DefaultOperation("'new'"));
        var plan = await _service.ReviewChangesAsync(state, Ct);
        string path = Path.Combine(Path.GetTempPath(), $"modeler_route_{Guid.NewGuid():N}.db");
        try
        {
            await using var other = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = path });
            var otherService = new DataModelService(other);
            await Assert.ThrowsAsync<InvalidOperationException>(() => otherService.ApplyReviewedChangesAsync(state, plan, Ct));
            Assert.Single(state.PendingOperations); Assert.Empty(await other.GetTableNamesAsync(Ct));
        }
        finally { if (File.Exists(path)) File.Delete(path); if (File.Exists(path + ".wal")) File.Delete(path + ".wal"); }
    }

    [Fact]
    public async Task DependentEditsUseOrderedPreviewAndApplyInOneBatch()
    {
        var state = await _service.BuildSelectionAsync(["children"], ct: Ct);
        var operations = new DataModelPendingOperation[]
        {
            new() { Kind = DataModelPendingOperationKind.AddColumn, TableName = "children", ColumnName = "extra", ColumnType = "VARCHAR(30)" },
            new() { Kind = DataModelPendingOperationKind.RenameColumn, TableName = "children", ColumnName = "extra", NewColumnName = "caption" },
            new() { Kind = DataModelPendingOperationKind.SetDefault, TableName = "children", ColumnName = "caption", ExpressionSql = "'ready'" },
            new() { Kind = DataModelPendingOperationKind.RenameTable, TableName = "children", NewTableName = "items" },
            new() { Kind = DataModelPendingOperationKind.CreateIndex, TableName = "items", IndexName = "idx_caption", ColumnNames = ["caption"] },
            new() { Kind = DataModelPendingOperationKind.AddForeignKey, TableName = "items", ConstraintName = "fk_extra", ColumnNames = ["tenant", "parent_id"], ReferencedTableName = "parents", ReferencedColumnNames = ["tenant", "id"] }
        };
        foreach (var operation in operations)
        { DataModelService.ValidateForStaging(state, operation); state.PendingOperations.Add(operation); }
        string before = DataModelGraphBuilder.SerializeState(state);
        var preview = DataModelSchemaProjection.ForEditing(state);
        Assert.Equal("items", preview.FromCanvas(state.Nodes[0])!.Name);
        Assert.Equal("'ready'", preview.Find("items")!.Columns.Single(column => column.Name == "caption").DefaultSql);
        Assert.Equal(before, DataModelGraphBuilder.SerializeState(state));
        var plan = await _service.ReviewChangesAsync(state, Ct);
        Assert.True(plan.CanApply, string.Join(";", plan.Errors));
        Assert.True((await _service.ApplyReviewedChangesAsync(state, plan, Ct)).Succeeded);
        var schema = (await _client.GetTableSchemaAsync("items", Ct))!;
        Assert.Contains(schema.Columns, column => column.Name == "caption" && column.DefaultSql == "'ready'");
        Assert.Contains(await _client.GetIndexesAsync(Ct), index => index.IndexName == "idx_caption");
        Assert.Single(state.Nodes); Assert.Equal("items", state.Nodes[0].Name);
    }

    [Fact]
    public async Task FutureCandidateKeysCannotValidateAnEarlierForeignKey()
    {
        var state = await Model();
        var create = new DataModelPendingOperation { Kind = DataModelPendingOperationKind.CreateTable, TableName = "draft_parent", Columns = [new() { Name = "tenant", TypeLabel = "INTEGER" }, new() { Name = "id", TypeLabel = "INTEGER" }] };
        var key = new DataModelPendingOperation { Kind = DataModelPendingOperationKind.AddPrimaryKey, TableName = "draft_parent", ConstraintName = "pk_draft", ColumnNames = ["tenant", "id"] };
        var fk = new DataModelPendingOperation { Kind = DataModelPendingOperationKind.AddForeignKey, TableName = "children", ConstraintName = "fk_draft", ColumnNames = ["tenant", "parent_id"], ReferencedTableName = "draft_parent", ReferencedColumnNames = ["tenant", "id"] };
        state.PendingOperations.AddRange([create, fk, key]);
        Assert.False((await _service.ReviewChangesAsync(state, Ct)).CanApply);
        state.PendingOperations = [create, key, fk];
        var plan = await _service.ReviewChangesAsync(state, Ct);
        Assert.True(plan.CanApply, string.Join(";", plan.Errors));
        Assert.True((await _service.ApplyReviewedChangesAsync(state, plan, Ct)).Succeeded);
    }

    [Fact]
    public async Task ContextAndOrphanChecksIncludeOffCanvasIncomingRelationships()
    {
        await Sql("CREATE TABLE off_canvas (id INTEGER PRIMARY KEY, tenant INTEGER, parent_id INTEGER);");
        await Sql("INSERT INTO off_canvas VALUES (1,99,99),(2,NULL,99);");
        var state = await _service.BuildSelectionAsync(["parents"], ct: Ct);
        Assert.Single(state.Nodes); Assert.Empty(state.Relationships);
        Assert.Contains(state.SchemaContext!.Relationships, relationship => relationship.LeftTable == "children");
        Assert.DoesNotContain("SchemaContext", DataModelGraphBuilder.SerializeState(state));
        state.PendingOperations.Add(new() { Kind = DataModelPendingOperationKind.AddForeignKey, TableName = "off_canvas", ConstraintName = "fk_candidate", ColumnNames = ["tenant", "parent_id"], ReferencedTableName = "parents", ReferencedColumnNames = ["tenant", "id"] });
        var checks = await _service.CheckDataAsync(state, "parents", Ct);
        Assert.Contains(checks, check => check.Description.Contains("off_canvas.fk_candidate") && check.Violations == 1 && check.SkippedReason is null);
        Assert.Contains(checks, check => check.Description.Contains("children.fk_parent"));
        Assert.Single(state.Nodes);
    }

    [Fact]
    public async Task CandidateChecksHonorNullabilityCollationsRenamesAndSkippedColumns()
    {
        await Sql("CREATE TABLE candidates (id INTEGER, label TEXT);");
        await Sql("INSERT INTO candidates VALUES (NULL,'Alpha'),(2,'alpha');");
        var state = await _service.BuildSelectionAsync(["candidates"], ct: Ct);
        state.PendingOperations.AddRange([
            new() { Kind = DataModelPendingOperationKind.RenameColumn, TableName = "candidates", ColumnName = "label", NewColumnName = "caption" },
            new() { Kind = DataModelPendingOperationKind.CreateIndex, TableName = "candidates", IndexName = "uq_case", ColumnNames = ["caption"], ColumnCollations = ["NOCASE"], IsUnique = true },
            new() { Kind = DataModelPendingOperationKind.AddPrimaryKey, TableName = "candidates", ConstraintName = "pk_candidate", ColumnNames = ["id"] },
            new() { Kind = DataModelPendingOperationKind.AddColumn, TableName = "candidates", ColumnName = "future", ColumnType = "TEXT", NotNull = true, ExpressionSql = "'x'" }
        ]);
        var checks = await _service.CheckDataAsync(state, "candidates", Ct);
        Assert.Contains(checks, check => check.Description.Contains("uq_case") && check.Violations == 1);
        Assert.Contains(checks, check => check.Description.Contains("NULL values in candidates.id") && check.Violations == 1);
        Assert.Contains(checks, check => check.Description.Contains("future") && check.SkippedReason is not null);
        Assert.Null((await _client.GetTableSchemaAsync("candidates", Ct))!.Columns.First(column => column.Name == "label").Collation);
    }

    [Fact]
    public async Task ReplacementAndRowversionRestrictionsAreCheckedAgainstPriorSteps()
    {
        var state = await Model();
        var replacement = new DataModelPendingOperation { Kind = DataModelPendingOperationKind.AddForeignKey, TableName = "children", ConstraintName = "fk_parent", ColumnNames = ["tenant", "parent_id"], ReferencedTableName = "parents", ReferencedColumnNames = ["tenant", "id"] };
        Assert.Throws<InvalidOperationException>(() => DataModelService.ValidateForStaging(state, replacement));
        DataModelService.ValidateBatchForStaging(state, [new() { Kind = DataModelPendingOperationKind.DropForeignKey, TableName = "children", ConstraintName = "fk_parent" }, replacement]);
        state.PendingOperations.Add(new() { Kind = DataModelPendingOperationKind.CreateTable, TableName = "draft" });
        var rowversion = new DataModelPendingOperation { Kind = DataModelPendingOperationKind.AddColumn, TableName = "draft", ColumnName = "rv", ColumnType = "ROWVERSION" };
        DataModelService.ValidateForStaging(state, rowversion);
        state.PendingOperations.Add(new() { Kind = DataModelPendingOperationKind.CreateIndex, TableName = "draft", IndexName = "idx_draft", ColumnNames = ["Id"] });
        Assert.Throws<InvalidOperationException>(() => DataModelService.ValidateForStaging(state, rowversion));
        state.PendingOperations.Add(rowversion);
        Assert.False((await _service.ReviewChangesAsync(state, Ct)).CanApply);
    }

    [Fact]
    public async Task SharedColumnDefinitionsPreserveFacetsAndRejectUnsupportedDefaults()
    {
        var column = new DataModelColumn { Name = "caption", TypeLabel = "VARCHAR(40)", Nullable = false, DefaultSql = "'ready'", Collation = "NOCASE", Checks = [new() { ConstraintName = "ck_caption", ColumnName = "caption", ExpressionSql = "caption <> ''" }] };
        await Sql(SchemaColumnRules.RenderCreateTable("shared", [column]));
        var schema = (await _client.GetTableSchemaAsync("shared", Ct))!;
        Assert.False(schema.Columns[0].Nullable); Assert.Equal("'ready'", schema.Columns[0].DefaultSql);
        Assert.Equal("NOCASE", schema.Columns[0].Collation); Assert.Single(schema.CheckConstraints);
        column.DefaultSql = "UPPER('ready')";
        Assert.Throws<InvalidOperationException>(() => SchemaColumnRules.RenderColumn(column));
    }

    [Fact]
    public async Task ShardedRoutePersistsAndAppliesOnlyToItsOwningShard()
    {
        string a = _path + ".shard-a", b = _path + ".shard-b";
        try
        {
            await using var sharded = await CSharpDbShardedClient.CreateAsync(new()
            {
                Keyspace = "tenants", MapVersion = 1, VirtualBucketCount = 2,
                Shards = [new() { ShardId = "a", DataSource = a }, new() { ShardId = "b", DataSource = b }],
                BucketRanges = [new() { ShardId = "a", StartBucketInclusive = 0, EndBucketExclusive = 1 }, new() { ShardId = "b", StartBucketInclusive = 1, EndBucketExclusive = 2 }],
                ExactKeyPins = new() { ["alice"] = "a", ["bob"] = "b" }
            }, ct: Ct);
            Assert.All(await sharded.ExecuteSqlOnAllShardsAsync("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);", Ct), result => Assert.Null(result.Error));
            var clientA = sharded.ForRoute(new() { Keyspace = "tenants", Key = "alice" });
            var clientB = sharded.ForRoute(new() { Keyspace = "tenants", Key = "bob" });
            var serviceA = new DataModelService(clientA); var serviceB = new DataModelService(clientB);
            var state = await serviceA.BuildSelectionAsync(["items"], ct: Ct);
            state.Nodes[0].X = 456;
            state.PendingOperations.AddRange([
                new() { Kind = DataModelPendingOperationKind.AddColumn, TableName = "items", ColumnName = "caption", ColumnType = "TEXT" },
                new() { Kind = DataModelPendingOperationKind.CreateIndex, TableName = "items", IndexName = "idx_caption", ColumnNames = ["caption"] }
            ]);
            await serviceA.SaveDiagramAsync("Route model", state, Ct);
            Assert.Null(await serviceB.LoadDiagramAsync("Route model", Ct));
            var plan = await serviceA.ReviewChangesAsync(state, Ct);
            Assert.True(plan.CanApply, string.Join(";", plan.Errors));
            await Assert.ThrowsAsync<InvalidOperationException>(() => serviceB.ApplyReviewedChangesAsync(state, plan, Ct));
            Assert.True((await serviceA.ApplyReviewedChangesAsync(state, plan, Ct)).Succeeded);
            Assert.Equal(456, (await serviceA.LoadDiagramAsync("Route model", Ct))!.Nodes[0].X);
            Assert.DoesNotContain((await clientB.GetTableSchemaAsync("items", Ct))!.Columns, column => column.Name == "caption");
            Assert.Contains((await clientA.GetTableSchemaAsync("items", Ct))!.Columns, column => column.Name == "caption");
        }
        finally { foreach (string file in new[] { a, a + ".wal", b, b + ".wal" }) if (File.Exists(file)) File.Delete(file); }
    }

    [Fact]
    public async Task RenamedCheckDefinitionsUseEngineRulesAndDependenciesMustBeRemovedInOrder()
    {
        await Sql("ALTER TABLE children ADD CONSTRAINT ck_code CHECK(code <> 'code');");
        var state = await Model();
        state.PendingOperations.Add(new() { Kind = DataModelPendingOperationKind.RenameColumn, TableName = "children", ColumnName = "code", NewColumnName = "caption" });
        var preview = DataModelSchemaProjection.ForEditing(state);
        string definition = preview.Find("children")!.Checks.Single(check => check.ConstraintName == "ck_code").ExpressionSql;
        Assert.Contains("caption", definition); Assert.Contains("'code'", definition);
        var drop = new DataModelPendingOperation { Kind = DataModelPendingOperationKind.DropColumn, TableName = "children", ColumnName = "caption" };
        Assert.Throws<InvalidOperationException>(() => DataModelService.ValidateForStaging(state, drop));
        state.PendingOperations.Add(new() { Kind = DataModelPendingOperationKind.DropConstraint, TableName = "children", ConstraintName = "ck_code" });
        DataModelService.ValidateForStaging(state, drop); state.PendingOperations.Add(drop);
        var plan = await _service.ReviewChangesAsync(state, Ct); Assert.True(plan.CanApply, string.Join(";", plan.Errors));
        Assert.True((await _service.ApplyReviewedChangesAsync(state, plan, Ct)).Succeeded);
        Assert.DoesNotContain((await _client.GetTableSchemaAsync("children", Ct))!.Columns, column => column.Name == "caption");
    }

    [Fact]
    public async Task UnsupportedCollationsAndPrimaryKeyNullabilityAreBlockedBeforeReview()
    {
        var state = await Model();
        Assert.Throws<InvalidOperationException>(() => DataModelService.ValidateForStaging(state, Op(DataModelPendingOperationKind.DropNotNull, "id")));
        Assert.ThrowsAny<Exception>(() => DataModelService.ValidateForStaging(state, new() { Kind = DataModelPendingOperationKind.SetCollation, TableName = "children", ColumnName = "amount", Collation = "NOCASE" }));
        Assert.ThrowsAny<Exception>(() => SchemaColumnRules.RenderColumn(new() { Name = "value", TypeLabel = "TEXT", Collation = "NOT_A_COLLATION" }));
    }

    public class ClientProxy : DispatchProxy
    {
        public ICSharpDbClient Inner = null!;
        public bool FailSave, NoTransactions;
        protected override object? Invoke(MethodInfo? method, object?[]? args)
        {
            if (NoTransactions && method!.Name == nameof(ICSharpDbClient.BeginTransactionAsync)) return Task.FromException<TransactionSessionInfo>(new NotSupportedException("Transactions disabled"));
            if (FailSave && method!.Name == nameof(ICSharpDbClient.ExecuteSqlAsync) && args?[0] is string sql && sql.Contains("UPDATE __data_model_diagrams"))
                return Task.FromException<SqlExecutionResult>(new IOException("Simulated diagram save failure"));
            return method!.Invoke(Inner, args);
        }
    }
}
