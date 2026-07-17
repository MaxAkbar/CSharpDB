using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class AlterTableRewriteTests : IAsyncLifetime
{
    private readonly string _databasePath =
        Path.Combine(Path.GetTempPath(), $"csharpdb_alter_rewrite_{Guid.NewGuid():N}.db");
    private Database _database = null!;

    public async ValueTask InitializeAsync() =>
        _database = await Database.OpenAsync(
            _databasePath,
            TestContext.Current.CancellationToken);

    public async ValueTask DisposeAsync()
    {
        await _database.DisposeAsync();
        if (File.Exists(_databasePath))
            File.Delete(_databasePath);
        if (File.Exists(_databasePath + ".wal"))
            File.Delete(_databasePath + ".wal");
    }

    [Fact]
    public async Task DropColumn_UsesShadowRootAndPreservesRowsIndexesAndReopen()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE rewrite_items (" +
            "id INTEGER PRIMARY KEY, kept TEXT NOT NULL, removed TEXT)",
            ct);
        await _database.ExecuteAsync(
            "CREATE INDEX ix_rewrite_items_kept ON rewrite_items (kept)",
            ct);

        string values = string.Join(
            ", ",
            Enumerable.Range(1, 200).Select(index =>
                $"({index}, 'kept-{index:D3}', '{new string('x', 80)}')"));
        await _database.ExecuteAsync(
            $"INSERT INTO rewrite_items VALUES {values}",
            ct);

        uint originalRootPage = _database.GetTableRootPage("rewrite_items");
        await _database.ExecuteAsync(
            "ALTER TABLE rewrite_items DROP COLUMN removed",
            ct);

        Assert.NotEqual(originalRootPage, _database.GetTableRootPage("rewrite_items"));
        Assert.Equal(
            ["id", "kept"],
            _database.GetTableSchema("rewrite_items")!.Columns.Select(column => column.Name));
        Assert.Contains(
            _database.GetIndexes(),
            index => string.Equals(
                index.IndexName,
                "ix_rewrite_items_kept",
                StringComparison.OrdinalIgnoreCase));

        await AssertRowsPreservedAsync(ct);

        await _database.DisposeAsync();
        _database = await Database.OpenAsync(_databasePath, ct);
        await AssertRowsPreservedAsync(ct);
    }

    [Fact]
    public async Task AddColumn_WithDefault_RewritesAndBackfillsExistingRows()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE add_default_items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO add_default_items VALUES (1, 'one'), (2, 'two')",
            ct);

        uint originalRootPage = _database.GetTableRootPage("add_default_items");
        await _database.ExecuteAsync(
            "ALTER TABLE add_default_items ADD COLUMN status TEXT NOT NULL DEFAULT 'new'",
            ct);

        Assert.NotEqual(originalRootPage, _database.GetTableRootPage("add_default_items"));
        await using (QueryResult result = await _database.ExecuteAsync(
            "SELECT id, status FROM add_default_items ORDER BY id",
            ct))
        {
            List<DbValue[]> rows = await result.ToListAsync(ct);
            Assert.Equal(2, rows.Count);
            Assert.All(rows, row => Assert.Equal("new", row[1].AsText));
        }

        await _database.ExecuteAsync(
            "INSERT INTO add_default_items (id, name) VALUES (3, 'three')",
            ct);
        await using QueryResult inserted = await _database.ExecuteAsync(
            "SELECT status FROM add_default_items WHERE id = 3",
            ct);
        Assert.Equal("new", Assert.Single(await inserted.ToListAsync(ct))[0].AsText);
    }

    [Fact]
    public async Task FailedRewrite_LeavesOriginalTableAndAbortsExplicitTransaction()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE failed_rewrite (id INTEGER PRIMARY KEY, payload TEXT)",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO failed_rewrite VALUES (1, 'original')",
            ct);
        uint originalRootPage = _database.GetTableRootPage("failed_rewrite");

        await _database.BeginTransactionAsync(ct);
        CSharpDbException failure = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _database.ExecuteAsync(
                "ALTER TABLE failed_rewrite ADD COLUMN required_value TEXT NOT NULL",
                ct));
        Assert.Equal(ErrorCode.ConstraintViolation, failure.Code);

        CSharpDbException commitFailure = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _database.CommitAsync(ct));
        Assert.Contains("rolled back", commitFailure.Message, StringComparison.OrdinalIgnoreCase);

        Assert.Equal(originalRootPage, _database.GetTableRootPage("failed_rewrite"));
        Assert.Equal(
            ["id", "payload"],
            _database.GetTableSchema("failed_rewrite")!.Columns.Select(column => column.Name));
        await using QueryResult result = await _database.ExecuteAsync(
            "SELECT payload FROM failed_rewrite WHERE id = 1",
            ct);
        Assert.Equal("original", Assert.Single(await result.ToListAsync(ct))[0].AsText);
    }

    [Fact]
    public async Task DropColumn_ReferencedByTransitiveView_FailsBeforeRewriteAndPreservesViewAfterReopen()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE view_dependency_items (" +
            "id INTEGER PRIMARY KEY, kept TEXT NOT NULL, removed TEXT)",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO view_dependency_items VALUES (1, 'kept', 'removed')",
            ct);
        await _database.ExecuteAsync(
            "CREATE VIEW z_inner_items AS " +
            "SELECT id, kept FROM view_dependency_items",
            ct);
        await _database.ExecuteAsync(
            "CREATE VIEW a_outer_items AS " +
            "SELECT id, kept FROM z_inner_items",
            ct);

        uint originalRootPage = _database.GetTableRootPage("view_dependency_items");
        CSharpDbException failure = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _database.ExecuteAsync(
                "ALTER TABLE view_dependency_items DROP COLUMN removed",
                ct));

        Assert.Equal(ErrorCode.ConstraintViolation, failure.Code);
        Assert.Contains("a_outer_items", failure.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(originalRootPage, _database.GetTableRootPage("view_dependency_items"));
        Assert.Equal(
            ["id", "kept", "removed"],
            _database.GetTableSchema("view_dependency_items")!.Columns.Select(column => column.Name));
        await AssertViewDependencyRowsAsync(ct);

        await _database.DisposeAsync();
        _database = await Database.OpenAsync(_databasePath, ct);

        Assert.Equal(
            ["id", "kept", "removed"],
            _database.GetTableSchema("view_dependency_items")!.Columns.Select(column => column.Name));
        await AssertViewDependencyRowsAsync(ct);
    }

    [Fact]
    public async Task RenameTable_ReferencedByView_IsRejectedWithoutChangingMetadata()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE rename_table_items (id INTEGER PRIMARY KEY, value TEXT)",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO rename_table_items VALUES (1, 'original')",
            ct);
        await _database.ExecuteAsync(
            "CREATE VIEW rename_table_items_view AS " +
            "SELECT id, value FROM rename_table_items",
            ct);

        CSharpDbException failure = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _database.ExecuteAsync(
                "ALTER TABLE rename_table_items RENAME TO renamed_table_items",
                ct));

        Assert.Equal(ErrorCode.ConstraintViolation, failure.Code);
        Assert.Contains("rename_table_items_view", failure.Message, StringComparison.OrdinalIgnoreCase);
        Assert.NotNull(_database.GetTableSchema("rename_table_items"));
        Assert.Null(_database.GetTableSchema("renamed_table_items"));
        await using QueryResult result = await _database.ExecuteAsync(
            "SELECT value FROM rename_table_items_view WHERE id = 1",
            ct);
        Assert.Equal("original", Assert.Single(await result.ToListAsync(ct))[0].AsText);
    }

    [Fact]
    public async Task RenameColumn_ReferencedByView_IsRejectedWithoutChangingMetadata()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE rename_column_items (id INTEGER PRIMARY KEY, old_value TEXT)",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO rename_column_items VALUES (1, 'original')",
            ct);
        await _database.ExecuteAsync(
            "CREATE VIEW rename_column_items_view AS " +
            "SELECT id, old_value FROM rename_column_items",
            ct);

        CSharpDbException failure = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _database.ExecuteAsync(
                "ALTER TABLE rename_column_items " +
                "RENAME COLUMN old_value TO new_value",
                ct));

        Assert.Equal(ErrorCode.ConstraintViolation, failure.Code);
        Assert.Contains("rename_column_items_view", failure.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(
            ["id", "old_value"],
            _database.GetTableSchema("rename_column_items")!.Columns.Select(column => column.Name));
        await using QueryResult result = await _database.ExecuteAsync(
            "SELECT old_value FROM rename_column_items_view WHERE id = 1",
            ct);
        Assert.Equal("original", Assert.Single(await result.ToListAsync(ct))[0].AsText);
    }

    [Theory]
    [InlineData("trigger")]
    [InlineData("cross-trigger")]
    [InlineData("column-rule")]
    [InlineData("table-rule")]
    public async Task RenameTable_WithTriggerOrValidationRule_IsRejected(
        string dependencyKind)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE rename_table_dependency (" +
            "id INTEGER PRIMARY KEY, old_value TEXT)",
            ct);

        string dependencyName;
        switch (dependencyKind)
        {
            case "trigger":
                dependencyName = "trg_rename_table_dependency";
                await _database.ExecuteAsync(
                    "CREATE TABLE rename_table_dependency_audit (id INTEGER)",
                    ct);
                await _database.ExecuteAsync(
                    "CREATE TRIGGER trg_rename_table_dependency " +
                    "AFTER INSERT ON rename_table_dependency " +
                    "BEGIN INSERT INTO rename_table_dependency_audit VALUES (NEW.id); END",
                    ct);
                break;
            case "cross-trigger":
                dependencyName = "trg_rename_table_cross_dependency";
                await _database.ExecuteAsync(
                    "CREATE TABLE rename_table_dependency_source (id INTEGER PRIMARY KEY)",
                    ct);
                await _database.ExecuteAsync(
                    "CREATE TRIGGER trg_rename_table_cross_dependency " +
                    "AFTER INSERT ON rename_table_dependency_source " +
                    "BEGIN INSERT INTO rename_table_dependency (id, old_value) " +
                    "VALUES (NEW.id, 'from-trigger'); END",
                    ct);
                break;
            case "column-rule":
                dependencyName = "rule_rename_table_column";
                await _database.ExecuteAsync(
                    "CREATE VALIDATION RULE rule_rename_table_column " +
                    "ON rename_table_dependency.old_value " +
                    "AS old_value IS NOT NULL MESSAGE 'value required'",
                    ct);
                break;
            case "table-rule":
                dependencyName = "rule_rename_table_all";
                await _database.ExecuteAsync(
                    "CREATE VALIDATION RULE rule_rename_table_all " +
                    "ON rename_table_dependency " +
                    "AS id > 0 MESSAGE 'id required'",
                    ct);
                break;
            default:
                throw new InvalidOperationException($"Unknown dependency kind '{dependencyKind}'.");
        }

        CSharpDbException failure = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _database.ExecuteAsync(
                "ALTER TABLE rename_table_dependency " +
                "RENAME TO renamed_table_dependency",
                ct));

        Assert.Equal(ErrorCode.ConstraintViolation, failure.Code);
        Assert.Contains(dependencyName, failure.Message, StringComparison.OrdinalIgnoreCase);
        Assert.NotNull(_database.GetTableSchema("rename_table_dependency"));
        Assert.Null(_database.GetTableSchema("renamed_table_dependency"));
    }

    [Theory]
    [InlineData("trigger")]
    [InlineData("cross-trigger")]
    [InlineData("column-rule")]
    [InlineData("expression-rule")]
    [InlineData("table-rule")]
    public async Task RenameColumn_WithTriggerOrValidationRule_IsRejected(
        string dependencyKind)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE rename_column_dependency (" +
            "id INTEGER PRIMARY KEY, old_value TEXT, untouched TEXT)",
            ct);

        string dependencyName;
        switch (dependencyKind)
        {
            case "trigger":
                dependencyName = "trg_rename_column_dependency";
                await _database.ExecuteAsync(
                    "CREATE TABLE rename_column_dependency_audit (id INTEGER)",
                    ct);
                await _database.ExecuteAsync(
                    "CREATE TRIGGER trg_rename_column_dependency " +
                    "AFTER INSERT ON rename_column_dependency " +
                    "BEGIN INSERT INTO rename_column_dependency_audit VALUES (NEW.id); END",
                    ct);
                break;
            case "cross-trigger":
                dependencyName = "trg_rename_column_cross_dependency";
                await _database.ExecuteAsync(
                    "CREATE TABLE rename_column_dependency_source (" +
                    "id INTEGER PRIMARY KEY, value TEXT)",
                    ct);
                await _database.ExecuteAsync(
                    "CREATE TRIGGER trg_rename_column_cross_dependency " +
                    "AFTER INSERT ON rename_column_dependency_source " +
                    "BEGIN UPDATE rename_column_dependency " +
                    "SET old_value = NEW.value WHERE id = NEW.id; END",
                    ct);
                break;
            case "column-rule":
                dependencyName = "rule_rename_column_value";
                await _database.ExecuteAsync(
                    "CREATE VALIDATION RULE rule_rename_column_value " +
                    "ON rename_column_dependency.old_value " +
                    "AS old_value IS NOT NULL MESSAGE 'value required'",
                    ct);
                break;
            case "expression-rule":
                dependencyName = "rule_rename_column_expression";
                await _database.ExecuteAsync(
                    "CREATE VALIDATION RULE rule_rename_column_expression " +
                    "ON rename_column_dependency.untouched " +
                    "AS old_value IS NOT NULL MESSAGE 'value required'",
                    ct);
                break;
            case "table-rule":
                dependencyName = "rule_rename_column_all";
                await _database.ExecuteAsync(
                    "CREATE VALIDATION RULE rule_rename_column_all " +
                    "ON rename_column_dependency " +
                    "AS old_value IS NOT NULL MESSAGE 'value required'",
                    ct);
                break;
            default:
                throw new InvalidOperationException($"Unknown dependency kind '{dependencyKind}'.");
        }

        CSharpDbException failure = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _database.ExecuteAsync(
                "ALTER TABLE rename_column_dependency " +
                "RENAME COLUMN old_value TO new_value",
                ct));

        Assert.Equal(ErrorCode.ConstraintViolation, failure.Code);
        Assert.Contains(dependencyName, failure.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(
            ["id", "old_value", "untouched"],
            _database.GetTableSchema("rename_column_dependency")!.Columns.Select(column => column.Name));
    }

    [Theory]
    [InlineData("cross-trigger")]
    [InlineData("expression-rule")]
    public async Task DropColumn_WithStoredCrossDependency_IsRejected(
        string dependencyKind)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE drop_column_dependency (" +
            "id INTEGER PRIMARY KEY, old_value TEXT, untouched TEXT)",
            ct);

        string dependencyName;
        switch (dependencyKind)
        {
            case "cross-trigger":
                dependencyName = "trg_drop_column_cross_dependency";
                await _database.ExecuteAsync(
                    "CREATE TABLE drop_column_dependency_source (" +
                    "id INTEGER PRIMARY KEY, value TEXT)",
                    ct);
                await _database.ExecuteAsync(
                    "CREATE TRIGGER trg_drop_column_cross_dependency " +
                    "AFTER INSERT ON drop_column_dependency_source " +
                    "BEGIN UPDATE drop_column_dependency " +
                    "SET old_value = NEW.value WHERE id = NEW.id; END",
                    ct);
                break;
            case "expression-rule":
                dependencyName = "rule_drop_column_expression";
                await _database.ExecuteAsync(
                    "CREATE VALIDATION RULE rule_drop_column_expression " +
                    "ON drop_column_dependency.untouched " +
                    "AS old_value IS NOT NULL MESSAGE 'value required'",
                    ct);
                break;
            default:
                throw new InvalidOperationException($"Unknown dependency kind '{dependencyKind}'.");
        }

        CSharpDbException failure = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _database.ExecuteAsync(
                "ALTER TABLE drop_column_dependency DROP COLUMN old_value",
                ct));

        Assert.Equal(ErrorCode.ConstraintViolation, failure.Code);
        Assert.Contains(dependencyName, failure.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(
            ["id", "old_value", "untouched"],
            _database.GetTableSchema("drop_column_dependency")!.Columns.Select(column => column.Name));
    }

    [Fact]
    public async Task FailedRewrite_PublicWriteTransaction_IsRollbackOnly()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE public_tx_rewrite (id INTEGER PRIMARY KEY, payload TEXT)",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO public_tx_rewrite VALUES (1, 'original')",
            ct);

        await using WriteTransaction transaction =
            await _database.BeginWriteTransactionAsync(ct);
        await transaction.ExecuteAsync(
            "INSERT INTO public_tx_rewrite VALUES (2, 'uncommitted')",
            ct);

        CSharpDbException rewriteFailure = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await transaction.ExecuteAsync(
                "ALTER TABLE public_tx_rewrite " +
                "ADD COLUMN required_value TEXT NOT NULL",
                ct));
        Assert.Equal(ErrorCode.ConstraintViolation, rewriteFailure.Code);

        await using (QueryResult visibleBeforeRollback = await transaction.ExecuteAsync(
                         "SELECT COUNT(*) FROM public_tx_rewrite",
                         ct))
        {
            Assert.Equal(
                2L,
                Assert.Single(await visibleBeforeRollback.ToListAsync(ct))[0].AsInteger);
        }

        CSharpDbException rejectedWrite = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await transaction.ExecuteAsync(
                "INSERT INTO public_tx_rewrite VALUES (3, 'rejected')",
                ct));
        Assert.Contains("aborted", rejectedWrite.Message, StringComparison.OrdinalIgnoreCase);

        CSharpDbException commitFailure = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await transaction.CommitAsync(ct));
        Assert.Contains("rolled back", commitFailure.Message, StringComparison.OrdinalIgnoreCase);

        Assert.Equal(
            ["id", "payload"],
            _database.GetTableSchema("public_tx_rewrite")!.Columns.Select(column => column.Name));
        await using QueryResult committedRows = await _database.ExecuteAsync(
            "SELECT id, payload FROM public_tx_rewrite ORDER BY id",
            ct);
        DbValue[] committedRow = Assert.Single(await committedRows.ToListAsync(ct));
        Assert.Equal(1L, committedRow[0].AsInteger);
        Assert.Equal("original", committedRow[1].AsText);
    }

    private async Task AssertRowsPreservedAsync(CancellationToken ct)
    {
        await using QueryResult result = await _database.ExecuteAsync(
            "SELECT id, kept FROM rewrite_items WHERE kept = 'kept-137'",
            ct);
        DbValue[] row = Assert.Single(await result.ToListAsync(ct));
        Assert.Equal(137L, row[0].AsInteger);
        Assert.Equal("kept-137", row[1].AsText);

        await using QueryResult count = await _database.ExecuteAsync(
            "SELECT COUNT(*) FROM rewrite_items",
            ct);
        Assert.Equal(200L, Assert.Single(await count.ToListAsync(ct))[0].AsInteger);
    }

    private async Task AssertViewDependencyRowsAsync(CancellationToken ct)
    {
        await using QueryResult result = await _database.ExecuteAsync(
            "SELECT id, kept FROM a_outer_items",
            ct);
        DbValue[] row = Assert.Single(await result.ToListAsync(ct));
        Assert.Equal(1L, row[0].AsInteger);
        Assert.Equal("kept", row[1].AsText);
    }
}
