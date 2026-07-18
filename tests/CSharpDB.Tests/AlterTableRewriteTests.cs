using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Storage.Diagnostics;

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

    [Fact]
    public async Task AlterColumnNumericType_UsesShadowRootAndPreservesRowsChecksAndUnchangedIndexes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE numeric_rewrite (" +
            "id INTEGER PRIMARY KEY, amount INTEGER NOT NULL CHECK (amount >= 0), tag TEXT NOT NULL)",
            ct);
        await _database.ExecuteAsync(
            "CREATE INDEX ix_numeric_rewrite_tag ON numeric_rewrite (tag)",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO numeric_rewrite VALUES (1, 10, 'ten'), (2, 25, 'twenty-five')",
            ct);

        uint integerRootPage = _database.GetTableRootPage("numeric_rewrite");
        await _database.ExecuteAsync(
            "ALTER TABLE numeric_rewrite ALTER COLUMN amount TYPE REAL",
            ct);

        Assert.NotEqual(integerRootPage, _database.GetTableRootPage("numeric_rewrite"));
        Assert.Equal(
            DbType.Real,
            _database.GetTableSchema("numeric_rewrite")!.Columns[1].Type);
        Assert.Contains(
            _database.GetIndexes(),
            index => string.Equals(
                index.IndexName,
                "ix_numeric_rewrite_tag",
                StringComparison.OrdinalIgnoreCase));
        await AssertNumericRewriteRowsAsync(DbType.Real, ct);

        await _database.DisposeAsync();
        _database = await Database.OpenAsync(_databasePath, ct);
        await AssertNumericRewriteRowsAsync(DbType.Real, ct);

        uint realRootPage = _database.GetTableRootPage("numeric_rewrite");
        await _database.ExecuteAsync(
            "ALTER TABLE numeric_rewrite ALTER COLUMN amount TYPE INTEGER",
            ct);

        Assert.NotEqual(realRootPage, _database.GetTableRootPage("numeric_rewrite"));
        Assert.Equal(
            DbType.Integer,
            _database.GetTableSchema("numeric_rewrite")!.Columns[1].Type);
        await AssertNumericRewriteRowsAsync(DbType.Integer, ct);
    }

    [Theory]
    [InlineData("INTEGER", "9007199254740993", "REAL", DbType.Integer)]
    [InlineData("REAL", "1.5", "INTEGER", DbType.Real)]
    public async Task AlterColumnNumericType_RejectsInexactValuesWithoutChangingStorage(
        string sourceType,
        string valueSql,
        string targetType,
        DbType expectedStoredType)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            $"CREATE TABLE unsafe_numeric_rewrite (id INTEGER PRIMARY KEY, amount {sourceType})",
            ct);
        await _database.ExecuteAsync(
            $"INSERT INTO unsafe_numeric_rewrite VALUES (1, {valueSql})",
            ct);

        uint originalRootPage = _database.GetTableRootPage("unsafe_numeric_rewrite");
        CSharpDbException failure = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _database.ExecuteAsync(
                $"ALTER TABLE unsafe_numeric_rewrite ALTER COLUMN amount TYPE {targetType}",
                ct));

        Assert.Equal(ErrorCode.TypeMismatch, failure.Code);
        Assert.Equal(originalRootPage, _database.GetTableRootPage("unsafe_numeric_rewrite"));
        Assert.Equal(
            expectedStoredType,
            _database.GetTableSchema("unsafe_numeric_rewrite")!.Columns[1].Type);
        await using QueryResult result = await _database.ExecuteAsync(
            "SELECT amount FROM unsafe_numeric_rewrite WHERE id = 1",
            ct);
        Assert.Equal(
            expectedStoredType,
            Assert.Single(await result.ToListAsync(ct))[0].Type);
    }

    [Fact]
    public async Task AlterColumnNumericType_WithIncompatibleDefault_ExplainsRecoveryBeforeRewrite()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE default_numeric_rewrite (" +
            "id INTEGER PRIMARY KEY, amount REAL DEFAULT 1.5)",
            ct);
        uint originalRootPage = _database.GetTableRootPage("default_numeric_rewrite");

        CSharpDbException failure = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _database.ExecuteAsync(
                "ALTER TABLE default_numeric_rewrite ALTER COLUMN amount TYPE INTEGER",
                ct));

        Assert.Equal(ErrorCode.TypeMismatch, failure.Code);
        Assert.Contains("DROP DEFAULT", failure.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(originalRootPage, _database.GetTableRootPage("default_numeric_rewrite"));
        Assert.Equal(
            DbType.Real,
            _database.GetTableSchema("default_numeric_rewrite")!.Columns[1].Type);
    }

    [Fact]
    public async Task AlterColumnCollation_RoundTripsThroughShadowRootsAndReopen()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE collation_rewrite (" +
            "id INTEGER PRIMARY KEY, code TEXT COLLATE BINARY, tag TEXT NOT NULL)",
            ct);
        await _database.ExecuteAsync(
            "CREATE INDEX ix_collation_rewrite_tag ON collation_rewrite (tag)",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO collation_rewrite VALUES (1, 'Alpha', 'first')",
            ct);
        Assert.Equal(0L, await CountMatchingCodeAsync("collation_rewrite", ct));

        uint binaryRootPage = _database.GetTableRootPage("collation_rewrite");
        await _database.ExecuteAsync(
            "ALTER TABLE collation_rewrite ALTER COLUMN code SET COLLATION NOCASE",
            ct);

        Assert.NotEqual(binaryRootPage, _database.GetTableRootPage("collation_rewrite"));
        Assert.Equal("NOCASE", _database.GetTableSchema("collation_rewrite")!.Columns[1].Collation);
        Assert.Equal(1L, await CountMatchingCodeAsync("collation_rewrite", ct));
        Assert.Contains(
            _database.GetIndexes(),
            index => string.Equals(
                index.IndexName,
                "ix_collation_rewrite_tag",
                StringComparison.OrdinalIgnoreCase));

        await _database.DisposeAsync();
        _database = await Database.OpenAsync(_databasePath, ct);
        Assert.Equal(1L, await CountMatchingCodeAsync("collation_rewrite", ct));

        uint noCaseRootPage = _database.GetTableRootPage("collation_rewrite");
        await _database.ExecuteAsync(
            "ALTER TABLE collation_rewrite ALTER COLUMN code DROP COLLATION",
            ct);

        Assert.NotEqual(noCaseRootPage, _database.GetTableRootPage("collation_rewrite"));
        Assert.Null(_database.GetTableSchema("collation_rewrite")!.Columns[1].Collation);
        Assert.Equal(0L, await CountMatchingCodeAsync("collation_rewrite", ct));
    }

    [Fact]
    public async Task AlterColumnCollation_RevalidatesChecksAndRollsBackOnFailure()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE checked_collation_rewrite (" +
            "id INTEGER PRIMARY KEY, " +
            "code TEXT COLLATE BINARY CHECK (code <> 'forbidden'))",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO checked_collation_rewrite VALUES (1, 'Forbidden')",
            ct);
        uint originalRootPage = _database.GetTableRootPage("checked_collation_rewrite");

        CSharpDbException failure = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _database.ExecuteAsync(
                "ALTER TABLE checked_collation_rewrite " +
                "ALTER COLUMN code SET COLLATION NOCASE",
                ct));

        Assert.Equal(ErrorCode.ConstraintViolation, failure.Code);
        Assert.Equal(
            originalRootPage,
            _database.GetTableRootPage("checked_collation_rewrite"));
        Assert.Equal(
            "BINARY",
            _database.GetTableSchema("checked_collation_rewrite")!.Columns[1].Collation);
        await using QueryResult result = await _database.ExecuteAsync(
            "SELECT code FROM checked_collation_rewrite WHERE id = 1",
            ct);
        Assert.Equal(
            "Forbidden",
            Assert.Single(await result.ToListAsync(ct))[0].AsText);
    }

    [Fact]
    public async Task AlterColumnCollation_RebuildsInheritedSqlIndexesAtomicallyAndPersists()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE indexed_collation_rewrite (" +
            "id INTEGER PRIMARY KEY, code TEXT COLLATE BINARY, tag TEXT NOT NULL)",
            ct);
        await _database.ExecuteAsync(
            "CREATE INDEX ix_indexed_collation_code " +
            "ON indexed_collation_rewrite (code)",
            ct);
        await _database.ExecuteAsync(
            "CREATE UNIQUE INDEX ux_indexed_collation_code " +
            "ON indexed_collation_rewrite (code)",
            ct);
        await _database.ExecuteAsync(
            "CREATE INDEX ix_indexed_collation_tag " +
            "ON indexed_collation_rewrite (tag)",
            ct);
        string values = string.Join(
            ", ",
            Enumerable.Range(1, 80).Select(index =>
                $"({index}, '{(index == 1 ? "Alpha" : $"Code-{index:D4}-{new string('x', 96)}")}', 'tag-{index % 7}')"));
        await _database.ExecuteAsync(
            $"INSERT INTO indexed_collation_rewrite VALUES {values}",
            ct);

        uint originalTableRoot = _database.GetTableRootPage("indexed_collation_rewrite");
        IReadOnlyDictionary<string, uint> originalIndexRoots =
            await GetIndexRootPagesAsync(ct);

        await _database.ExecuteAsync(
            "ALTER TABLE indexed_collation_rewrite " +
            "ALTER COLUMN code SET COLLATION NOCASE",
            ct);

        IReadOnlyDictionary<string, uint> rewrittenIndexRoots =
            await GetIndexRootPagesAsync(ct);
        Assert.NotEqual(
            originalTableRoot,
            _database.GetTableRootPage("indexed_collation_rewrite"));
        Assert.NotEqual(
            originalIndexRoots["ix_indexed_collation_code"],
            rewrittenIndexRoots["ix_indexed_collation_code"]);
        Assert.NotEqual(
            originalIndexRoots["ux_indexed_collation_code"],
            rewrittenIndexRoots["ux_indexed_collation_code"]);
        Assert.Equal(
            originalIndexRoots["ix_indexed_collation_tag"],
            rewrittenIndexRoots["ix_indexed_collation_tag"]);
        Assert.Equal(1L, await CountMatchingCodeAsync("indexed_collation_rewrite", ct));

        CSharpDbException duplicate = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _database.ExecuteAsync(
                "INSERT INTO indexed_collation_rewrite VALUES (81, 'ALPHA', 'duplicate')",
                ct));
        Assert.Equal(ErrorCode.ConstraintViolation, duplicate.Code);

        await _database.DisposeAsync();
        _database = await Database.OpenAsync(_databasePath, ct);

        Assert.Equal(
            "NOCASE",
            _database.GetTableSchema("indexed_collation_rewrite")!.Columns[1].Collation);
        Assert.Equal(1L, await CountMatchingCodeAsync("indexed_collation_rewrite", ct));
        Assert.Equal(
            rewrittenIndexRoots["ix_indexed_collation_code"],
            (await GetIndexRootPagesAsync(ct))["ix_indexed_collation_code"]);
    }

    [Fact]
    public async Task AlterColumnCollation_UniqueIndexCollisionLeavesOriginalRootsUsable()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE colliding_collation_rewrite (" +
            "id INTEGER PRIMARY KEY, code TEXT COLLATE BINARY)",
            ct);
        await _database.ExecuteAsync(
            "CREATE UNIQUE INDEX ux_colliding_collation_code " +
            "ON colliding_collation_rewrite (code)",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO colliding_collation_rewrite VALUES " +
            "(1, 'Alpha'), (2, 'alpha')",
            ct);

        uint originalTableRoot = _database.GetTableRootPage("colliding_collation_rewrite");
        uint originalIndexRoot =
            (await GetIndexRootPagesAsync(ct))["ux_colliding_collation_code"];

        CSharpDbException failure = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _database.ExecuteAsync(
                "ALTER TABLE colliding_collation_rewrite " +
                "ALTER COLUMN code SET COLLATION NOCASE",
                ct));

        Assert.Equal(ErrorCode.ConstraintViolation, failure.Code);
        Assert.Equal(
            originalTableRoot,
            _database.GetTableRootPage("colliding_collation_rewrite"));
        Assert.Equal(
            originalIndexRoot,
            (await GetIndexRootPagesAsync(ct))["ux_colliding_collation_code"]);
        Assert.Equal(
            "BINARY",
            _database.GetTableSchema("colliding_collation_rewrite")!.Columns[1].Collation);

        await _database.DisposeAsync();
        _database = await Database.OpenAsync(_databasePath, ct);

        Assert.Equal(
            originalTableRoot,
            _database.GetTableRootPage("colliding_collation_rewrite"));
        Assert.Equal(
            originalIndexRoot,
            (await GetIndexRootPagesAsync(ct))["ux_colliding_collation_code"]);
        await using QueryResult rows = await _database.ExecuteAsync(
            "SELECT id FROM colliding_collation_rewrite WHERE code = 'alpha'",
            ct);
        Assert.Equal(2L, Assert.Single(await rows.ToListAsync(ct))[0].AsInteger);
    }

    [Fact]
    public async Task AlterColumnCollation_ExplicitIndexCollationKeepsIndexRoot()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE explicit_index_collation (" +
            "id INTEGER PRIMARY KEY, code TEXT COLLATE BINARY)",
            ct);
        await _database.ExecuteAsync(
            "CREATE INDEX ix_explicit_index_collation_code " +
            "ON explicit_index_collation (code COLLATE BINARY)",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO explicit_index_collation VALUES (1, 'Alpha')",
            ct);

        uint originalIndexRoot =
            (await GetIndexRootPagesAsync(ct))["ix_explicit_index_collation_code"];
        await _database.ExecuteAsync(
            "ALTER TABLE explicit_index_collation " +
            "ALTER COLUMN code SET COLLATION NOCASE",
            ct);

        Assert.Equal(
            originalIndexRoot,
            (await GetIndexRootPagesAsync(ct))["ix_explicit_index_collation_code"]);
        Assert.Equal(1L, await CountMatchingCodeAsync("explicit_index_collation", ct));
    }

    [Fact]
    public async Task AlterColumnCollation_PublicWriteTransactionRollbackRestoresAllRoots()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE rollback_indexed_collation (" +
            "id INTEGER PRIMARY KEY, code TEXT COLLATE BINARY)",
            ct);
        await _database.ExecuteAsync(
            "CREATE INDEX ix_rollback_indexed_collation_code " +
            "ON rollback_indexed_collation (code)",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO rollback_indexed_collation VALUES (1, 'Alpha')",
            ct);

        uint originalTableRoot = _database.GetTableRootPage("rollback_indexed_collation");
        uint originalIndexRoot =
            (await GetIndexRootPagesAsync(ct))["ix_rollback_indexed_collation_code"];

        await using (WriteTransaction transaction =
            await _database.BeginWriteTransactionAsync(ct))
        {
            await transaction.ExecuteAsync(
                "ALTER TABLE rollback_indexed_collation " +
                "ALTER COLUMN code SET COLLATION NOCASE",
                ct);
            await transaction.RollbackAsync(ct);
        }

        Assert.Equal(
            originalTableRoot,
            _database.GetTableRootPage("rollback_indexed_collation"));
        Assert.Equal(
            originalIndexRoot,
            (await GetIndexRootPagesAsync(ct))["ix_rollback_indexed_collation_code"]);
        Assert.Equal(
            "BINARY",
            _database.GetTableSchema("rollback_indexed_collation")!.Columns[1].Collation);
        Assert.Equal(0L, await CountMatchingCodeAsync("rollback_indexed_collation", ct));
    }

    [Fact]
    public async Task AddPhysicalIntegerPrimaryKey_RekeysRowsAndRebuildsRelationalIndexes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE rekey_parent (id INTEGER PRIMARY KEY)",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO rekey_parent VALUES (1), (2)",
            ct);
        await _database.ExecuteAsync(
            "CREATE TABLE rekey_items (" +
            "id INTEGER NOT NULL, code TEXT NOT NULL, parent_id INTEGER, payload TEXT, " +
            "CONSTRAINT uq_rekey_items_code UNIQUE (code), " +
            "CONSTRAINT fk_rekey_items_parent FOREIGN KEY (parent_id) " +
            "REFERENCES rekey_parent(id))",
            ct);
        await _database.ExecuteAsync(
            "CREATE INDEX ix_rekey_items_payload ON rekey_items (payload)",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO rekey_items VALUES " +
            "(10, 'ten', 1, 'first'), (20, 'twenty', 2, 'second')",
            ct);

        uint originalTableRoot = _database.GetTableRootPage("rekey_items");
        string[] tableIndexNames = _database.GetIndexes()
            .Where(index => string.Equals(
                index.TableName,
                "rekey_items",
                StringComparison.OrdinalIgnoreCase))
            .Select(index => index.IndexName)
            .ToArray();
        Assert.Equal(3, tableIndexNames.Length);
        IReadOnlyDictionary<string, uint> originalIndexRoots =
            await GetIndexRootPagesAsync(ct);

        await _database.ExecuteAsync(
            "ALTER TABLE rekey_items " +
            "ADD CONSTRAINT pk_rekey_items PRIMARY KEY (id)",
            ct);

        Assert.NotEqual(
            originalTableRoot,
            _database.GetTableRootPage("rekey_items"));
        IReadOnlyDictionary<string, uint> rekeyedIndexRoots =
            await GetIndexRootPagesAsync(ct);
        Assert.All(
            tableIndexNames,
            indexName => Assert.NotEqual(
                originalIndexRoots[indexName],
                rekeyedIndexRoots[indexName]));

        ColumnDefinition idColumn =
            _database.GetTableSchema("rekey_items")!.Columns[0];
        Assert.True(idColumn.IsPrimaryKey);
        Assert.True(idColumn.IsIdentity);
        Assert.False(idColumn.Nullable);
        await using (QueryResult rows = await _database.ExecuteAsync(
            "SELECT id, code, parent_id FROM rekey_items ORDER BY id",
            ct))
        {
            List<DbValue[]> values = await rows.ToListAsync(ct);
            Assert.Equal([10L, 20L], values.Select(row => row[0].AsInteger));
        }

        await _database.ExecuteAsync(
            "INSERT INTO rekey_items (code, parent_id, payload) " +
            "VALUES ('twenty-one', 1, 'generated')",
            ct);
        await using (QueryResult generated = await _database.ExecuteAsync(
            "SELECT id FROM rekey_items WHERE code = 'twenty-one'",
            ct))
        {
            Assert.Equal(
                21L,
                Assert.Single(await generated.ToListAsync(ct))[0].AsInteger);
        }

        await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _database.ExecuteAsync(
                "INSERT INTO rekey_items (id, code, parent_id) " +
                "VALUES (30, 'ten', 1)",
                ct));
        await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _database.ExecuteAsync(
                "INSERT INTO rekey_items (id, code, parent_id) " +
                "VALUES (30, 'thirty', 999)",
                ct));

        await _database.DisposeAsync();
        _database = await Database.OpenAsync(_databasePath, ct);

        Assert.True(
            _database.GetTableSchema("rekey_items")!.Columns[0].IsPrimaryKey);
        await using QueryResult reopened = await _database.ExecuteAsync(
            "SELECT code FROM rekey_items WHERE id = 20",
            ct);
        Assert.Equal(
            "twenty",
            Assert.Single(await reopened.ToListAsync(ct))[0].AsText);
    }

    [Fact]
    public async Task AddPhysicalIntegerPrimaryKey_DuplicateValuesLeaveOriginalRootsUsable()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE failed_rekey_items (" +
            "id INTEGER NOT NULL, code TEXT NOT NULL)",
            ct);
        await _database.ExecuteAsync(
            "CREATE INDEX ix_failed_rekey_code ON failed_rekey_items (code)",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO failed_rekey_items VALUES " +
            "(10, 'first'), (10, 'second')",
            ct);

        uint originalTableRoot = _database.GetTableRootPage("failed_rekey_items");
        uint originalIndexRoot =
            (await GetIndexRootPagesAsync(ct))["ix_failed_rekey_code"];

        CSharpDbException failure = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _database.ExecuteAsync(
                "ALTER TABLE failed_rekey_items " +
                "ADD CONSTRAINT pk_failed_rekey PRIMARY KEY (id)",
                ct));

        Assert.Equal(ErrorCode.ConstraintViolation, failure.Code);
        Assert.Contains("duplicate", failure.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(
            originalTableRoot,
            _database.GetTableRootPage("failed_rekey_items"));
        Assert.Equal(
            originalIndexRoot,
            (await GetIndexRootPagesAsync(ct))["ix_failed_rekey_code"]);
        Assert.DoesNotContain(
            _database.GetTableSchema("failed_rekey_items")!.KeyConstraints,
            key => key.Kind == KeyConstraintKind.PrimaryKey);

        await _database.DisposeAsync();
        _database = await Database.OpenAsync(_databasePath, ct);

        await using QueryResult indexedLookup = await _database.ExecuteAsync(
            "SELECT id FROM failed_rekey_items WHERE code = 'second'",
            ct);
        Assert.Equal(
            10L,
            Assert.Single(await indexedLookup.ToListAsync(ct))[0].AsInteger);
    }

    [Fact]
    public async Task AddPhysicalIntegerPrimaryKey_FullTextIndexRejectsBeforeChangingRoots()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE fulltext_rekey_items (" +
            "id INTEGER NOT NULL, body TEXT NOT NULL)",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO fulltext_rekey_items VALUES " +
            "(10, 'searchable first'), (20, 'searchable second')",
            ct);
        await _database.EnsureFullTextIndexAsync(
            "fts_fulltext_rekey_items",
            "fulltext_rekey_items",
            ["body"],
            ct: ct);

        uint originalTableRoot =
            _database.GetTableRootPage("fulltext_rekey_items");
        string[] tableIndexNames = _database.GetIndexes()
            .Where(index => string.Equals(
                index.TableName,
                "fulltext_rekey_items",
                StringComparison.OrdinalIgnoreCase))
            .Select(index => index.IndexName)
            .ToArray();
        IReadOnlyDictionary<string, uint> originalIndexRoots =
            await GetIndexRootPagesAsync(ct);

        CSharpDbException failure = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _database.ExecuteAsync(
                "ALTER TABLE fulltext_rekey_items " +
                "ADD CONSTRAINT pk_fulltext_rekey_items PRIMARY KEY (id)",
                ct));

        Assert.Equal(ErrorCode.ConstraintViolation, failure.Code);
        Assert.Contains("physically rekey", failure.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(
            originalTableRoot,
            _database.GetTableRootPage("fulltext_rekey_items"));
        IReadOnlyDictionary<string, uint> unchangedIndexRoots =
            await GetIndexRootPagesAsync(ct);
        Assert.All(
            tableIndexNames,
            indexName => Assert.Equal(
                originalIndexRoots[indexName],
                unchangedIndexRoots[indexName]));
        Assert.DoesNotContain(
            _database.GetTableSchema("fulltext_rekey_items")!.KeyConstraints,
            key => key.Kind == KeyConstraintKind.PrimaryKey);

        await _database.DisposeAsync();
        _database = await Database.OpenAsync(_databasePath, ct);

        FullTextSearchHit hit = Assert.Single(
            await _database.SearchAsync(
                "fts_fulltext_rekey_items",
                "second",
                ct));
        Assert.Equal(2L, hit.RowId);
    }

    [Theory]
    [InlineData("primary", "primary-key")]
    [InlineData("key", "uq_alter_dependency_value")]
    [InlineData("index", "ix_alter_dependency_value")]
    [InlineData("outgoing-fk", "fk_alter_dependency_parent")]
    [InlineData("incoming-fk", "fk_alter_dependency_child")]
    [InlineData("view", "alter_dependency_view")]
    [InlineData("trigger", "trg_alter_dependency")]
    [InlineData("validation", "rule_alter_dependency")]
    public async Task AlterColumnShape_RejectsUnsupportedDependenciesBeforeRewrite(
        string dependencyKind,
        string expectedDependency)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        if (dependencyKind == "outgoing-fk")
        {
            await _database.ExecuteAsync(
                "CREATE TABLE alter_dependency_parent (id INTEGER PRIMARY KEY)",
                ct);
        }

        string keyClause = dependencyKind is "key" or "incoming-fk"
            ? ", CONSTRAINT uq_alter_dependency_value UNIQUE (value)"
            : string.Empty;
        string foreignKeyClause = dependencyKind == "outgoing-fk"
            ? ", CONSTRAINT fk_alter_dependency_parent " +
              "FOREIGN KEY (value) REFERENCES alter_dependency_parent(id)"
            : string.Empty;
        await _database.ExecuteAsync(
            "CREATE TABLE alter_dependency_items (" +
            "id INTEGER PRIMARY KEY, value INTEGER, untouched TEXT" +
            keyClause +
            foreignKeyClause +
            ")",
            ct);

        switch (dependencyKind)
        {
            case "index":
                await _database.ExecuteAsync(
                    "CREATE INDEX ix_alter_dependency_value " +
                    "ON alter_dependency_items (value)",
                    ct);
                break;
            case "incoming-fk":
                await _database.ExecuteAsync(
                    "CREATE TABLE alter_dependency_child (" +
                    "id INTEGER PRIMARY KEY, dependency_value INTEGER, " +
                    "CONSTRAINT fk_alter_dependency_child " +
                    "FOREIGN KEY (dependency_value) " +
                    "REFERENCES alter_dependency_items(value))",
                    ct);
                break;
            case "view":
                await _database.ExecuteAsync(
                    "CREATE VIEW alter_dependency_view AS " +
                    "SELECT value FROM alter_dependency_items",
                    ct);
                break;
            case "trigger":
                await _database.ExecuteAsync(
                    "CREATE TABLE alter_dependency_audit (id INTEGER)",
                    ct);
                await _database.ExecuteAsync(
                    "CREATE TRIGGER trg_alter_dependency " +
                    "AFTER INSERT ON alter_dependency_items " +
                    "BEGIN INSERT INTO alter_dependency_audit VALUES (NEW.id); END",
                    ct);
                break;
            case "validation":
                await _database.ExecuteAsync(
                    "CREATE VALIDATION RULE rule_alter_dependency " +
                    "ON alter_dependency_items.value " +
                    "AS value >= 0 MESSAGE 'value must be non-negative'",
                    ct);
                break;
        }

        string targetColumn = dependencyKind == "primary" ? "id" : "value";
        uint originalRootPage = _database.GetTableRootPage("alter_dependency_items");
        CSharpDbException failure = await Assert.ThrowsAsync<CSharpDbException>(
            async () => await _database.ExecuteAsync(
                "ALTER TABLE alter_dependency_items " +
                $"ALTER COLUMN {targetColumn} TYPE REAL",
                ct));

        Assert.Equal(ErrorCode.ConstraintViolation, failure.Code);
        Assert.Contains(expectedDependency, failure.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(originalRootPage, _database.GetTableRootPage("alter_dependency_items"));
        Assert.Equal(
            DbType.Integer,
            _database.GetTableSchema("alter_dependency_items")!
                .Columns.Single(column => string.Equals(
                    column.Name,
                    targetColumn,
                    StringComparison.OrdinalIgnoreCase))
                .Type);
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

    private async Task AssertNumericRewriteRowsAsync(
        DbType expectedType,
        CancellationToken ct)
    {
        await using QueryResult result = await _database.ExecuteAsync(
            "SELECT amount, tag FROM numeric_rewrite ORDER BY id",
            ct);
        List<DbValue[]> rows = await result.ToListAsync(ct);
        Assert.Equal(2, rows.Count);
        Assert.All(rows, row => Assert.Equal(expectedType, row[0].Type));
        if (expectedType == DbType.Real)
        {
            Assert.Equal(10d, rows[0][0].AsReal);
            Assert.Equal(25d, rows[1][0].AsReal);
        }
        else
        {
            Assert.Equal(10L, rows[0][0].AsInteger);
            Assert.Equal(25L, rows[1][0].AsInteger);
        }
        Assert.Equal("ten", rows[0][1].AsText);
        Assert.Equal("twenty-five", rows[1][1].AsText);
    }

    private async Task<long> CountMatchingCodeAsync(
        string tableName,
        CancellationToken ct)
    {
        await using QueryResult result = await _database.ExecuteAsync(
            $"SELECT COUNT(*) FROM {tableName} WHERE code = 'alpha'",
            ct);
        return Assert.Single(await result.ToListAsync(ct))[0].AsInteger;
    }

    private async Task<IReadOnlyDictionary<string, uint>> GetIndexRootPagesAsync(
        CancellationToken ct)
    {
        IndexInspectReport report = await IndexInspector.CheckAsync(
            _databasePath,
            ct: ct);
        Assert.DoesNotContain(
            report.Issues,
            issue => issue.Severity == InspectSeverity.Error);
        return report.Indexes.ToDictionary(
            index => index.IndexName,
            index => index.RootPage,
            StringComparer.OrdinalIgnoreCase);
    }
}
