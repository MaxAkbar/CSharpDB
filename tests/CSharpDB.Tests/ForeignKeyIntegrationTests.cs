using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class ForeignKeyIntegrationTests : IAsyncLifetime
{
    private readonly string _dbPath;
    private Database _db = null!;

    public ForeignKeyIntegrationTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_fk_test_{Guid.NewGuid():N}.db");
    }

    public async ValueTask InitializeAsync()
    {
        _db = await Database.OpenAsync(_dbPath, TestContext.Current.CancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        await _db.DisposeAsync();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        if (File.Exists(_dbPath + ".wal")) File.Delete(_dbPath + ".wal");
    }

    [Fact]
    public async Task ForeignKeys_InsertRejectsMissingParent_AndAllowsNullChildValue()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id))", ct);

        await _db.ExecuteAsync("INSERT INTO children VALUES (1, NULL)", ct);

        var error = await Assert.ThrowsAsync<CSharpDbException>(
            () => _db.ExecuteAsync("INSERT INTO children VALUES (2, 999)", ct).AsTask());
        Assert.Equal(ErrorCode.ConstraintViolation, error.Code);

        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM children", ct));
    }

    [Fact]
    public async Task ForeignKeys_DeleteRestrictPreventsDeletingReferencedParent()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id))", ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO children VALUES (1, 1)", ct);

        var error = await Assert.ThrowsAsync<CSharpDbException>(
            () => _db.ExecuteAsync("DELETE FROM parents WHERE id = 1", ct).AsTask());
        Assert.Equal(ErrorCode.ConstraintViolation, error.Code);

        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM parents", ct));
        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM children", ct));
    }

    [Fact]
    public async Task ForeignKeys_DeleteCascadeDeletesDependentRows()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id) ON DELETE CASCADE)", ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO children VALUES (1, 1)", ct);
        await _db.ExecuteAsync("INSERT INTO children VALUES (2, 1)", ct);

        await _db.ExecuteAsync("DELETE FROM parents WHERE id = 1", ct);

        Assert.Equal(0L, await ScalarIntAsync("SELECT COUNT(*) FROM parents", ct));
        Assert.Equal(0L, await ScalarIntAsync("SELECT COUNT(*) FROM children", ct));
    }

    [Fact]
    public async Task ForeignKeys_DeleteSetNull_UpdatesScalarChildrenAndIndexes()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync(
            "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id) ON DELETE SET NULL)",
            ct);
        await _db.ExecuteAsync("CREATE INDEX idx_children_parent_id ON children(parent_id)", ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES (2)", ct);
        await _db.ExecuteAsync("INSERT INTO children VALUES (10, 1)", ct);
        await _db.ExecuteAsync("INSERT INTO children VALUES (20, 2)", ct);

        await _db.ExecuteAsync("DELETE FROM parents WHERE id = 1", ct);

        await using (QueryResult rowsResult =
                     await _db.ExecuteAsync("SELECT id, parent_id FROM children ORDER BY id", ct))
        {
            IReadOnlyList<DbValue[]> rows = await rowsResult.ToListAsync(ct);
            Assert.Equal(2, rows.Count);
            Assert.Equal(10L, rows[0][0].AsInteger);
            Assert.True(rows[0][1].IsNull);
            Assert.Equal(20L, rows[1][0].AsInteger);
            Assert.Equal(2L, rows[1][1].AsInteger);
        }

        Assert.Equal(
            0L,
            await ScalarIntAsync("SELECT COUNT(*) FROM children WHERE parent_id = 1", ct));
        Assert.Equal(
            1L,
            await ScalarIntAsync("SELECT COUNT(*) FROM children WHERE parent_id = 2", ct));

        await _db.ExecuteAsync("INSERT INTO parents VALUES (3)", ct);
        await _db.ExecuteAsync("UPDATE children SET parent_id = 3 WHERE id = 10", ct);
        Assert.Equal(
            1L,
            await ScalarIntAsync("SELECT COUNT(*) FROM children WHERE parent_id = 3", ct));

        await _db.ExecuteAsync("DELETE FROM parents WHERE id = 3", ct);
        Assert.Equal(
            0L,
            await ScalarIntAsync("SELECT COUNT(*) FROM children WHERE parent_id = 3", ct));
        Assert.Equal(
            1L,
            await ScalarIntAsync("SELECT COUNT(*) FROM children WHERE id = 10 AND parent_id IS NULL", ct));
    }

    [Fact]
    public async Task ForeignKeys_DeleteSetNull_NullsEveryCompositeChildColumn()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            "CREATE TABLE parents (tenant_id INTEGER, code TEXT, PRIMARY KEY (tenant_id, code))",
            ct);
        await _db.ExecuteAsync(
            """
            CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent_tenant_id INTEGER,
                parent_code TEXT,
                FOREIGN KEY (parent_tenant_id, parent_code)
                    REFERENCES parents(tenant_id, code)
                    ON DELETE SET NULL
            )
            """,
            ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES (1, 'one')", ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES (2, 'two')", ct);
        await _db.ExecuteAsync("INSERT INTO children VALUES (10, 1, 'one')", ct);
        await _db.ExecuteAsync("INSERT INTO children VALUES (20, 2, 'two')", ct);

        await _db.ExecuteAsync(
            "DELETE FROM parents WHERE tenant_id = 1 AND code = 'one'",
            ct);

        await using QueryResult result =
            await _db.ExecuteAsync(
                "SELECT id, parent_tenant_id, parent_code FROM children ORDER BY id",
                ct);
        IReadOnlyList<DbValue[]> rows = await result.ToListAsync(ct);
        Assert.Equal(2, rows.Count);
        Assert.Equal(10L, rows[0][0].AsInteger);
        Assert.True(rows[0][1].IsNull);
        Assert.True(rows[0][2].IsNull);
        Assert.Equal(20L, rows[1][0].AsInteger);
        Assert.Equal(2L, rows[1][1].AsInteger);
        Assert.Equal("two", rows[1][2].AsText);
    }

    [Fact]
    public async Task ForeignKeys_SetNull_RejectsAnyNonNullableCompositeChildColumn()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            "CREATE TABLE parents (tenant_id INTEGER, code TEXT, PRIMARY KEY (tenant_id, code))",
            ct);

        CSharpDbException error = await Assert.ThrowsAsync<CSharpDbException>(
            () => _db.ExecuteAsync(
                """
                CREATE TABLE children (
                    id INTEGER PRIMARY KEY,
                    parent_tenant_id INTEGER,
                    parent_code TEXT NOT NULL,
                    FOREIGN KEY (parent_tenant_id, parent_code)
                        REFERENCES parents(tenant_id, code)
                        ON DELETE SET NULL
                )
                """,
                ct).AsTask());

        Assert.Equal(ErrorCode.ConstraintViolation, error.Code);
        Assert.Contains("every child column", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("parent_code", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Null(_db.GetTableSchema("children"));
    }

    [Fact]
    public async Task ForeignKeys_SetNull_BlocksMakingChildColumnNotNull()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            "CREATE TABLE parents (id INTEGER PRIMARY KEY)",
            ct);
        await _db.ExecuteAsync(
            "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id) ON DELETE SET NULL)",
            ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO children VALUES (10, 1)", ct);

        CSharpDbException error = await Assert.ThrowsAsync<CSharpDbException>(
            () => _db.ExecuteAsync(
                "ALTER TABLE children ALTER COLUMN parent_id SET NOT NULL",
                ct).AsTask());

        Assert.Equal(ErrorCode.ConstraintViolation, error.Code);
        Assert.Contains("SET NULL", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.True(
            Assert.Single(
                _db.GetTableSchema("children")!.Columns,
                column => column.Name == "parent_id").Nullable);

        await _db.ExecuteAsync("DELETE FROM parents WHERE id = 1", ct);
        Assert.Equal(
            1L,
            await ScalarIntAsync(
                "SELECT COUNT(*) FROM children WHERE id = 10 AND parent_id IS NULL",
                ct));
    }

    [Fact]
    public async Task ForeignKeys_SetNull_BlocksAddingPrimaryKeyOverChildColumn()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            "CREATE TABLE parents (id INTEGER PRIMARY KEY)",
            ct);
        await _db.ExecuteAsync(
            "CREATE TABLE children (parent_id INTEGER REFERENCES parents(id) ON DELETE SET NULL)",
            ct);

        CSharpDbException error = await Assert.ThrowsAsync<CSharpDbException>(
            () => _db.ExecuteAsync(
                "ALTER TABLE children ADD CONSTRAINT pk_children PRIMARY KEY (parent_id)",
                ct).AsTask());

        Assert.Equal(ErrorCode.ConstraintViolation, error.Code);
        Assert.Contains("SET NULL", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Empty(_db.GetTableSchema("children")!.KeyConstraints);
        Assert.True(_db.GetTableSchema("children")!.Columns[0].Nullable);
    }

    [Fact]
    public async Task ForeignKeys_DeleteSetDefault_UsesLiteralDefaultAndMaintainsIndexes()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync(
            """
            CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER NOT NULL DEFAULT 2
                    REFERENCES parents(id) ON DELETE SET DEFAULT
            )
            """,
            ct);
        await _db.ExecuteAsync(
            "CREATE INDEX idx_children_parent_id ON children(parent_id)",
            ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES (2)", ct);
        await _db.ExecuteAsync("INSERT INTO children VALUES (10, 1)", ct);

        await _db.ExecuteAsync("DELETE FROM parents WHERE id = 1", ct);

        Assert.Equal(
            1L,
            await ScalarIntAsync(
                "SELECT COUNT(*) FROM children WHERE id = 10 AND parent_id = 2",
                ct));
        Assert.Equal(
            0L,
            await ScalarIntAsync(
                "SELECT COUNT(*) FROM children WHERE parent_id = 1",
                ct));
    }

    [Fact]
    public async Task ForeignKeys_SetDefaultWithoutUsableDefault_IsRejectedForNotNullChild()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);

        CSharpDbException error = await Assert.ThrowsAsync<CSharpDbException>(
            () => _db.ExecuteAsync(
                """
                CREATE TABLE children (
                    id INTEGER PRIMARY KEY,
                    parent_id INTEGER NOT NULL
                        REFERENCES parents(id) ON UPDATE SET DEFAULT
                )
                """,
                ct).AsTask());

        Assert.Equal(ErrorCode.ConstraintViolation, error.Code);
        Assert.Contains("non-NULL literal default", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Null(_db.GetTableSchema("children"));
    }

    [Fact]
    public async Task ForeignKeys_DeleteSetDefault_InvalidResultRollsBackWholeDelete()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync(
            """
            CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER NOT NULL DEFAULT 1
                    REFERENCES parents(id) ON DELETE SET DEFAULT
            )
            """,
            ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO children VALUES (10, 1)", ct);

        CSharpDbException error = await Assert.ThrowsAsync<CSharpDbException>(
            () => _db.ExecuteAsync(
                "DELETE FROM parents WHERE id = 1",
                ct).AsTask());

        Assert.Equal(ErrorCode.ConstraintViolation, error.Code);
        Assert.Equal(
            1L,
            await ScalarIntAsync("SELECT COUNT(*) FROM parents WHERE id = 1", ct));
        Assert.Equal(
            1L,
            await ScalarIntAsync(
                "SELECT COUNT(*) FROM children WHERE id = 10 AND parent_id = 1",
                ct));
    }

    [Fact]
    public async Task ForeignKeys_DeleteSetDefault_UsesImplicitNullForNullableChild()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync(
            """
            CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER
                    REFERENCES parents(id) ON DELETE SET DEFAULT
            )
            """,
            ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO children VALUES (10, 1)", ct);

        await _db.ExecuteAsync("DELETE FROM parents WHERE id = 1", ct);

        Assert.Equal(
            1L,
            await ScalarIntAsync(
                "SELECT COUNT(*) FROM children WHERE id = 10 AND parent_id IS NULL",
                ct));
    }

    [Fact]
    public async Task ForeignKeys_DeleteSetDefault_CanRekeyNonNullTextPrimaryChild()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id TEXT PRIMARY KEY)", ct);
        await _db.ExecuteAsync(
            """
            CREATE TABLE children (
                parent_id TEXT PRIMARY KEY DEFAULT 'fallback'
                    REFERENCES parents(id) ON DELETE SET DEFAULT
            )
            """,
            ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES ('original')", ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES ('fallback')", ct);
        await _db.ExecuteAsync("INSERT INTO children VALUES ('original')", ct);

        await _db.ExecuteAsync("DELETE FROM parents WHERE id = 'original'", ct);

        Assert.Equal(
            1L,
            await ScalarIntAsync(
                "SELECT COUNT(*) FROM children WHERE parent_id = 'fallback'",
                ct));
        Assert.Equal(
            0L,
            await ScalarIntAsync(
                "SELECT COUNT(*) FROM children WHERE parent_id = 'original'",
                ct));
    }

    [Fact]
    public async Task ForeignKeys_UpdateSetDefault_MissingParentRollsBackWholeStatement()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync(
            """
            CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER NOT NULL DEFAULT 99
                    REFERENCES parents(id) ON UPDATE SET DEFAULT
            )
            """,
            ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO children VALUES (10, 1)", ct);

        CSharpDbException error = await Assert.ThrowsAsync<CSharpDbException>(
            () => _db.ExecuteAsync(
                "UPDATE parents SET id = 2 WHERE id = 1",
                ct).AsTask());

        Assert.Equal(ErrorCode.ConstraintViolation, error.Code);
        Assert.Equal(
            1L,
            await ScalarIntAsync("SELECT COUNT(*) FROM parents WHERE id = 1", ct));
        Assert.Equal(
            1L,
            await ScalarIntAsync(
                "SELECT COUNT(*) FROM children WHERE id = 10 AND parent_id = 1",
                ct));
    }

    [Fact]
    public async Task ForeignKeys_UpdateCascade_PropagatesScalarKeyAndIndexes()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync(
            """
            CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER NOT NULL
                    REFERENCES parents(id) ON UPDATE CASCADE
            )
            """,
            ct);
        await _db.ExecuteAsync(
            "CREATE INDEX idx_children_parent_id ON children(parent_id)",
            ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO children VALUES (10, 1)", ct);

        await _db.ExecuteAsync("UPDATE parents SET id = 3 WHERE id = 1", ct);

        Assert.Equal(
            1L,
            await ScalarIntAsync("SELECT COUNT(*) FROM parents WHERE id = 3", ct));
        Assert.Equal(
            1L,
            await ScalarIntAsync(
                "SELECT COUNT(*) FROM children WHERE id = 10 AND parent_id = 3",
                ct));
        Assert.Equal(
            0L,
            await ScalarIntAsync("SELECT COUNT(*) FROM children WHERE parent_id = 1", ct));
    }

    [Fact]
    public async Task ForeignKeys_UpdateCascade_PropagatesCompositeKey()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            "CREATE TABLE parents (tenant_id INTEGER, code TEXT, PRIMARY KEY (tenant_id, code))",
            ct);
        await _db.ExecuteAsync(
            """
            CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent_tenant_id INTEGER NOT NULL,
                parent_code TEXT NOT NULL,
                FOREIGN KEY (parent_tenant_id, parent_code)
                    REFERENCES parents(tenant_id, code)
                    ON UPDATE CASCADE
            )
            """,
            ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES (1, 'one')", ct);
        await _db.ExecuteAsync("INSERT INTO children VALUES (10, 1, 'one')", ct);

        await _db.ExecuteAsync(
            "UPDATE parents SET tenant_id = 2, code = 'two' WHERE tenant_id = 1",
            ct);

        Assert.Equal(
            1L,
            await ScalarIntAsync(
                "SELECT COUNT(*) FROM children WHERE parent_tenant_id = 2 AND parent_code = 'two'",
                ct));
    }

    [Fact]
    public async Task ForeignKeys_UpdateSetNullAndSetDefault_ApplyImmediateActions()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync(
            """
            CREATE TABLE null_children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER REFERENCES parents(id) ON UPDATE SET NULL
            )
            """,
            ct);
        await _db.ExecuteAsync(
            """
            CREATE TABLE default_children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER NOT NULL DEFAULT 2
                    REFERENCES parents(id) ON UPDATE SET DEFAULT
            )
            """,
            ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES (2)", ct);
        await _db.ExecuteAsync("INSERT INTO null_children VALUES (10, 1)", ct);
        await _db.ExecuteAsync("INSERT INTO default_children VALUES (20, 1)", ct);

        await _db.ExecuteAsync("UPDATE parents SET id = 3 WHERE id = 1", ct);

        Assert.Equal(
            1L,
            await ScalarIntAsync(
                "SELECT COUNT(*) FROM null_children WHERE id = 10 AND parent_id IS NULL",
                ct));
        Assert.Equal(
            1L,
            await ScalarIntAsync(
                "SELECT COUNT(*) FROM default_children WHERE id = 20 AND parent_id = 2",
                ct));
    }

    [Fact]
    public async Task ForeignKeys_UpdateCascade_RepairsSelfReference()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            """
            CREATE TABLE nodes (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER REFERENCES nodes(id) ON UPDATE CASCADE
            )
            """,
            ct);
        await _db.ExecuteAsync("INSERT INTO nodes VALUES (1, 1)", ct);

        await _db.ExecuteAsync("UPDATE nodes SET id = 2 WHERE id = 1", ct);

        Assert.Equal(
            1L,
            await ScalarIntAsync(
                "SELECT COUNT(*) FROM nodes WHERE id = 2 AND parent_id = 2",
                ct));
    }

    [Fact]
    public async Task ForeignKeys_UpdateRestrict_SelfReferenceCannotLeaveOldKey()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            """
            CREATE TABLE nodes (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER REFERENCES nodes(id) ON UPDATE RESTRICT
            )
            """,
            ct);
        await _db.ExecuteAsync("INSERT INTO nodes VALUES (1, 1)", ct);

        CSharpDbException error = await Assert.ThrowsAsync<CSharpDbException>(
            () => _db.ExecuteAsync(
                "UPDATE nodes SET id = 2 WHERE id = 1",
                ct).AsTask());

        Assert.Equal(ErrorCode.ConstraintViolation, error.Code);
        Assert.Equal(
            1L,
            await ScalarIntAsync(
                "SELECT COUNT(*) FROM nodes WHERE id = 1 AND parent_id = 1",
                ct));
    }

    [Fact]
    public async Task ForeignKeys_UpdateSetNull_RejectsNonNullableAndPrimaryKeyChildren()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);

        CSharpDbException notNullError = await Assert.ThrowsAsync<CSharpDbException>(
            () => _db.ExecuteAsync(
                """
                CREATE TABLE required_children (
                    id INTEGER PRIMARY KEY,
                    parent_id INTEGER NOT NULL
                        REFERENCES parents(id) ON UPDATE SET NULL
                )
                """,
                ct).AsTask());
        Assert.Equal(ErrorCode.ConstraintViolation, notNullError.Code);
        Assert.Contains("nullable", notNullError.Message, StringComparison.OrdinalIgnoreCase);

        CSharpDbException primaryKeyError = await Assert.ThrowsAsync<CSharpDbException>(
            () => _db.ExecuteAsync(
                """
                CREATE TABLE keyed_children (
                    parent_id INTEGER PRIMARY KEY
                        REFERENCES parents(id) ON UPDATE SET NULL
                )
                """,
                ct).AsTask());
        Assert.Equal(ErrorCode.ConstraintViolation, primaryKeyError.Code);
        Assert.Contains("outside the primary key", primaryKeyError.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ForeignKeys_SetDefault_BlocksDroppingRequiredDefault()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync(
            """
            CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER NOT NULL DEFAULT 2
                    REFERENCES parents(id) ON DELETE SET DEFAULT
            )
            """,
            ct);

        CSharpDbException error = await Assert.ThrowsAsync<CSharpDbException>(
            () => _db.ExecuteAsync(
                "ALTER TABLE children ALTER COLUMN parent_id DROP DEFAULT",
                ct).AsTask());

        Assert.Equal(ErrorCode.ConstraintViolation, error.Code);
        ColumnDefinition parentId = Assert.Single(
            _db.GetTableSchema("children")!.Columns,
            column => column.Name == "parent_id");
        Assert.Equal("2", parentId.DefaultSql);
    }

    [Fact]
    public async Task ForeignKeys_DeleteSetDefault_AppliesCompositeDefaultTuple()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            "CREATE TABLE parents (tenant_id INTEGER, code TEXT, PRIMARY KEY (tenant_id, code))",
            ct);
        await _db.ExecuteAsync(
            """
            CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent_tenant_id INTEGER NOT NULL DEFAULT 2,
                parent_code TEXT NOT NULL DEFAULT 'fallback',
                FOREIGN KEY (parent_tenant_id, parent_code)
                    REFERENCES parents(tenant_id, code)
                    ON DELETE SET DEFAULT
            )
            """,
            ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES (1, 'one')", ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES (2, 'fallback')", ct);
        await _db.ExecuteAsync("INSERT INTO children VALUES (10, 1, 'one')", ct);

        await _db.ExecuteAsync(
            "DELETE FROM parents WHERE tenant_id = 1 AND code = 'one'",
            ct);

        Assert.Equal(
            1L,
            await ScalarIntAsync(
                "SELECT COUNT(*) FROM children WHERE parent_tenant_id = 2 AND parent_code = 'fallback'",
                ct));
    }

    [Fact]
    public async Task ForeignKeys_UpdateCascade_CheckFailureRollsBackWholeStatement()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync(
            """
            CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER NOT NULL
                    REFERENCES parents(id) ON UPDATE CASCADE,
                CHECK (parent_id < 5)
            )
            """,
            ct);
        await _db.ExecuteAsync(
            "CREATE TABLE update_log (id INTEGER PRIMARY KEY)",
            ct);
        await _db.ExecuteAsync(
            """
            CREATE TRIGGER parent_before_update BEFORE UPDATE ON parents
            BEGIN
                INSERT INTO update_log VALUES (OLD.id);
            END
            """,
            ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO children VALUES (10, 1)", ct);

        CSharpDbException error = await Assert.ThrowsAsync<CSharpDbException>(
            () => _db.ExecuteAsync(
                "UPDATE parents SET id = 10 WHERE id = 1",
                ct).AsTask());

        Assert.Equal(ErrorCode.ConstraintViolation, error.Code);
        Assert.Equal(
            1L,
            await ScalarIntAsync("SELECT COUNT(*) FROM parents WHERE id = 1", ct));
        Assert.Equal(
            1L,
            await ScalarIntAsync(
                "SELECT COUNT(*) FROM children WHERE id = 10 AND parent_id = 1",
                ct));
        Assert.Equal(0L, await ScalarIntAsync("SELECT COUNT(*) FROM update_log", ct));
    }

    [Fact]
    public async Task ForeignKeys_UpdateCascade_FiresChildTriggersAndAdvancesRowVersion()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync(
            """
            CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER NOT NULL
                    REFERENCES parents(id) ON UPDATE CASCADE,
                version BLOB ROWVERSION NOT NULL
            )
            """,
            ct);
        await _db.ExecuteAsync(
            "CREATE TABLE update_log (id INTEGER PRIMARY KEY, old_parent INTEGER, new_parent INTEGER)",
            ct);
        await _db.ExecuteAsync(
            """
            CREATE TRIGGER child_after_update AFTER UPDATE ON children
            BEGIN
                INSERT INTO update_log VALUES (OLD.id, OLD.parent_id, NEW.parent_id);
            END
            """,
            ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO children (id, parent_id) VALUES (10, 1)", ct);

        byte[] originalVersion;
        await using (QueryResult original =
                     await _db.ExecuteAsync("SELECT version FROM children WHERE id = 10", ct))
        {
            originalVersion = Assert.Single(await original.ToListAsync(ct))[0].AsBlob.ToArray();
        }

        await _db.ExecuteAsync("UPDATE parents SET id = 2 WHERE id = 1", ct);

        await using QueryResult result =
            await _db.ExecuteAsync(
                "SELECT parent_id, version FROM children WHERE id = 10",
                ct);
        DbValue[] child = Assert.Single(await result.ToListAsync(ct));
        Assert.Equal(2L, child[0].AsInteger);
        Assert.False(originalVersion.AsSpan().SequenceEqual(child[1].AsBlob));
        Assert.Equal(
            1L,
            await ScalarIntAsync(
                "SELECT COUNT(*) FROM update_log WHERE old_parent = 1 AND new_parent = 2",
                ct));
    }

    [Fact]
    public async Task ForeignKeys_UpdateCascade_CanChainIntoSetDefault()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE roots (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync(
            """
            CREATE TABLE branches (
                id INTEGER PRIMARY KEY
                    REFERENCES roots(id) ON UPDATE CASCADE
            )
            """,
            ct);
        await _db.ExecuteAsync(
            """
            CREATE TABLE leaves (
                id INTEGER PRIMARY KEY,
                branch_id INTEGER NOT NULL DEFAULT 3
                    REFERENCES branches(id) ON UPDATE SET DEFAULT
            )
            """,
            ct);
        await _db.ExecuteAsync("INSERT INTO roots VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO roots VALUES (3)", ct);
        await _db.ExecuteAsync("INSERT INTO branches VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO branches VALUES (3)", ct);
        await _db.ExecuteAsync("INSERT INTO leaves VALUES (10, 1)", ct);

        await _db.ExecuteAsync("UPDATE roots SET id = 2 WHERE id = 1", ct);

        Assert.Equal(
            1L,
            await ScalarIntAsync("SELECT COUNT(*) FROM branches WHERE id = 2", ct));
        Assert.Equal(
            1L,
            await ScalarIntAsync(
                "SELECT COUNT(*) FROM leaves WHERE id = 10 AND branch_id = 3",
                ct));
    }

    [Fact]
    public async Task ForeignKeys_DeleteSetNull_FiresUpdateTriggersAndAdvancesRowVersion()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync(
            """
            CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER REFERENCES parents(id) ON DELETE SET NULL,
                version BLOB ROWVERSION NOT NULL
            )
            """,
            ct);
        await _db.ExecuteAsync(
            "CREATE TABLE update_log (id INTEGER PRIMARY KEY, phase TEXT, old_parent_id INTEGER, new_parent_id INTEGER)",
            ct);
        await _db.ExecuteAsync(
            """
            CREATE TRIGGER children_before_update BEFORE UPDATE ON children
            BEGIN
                INSERT INTO update_log
                VALUES (1, 'before', OLD.parent_id, NEW.parent_id);
            END
            """,
            ct);
        await _db.ExecuteAsync(
            """
            CREATE TRIGGER children_after_update AFTER UPDATE ON children
            BEGIN
                INSERT INTO update_log
                VALUES (2, 'after', OLD.parent_id, NEW.parent_id);
            END
            """,
            ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO children (id, parent_id) VALUES (10, 1)", ct);

        byte[] originalVersion;
        await using (QueryResult original =
                     await _db.ExecuteAsync("SELECT version FROM children WHERE id = 10", ct))
        {
            originalVersion = Assert.Single(await original.ToListAsync(ct))[0].AsBlob.ToArray();
        }

        await _db.ExecuteAsync("DELETE FROM parents WHERE id = 1", ct);

        byte[] updatedVersion;
        await using (QueryResult updated =
                     await _db.ExecuteAsync("SELECT parent_id, version FROM children WHERE id = 10", ct))
        {
            DbValue[] row = Assert.Single(await updated.ToListAsync(ct));
            Assert.True(row[0].IsNull);
            updatedVersion = row[1].AsBlob.ToArray();
        }
        Assert.False(originalVersion.AsSpan().SequenceEqual(updatedVersion));

        await using QueryResult logResult =
            await _db.ExecuteAsync(
                "SELECT phase, old_parent_id, new_parent_id FROM update_log ORDER BY id",
                ct);
        IReadOnlyList<DbValue[]> logRows = await logResult.ToListAsync(ct);
        Assert.Equal(2, logRows.Count);
        Assert.Equal("before", logRows[0][0].AsText);
        Assert.Equal("after", logRows[1][0].AsText);
        Assert.All(logRows, row =>
        {
            Assert.Equal(1L, row[1].AsInteger);
            Assert.True(row[2].IsNull);
        });
    }

    [Fact]
    public async Task ForeignKeys_DeleteSetNull_RuntimeConstraintFailureRollsBackWholeDelete()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync(
            """
            CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER REFERENCES parents(id) ON DELETE SET NULL,
                CHECK (parent_id IS NOT NULL)
            )
            """,
            ct);
        await _db.ExecuteAsync("CREATE TABLE delete_log (parent_id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync(
            """
            CREATE TRIGGER parents_before_delete BEFORE DELETE ON parents
            BEGIN
                INSERT INTO delete_log VALUES (OLD.id);
            END
            """,
            ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO children VALUES (10, 1)", ct);

        CSharpDbException error = await Assert.ThrowsAsync<CSharpDbException>(
            () => _db.ExecuteAsync("DELETE FROM parents WHERE id = 1", ct).AsTask());

        Assert.Equal(ErrorCode.ConstraintViolation, error.Code);
        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM parents WHERE id = 1", ct));
        Assert.Equal(
            1L,
            await ScalarIntAsync("SELECT COUNT(*) FROM children WHERE id = 10 AND parent_id = 1", ct));
        Assert.Equal(0L, await ScalarIntAsync("SELECT COUNT(*) FROM delete_log", ct));
    }

    [Fact]
    public async Task ForeignKeys_DeleteCascadeChain_CanTerminateInSetNull()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE roots (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync(
            "CREATE TABLE branches (id INTEGER PRIMARY KEY, root_id INTEGER REFERENCES roots(id) ON DELETE CASCADE)",
            ct);
        await _db.ExecuteAsync(
            "CREATE TABLE leaves (id INTEGER PRIMARY KEY, branch_id INTEGER REFERENCES branches(id) ON DELETE SET NULL)",
            ct);
        await _db.ExecuteAsync("INSERT INTO roots VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO branches VALUES (10, 1)", ct);
        await _db.ExecuteAsync("INSERT INTO leaves VALUES (100, 10)", ct);

        await _db.ExecuteAsync("DELETE FROM roots WHERE id = 1", ct);

        Assert.Equal(0L, await ScalarIntAsync("SELECT COUNT(*) FROM roots", ct));
        Assert.Equal(0L, await ScalarIntAsync("SELECT COUNT(*) FROM branches", ct));
        Assert.Equal(
            1L,
            await ScalarIntAsync("SELECT COUNT(*) FROM leaves WHERE id = 100 AND branch_id IS NULL", ct));
    }

    [Fact]
    public async Task ForeignKeys_DeleteCascade_PreservesTwoTableCycleBehavior()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            "CREATE TABLE first_nodes (id INTEGER PRIMARY KEY, second_id INTEGER)",
            ct);
        await _db.ExecuteAsync(
            "CREATE TABLE second_nodes (id INTEGER PRIMARY KEY, first_id INTEGER REFERENCES first_nodes(id) ON DELETE CASCADE)",
            ct);
        await _db.ExecuteAsync(
            """
            ALTER TABLE first_nodes
            ADD CONSTRAINT fk_first_second
            FOREIGN KEY (second_id) REFERENCES second_nodes(id)
            ON DELETE CASCADE
            """,
            ct);
        await _db.ExecuteAsync("INSERT INTO first_nodes VALUES (1, NULL)", ct);
        await _db.ExecuteAsync("INSERT INTO second_nodes VALUES (1, 1)", ct);
        await _db.ExecuteAsync("UPDATE first_nodes SET second_id = 1 WHERE id = 1", ct);

        await _db.ExecuteAsync("DELETE FROM first_nodes WHERE id = 1", ct);

        Assert.Equal(0L, await ScalarIntAsync("SELECT COUNT(*) FROM first_nodes", ct));
        Assert.Equal(0L, await ScalarIntAsync("SELECT COUNT(*) FROM second_nodes", ct));
    }

    [Fact]
    public async Task ForeignKeys_UpdatingReferencedParentKeyIsRejected()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id) ON DELETE CASCADE)", ct);
        await _db.ExecuteAsync("INSERT INTO parents VALUES (1)", ct);
        await _db.ExecuteAsync("INSERT INTO children VALUES (1, 1)", ct);

        var error = await Assert.ThrowsAsync<CSharpDbException>(
            () => _db.ExecuteAsync("UPDATE parents SET id = 2 WHERE id = 1", ct).AsTask());
        Assert.Equal(ErrorCode.ConstraintViolation, error.Code);

        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM parents WHERE id = 1", ct));
        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM children WHERE parent_id = 1", ct));
    }

    [Fact]
    public async Task ForeignKeys_RenameTableAndColumn_RewritesReferencedMetadata()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id) ON DELETE CASCADE)", ct);
        TableSchema originalParent = _db.GetTableSchema("parents")!;
        TableSchema originalChild = _db.GetTableSchema("children")!;
        ForeignKeyDefinition originalForeignKey =
            Assert.Single(originalChild.ForeignKeys);
        Guid parentTableId = originalParent.SchemaId;
        Guid parentColumnId = Assert.Single(originalParent.Columns).SchemaId;
        Guid childColumnId = originalChild.Columns.Single(
            column => column.Name == "parent_id").SchemaId;
        Assert.Equal(parentTableId, originalForeignKey.ReferencedTableSchemaId);
        Assert.Equal([parentColumnId], originalForeignKey.ReferencedColumnSchemaIds);
        Assert.Equal([childColumnId], originalForeignKey.ColumnSchemaIds);
        Assert.NotEqual(Guid.Empty, originalForeignKey.ReferencedKeySchemaId);

        await _db.ExecuteAsync("ALTER TABLE parents RENAME TO accounts", ct);
        await _db.ExecuteAsync("ALTER TABLE accounts RENAME COLUMN id TO account_id", ct);

        Assert.Contains(_db.GetTableNames(), static name => string.Equals(name, "accounts", StringComparison.OrdinalIgnoreCase));
        TableSchema schema = _db.GetTableSchema("children")!;
        ForeignKeyDefinition foreignKey = Assert.Single(schema.ForeignKeys);
        Assert.Equal("accounts", foreignKey.ReferencedTableName);
        Assert.Equal("account_id", foreignKey.ReferencedColumnName);
        Assert.Equal(parentTableId, foreignKey.ReferencedTableSchemaId);
        Assert.Equal([parentColumnId], foreignKey.ReferencedColumnSchemaIds);
        Assert.Equal([childColumnId], foreignKey.ColumnSchemaIds);
        Assert.Equal(
            originalForeignKey.ReferencedKeySchemaId,
            foreignKey.ReferencedKeySchemaId);

        await _db.DisposeAsync();
        _db = await Database.OpenAsync(_dbPath, ct);
        ForeignKeyDefinition reopened =
            Assert.Single(_db.GetTableSchema("children")!.ForeignKeys);
        Assert.Equal(parentTableId, reopened.ReferencedTableSchemaId);
        Assert.Equal([parentColumnId], reopened.ReferencedColumnSchemaIds);
        Assert.Equal([childColumnId], reopened.ColumnSchemaIds);
    }

    [Fact]
    public async Task ForeignKeys_DropParentTableOrBackingUniqueIndex_IsRejected()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY, code TEXT NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_parents_code ON parents(code)", ct);
        await _db.ExecuteAsync("CREATE TABLE children (id INTEGER PRIMARY KEY, parent_code TEXT REFERENCES parents(code))", ct);

        var dropIndexError = await Assert.ThrowsAsync<CSharpDbException>(
            () => _db.ExecuteAsync("DROP INDEX idx_parents_code", ct).AsTask());
        Assert.Equal(ErrorCode.ConstraintViolation, dropIndexError.Code);

        var dropTableError = await Assert.ThrowsAsync<CSharpDbException>(
            () => _db.ExecuteAsync("DROP TABLE parents", ct).AsTask());
        Assert.Equal(ErrorCode.ConstraintViolation, dropTableError.Code);
    }

    [Fact]
    public async Task ForeignKeys_SelfReferencingCascade_DeletesEntireChain()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync(
            "CREATE TABLE nodes (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES nodes(id) ON DELETE CASCADE)",
            ct);

        await _db.ExecuteAsync("INSERT INTO nodes VALUES (1, 1)", ct);
        await _db.ExecuteAsync("INSERT INTO nodes VALUES (2, 1)", ct);
        await _db.ExecuteAsync("INSERT INTO nodes VALUES (3, 2)", ct);

        await _db.ExecuteAsync("DELETE FROM nodes WHERE id = 1", ct);

        Assert.Equal(0L, await ScalarIntAsync("SELECT COUNT(*) FROM nodes", ct));
    }

    [Fact]
    public async Task ForeignKeys_CannotDropHiddenSupportIndexDirectly()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id))", ct);

        TableSchema schema = _db.GetTableSchema("children")!;
        string supportingIndexName = Assert.Single(schema.ForeignKeys).SupportingIndexName;

        var error = await Assert.ThrowsAsync<CSharpDbException>(
            () => _db.ExecuteAsync($"DROP INDEX {supportingIndexName}", ct).AsTask());
        Assert.Equal(ErrorCode.SyntaxError, error.Code);
    }

    [Fact]
    public async Task ForeignKeys_DropConstraint_RemovesEnforcement_AndSupportIndex()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY, code TEXT NOT NULL)", ct);
        await _db.ExecuteAsync("CREATE UNIQUE INDEX idx_parents_code ON parents(code)", ct);
        await _db.ExecuteAsync("CREATE TABLE children (id INTEGER PRIMARY KEY, parent_code TEXT REFERENCES parents(code))", ct);

        TableSchema schema = _db.GetTableSchema("children")!;
        ForeignKeyDefinition foreignKey = Assert.Single(schema.ForeignKeys);

        await _db.ExecuteAsync($"ALTER TABLE children DROP CONSTRAINT {foreignKey.ConstraintName}", ct);

        schema = _db.GetTableSchema("children")!;
        Assert.Empty(schema.ForeignKeys);
        Assert.DoesNotContain(
            _db.GetIndexes(),
            index => string.Equals(index.IndexName, foreignKey.SupportingIndexName, StringComparison.OrdinalIgnoreCase));

        await _db.ExecuteAsync("INSERT INTO children VALUES (1, 'missing-parent')", ct);
        await _db.ExecuteAsync("DROP INDEX idx_parents_code", ct);
        await _db.ExecuteAsync("DROP TABLE parents", ct);

        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM children", ct));
        Assert.Equal(0L, await ScalarIntAsync("SELECT COUNT(*) FROM sys.foreign_keys", ct));
    }

    [Fact]
    public async Task ForeignKeys_DropConstraint_MissingConstraintIsRejected()
    {
        var ct = TestContext.Current.CancellationToken;
        await _db.ExecuteAsync("CREATE TABLE parents (id INTEGER PRIMARY KEY)", ct);
        await _db.ExecuteAsync("CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id))", ct);

        var error = await Assert.ThrowsAsync<CSharpDbException>(
            () => _db.ExecuteAsync("ALTER TABLE children DROP CONSTRAINT fk_missing", ct).AsTask());
        Assert.Equal(ErrorCode.ConstraintViolation, error.Code);
    }

    private async Task<long> ScalarIntAsync(string sql, CancellationToken ct)
    {
        await using var result = await _db.ExecuteAsync(sql, ct);
        DbValue[] row = Assert.Single(await result.ToListAsync(ct));
        return row[0].AsInteger;
    }
}
