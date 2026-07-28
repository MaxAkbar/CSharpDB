using System.Reflection;
using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Storage.Catalog;
using CSharpDB.Storage.Serialization;
using CSharpDB.Storage.StorageEngine;

namespace CSharpDB.Tests;

public sealed class SchemaIdentityCatalogTests : IAsyncLifetime
{
    private readonly string _databasePath =
        Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_schema_identity_catalog_{Guid.NewGuid():N}.db");
    private Database _database = null!;

    public async ValueTask InitializeAsync() =>
        _database = await Database.OpenAsync(
            _databasePath,
            TestContext.Current.CancellationToken);

    public async ValueTask DisposeAsync()
    {
        await _database.DisposeAsync();
        DeleteDatabaseFiles(_databasePath);
    }

    [Fact]
    public async Task AddUniqueConstraint_PreservesExistingUnnamedPrimaryKeyIdentity()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE identity_keys (" +
            "id INTEGER, code TEXT, PRIMARY KEY (id))",
            ct);

        Guid primaryKeyId = Assert.Single(
            _database.GetTableSchema("identity_keys")!.KeyConstraints).SchemaId;

        await _database.ExecuteAsync(
            "ALTER TABLE identity_keys " +
            "ADD CONSTRAINT uq_identity_keys_code UNIQUE (code)",
            ct);

        TableSchema updated = _database.GetTableSchema("identity_keys")!;
        Assert.Equal(
            primaryKeyId,
            updated.KeyConstraints.Single(
                key => key.Kind == KeyConstraintKind.PrimaryKey).SchemaId);
        Assert.NotEqual(
            Guid.Empty,
            updated.KeyConstraints.Single(
                key => key.Kind == KeyConstraintKind.Unique).SchemaId);

        await _database.DisposeAsync();
        _database = await Database.OpenAsync(_databasePath, ct);
        Assert.Equal(
            primaryKeyId,
            _database.GetTableSchema("identity_keys")!.KeyConstraints.Single(
                key => key.Kind == KeyConstraintKind.PrimaryKey).SchemaId);
    }

    [Fact]
    public async Task DropReferencedPrimaryKey_RetargetsBindingToEquivalentUniqueKey()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE binding_parents (" +
            "code TEXT, " +
            "CONSTRAINT pk_binding_parents PRIMARY KEY (code), " +
            "CONSTRAINT uq_binding_parents UNIQUE (code))",
            ct);
        await _database.ExecuteAsync(
            "CREATE TABLE binding_children (" +
            "id INTEGER PRIMARY KEY, parent_code TEXT, " +
            "CONSTRAINT fk_binding_children FOREIGN KEY (parent_code) " +
            "REFERENCES binding_parents(code))",
            ct);

        TableSchema parent = _database.GetTableSchema("binding_parents")!;
        Guid primaryKeyId = parent.KeyConstraints.Single(
            key => key.Kind == KeyConstraintKind.PrimaryKey).SchemaId;
        Guid uniqueKeyId = parent.KeyConstraints.Single(
            key => key.Kind == KeyConstraintKind.Unique).SchemaId;
        Assert.Equal(
            primaryKeyId,
            Assert.Single(
                _database.GetTableSchema("binding_children")!.ForeignKeys)
                .ReferencedKeySchemaId);

        await _database.ExecuteAsync(
            "ALTER TABLE binding_parents " +
            "DROP CONSTRAINT pk_binding_parents",
            ct);

        ForeignKeyDefinition retargeted = Assert.Single(
            _database.GetTableSchema("binding_children")!.ForeignKeys);
        Assert.Equal(uniqueKeyId, retargeted.ReferencedKeySchemaId);

        await _database.DisposeAsync();
        _database = await Database.OpenAsync(_databasePath, ct);
        Assert.Equal(
            uniqueKeyId,
            Assert.Single(
                _database.GetTableSchema("binding_children")!.ForeignKeys)
            .ReferencedKeySchemaId);
    }

    [Fact]
    public async Task OrdinaryCatalogUpdate_PreservesExplicitEquivalentReferencedKeyBinding()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE equivalent_key_parent (" +
            "code TEXT, " +
            "CONSTRAINT pk_equivalent_key PRIMARY KEY (code), " +
            "CONSTRAINT uq_equivalent_key UNIQUE (code))",
            ct);
        await _database.ExecuteAsync(
            "CREATE TABLE equivalent_key_child (" +
            "id INTEGER PRIMARY KEY, parent_code TEXT, " +
            "CONSTRAINT fk_equivalent_key FOREIGN KEY (parent_code) " +
            "REFERENCES equivalent_key_parent(code))",
            ct);

        TableSchema parent =
            _database.GetTableSchema("equivalent_key_parent")!;
        TableSchema child =
            _database.GetTableSchema("equivalent_key_child")!;
        Guid uniqueKeyId = parent.KeyConstraints.Single(key =>
            key.Kind == KeyConstraintKind.Unique).SchemaId;
        ForeignKeyDefinition currentForeignKey =
            Assert.Single(child.ForeignKeys);
        var updated = new TableSchema
        {
            SchemaId = child.SchemaId,
            TableName = child.TableName,
            Columns = child.Columns,
            ForeignKeys =
            [
                new ForeignKeyDefinition
                {
                    SchemaId = currentForeignKey.SchemaId,
                    ColumnSchemaIds =
                        currentForeignKey.ColumnSchemaIds,
                    ReferencedTableSchemaId =
                        currentForeignKey.ReferencedTableSchemaId,
                    ReferencedColumnSchemaIds =
                        currentForeignKey.ReferencedColumnSchemaIds,
                    ReferencedKeySchemaId = uniqueKeyId,
                    ConstraintName =
                        currentForeignKey.ConstraintName,
                    ColumnName = currentForeignKey.ColumnName,
                    ReferencedTableName =
                        currentForeignKey.ReferencedTableName,
                    ReferencedColumnName =
                        currentForeignKey.ReferencedColumnName,
                    ColumnNames = currentForeignKey.ColumnNames,
                    ReferencedColumnNames =
                        currentForeignKey.ReferencedColumnNames,
                    OnDelete = currentForeignKey.OnDelete,
                    SupportingIndexName =
                        currentForeignKey.SupportingIndexName,
                },
            ],
            CheckConstraints = child.CheckConstraints,
            KeyConstraints = child.KeyConstraints,
            NextRowId = child.NextRowId,
        };

        await _database.BeginTransactionAsync(ct);
        await GetCatalog(_database).UpdateTableSchemaAsync(
            child.TableName,
            updated,
            ct);
        await _database.CommitAsync(ct);

        Assert.Equal(
            uniqueKeyId,
            Assert.Single(
                _database.GetTableSchema(child.TableName)!.ForeignKeys)
                .ReferencedKeySchemaId);
        await _database.DisposeAsync();
        _database = await Database.OpenAsync(_databasePath, ct);
        Assert.Equal(
            uniqueKeyId,
            Assert.Single(
                _database.GetTableSchema(child.TableName)!.ForeignKeys)
                .ReferencedKeySchemaId);
    }

    [Fact]
    public async Task OrdinaryCatalogUpdates_RejectIdentityReplacementAndCrossObjectDuplicates()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE identity_guard (id INTEGER PRIMARY KEY, value TEXT)",
            ct);

        SchemaCatalog catalog = GetCatalog(_database);
        TableSchema current = _database.GetTableSchema("identity_guard")!;
        Guid replacementId = Guid.NewGuid();
        var replacement = new TableSchema
        {
            SchemaId = replacementId,
            TableName = current.TableName,
            Columns = current.Columns,
            ForeignKeys = current.ForeignKeys,
            CheckConstraints = current.CheckConstraints,
            KeyConstraints = current.KeyConstraints,
            NextRowId = current.NextRowId,
        };

        CSharpDbException replacementError =
            await Assert.ThrowsAsync<CSharpDbException>(
                () => catalog.UpdateTableSchemaAsync(
                    current.TableName,
                    replacement,
                    ct).AsTask());
        Assert.Equal(ErrorCode.ConstraintViolation, replacementError.Code);
        Assert.Equal(
            current.SchemaId,
            _database.GetTableSchema("identity_guard")!.SchemaId);

        var duplicate = new TableSchema
        {
            SchemaId = current.Columns[0].SchemaId,
            TableName = "duplicate_identity",
            Columns =
            [
                new ColumnDefinition
                {
                    SchemaId = Guid.NewGuid(),
                    Name = "id",
                    Type = DbType.Integer,
                    Nullable = false,
                    IsPrimaryKey = true,
                    IsIdentity = true,
                },
            ],
            NextRowId = 1,
        };
        CSharpDbException duplicateError =
            await Assert.ThrowsAsync<CSharpDbException>(
                () => catalog.CreateTableExactAsync(duplicate, ct).AsTask());
        Assert.Equal(ErrorCode.ConstraintViolation, duplicateError.Code);
        Assert.Null(_database.GetTableSchema("duplicate_identity"));
    }

    [Fact]
    public async Task OrdinaryCatalogUpdates_RejectExistingIdentitySwapsAcrossObjects()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE identity_swap_parent_a (id INTEGER PRIMARY KEY)",
            ct);
        await _database.ExecuteAsync(
            "CREATE TABLE identity_swap_parent_b (id INTEGER PRIMARY KEY)",
            ct);
        await _database.ExecuteAsync(
            """
            CREATE TABLE identity_swap_guard (
                id INTEGER PRIMARY KEY,
                left_value INTEGER,
                right_value INTEGER,
                CONSTRAINT ck_identity_swap_left CHECK (left_value >= 0),
                CONSTRAINT ck_identity_swap_right CHECK (right_value >= 0),
                CONSTRAINT uq_identity_swap_left UNIQUE (left_value),
                CONSTRAINT uq_identity_swap_right UNIQUE (right_value),
                CONSTRAINT fk_identity_swap_left
                    FOREIGN KEY (left_value)
                    REFERENCES identity_swap_parent_a(id),
                CONSTRAINT fk_identity_swap_right
                    FOREIGN KEY (right_value)
                    REFERENCES identity_swap_parent_b(id)
            )
            """,
            ct);

        SchemaCatalog catalog = GetCatalog(_database);
        TableSchema current =
            _database.GetTableSchema("identity_swap_guard")!;
        ColumnDefinition leftColumn = current.Columns.Single(column =>
            column.Name == "left_value");
        ColumnDefinition rightColumn = current.Columns.Single(column =>
            column.Name == "right_value");
        ForeignKeyDefinition leftForeignKey =
            current.ForeignKeys.Single(foreignKey =>
                foreignKey.ConstraintName == "fk_identity_swap_left");
        ForeignKeyDefinition rightForeignKey =
            current.ForeignKeys.Single(foreignKey =>
                foreignKey.ConstraintName == "fk_identity_swap_right");
        CheckConstraintDefinition leftCheck =
            current.CheckConstraints.Single(check =>
                check.ConstraintName == "ck_identity_swap_left");
        CheckConstraintDefinition rightCheck =
            current.CheckConstraints.Single(check =>
                check.ConstraintName == "ck_identity_swap_right");
        KeyConstraintDefinition leftKey =
            current.KeyConstraints.Single(key =>
                key.ConstraintName == "uq_identity_swap_left");
        KeyConstraintDefinition rightKey =
            current.KeyConstraints.Single(key =>
                key.ConstraintName == "uq_identity_swap_right");

        TableSchema[] identitySwaps =
        [
            CopySchema(
                current,
                columns: current.Columns.Select(column =>
                    column.Name switch
                    {
                        "left_value" =>
                            CopyColumn(column, rightColumn.SchemaId),
                        "right_value" =>
                            CopyColumn(column, leftColumn.SchemaId),
                        _ => column,
                    }).ToArray()),
            CopySchema(
                current,
                foreignKeys: current.ForeignKeys.Select(foreignKey =>
                    foreignKey.ConstraintName switch
                    {
                        "fk_identity_swap_left" =>
                            CopyForeignKey(
                                foreignKey,
                                rightForeignKey.SchemaId),
                        "fk_identity_swap_right" =>
                            CopyForeignKey(
                                foreignKey,
                                leftForeignKey.SchemaId),
                        _ => foreignKey,
                    }).ToArray()),
            CopySchema(
                current,
                checks: current.CheckConstraints.Select(check =>
                    check.ConstraintName switch
                    {
                        "ck_identity_swap_left" =>
                            CopyCheck(check, rightCheck.SchemaId),
                        "ck_identity_swap_right" =>
                            CopyCheck(check, leftCheck.SchemaId),
                        _ => check,
                    }).ToArray()),
            CopySchema(
                current,
                keys: current.KeyConstraints.Select(key =>
                    key.ConstraintName switch
                    {
                        "uq_identity_swap_left" =>
                            CopyKey(key, rightKey.SchemaId),
                        "uq_identity_swap_right" =>
                            CopyKey(key, leftKey.SchemaId),
                        _ => key,
                    }).ToArray()),
        ];

        foreach (TableSchema identitySwap in identitySwaps)
        {
            CSharpDbException error =
                await Assert.ThrowsAsync<CSharpDbException>(
                    () => catalog.UpdateTableSchemaAsync(
                        current.TableName,
                        identitySwap,
                        ct).AsTask());
            Assert.Equal(ErrorCode.ConstraintViolation, error.Code);
        }

        TableSchema unchanged =
            _database.GetTableSchema(current.TableName)!;
        Assert.Equal(
            current.Columns.Select(static item => item.SchemaId),
            unchanged.Columns.Select(static item => item.SchemaId));
        Assert.Equal(
            current.ForeignKeys.Select(static item => item.SchemaId),
            unchanged.ForeignKeys.Select(static item => item.SchemaId));
        Assert.Equal(
            current.CheckConstraints.Select(static item => item.SchemaId),
            unchanged.CheckConstraints.Select(static item => item.SchemaId));
        Assert.Equal(
            current.KeyConstraints.Select(static item => item.SchemaId),
            unchanged.KeyConstraints.Select(static item => item.SchemaId));
    }

    [Fact]
    public async Task OrdinaryCatalogUpdates_RejectIdentityReassignmentAcrossObjectKinds()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE identity_kind_parent (id INTEGER PRIMARY KEY)",
            ct);
        await _database.ExecuteAsync(
            """
            CREATE TABLE identity_kind_guard (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER,
                constrained_value INTEGER,
                unused_value TEXT,
                CONSTRAINT ck_identity_kind
                    CHECK (constrained_value >= 0),
                CONSTRAINT uq_identity_kind
                    UNIQUE (constrained_value),
                CONSTRAINT fk_identity_kind
                    FOREIGN KEY (parent_id)
                    REFERENCES identity_kind_parent(id)
            )
            """,
            ct);

        SchemaCatalog catalog = GetCatalog(_database);
        TableSchema current =
            _database.GetTableSchema("identity_kind_guard")!;
        ForeignKeyDefinition foreignKey =
            Assert.Single(current.ForeignKeys);
        CheckConstraintDefinition check =
            Assert.Single(current.CheckConstraints);
        KeyConstraintDefinition uniqueKey =
            current.KeyConstraints.Single(key =>
                key.ConstraintName == "uq_identity_kind");
        ColumnDefinition unusedColumn =
            current.Columns.Single(column =>
                column.Name == "unused_value");

        TableSchema[] crossKindReassignments =
        [
            CopySchema(
                current,
                columns:
                [
                    .. current.Columns,
                    new ColumnDefinition
                    {
                        SchemaId = foreignKey.SchemaId,
                        Name = "stolen_fk_identity",
                        Type = DbType.Text,
                        Nullable = true,
                    },
                ],
                foreignKeys: []),
            CopySchema(
                current,
                foreignKeys:
                [
                    .. current.ForeignKeys,
                    new ForeignKeyDefinition
                    {
                        SchemaId = check.SchemaId,
                        ColumnSchemaIds =
                            foreignKey.ColumnSchemaIds,
                        ReferencedTableSchemaId =
                            foreignKey.ReferencedTableSchemaId,
                        ReferencedColumnSchemaIds =
                            foreignKey.ReferencedColumnSchemaIds,
                        ReferencedKeySchemaId =
                            foreignKey.ReferencedKeySchemaId,
                        ConstraintName =
                            "fk_stolen_check_identity",
                        ColumnName = foreignKey.ColumnName,
                        ReferencedTableName =
                            foreignKey.ReferencedTableName,
                        ReferencedColumnName =
                            foreignKey.ReferencedColumnName,
                        ColumnNames = foreignKey.ColumnNames,
                        ReferencedColumnNames =
                            foreignKey.ReferencedColumnNames,
                        OnDelete = foreignKey.OnDelete,
                        SupportingIndexName =
                            "__fk_stolen_check_identity",
                    },
                ],
                checks: []),
            CopySchema(
                current,
                checks:
                [
                    .. current.CheckConstraints,
                    new CheckConstraintDefinition
                    {
                        SchemaId = uniqueKey.SchemaId,
                        ConstraintName =
                            "ck_stolen_key_identity",
                        ExpressionSql = "parent_id >= 0",
                        ColumnName = "parent_id",
                    },
                ],
                keys: current.KeyConstraints.Where(key =>
                    key.SchemaId != uniqueKey.SchemaId).ToArray()),
            CopySchema(
                current,
                columns: current.Columns.Where(column =>
                    column.SchemaId != unusedColumn.SchemaId).ToArray(),
                keys:
                [
                    .. current.KeyConstraints,
                    new KeyConstraintDefinition
                    {
                        SchemaId = unusedColumn.SchemaId,
                        ConstraintName =
                            "uq_stolen_column_identity",
                        Kind = KeyConstraintKind.Unique,
                        Columns = ["parent_id"],
                    },
                ]),
        ];

        foreach (TableSchema reassignment in crossKindReassignments)
        {
            CSharpDbException error =
                await Assert.ThrowsAsync<CSharpDbException>(
                    () => catalog.UpdateTableSchemaAsync(
                        current.TableName,
                        reassignment,
                        ct).AsTask());
            Assert.Equal(ErrorCode.ConstraintViolation, error.Code);
            Assert.Contains(
                "cannot be reassigned",
                error.Message,
                StringComparison.OrdinalIgnoreCase);
        }

        TableSchema unchanged =
            _database.GetTableSchema(current.TableName)!;
        Assert.Equal(
            current.Columns.Select(static item => item.SchemaId),
            unchanged.Columns.Select(static item => item.SchemaId));
        Assert.Equal(
            current.ForeignKeys.Select(static item => item.SchemaId),
            unchanged.ForeignKeys.Select(static item => item.SchemaId));
        Assert.Equal(
            current.CheckConstraints.Select(static item => item.SchemaId),
            unchanged.CheckConstraints.Select(static item => item.SchemaId));
        Assert.Equal(
            current.KeyConstraints.Select(static item => item.SchemaId),
            unchanged.KeyConstraints.Select(static item => item.SchemaId));
    }

    [Fact]
    public async Task OrdinaryCatalogUpdates_RejectColumnIdentityLossDuringRename()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE identity_rename_guard (id INTEGER PRIMARY KEY, value TEXT)",
            ct);

        SchemaCatalog catalog = GetCatalog(_database);
        TableSchema current =
            _database.GetTableSchema("identity_rename_guard")!;
        ColumnDefinition idColumn = current.Columns[0];
        ColumnDefinition valueColumn = current.Columns[1];

        foreach (Guid requestedIdentity in
                 new[] { Guid.Empty, Guid.NewGuid() })
        {
            var renamed = new TableSchema
            {
                SchemaId = current.SchemaId,
                TableName = current.TableName,
                Columns =
                [
                    idColumn,
                    new ColumnDefinition
                    {
                        SchemaId = requestedIdentity,
                        Name = "renamed_value",
                        Type = valueColumn.Type,
                        Nullable = valueColumn.Nullable,
                        IsPrimaryKey = valueColumn.IsPrimaryKey,
                        IsIdentity = valueColumn.IsIdentity,
                        IsRowVersion = valueColumn.IsRowVersion,
                        Collation = valueColumn.Collation,
                        DefaultSql = valueColumn.DefaultSql,
                    },
                ],
                ForeignKeys = current.ForeignKeys,
                CheckConstraints = current.CheckConstraints,
                KeyConstraints = current.KeyConstraints,
                NextRowId = current.NextRowId,
            };

            CSharpDbException error =
                await Assert.ThrowsAsync<CSharpDbException>(
                    () => catalog.UpdateTableSchemaAsync(
                        current.TableName,
                        renamed,
                        ct).AsTask());
            Assert.Equal(ErrorCode.ConstraintViolation, error.Code);
        }

        TableSchema unchanged =
            _database.GetTableSchema("identity_rename_guard")!;
        Assert.Equal(
            current.Columns.Select(column =>
                (column.Name, column.SchemaId)),
            unchanged.Columns.Select(column =>
                (column.Name, column.SchemaId)));
    }

    [Fact]
    public async Task StableForeignKeyBinding_DoesNotRetargetToSameNameReplacement()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE identity_retarget_parent (id INTEGER PRIMARY KEY)",
            ct);
        await _database.ExecuteAsync(
            "CREATE TABLE identity_retarget_child (" +
            "id INTEGER PRIMARY KEY, parent_id INTEGER, " +
            "FOREIGN KEY (parent_id) REFERENCES identity_retarget_parent(id))",
            ct);

        Guid originalParentId =
            _database.GetTableSchema("identity_retarget_parent")!.SchemaId;
        SchemaCatalog catalog = GetCatalog(_database);

        await _database.BeginTransactionAsync(ct);
        try
        {
            await catalog.DropTableAsync(
                "identity_retarget_parent",
                ct);
            CSharpDbException error =
                await Assert.ThrowsAsync<CSharpDbException>(
                    () => catalog.CreateTableAsync(
                        new TableSchema
                        {
                            TableName = "identity_retarget_parent",
                            Columns =
                            [
                                new ColumnDefinition
                                {
                                    Name = "id",
                                    Type = DbType.Integer,
                                    Nullable = false,
                                    IsPrimaryKey = true,
                                    IsIdentity = true,
                                },
                            ],
                        },
                        ct).AsTask());
            Assert.Equal(ErrorCode.ConstraintViolation, error.Code);
        }
        finally
        {
            await _database.RollbackAsync(CancellationToken.None);
        }

        Assert.Equal(
            originalParentId,
            _database.GetTableSchema("identity_retarget_parent")!.SchemaId);
        Assert.Equal(
            originalParentId,
            Assert.Single(
                _database.GetTableSchema("identity_retarget_child")!
                    .ForeignKeys)
                .ReferencedTableSchemaId);
    }

    [Fact]
    public async Task RollbackToEmptyCatalog_ClearsForeignKeyProjectionCaches()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        SchemaCatalog catalog = GetCatalog(_database);

        await _database.BeginTransactionAsync(ct);
        await _database.ExecuteAsync(
            "CREATE TABLE rollback_parent (id INTEGER PRIMARY KEY)",
            ct);
        await _database.ExecuteAsync(
            "CREATE TABLE rollback_child (" +
            "id INTEGER PRIMARY KEY, parent_id INTEGER, " +
            "FOREIGN KEY (parent_id) REFERENCES rollback_parent(id))",
            ct);
        Assert.Single(catalog.GetForeignKeysForTable("rollback_child"));
        Assert.Single(
            catalog.GetReferencingForeignKeys("rollback_parent"));

        await _database.RollbackAsync(ct);

        Assert.Null(_database.GetTableSchema("rollback_parent"));
        Assert.Null(_database.GetTableSchema("rollback_child"));
        Assert.Empty(catalog.GetForeignKeysForTable("rollback_child"));
        Assert.Empty(
            catalog.GetReferencingForeignKeys("rollback_parent"));
    }

    [Fact]
    public async Task TrustedIdentityAdoption_RebindsExternalForeignKeyByLiveName()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE adoption_parent_a (id INTEGER PRIMARY KEY)",
            ct);
        await _database.ExecuteAsync(
            "CREATE TABLE adoption_parent_b (id INTEGER PRIMARY KEY)",
            ct);
        await _database.ExecuteAsync(
            "CREATE TABLE adoption_child (" +
            "id INTEGER PRIMARY KEY, parent_id INTEGER, " +
            "CONSTRAINT fk_adoption_child FOREIGN KEY (parent_id) " +
            "REFERENCES adoption_parent_a(id))",
            ct);

        TableSchema child =
            _database.GetTableSchema("adoption_child")!;
        TableSchema parentA =
            _database.GetTableSchema("adoption_parent_a")!;
        TableSchema parentB =
            _database.GetTableSchema("adoption_parent_b")!;
        ForeignKeyDefinition liveForeignKey =
            Assert.Single(child.ForeignKeys);
        var identitySource = new TableSchema
        {
            SchemaId = child.SchemaId,
            TableName = child.TableName,
            Columns = child.Columns,
            ForeignKeys =
            [
                new ForeignKeyDefinition
                {
                    SchemaId = liveForeignKey.SchemaId,
                    ColumnSchemaIds =
                        liveForeignKey.ColumnSchemaIds,
                    ReferencedTableSchemaId = parentB.SchemaId,
                    ReferencedColumnSchemaIds =
                    [
                        parentB.Columns.Single(column =>
                            column.Name == "id").SchemaId,
                    ],
                    ReferencedKeySchemaId =
                        Assert.Single(parentB.KeyConstraints).SchemaId,
                    ConstraintName =
                        liveForeignKey.ConstraintName,
                    ColumnName = liveForeignKey.ColumnName,
                    ReferencedTableName =
                        liveForeignKey.ReferencedTableName,
                    ReferencedColumnName =
                        liveForeignKey.ReferencedColumnName,
                    ColumnNames = liveForeignKey.ColumnNames,
                    ReferencedColumnNames =
                        liveForeignKey.ReferencedColumnNames,
                    OnDelete = liveForeignKey.OnDelete,
                    SupportingIndexName =
                        liveForeignKey.SupportingIndexName,
                },
            ],
            CheckConstraints = child.CheckConstraints,
            KeyConstraints = child.KeyConstraints,
            NextRowId = child.NextRowId,
        };

        await _database.BeginTransactionAsync(ct);
        await GetCatalog(_database).ApplyTableSchemaIdentitiesAsync(
            child.TableName,
            identitySource,
            ct);
        await _database.CommitAsync(ct);

        ForeignKeyDefinition rebound = Assert.Single(
            _database.GetTableSchema("adoption_child")!.ForeignKeys);
        Assert.Equal(parentA.SchemaId, rebound.ReferencedTableSchemaId);
        Assert.Equal(
            [parentA.Columns.Single(column => column.Name == "id").SchemaId],
            rebound.ReferencedColumnSchemaIds);
        Assert.Equal(
            Assert.Single(parentA.KeyConstraints).SchemaId,
            rebound.ReferencedKeySchemaId);
    }

    [Fact]
    public async Task TrustedIdentityAdoption_RequiresActiveWriteTransaction()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE adoption_transaction_guard (id INTEGER PRIMARY KEY)",
            ct);

        TableSchema live =
            _database.GetTableSchema("adoption_transaction_guard")!;
        InvalidOperationException error =
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => GetCatalog(_database)
                    .ApplyTableSchemaIdentitiesAsync(
                        live.TableName,
                        live,
                        ct)
                    .AsTask());

        Assert.Contains(
            "explicit storage transaction",
            error.Message,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task TrustedIdentityAdoption_MatchesReorderedConstraintsByStructure()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE restore_stage (" +
            "id INTEGER, code TEXT, " +
            "CONSTRAINT ck_restore_id CHECK (id >= 0), " +
            "CONSTRAINT ck_restore_code CHECK (code <> ''), " +
            "CONSTRAINT uq_restore_id UNIQUE (id), " +
            "CONSTRAINT uq_restore_code UNIQUE (code))",
            ct);

        TableSchema live = _database.GetTableSchema("restore_stage")!;
        Guid adoptedTableId = Guid.NewGuid();
        Guid[] adoptedColumnIds = [Guid.NewGuid(), Guid.NewGuid()];
        var sourceChecks = live.CheckConstraints
            .Select(check => new CheckConstraintDefinition
            {
                SchemaId = Guid.NewGuid(),
                ConstraintName = check.ConstraintName,
                ExpressionSql = check.ExpressionSql,
                ColumnName = check.ColumnName,
            })
            .Reverse()
            .ToArray();
        var sourceKeys = live.KeyConstraints
            .Select(key => new KeyConstraintDefinition
            {
                SchemaId = Guid.NewGuid(),
                ConstraintName = key.ConstraintName,
                Kind = key.Kind,
                Columns = key.Columns,
                BackingIndexName = key.BackingIndexName,
            })
            .Reverse()
            .ToArray();
        var identitySource = new TableSchema
        {
            SchemaId = adoptedTableId,
            TableName = "archived_table",
            Columns = live.Columns.Select((column, index) =>
                new ColumnDefinition
                {
                    SchemaId = adoptedColumnIds[index],
                    Name = column.Name,
                    Type = column.Type,
                    Nullable = column.Nullable,
                    IsPrimaryKey = column.IsPrimaryKey,
                    IsIdentity = column.IsIdentity,
                    IsRowVersion = column.IsRowVersion,
                    Collation = column.Collation,
                    DefaultSql = column.DefaultSql,
                }).ToArray(),
            CheckConstraints = sourceChecks,
            KeyConstraints = sourceKeys,
            NextRowId = live.NextRowId,
        };

        await _database.BeginTransactionAsync(ct);
        await GetCatalog(_database).ApplyTableSchemaIdentitiesAsync(
            live.TableName,
            identitySource,
            ct);
        await _database.CommitAsync(ct);

        TableSchema adopted = _database.GetTableSchema(live.TableName)!;
        Assert.Equal(adoptedTableId, adopted.SchemaId);
        Assert.Equal(
            adoptedColumnIds,
            adopted.Columns.Select(column => column.SchemaId));
        foreach (CheckConstraintDefinition source in sourceChecks)
        {
            Assert.Equal(
                source.SchemaId,
                adopted.CheckConstraints.Single(check =>
                    string.Equals(
                        check.ConstraintName,
                        source.ConstraintName,
                        StringComparison.OrdinalIgnoreCase)).SchemaId);
        }
        foreach (KeyConstraintDefinition source in sourceKeys)
        {
            Assert.Equal(
                source.SchemaId,
                adopted.KeyConstraints.Single(key =>
                    string.Equals(
                        key.ConstraintName,
                        source.ConstraintName,
                        StringComparison.OrdinalIgnoreCase)).SchemaId);
        }
    }

    [Fact]
    public async Task TrustedIdentityAdoption_PreservesExactSelfReferencedKeyIdentity()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            """
            CREATE TABLE adoption_self_key (
                id INTEGER,
                parent_id INTEGER,
                CONSTRAINT pk_adoption_self PRIMARY KEY (id),
                CONSTRAINT uq_adoption_self UNIQUE (id),
                CONSTRAINT fk_adoption_self
                    FOREIGN KEY (parent_id)
                    REFERENCES adoption_self_key(id)
            )
            """,
            ct);

        TableSchema live =
            _database.GetTableSchema("adoption_self_key")!;
        Guid sourceTableId = Guid.NewGuid();
        Guid[] sourceColumnIds =
            [Guid.NewGuid(), Guid.NewGuid()];
        KeyConstraintDefinition[] sourceKeys =
            live.KeyConstraints.Select(key =>
                new KeyConstraintDefinition
                {
                    SchemaId = Guid.NewGuid(),
                    ConstraintName = key.ConstraintName,
                    Kind = key.Kind,
                    Columns = key.Columns,
                    BackingIndexName = key.BackingIndexName,
                }).ToArray();
        KeyConstraintDefinition selectedSourceKey =
            sourceKeys.Single(key =>
                key.Kind == KeyConstraintKind.Unique);
        ForeignKeyDefinition liveForeignKey =
            Assert.Single(live.ForeignKeys);
        var identitySource = new TableSchema
        {
            SchemaId = sourceTableId,
            TableName = "archived_self_key",
            Columns = live.Columns.Select((column, index) =>
                new ColumnDefinition
                {
                    SchemaId = sourceColumnIds[index],
                    Name = column.Name,
                    Type = column.Type,
                    Nullable = column.Nullable,
                    IsPrimaryKey = column.IsPrimaryKey,
                    IsIdentity = column.IsIdentity,
                    IsRowVersion = column.IsRowVersion,
                    Collation = column.Collation,
                    DefaultSql = column.DefaultSql,
                }).ToArray(),
            ForeignKeys =
            [
                new ForeignKeyDefinition
                {
                    SchemaId = Guid.NewGuid(),
                    ColumnSchemaIds = [sourceColumnIds[1]],
                    ReferencedTableSchemaId = sourceTableId,
                    ReferencedColumnSchemaIds =
                        [sourceColumnIds[0]],
                    ReferencedKeySchemaId =
                        selectedSourceKey.SchemaId,
                    ConstraintName =
                        liveForeignKey.ConstraintName,
                    ColumnName = liveForeignKey.ColumnName,
                    ColumnNames = liveForeignKey.ColumnNames,
                    ReferencedTableName =
                        "archived_self_key",
                    ReferencedColumnName =
                        liveForeignKey.ReferencedColumnName,
                    ReferencedColumnNames =
                        liveForeignKey.ReferencedColumnNames,
                    OnDelete = liveForeignKey.OnDelete,
                    SupportingIndexName =
                        liveForeignKey.SupportingIndexName,
                },
            ],
            CheckConstraints = live.CheckConstraints,
            KeyConstraints = sourceKeys,
            NextRowId = live.NextRowId,
        };

        await _database.BeginTransactionAsync(ct);
        await GetCatalog(_database).ApplyTableSchemaIdentitiesAsync(
            live.TableName,
            identitySource,
            ct);
        await _database.CommitAsync(ct);

        TableSchema adopted =
            _database.GetTableSchema(live.TableName)!;
        Assert.Equal(
            selectedSourceKey.SchemaId,
            Assert.Single(adopted.ForeignKeys)
                .ReferencedKeySchemaId);
        await _database.DisposeAsync();
        _database = await Database.OpenAsync(_databasePath, ct);
        Assert.Equal(
            selectedSourceKey.SchemaId,
            Assert.Single(
                _database.GetTableSchema(live.TableName)!.ForeignKeys)
                .ReferencedKeySchemaId);
    }

    [Fact]
    public async Task Open_HydratesEmptyLegacyForeignKeyBindingsWithoutPersistingDuringLoad()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.DisposeAsync();

        var strippingSerializer = new EmptyBindingSchemaSerializer();
        _database = await Database.OpenAsync(
            _databasePath,
            CreateOptions(strippingSerializer),
            ct);
        await _database.ExecuteAsync(
            "CREATE TABLE legacy_binding_parent (" +
            "code TEXT, CONSTRAINT uq_legacy_binding_parent UNIQUE (code))",
            ct);
        await _database.ExecuteAsync(
            "CREATE TABLE legacy_binding_child (" +
            "id INTEGER PRIMARY KEY, parent_code TEXT, " +
            "CONSTRAINT fk_legacy_binding_child FOREIGN KEY (parent_code) " +
            "REFERENCES legacy_binding_parent(code))",
            ct);
        await _database.DisposeAsync();

        _database = await Database.OpenAsync(_databasePath, ct);
        AssertHydratedLegacyBinding(_database);
        await _database.DisposeAsync();

        var observingSerializer = new ObservingSchemaSerializer();
        _database = await Database.OpenAsync(
            _databasePath,
            CreateOptions(observingSerializer),
            ct);
        Assert.True(observingSerializer.SawEmptyForeignKeyBindings);
        AssertHydratedLegacyBinding(_database);
    }

    [Fact]
    public async Task Open_RejectsNonemptyForeignKeyColumnBindingMismatch()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE corrupt_binding_parent (id INTEGER PRIMARY KEY)",
            ct);
        await _database.ExecuteAsync(
            "CREATE TABLE corrupt_binding_child (" +
            "id INTEGER PRIMARY KEY, parent_id INTEGER, alternate_id INTEGER, " +
            "CONSTRAINT fk_corrupt_binding FOREIGN KEY (parent_id) " +
            "REFERENCES corrupt_binding_parent(id))",
            ct);
        await _database.DisposeAsync();

        var serializer =
            new MismatchedBindingSchemaSerializer(
                "corrupt_binding_child",
                "alternate_id");
        CSharpDbException error =
            await Assert.ThrowsAsync<CSharpDbException>(
                () => Database.OpenAsync(
                    _databasePath,
                    CreateOptions(serializer),
                    ct).AsTask());
        Assert.Equal(ErrorCode.CorruptDatabase, error.Code);

        _database = await Database.OpenAsync(_databasePath, ct);
    }

    [Fact]
    public async Task Open_RejectsRepeatedLegacyForeignKeyColumns()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE repeated_legacy_parent (" +
            "tenant_id INTEGER, item_id INTEGER, " +
            "PRIMARY KEY (tenant_id, item_id))",
            ct);
        await _database.ExecuteAsync(
            "CREATE TABLE repeated_legacy_child (" +
            "id INTEGER PRIMARY KEY, tenant_id INTEGER, item_id INTEGER, " +
            "FOREIGN KEY (tenant_id, item_id) " +
            "REFERENCES repeated_legacy_parent(tenant_id, item_id))",
            ct);
        await _database.DisposeAsync();

        var serializer =
            new RepeatedLegacyBindingSchemaSerializer(
                "repeated_legacy_child");
        CSharpDbException error =
            await Assert.ThrowsAsync<CSharpDbException>(
                () => Database.OpenAsync(
                    _databasePath,
                    CreateOptions(serializer),
                    ct).AsTask());
        Assert.Equal(ErrorCode.CorruptDatabase, error.Code);

        _database = await Database.OpenAsync(_databasePath, ct);
    }

    private static void AssertHydratedLegacyBinding(Database database)
    {
        TableSchema parent = database.GetTableSchema(
            "legacy_binding_parent")!;
        TableSchema child = database.GetTableSchema(
            "legacy_binding_child")!;
        ForeignKeyDefinition foreignKey = Assert.Single(child.ForeignKeys);
        Assert.Equal(parent.SchemaId, foreignKey.ReferencedTableSchemaId);
        Assert.Equal(
            [child.Columns.Single(column => column.Name == "parent_code").SchemaId],
            foreignKey.ColumnSchemaIds);
        Assert.Equal(
            [parent.Columns.Single(column => column.Name == "code").SchemaId],
            foreignKey.ReferencedColumnSchemaIds);
        Assert.Equal(
            Assert.Single(parent.KeyConstraints).SchemaId,
            foreignKey.ReferencedKeySchemaId);
    }

    private static DatabaseOptions CreateOptions(ISchemaSerializer serializer) =>
        new()
        {
            StorageEngineOptions = new StorageEngineOptions
            {
                SerializerProvider = new TestSerializerProvider(serializer),
            },
        };

    private static SchemaCatalog GetCatalog(Database database)
    {
        FieldInfo field = typeof(Database).GetField(
            "_catalog",
            BindingFlags.Instance | BindingFlags.NonPublic)!;
        return Assert.IsType<SchemaCatalog>(field.GetValue(database));
    }

    private static TableSchema CopySchema(
        TableSchema schema,
        IReadOnlyList<ColumnDefinition>? columns = null,
        IReadOnlyList<ForeignKeyDefinition>? foreignKeys = null,
        IReadOnlyList<CheckConstraintDefinition>? checks = null,
        IReadOnlyList<KeyConstraintDefinition>? keys = null) =>
        new()
        {
            SchemaId = schema.SchemaId,
            TableName = schema.TableName,
            Columns = columns ?? schema.Columns,
            ForeignKeys = foreignKeys ?? schema.ForeignKeys,
            CheckConstraints = checks ?? schema.CheckConstraints,
            KeyConstraints = keys ?? schema.KeyConstraints,
            QualifiedMappings = schema.QualifiedMappings,
            NextRowId = schema.NextRowId,
        };

    private static ColumnDefinition CopyColumn(
        ColumnDefinition column,
        Guid schemaId) =>
        new()
        {
            SchemaId = schemaId,
            Name = column.Name,
            Type = column.Type,
            Nullable = column.Nullable,
            IsPrimaryKey = column.IsPrimaryKey,
            IsIdentity = column.IsIdentity,
            IsRowVersion = column.IsRowVersion,
            Collation = column.Collation,
            DefaultSql = column.DefaultSql,
        };

    private static ForeignKeyDefinition CopyForeignKey(
        ForeignKeyDefinition foreignKey,
        Guid schemaId) =>
        new()
        {
            SchemaId = schemaId,
            ColumnSchemaIds = foreignKey.ColumnSchemaIds,
            ReferencedTableSchemaId =
                foreignKey.ReferencedTableSchemaId,
            ReferencedColumnSchemaIds =
                foreignKey.ReferencedColumnSchemaIds,
            ReferencedKeySchemaId =
                foreignKey.ReferencedKeySchemaId,
            ConstraintName = foreignKey.ConstraintName,
            ColumnName = foreignKey.ColumnName,
            ReferencedTableName = foreignKey.ReferencedTableName,
            ReferencedColumnName = foreignKey.ReferencedColumnName,
            ColumnNames = foreignKey.ColumnNames,
            ReferencedColumnNames =
                foreignKey.ReferencedColumnNames,
            OnDelete = foreignKey.OnDelete,
            SupportingIndexName = foreignKey.SupportingIndexName,
        };

    private static CheckConstraintDefinition CopyCheck(
        CheckConstraintDefinition check,
        Guid schemaId) =>
        new()
        {
            SchemaId = schemaId,
            ConstraintName = check.ConstraintName,
            ExpressionSql = check.ExpressionSql,
            ColumnName = check.ColumnName,
        };

    private static KeyConstraintDefinition CopyKey(
        KeyConstraintDefinition key,
        Guid schemaId) =>
        new()
        {
            SchemaId = schemaId,
            ConstraintName = key.ConstraintName,
            Kind = key.Kind,
            Columns = key.Columns,
            BackingIndexName = key.BackingIndexName,
        };

    private static void DeleteDatabaseFiles(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
        if (File.Exists(path + ".wal"))
            File.Delete(path + ".wal");
    }

    private sealed class TestSerializerProvider : ISerializerProvider
    {
        public TestSerializerProvider(ISchemaSerializer schemaSerializer)
        {
            SchemaSerializer = schemaSerializer;
        }

        public IRecordSerializer RecordSerializer { get; } =
            new DefaultRecordSerializer();

        public ISchemaSerializer SchemaSerializer { get; }
    }

    private class DelegatingSchemaSerializer : ISchemaSerializer
    {
        protected readonly DefaultSchemaSerializer Inner = new();

        public virtual byte[] Serialize(TableSchema schema) =>
            Inner.Serialize(schema);

        public virtual TableSchema Deserialize(ReadOnlySpan<byte> data) =>
            Inner.Deserialize(data);

        public byte[] SerializeIndex(IndexSchema index) =>
            Inner.SerializeIndex(index);

        public IndexSchema DeserializeIndex(ReadOnlySpan<byte> data) =>
            Inner.DeserializeIndex(data);

        public long TableNameToKey(string tableName) =>
            Inner.TableNameToKey(tableName);

        public long IndexNameToKey(string indexName) =>
            Inner.IndexNameToKey(indexName);

        public long ViewNameToKey(string viewName) =>
            Inner.ViewNameToKey(viewName);

        public long TriggerNameToKey(string triggerName) =>
            Inner.TriggerNameToKey(triggerName);

        public byte[] SerializeTrigger(TriggerSchema trigger) =>
            Inner.SerializeTrigger(trigger);

        public TriggerSchema DeserializeTrigger(ReadOnlySpan<byte> data) =>
            Inner.DeserializeTrigger(data);
    }

    private sealed class EmptyBindingSchemaSerializer :
        DelegatingSchemaSerializer
    {
        public override byte[] Serialize(TableSchema schema) =>
            Inner.Serialize(WithoutForeignKeyBindings(schema));
    }

    private sealed class ObservingSchemaSerializer :
        DelegatingSchemaSerializer
    {
        public bool SawEmptyForeignKeyBindings { get; private set; }

        public override TableSchema Deserialize(ReadOnlySpan<byte> data)
        {
            TableSchema schema = base.Deserialize(data);
            SawEmptyForeignKeyBindings |= schema.ForeignKeys.Any(
                foreignKey =>
                    foreignKey.ColumnSchemaIds.Count == 0 &&
                    foreignKey.ReferencedTableSchemaId == Guid.Empty &&
                    foreignKey.ReferencedColumnSchemaIds.Count == 0 &&
                    foreignKey.ReferencedKeySchemaId == Guid.Empty);
            return schema;
        }
    }

    private sealed class MismatchedBindingSchemaSerializer(
        string tableName,
        string wrongColumnName) : DelegatingSchemaSerializer
    {
        public override TableSchema Deserialize(ReadOnlySpan<byte> data)
        {
            TableSchema schema = base.Deserialize(data);
            if (!string.Equals(
                    schema.TableName,
                    tableName,
                    StringComparison.OrdinalIgnoreCase) ||
                schema.ForeignKeys.Count == 0)
            {
                return schema;
            }

            Guid wrongColumnId = schema.Columns.Single(column =>
                string.Equals(
                    column.Name,
                    wrongColumnName,
                    StringComparison.OrdinalIgnoreCase)).SchemaId;
            return new TableSchema
            {
                SchemaId = schema.SchemaId,
                TableName = schema.TableName,
                Columns = schema.Columns,
                ForeignKeys = schema.ForeignKeys.Select(foreignKey =>
                    new ForeignKeyDefinition
                    {
                        SchemaId = foreignKey.SchemaId,
                        ColumnSchemaIds = [wrongColumnId],
                        ReferencedTableSchemaId =
                            foreignKey.ReferencedTableSchemaId,
                        ReferencedColumnSchemaIds =
                            foreignKey.ReferencedColumnSchemaIds,
                        ReferencedKeySchemaId =
                            foreignKey.ReferencedKeySchemaId,
                        ConstraintName = foreignKey.ConstraintName,
                        ColumnName = foreignKey.ColumnName,
                        ReferencedTableName =
                            foreignKey.ReferencedTableName,
                        ReferencedColumnName =
                            foreignKey.ReferencedColumnName,
                        ColumnNames = foreignKey.ColumnNames,
                        ReferencedColumnNames =
                            foreignKey.ReferencedColumnNames,
                        OnDelete = foreignKey.OnDelete,
                        SupportingIndexName =
                            foreignKey.SupportingIndexName,
                    }).ToArray(),
                CheckConstraints = schema.CheckConstraints,
                KeyConstraints = schema.KeyConstraints,
                QualifiedMappings = schema.QualifiedMappings,
                NextRowId = schema.NextRowId,
            };
        }
    }

    private sealed class RepeatedLegacyBindingSchemaSerializer(
        string tableName) : DelegatingSchemaSerializer
    {
        public override TableSchema Deserialize(ReadOnlySpan<byte> data)
        {
            TableSchema schema = base.Deserialize(data);
            if (!string.Equals(
                    schema.TableName,
                    tableName,
                    StringComparison.OrdinalIgnoreCase) ||
                schema.ForeignKeys.Count == 0)
            {
                return schema;
            }

            ForeignKeyDefinition foreignKey =
                Assert.Single(schema.ForeignKeys);
            return new TableSchema
            {
                SchemaId = schema.SchemaId,
                TableName = schema.TableName,
                Columns = schema.Columns,
                ForeignKeys =
                [
                    new ForeignKeyDefinition
                    {
                        SchemaId = foreignKey.SchemaId,
                        ConstraintName =
                            foreignKey.ConstraintName,
                        ColumnName = foreignKey.ColumnName,
                        ColumnNames =
                        [
                            foreignKey.ColumnName,
                            foreignKey.ColumnName,
                        ],
                        ReferencedTableName =
                            foreignKey.ReferencedTableName,
                        ReferencedColumnName =
                            foreignKey.ReferencedColumnName,
                        ReferencedColumnNames =
                            foreignKey.ReferencedColumnNames,
                        OnDelete = foreignKey.OnDelete,
                        SupportingIndexName =
                            foreignKey.SupportingIndexName,
                    },
                ],
                CheckConstraints = schema.CheckConstraints,
                KeyConstraints = schema.KeyConstraints,
                QualifiedMappings = schema.QualifiedMappings,
                NextRowId = schema.NextRowId,
            };
        }
    }

    private static TableSchema WithoutForeignKeyBindings(TableSchema schema) =>
        new()
        {
            SchemaId = schema.SchemaId,
            TableName = schema.TableName,
            Columns = schema.Columns,
            ForeignKeys = schema.ForeignKeys.Select(foreignKey =>
                new ForeignKeyDefinition
                {
                    SchemaId = foreignKey.SchemaId,
                    ConstraintName = foreignKey.ConstraintName,
                    ColumnName = foreignKey.ColumnName,
                    ReferencedTableName = foreignKey.ReferencedTableName,
                    ReferencedColumnName = foreignKey.ReferencedColumnName,
                    ColumnNames = foreignKey.ColumnNames,
                    ReferencedColumnNames =
                        foreignKey.ReferencedColumnNames,
                    OnDelete = foreignKey.OnDelete,
                    SupportingIndexName =
                        foreignKey.SupportingIndexName,
                }).ToArray(),
            CheckConstraints = schema.CheckConstraints,
            KeyConstraints = schema.KeyConstraints,
            QualifiedMappings = schema.QualifiedMappings,
            NextRowId = schema.NextRowId,
        };
}
