using System.Text;
using System.Security.Cryptography;
using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Sql;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.Tests;

public sealed class LogicalKeyParserSerializerTests
{
    [Fact]
    public void Parser_TableLevelPrimaryAndUniqueKeys_PreserveNamesKindsAndColumnOrder()
    {
        var statement = Assert.IsType<CreateTableStatement>(Parser.Parse(
            "CREATE TABLE memberships (" +
            "tenant_id INTEGER, user_code TEXT, shard INTEGER, " +
            "CONSTRAINT pk_memberships PRIMARY KEY (tenant_id, user_code), " +
            "UNIQUE (user_code, shard), " +
            "CONSTRAINT uq_memberships_shard UNIQUE (shard, tenant_id))"));

        Assert.Collection(
            statement.KeyConstraints,
            primary =>
            {
                Assert.Equal("pk_memberships", primary.ConstraintName);
                Assert.Equal(KeyConstraintKind.PrimaryKey, primary.Kind);
                Assert.Equal(["tenant_id", "user_code"], primary.Columns);
            },
            unnamedUnique =>
            {
                Assert.Null(unnamedUnique.ConstraintName);
                Assert.Equal(KeyConstraintKind.Unique, unnamedUnique.Kind);
                Assert.Equal(["user_code", "shard"], unnamedUnique.Columns);
            },
            namedUnique =>
            {
                Assert.Equal("uq_memberships_shard", namedUnique.ConstraintName);
                Assert.Equal(KeyConstraintKind.Unique, namedUnique.Kind);
                Assert.Equal(["shard", "tenant_id"], namedUnique.Columns);
            });
    }

    [Fact]
    public void Serializer_LogicalKeys_RoundTripWithoutInferringCompositeIdentity()
    {
        var schema = new TableSchema
        {
            TableName = "memberships",
            Columns =
            [
                new ColumnDefinition
                {
                    Name = "tenant_id",
                    Type = DbType.Integer,
                    Nullable = false,
                    IsPrimaryKey = true,
                },
                new ColumnDefinition
                {
                    Name = "user_code",
                    Type = DbType.Text,
                    Nullable = false,
                    IsPrimaryKey = true,
                    Collation = "NOCASE",
                },
                new ColumnDefinition
                {
                    Name = "external_id",
                    Type = DbType.Text,
                    Nullable = true,
                },
            ],
            KeyConstraints =
            [
                new KeyConstraintDefinition
                {
                    ConstraintName = "pk_memberships",
                    Kind = KeyConstraintKind.PrimaryKey,
                    Columns = ["tenant_id", "user_code"],
                    BackingIndexName = "__key_memberships_pk_01234567",
                },
                new KeyConstraintDefinition
                {
                    ConstraintName = null,
                    Kind = KeyConstraintKind.Unique,
                    Columns = ["external_id", "tenant_id"],
                    BackingIndexName = "__key_memberships_uq_89abcdef",
                },
            ],
            NextRowId = 19,
        };

        TableSchema decoded = SchemaSerializer.Deserialize(SchemaSerializer.Serialize(schema));

        Assert.Equal(-1, decoded.PrimaryKeyColumnIndex);
        Assert.False(decoded.Columns[0].IsIdentity);
        Assert.False(decoded.Columns[1].IsIdentity);
        Assert.Collection(
            decoded.KeyConstraints,
            primary =>
            {
                Assert.Equal("pk_memberships", primary.ConstraintName);
                Assert.Equal(KeyConstraintKind.PrimaryKey, primary.Kind);
                Assert.Equal(["tenant_id", "user_code"], primary.Columns);
                Assert.Equal("__key_memberships_pk_01234567", primary.BackingIndexName);
            },
            unique =>
            {
                Assert.Null(unique.ConstraintName);
                Assert.Equal(KeyConstraintKind.Unique, unique.Kind);
                Assert.Equal(["external_id", "tenant_id"], unique.Columns);
                Assert.Equal("__key_memberships_uq_89abcdef", unique.BackingIndexName);
            });
    }

    [Fact]
    public void Serializer_Version3Payload_DefaultsLogicalKeysAndPreservesLegacyIntegerIdentity()
    {
        TableSchema decoded = SchemaSerializer.Deserialize(BuildVersion3TablePayload());

        Assert.Equal("legacy_v3", decoded.TableName);
        Assert.Equal(42L, decoded.NextRowId);
        Assert.True(decoded.Columns[0].IsIdentity);
        Assert.Equal("'fallback'", decoded.Columns[1].DefaultSql);
        Assert.Empty(decoded.KeyConstraints);
    }

    private static byte[] BuildVersion3TablePayload()
    {
        using var stream = new MemoryStream();
        WriteString(stream, "legacy_v3");
        WriteVarint(stream, 2);

        WriteString(stream, "id");
        stream.WriteByte((byte)DbType.Integer);
        stream.WriteByte(0x02); // PRIMARY KEY; legacy payload has no explicit identity bit.

        WriteString(stream, "value");
        stream.WriteByte((byte)DbType.Text);
        stream.WriteByte(0x01); // nullable

        WriteVarint(stream, 42); // next row id
        WriteVarint(stream, 3); // metadata version
        WriteVarint(stream, 2); // collation column count
        WriteVarint(stream, 0);
        WriteVarint(stream, 0);
        WriteVarint(stream, 0); // foreign key count
        WriteVarint(stream, 2); // default column count
        WriteVarint(stream, 0);
        WriteNullableString(stream, "'fallback'");
        WriteVarint(stream, 0); // check constraint count
        return stream.ToArray();
    }

    private static void WriteNullableString(Stream stream, string? value)
    {
        if (value is null)
        {
            WriteVarint(stream, 0);
            return;
        }

        byte[] bytes = Encoding.UTF8.GetBytes(value);
        WriteVarint(stream, checked((ulong)bytes.Length + 1));
        stream.Write(bytes);
    }

    private static void WriteString(Stream stream, string value)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(value);
        WriteVarint(stream, (ulong)bytes.Length);
        stream.Write(bytes);
    }

    private static void WriteVarint(Stream stream, ulong value)
    {
        Span<byte> buffer = stackalloc byte[10];
        int length = Varint.Write(buffer, value);
        stream.Write(buffer[..length]);
    }
}

public sealed class LogicalKeyConstraintTests : IAsyncLifetime
{
    private readonly string _dbPath =
        Path.Combine(Path.GetTempPath(), $"csharpdb_logical_keys_{Guid.NewGuid():N}.db");
    private Database _database = null!;

    public async ValueTask InitializeAsync() =>
        _database = await Database.OpenAsync(_dbPath, TestContext.Current.CancellationToken);

    public async ValueTask DisposeAsync()
    {
        await _database.DisposeAsync();
        if (File.Exists(_dbPath))
            File.Delete(_dbPath);
        if (File.Exists(_dbPath + ".wal"))
            File.Delete(_dbPath + ".wal");
    }

    [Fact]
    public async Task CompositePrimaryKey_IsNotIdentityAndRejectsNullsAndDuplicates()
    {
        var ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE memberships (" +
            "tenant_id INTEGER, user_code TEXT COLLATE NOCASE, payload TEXT, " +
            "CONSTRAINT pk_memberships PRIMARY KEY (tenant_id, user_code))",
            ct);

        TableSchema schema = _database.GetTableSchema("memberships")!;
        KeyConstraintDefinition primary = Assert.Single(schema.KeyConstraints);
        Assert.Equal(KeyConstraintKind.PrimaryKey, primary.Kind);
        Assert.Equal(["tenant_id", "user_code"], primary.Columns);
        Assert.NotNull(primary.BackingIndexName);
        Assert.Equal(-1, schema.PrimaryKeyColumnIndex);
        Assert.All(schema.Columns.Take(2), column =>
        {
            Assert.True(column.IsPrimaryKey);
            Assert.False(column.Nullable);
            Assert.False(column.IsIdentity);
        });
        Assert.Contains(
            _database.GetIndexes(),
            index => string.Equals(index.IndexName, primary.BackingIndexName, StringComparison.OrdinalIgnoreCase) &&
                index.Kind == IndexKind.ConstraintInternal &&
                index.IsUnique);

        await _database.ExecuteAsync("INSERT INTO memberships VALUES (1, 'Alpha', 'first')", ct);
        await _database.ExecuteAsync("INSERT INTO memberships VALUES (1, 'Beta', 'second')", ct);

        await AssertWriteRejectedAsync(
            "INSERT INTO memberships VALUES (1, 'alpha', 'duplicate')",
            ct);
        await AssertWriteRejectedAsync(
            "INSERT INTO memberships VALUES (NULL, 'Gamma', 'null tenant')",
            ct);
        await AssertWriteRejectedAsync(
            "INSERT INTO memberships VALUES (2, NULL, 'null code')",
            ct);
        await AssertWriteRejectedAsync(
            "UPDATE memberships SET user_code = 'ALPHA' WHERE user_code = 'Beta'",
            ct);

        Assert.Equal(1L, await ScalarIntAsync(
            "SELECT COUNT(*) FROM memberships WHERE user_code = 'Beta'",
            ct));
    }

    [Fact]
    public async Task CompositeUnique_AllowsNullTuplesAndRejectsCompleteDuplicates()
    {
        var ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE aliases (" +
            "id INTEGER PRIMARY KEY, tenant_id INTEGER, alias TEXT COLLATE NOCASE, " +
            "CONSTRAINT uq_aliases_tenant_alias UNIQUE (tenant_id, alias))",
            ct);

        await _database.ExecuteAsync(
            "INSERT INTO aliases VALUES " +
            "(1, 7, 'North'), (2, NULL, 'North'), (3, NULL, 'North'), " +
            "(4, 7, NULL), (5, 7, NULL)",
            ct);

        await AssertWriteRejectedAsync(
            "INSERT INTO aliases VALUES (6, 7, 'north')",
            ct);
        Assert.Equal(5L, await ScalarIntAsync("SELECT COUNT(*) FROM aliases", ct));
    }

    [Fact]
    public async Task CompositeForeignKey_EnforcesMatchSimple_CascadesAndSurvivesReopen()
    {
        var ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE parents (tenant_id INTEGER, code TEXT COLLATE NOCASE, " +
            "CONSTRAINT pk_parents PRIMARY KEY (tenant_id, code))",
            ct);
        await _database.ExecuteAsync(
            "CREATE TABLE children (id INTEGER PRIMARY KEY, tenant_id INTEGER, parent_code TEXT COLLATE NOCASE, " +
            "CONSTRAINT fk_children_parent FOREIGN KEY (tenant_id, parent_code) " +
            "REFERENCES parents(tenant_id, code) ON DELETE CASCADE)",
            ct);

        await _database.ExecuteAsync("INSERT INTO parents VALUES (7, 'North')", ct);
        await _database.ExecuteAsync("INSERT INTO children VALUES (1, 7, 'north')", ct);
        // MATCH SIMPLE permits an incomplete child key.
        await _database.ExecuteAsync("INSERT INTO children VALUES (2, 7, NULL)", ct);
        await AssertWriteRejectedAsync("INSERT INTO children VALUES (3, 7, 'missing')", ct);

        await AssertWriteRejectedAsync("UPDATE parents SET code = 'South' WHERE tenant_id = 7", ct);
        await _database.ExecuteAsync("DELETE FROM parents WHERE tenant_id = 7", ct);
        Assert.Equal(1L, await ScalarIntAsync("SELECT COUNT(*) FROM children", ct));

        await _database.DisposeAsync();
        _database = await Database.OpenAsync(_dbPath, ct);
        TableSchema schema = _database.GetTableSchema("children")!;
        ForeignKeyDefinition foreignKey = Assert.Single(schema.ForeignKeys);
        Assert.Equal(["tenant_id", "parent_code"], foreignKey.ColumnNames);
        Assert.Equal(["tenant_id", "code"], foreignKey.ReferencedColumnNames);
        Assert.Equal("tenant_id", foreignKey.ColumnName);
        Assert.Equal("tenant_id", foreignKey.ReferencedColumnName);

        await using var catalog = await _database.ExecuteAsync(
            "SELECT column_name, referenced_column_name, ordinal_position " +
            "FROM sys.foreign_keys WHERE constraint_name = 'fk_children_parent' " +
            "ORDER BY ordinal_position",
            ct);
        List<DbValue[]> catalogRows = await catalog.ToListAsync(ct);
        Assert.Collection(
            catalogRows,
            row =>
            {
                Assert.Equal("tenant_id", row[0].AsText);
                Assert.Equal("tenant_id", row[1].AsText);
                Assert.Equal(1L, row[2].AsInteger);
            },
            row =>
            {
                Assert.Equal("parent_code", row[0].AsText);
                Assert.Equal("code", row[1].AsText);
                Assert.Equal(2L, row[2].AsInteger);
            });

        await using var objects = await _database.ExecuteAsync(
            "SELECT object_name FROM sys.objects",
            ct);
        int materializedObjectCount = (await objects.ToListAsync(ct)).Count;
        Assert.Equal(
            materializedObjectCount,
            await ScalarIntAsync("SELECT COUNT(*) FROM sys.objects", ct));
    }

    [Fact]
    public async Task CompositeForeignKey_RenamesAllParticipatingColumnsAndBlocksDrops()
    {
        var ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE parents (id INTEGER PRIMARY KEY, tenant_id INTEGER, code TEXT, UNIQUE (tenant_id, code))",
            ct);
        await _database.ExecuteAsync(
            "CREATE TABLE children (id INTEGER PRIMARY KEY, tenant_id INTEGER, parent_code TEXT, " +
            "FOREIGN KEY (tenant_id, parent_code) REFERENCES parents(tenant_id, code))",
            ct);

        await _database.ExecuteAsync("ALTER TABLE parents RENAME COLUMN code TO parent_key", ct);
        await _database.ExecuteAsync("ALTER TABLE children RENAME COLUMN parent_code TO parent_key", ct);
        ForeignKeyDefinition foreignKey = Assert.Single(_database.GetTableSchema("children")!.ForeignKeys);
        Assert.Equal(["tenant_id", "parent_key"], foreignKey.ColumnNames);
        Assert.Equal(["tenant_id", "parent_key"], foreignKey.ReferencedColumnNames);

        await AssertWriteRejectedAsync("ALTER TABLE children DROP COLUMN parent_key", ct);
        await AssertWriteRejectedAsync("ALTER TABLE parents DROP COLUMN parent_key", ct);
    }

    [Fact]
    public async Task CompositeForeignKey_RequiresOrderedCandidateKeyMatchingTypesAndCollations()
    {
        var ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE parents (id INTEGER PRIMARY KEY, tenant_id INTEGER, code TEXT COLLATE NOCASE, " +
            "UNIQUE (tenant_id, code))",
            ct);

        CSharpDbException collationError = await Assert.ThrowsAsync<CSharpDbException>(() => _database.ExecuteAsync(
            "CREATE TABLE bad_collation (tenant_id INTEGER, code TEXT, " +
            "FOREIGN KEY (tenant_id, code) REFERENCES parents(tenant_id, code))", ct).AsTask());
        Assert.Equal(ErrorCode.ConstraintViolation, collationError.Code);

        await _database.ExecuteAsync(
            "CREATE TABLE ordered_parents (left_key INTEGER, right_key INTEGER, UNIQUE (left_key, right_key))",
            ct);
        CSharpDbException orderError = await Assert.ThrowsAsync<CSharpDbException>(() => _database.ExecuteAsync(
            "CREATE TABLE bad_order (left_key INTEGER, right_key INTEGER, " +
            "FOREIGN KEY (left_key, right_key) REFERENCES ordered_parents(right_key, left_key))", ct).AsTask());
        Assert.Equal(ErrorCode.ConstraintViolation, orderError.Code);

        CSharpDbException typeError = await Assert.ThrowsAsync<CSharpDbException>(() => _database.ExecuteAsync(
            "CREATE TABLE bad_type (tenant_id INTEGER, code INTEGER, " +
            "FOREIGN KEY (tenant_id, code) REFERENCES parents(tenant_id, code))", ct).AsTask());
        Assert.Equal(ErrorCode.TypeMismatch, typeError.Code);
    }

    [Fact]
    public async Task TableLevelSingleColumnKeys_PreserveIntegerIdentityButNotTextIdentity()
    {
        var ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE generated_keys (id INTEGER, value TEXT, PRIMARY KEY (id))",
            ct);
        await _database.ExecuteAsync(
            "CREATE TABLE text_keys (code TEXT, value TEXT, PRIMARY KEY (code))",
            ct);

        await _database.ExecuteAsync("INSERT INTO generated_keys (value) VALUES ('generated')", ct);
        Assert.Equal(1L, await ScalarIntAsync("SELECT id FROM generated_keys", ct));

        TableSchema integerSchema = _database.GetTableSchema("generated_keys")!;
        Assert.Equal(0, integerSchema.PrimaryKeyColumnIndex);
        Assert.True(integerSchema.Columns[0].IsIdentity);
        Assert.Null(Assert.Single(integerSchema.KeyConstraints).BackingIndexName);

        TableSchema textSchema = _database.GetTableSchema("text_keys")!;
        Assert.Equal(0, textSchema.PrimaryKeyColumnIndex);
        Assert.False(textSchema.Columns[0].IsIdentity);
        Assert.NotNull(Assert.Single(textSchema.KeyConstraints).BackingIndexName);

        await _database.ExecuteAsync("INSERT INTO text_keys VALUES ('code-1', 'first')", ct);
        await AssertWriteRejectedAsync("INSERT INTO text_keys VALUES ('code-1', 'duplicate')", ct);
        await AssertWriteRejectedAsync("INSERT INTO text_keys VALUES (NULL, 'null')", ct);
    }

    [Fact]
    public async Task InlineRealPrimaryKey_PreservesLegacyScalarMetadata()
    {
        var ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE legacy_real_key (reading REAL PRIMARY KEY, value TEXT NOT NULL)",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO legacy_real_key VALUES (1.5, 'first'), (2.5, 'second')",
            ct);

        TableSchema schema = _database.GetTableSchema("legacy_real_key")!;
        Assert.Equal(0, schema.PrimaryKeyColumnIndex);
        Assert.True(schema.Columns[0].IsPrimaryKey);
        Assert.False(schema.Columns[0].IsIdentity);
        Assert.Empty(schema.KeyConstraints);
        Assert.Equal(2L, await ScalarIntAsync("SELECT COUNT(*) FROM legacy_real_key", ct));
    }

    [Fact]
    public async Task LogicalKeys_SurviveReopenAndAppearInSystemMetadata()
    {
        var ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE persisted_keys (" +
            "tenant_id INTEGER, code TEXT, external_id TEXT, " +
            "CONSTRAINT pk_persisted_keys PRIMARY KEY (tenant_id, code), " +
            "UNIQUE (external_id, tenant_id))",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO persisted_keys VALUES (1, 'one', 'external')",
            ct);

        await _database.DisposeAsync();
        _database = await Database.OpenAsync(_dbPath, ct);

        TableSchema schema = _database.GetTableSchema("persisted_keys")!;
        Assert.Equal(2, schema.KeyConstraints.Count);
        Assert.All(schema.Columns.Take(2), column => Assert.False(column.IsIdentity));
        await AssertWriteRejectedAsync(
            "INSERT INTO persisted_keys VALUES (1, 'one', 'other')",
            ct);
        await AssertWriteRejectedAsync(
            "INSERT INTO persisted_keys VALUES (1, 'two', 'external')",
            ct);

        await using var metadata = await _database.ExecuteAsync(
            "SELECT constraint_name, constraint_type, column_name, ordinal_position, backing_index_name " +
            "FROM sys.key_constraints " +
            "WHERE table_name = 'persisted_keys' AND constraint_name = 'pk_persisted_keys' " +
            "ORDER BY ordinal_position",
            ct);
        IReadOnlyList<DbValue[]> rows = await metadata.ToListAsync(ct);
        Assert.Collection(
            rows,
            first =>
            {
                Assert.Equal("pk_persisted_keys", first[0].AsText);
                Assert.Equal("PRIMARY KEY", first[1].AsText);
                Assert.Equal("tenant_id", first[2].AsText);
                Assert.Equal(1L, first[3].AsInteger);
                Assert.False(first[4].IsNull);
            },
            second =>
            {
                Assert.Equal("code", second[2].AsText);
                Assert.Equal(2L, second[3].AsInteger);
                Assert.Equal(expected: rows[0][4].AsText, actual: second[4].AsText);
            });

        Assert.Equal(2L, await ScalarIntAsync(
            "SELECT COUNT(*) FROM sys.key_constraints " +
            "WHERE table_name = 'persisted_keys' AND constraint_name IS NULL",
            ct));
    }

    [Fact]
    public async Task RenameAndDropConstraint_UpdateDependenciesAndEnforcement()
    {
        var ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE rename_keys (" +
            "id INTEGER PRIMARY KEY, tenant_id INTEGER, code TEXT, " +
            "CONSTRAINT uq_rename_keys UNIQUE (tenant_id, code))",
            ct);
        await _database.ExecuteAsync("INSERT INTO rename_keys VALUES (1, 4, 'alpha')", ct);

        string backingIndexName = _database.GetTableSchema("rename_keys")!
            .KeyConstraints.Single(key => key.Kind == KeyConstraintKind.Unique)
            .BackingIndexName!;

        await _database.ExecuteAsync("ALTER TABLE rename_keys RENAME COLUMN code TO key_code", ct);
        await _database.ExecuteAsync("ALTER TABLE rename_keys RENAME TO renamed_keys", ct);

        TableSchema renamedSchema = _database.GetTableSchema("renamed_keys")!;
        KeyConstraintDefinition renamedUnique = renamedSchema.KeyConstraints.Single(
            key => key.Kind == KeyConstraintKind.Unique);
        Assert.Equal(["tenant_id", "key_code"], renamedUnique.Columns);
        Assert.Equal(backingIndexName, renamedUnique.BackingIndexName);
        Assert.Contains(
            _database.GetIndexes(),
            index => string.Equals(index.IndexName, backingIndexName, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(index.TableName, "renamed_keys", StringComparison.OrdinalIgnoreCase) &&
                index.Columns.SequenceEqual(["tenant_id", "key_code"]));

        await AssertWriteRejectedAsync(
            "INSERT INTO renamed_keys VALUES (2, 4, 'alpha')",
            ct);

        CSharpDbException dropColumnError = await Assert.ThrowsAsync<CSharpDbException>(
            () => _database.ExecuteAsync(
                "ALTER TABLE renamed_keys DROP COLUMN key_code",
                ct).AsTask());
        Assert.Equal(ErrorCode.ConstraintViolation, dropColumnError.Code);

        CSharpDbException directDropError = await Assert.ThrowsAsync<CSharpDbException>(
            () => _database.ExecuteAsync($"DROP INDEX {backingIndexName}", ct).AsTask());
        Assert.Equal(ErrorCode.SyntaxError, directDropError.Code);

        await _database.ExecuteAsync(
            "ALTER TABLE renamed_keys DROP CONSTRAINT uq_rename_keys",
            ct);
        Assert.DoesNotContain(
            _database.GetTableSchema("renamed_keys")!.KeyConstraints,
            key => key.Kind == KeyConstraintKind.Unique);
        Assert.DoesNotContain(
            _database.GetIndexes(),
            index => string.Equals(index.IndexName, backingIndexName, StringComparison.OrdinalIgnoreCase));

        await _database.ExecuteAsync("INSERT INTO renamed_keys VALUES (2, 4, 'alpha')", ct);
        Assert.Equal(2L, await ScalarIntAsync("SELECT COUNT(*) FROM renamed_keys", ct));
    }

    [Fact]
    public async Task ReferencedUniqueConstraint_CannotBeDroppedWhileForeignKeyDependsOnIt()
    {
        var ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE parent_codes (" +
            "id INTEGER PRIMARY KEY, code TEXT, CONSTRAINT uq_parent_codes UNIQUE (code))",
            ct);
        await _database.ExecuteAsync(
            "CREATE TABLE child_codes (" +
            "id INTEGER PRIMARY KEY, parent_code TEXT REFERENCES parent_codes(code))",
            ct);

        CSharpDbException error = await Assert.ThrowsAsync<CSharpDbException>(
            () => _database.ExecuteAsync(
                "ALTER TABLE parent_codes DROP CONSTRAINT uq_parent_codes",
                ct).AsTask());
        Assert.Equal(ErrorCode.ConstraintViolation, error.Code);
    }

    [Fact]
    public async Task KeyConstraintName_CannotCollideWithGeneratedForeignKeyName()
    {
        var ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE key_name_parents (id INTEGER PRIMARY KEY)",
            ct);

        const string tableName = "key_name_children";
        const string columnName = "parent_id";
        string identity = $"{tableName}|{columnName}|key_name_parents|id";
        string suffix = Convert.ToHexString(
            SHA256.HashData(Encoding.UTF8.GetBytes(identity)),
            0,
            4).ToLowerInvariant();
        string generatedForeignKeyName = $"fk_{tableName}_{columnName}_{suffix}";

        CSharpDbException error = await Assert.ThrowsAsync<CSharpDbException>(
            () => _database.ExecuteAsync(
                $"CREATE TABLE {tableName} (" +
                "id INTEGER PRIMARY KEY, " +
                $"{columnName} INTEGER REFERENCES key_name_parents(id), " +
                $"CONSTRAINT {generatedForeignKeyName} UNIQUE ({columnName}))",
                ct).AsTask());

        Assert.Equal(ErrorCode.SyntaxError, error.Code);
        Assert.Contains("specified multiple times", error.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Null(_database.GetTableSchema(tableName));
    }

    [Fact]
    public async Task Reindex_RebuildsConstraintOwnedIndexAndPreservesEnforcement()
    {
        var ct = TestContext.Current.CancellationToken;
        await _database.ExecuteAsync(
            "CREATE TABLE reindex_keys (" +
            "id INTEGER PRIMARY KEY, tenant_id INTEGER, code TEXT, " +
            "CONSTRAINT uq_reindex_keys UNIQUE (tenant_id, code))",
            ct);
        await _database.ExecuteAsync(
            "INSERT INTO reindex_keys VALUES (1, 7, 'alpha')",
            ct);

        await _database.DisposeAsync();
        DatabaseReindexResult result = await DatabaseMaintenanceCoordinator.ReindexAsync(
            _dbPath,
            new DatabaseReindexRequest
            {
                Scope = DatabaseReindexScope.Table,
                Name = "reindex_keys",
            },
            ct);
        _database = await Database.OpenAsync(_dbPath, ct);

        Assert.Equal(1, result.RebuiltIndexCount);
        await AssertWriteRejectedAsync(
            "INSERT INTO reindex_keys VALUES (2, 7, 'alpha')",
            ct);
    }

    private async Task AssertWriteRejectedAsync(string sql, CancellationToken ct)
    {
        CSharpDbException error = await Assert.ThrowsAsync<CSharpDbException>(
            () => _database.ExecuteAsync(sql, ct).AsTask());
        Assert.True(
            error.Code is ErrorCode.ConstraintViolation or ErrorCode.DuplicateKey,
            $"Expected a constraint error but got {error.Code}: {error.Message}");
    }

    private async Task<long> ScalarIntAsync(string sql, CancellationToken ct)
    {
        await using var result = await _database.ExecuteAsync(sql, ct);
        DbValue[] row = Assert.Single(await result.ToListAsync(ct));
        return row[0].AsInteger;
    }
}
