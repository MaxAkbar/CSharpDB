using System.Text;
using System.Threading;
using CSharpDB.Client;
using CSharpDB.Client.Internal;
using CSharpDB.Client.Models;
using CSharpDB.Engine;
using CSharpDB.ImportExport.TableArchives;
using PrimitiveDbType = CSharpDB.Primitives.DbType;
using PrimitiveDbValue = CSharpDB.Primitives.DbValue;
using PrimitiveScalarFunctionOptions = CSharpDB.Primitives.DbScalarFunctionOptions;

namespace CSharpDB.Tests;

public sealed class EngineTransportClientTests
{
    [Fact]
    public async Task ExecuteSqlAsync_PreservesBitLengthsAndKeepsBlobAsBytes()
    {
        string dbPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_engine_transport_bits_{Guid.NewGuid():N}.db");
        CancellationToken ct = TestContext.Current.CancellationToken;

        try
        {
            await using var client = new EngineTransportClient(dbPath);
            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE transport_bits (" +
                "id INTEGER PRIMARY KEY, fixed_bits BIT(3), " +
                "varying_bits VARBIT(8), payload BLOB); " +
                "INSERT INTO transport_bits VALUES (1, '1', '1', X'80');",
                ct)).Error);

            SqlExecutionResult query = await client.ExecuteSqlAsync(
                "SELECT fixed_bits, varying_bits, payload FROM transport_bits WHERE id = 1;",
                ct);
            Assert.Null(query.Error);
            object?[] row = Assert.Single(query.Rows!);
            SqlBitString fixedBits = Assert.IsType<SqlBitString>(row[0]);
            SqlBitString varyingBits = Assert.IsType<SqlBitString>(row[1]);
            Assert.Equal(3, fixedBits.BitLength);
            Assert.Equal("100", fixedBits.ToBitString());
            Assert.Equal(1, varyingBits.BitLength);
            Assert.Equal("1", varyingBits.ToBitString());
            Assert.Equal(new byte[] { 0x80 }, fixedBits.PackedBytes.ToArray());
            Assert.Equal(new byte[] { 0x80 }, varyingBits.PackedBytes.ToArray());
            Assert.Equal(new byte[] { 0x80 }, Assert.IsType<byte[]>(row[2]));

            TransactionSessionInfo transaction = await client.BeginTransactionAsync(ct);
            var reader = Assert.IsAssignableFrom<ICSharpDbTransactionalSnapshotReader>(client);
            await using (ForwardOnlyQueryCursor cursor = Assert.IsType<ForwardOnlyQueryCursor>(
                await reader.TryOpenForwardOnlyQueryCursorAsync(
                    transaction.TransactionId,
                    "SELECT fixed_bits, varying_bits, payload FROM transport_bits WHERE id = 1;",
                    ct)))
            {
                object?[] cursorRow = Assert.Single(await cursor.ReadNextAsync(1, ct));
                Assert.Equal(fixedBits, Assert.IsType<SqlBitString>(cursorRow[0]));
                Assert.Equal(varyingBits, Assert.IsType<SqlBitString>(cursorRow[1]));
                Assert.IsType<byte[]>(cursorRow[2]);
            }
            await client.RollbackTransactionAsync(transaction.TransactionId, ct);

            Assert.Equal(1, await client.InsertRowAsync(
                "transport_bits",
                new Dictionary<string, object?>
                {
                    ["id"] = 2,
                    ["fixed_bits"] = fixedBits,
                    ["varying_bits"] = varyingBits,
                    ["payload"] = row[2],
                },
                ct));

            SqlExecutionResult copied = await client.ExecuteSqlAsync(
                "SELECT CAST(fixed_bits AS TEXT), CAST(varying_bits AS TEXT), payload " +
                "FROM transport_bits WHERE id = 2;",
                ct);
            object?[] copiedRow = Assert.Single(copied.Rows!);
            Assert.Equal("100", copiedRow[0]);
            Assert.Equal("1", copiedRow[1]);
            Assert.IsType<byte[]>(copiedRow[2]);
        }
        finally
        {
            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal"))
                File.Delete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task ExecuteSqlAsync_PreservesResourceLimitErrorCode()
    {
        string dbPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_engine_transport_window_limit_{Guid.NewGuid():N}.db");
        CancellationToken ct = TestContext.Current.CancellationToken;

        try
        {
            await using var client = new EngineTransportClient(
                dbPath,
                new DatabaseOptions
                {
                    WindowExecution = new CSharpDB.Primitives.WindowExecutionOptions
                    {
                        MaxPartitionRows = 2,
                        MaxBufferedRows = 4,
                    },
                });
            SqlExecutionResult seed = await client.ExecuteSqlAsync(
                """
                CREATE TABLE window_limit_rows (id INTEGER PRIMARY KEY, group_id INTEGER);
                INSERT INTO window_limit_rows VALUES (1, 1), (2, 1), (3, 1);
                """,
                ct);
            Assert.Null(seed.Error);

            SqlExecutionResult result = await client.ExecuteSqlAsync(
                """
                SELECT ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY id)
                FROM window_limit_rows;
                """,
                ct);

            Assert.Equal(CSharpDB.Primitives.ErrorCode.ResourceLimitExceeded, result.ErrorCode);
            Assert.Contains("partition", result.Error, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal"))
                File.Delete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task GetTableSchemaAsync_MapsRowVersionMetadata()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_rowversion_{Guid.NewGuid():N}.db");

        try
        {
            await using var client = new EngineTransportClient(dbPath);
            SqlExecutionResult create = await client.ExecuteSqlAsync(
                "CREATE TABLE transport_versions (id INTEGER PRIMARY KEY, version BLOB ROWVERSION NOT NULL);",
                TestContext.Current.CancellationToken);
            Assert.Null(create.Error);

            CSharpDB.Client.Models.TableSchema schema = Assert.IsType<CSharpDB.Client.Models.TableSchema>(
                await client.GetTableSchemaAsync(
                    "transport_versions",
                    TestContext.Current.CancellationToken));
            ColumnDefinition version = Assert.Single(schema.Columns, column => column.Name == "version");

            Assert.Equal(CSharpDB.Client.Models.DbType.Blob, version.Type);
            Assert.False(version.Nullable);
            Assert.True(version.IsRowVersion);
        }
        finally
        {
            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal"))
                File.Delete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task InsertRowAsync_EmptyValuesGeneratesRowVersionDefault()
    {
        string dbPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_engine_transport_rowversion_insert_{Guid.NewGuid():N}.db");

        try
        {
            await using var client = new EngineTransportClient(dbPath);
            SqlExecutionResult create = await client.ExecuteSqlAsync(
                "CREATE TABLE generated_rows (version BLOB ROWVERSION NOT NULL);",
                TestContext.Current.CancellationToken);
            Assert.Null(create.Error);

            int inserted = await client.InsertRowAsync(
                "generated_rows",
                new Dictionary<string, object?>(),
                TestContext.Current.CancellationToken);
            Assert.Equal(1, inserted);

            SqlExecutionResult query = await client.ExecuteSqlAsync(
                "SELECT version FROM generated_rows",
                TestContext.Current.CancellationToken);
            object?[] row = Assert.Single(query.Rows!);
            Assert.Equal(
                new byte[] { 0, 0, 0, 0, 0, 0, 0, 1 },
                Assert.IsType<byte[]>(row[0]));
        }
        finally
        {
            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal"))
                File.Delete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task DirectCrud_QuotesUnusualColumnIdentifiers()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_engine_transport_quoted_crud_{Guid.NewGuid():N}.db");

        try
        {
            await using var client = new EngineTransportClient(dbPath);
            SqlExecutionResult create = await client.ExecuteSqlAsync(
                """
                CREATE TABLE direct_identifier_rows (
                    "order id" INTEGER PRIMARY KEY,
                    "select" TEXT NOT NULL,
                    "path/value" TEXT NOT NULL,
                    "display""name" TEXT NOT NULL
                );
                """,
                ct);
            Assert.Null(create.Error);

            int inserted = await client.InsertRowAsync(
                "direct_identifier_rows",
                new Dictionary<string, object?>
                {
                    ["order id"] = 7L,
                    ["select"] = "before",
                    ["path/value"] = "/first",
                    ["display\"name"] = "Ada",
                },
                ct);
            Assert.Equal(1, inserted);

            Dictionary<string, object?>? insertedRow =
                await client.GetRowByPkAsync(
                    "direct_identifier_rows",
                    "order id",
                    7L,
                    ct);
            Assert.NotNull(insertedRow);
            Assert.Equal("before", insertedRow["select"]);
            Assert.Equal("/first", insertedRow["path/value"]);
            Assert.Equal("Ada", insertedRow["display\"name"]);

            int updated = await client.UpdateRowAsync(
                "direct_identifier_rows",
                "order id",
                7L,
                new Dictionary<string, object?>
                {
                    ["select"] = "after",
                    ["path/value"] = "/second",
                    ["display\"name"] = "Grace",
                },
                ct);
            Assert.Equal(1, updated);

            Dictionary<string, object?>? updatedRow =
                await client.GetRowByPkAsync(
                    "direct_identifier_rows",
                    "order id",
                    7L,
                    ct);
            Assert.NotNull(updatedRow);
            Assert.Equal("after", updatedRow["select"]);
            Assert.Equal("/second", updatedRow["path/value"]);
            Assert.Equal("Grace", updatedRow["display\"name"]);

            Assert.Equal(
                1,
                await client.DeleteRowAsync(
                    "direct_identifier_rows",
                    "order id",
                    7L,
                    ct));
            Assert.Null(
                await client.GetRowByPkAsync(
                    "direct_identifier_rows",
                    "order id",
                    7L,
                    ct));
        }
        finally
        {
            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal"))
                File.Delete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task DirectCrud_UsesCanonicalClrTemporalAndUuidValues()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_engine_transport_logical_crud_{Guid.NewGuid():N}.db");
        DateOnly dateKey = new(2026, 8, 5);
        TimeOnly initialTime = new TimeOnly(14, 30, 15, 123, 456)
            .Add(TimeSpan.FromTicks(7));
        DateTime initialTimestamp = new DateTime(
                2026, 8, 5, 14, 30, 15, 123, DateTimeKind.Unspecified)
            .AddTicks(4567);
        DateTimeOffset initialZoned = new(initialTimestamp, TimeSpan.FromHours(-7));
        Guid initialUuid = Guid.Parse("01234567-89ab-cdef-0123-456789abcdef");

        try
        {
            await using var client = new EngineTransportClient(dbPath);
            SqlExecutionResult create = await client.ExecuteSqlAsync(
                """
                CREATE TABLE direct_temporal_rows (
                    business_date DATE PRIMARY KEY,
                    clock TIME(7) NOT NULL,
                    wall_time TIMESTAMP(7) NOT NULL,
                    zoned_time TIMESTAMP(7) WITH TIME ZONE NOT NULL,
                    correlation_id UUID NOT NULL
                );
                CREATE TABLE direct_uuid_rows (
                    id UUID PRIMARY KEY,
                    effective_date DATE NOT NULL
                );
                """,
                ct);
            Assert.Null(create.Error);

            Assert.Equal(
                1,
                await client.InsertRowAsync(
                    "direct_temporal_rows",
                    new Dictionary<string, object?>
                    {
                        ["business_date"] = dateKey,
                        ["clock"] = initialTime,
                        ["wall_time"] = initialTimestamp,
                        ["zoned_time"] = initialZoned,
                        ["correlation_id"] = initialUuid,
                    },
                    ct));

            Dictionary<string, object?> initialRow = Assert.IsType<Dictionary<string, object?>>(
                await client.GetRowByPkAsync(
                    "direct_temporal_rows",
                    "business_date",
                    dateKey,
                    ct));
            Assert.Equal("2026-08-05", initialRow["business_date"]);
            Assert.Equal("14:30:15.1234567", initialRow["clock"]);
            Assert.Equal("2026-08-05 14:30:15.1234567", initialRow["wall_time"]);
            Assert.Equal("2026-08-05 21:30:15.1234567+00:00", initialRow["zoned_time"]);
            Assert.Equal(
                initialUuid.ToByteArray(bigEndian: true),
                Assert.IsType<byte[]>(initialRow["correlation_id"]));

            TimeOnly updatedTime = new(9, 8, 7, 654, 321);
            DateTime updatedTimestamp = new DateTime(
                    2027, 1, 2, 9, 8, 7, 654, DateTimeKind.Unspecified)
                .AddTicks(3210);
            DateTimeOffset updatedZoned = new(updatedTimestamp, TimeSpan.FromHours(5.5));
            Guid updatedUuid = Guid.Parse("fedcba98-7654-3210-fedc-ba9876543210");
            Assert.Equal(
                1,
                await client.UpdateRowAsync(
                    "direct_temporal_rows",
                    "business_date",
                    dateKey,
                    new Dictionary<string, object?>
                    {
                        ["clock"] = updatedTime,
                        ["wall_time"] = updatedTimestamp,
                        ["zoned_time"] = updatedZoned,
                        ["correlation_id"] = updatedUuid,
                    },
                    ct));

            Dictionary<string, object?> updatedRow = Assert.IsType<Dictionary<string, object?>>(
                await client.GetRowByPkAsync(
                    "direct_temporal_rows",
                    "business_date",
                    dateKey,
                    ct));
            Assert.Equal("09:08:07.654321", updatedRow["clock"]);
            Assert.Equal("2027-01-02 09:08:07.654321", updatedRow["wall_time"]);
            Assert.Equal("2027-01-02 03:38:07.654321+00:00", updatedRow["zoned_time"]);
            Assert.Equal(
                updatedUuid.ToByteArray(bigEndian: true),
                Assert.IsType<byte[]>(updatedRow["correlation_id"]));

            Assert.Equal(
                1,
                await client.DeleteRowAsync(
                    "direct_temporal_rows",
                    "business_date",
                    dateKey,
                    ct));
            Assert.Null(
                await client.GetRowByPkAsync(
                    "direct_temporal_rows",
                    "business_date",
                    dateKey,
                    ct));

            Guid uuidKey = Guid.Parse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
            Assert.Equal(
                1,
                await client.InsertRowAsync(
                    "direct_uuid_rows",
                    new Dictionary<string, object?>
                    {
                        ["id"] = uuidKey,
                        ["effective_date"] = dateKey,
                    },
                    ct));
            Assert.NotNull(
                await client.GetRowByPkAsync(
                    "direct_uuid_rows",
                    "id",
                    uuidKey,
                    ct));
            Assert.Equal(
                1,
                await client.UpdateRowAsync(
                    "direct_uuid_rows",
                    "id",
                    uuidKey,
                    new Dictionary<string, object?>
                    {
                        ["effective_date"] = dateKey.AddDays(1),
                    },
                    ct));
            Assert.Equal(
                "2026-08-06",
                (await client.GetRowByPkAsync(
                    "direct_uuid_rows",
                    "id",
                    uuidKey,
                    ct))!["effective_date"]);
            Assert.Equal(
                1,
                await client.DeleteRowAsync(
                    "direct_uuid_rows",
                    "id",
                    uuidKey,
                    ct));
            Assert.Null(
                await client.GetRowByPkAsync(
                    "direct_uuid_rows",
                    "id",
                    uuidKey,
                    ct));
        }
        finally
        {
            await DeleteDatabaseFilesAsync(dbPath);
        }
    }

    [Fact]
    public async Task DirectProcedureParameters_UseCanonicalClrTemporalAndUuidValues()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_engine_transport_logical_parameters_{Guid.NewGuid():N}.db");
        DateOnly date = new(2026, 8, 5);
        TimeOnly time = new TimeOnly(14, 30, 15, 123, 456)
            .Add(TimeSpan.FromTicks(7));
        DateTime timestamp = new DateTime(
                2026, 8, 5, 14, 30, 15, 123, DateTimeKind.Unspecified)
            .AddTicks(4567);
        DateTimeOffset zoned = new(timestamp, TimeSpan.FromHours(-7));
        Guid uuid = Guid.Parse("01234567-89ab-cdef-0123-456789abcdef");

        try
        {
            await using var client = new EngineTransportClient(dbPath);
            SqlExecutionResult create = await client.ExecuteSqlAsync(
                """
                CREATE TABLE direct_parameter_rows (
                    business_date DATE PRIMARY KEY,
                    clock TIME(7) NOT NULL,
                    wall_time TIMESTAMP(7) NOT NULL,
                    zoned_time TIMESTAMP(7) WITH TIME ZONE NOT NULL,
                    correlation_id UUID NOT NULL
                );
                """,
                ct);
            Assert.Null(create.Error);
            await client.CreateProcedureAsync(
                new ProcedureDefinition
                {
                    Name = "InsertLogicalParameters",
                    BodySql = """
                        INSERT INTO direct_parameter_rows
                            (business_date, clock, wall_time, zoned_time, correlation_id)
                        VALUES (@date, @time, @timestamp, @zoned, @uuid);
                        SELECT business_date, clock, wall_time, zoned_time, correlation_id
                        FROM direct_parameter_rows
                        WHERE business_date = @date;
                        """,
                    Parameters =
                    [
                        new ProcedureParameterDefinition { Name = "date", Type = CSharpDB.Client.Models.DbType.Text, Required = true },
                        new ProcedureParameterDefinition { Name = "time", Type = CSharpDB.Client.Models.DbType.Text, Required = true },
                        new ProcedureParameterDefinition { Name = "timestamp", Type = CSharpDB.Client.Models.DbType.Text, Required = true },
                        new ProcedureParameterDefinition { Name = "zoned", Type = CSharpDB.Client.Models.DbType.Text, Required = true },
                        new ProcedureParameterDefinition { Name = "uuid", Type = CSharpDB.Client.Models.DbType.Text, Required = true },
                    ],
                    IsEnabled = true,
                    CreatedUtc = DateTime.UtcNow,
                    UpdatedUtc = DateTime.UtcNow,
                },
                ct);

            ProcedureExecutionResult result = await client.ExecuteProcedureAsync(
                "InsertLogicalParameters",
                new Dictionary<string, object?>
                {
                    ["date"] = date,
                    ["time"] = time,
                    ["timestamp"] = timestamp,
                    ["zoned"] = zoned,
                    ["uuid"] = uuid,
                },
                ct);

            Assert.True(result.Succeeded, result.Error);
            object?[] row = Assert.Single(result.Statements[1].Rows!);
            Assert.Equal("2026-08-05", row[0]);
            Assert.Equal("14:30:15.1234567", row[1]);
            Assert.Equal("2026-08-05 14:30:15.1234567", row[2]);
            Assert.Equal("2026-08-05 21:30:15.1234567+00:00", row[3]);
            Assert.Equal(
                uuid.ToByteArray(bigEndian: true),
                Assert.IsType<byte[]>(row[4]));
        }
        finally
        {
            await DeleteDatabaseFilesAsync(dbPath);
        }
    }

    [Fact]
    public async Task DirectDdl_QuotesCatalogValidTableAndColumnIdentifiers()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string dbPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_engine_transport_quoted_ddl_{Guid.NewGuid():N}.db");

        try
        {
            await using var client = new EngineTransportClient(dbPath);
            SqlExecutionResult create = await client.ExecuteSqlAsync(
                """
                CREATE TABLE "http/items" (
                    "order id" INTEGER PRIMARY KEY
                );
                """,
                ct);
            Assert.Null(create.Error);

            await client.AddColumnAsync(
                "http/items",
                "select",
                CSharpDB.Client.Models.DbType.Text,
                notNull: false,
                ct);
            Assert.Contains(
                (await client.GetTableSchemaAsync("http/items", ct))!.Columns,
                column => column.Name == "select");

            await client.RenameColumnAsync(
                "http/items",
                "select",
                "path/value",
                ct);
            Assert.Contains(
                (await client.GetTableSchemaAsync("http/items", ct))!.Columns,
                column => column.Name == "path/value");

            await client.RenameTableAsync(
                "http/items",
                "renamed items",
                ct);
            Assert.Null(
                await client.GetTableSchemaAsync("http/items", ct));
            Assert.NotNull(
                await client.GetTableSchemaAsync("renamed items", ct));

            await client.DropColumnAsync(
                "renamed items",
                "path/value",
                ct);
            CSharpDB.Client.Models.TableSchema renamedSchema =
                Assert.IsType<CSharpDB.Client.Models.TableSchema>(
                    await client.GetTableSchemaAsync(
                        "renamed items",
                        ct));
            Assert.DoesNotContain(
                renamedSchema.Columns,
                column => column.Name == "path/value");

            await client.DropTableAsync("renamed items", ct);
            Assert.Null(
                await client.GetTableSchemaAsync("renamed items", ct));
        }
        finally
        {
            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal"))
                File.Delete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task GetTableSchemaAsync_MapsDefaultsChecksAndLogicalKeys()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_schema_{Guid.NewGuid():N}.db");

        try
        {
            await using var client = new EngineTransportClient(dbPath);
            SqlExecutionResult create = await client.ExecuteSqlAsync(
                """
                CREATE TABLE transport_schema (
                    id INTEGER PRIMARY KEY,
                    tenant TEXT NOT NULL,
                    code TEXT DEFAULT 'new',
                    score INTEGER,
                    CONSTRAINT ck_transport_score CHECK (score >= 0),
                    CONSTRAINT uq_transport_tenant_code UNIQUE (tenant, code)
                );
                """,
                TestContext.Current.CancellationToken);
            Assert.Null(create.Error);

            CSharpDB.Client.Models.TableSchema? schema = await client.GetTableSchemaAsync(
                "transport_schema",
                TestContext.Current.CancellationToken);

            Assert.NotNull(schema);
            Assert.NotEqual(Guid.Empty, schema!.SchemaId);
            Assert.All(
                schema.Columns,
                column => Assert.NotEqual(Guid.Empty, column.SchemaId));
            Assert.Equal("'new'", Assert.Single(schema.Columns, column => column.Name == "code").DefaultSql);
            CheckConstraintDefinition check = Assert.Single(schema.CheckConstraints);
            Assert.NotEqual(Guid.Empty, check.SchemaId);
            Assert.Equal("ck_transport_score", check.ConstraintName);
            Assert.Contains("score", check.ExpressionSql, StringComparison.OrdinalIgnoreCase);
            KeyConstraintDefinition unique = Assert.Single(
                schema.KeyConstraints,
                key => key.Kind == KeyConstraintKind.Unique);
            Assert.NotEqual(Guid.Empty, unique.SchemaId);
            Assert.Equal("uq_transport_tenant_code", unique.ConstraintName);
            Assert.Equal(["tenant", "code"], unique.Columns);
        }
        finally
        {
            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal"))
                File.Delete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task GetTableSchemaAsync_MapsOrderedCompositeForeignKeyColumns()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_composite_fk_{Guid.NewGuid():N}.db");

        try
        {
            await using var client = new EngineTransportClient(dbPath);
            SqlExecutionResult create = await client.ExecuteSqlAsync(
                """
                CREATE TABLE transport_parents (
                    tenant_id INTEGER,
                    code TEXT,
                    PRIMARY KEY (tenant_id, code)
                );
                CREATE TABLE transport_children (
                    id INTEGER PRIMARY KEY,
                    tenant_id INTEGER,
                    parent_code TEXT,
                    CONSTRAINT fk_transport_parent
                        FOREIGN KEY (tenant_id, parent_code)
                        REFERENCES transport_parents (tenant_id, code)
                        ON DELETE SET NULL
                        ON UPDATE NO ACTION
                );
                """,
                TestContext.Current.CancellationToken);
            Assert.Null(create.Error);

            CSharpDB.Client.Models.TableSchema schema = Assert.IsType<CSharpDB.Client.Models.TableSchema>(
                await client.GetTableSchemaAsync(
                    "transport_children",
                    TestContext.Current.CancellationToken));
            ForeignKeyDefinition foreignKey = Assert.Single(schema.ForeignKeys);

            Assert.Equal("tenant_id", foreignKey.ColumnName);
            Assert.Equal("tenant_id", foreignKey.ReferencedColumnName);
            Assert.Equal(["tenant_id", "parent_code"], foreignKey.ColumnNames);
            Assert.Equal(["tenant_id", "code"], foreignKey.ReferencedColumnNames);
            Assert.Equal(ForeignKeyOnDeleteAction.SetNull, foreignKey.OnDelete);
            Assert.Equal(ForeignKeyOnDeleteAction.NoAction, foreignKey.OnUpdate);
        }
        finally
        {
            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal"))
                File.Delete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task GetTableSchemaAsync_MapsFullImmediateForeignKeyActionMatrix()
    {
        string dbPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_engine_transport_fk_actions_{Guid.NewGuid():N}.db");

        try
        {
            await using var client = new EngineTransportClient(dbPath);
            SqlExecutionResult create = await client.ExecuteSqlAsync(
                """
                CREATE TABLE transport_action_parents (id INTEGER PRIMARY KEY);
                CREATE TABLE transport_action_children (
                    id INTEGER PRIMARY KEY,
                    restrict_id INTEGER REFERENCES transport_action_parents(id)
                        ON DELETE RESTRICT ON UPDATE RESTRICT,
                    no_action_id INTEGER REFERENCES transport_action_parents(id)
                        ON DELETE NO ACTION ON UPDATE NO ACTION,
                    cascade_id INTEGER REFERENCES transport_action_parents(id)
                        ON DELETE CASCADE ON UPDATE CASCADE,
                    set_null_id INTEGER REFERENCES transport_action_parents(id)
                        ON DELETE SET NULL ON UPDATE SET NULL,
                    set_default_id INTEGER DEFAULT 1
                        REFERENCES transport_action_parents(id)
                        ON DELETE SET DEFAULT ON UPDATE SET DEFAULT
                );
                """,
                TestContext.Current.CancellationToken);
            Assert.Null(create.Error);

            CSharpDB.Client.Models.TableSchema schema =
                Assert.IsType<CSharpDB.Client.Models.TableSchema>(
                    await client.GetTableSchemaAsync(
                        "transport_action_children",
                        TestContext.Current.CancellationToken));
            Dictionary<string, ForeignKeyDefinition> byColumn =
                schema.ForeignKeys.ToDictionary(
                    foreignKey => foreignKey.ColumnName,
                    StringComparer.Ordinal);
            Assert.Equal(5, byColumn.Count);

            foreach ((string columnName, ForeignKeyOnDeleteAction action) in
                     new[]
                     {
                         ("restrict_id", ForeignKeyOnDeleteAction.Restrict),
                         ("no_action_id", ForeignKeyOnDeleteAction.NoAction),
                         ("cascade_id", ForeignKeyOnDeleteAction.Cascade),
                         ("set_null_id", ForeignKeyOnDeleteAction.SetNull),
                         ("set_default_id", ForeignKeyOnDeleteAction.SetDefault),
                     })
            {
                Assert.Equal(action, byColumn[columnName].OnDelete);
                Assert.Equal(action, byColumn[columnName].OnUpdate);
            }
        }
        finally
        {
            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal"))
                File.Delete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task ReleaseCachedDatabaseAsync_CancellationKeepsPendingOpenCached()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_{Guid.NewGuid():N}.db");
        var openEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowOpen = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int openCount = 0;

        try
        {
            await using (var client = new EngineTransportClient(
                             dbPath,
                             async (path, ct) =>
                             {
                                 Interlocked.Increment(ref openCount);
                                 openEntered.TrySetResult();
                                 await allowOpen.Task;
                                 return await Database.OpenAsync(path, ct);
                             }))
            {
                Task<IReadOnlyList<string>> initialRequest = client.GetTableNamesAsync(CancellationToken.None);
                await openEntered.Task.WaitAsync(TestContext.Current.CancellationToken);

                using var cts = new CancellationTokenSource();
                Task releaseTask = client.ReleaseCachedDatabaseAsync(cts.Token).AsTask();
                cts.Cancel();

                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => releaseTask);

                allowOpen.TrySetResult();
                var tables = await initialRequest;
                Assert.Empty(tables);

                Database? cached = await client.TryGetDatabaseAsync(TestContext.Current.CancellationToken);
                Assert.NotNull(cached);
                Assert.Equal(1, Volatile.Read(ref openCount));
            }
        }
        finally
        {
            allowOpen.TrySetResult();

            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal"))
                File.Delete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task ReleaseCachedDatabaseAsync_BlocksNewGetsUntilOldCachedDatabaseIsDisposed()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_{Guid.NewGuid():N}.db");
        var firstOpenEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowFirstOpen = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int openCount = 0;

        try
        {
            await using var client = new EngineTransportClient(
                dbPath,
                async (path, ct) =>
                {
                    int sequence = Interlocked.Increment(ref openCount);
                    if (sequence == 1)
                    {
                        firstOpenEntered.TrySetResult();
                        await allowFirstOpen.Task;
                    }

                    return await Database.OpenAsync(path, ct);
                });

            _ = client.TryGetDatabaseAsync(CancellationToken.None);
            await firstOpenEntered.Task.WaitAsync(TestContext.Current.CancellationToken);

            Task releaseTask = client.ReleaseCachedDatabaseAsync(TestContext.Current.CancellationToken).AsTask();
            await Task.Delay(50, TestContext.Current.CancellationToken);

            Task<Database?> secondGetTask = client.TryGetDatabaseAsync(TestContext.Current.CancellationToken).AsTask();
            await Task.Delay(50, TestContext.Current.CancellationToken);

            Assert.Equal(1, Volatile.Read(ref openCount));
            Assert.False(secondGetTask.IsCompleted);

            allowFirstOpen.TrySetResult();
            await releaseTask;

            Database? reopened = await secondGetTask;
            Assert.NotNull(reopened);
            Assert.Equal(2, Volatile.Read(ref openCount));
        }
        finally
        {
            allowFirstOpen.TrySetResult();

            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal"))
                File.Delete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task GetDatabaseAsync_WhenReleaseStartsAfterWaitBegins_RetriesInsteadOfReturningDisposedDatabase()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_{Guid.NewGuid():N}.db");
        var firstOpenEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowFirstOpen = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int openCount = 0;

        try
        {
            await using var client = new EngineTransportClient(
                dbPath,
                async (path, ct) =>
                {
                    int sequence = Interlocked.Increment(ref openCount);
                    if (sequence == 1)
                    {
                        firstOpenEntered.TrySetResult();
                        await allowFirstOpen.Task;
                    }

                    return await Database.OpenAsync(path, ct);
                });

            Task<Database?> firstGetTask = client.TryGetDatabaseAsync(CancellationToken.None).AsTask();
            await firstOpenEntered.Task.WaitAsync(TestContext.Current.CancellationToken);

            Task<Database?> secondGetTask = client.TryGetDatabaseAsync(CancellationToken.None).AsTask();
            Task releaseTask = client.ReleaseCachedDatabaseAsync(TestContext.Current.CancellationToken).AsTask();

            allowFirstOpen.TrySetResult();
            await releaseTask;

            Database? firstResult = await firstGetTask;
            Database? secondResult = await secondGetTask;

            Assert.NotNull(firstResult);
            Assert.NotNull(secondResult);
            Assert.Equal(2, Volatile.Read(ref openCount));
        }
        finally
        {
            allowFirstOpen.TrySetResult();

            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal"))
                File.Delete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task GetRowByPkAsync_UsesTargetedLookupForExistingAndMissingRows()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_{Guid.NewGuid():N}.db");

        try
        {
            await using var client = new EngineTransportClient(dbPath);
            await client.ExecuteSqlAsync(
                """
                CREATE TABLE Users (
                    Id INTEGER PRIMARY KEY,
                    Name TEXT NOT NULL
                );
                INSERT INTO Users VALUES (1, 'Ada');
                INSERT INTO Users VALUES (2, 'Grace');
                INSERT INTO Users VALUES (3, 'Linus');
                """,
                TestContext.Current.CancellationToken);

            Dictionary<string, object?>? first = await client.GetRowByPkAsync("Users", "Id", 1L, TestContext.Current.CancellationToken);
            Dictionary<string, object?>? middle = await client.GetRowByPkAsync("Users", "Id", 2L, TestContext.Current.CancellationToken);
            Dictionary<string, object?>? last = await client.GetRowByPkAsync("Users", "Id", 3L, TestContext.Current.CancellationToken);
            Dictionary<string, object?>? missing = await client.GetRowByPkAsync("Users", "Id", 99L, TestContext.Current.CancellationToken);

            Assert.NotNull(first);
            Assert.Equal("Ada", first!["Name"]);
            Assert.NotNull(middle);
            Assert.Equal("Grace", middle!["Name"]);
            Assert.NotNull(last);
            Assert.Equal("Linus", last!["Name"]);
            Assert.Null(missing);
        }
        finally
        {
            if (File.Exists(dbPath))
                File.Delete(dbPath);
            if (File.Exists(dbPath + ".wal"))
                File.Delete(dbPath + ".wal");
        }
    }

    [Fact]
    public async Task ExportTableArchiveAsync_WritesNativeArchiveFromDirectSnapshot()
    {
        string directory = Path.Combine(Path.GetTempPath(), $"csharpdb_table_export_{Guid.NewGuid():N}");
        string dbPath = Path.Combine(directory, "export.db");
        string archivePath = Path.Combine(directory, "exports", "customers.csdbtable");

        try
        {
            Directory.CreateDirectory(directory);
            await using var client = new EngineTransportClient(dbPath);
            await client.ExecuteSqlAsync(
                """
                CREATE TABLE Customers (
                    Id INTEGER PRIMARY KEY,
                    Name TEXT NOT NULL,
                    Balance REAL,
                    Payload BLOB
                );
                INSERT INTO Customers VALUES (1, 'Ada', 10.5, X'0102FF');
                INSERT INTO Customers VALUES (2, 'Grace', NULL, NULL);
                """,
                TestContext.Current.CancellationToken);

            var exporter = Assert.IsAssignableFrom<ICSharpDbTableArchiveExporter>(client);
            Assert.True(exporter.SupportsTableArchiveExport);

            var export = await exporter.ExportTableArchiveAsync(
                "Customers",
                archivePath,
                TestContext.Current.CancellationToken);

            Assert.Equal("Customers", export.TableName);
            Assert.Equal("customers.csdbtable", export.FileName);
            Assert.Equal(2, export.RowCount);
            Assert.True(File.Exists(archivePath));

            var schema = await TableArchiveReader.ReadTableSchemaAsync(
                archivePath,
                ct: TestContext.Current.CancellationToken);
            Assert.Equal("Customers", schema.TableName);
            Assert.Equal(4, schema.Columns.Count);
            Assert.True(schema.Columns[0].IsPrimaryKey);

            var rows = new List<CSharpDB.Primitives.DbValue[]>();
            await foreach (var row in TableArchiveReader.ReadRowsAsync(
                               archivePath,
                               TestContext.Current.CancellationToken))
            {
                rows.Add(row);
            }

            Assert.Equal(2, rows.Count);
            Assert.Equal(1, rows[0][0].AsInteger);
            Assert.Equal("Ada", rows[0][1].AsText);
            Assert.Equal(10.5, rows[0][2].AsReal);
            Assert.Equal(new byte[] { 0x01, 0x02, 0xff }, rows[0][3].AsBlob);
            Assert.Equal(2, rows[1][0].AsInteger);
            Assert.Equal("Grace", rows[1][1].AsText);
            Assert.True(rows[1][2].IsNull);
            Assert.True(rows[1][3].IsNull);
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task ExportTableArchiveAsync_ReportsProgressAndHonorsCancellation()
    {
        var testToken = TestContext.Current.CancellationToken;
        string directory = Path.Combine(Path.GetTempPath(), $"csharpdb_table_export_cancel_{Guid.NewGuid():N}");
        string dbPath = Path.Combine(directory, "export.db");
        string archivePath = Path.Combine(directory, "exports", "customers.csdbtable");

        try
        {
            Directory.CreateDirectory(directory);
            await using var client = new EngineTransportClient(dbPath);
            await client.ExecuteSqlAsync(
                "CREATE TABLE Customers (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);",
                testToken);

            for (int start = 1; start <= 5_000; start += 500)
            {
                var sql = new StringBuilder("INSERT INTO Customers (Id, Name) VALUES ");
                for (int i = 0; i < 500; i++)
                {
                    if (i > 0)
                        sql.Append(", ");

                    int id = start + i;
                    sql.Append('(')
                        .Append(id)
                        .Append(", 'Customer ")
                        .Append(id)
                        .Append("')");
                }

                sql.Append(';');
                await client.ExecuteSqlAsync(sql.ToString(), testToken);
            }

            var exporter = Assert.IsAssignableFrom<ICSharpDbTableArchiveProgressExporter>(client);
            using var exportCts = CancellationTokenSource.CreateLinkedTokenSource(testToken);
            long highestRowsExported = 0;
            var progress = new InlineProgress<TableArchiveExportProgress>(p =>
            {
                highestRowsExported = Math.Max(highestRowsExported, p.RowsExported);
                if (p.RowsExported >= 1_000)
                    exportCts.Cancel();
            });

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await exporter.ExportTableArchiveAsync("Customers", archivePath, progress, exportCts.Token));

            Assert.True(highestRowsExported >= 1_000);
        }
        finally
        {
            if (Directory.Exists(directory))
                Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task ClientManagedTransactions_ReuseOneDirectDatabaseAcrossCommitAndRollback()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_reuse_{Guid.NewGuid():N}.db");
        int openCount = 0;

        try
        {
            await using var client = new EngineTransportClient(
                dbPath,
                async (path, ct) =>
                {
                    Interlocked.Increment(ref openCount);
                    return await Database.OpenAsync(path, ct);
                });

            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE transaction_reuse (id INTEGER PRIMARY KEY);",
                TestContext.Current.CancellationToken)).Error);

            TransactionSessionInfo first = await client.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await client.ExecuteInTransactionAsync(
                first.TransactionId,
                "INSERT INTO transaction_reuse VALUES (1);",
                TestContext.Current.CancellationToken);
            await client.CommitTransactionAsync(first.TransactionId, TestContext.Current.CancellationToken);

            TransactionSessionInfo second = await client.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await client.ExecuteInTransactionAsync(
                second.TransactionId,
                "INSERT INTO transaction_reuse VALUES (2);",
                TestContext.Current.CancellationToken);
            await client.RollbackTransactionAsync(second.TransactionId, TestContext.Current.CancellationToken);

            TransactionSessionInfo third = await client.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await client.ExecuteInTransactionAsync(
                third.TransactionId,
                "INSERT INTO transaction_reuse VALUES (3);",
                TestContext.Current.CancellationToken);
            await client.CommitTransactionAsync(third.TransactionId, TestContext.Current.CancellationToken);

            SqlExecutionResult result = await client.ExecuteSqlAsync(
                "SELECT id FROM transaction_reuse ORDER BY id;",
                TestContext.Current.CancellationToken);

            Assert.Null(result.Error);
            Assert.Equal([1L, 3L], result.Rows!.Select(row => row[0]).ToArray());
            Assert.Equal(1, Volatile.Read(ref openCount));
        }
        finally
        {
            await DeleteDatabaseFilesAsync(dbPath);
        }
    }

    [Fact]
    public async Task ClientManagedTransactionHandoff_ClearsTemporaryStateAtBothBoundaries()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_temp_reuse_{Guid.NewGuid():N}.db");
        int openCount = 0;

        try
        {
            await using var client = new EngineTransportClient(
                dbPath,
                async (path, ct) =>
                {
                    Interlocked.Increment(ref openCount);
                    return await Database.OpenAsync(path, ct);
                });

            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TEMP TABLE before_transaction (id INTEGER PRIMARY KEY);",
                TestContext.Current.CancellationToken)).Error);

            TransactionSessionInfo first = await client.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await Assert.ThrowsAsync<CSharpDB.Primitives.CSharpDbException>(async () =>
                await client.ExecuteInTransactionAsync(
                    first.TransactionId,
                    "SELECT * FROM before_transaction;",
                    TestContext.Current.CancellationToken));

            await client.ExecuteInTransactionAsync(
                first.TransactionId,
                "CREATE TEMP TABLE during_transaction (id INTEGER PRIMARY KEY);",
                TestContext.Current.CancellationToken);
            await client.CommitTransactionAsync(first.TransactionId, TestContext.Current.CancellationToken);

            TransactionSessionInfo second = await client.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await Assert.ThrowsAsync<CSharpDB.Primitives.CSharpDbException>(async () =>
                await client.ExecuteInTransactionAsync(
                    second.TransactionId,
                    "SELECT * FROM during_transaction;",
                    TestContext.Current.CancellationToken));
            await client.RollbackTransactionAsync(second.TransactionId, TestContext.Current.CancellationToken);

            Assert.Equal(1, Volatile.Read(ref openCount));
        }
        finally
        {
            await DeleteDatabaseFilesAsync(dbPath);
        }
    }

    [Fact]
    public async Task ClientManagedTransactionFailure_DoesNotRecycleUncertainDatabaseState()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_failed_reuse_{Guid.NewGuid():N}.db");
        int openCount = 0;

        try
        {
            await using var client = new EngineTransportClient(
                dbPath,
                async (path, ct) =>
                {
                    Interlocked.Increment(ref openCount);
                    return await Database.OpenAsync(path, ct);
                });

            Assert.Null((await client.ExecuteSqlAsync(
                """
                CREATE TABLE failed_transaction_reuse (id INTEGER PRIMARY KEY);
                INSERT INTO failed_transaction_reuse VALUES (1);
                """,
                TestContext.Current.CancellationToken)).Error);

            TransactionSessionInfo transaction = await client.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await Assert.ThrowsAsync<CSharpDB.Primitives.CSharpDbException>(async () =>
                await client.ExecuteInTransactionAsync(
                    transaction.TransactionId,
                    "INSERT INTO failed_transaction_reuse VALUES (1);",
                    TestContext.Current.CancellationToken));
            await Assert.ThrowsAsync<CSharpDB.Primitives.CSharpDbException>(async () =>
                await client.CommitTransactionAsync(
                    transaction.TransactionId,
                    TestContext.Current.CancellationToken));

            Assert.Equal(
                1,
                await client.GetRowCountAsync(
                    "failed_transaction_reuse",
                    TestContext.Current.CancellationToken));
            Assert.Equal(2, Volatile.Read(ref openCount));
        }
        finally
        {
            await DeleteDatabaseFilesAsync(dbPath);
        }
    }

    [Fact]
    public async Task ClientManagedTransactionCompletion_PreservesCompetingOrdinaryHandle()
    {
        Database competingDatabase = await Database.OpenInMemoryAsync(
            TestContext.Current.CancellationToken);
        bool competingDatabaseHandedOff = false;
        int openCount = 0;

        try
        {
            await using var client = new EngineTransportClient(
                ":memory:competing-handle",
                async (_, ct) =>
                {
                    if (Interlocked.Increment(ref openCount) == 2)
                    {
                        competingDatabaseHandedOff = true;
                        return competingDatabase;
                    }

                    return await Database.OpenInMemoryAsync(ct);
                });

            Assert.Null((await client.ExecuteSqlAsync(
                "CREATE TABLE competing_handle (id INTEGER PRIMARY KEY);",
                TestContext.Current.CancellationToken)).Error);

            TransactionSessionInfo transaction = await client.BeginTransactionAsync(TestContext.Current.CancellationToken);
            Assert.Empty(await client.GetTableNamesAsync(TestContext.Current.CancellationToken));
            Assert.Equal(2, Volatile.Read(ref openCount));

            await client.RollbackTransactionAsync(transaction.TransactionId, TestContext.Current.CancellationToken);
            Assert.Same(
                competingDatabase,
                await client.TryGetDatabaseAsync(TestContext.Current.CancellationToken));
            Assert.Equal(2, Volatile.Read(ref openCount));
        }
        finally
        {
            if (!competingDatabaseHandedOff)
                await competingDatabase.DisposeAsync();
        }
    }

    [Fact]
    public async Task ClientManagedTransactionCompletion_DoesNotAdoptAfterTransientOrdinaryOpen()
    {
        int openCount = 0;

        await using var client = new EngineTransportClient(
            ":memory:transient-ordinary-open",
            async (_, ct) =>
            {
                Interlocked.Increment(ref openCount);
                return await Database.OpenInMemoryAsync(ct);
            });

        TransactionSessionInfo transaction = await client.BeginTransactionAsync(TestContext.Current.CancellationToken);
        Assert.Empty(await client.GetTableNamesAsync(TestContext.Current.CancellationToken));
        await client.ReleaseCachedDatabaseAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, Volatile.Read(ref openCount));

        await client.RollbackTransactionAsync(transaction.TransactionId, TestContext.Current.CancellationToken);
        Assert.Empty(await client.GetTableNamesAsync(TestContext.Current.CancellationToken));
        Assert.Equal(3, Volatile.Read(ref openCount));
    }

    [Fact]
    public async Task OverlappingClientManagedTransactions_DoNotAdoptStaleDatabaseHandle()
    {
        int openCount = 0;

        await using var client = new EngineTransportClient(
            ":memory:overlapping-transactions",
            async (_, ct) =>
            {
                Interlocked.Increment(ref openCount);
                return await Database.OpenInMemoryAsync(ct);
            });

        TransactionSessionInfo first = await client.BeginTransactionAsync(
            TestContext.Current.CancellationToken);
        TransactionSessionInfo second = await client.BeginTransactionAsync(
            TestContext.Current.CancellationToken);

        await client.ExecuteInTransactionAsync(
            first.TransactionId,
            "CREATE TABLE stale_overlap_schema (id INTEGER PRIMARY KEY);",
            TestContext.Current.CancellationToken);
        await client.CommitTransactionAsync(first.TransactionId, TestContext.Current.CancellationToken);
        await client.RollbackTransactionAsync(second.TransactionId, TestContext.Current.CancellationToken);

        Assert.Empty(await client.GetTableNamesAsync(TestContext.Current.CancellationToken));
        Assert.Equal(3, Volatile.Read(ref openCount));
    }

    [Fact]
    public async Task ClientManagedTransactions_HybridDisposeTriggerPersistsBeforeCompletionReturns()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_hybrid_boundary_{Guid.NewGuid():N}.db");
        var hybridOptions = new HybridDatabaseOptions
        {
            PersistenceMode = HybridPersistenceMode.Snapshot,
            PersistenceTriggers = HybridPersistenceTriggers.Dispose,
        };

        try
        {
            await using var client = new EngineTransportClient(
                dbPath,
                hybridDatabaseOptions: hybridOptions);
            TransactionSessionInfo transaction = await client.BeginTransactionAsync(
                TestContext.Current.CancellationToken);
            await client.ExecuteInTransactionAsync(
                transaction.TransactionId,
                "CREATE TABLE hybrid_boundary (id INTEGER PRIMARY KEY, value TEXT);",
                TestContext.Current.CancellationToken);
            await client.ExecuteInTransactionAsync(
                transaction.TransactionId,
                "INSERT INTO hybrid_boundary VALUES (1, 'persisted-on-dispose');",
                TestContext.Current.CancellationToken);
            await client.CommitTransactionAsync(transaction.TransactionId, TestContext.Current.CancellationToken);

            await using var reopened = await Database.OpenAsync(
                dbPath,
                TestContext.Current.CancellationToken);
            await using var result = await reopened.ExecuteAsync(
                "SELECT value FROM hybrid_boundary WHERE id = 1;",
                TestContext.Current.CancellationToken);
            var rows = await result.ToListAsync(TestContext.Current.CancellationToken);
            Assert.Equal("persisted-on-dispose", Assert.Single(rows)[0].AsText);
        }
        finally
        {
            await DeleteDatabaseFilesAsync(dbPath);
        }
    }

    [Fact]
    public async Task ClientManagedTransactionCompletion_WaitsForInFlightStatementBeforeReuse()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_inflight_{Guid.NewGuid():N}.db");
        var executeEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var allowExecute = new ManualResetEventSlim();
        int openCount = 0;
        var options = new DatabaseOptions().ConfigureFunctions(functions =>
            functions.AddScalar(
                "WaitForCompletion",
                0,
                new PrimitiveScalarFunctionOptions(PrimitiveDbType.Integer),
                (_, _) =>
                {
                    executeEntered.TrySetResult();
                    allowExecute.Wait();
                    return PrimitiveDbValue.FromInteger(1);
                }));

        try
        {
            await using var client = new EngineTransportClient(
                dbPath,
                async (path, ct) =>
                {
                    Interlocked.Increment(ref openCount);
                    return await Database.OpenAsync(path, options, ct);
                },
                options);

            TransactionSessionInfo transaction = await client.BeginTransactionAsync(TestContext.Current.CancellationToken);
            Task<SqlExecutionResult> executeTask = Task.Run(
                async () => await client.ExecuteInTransactionAsync(
                    transaction.TransactionId,
                    "SELECT WaitForCompletion();",
                    TestContext.Current.CancellationToken),
                TestContext.Current.CancellationToken);
            await executeEntered.Task.WaitAsync(TestContext.Current.CancellationToken);

            Task commitTask = client.CommitTransactionAsync(
                transaction.TransactionId,
                TestContext.Current.CancellationToken);
            await Task.Delay(50, TestContext.Current.CancellationToken);
            Assert.False(commitTask.IsCompleted);

            allowExecute.Set();
            SqlExecutionResult result = await executeTask;
            await commitTask;

            Assert.Equal(1L, Assert.Single(result.Rows!)[0]);
            Assert.Equal(1, Volatile.Read(ref openCount));
            Assert.Empty(await client.GetTableNamesAsync(TestContext.Current.CancellationToken));
            Assert.Equal(1, Volatile.Read(ref openCount));
        }
        finally
        {
            allowExecute.Set();
            await DeleteDatabaseFilesAsync(dbPath);
        }
    }

    [Fact]
    public async Task ClientManagedTransactionCompletion_CancellationWhileWaitingLeavesTransactionUsable()
    {
        var executeEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var allowExecute = new ManualResetEventSlim();
        int openCount = 0;
        var options = new DatabaseOptions().ConfigureFunctions(functions =>
            functions.AddScalar(
                "WaitForCanceledCompletion",
                0,
                new PrimitiveScalarFunctionOptions(PrimitiveDbType.Integer),
                (_, _) =>
                {
                    executeEntered.TrySetResult();
                    allowExecute.Wait();
                    return PrimitiveDbValue.FromInteger(1);
                }));

        try
        {
            await using var client = new EngineTransportClient(
                ":memory:canceled-transaction-completion",
                async (_, ct) =>
                {
                    Interlocked.Increment(ref openCount);
                    return await Database.OpenInMemoryAsync(options, ct);
                },
                options);

            TransactionSessionInfo transaction = await client.BeginTransactionAsync(
                TestContext.Current.CancellationToken);
            Task<SqlExecutionResult> executeTask = Task.Run(
                async () => await client.ExecuteInTransactionAsync(
                    transaction.TransactionId,
                    "SELECT WaitForCanceledCompletion();",
                    TestContext.Current.CancellationToken),
                TestContext.Current.CancellationToken);
            await executeEntered.Task.WaitAsync(TestContext.Current.CancellationToken);

            using var cancellation = new CancellationTokenSource();
            Task canceledCommit = client.CommitTransactionAsync(
                transaction.TransactionId,
                cancellation.Token);
            Assert.False(canceledCommit.IsCompleted);
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => canceledCommit);

            allowExecute.Set();
            Assert.Equal(1L, Assert.Single((await executeTask).Rows!)[0]);

            await client.CommitTransactionAsync(
                transaction.TransactionId,
                TestContext.Current.CancellationToken);
            Assert.Empty(await client.GetTableNamesAsync(TestContext.Current.CancellationToken));
            Assert.Equal(1, Volatile.Read(ref openCount));
        }
        finally
        {
            allowExecute.Set();
        }
    }

    [Fact]
    public async Task DisposeAsync_WaitsForInFlightTransactionStatementAndIsIdempotent()
    {
        var executeEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var allowExecute = new ManualResetEventSlim();
        int openCount = 0;
        var options = new DatabaseOptions().ConfigureFunctions(functions =>
            functions.AddScalar(
                "WaitForClientDispose",
                0,
                new PrimitiveScalarFunctionOptions(PrimitiveDbType.Integer),
                (_, _) =>
                {
                    executeEntered.TrySetResult();
                    allowExecute.Wait();
                    return PrimitiveDbValue.FromInteger(1);
                }));
        var client = new EngineTransportClient(
            ":memory:transaction-dispose",
            async (_, ct) =>
            {
                Interlocked.Increment(ref openCount);
                return await Database.OpenInMemoryAsync(options, ct);
            },
            options);

        try
        {
            TransactionSessionInfo transaction = await client.BeginTransactionAsync(
                TestContext.Current.CancellationToken);
            Task<SqlExecutionResult> executeTask = Task.Run(
                async () => await client.ExecuteInTransactionAsync(
                    transaction.TransactionId,
                    "SELECT WaitForClientDispose();",
                    TestContext.Current.CancellationToken),
                TestContext.Current.CancellationToken);
            await executeEntered.Task.WaitAsync(TestContext.Current.CancellationToken);

            Task firstDispose = client.DisposeAsync().AsTask();
            Task secondDispose = client.DisposeAsync().AsTask();
            Assert.Same(firstDispose, secondDispose);
            Assert.False(firstDispose.IsCompleted);

            allowExecute.Set();
            Assert.Equal(1L, Assert.Single((await executeTask).Rows!)[0]);
            await firstDispose;
            await secondDispose;
            Assert.Equal(1, Volatile.Read(ref openCount));
        }
        finally
        {
            allowExecute.Set();
            await client.DisposeAsync();
        }
    }

    [Fact]
    public async Task BeginTransaction_ActiveSnapshotReaderRestoresCachedDatabase()
    {
        string dbPath = Path.Combine(Path.GetTempPath(), $"csharpdb_engine_transport_snapshot_guard_{Guid.NewGuid():N}.db");
        int openCount = 0;

        try
        {
            await using var client = new EngineTransportClient(
                dbPath,
                async (path, ct) =>
                {
                    Interlocked.Increment(ref openCount);
                    return await Database.OpenAsync(path, ct);
                });

            Database database = Assert.IsType<Database>(
                await client.TryGetDatabaseAsync(TestContext.Current.CancellationToken));
            using (Database.ReaderSession reader = database.CreateReaderSession())
            {
                CSharpDbClientException exception = await Assert.ThrowsAsync<CSharpDbClientException>(
                    async () => await client.BeginTransactionAsync(TestContext.Current.CancellationToken));
                Assert.Contains("snapshot readers", exception.Message, StringComparison.OrdinalIgnoreCase);
            }

            TransactionSessionInfo transaction = await client.BeginTransactionAsync(TestContext.Current.CancellationToken);
            await client.RollbackTransactionAsync(transaction.TransactionId, TestContext.Current.CancellationToken);
            Assert.Equal(1, Volatile.Read(ref openCount));
        }
        finally
        {
            await DeleteDatabaseFilesAsync(dbPath);
        }
    }

    private static async ValueTask DeleteDatabaseFilesAsync(string dbPath)
    {
        await DeleteIfExistsAsync(dbPath);
        await DeleteIfExistsAsync(dbPath + ".wal");
    }

    private static async ValueTask DeleteIfExistsAsync(string path)
    {
        if (!File.Exists(path))
            return;

        var timeout = System.Diagnostics.Stopwatch.StartNew();
        Exception? lastException = null;
        while (true)
        {
            try
            {
                File.Delete(path);
                return;
            }
            catch (IOException ex) when (timeout.Elapsed < TimeSpan.FromSeconds(2))
            {
                lastException = ex;
            }
            catch (UnauthorizedAccessException ex) when (timeout.Elapsed < TimeSpan.FromSeconds(2))
            {
                lastException = ex;
            }

            if (!File.Exists(path))
                return;
            if (timeout.Elapsed >= TimeSpan.FromSeconds(2))
                break;

            await Task.Delay(25);
        }

        throw new IOException(
            $"Failed to delete temporary database file '{path}' within the cleanup timeout.",
            lastException);
    }

    private sealed class InlineProgress<T>(Action<T> report) : IProgress<T>
    {
        public void Report(T value) => report(value);
    }
}
