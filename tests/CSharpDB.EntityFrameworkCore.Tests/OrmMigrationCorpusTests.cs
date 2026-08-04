using System.Data;
using CSharpDB.Data;
using CSharpDB.Sql;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace CSharpDB.EntityFrameworkCore.Tests;

[Collection("ConnectionPoolState")]
public sealed class OrmMigrationCorpusTests : IAsyncLifetime
{
    private const string InitialMigration = "20260729000100_OrmCorpusInitial";
    private const string RewriteMigration = "20260729000200_OrmCorpusRewriteAndRename";
    private const string LatestMigration = "20260729000300_OrmCorpusCompositeRekey";
    private const string RollbackInitialMigration = "20260729010100_OrmRollbackInitial";
    private const string RollbackFailingMigration = "20260729010200_OrmRollbackFails";

    private readonly string _workspace = Path.Combine(
        Path.GetTempPath(),
        $"csharpdb_orm_migration_{Guid.NewGuid():N}");

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public async ValueTask InitializeAsync()
    {
        Directory.CreateDirectory(_workspace);
        await CSharpDbConnection.ClearAllPoolsAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await CSharpDbConnection.ClearAllPoolsAsync();

        if (Directory.Exists(_workspace))
            Directory.Delete(_workspace, recursive: true);
    }

    [Fact]
    public async Task GeneratedSqlCorpus_MatchesSnapshots_AndReplaysWithoutEf()
    {
        string generationPath = DatabasePath("corpus-generation.db");
        string generatedUp;
        string generatedDown;

        await using (var db = new OrmMigrationContext(ConnectionString(generationPath)))
        {
            IMigrator migrator = db.GetService<IMigrator>();
            generatedUp = NormalizeSql(migrator.GenerateScript());
            generatedDown = NormalizeSql(
                migrator.GenerateScript(
                    LatestMigration,
                    Migration.InitialDatabase));
        }

        string expectedUp = ReadCorpus("orm-migration-corpus-up.sql");
        string expectedDown = ReadCorpus("orm-migration-corpus-down.sql");

        Assert.Equal(expectedUp, generatedUp);
        Assert.Equal(expectedDown, generatedDown);

        string replayPath = DatabasePath("corpus-replay.db");
        await ExecuteScriptAsync(replayPath, expectedUp);

        await using (var connection = new CSharpDbConnection(ConnectionString(replayPath)))
        {
            await connection.OpenAsync(Ct);
            AssertFinalSchema(connection);
        }

        await using (var db = new OrmMigrationContext(ConnectionString(replayPath)))
        {
            Assert.Equal(
                [InitialMigration, RewriteMigration, LatestMigration],
                (await db.Database.GetAppliedMigrationsAsync(Ct)).ToArray());
        }

        await ExecuteScriptAsync(replayPath, expectedDown);

        await using (var connection = new CSharpDbConnection(ConnectionString(replayPath)))
        {
            await connection.OpenAsync(Ct);
            DataTable tables = connection.GetSchema("Tables");
            HashSet<string> remainingTables = tables.Rows
                .Cast<DataRow>()
                .Select(row => Assert.IsType<string>(row["TABLE_NAME"]))
                .ToHashSet(StringComparer.Ordinal);
            Assert.DoesNotContain(
                new[]
                {
                    "Organizations",
                    "WorkItems",
                    "MemberProfiles",
                    "Members",
                    "RekeyCandidates",
                    "ActionParents",
                    "ActionNoAction",
                    "ActionRestrict",
                    "ActionCascade",
                    "ActionSetNull",
                    "ActionSetDefault",
                },
                remainingTables.Contains);
        }

        await using (var db = new OrmMigrationContext(ConnectionString(replayPath)))
        {
            Assert.Empty(await db.Database.GetAppliedMigrationsAsync(Ct));
        }
    }

    [Fact]
    public async Task MigrationChain_AppliesToEmptyDatabase_AndSupportsRuntimeCrud()
    {
        string dbPath = DatabasePath("empty-runtime.db");

        await using (var db = new OrmMigrationContext(ConnectionString(dbPath)))
        {
            await db.Database.MigrateAsync(Ct);

            var organization = new OrmOrganization
            {
                TenantId = 7,
                OrganizationId = 11,
                Name = "runtime",
            };
            organization.WorkItems.Add(new OrmWorkItem
            {
                TenantId = 7,
                TaskId = 101,
                Title = "after migration",
            });

            db.Organizations.Add(organization);
            db.Members.Add(new OrmMember
            {
                Id = 1,
                Handle = "MixedCase",
                Rating = 8,
            });
            await db.SaveChangesAsync(Ct);
            await db.Database.ExecuteSqlRawAsync(
                "INSERT INTO Members (Id) VALUES (2);",
                cancellationToken: Ct);
        }

        await CSharpDbConnection.ClearAllPoolsAsync();

        await using (var reopened = new OrmMigrationContext(ConnectionString(dbPath)))
        {
            OrmOrganization organization = await reopened.Organizations.SingleAsync(Ct);
            OrmWorkItem workItem = await reopened.WorkItems.SingleAsync(Ct);
            OrmMember member = await reopened.Members
                .SingleAsync(item => item.Handle == "mixedcase", Ct);
            OrmMember defaulted = await reopened.Members
                .SingleAsync(item => item.Id == 2, Ct);

            Assert.Equal("runtime", organization.Name);
            Assert.Equal("after migration", workItem.Title);
            Assert.Equal("active", member.State);
            Assert.Equal(8D, member.Rating);
            Assert.Equal("member", defaulted.Handle);
            Assert.Equal("active", defaulted.State);
            Assert.Equal(2D, defaulted.Rating);

            member.Rating = 9;
            workItem.Title = "updated after migration";
            reopened.Members.Remove(defaulted);
            await reopened.SaveChangesAsync(Ct);
            reopened.ChangeTracker.Clear();

            Assert.Equal(
                9D,
                (await reopened.Members.SingleAsync(item => item.Id == 1, Ct)).Rating);
            Assert.Equal(
                "updated after migration",
                (await reopened.WorkItems.SingleAsync(Ct)).Title);
            Assert.False(await reopened.Members.AnyAsync(item => item.Id == 2, Ct));

            reopened.Members.Add(new OrmMember
            {
                Id = 3,
                Handle = "invalid",
                Rating = -1,
            });
            await Assert.ThrowsAsync<DbUpdateException>(
                () => reopened.SaveChangesAsync(Ct));
            reopened.ChangeTracker.Clear();
        }

        await using var connection = new CSharpDbConnection(ConnectionString(dbPath));
        await connection.OpenAsync(Ct);
        AssertFinalSchema(connection);
    }

    [Fact]
    public async Task MigrationChain_UpgradesAndDowngradesPopulatedDatabase_WithoutLosingData()
    {
        string dbPath = DatabasePath("populated-chain.db");

        await using (var db = new OrmMigrationContext(ConnectionString(dbPath)))
        {
            await db.GetService<IMigrator>().MigrateAsync(InitialMigration, Ct);
        }

        await ExecuteScriptAsync(
            dbPath,
            """
            INSERT INTO Organizations (TenantId, OrganizationId, Name)
            VALUES (3, 9, 'preserved organization');
            INSERT INTO WorkItems (TenantId, TaskId, OrganizationId, Title)
            VALUES (3, 41, 9, 'preserved task');
            INSERT INTO MemberProfiles (Id, Code, Rating)
            VALUES (5, 'LegacyCode', 6);
            INSERT INTO RekeyCandidates (Id, Label)
            VALUES (10, 'ten'), (20, 'twenty');
            """);

        await using (var db = new OrmMigrationContext(ConnectionString(dbPath)))
        {
            await db.GetService<IMigrator>().MigrateAsync(RewriteMigration, Ct);
            Assert.Equal(
                [InitialMigration, RewriteMigration],
                (await db.Database.GetAppliedMigrationsAsync(Ct)).ToArray());
        }

        await CSharpDbConnection.ClearAllPoolsAsync();

        await using (var connection = new CSharpDbConnection(ConnectionString(dbPath)))
        {
            await connection.OpenAsync(Ct);
            DataRow physicalKey = Assert.Single(
                connection.GetSchema(
                        "KeyConstraints",
                        [null, null, "RekeyCandidates", "PK_RekeyCandidates_Physical"])
                    .Rows
                    .Cast<DataRow>());
            Assert.Equal("PRIMARY KEY", physicalKey["CONSTRAINT_TYPE"]);
            Assert.Equal(1, physicalKey["COLUMN_COUNT"]);
            Assert.Equal(
                "Id",
                Assert.Single(
                    connection.GetSchema(
                            "KeyColumns",
                            [null, null, "RekeyCandidates", "PK_RekeyCandidates_Physical"])
                        .Rows
                        .Cast<DataRow>())["COLUMN_NAME"]);
        }

        await using (var db = new OrmMigrationContext(ConnectionString(dbPath)))
        {
            await db.Database.MigrateAsync(Ct);
        }

        await CSharpDbConnection.ClearAllPoolsAsync();

        await using (var reopened = new OrmMigrationContext(ConnectionString(dbPath)))
        {
            Assert.Equal(
                [InitialMigration, RewriteMigration, LatestMigration],
                (await reopened.Database.GetAppliedMigrationsAsync(Ct)).ToArray());

            OrmMember member = await reopened.Members
                .SingleAsync(item => item.Id == 5, Ct);
            Assert.Equal("LegacyCode", member.Handle);
            Assert.Equal(6D, member.Rating);

            OrmWorkItem workItem = await reopened.WorkItems
                .SingleAsync(item => item.TenantId == 3 && item.TaskId == 41, Ct);
            Assert.Equal("preserved task", workItem.Title);
        }

        Assert.Equal(
            2L,
            Convert.ToInt64(
                await ExecuteScalarAsync(
                    dbPath,
                    "SELECT COUNT(*) FROM RekeyCandidates WHERE Region = 'west';")));

        await ExerciseReferentialActionsAsync(dbPath);

        await using (var db = new OrmMigrationContext(ConnectionString(dbPath)))
        {
            await db.GetService<IMigrator>().MigrateAsync(InitialMigration, Ct);
        }

        await CSharpDbConnection.ClearAllPoolsAsync();

        await using (var connection = new CSharpDbConnection(ConnectionString(dbPath)))
        {
            await connection.OpenAsync(Ct);

            DataRow code = Assert.Single(
                connection.GetSchema(
                        "Columns",
                        [null, null, "MemberProfiles", "Code"])
                    .Rows
                    .Cast<DataRow>());
            Assert.Equal("TEXT", code["DATA_TYPE"]);
            Assert.Equal("NO", code["IS_NULLABLE"]);
            Assert.Equal("'legacy'", code["COLUMN_DEFAULT"]);
            Assert.Equal(DBNull.Value, code["COLLATION_NAME"]);

            DataRow rating = Assert.Single(
                connection.GetSchema(
                        "Columns",
                        [null, null, "MemberProfiles", "Rating"])
                    .Rows
                    .Cast<DataRow>());
            Assert.Equal("INTEGER", rating["DATA_TYPE"]);
            Assert.Equal("NO", rating["IS_NULLABLE"]);
            Assert.Equal("1", rating["COLUMN_DEFAULT"]);

            Assert.Single(
                connection.GetSchema(
                        "Indexes",
                        [null, null, "MemberProfiles", "IX_MemberProfiles_Code"])
                    .Rows
                    .Cast<DataRow>());
            Assert.Empty(
                connection.GetSchema(
                        "KeyConstraints",
                        [null, null, "RekeyCandidates", null])
                    .Rows
                    .Cast<DataRow>());
            Assert.DoesNotContain(
                connection.GetSchema("Tables").Rows.Cast<DataRow>(),
                row => string.Equals(
                    Assert.IsType<string>(row["TABLE_NAME"]),
                    "Members",
                    StringComparison.Ordinal));
        }

        Assert.Equal(
            "LegacyCode",
            await ExecuteScalarAsync(
                dbPath,
                "SELECT Code FROM MemberProfiles WHERE Id = 5;"));
        Assert.Equal(
            6L,
            Convert.ToInt64(
                await ExecuteScalarAsync(
                    dbPath,
                    "SELECT Rating FROM MemberProfiles WHERE Id = 5;")));
        Assert.Equal(
            2L,
            Convert.ToInt64(
                await ExecuteScalarAsync(
                    dbPath,
                    "SELECT COUNT(*) FROM RekeyCandidates;")));

        await ExecuteScriptAsync(
            dbPath,
            "INSERT INTO MemberProfiles (Id) VALUES (6);");
        Assert.Equal(
            0L,
            Convert.ToInt64(
                await ExecuteScalarAsync(
                    dbPath,
                    "SELECT COUNT(*) FROM MemberProfiles WHERE Id = 6 AND Code = 'LEGACY';")));

        await using (var db = new OrmMigrationContext(ConnectionString(dbPath)))
        {
            Assert.Equal(
                [InitialMigration],
                (await db.Database.GetAppliedMigrationsAsync(Ct)).ToArray());
            await db.Database.MigrateAsync(Ct);
        }

        await using var finalConnection = new CSharpDbConnection(ConnectionString(dbPath));
        await finalConnection.OpenAsync(Ct);
        AssertFinalSchema(finalConnection);
    }

    [Fact]
    public async Task FailedMigration_RollsBackSchemaHistoryAndData_AndDatabaseReopens()
    {
        string dbPath = DatabasePath("failed-rollback.db");

        await using (var db = new OrmRollbackContext(ConnectionString(dbPath)))
        {
            await db.GetService<IMigrator>().MigrateAsync(RollbackInitialMigration, Ct);
        }

        await ExecuteScriptAsync(
            dbPath,
            """
            INSERT INTO RollbackItems (Id, Score, Label)
            VALUES (1, NULL, 'preserved');
            """);

        await using (var db = new OrmRollbackContext(ConnectionString(dbPath)))
        {
            await Assert.ThrowsAsync<CSharpDbDataException>(
                () => db.GetService<IMigrator>()
                    .MigrateAsync(RollbackFailingMigration, Ct));
        }

        await CSharpDbConnection.ClearAllPoolsAsync();

        await using (var connection = new CSharpDbConnection(ConnectionString(dbPath)))
        {
            await connection.OpenAsync(Ct);

            DataRow score = Assert.Single(
                connection.GetSchema("Columns", [null, null, "RollbackItems", "Score"])
                    .Rows
                    .Cast<DataRow>());
            Assert.Equal("INTEGER", score["DATA_TYPE"]);
            Assert.Equal("YES", score["IS_NULLABLE"]);
            Assert.Equal("1", score["COLUMN_DEFAULT"]);

            DataTable columns = connection.GetSchema("Columns", [null, null, "RollbackItems", null]);
            Assert.DoesNotContain(
                columns.Rows.Cast<DataRow>(),
                row => string.Equals(
                    Assert.IsType<string>(row["COLUMN_NAME"]),
                    "MigrationMarker",
                    StringComparison.Ordinal));
        }

        Assert.Equal(
            "preserved",
            await ExecuteScalarAsync(
                dbPath,
                "SELECT Label FROM RollbackItems WHERE Id = 1;"));
        Assert.Equal(
            1L,
            Convert.ToInt64(
                await ExecuteScalarAsync(
                    dbPath,
                    "SELECT COUNT(*) FROM RollbackItems WHERE Id = 1 AND Score IS NULL;")));

        await using (var verify = new OrmRollbackContext(ConnectionString(dbPath)))
        {
            Assert.Equal(
                [RollbackInitialMigration],
                (await verify.Database.GetAppliedMigrationsAsync(Ct)).ToArray());
        }

        await ExecuteScriptAsync(
            dbPath,
            "UPDATE RollbackItems SET Score = 3 WHERE Id = 1;");

        await using (var recovered = new OrmRollbackContext(ConnectionString(dbPath)))
        {
            await recovered.GetService<IMigrator>()
                .MigrateAsync(RollbackFailingMigration, Ct);
            Assert.Equal(
                [RollbackInitialMigration, RollbackFailingMigration],
                (await recovered.Database.GetAppliedMigrationsAsync(Ct)).ToArray());
        }

        await CSharpDbConnection.ClearAllPoolsAsync();

        await using (var connection = new CSharpDbConnection(ConnectionString(dbPath)))
        {
            await connection.OpenAsync(Ct);
            DataRow score = Assert.Single(
                connection.GetSchema("Columns", [null, null, "RollbackItems", "Score"])
                    .Rows
                    .Cast<DataRow>());
            Assert.Equal("REAL", score["DATA_TYPE"]);
            Assert.Equal("NO", score["IS_NULLABLE"]);
            Assert.Equal("2.0", score["COLUMN_DEFAULT"]);

            Assert.Single(
                connection.GetSchema(
                        "Columns",
                        [null, null, "RollbackItems", "MigrationMarker"])
                    .Rows
                    .Cast<DataRow>());
        }

        Assert.Equal(
            3D,
            Convert.ToDouble(
                await ExecuteScalarAsync(
                    dbPath,
                    "SELECT Score FROM RollbackItems WHERE Id = 1;")));
    }

    [Fact]
    public void UnsupportedSequenceOperations_FailDuringSqlGeneration_WithStableDiagnostic()
    {
        using var db = new OrmMigrationContext(ConnectionString(DatabasePath("unsupported.db")));
        IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();

        MigrationOperation[] operations =
        [
            new CreateSequenceOperation
            {
                Name = "UnsupportedSequence",
                ClrType = typeof(long),
            },
            new AlterSequenceOperation
            {
                Name = "UnsupportedSequence",
            },
            new RenameSequenceOperation
            {
                Name = "UnsupportedSequence",
                NewName = "StillUnsupported",
            },
            new DropSequenceOperation
            {
                Name = "UnsupportedSequence",
            },
            new RestartSequenceOperation
            {
                Name = "UnsupportedSequence",
            },
        ];

        foreach (MigrationOperation operation in operations)
        {
            NotSupportedException error = Assert.Throws<NotSupportedException>(
                () => generator.Generate([operation], model: null));

            Assert.Equal(
                "CDBEF2001: The CSharpDB EF Core provider does not support sequences in v1.",
                error.Message);
        }
    }

    [Fact]
    public void UnsupportedCollation_FailsDuringSqlGeneration_WithStableDiagnostic()
    {
        using var db = new OrmMigrationContext(
            ConnectionString(DatabasePath("unsupported-collation.db")));
        IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();
        var alterColumn = new AlterColumnOperation
        {
            Name = "Code",
            Table = "Items",
            ClrType = typeof(string),
            ColumnType = "TEXT",
            IsNullable = true,
            Collation = "NOT_A_COLLATION",
        };
        alterColumn.OldColumn.ClrType = typeof(string);
        alterColumn.OldColumn.ColumnType = "TEXT";
        alterColumn.OldColumn.IsNullable = true;

        MigrationOperation[] operations =
        [
            new AddColumnOperation
            {
                Name = "Code",
                Table = "Items",
                ClrType = typeof(string),
                ColumnType = "TEXT",
                IsNullable = true,
                Collation = "NOT_A_COLLATION",
            },
            alterColumn,
        ];

        foreach (MigrationOperation operation in operations)
        {
            NotSupportedException error = Assert.Throws<NotSupportedException>(
                () => generator.Generate([operation], model: null));

            Assert.Equal(
                "CDBEF2001: The CSharpDB EF Core provider does not support collation 'NOT_A_COLLATION'. " +
                "Supported collations are BINARY, NOCASE, NOCASE_AI, ICU:<locale> in v1.",
                error.Message);
        }
    }

    [Fact]
    public void UnsupportedSchema_FailsDuringSqlGeneration_WithStableDiagnostic()
    {
        using var db = new OrmMigrationContext(
            ConnectionString(DatabasePath("unsupported-schema.db")));
        IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();

        NotSupportedException error = Assert.Throws<NotSupportedException>(
            () => generator.Generate(
                [
                    new RenameTableOperation
                    {
                        Name = "Items",
                        Schema = "tenant",
                        NewName = "RenamedItems",
                    },
                ],
                model: null));

        Assert.Equal(
            "CDBEF2001: Schemas are not supported by the CSharpDB EF Core provider. " +
            "'tenant.Items' is not valid.",
            error.Message);
    }

    [Fact]
    public void UnsupportedDatabaseAndTableAlterations_FailDuringSqlGeneration_WithStableDiagnostics()
    {
        using var db = new OrmMigrationContext(
            ConnectionString(DatabasePath("unsupported-alterations.db")));
        IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();

        (MigrationOperation Operation, string Feature)[] cases =
        [
            (new AlterDatabaseOperation(), "database-level alterations"),
            (
                new AlterTableOperation
                {
                    Name = "Items",
                },
                "table-level alterations"),
        ];

        foreach ((MigrationOperation operation, string feature) in cases)
        {
            NotSupportedException error = Assert.Throws<NotSupportedException>(
                () => generator.Generate([operation], model: null));

            Assert.Equal(
                $"CDBEF2001: The CSharpDB EF Core provider does not support {feature} in v1.",
                error.Message);
        }
    }

    [Fact]
    public void UnsupportedSchemaComments_FailDuringSqlGeneration_WithStableDiagnostics()
    {
        using var db = new OrmMigrationContext(
            ConnectionString(DatabasePath("unsupported-comments.db")));
        IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();

        (MigrationOperation Operation, string Feature)[] cases =
        [
            (
                new CreateTableOperation
                {
                    Name = "Items",
                    Comment = "unsupported table comment",
                },
                "table comments on table 'Items'"),
            (
                new AddColumnOperation
                {
                    Name = "Code",
                    Table = "Items",
                    ClrType = typeof(string),
                    ColumnType = "TEXT",
                    IsNullable = true,
                    Comment = "unsupported column comment",
                },
                "column comments on column 'Items.Code'"),
        ];

        foreach ((MigrationOperation operation, string feature) in cases)
        {
            NotSupportedException error = Assert.Throws<NotSupportedException>(
                () => generator.Generate([operation], model: null));

            Assert.Equal(
                $"CDBEF2001: The CSharpDB EF Core provider does not support {feature} in v1.",
                error.Message);
        }
    }

    private static void AssertFinalSchema(CSharpDbConnection connection)
    {
        DataRow handle = Assert.Single(
            connection.GetSchema("Columns", [null, null, "Members", "Handle"])
                .Rows
                .Cast<DataRow>());
        Assert.Equal("TEXT", handle["DATA_TYPE"]);
        Assert.Equal("YES", handle["IS_NULLABLE"]);
        Assert.Equal("'member'", handle["COLUMN_DEFAULT"]);
        Assert.Equal("NOCASE", handle["COLLATION_NAME"]);

        DataRow rating = Assert.Single(
            connection.GetSchema("Columns", [null, null, "Members", "Rating"])
                .Rows
                .Cast<DataRow>());
        Assert.Equal("REAL", rating["DATA_TYPE"]);
        Assert.Equal("NO", rating["IS_NULLABLE"]);
        Assert.Equal("2.0", rating["COLUMN_DEFAULT"]);

        DataRow memberIndex = Assert.Single(
            connection.GetSchema("Indexes", [null, null, "Members", "IX_Members_Handle"])
                .Rows
                .Cast<DataRow>());
        Assert.Equal("Handle", memberIndex["COLUMN_LIST"]);

        DataRow ratingCheck = Assert.Single(
            connection.GetSchema(
                    "CheckConstraints",
                    [null, null, "Members", "CK_Members_Rating"])
                .Rows
                .Cast<DataRow>());
        Assert.Contains(
            "\"Rating\" >= 0",
            Assert.IsType<string>(ratingCheck["CHECK_CLAUSE"]),
            StringComparison.Ordinal);

        DataRow compositePrimaryKey = Assert.Single(
            connection.GetSchema("KeyConstraints", [null, null, "RekeyCandidates", "PK_RekeyCandidates"])
                .Rows
                .Cast<DataRow>());
        Assert.Equal("PRIMARY KEY", compositePrimaryKey["CONSTRAINT_TYPE"]);

        DataRow[] keyColumns = connection.GetSchema(
                "KeyColumns",
                [null, null, "RekeyCandidates", "PK_RekeyCandidates"])
            .Rows
            .Cast<DataRow>()
            .OrderBy(row => Convert.ToInt32(row["ORDINAL_POSITION"]))
            .ToArray();
        Assert.Equal(
            ["Region", "Id"],
            keyColumns.Select(row => Assert.IsType<string>(row["COLUMN_NAME"])).ToArray());

        DataRow[] relationshipColumns = connection.GetSchema(
                "ForeignKeys",
                [null, null, "WorkItems", "FK_WorkItems_Organizations_TenantId_OrganizationId"])
            .Rows
            .Cast<DataRow>()
            .OrderBy(row => Convert.ToInt32(row["ORDINAL_POSITION"]))
            .ToArray();
        Assert.All(
            relationshipColumns,
            relationship => Assert.Equal("CASCADE", relationship["DELETE_RULE"]));
        Assert.Equal(
            ["TenantId", "OrganizationId"],
            relationshipColumns
                .Select(row => Assert.IsType<string>(row["COLUMN_NAME"]))
                .ToArray());

        DataRow[] actions = connection.GetSchema("ForeignKeys")
            .Rows
            .Cast<DataRow>()
            .Where(row => Assert.IsType<string>(row["TABLE_NAME"]).StartsWith(
                "Action",
                StringComparison.Ordinal))
            .ToArray();
        Assert.Equal(
            ["CASCADE", "NO ACTION", "RESTRICT", "SET DEFAULT", "SET NULL"],
            actions
                .Select(row => Assert.IsType<string>(row["DELETE_RULE"]))
                .Order(StringComparer.Ordinal)
                .ToArray());
        Assert.Equal(
            ["CASCADE", "NO ACTION", "RESTRICT", "SET DEFAULT", "SET NULL"],
            actions
                .Select(row => Assert.IsType<string>(row["UPDATE_RULE"]))
                .Order(StringComparer.Ordinal)
                .ToArray());
    }

    private static async Task ExerciseReferentialActionsAsync(string dbPath)
    {
        await ExecuteScriptAsync(
            dbPath,
            """
            INSERT INTO ActionParents (Id)
            VALUES (1), (20), (30), (32), (40), (42), (50), (60);
            INSERT INTO ActionCascade (Id, ParentId) VALUES (1, 20);
            INSERT INTO ActionSetNull (Id, ParentId) VALUES (1, 30), (2, 32);
            INSERT INTO ActionSetDefault (Id, ParentId) VALUES (1, 40), (2, 42);
            INSERT INTO ActionRestrict (Id, ParentId) VALUES (1, 50);
            INSERT INTO ActionNoAction (Id, ParentId) VALUES (1, 60);
            UPDATE ActionParents SET Id = 21 WHERE Id = 20;
            UPDATE ActionParents SET Id = 31 WHERE Id = 30;
            UPDATE ActionParents SET Id = 41 WHERE Id = 40;
            DELETE FROM ActionParents WHERE Id = 21;
            DELETE FROM ActionParents WHERE Id = 32;
            DELETE FROM ActionParents WHERE Id = 42;
            """);

        Assert.Equal(
            0L,
            Convert.ToInt64(
                await ExecuteScalarAsync(
                    dbPath,
                    "SELECT COUNT(*) FROM ActionCascade;")));
        Assert.Equal(
            2L,
            Convert.ToInt64(
                await ExecuteScalarAsync(
                    dbPath,
                    "SELECT COUNT(*) FROM ActionSetNull WHERE ParentId IS NULL;")));
        Assert.Equal(
            2L,
            Convert.ToInt64(
                await ExecuteScalarAsync(
                    dbPath,
                    "SELECT COUNT(*) FROM ActionSetDefault WHERE ParentId = 1;")));

        foreach (string blockedStatement in
                 new[]
                 {
                     "UPDATE ActionParents SET Id = 51 WHERE Id = 50;",
                     "DELETE FROM ActionParents WHERE Id = 50;",
                     "UPDATE ActionParents SET Id = 61 WHERE Id = 60;",
                     "DELETE FROM ActionParents WHERE Id = 60;",
                 })
        {
            await Assert.ThrowsAsync<CSharpDbDataException>(
                () => ExecuteScriptAsync(dbPath, blockedStatement));
        }

        Assert.Equal(
            2L,
            Convert.ToInt64(
                await ExecuteScalarAsync(
                    dbPath,
                    "SELECT COUNT(*) FROM ActionParents WHERE Id IN (50, 60);")));
    }

    private string DatabasePath(string name) => Path.Combine(_workspace, name);

    private static string ConnectionString(string dbPath) => $"Data Source={dbPath}";

    private static string ReadCorpus(string name)
    {
        string path = Path.Combine(AppContext.BaseDirectory, "MigrationCorpus", name);
        return NormalizeSql(File.ReadAllText(path));
    }

    private static string NormalizeSql(string sql) =>
        sql.Replace("\r\n", "\n", StringComparison.Ordinal).Trim() + "\n";

    private static async Task ExecuteScriptAsync(string dbPath, string script)
    {
        await using var connection = new CSharpDbConnection(ConnectionString(dbPath));
        await connection.OpenAsync(Ct);

        foreach (string statement in SqlScriptSplitter.SplitExecutableStatements(script))
        {
            await using var command = connection.CreateCommand();
            command.CommandText = statement;
            await command.ExecuteNonQueryAsync(Ct);
        }
    }

    private static async Task<object?> ExecuteScalarAsync(string dbPath, string sql)
    {
        await using var connection = new CSharpDbConnection(ConnectionString(dbPath));
        await connection.OpenAsync(Ct);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteScalarAsync(Ct);
    }

    private sealed class OrmMigrationContext(string connectionString) : DbContext
    {
        public DbSet<OrmOrganization> Organizations => Set<OrmOrganization>();

        public DbSet<OrmWorkItem> WorkItems => Set<OrmWorkItem>();

        public DbSet<OrmMember> Members => Set<OrmMember>();

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) =>
            optionsBuilder.UseCSharpDb(connectionString);

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<OrmOrganization>(organization =>
            {
                organization.ToTable(
                    "Organizations",
                    table => table.HasCheckConstraint(
                        "CK_Organizations_Name",
                        "Name <> ''"));
                organization.HasKey(item => new { item.TenantId, item.OrganizationId })
                    .HasName("PK_Organizations");
                organization.Property(item => item.Name)
                    .HasDefaultValue("unnamed");
            });

            modelBuilder.Entity<OrmWorkItem>(workItem =>
            {
                workItem.ToTable(
                    "WorkItems",
                    table => table.HasCheckConstraint(
                        "CK_WorkItems_State",
                        "State IN ('open', 'closed')"));
                workItem.HasKey(item => new { item.TenantId, item.TaskId })
                    .HasName("PK_WorkItems");
                workItem.Property(item => item.Title)
                    .HasDefaultValue("untitled");
                workItem.Property(item => item.State)
                    .HasDefaultValue("open");
                workItem.HasOne(item => item.Organization)
                    .WithMany(item => item.WorkItems)
                    .HasForeignKey(item => new { item.TenantId, item.OrganizationId })
                    .HasConstraintName("FK_WorkItems_Organizations_TenantId_OrganizationId")
                    .OnDelete(DeleteBehavior.Cascade);
            });

            modelBuilder.Entity<OrmMember>(member =>
            {
                member.ToTable(
                    "Members",
                    table => table.HasCheckConstraint(
                        "CK_Members_Rating",
                        "Rating >= 0"));
                member.HasKey(item => item.Id)
                    .HasName("PK_Members");
                member.Property(item => item.Handle)
                    .UseCollation("NOCASE")
                    .HasDefaultValue("member");
                member.Property(item => item.Rating)
                    .HasDefaultValue(2D);
                member.Property(item => item.State)
                    .HasDefaultValue("active");
                member.HasIndex(item => item.Handle)
                    .HasDatabaseName("IX_Members_Handle");
            });
        }
    }

    private sealed class OrmRollbackContext(string connectionString) : DbContext
    {
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) =>
            optionsBuilder.UseCSharpDb(connectionString);
    }

    private sealed class OrmOrganization
    {
        public int TenantId { get; set; }

        public int OrganizationId { get; set; }

        public string Name { get; set; } = string.Empty;

        public List<OrmWorkItem> WorkItems { get; set; } = [];
    }

    private sealed class OrmWorkItem
    {
        public int TenantId { get; set; }

        public int TaskId { get; set; }

        public int OrganizationId { get; set; }

        public string Title { get; set; } = string.Empty;

        public string State { get; set; } = "open";

        public OrmOrganization Organization { get; set; } = null!;
    }

    private sealed class OrmMember
    {
        public int Id { get; set; }

        public string? Handle { get; set; }

        public double Rating { get; set; }

        public string State { get; set; } = "active";
    }

    [DbContext(typeof(OrmMigrationContext))]
    [Migration(InitialMigration)]
    private sealed class OrmCorpusInitial : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "Organizations",
                columns: table => new
                {
                    TenantId = table.Column<int>(type: "INTEGER", nullable: false),
                    OrganizationId = table.Column<int>(type: "INTEGER", nullable: false),
                    Name = table.Column<string>(
                        type: "TEXT",
                        nullable: false,
                        defaultValue: "unnamed"),
                },
                constraints: table =>
                {
                    table.PrimaryKey(
                        "PK_Organizations",
                        row => new { row.TenantId, row.OrganizationId });
                    table.CheckConstraint(
                        "CK_Organizations_Name",
                        "Name <> ''");
                });

            migrationBuilder.CreateTable(
                name: "WorkItems",
                columns: table => new
                {
                    TenantId = table.Column<int>(type: "INTEGER", nullable: false),
                    TaskId = table.Column<int>(type: "INTEGER", nullable: false),
                    OrganizationId = table.Column<int>(type: "INTEGER", nullable: false),
                    Title = table.Column<string>(
                        type: "TEXT",
                        nullable: false,
                        defaultValue: "untitled"),
                    State = table.Column<string>(
                        type: "TEXT",
                        nullable: false,
                        defaultValue: "open"),
                },
                constraints: table =>
                {
                    table.PrimaryKey(
                        "PK_WorkItems",
                        row => new { row.TenantId, row.TaskId });
                    table.CheckConstraint(
                        "CK_WorkItems_State",
                        "State IN ('open', 'closed')");
                    table.ForeignKey(
                        name: "FK_WorkItems_Organizations_TenantId_OrganizationId",
                        columns: row => new { row.TenantId, row.OrganizationId },
                        principalTable: "Organizations",
                        principalColumns: new[] { "TenantId", "OrganizationId" },
                        onUpdate: ReferentialAction.Cascade,
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_WorkItems_TenantId_OrganizationId",
                table: "WorkItems",
                columns: new[] { "TenantId", "OrganizationId" });

            migrationBuilder.CreateTable(
                name: "MemberProfiles",
                columns: table => new
                {
                    Id = table.Column<int>(type: "INTEGER", nullable: false),
                    Code = table.Column<string>(
                        type: "TEXT",
                        nullable: false,
                        defaultValue: "legacy",
                        collation: "BINARY"),
                    Rating = table.Column<int>(
                        type: "INTEGER",
                        nullable: false,
                        defaultValue: 1),
                    State = table.Column<string>(
                        type: "TEXT",
                        nullable: false,
                        defaultValue: "active"),
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Members", row => row.Id);
                    table.CheckConstraint("CK_Members_Rating", "Rating >= 0");
                });

            migrationBuilder.CreateIndex(
                name: "IX_MemberProfiles_Code",
                table: "MemberProfiles",
                column: "Code");

            migrationBuilder.CreateTable(
                name: "RekeyCandidates",
                columns: table => new
                {
                    Id = table.Column<int>(type: "INTEGER", nullable: false),
                    Label = table.Column<string>(type: "TEXT", nullable: false),
                });

            migrationBuilder.CreateTable(
                name: "ActionParents",
                columns: table => new
                {
                    Id = table.Column<int>(type: "INTEGER", nullable: false),
                },
                constraints: table =>
                    table.PrimaryKey("PK_ActionParents", row => row.Id));

            CreateActionChild(
                migrationBuilder,
                "ActionNoAction",
                "FK_ActionNoAction_Parents",
                nullable: false,
                defaultValue: null,
                ReferentialAction.NoAction);
            CreateActionChild(
                migrationBuilder,
                "ActionRestrict",
                "FK_ActionRestrict_Parents",
                nullable: false,
                defaultValue: null,
                ReferentialAction.Restrict);
            CreateActionChild(
                migrationBuilder,
                "ActionCascade",
                "FK_ActionCascade_Parents",
                nullable: false,
                defaultValue: null,
                ReferentialAction.Cascade);
            CreateActionChild(
                migrationBuilder,
                "ActionSetNull",
                "FK_ActionSetNull_Parents",
                nullable: true,
                defaultValue: null,
                ReferentialAction.SetNull);
            CreateActionChild(
                migrationBuilder,
                "ActionSetDefault",
                "FK_ActionSetDefault_Parents",
                nullable: false,
                defaultValue: 1,
                ReferentialAction.SetDefault);
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable("ActionCascade");
            migrationBuilder.DropTable("ActionNoAction");
            migrationBuilder.DropTable("ActionRestrict");
            migrationBuilder.DropTable("ActionSetDefault");
            migrationBuilder.DropTable("ActionSetNull");
            migrationBuilder.DropTable("WorkItems");
            migrationBuilder.DropTable("ActionParents");
            migrationBuilder.DropTable("MemberProfiles");
            migrationBuilder.DropTable("Organizations");
            migrationBuilder.DropTable("RekeyCandidates");
        }

        private static void CreateActionChild(
            MigrationBuilder migrationBuilder,
            string tableName,
            string foreignKeyName,
            bool nullable,
            int? defaultValue,
            ReferentialAction action)
        {
            migrationBuilder.CreateTable(
                name: tableName,
                columns: table => new
                {
                    Id = table.Column<int>(type: "INTEGER", nullable: false),
                    ParentId = table.Column<int>(
                        type: "INTEGER",
                        nullable: nullable,
                        defaultValue: defaultValue),
                },
                constraints: table =>
                {
                    table.PrimaryKey($"PK_{tableName}", row => row.Id);
                    table.ForeignKey(
                        name: foreignKeyName,
                        column: row => row.ParentId,
                        principalTable: "ActionParents",
                        principalColumn: "Id",
                        onUpdate: action,
                        onDelete: action);
                });
        }
    }

    [DbContext(typeof(OrmMigrationContext))]
    [Migration(RewriteMigration)]
    private sealed class OrmCorpusRewriteAndRename : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddPrimaryKey(
                name: "PK_RekeyCandidates_Physical",
                table: "RekeyCandidates",
                column: "Id");

            migrationBuilder.AlterColumn<double>(
                name: "Rating",
                table: "MemberProfiles",
                type: "REAL",
                nullable: false,
                defaultValue: 2D,
                oldClrType: typeof(int),
                oldType: "INTEGER",
                oldNullable: false,
                oldDefaultValue: 1);

            migrationBuilder.AlterColumn<string>(
                name: "Code",
                table: "MemberProfiles",
                type: "TEXT",
                nullable: true,
                defaultValue: "member",
                collation: "NOCASE",
                oldClrType: typeof(string),
                oldType: "TEXT",
                oldNullable: false,
                oldDefaultValue: "legacy",
                oldCollation: "BINARY");

            migrationBuilder.RenameTable(
                name: "MemberProfiles",
                newName: "Members");
            migrationBuilder.RenameColumn(
                name: "Code",
                table: "Members",
                newName: "Handle");
            migrationBuilder.RenameIndex(
                name: "IX_MemberProfiles_Code",
                table: "Members",
                newName: "IX_Members_Handle");
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.RenameIndex(
                name: "IX_Members_Handle",
                table: "Members",
                newName: "IX_MemberProfiles_Code");
            migrationBuilder.RenameColumn(
                name: "Handle",
                table: "Members",
                newName: "Code");
            migrationBuilder.RenameTable(
                name: "Members",
                newName: "MemberProfiles");

            migrationBuilder.AlterColumn<string>(
                name: "Code",
                table: "MemberProfiles",
                type: "TEXT",
                nullable: false,
                defaultValue: "legacy",
                collation: "BINARY",
                oldClrType: typeof(string),
                oldType: "TEXT",
                oldNullable: true,
                oldDefaultValue: "member",
                oldCollation: "NOCASE");

            migrationBuilder.AlterColumn<int>(
                name: "Rating",
                table: "MemberProfiles",
                type: "INTEGER",
                nullable: false,
                defaultValue: 1,
                oldClrType: typeof(double),
                oldType: "REAL",
                oldNullable: false,
                oldDefaultValue: 2D);

            migrationBuilder.DropPrimaryKey(
                name: "PK_RekeyCandidates_Physical",
                table: "RekeyCandidates");
        }
    }

    [DbContext(typeof(OrmMigrationContext))]
    [Migration(LatestMigration)]
    private sealed class OrmCorpusCompositeRekey : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropPrimaryKey(
                name: "PK_RekeyCandidates_Physical",
                table: "RekeyCandidates");
            migrationBuilder.AddColumn<string>(
                name: "Region",
                table: "RekeyCandidates",
                type: "TEXT",
                nullable: false,
                defaultValue: "west");
            migrationBuilder.AddPrimaryKey(
                name: "PK_RekeyCandidates",
                table: "RekeyCandidates",
                columns: new[] { "Region", "Id" });
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropPrimaryKey(
                name: "PK_RekeyCandidates",
                table: "RekeyCandidates");
            migrationBuilder.DropColumn(
                name: "Region",
                table: "RekeyCandidates");
            migrationBuilder.AddPrimaryKey(
                name: "PK_RekeyCandidates_Physical",
                table: "RekeyCandidates",
                column: "Id");
        }
    }

    [DbContext(typeof(OrmRollbackContext))]
    [Migration(RollbackInitialMigration)]
    private sealed class OrmRollbackInitial : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "RollbackItems",
                columns: table => new
                {
                    Id = table.Column<int>(type: "INTEGER", nullable: false),
                    Score = table.Column<int>(
                        type: "INTEGER",
                        nullable: true,
                        defaultValue: 1),
                    Label = table.Column<string>(type: "TEXT", nullable: false),
                },
                constraints: table =>
                    table.PrimaryKey("PK_RollbackItems", row => row.Id));
        }

        protected override void Down(MigrationBuilder migrationBuilder) =>
            migrationBuilder.DropTable("RollbackItems");
    }

    [DbContext(typeof(OrmRollbackContext))]
    [Migration(RollbackFailingMigration)]
    private sealed class OrmRollbackFails : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "MigrationMarker",
                table: "RollbackItems",
                type: "TEXT",
                nullable: true);
            migrationBuilder.AlterColumn<double>(
                name: "Score",
                table: "RollbackItems",
                type: "REAL",
                nullable: false,
                defaultValue: 2D,
                oldClrType: typeof(int),
                oldType: "INTEGER",
                oldNullable: true,
                oldDefaultValue: 1);
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AlterColumn<int>(
                name: "Score",
                table: "RollbackItems",
                type: "INTEGER",
                nullable: true,
                defaultValue: 1,
                oldClrType: typeof(double),
                oldType: "REAL",
                oldNullable: false,
                oldDefaultValue: 2D);
            migrationBuilder.DropColumn(
                name: "MigrationMarker",
                table: "RollbackItems");
        }
    }
}
