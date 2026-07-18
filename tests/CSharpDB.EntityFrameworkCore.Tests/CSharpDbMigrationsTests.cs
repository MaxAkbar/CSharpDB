using CSharpDB.Data;
using CSharpDB.Sql;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Storage;

namespace CSharpDB.EntityFrameworkCore.Tests;

[Collection("ConnectionPoolState")]
public sealed class CSharpDbMigrationsTests : IAsyncLifetime
{
    private readonly string _workspace = Path.Combine(Path.GetTempPath(), $"csharpdb_efcore_migrations_{Guid.NewGuid():N}");

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
    public async Task DatabaseMigrate_AppliesCompiledMigrations_AndCanBeReRun()
    {
        string dbPath = Path.Combine(_workspace, "migrations.db");

        await using var db = new MigrationRuntimeContext($"Data Source={dbPath}");

        await db.Database.MigrateAsync(Ct);
        await db.Database.MigrateAsync(Ct);

        IReadOnlyList<string> appliedMigrations = (await db.Database.GetAppliedMigrationsAsync(Ct)).ToList();

        Assert.Equal(
            [
                "202604160001_InitialCreate",
                "202604160002_AddSlugIndex",
            ],
            appliedMigrations);

        db.Blogs.Add(new MigrationBlog
        {
            Name = "release-notes",
            Slug = "release-notes",
        });

        await db.SaveChangesAsync(Ct);

        Assert.Equal(1, await db.Blogs.CountAsync(Ct));
    }

    [Fact]
    public void Migrator_GenerateScript_ProducesSupportedSql()
    {
        string dbPath = Path.Combine(_workspace, "script.db");

        using var db = new MigrationRuntimeContext($"Data Source={dbPath}");
        IMigrator migrator = db.GetService<IMigrator>();

        string script = migrator.GenerateScript();

        Assert.Contains("CREATE TABLE \"Blogs\"", script, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE \"Blogs\" ADD COLUMN \"Slug\" TEXT", script, StringComparison.Ordinal);
        Assert.Contains("CREATE INDEX \"IX_Blogs_Slug\" ON \"Blogs\" (\"Slug\")", script, StringComparison.Ordinal);
    }

    [Fact]
    public void SqlGenerationHelper_QuotesAndEscapesIdentifiers()
    {
        string dbPath = Path.Combine(_workspace, "identifier-generation.db");

        using var db = new MigrationRuntimeContext($"Data Source={dbPath}");
        ISqlGenerationHelper helper = db.GetService<ISqlGenerationHelper>();

        Assert.Equal("\"select\"", helper.DelimitIdentifier("select"));
        Assert.Equal("\"display \"\"name\"\"\"", helper.DelimitIdentifier("display \"name\""));
    }

    [Fact]
    public void Migrator_GenerateIdempotentScript_ProducesConditionalMigrationBlocks()
    {
        string dbPath = Path.Combine(_workspace, "idempotent.db");

        using var db = new MigrationRuntimeContext($"Data Source={dbPath}");
        IMigrator migrator = db.GetService<IMigrator>();

        string script = migrator.GenerateScript(options: MigrationsSqlGenerationOptions.Idempotent);

        Assert.Contains(
            "IF NOT EXISTS (SELECT 1 FROM __EFMigrationsHistory WHERE MigrationId = '202604160001_InitialCreate') BEGIN",
            script,
            StringComparison.Ordinal);
        Assert.Contains(
            "IF NOT EXISTS (SELECT 1 FROM __EFMigrationsHistory WHERE MigrationId = '202604160002_AddSlugIndex') BEGIN",
            script,
            StringComparison.Ordinal);
        Assert.True(script.Split("IF NOT EXISTS", StringSplitOptions.None).Length - 1 >= 2);
        Assert.True(script.Split("END;", StringSplitOptions.None).Length - 1 >= 2);
    }

    [Fact]
    public async Task Migrator_GenerateIdempotentScript_CanRunRepeatedlyFromEmptyDatabase()
    {
        string dbPath = Path.Combine(_workspace, "idempotent-empty.db");
        string script;

        await using (var db = new MigrationRuntimeContext($"Data Source={dbPath}"))
        {
            script = db.GetService<IMigrator>()
                .GenerateScript(options: MigrationsSqlGenerationOptions.Idempotent);
        }

        await ExecuteScriptAsync(dbPath, script);
        await ExecuteScriptAsync(dbPath, script);

        await using var verify = new MigrationRuntimeContext($"Data Source={dbPath}");
        Assert.Equal(2, (await verify.Database.GetAppliedMigrationsAsync(Ct)).Count());
        Assert.Equal(0, await verify.Blogs.CountAsync(Ct));
    }

    [Fact]
    public async Task Migrator_GenerateIdempotentScript_AppliesOnlyMissingMigration()
    {
        string dbPath = Path.Combine(_workspace, "idempotent-partial.db");
        string script;

        await using (var db = new MigrationRuntimeContext($"Data Source={dbPath}"))
        {
            IMigrator migrator = db.GetService<IMigrator>();
            script = migrator.GenerateScript(options: MigrationsSqlGenerationOptions.Idempotent);
        }

        await ExecuteScriptAsync(
            dbPath,
            """
            CREATE TABLE Blogs (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            CREATE TABLE __EFMigrationsHistory (
                MigrationId TEXT NOT NULL PRIMARY KEY,
                ProductVersion TEXT NOT NULL
            );
            INSERT INTO __EFMigrationsHistory (MigrationId, ProductVersion)
            VALUES ('202604160001_InitialCreate', '10.0.8');
            INSERT INTO Blogs (Name) VALUES ('preserved');
            """);
        await ExecuteScriptAsync(dbPath, script);

        await using var verify = new MigrationRuntimeContext($"Data Source={dbPath}");
        Assert.Equal(2, (await verify.Database.GetAppliedMigrationsAsync(Ct)).Count());
        MigrationBlog blog = await verify.Blogs.SingleAsync(Ct);
        Assert.Equal("preserved", blog.Name);
        Assert.Null(blog.Slug);
    }

    [Fact]
    public async Task MigrationsSqlGenerator_StandaloneForeignKey_CanBeAddedAndDropped()
    {
        string dbPath = Path.Combine(_workspace, "foreign-key-generator.db");
        await ExecuteScriptAsync(
            dbPath,
            """
            CREATE TABLE Blogs (Id INTEGER PRIMARY KEY);
            CREATE TABLE Posts (Id INTEGER PRIMARY KEY, BlogId INTEGER);
            INSERT INTO Blogs (Id) VALUES (1);
            INSERT INTO Posts (Id, BlogId) VALUES (1, 1);
            """);

        string addSql;
        string dropSql;
        using (var db = new MigrationRuntimeContext($"Data Source={dbPath}"))
        {
            IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();
            addSql = Assert.Single(generator.Generate(
                [
                    new AddForeignKeyOperation
                    {
                        Name = "FK_Posts_Blogs_BlogId",
                        Table = "Posts",
                        Columns = ["BlogId"],
                        PrincipalTable = "Blogs",
                        PrincipalColumns = ["Id"],
                        OnDelete = ReferentialAction.Cascade,
                    },
                ],
                model: null)).CommandText;
            dropSql = Assert.Single(generator.Generate(
                [
                    new DropForeignKeyOperation
                    {
                        Name = "FK_Posts_Blogs_BlogId",
                        Table = "Posts",
                    },
                ],
                model: null)).CommandText;
        }

        await ExecuteScriptAsync(dbPath, addSql);
        await CSharpDbConnection.ClearAllPoolsAsync();
        await Assert.ThrowsAsync<CSharpDbDataException>(
            () => ExecuteScriptAsync(dbPath, "INSERT INTO Posts (Id, BlogId) VALUES (2, 999);"));

        await ExecuteScriptAsync(dbPath, dropSql);
        await ExecuteScriptAsync(dbPath, "INSERT INTO Posts (Id, BlogId) VALUES (2, 999);");
    }

    [Fact]
    public async Task MigrationsSqlGenerator_CreateTable_PreservesForeignAndUniqueConstraintNames()
    {
        string dbPath = Path.Combine(_workspace, "named-create-constraints.db");
        string createSql;
        string dropSql;

        using (var db = new MigrationRuntimeContext($"Data Source={dbPath}"))
        {
            IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();
            var parent = new CreateTableOperation { Name = "Parents" };
            parent.Columns.Add(new AddColumnOperation
            {
                Name = "Id",
                Table = parent.Name,
                ClrType = typeof(int),
                ColumnType = "INTEGER",
                IsNullable = false,
            });
            parent.PrimaryKey = new AddPrimaryKeyOperation
            {
                Name = "PK_Parents",
                Table = parent.Name,
                Columns = ["Id"],
            };

            var child = new CreateTableOperation { Name = "Children" };
            child.Columns.Add(new AddColumnOperation
            {
                Name = "Id",
                Table = child.Name,
                ClrType = typeof(int),
                ColumnType = "INTEGER",
                IsNullable = false,
            });
            child.Columns.Add(new AddColumnOperation
            {
                Name = "ParentId",
                Table = child.Name,
                ClrType = typeof(int),
                ColumnType = "INTEGER",
                IsNullable = false,
            });
            child.Columns.Add(new AddColumnOperation
            {
                Name = "Code",
                Table = child.Name,
                ClrType = typeof(string),
                ColumnType = "TEXT",
                IsNullable = false,
            });
            child.PrimaryKey = new AddPrimaryKeyOperation
            {
                Name = "PK_Children",
                Table = child.Name,
                Columns = ["Id"],
            };
            child.UniqueConstraints.Add(new AddUniqueConstraintOperation
            {
                Name = "AK_Children_Code",
                Table = child.Name,
                Columns = ["Code"],
            });
            child.ForeignKeys.Add(new AddForeignKeyOperation
            {
                Name = "FK_Children_Parents_ParentId",
                Table = child.Name,
                Columns = ["ParentId"],
                PrincipalTable = parent.Name,
                PrincipalColumns = ["Id"],
            });

            createSql = string.Concat(generator.Generate([parent, child], model: null)
                .Select(command => command.CommandText));
            dropSql = string.Concat(generator.Generate(
                [
                    new DropForeignKeyOperation
                    {
                        Name = "FK_Children_Parents_ParentId",
                        Table = "Children",
                    },
                    new DropUniqueConstraintOperation
                    {
                        Name = "AK_Children_Code",
                        Table = "Children",
                    },
                ],
                model: null).Select(command => command.CommandText));
        }

        Assert.Contains(
            "CONSTRAINT \"FK_Children_Parents_ParentId\" FOREIGN KEY",
            createSql,
            StringComparison.Ordinal);
        Assert.Contains(
            "CONSTRAINT \"PK_Parents\" PRIMARY KEY (\"Id\")",
            createSql,
            StringComparison.Ordinal);
        Assert.Contains(
            "CONSTRAINT \"AK_Children_Code\" UNIQUE",
            createSql,
            StringComparison.Ordinal);

        await ExecuteScriptAsync(dbPath, createSql);
        await ExecuteScriptAsync(dbPath, "INSERT INTO Parents (Id) VALUES (1);");
        await ExecuteScriptAsync(
            dbPath,
            "INSERT INTO Children (Id, ParentId, Code) VALUES (1, 1, 'same');");
        await ExecuteScriptAsync(dbPath, dropSql);
        await ExecuteScriptAsync(
            dbPath,
            "INSERT INTO Children (Id, ParentId, Code) VALUES (2, 999, 'same');");
    }

    [Fact]
    public async Task MigrationsSqlGenerator_StandaloneCompositeForeignKey_CanBeAddedAndDropped()
    {
        string dbPath = Path.Combine(_workspace, "composite-foreign-key-generator.db");
        await ExecuteScriptAsync(
            dbPath,
            """
            CREATE TABLE Parents (
                TenantId INTEGER NOT NULL,
                ParentNo INTEGER NOT NULL,
                CONSTRAINT PK_Parents PRIMARY KEY (TenantId, ParentNo)
            );
            CREATE TABLE Children (
                Id INTEGER PRIMARY KEY,
                TenantId INTEGER NOT NULL,
                ParentNo INTEGER NOT NULL
            );
            INSERT INTO Parents (TenantId, ParentNo) VALUES (7, 42);
            INSERT INTO Children (Id, TenantId, ParentNo) VALUES (1, 7, 42);
            """);

        string addSql;
        string dropSql;
        using (var db = new MigrationRuntimeContext($"Data Source={dbPath}"))
        {
            IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();
            addSql = Assert.Single(generator.Generate(
                [
                    new AddForeignKeyOperation
                    {
                        Name = "FK_Children_Parents",
                        Table = "Children",
                        Columns = ["TenantId", "ParentNo"],
                        PrincipalTable = "Parents",
                        PrincipalColumns = ["TenantId", "ParentNo"],
                    },
                ],
                model: null)).CommandText;
            dropSql = Assert.Single(generator.Generate(
                [
                    new DropForeignKeyOperation
                    {
                        Name = "FK_Children_Parents",
                        Table = "Children",
                    },
                ],
                model: null)).CommandText;
        }

        await ExecuteScriptAsync(dbPath, addSql);
        await Assert.ThrowsAsync<CSharpDbDataException>(
            () => ExecuteScriptAsync(
                dbPath,
                "INSERT INTO Children (Id, TenantId, ParentNo) VALUES (2, 7, 999);"));

        await ExecuteScriptAsync(dbPath, dropSql);
        await ExecuteScriptAsync(
            dbPath,
            "INSERT INTO Children (Id, TenantId, ParentNo) VALUES (2, 7, 999);");
    }

    [Fact]
    public void MigrationsSqlGenerator_EmitsCompositeForeignKeysAndStandalonePrimaryKeyChanges()
    {
        string dbPath = Path.Combine(_workspace, "key-limit-generator.db");

        using var db = new MigrationRuntimeContext($"Data Source={dbPath}");
        IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();

        var createTable = new CreateTableOperation { Name = "child" };
        createTable.Columns.Add(new AddColumnOperation
        {
            Name = "TenantId",
            Table = createTable.Name,
            ClrType = typeof(int),
            ColumnType = "INTEGER",
            IsNullable = false,
        });
        createTable.Columns.Add(new AddColumnOperation
        {
            Name = "OrderId",
            Table = createTable.Name,
            ClrType = typeof(int),
            ColumnType = "INTEGER",
            IsNullable = false,
        });
        createTable.ForeignKeys.Add(new AddForeignKeyOperation
        {
            Name = "FK_child_parent",
            Table = createTable.Name,
            Columns = ["TenantId", "OrderId"],
            PrincipalTable = "parent",
            PrincipalColumns = ["TenantId", "OrderId"],
        });

        string compositeForeignKeySql = Assert.Single(
            generator.Generate([createTable], model: null)).CommandText;
        Assert.Contains(
            "CONSTRAINT \"FK_child_parent\" FOREIGN KEY (\"TenantId\", \"OrderId\") " +
            "REFERENCES \"parent\" (\"TenantId\", \"OrderId\")",
            compositeForeignKeySql,
            StringComparison.Ordinal);

        var addPrimaryKey = new AddPrimaryKeyOperation
        {
            Name = "PK_child",
            Table = "child",
            Columns = ["TenantId", "OrderId"],
        };
        string addPrimaryKeySql = Assert.Single(
            generator.Generate([addPrimaryKey], model: null)).CommandText;
        string dropPrimaryKeySql = Assert.Single(
            generator.Generate(
                [
                    new DropPrimaryKeyOperation
                    {
                        Name = "PK_child",
                        Table = "child",
                    },
                ],
                model: null)).CommandText;

        Assert.Contains(
            "ALTER TABLE \"child\" ADD CONSTRAINT \"PK_child\" PRIMARY KEY (\"TenantId\", \"OrderId\")",
            addPrimaryKeySql,
            StringComparison.Ordinal);
        Assert.Contains(
            "ALTER TABLE \"child\" DROP CONSTRAINT \"PK_child\"",
            dropPrimaryKeySql,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task MigrationsSqlGenerator_StandaloneCompositePrimaryKey_CanBeAddedAndDropped()
    {
        string dbPath = Path.Combine(_workspace, "composite-primary-key-generator.db");
        await ExecuteScriptAsync(
            dbPath,
            """
            CREATE TABLE Items (
                TenantId INTEGER NOT NULL,
                Code TEXT NOT NULL,
                Payload TEXT
            );
            INSERT INTO Items (TenantId, Code, Payload)
            VALUES (7, 'alpha', 'first'), (7, 'beta', 'second');
            """);

        string addSql;
        string dropSql;
        using (var db = new MigrationRuntimeContext($"Data Source={dbPath}"))
        {
            IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();
            addSql = Assert.Single(generator.Generate(
                [
                    new AddPrimaryKeyOperation
                    {
                        Name = "PK_Items",
                        Table = "Items",
                        Columns = ["TenantId", "Code"],
                    },
                ],
                model: null)).CommandText;
            dropSql = Assert.Single(generator.Generate(
                [
                    new DropPrimaryKeyOperation
                    {
                        Name = "PK_Items",
                        Table = "Items",
                    },
                ],
                model: null)).CommandText;
        }

        await ExecuteScriptAsync(dbPath, addSql);
        await CSharpDbConnection.ClearAllPoolsAsync();

        await Assert.ThrowsAsync<CSharpDbDataException>(
            () => ExecuteScriptAsync(
                dbPath,
                "ALTER TABLE Items DROP CONSTRAINT PK_Wrong;"));
        await Assert.ThrowsAsync<CSharpDbDataException>(
            () => ExecuteScriptAsync(
                dbPath,
                "ALTER TABLE Items ADD CONSTRAINT PK_Items_Second PRIMARY KEY (Code);"));
        Assert.Equal(
            2L,
            Convert.ToInt64(await ExecuteScalarValueAsync(
                dbPath,
                """
                SELECT COUNT(*) FROM sys.key_constraints
                WHERE table_name = 'Items' AND constraint_type = 'PRIMARY KEY'
                """)));
        await Assert.ThrowsAsync<CSharpDbDataException>(
            () => ExecuteScriptAsync(
                dbPath,
                "INSERT INTO Items (TenantId, Code, Payload) VALUES (7, 'alpha', 'duplicate');"));

        await ExecuteScriptAsync(dbPath, dropSql);
        await CSharpDbConnection.ClearAllPoolsAsync();
        Assert.Equal(
            0L,
            Convert.ToInt64(await ExecuteScalarValueAsync(
                dbPath,
                """
                SELECT COUNT(*) FROM sys.key_constraints
                WHERE table_name = 'Items' AND constraint_type = 'PRIMARY KEY'
                """)));
        await ExecuteScriptAsync(
            dbPath,
            "INSERT INTO Items (TenantId, Code, Payload) VALUES (7, 'alpha', 'allowed');");
    }

    [Fact]
    public async Task MigrationsSqlGenerator_StandaloneTextPrimaryKey_PreservesNotNullAfterDrop()
    {
        string dbPath = Path.Combine(_workspace, "text-primary-key-generator.db");
        await ExecuteScriptAsync(
            dbPath,
            """
            CREATE TABLE Items (Code TEXT, Payload TEXT);
            INSERT INTO Items (Code, Payload) VALUES ('alpha', 'first');
            """);

        string addSql;
        string dropSql;
        using (var db = new MigrationRuntimeContext($"Data Source={dbPath}"))
        {
            IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();
            addSql = Assert.Single(generator.Generate(
                [
                    new AddPrimaryKeyOperation
                    {
                        Name = "PK_Items",
                        Table = "Items",
                        Columns = ["Code"],
                    },
                ],
                model: null)).CommandText;
            dropSql = Assert.Single(generator.Generate(
                [
                    new DropPrimaryKeyOperation
                    {
                        Name = "PK_Items",
                        Table = "Items",
                    },
                ],
                model: null)).CommandText;
        }

        await ExecuteScriptAsync(dbPath, addSql);
        await CSharpDbConnection.ClearAllPoolsAsync();
        await Assert.ThrowsAsync<CSharpDbDataException>(
            () => ExecuteScriptAsync(
                dbPath,
                "INSERT INTO Items (Code, Payload) VALUES ('alpha', 'duplicate');"));
        await Assert.ThrowsAsync<CSharpDbDataException>(
            () => ExecuteScriptAsync(
                dbPath,
                "INSERT INTO Items (Code, Payload) VALUES (NULL, 'null');"));

        await ExecuteScriptAsync(dbPath, dropSql);
        await CSharpDbConnection.ClearAllPoolsAsync();
        await ExecuteScriptAsync(
            dbPath,
            "INSERT INTO Items (Code, Payload) VALUES ('alpha', 'allowed');");
        await Assert.ThrowsAsync<CSharpDbDataException>(
            () => ExecuteScriptAsync(
                dbPath,
                "INSERT INTO Items (Code, Payload) VALUES (NULL, 'still-not-null');"));
    }

    [Fact]
    public async Task MigrationsSqlGenerator_StandaloneIntegerPrimaryKey_RekeysUnalignedRowsAndPersists()
    {
        string alignedPath = Path.Combine(_workspace, "integer-primary-key-aligned.db");
        string emptyPath = Path.Combine(_workspace, "integer-primary-key-empty.db");
        string unalignedPath = Path.Combine(_workspace, "integer-primary-key-unaligned.db");
        await ExecuteScriptAsync(
            alignedPath,
            """
            CREATE TABLE Items (Id INTEGER);
            INSERT INTO Items (Id) VALUES (1), (2);
            """);
        await ExecuteScriptAsync(
            emptyPath,
            "CREATE TABLE Items (Id INTEGER);");
        await ExecuteScriptAsync(
            unalignedPath,
            """
            CREATE TABLE Items (
                Id INTEGER,
                Code TEXT NOT NULL,
                Payload TEXT,
                CONSTRAINT UQ_Items_Code UNIQUE (Code)
            );
            CREATE INDEX IX_Items_Payload ON Items (Payload);
            INSERT INTO Items (Id, Code, Payload)
            VALUES (10, 'ten', 'first'), (20, 'twenty', 'second');
            """);

        string addSql;
        string dropSql;
        using (var db = new MigrationRuntimeContext($"Data Source={alignedPath}"))
        {
            IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();
            addSql = Assert.Single(generator.Generate(
                [
                    new AddPrimaryKeyOperation
                    {
                        Name = "PK_Items",
                        Table = "Items",
                        Columns = ["Id"],
                    },
                ],
                model: null)).CommandText;
            dropSql = Assert.Single(generator.Generate(
                [
                    new DropPrimaryKeyOperation
                    {
                        Name = "PK_Items",
                        Table = "Items",
                    },
                ],
                model: null)).CommandText;
        }

        await ExecuteScriptAsync(alignedPath, addSql);
        await CSharpDbConnection.ClearAllPoolsAsync();
        await ExecuteScriptAsync(alignedPath, "INSERT INTO Items DEFAULT VALUES;");
        Assert.Equal(
            3L,
            Convert.ToInt64(await ExecuteScalarValueAsync(
                alignedPath,
                "SELECT Id FROM Items WHERE Id = 3")));

        await ExecuteScriptAsync(alignedPath, dropSql);
        await CSharpDbConnection.ClearAllPoolsAsync();
        await Assert.ThrowsAsync<CSharpDbDataException>(
            () => ExecuteScriptAsync(alignedPath, "INSERT INTO Items DEFAULT VALUES;"));
        await ExecuteScriptAsync(alignedPath, "INSERT INTO Items (Id) VALUES (1);");

        await ExecuteScriptAsync(emptyPath, addSql);
        await ExecuteScriptAsync(emptyPath, "INSERT INTO Items DEFAULT VALUES;");
        Assert.Equal(
            1L,
            Convert.ToInt64(await ExecuteScalarValueAsync(
                emptyPath,
                "SELECT Id FROM Items WHERE Id = 1")));

        await ExecuteScriptAsync(unalignedPath, addSql);
        await CSharpDbConnection.ClearAllPoolsAsync();
        Assert.Equal(
            1L,
            Convert.ToInt64(await ExecuteScalarValueAsync(
                unalignedPath,
                """
                SELECT COUNT(*) FROM sys.key_constraints
                WHERE table_name = 'Items' AND constraint_type = 'PRIMARY KEY'
                """)));
        await ExecuteScriptAsync(
            unalignedPath,
            "INSERT INTO Items (Code, Payload) VALUES ('generated', 'third');");
        Assert.Equal(
            21L,
            Convert.ToInt64(await ExecuteScalarValueAsync(
                unalignedPath,
                "SELECT Id FROM Items WHERE Code = 'generated'")));
        Assert.Equal(
            "second",
            Convert.ToString(await ExecuteScalarValueAsync(
                unalignedPath,
                "SELECT Payload FROM Items WHERE Id = 20")));
        await Assert.ThrowsAsync<CSharpDbDataException>(
            () => ExecuteScriptAsync(
                unalignedPath,
                "INSERT INTO Items (Id, Code) VALUES (30, 'ten');"));

        await ExecuteScriptAsync(unalignedPath, dropSql);
        await CSharpDbConnection.ClearAllPoolsAsync();
        await ExecuteScriptAsync(
            unalignedPath,
            "INSERT INTO Items (Id, Code) VALUES (10, 'after-drop');");
        Assert.Equal(
            2L,
            Convert.ToInt64(await ExecuteScalarValueAsync(
                unalignedPath,
                "SELECT COUNT(*) FROM Items WHERE Id = 10")));
    }

    [Fact]
    public async Task MigrationsSqlGenerator_StandaloneIntegerPrimaryKey_RekeyRollsBackMigrationTransaction()
    {
        string dbPath = Path.Combine(_workspace, "integer-primary-key-rekey-rollback.db");
        await ExecuteScriptAsync(
            dbPath,
            """
            CREATE TABLE Items (
                Id INTEGER,
                Code TEXT,
                Payload TEXT,
                CONSTRAINT UQ_Items_Code UNIQUE (Code)
            );
            CREATE INDEX IX_Items_Payload ON Items (Payload);
            INSERT INTO Items (Id, Code, Payload)
            VALUES (10, 'ten', 'first'), (20, NULL, 'second');
            """);

        string addSql;
        using (var db = new MigrationRuntimeContext($"Data Source={dbPath}"))
        {
            IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();
            addSql = Assert.Single(generator.Generate(
                [
                    new AddPrimaryKeyOperation
                    {
                        Name = "PK_Items",
                        Table = "Items",
                        Columns = ["Id"],
                    },
                ],
                model: null)).CommandText;
        }

        await using (var connection = new CSharpDbConnection($"Data Source={dbPath}"))
        {
            await connection.OpenAsync(Ct);
            await using var transaction = await connection.BeginTransactionAsync(Ct);

            foreach (string statement in SqlScriptSplitter.SplitExecutableStatements(addSql))
            {
                await using var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = statement;
                await command.ExecuteNonQueryAsync(Ct);
            }

            await using (var failingCommand = connection.CreateCommand())
            {
                failingCommand.Transaction = transaction;
                failingCommand.CommandText =
                    "ALTER TABLE Items ALTER COLUMN Code SET NOT NULL";
                await Assert.ThrowsAsync<CSharpDbDataException>(
                    () => failingCommand.ExecuteNonQueryAsync(Ct));
            }

            await transaction.RollbackAsync(Ct);
        }

        await CSharpDbConnection.ClearAllPoolsAsync();
        Assert.Equal(
            0L,
            Convert.ToInt64(await ExecuteScalarValueAsync(
                dbPath,
                """
                SELECT COUNT(*) FROM sys.key_constraints
                WHERE table_name = 'Items' AND constraint_type = 'PRIMARY KEY'
                """)));
        await ExecuteScriptAsync(
            dbPath,
            "INSERT INTO Items (Id, Code, Payload) VALUES (10, 'after', 'third');");
        Assert.Equal(
            2L,
            Convert.ToInt64(await ExecuteScalarValueAsync(
                dbPath,
                "SELECT COUNT(*) FROM Items WHERE Id = 10")));
        Assert.Equal(
            "second",
            Convert.ToString(await ExecuteScalarValueAsync(
                dbPath,
                "SELECT Payload FROM Items WHERE Payload = 'second'")));
        await Assert.ThrowsAsync<CSharpDbDataException>(
            () => ExecuteScriptAsync(
                dbPath,
                "INSERT INTO Items (Id, Code) VALUES (30, 'ten');"));
    }

    [Fact]
    public async Task AlterTable_DropPrimaryKey_RawSyntaxHandlesUnnamedPrimaryKey()
    {
        string dbPath = Path.Combine(_workspace, "drop-unnamed-primary-key.db");
        await ExecuteScriptAsync(
            dbPath,
            """
            CREATE TABLE Items (Id INTEGER PRIMARY KEY);
            INSERT INTO Items (Id) VALUES (1);
            """);

        Assert.Equal(
            1L,
            Convert.ToInt64(await ExecuteScalarValueAsync(
                dbPath,
                """
                SELECT COUNT(*) FROM sys.key_constraints
                WHERE table_name = 'Items' AND constraint_type = 'PRIMARY KEY'
                """)));

        await ExecuteScriptAsync(dbPath, "ALTER TABLE Items DROP PRIMARY KEY;");
        await CSharpDbConnection.ClearAllPoolsAsync();
        await ExecuteScriptAsync(dbPath, "INSERT INTO Items (Id) VALUES (1);");
        Assert.Equal(
            0L,
            Convert.ToInt64(await ExecuteScalarValueAsync(
                dbPath,
                """
                SELECT COUNT(*) FROM sys.key_constraints
                WHERE table_name = 'Items' AND constraint_type = 'PRIMARY KEY'
                """)));
    }

    [Fact]
    public async Task MigrationsSqlGenerator_AddPrimaryKey_RejectsInvalidRowsWithoutResidualMetadata()
    {
        string dbPath = Path.Combine(_workspace, "primary-key-invalid-existing-data.db");
        await ExecuteScriptAsync(
            dbPath,
            """
            CREATE TABLE DuplicateItems (Id INTEGER NOT NULL, Code TEXT);
            INSERT INTO DuplicateItems (Id, Code) VALUES (1, 'same'), (2, 'same');
            CREATE TABLE NullItems (Id INTEGER NOT NULL, Code TEXT);
            INSERT INTO NullItems (Id, Code) VALUES (1, NULL);
            """);

        string duplicateAddSql;
        string nullAddSql;
        using (var db = new MigrationRuntimeContext($"Data Source={dbPath}"))
        {
            IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();
            duplicateAddSql = Assert.Single(generator.Generate(
                [
                    new AddPrimaryKeyOperation
                    {
                        Name = "PK_DuplicateItems",
                        Table = "DuplicateItems",
                        Columns = ["Code"],
                    },
                ],
                model: null)).CommandText;
            nullAddSql = Assert.Single(generator.Generate(
                [
                    new AddPrimaryKeyOperation
                    {
                        Name = "PK_NullItems",
                        Table = "NullItems",
                        Columns = ["Code"],
                    },
                ],
                model: null)).CommandText;
        }

        await Assert.ThrowsAsync<CSharpDbDataException>(
            () => ExecuteScriptAsync(dbPath, duplicateAddSql));
        await Assert.ThrowsAsync<CSharpDbDataException>(
            () => ExecuteScriptAsync(dbPath, nullAddSql));
        Assert.Equal(
            0L,
            Convert.ToInt64(await ExecuteScalarValueAsync(
                dbPath,
                """
                SELECT COUNT(*) FROM sys.key_constraints
                WHERE constraint_type = 'PRIMARY KEY'
                """)));

        await ExecuteScriptAsync(dbPath, "DELETE FROM DuplicateItems WHERE Id = 2;");
        await ExecuteScriptAsync(dbPath, "UPDATE NullItems SET Code = 'fixed' WHERE Id = 1;");
        await ExecuteScriptAsync(dbPath, duplicateAddSql);
        await ExecuteScriptAsync(dbPath, nullAddSql);
    }

    [Fact]
    public async Task MigrationsSqlGenerator_DropPrimaryKey_RequiresAlternateCandidateForForeignKeys()
    {
        string dbPath = Path.Combine(_workspace, "drop-primary-key-dependency.db");
        await ExecuteScriptAsync(
            dbPath,
            """
            CREATE TABLE Parents (
                Code TEXT NOT NULL,
                CONSTRAINT PK_Parents PRIMARY KEY (Code)
            );
            CREATE TABLE Children (
                Id INTEGER PRIMARY KEY,
                ParentCode TEXT NOT NULL,
                CONSTRAINT FK_Children_Parents
                    FOREIGN KEY (ParentCode) REFERENCES Parents (Code)
            );
            INSERT INTO Parents (Code) VALUES ('alpha');
            INSERT INTO Children (Id, ParentCode) VALUES (1, 'alpha');
            """);

        string dropSql;
        using (var db = new MigrationRuntimeContext($"Data Source={dbPath}"))
        {
            IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();
            dropSql = Assert.Single(generator.Generate(
                [
                    new DropPrimaryKeyOperation
                    {
                        Name = "PK_Parents",
                        Table = "Parents",
                    },
                ],
                model: null)).CommandText;
        }

        var dependencyError = await Assert.ThrowsAsync<CSharpDbDataException>(
            () => ExecuteScriptAsync(dbPath, dropSql));
        Assert.Contains(
            "no equivalent UNIQUE candidate key remains",
            dependencyError.Message,
            StringComparison.OrdinalIgnoreCase);

        await ExecuteScriptAsync(
            dbPath,
            "ALTER TABLE Parents ADD CONSTRAINT AK_Parents_Code UNIQUE (Code);");
        await ExecuteScriptAsync(dbPath, dropSql);
        await CSharpDbConnection.ClearAllPoolsAsync();

        await Assert.ThrowsAsync<CSharpDbDataException>(
            () => ExecuteScriptAsync(
                dbPath,
                "INSERT INTO Children (Id, ParentCode) VALUES (2, 'missing');"));
        await Assert.ThrowsAsync<CSharpDbDataException>(
            () => ExecuteScriptAsync(
                dbPath,
                "INSERT INTO Parents (Code) VALUES ('alpha');"));
        Assert.Equal(
            0L,
            Convert.ToInt64(await ExecuteScalarValueAsync(
                dbPath,
                """
                SELECT COUNT(*) FROM sys.key_constraints
                WHERE table_name = 'Parents' AND constraint_type = 'PRIMARY KEY'
                """)));
    }

    [Fact]
    public void MigrationsSqlGenerator_EmitsLiteralDefaultAndCreateTableCheck()
    {
        string dbPath = Path.Combine(_workspace, "default-check-generator.db");

        using var db = new MigrationRuntimeContext($"Data Source={dbPath}");
        IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();

        var createTable = new CreateTableOperation { Name = "order details" };
        createTable.Columns.Add(new AddColumnOperation
        {
            Name = "Id",
            Table = createTable.Name,
            ClrType = typeof(int),
            ColumnType = "INTEGER",
            IsNullable = false,
        });
        createTable.Columns.Add(new AddColumnOperation
        {
            Name = "Status",
            Table = createTable.Name,
            ClrType = typeof(string),
            ColumnType = "TEXT",
            IsNullable = true,
            DefaultValue = "new",
        });
        createTable.PrimaryKey = new AddPrimaryKeyOperation
        {
            Name = "PK_order_details",
            Table = createTable.Name,
            Columns = ["Id"],
        };
        createTable.CheckConstraints.Add(new AddCheckConstraintOperation
        {
            Name = "CK_order_details_status",
            Table = createTable.Name,
            Sql = "\"Status\" <> ''",
        });

        string sql = Assert.Single(generator.Generate([createTable], model: null)).CommandText;

        Assert.Contains("\"Status\" TEXT DEFAULT 'new'", sql, StringComparison.Ordinal);
        Assert.Contains(
            "CONSTRAINT \"CK_order_details_status\" CHECK (\"Status\" <> '')",
            sql,
            StringComparison.Ordinal);
    }

    [Fact]
    public void MigrationsSqlGenerator_EmitsCompositePrimaryKeyAndIndexes()
    {
        string dbPath = Path.Combine(_workspace, "composite-key-generator.db");

        using var db = new MigrationRuntimeContext($"Data Source={dbPath}");
        IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();

        var createTable = new CreateTableOperation { Name = "order lines" };
        createTable.Columns.Add(new AddColumnOperation
        {
            Name = "Order Id",
            Table = createTable.Name,
            ClrType = typeof(int),
            ColumnType = "INTEGER",
            IsNullable = false,
        });
        createTable.Columns.Add(new AddColumnOperation
        {
            Name = "Line No",
            Table = createTable.Name,
            ClrType = typeof(int),
            ColumnType = "INTEGER",
            IsNullable = false,
        });
        createTable.Columns.Add(new AddColumnOperation
        {
            Name = "Sku",
            Table = createTable.Name,
            ClrType = typeof(string),
            ColumnType = "TEXT",
            IsNullable = false,
        });
        createTable.PrimaryKey = new AddPrimaryKeyOperation
        {
            Name = "PK_order_lines",
            Table = createTable.Name,
            Columns = ["Order Id", "Line No"],
        };

        var uniqueIndex = new CreateIndexOperation
        {
            Name = "IX_order_lines_order_sku",
            Table = createTable.Name,
            Columns = ["Order Id", "Sku"],
            IsUnique = true,
        };
        var nonUniqueIndex = new CreateIndexOperation
        {
            Name = "IX_order_lines_sku_line",
            Table = createTable.Name,
            Columns = ["Sku", "Line No"],
        };

        IReadOnlyList<MigrationCommand> commands = generator.Generate(
            [createTable, uniqueIndex, nonUniqueIndex],
            model: null);
        string sql = string.Concat(commands.Select(command => command.CommandText));

        Assert.Contains(
            "CONSTRAINT \"PK_order_lines\" PRIMARY KEY (\"Order Id\", \"Line No\")",
            sql,
            StringComparison.Ordinal);
        Assert.DoesNotContain("\"Order Id\" INTEGER PRIMARY KEY", sql, StringComparison.Ordinal);
        Assert.Contains(
            "CREATE UNIQUE INDEX \"IX_order_lines_order_sku\" ON \"order lines\" (\"Order Id\", \"Sku\")",
            sql,
            StringComparison.Ordinal);
        Assert.Contains(
            "CREATE INDEX \"IX_order_lines_sku_line\" ON \"order lines\" (\"Sku\", \"Line No\")",
            sql,
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task MigrationsSqlGenerator_RenameIndex_ExecutesAndPersists()
    {
        string dbPath = Path.Combine(_workspace, "rename-index-generator.db");
        await ExecuteScriptAsync(
            dbPath,
            """
            CREATE TABLE Items (
                Id INTEGER PRIMARY KEY,
                Code TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IX_Items_Code ON Items (Code);
            CREATE INDEX IX_Items_Id ON Items (Id);
            CREATE TABLE ItemRefs (
                Id INTEGER PRIMARY KEY,
                ItemCode TEXT NOT NULL,
                CONSTRAINT FK_ItemRefs_Items_Code
                    FOREIGN KEY (ItemCode) REFERENCES Items (Code)
            );
            INSERT INTO Items (Id, Code) VALUES (1, 'alpha');
            INSERT INTO ItemRefs (Id, ItemCode) VALUES (1, 'alpha');
            """);

        string sql;
        using (var db = new MigrationRuntimeContext($"Data Source={dbPath}"))
        {
            IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();
            sql = Assert.Single(generator.Generate(
                [
                    new RenameIndexOperation
                    {
                        Name = "IX_Items_Code",
                        NewName = "IX_Items_Code_Renamed",
                        Table = "Items",
                    },
                ],
                model: null)).CommandText;
        }

        Assert.Equal(
            $"ALTER TABLE \"Items\" RENAME INDEX \"IX_Items_Code\" TO \"IX_Items_Code_Renamed\";{Environment.NewLine}",
            sql);

        await Assert.ThrowsAsync<CSharpDbDataException>(
            () => ExecuteScriptAsync(
                dbPath,
                "ALTER TABLE Items RENAME INDEX IX_Items_Code TO IX_Items_Id;"));

        await ExecuteScriptAsync(dbPath, sql);
        await CSharpDbConnection.ClearAllPoolsAsync();

        await using (var connection = new CSharpDbConnection($"Data Source={dbPath}"))
        {
            await connection.OpenAsync(Ct);
            await using var command = connection.CreateCommand();
            command.CommandText =
                "SELECT COUNT(*) FROM sys.indexes " +
                "WHERE index_name = 'IX_Items_Code_Renamed'";
            Assert.Equal(1L, Convert.ToInt64(await command.ExecuteScalarAsync(Ct)));

            command.CommandText =
                "SELECT COUNT(*) FROM sys.indexes " +
                "WHERE index_name = 'IX_Items_Code'";
            Assert.Equal(0L, Convert.ToInt64(await command.ExecuteScalarAsync(Ct)));
        }

        await Assert.ThrowsAsync<CSharpDbDataException>(
            () => ExecuteScriptAsync(
                dbPath,
                "INSERT INTO Items (Id, Code) VALUES (2, 'alpha');"));
        await Assert.ThrowsAsync<CSharpDbDataException>(
            () => ExecuteScriptAsync(
                dbPath,
                "INSERT INTO ItemRefs (Id, ItemCode) VALUES (2, 'missing');"));

        await ExecuteScriptAsync(
            dbPath,
            "ALTER TABLE ItemRefs DROP CONSTRAINT FK_ItemRefs_Items_Code;");
        await ExecuteScriptAsync(dbPath, "DROP INDEX IX_Items_Code_Renamed;");
        await ExecuteScriptAsync(
            dbPath,
            "INSERT INTO Items (Id, Code) VALUES (2, 'alpha');");
    }

    [Fact]
    public async Task MigrationsSqlGenerator_AlterColumnDefaultAndNullability_ExecutesAndPersists()
    {
        string dbPath = Path.Combine(_workspace, "alter-column.db");
        await ExecuteScriptAsync(
            dbPath,
            """
            CREATE TABLE Items (
                Id INTEGER PRIMARY KEY,
                Status TEXT
            );
            INSERT INTO Items (Id, Status) VALUES (1, 'existing');
            """);

        string sql;
        using (var db = new MigrationRuntimeContext($"Data Source={dbPath}"))
        {
            IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();
            var operation = new AlterColumnOperation
            {
                Name = "Status",
                Table = "Items",
                ClrType = typeof(string),
                ColumnType = "TEXT",
                IsNullable = false,
                DefaultValue = "new",
                OldColumn = new AddColumnOperation
                {
                    Name = "Status",
                    Table = "Items",
                    ClrType = typeof(string),
                    ColumnType = "TEXT",
                    IsNullable = true,
                },
            };

            sql = string.Concat(generator.Generate([operation], model: null)
                .Select(command => command.CommandText));
        }

        Assert.Contains(
            "ALTER TABLE \"Items\" ALTER COLUMN \"Status\" SET DEFAULT 'new'",
            sql,
            StringComparison.Ordinal);
        Assert.Contains(
            "ALTER TABLE \"Items\" ALTER COLUMN \"Status\" SET NOT NULL",
            sql,
            StringComparison.Ordinal);

        await ExecuteScriptAsync(dbPath, sql);
        await ExecuteScriptAsync(dbPath, "INSERT INTO Items (Id) VALUES (2);");
        await CSharpDbConnection.ClearAllPoolsAsync();

        await using var connection = new CSharpDbConnection($"Data Source={dbPath}");
        await connection.OpenAsync(Ct);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT Status FROM Items WHERE Id = 2";
        Assert.Equal("new", await command.ExecuteScalarAsync(Ct));
    }

    [Fact]
    public async Task MigrationsSqlGenerator_AlterColumnSetNotNull_RejectsExistingNullsWithoutChangingSchema()
    {
        string dbPath = Path.Combine(_workspace, "alter-column-null-rejection.db");
        await ExecuteScriptAsync(
            dbPath,
            """
            CREATE TABLE Items (
                Id INTEGER PRIMARY KEY,
                Status TEXT
            );
            INSERT INTO Items (Id, Status) VALUES (1, NULL);
            """);

        string sql;
        using (var db = new MigrationRuntimeContext($"Data Source={dbPath}"))
        {
            IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();
            sql = Assert.Single(generator.Generate(
                [
                    new AlterColumnOperation
                    {
                        Name = "Status",
                        Table = "Items",
                        ClrType = typeof(string),
                        ColumnType = "TEXT",
                        IsNullable = false,
                        OldColumn = new AddColumnOperation
                        {
                            Name = "Status",
                            Table = "Items",
                            ClrType = typeof(string),
                            ColumnType = "TEXT",
                            IsNullable = true,
                        },
                    },
                ],
                model: null)).CommandText;
        }

        await Assert.ThrowsAsync<CSharpDbDataException>(() => ExecuteScriptAsync(dbPath, sql));
        await ExecuteScriptAsync(dbPath, "INSERT INTO Items (Id, Status) VALUES (2, NULL);");
    }

    [Fact]
    public void MigrationsSqlGenerator_NonNumericAlterColumnTypeChange_RemainsExplicitlyRejected()
    {
        string dbPath = Path.Combine(_workspace, "alter-column-type-rejection.db");
        using var db = new MigrationRuntimeContext($"Data Source={dbPath}");
        IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();

        var operation = new AlterColumnOperation
        {
            Name = "Value",
            Table = "Items",
            ClrType = typeof(long),
            ColumnType = "INTEGER",
            IsNullable = false,
            OldColumn = new AddColumnOperation
            {
                Name = "Value",
                Table = "Items",
                ClrType = typeof(string),
                ColumnType = "TEXT",
                IsNullable = false,
            },
        };

        var error = Assert.Throws<NotSupportedException>(
            () => generator.Generate([operation], model: null));
        Assert.Contains("from store type 'TEXT' to 'INTEGER'", error.Message, StringComparison.Ordinal);
        Assert.Contains("INTEGER-to-REAL", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void MigrationsSqlGenerator_AlterColumn_InfersOldStoreTypeWithoutUsingTargetModelColumn()
    {
        string dbPath = Path.Combine(_workspace, "alter-column-old-type-inference.db");
        using var db = new MigrationRuntimeContext($"Data Source={dbPath}");
        IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();

        var operation = new AlterColumnOperation
        {
            Name = "Name",
            Table = "Blogs",
            ClrType = typeof(string),
            IsNullable = false,
            OldColumn = new AddColumnOperation
            {
                Name = "Name",
                Table = "Blogs",
                ClrType = typeof(double),
                IsNullable = false,
            },
        };

        var error = Assert.Throws<NotSupportedException>(
            () => generator.Generate([operation], db.Model));
        Assert.Contains("from store type 'REAL' to 'TEXT'", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task MigrationsSqlGenerator_AlterColumnNumericType_ExecutesUpAndDownAndPersists()
    {
        string dbPath = Path.Combine(_workspace, "alter-column-numeric-type.db");
        await ExecuteScriptAsync(
            dbPath,
            """
            CREATE TABLE Items (
                Id INTEGER PRIMARY KEY,
                Value INTEGER DEFAULT 1
            );
            INSERT INTO Items (Id, Value) VALUES (1, 42);
            INSERT INTO Items (Id) VALUES (2);
            """);

        string upSql;
        string downSql;
        using (var db = new MigrationRuntimeContext($"Data Source={dbPath}"))
        {
            IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();
            var up = new AlterColumnOperation
            {
                Name = "Value",
                Table = "Items",
                ClrType = typeof(double),
                ColumnType = "REAL",
                IsNullable = false,
                DefaultValue = 2.0,
                OldColumn = new AddColumnOperation
                {
                    Name = "Value",
                    Table = "Items",
                    ClrType = typeof(long),
                    ColumnType = "INTEGER",
                    IsNullable = true,
                    DefaultValue = 1L,
                },
            };
            upSql = string.Concat(generator.Generate([up], model: null)
                .Select(command => command.CommandText));

            var down = new AlterColumnOperation
            {
                Name = "Value",
                Table = "Items",
                ClrType = typeof(long),
                ColumnType = "INTEGER",
                IsNullable = true,
                DefaultValue = 1L,
                OldColumn = new AddColumnOperation
                {
                    Name = "Value",
                    Table = "Items",
                    ClrType = typeof(double),
                    ColumnType = "REAL",
                    IsNullable = false,
                    DefaultValue = 2.0,
                },
            };
            downSql = string.Concat(generator.Generate([down], model: null)
                .Select(command => command.CommandText));
        }

        AssertCommandsInOrder(
            upSql,
            "ALTER TABLE \"Items\" ALTER COLUMN \"Value\" DROP DEFAULT",
            "ALTER TABLE \"Items\" ALTER COLUMN \"Value\" TYPE REAL",
            "ALTER TABLE \"Items\" ALTER COLUMN \"Value\" SET DEFAULT",
            "ALTER TABLE \"Items\" ALTER COLUMN \"Value\" SET NOT NULL");

        await ExecuteScriptAsync(dbPath, upSql);
        Assert.Equal(
            "REAL",
            await ExecuteScalarValueAsync(
                dbPath,
                "SELECT data_type FROM sys.columns WHERE table_name = 'Items' AND column_name = 'Value'"));
        Assert.Equal(42d, Convert.ToDouble(await ExecuteScalarValueAsync(
            dbPath,
            "SELECT Value FROM Items WHERE Id = 1")));
        Assert.Equal(1d, Convert.ToDouble(await ExecuteScalarValueAsync(
            dbPath,
            "SELECT Value FROM Items WHERE Id = 2")));

        await ExecuteScriptAsync(dbPath, "INSERT INTO Items (Id) VALUES (3);");
        Assert.Equal(2d, Convert.ToDouble(await ExecuteScalarValueAsync(
            dbPath,
            "SELECT Value FROM Items WHERE Id = 3")));

        AssertCommandsInOrder(
            downSql,
            "ALTER TABLE \"Items\" ALTER COLUMN \"Value\" DROP DEFAULT",
            "ALTER TABLE \"Items\" ALTER COLUMN \"Value\" TYPE INTEGER",
            "ALTER TABLE \"Items\" ALTER COLUMN \"Value\" SET DEFAULT",
            "ALTER TABLE \"Items\" ALTER COLUMN \"Value\" DROP NOT NULL");

        await ExecuteScriptAsync(dbPath, downSql);
        await CSharpDbConnection.ClearAllPoolsAsync();

        Assert.Equal(
            "INTEGER",
            await ExecuteScalarValueAsync(
                dbPath,
                "SELECT data_type FROM sys.columns WHERE table_name = 'Items' AND column_name = 'Value'"));
        Assert.Equal(42L, Convert.ToInt64(await ExecuteScalarValueAsync(
            dbPath,
            "SELECT Value FROM Items WHERE Id = 1")));
        Assert.Equal(2L, Convert.ToInt64(await ExecuteScalarValueAsync(
            dbPath,
            "SELECT Value FROM Items WHERE Id = 3")));

        await ExecuteScriptAsync(
            dbPath,
            "INSERT INTO Items (Id) VALUES (4); " +
            "INSERT INTO Items (Id, Value) VALUES (5, NULL);");
        Assert.Equal(1L, Convert.ToInt64(await ExecuteScalarValueAsync(
            dbPath,
            "SELECT Value FROM Items WHERE Id = 4")));
        Assert.Equal(1L, Convert.ToInt64(await ExecuteScalarValueAsync(
            dbPath,
            "SELECT COUNT(*) FROM Items WHERE Id = 5 AND Value IS NULL")));
    }

    [Fact]
    public async Task MigrationsSqlGenerator_AlterColumnNumericType_FailureRollsBackCompoundTransaction()
    {
        string dbPath = Path.Combine(_workspace, "alter-column-numeric-rollback.db");
        await ExecuteScriptAsync(
            dbPath,
            """
            CREATE TABLE Items (
                Id INTEGER PRIMARY KEY,
                Value REAL DEFAULT 1.5
            );
            INSERT INTO Items (Id, Value) VALUES (1, 2.5);
            """);

        string sql;
        using (var db = new MigrationRuntimeContext($"Data Source={dbPath}"))
        {
            IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();
            sql = string.Concat(generator.Generate(
                    [
                        new AlterColumnOperation
                        {
                            Name = "Value",
                            Table = "Items",
                            ClrType = typeof(long),
                            ColumnType = "INTEGER",
                            IsNullable = true,
                            DefaultValue = 1L,
                            OldColumn = new AddColumnOperation
                            {
                                Name = "Value",
                                Table = "Items",
                                ClrType = typeof(double),
                                ColumnType = "REAL",
                                IsNullable = true,
                                DefaultValue = 1.5,
                            },
                        },
                    ],
                    model: null)
                .Select(command => command.CommandText));
        }

        await using (var connection = new CSharpDbConnection($"Data Source={dbPath}"))
        {
            await connection.OpenAsync(Ct);
            await using var transaction = await connection.BeginTransactionAsync(Ct);

            await Assert.ThrowsAsync<CSharpDbDataException>(async () =>
            {
                foreach (string statement in SqlScriptSplitter.SplitExecutableStatements(sql))
                {
                    await using var command = connection.CreateCommand();
                    command.Transaction = transaction;
                    command.CommandText = statement;
                    await command.ExecuteNonQueryAsync(Ct);
                }
            });

            await transaction.RollbackAsync(Ct);
        }

        await CSharpDbConnection.ClearAllPoolsAsync();
        Assert.Equal(
            "REAL",
            await ExecuteScalarValueAsync(
                dbPath,
                "SELECT data_type FROM sys.columns WHERE table_name = 'Items' AND column_name = 'Value'"));
        Assert.Equal(2.5d, Convert.ToDouble(await ExecuteScalarValueAsync(
            dbPath,
            "SELECT Value FROM Items WHERE Id = 1")));

        await ExecuteScriptAsync(dbPath, "INSERT INTO Items (Id) VALUES (2);");
        Assert.Equal(1.5d, Convert.ToDouble(await ExecuteScalarValueAsync(
            dbPath,
            "SELECT Value FROM Items WHERE Id = 2")));
    }

    [Fact]
    public async Task MigrationsSqlGenerator_AlterColumnCollation_ExecutesUpAndDownAndPersists()
    {
        string dbPath = Path.Combine(_workspace, "alter-column-collation.db");
        await ExecuteScriptAsync(
            dbPath,
            """
            CREATE TABLE Items (
                Id INTEGER PRIMARY KEY,
                Code TEXT NOT NULL
            );
            CREATE UNIQUE INDEX UX_Items_Code ON Items (Code);
            INSERT INTO Items (Id, Code) VALUES (1, 'Alpha');
            """);

        string createSql;
        string upSql;
        string downSql;
        using (var db = new MigrationRuntimeContext($"Data Source={dbPath}"))
        {
            IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();

            var create = new CreateTableOperation { Name = "CollatedItems" };
            create.Columns.Add(new AddColumnOperation
            {
                Name = "Code",
                Table = "CollatedItems",
                ClrType = typeof(string),
                ColumnType = "TEXT",
                Collation = "NOCASE",
                IsNullable = false,
            });
            createSql = Assert.Single(generator.Generate([create], model: null)).CommandText;

            upSql = Assert.Single(generator.Generate(
                [
                    new AlterColumnOperation
                    {
                        Name = "Code",
                        Table = "Items",
                        ClrType = typeof(string),
                        ColumnType = "TEXT",
                        Collation = "NOCASE",
                        IsNullable = false,
                        OldColumn = new AddColumnOperation
                        {
                            Name = "Code",
                            Table = "Items",
                            ClrType = typeof(string),
                            ColumnType = "TEXT",
                            IsNullable = false,
                        },
                    },
                ],
                model: null)).CommandText;

            downSql = Assert.Single(generator.Generate(
                [
                    new AlterColumnOperation
                    {
                        Name = "Code",
                        Table = "Items",
                        ClrType = typeof(string),
                        ColumnType = "TEXT",
                        IsNullable = false,
                        OldColumn = new AddColumnOperation
                        {
                            Name = "Code",
                            Table = "Items",
                            ClrType = typeof(string),
                            ColumnType = "TEXT",
                            Collation = "NOCASE",
                            IsNullable = false,
                        },
                    },
                ],
                model: null)).CommandText;

            Assert.Empty(generator.Generate(
                [
                    new AlterColumnOperation
                    {
                        Name = "Code",
                        Table = "Items",
                        ClrType = typeof(string),
                        ColumnType = "TEXT",
                        Collation = " binary ",
                        IsNullable = false,
                        OldColumn = new AddColumnOperation
                        {
                            Name = "Code",
                            Table = "Items",
                            ClrType = typeof(string),
                            ColumnType = "TEXT",
                            IsNullable = false,
                        },
                    },
                ],
                model: null));

            var invalidCollation = Assert.Throws<NotSupportedException>(
                () => generator.Generate(
                    [
                        new AddColumnOperation
                        {
                            Name = "Quantity",
                            Table = "Items",
                            ClrType = typeof(long),
                            ColumnType = "INTEGER",
                            Collation = "NOCASE",
                            IsNullable = true,
                        },
                    ],
                    model: null));
            Assert.Contains("non-TEXT", invalidCollation.Message, StringComparison.Ordinal);
        }

        Assert.Contains("\"Code\" TEXT COLLATE \"NOCASE\" NOT NULL", createSql, StringComparison.Ordinal);
        Assert.Contains("SET COLLATION \"NOCASE\"", upSql, StringComparison.Ordinal);
        Assert.Contains("DROP COLLATION", downSql, StringComparison.Ordinal);

        await ExecuteScriptAsync(dbPath, createSql);
        await ExecuteScriptAsync(
            dbPath,
            "INSERT INTO CollatedItems (Code) VALUES ('Alpha');");
        Assert.Equal(
            "NOCASE",
            await ExecuteScalarValueAsync(
                dbPath,
                "SELECT collation FROM sys.columns " +
                "WHERE table_name = 'CollatedItems' AND column_name = 'Code'"));
        Assert.Equal(
            1L,
            Convert.ToInt64(await ExecuteScalarValueAsync(
                dbPath,
                "SELECT COUNT(*) FROM CollatedItems WHERE Code = 'alpha'")));

        Assert.Equal(
            0L,
            Convert.ToInt64(await ExecuteScalarValueAsync(
                dbPath,
                "SELECT COUNT(*) FROM Items WHERE Code = 'alpha'")));

        await ExecuteScriptAsync(dbPath, upSql);
        await CSharpDbConnection.ClearAllPoolsAsync();
        Assert.Equal(
            "NOCASE",
            await ExecuteScalarValueAsync(
                dbPath,
                "SELECT collation FROM sys.columns WHERE table_name = 'Items' AND column_name = 'Code'"));
        Assert.Equal(
            1L,
            Convert.ToInt64(await ExecuteScalarValueAsync(
                dbPath,
                "SELECT COUNT(*) FROM Items WHERE Code = 'alpha'")));
        CSharpDbDataException duplicate = await Assert.ThrowsAsync<CSharpDbDataException>(
            () => ExecuteScriptAsync(
                dbPath,
                "INSERT INTO Items (Id, Code) VALUES (2, 'ALPHA');"));
        Assert.Contains("unique", duplicate.Message, StringComparison.OrdinalIgnoreCase);

        await ExecuteScriptAsync(dbPath, downSql);
        await CSharpDbConnection.ClearAllPoolsAsync();
        object? droppedCollation = await ExecuteScalarValueAsync(
            dbPath,
            "SELECT collation FROM sys.columns WHERE table_name = 'Items' AND column_name = 'Code'");
        Assert.True(
            droppedCollation is null or DBNull,
            $"Expected NULL collation metadata, got '{droppedCollation}'.");
        Assert.Equal(
            0L,
            Convert.ToInt64(await ExecuteScalarValueAsync(
                dbPath,
                "SELECT COUNT(*) FROM Items WHERE Code = 'alpha'")));
        await ExecuteScriptAsync(
            dbPath,
            "INSERT INTO Items (Id, Code) VALUES (2, 'ALPHA');");
        Assert.Equal(
            2L,
            Convert.ToInt64(await ExecuteScalarValueAsync(
                dbPath,
                "SELECT COUNT(*) FROM Items")));
    }

    [Fact]
    public async Task MigrationsSqlGenerator_AlterColumnCollation_RebuiltIndexRollsBackWithCompoundMigration()
    {
        string dbPath = Path.Combine(_workspace, "alter-column-collation-rollback.db");
        await ExecuteScriptAsync(
            dbPath,
            """
            CREATE TABLE Items (
                Id INTEGER PRIMARY KEY,
                Code TEXT
            );
            CREATE UNIQUE INDEX UX_Items_Code ON Items (Code);
            INSERT INTO Items (Id, Code) VALUES (1, 'Alpha'), (2, NULL);
            """);

        string[] sqlCommands;
        using (var db = new MigrationRuntimeContext($"Data Source={dbPath}"))
        {
            IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();
            sqlCommands = generator.Generate(
                [
                    new AlterColumnOperation
                    {
                        Name = "Code",
                        Table = "Items",
                        ClrType = typeof(string),
                        ColumnType = "TEXT",
                        Collation = "NOCASE",
                        IsNullable = false,
                        OldColumn = new AddColumnOperation
                        {
                            Name = "Code",
                            Table = "Items",
                            ClrType = typeof(string),
                            ColumnType = "TEXT",
                            IsNullable = true,
                        },
                    },
                ],
                model: null)
                .Select(command => command.CommandText)
                .ToArray();
        }

        Assert.Equal(2, sqlCommands.Length);
        string sql = string.Concat(sqlCommands);
        Assert.True(
            sql.IndexOf("SET COLLATION", StringComparison.Ordinal) <
            sql.IndexOf("SET NOT NULL", StringComparison.Ordinal));

        await using (var connection = new CSharpDbConnection($"Data Source={dbPath}"))
        {
            await connection.OpenAsync(Ct);
            await using var transaction = await connection.BeginTransactionAsync(Ct);

            await Assert.ThrowsAsync<CSharpDbDataException>(async () =>
            {
                foreach (string commandText in sqlCommands)
                {
                    foreach (string statement in SqlScriptSplitter.SplitExecutableStatements(commandText))
                    {
                        await using var command = connection.CreateCommand();
                        command.Transaction = transaction;
                        command.CommandText = statement;
                        await command.ExecuteNonQueryAsync(Ct);
                    }
                }
            });

            await transaction.RollbackAsync(Ct);
        }

        await CSharpDbConnection.ClearAllPoolsAsync();
        object? collation = await ExecuteScalarValueAsync(
            dbPath,
            "SELECT collation FROM sys.columns " +
            "WHERE table_name = 'Items' AND column_name = 'Code'");
        Assert.True(collation is null or DBNull);
        Assert.Equal(
            1L,
            Convert.ToInt64(await ExecuteScalarValueAsync(
                dbPath,
                "SELECT COUNT(*) FROM Items WHERE Code IS NULL")));

        await ExecuteScriptAsync(
            dbPath,
            "INSERT INTO Items (Id, Code) VALUES (3, 'ALPHA');");
        Assert.Equal(
            3L,
            Convert.ToInt64(await ExecuteScalarValueAsync(
                dbPath,
                "SELECT COUNT(*) FROM Items")));
    }

    [Fact]
    public async Task MigrationsSqlGenerator_AlterColumnNumericType_RejectsDependentIndexWithoutChangingSchema()
    {
        string dbPath = Path.Combine(_workspace, "alter-column-index-dependency.db");
        await ExecuteScriptAsync(
            dbPath,
            """
            CREATE TABLE NumericItems (
                Id INTEGER PRIMARY KEY,
                Value INTEGER
            );
            CREATE INDEX IX_NumericItems_Value ON NumericItems (Value);
            """);

        string numericSql;
        using (var db = new MigrationRuntimeContext($"Data Source={dbPath}"))
        {
            IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();
            numericSql = Assert.Single(generator.Generate(
                [
                    new AlterColumnOperation
                    {
                        Name = "Value",
                        Table = "NumericItems",
                        ClrType = typeof(double),
                        ColumnType = "REAL",
                        IsNullable = true,
                        OldColumn = new AddColumnOperation
                        {
                            Name = "Value",
                            Table = "NumericItems",
                            ClrType = typeof(long),
                            ColumnType = "INTEGER",
                            IsNullable = true,
                        },
                    },
                ],
                model: null)).CommandText;
        }

        CSharpDbDataException numericFailure = await Assert.ThrowsAsync<CSharpDbDataException>(
            () => ExecuteScriptAsync(dbPath, numericSql));
        Assert.Contains("IX_NumericItems_Value", numericFailure.Message, StringComparison.OrdinalIgnoreCase);

        Assert.Equal(
            "INTEGER",
            await ExecuteScalarValueAsync(
                dbPath,
                "SELECT data_type FROM sys.columns " +
                "WHERE table_name = 'NumericItems' AND column_name = 'Value'"));
    }

    [Fact]
    public async Task MigrationsSqlGenerator_StandaloneCheckConstraint_CanBeAddedAndDropped()
    {
        string dbPath = Path.Combine(_workspace, "alter-check.db");
        await ExecuteScriptAsync(
            dbPath,
            """
            CREATE TABLE Items (
                Id INTEGER PRIMARY KEY,
                Quantity INTEGER NOT NULL
            );
            INSERT INTO Items (Id, Quantity) VALUES (1, 1);
            """);

        string addSql;
        string dropSql;
        using (var db = new MigrationRuntimeContext($"Data Source={dbPath}"))
        {
            IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();
            addSql = Assert.Single(generator.Generate(
                [
                    new AddCheckConstraintOperation
                    {
                        Name = "CK_Items_Quantity",
                        Table = "Items",
                        Sql = "\"Quantity\" > 0",
                    },
                ],
                model: null)).CommandText;
            dropSql = Assert.Single(generator.Generate(
                [
                    new DropCheckConstraintOperation
                    {
                        Name = "CK_Items_Quantity",
                        Table = "Items",
                    },
                ],
                model: null)).CommandText;
        }

        await ExecuteScriptAsync(dbPath, addSql);
        await Assert.ThrowsAsync<CSharpDbDataException>(
            () => ExecuteScriptAsync(dbPath, "UPDATE Items SET Quantity = 0 WHERE Id = 1;"));

        await ExecuteScriptAsync(dbPath, dropSql);
        await ExecuteScriptAsync(dbPath, "UPDATE Items SET Quantity = 0 WHERE Id = 1;");
    }

    [Fact]
    public async Task MigrationsSqlGenerator_StandaloneUniqueConstraint_CanBeAddedAndDropped()
    {
        string dbPath = Path.Combine(_workspace, "alter-unique.db");
        await ExecuteScriptAsync(
            dbPath,
            """
            CREATE TABLE Items (
                Id INTEGER PRIMARY KEY,
                Code TEXT NOT NULL
            );
            INSERT INTO Items (Id, Code) VALUES (1, 'alpha');
            """);

        string addSql;
        string dropSql;
        using (var db = new MigrationRuntimeContext($"Data Source={dbPath}"))
        {
            IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();
            addSql = Assert.Single(generator.Generate(
                [
                    new AddUniqueConstraintOperation
                    {
                        Name = "AK_Items_Code",
                        Table = "Items",
                        Columns = ["Code"],
                    },
                ],
                model: null)).CommandText;
            dropSql = Assert.Single(generator.Generate(
                [
                    new DropUniqueConstraintOperation
                    {
                        Name = "AK_Items_Code",
                        Table = "Items",
                    },
                ],
                model: null)).CommandText;
        }

        await ExecuteScriptAsync(dbPath, addSql);
        await CSharpDbConnection.ClearAllPoolsAsync();
        await Assert.ThrowsAsync<CSharpDbDataException>(
            () => ExecuteScriptAsync(dbPath, "INSERT INTO Items (Id, Code) VALUES (2, 'alpha');"));

        await ExecuteScriptAsync(dbPath, dropSql);
        await ExecuteScriptAsync(dbPath, "INSERT INTO Items (Id, Code) VALUES (2, 'alpha');");
    }

    [Fact]
    public async Task MigrationsSqlGenerator_AddConstraints_RejectsInvalidExistingDataWithoutPersistingMetadata()
    {
        string dbPath = Path.Combine(_workspace, "alter-constraint-rejection.db");
        await ExecuteScriptAsync(
            dbPath,
            """
            CREATE TABLE Parents (Id INTEGER PRIMARY KEY);
            CREATE TABLE Children (Id INTEGER PRIMARY KEY, ParentId INTEGER);
            CREATE TABLE Codes (Id INTEGER PRIMARY KEY, Code TEXT NOT NULL);
            INSERT INTO Children (Id, ParentId) VALUES (1, 999);
            INSERT INTO Codes (Id, Code) VALUES (1, 'duplicate'), (2, 'duplicate');
            """);

        string addForeignKeySql;
        string addUniqueSql;
        using (var db = new MigrationRuntimeContext($"Data Source={dbPath}"))
        {
            IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();
            addForeignKeySql = Assert.Single(generator.Generate(
                [
                    new AddForeignKeyOperation
                    {
                        Name = "FK_Children_Parents_ParentId",
                        Table = "Children",
                        Columns = ["ParentId"],
                        PrincipalTable = "Parents",
                        PrincipalColumns = ["Id"],
                    },
                ],
                model: null)).CommandText;
            addUniqueSql = Assert.Single(generator.Generate(
                [
                    new AddUniqueConstraintOperation
                    {
                        Name = "AK_Codes_Code",
                        Table = "Codes",
                        Columns = ["Code"],
                    },
                ],
                model: null)).CommandText;
        }

        await Assert.ThrowsAsync<CSharpDbDataException>(
            () => ExecuteScriptAsync(dbPath, addForeignKeySql));
        await Assert.ThrowsAsync<CSharpDbDataException>(
            () => ExecuteScriptAsync(dbPath, addUniqueSql));

        await ExecuteScriptAsync(dbPath, "INSERT INTO Children (Id, ParentId) VALUES (2, 999);");
        await ExecuteScriptAsync(dbPath, "INSERT INTO Codes (Id, Code) VALUES (3, 'duplicate');");
    }

    private sealed class MigrationRuntimeContext(string connectionString) : DbContext
    {
        public DbSet<MigrationBlog> Blogs => Set<MigrationBlog>();

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.UseCSharpDb(connectionString);
    }

    private static async Task ExecuteScriptAsync(string dbPath, string script)
    {
        await using var connection = new CSharpDbConnection($"Data Source={dbPath}");
        await connection.OpenAsync(Ct);

        foreach (string statement in SqlScriptSplitter.SplitExecutableStatements(script))
        {
            await using var command = connection.CreateCommand();
            command.CommandText = statement;
            await command.ExecuteNonQueryAsync(Ct);
        }
    }

    private static async Task<object?> ExecuteScalarValueAsync(string dbPath, string sql)
    {
        await using var connection = new CSharpDbConnection($"Data Source={dbPath}");
        await connection.OpenAsync(Ct);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteScalarAsync(Ct);
    }

    private static void AssertCommandsInOrder(string sql, params string[] commands)
    {
        int previousIndex = -1;
        foreach (string command in commands)
        {
            int currentIndex = sql.IndexOf(command, StringComparison.Ordinal);
            Assert.True(
                currentIndex > previousIndex,
                $"Expected command '{command}' after position {previousIndex}.{Environment.NewLine}{sql}");
            previousIndex = currentIndex;
        }
    }

    private sealed class MigrationBlog
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public string? Slug { get; set; }
    }

    [DbContext(typeof(MigrationRuntimeContext))]
    [Migration("202604160001_InitialCreate")]
    private sealed class InitialCreate : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "Blogs",
                columns: table => new
                {
                    Id = table.Column<int>(type: "INTEGER", nullable: false),
                    Name = table.Column<string>(type: "TEXT", nullable: false),
                },
                constraints: table => table.PrimaryKey("PK_Blogs", row => row.Id));
        }

        protected override void Down(MigrationBuilder migrationBuilder)
            => migrationBuilder.DropTable("Blogs");
    }

    [DbContext(typeof(MigrationRuntimeContext))]
    [Migration("202604160002_AddSlugIndex")]
    private sealed class AddSlugIndex : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "Slug",
                table: "Blogs",
                type: "TEXT",
                nullable: true);

            migrationBuilder.CreateIndex(
                name: "IX_Blogs_Slug",
                table: "Blogs",
                column: "Slug");
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(
                name: "IX_Blogs_Slug",
                table: "Blogs");

            migrationBuilder.DropColumn(
                name: "Slug",
                table: "Blogs");
        }
    }
}
