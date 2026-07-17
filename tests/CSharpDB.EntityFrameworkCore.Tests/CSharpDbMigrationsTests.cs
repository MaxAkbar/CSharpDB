using CSharpDB.Data;
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
    public void Migrator_GenerateIdempotentScript_ThrowsClearly()
    {
        string dbPath = Path.Combine(_workspace, "idempotent.db");

        using var db = new MigrationRuntimeContext($"Data Source={dbPath}");
        IMigrator migrator = db.GetService<IMigrator>();

        var error = Assert.Throws<NotSupportedException>(() => migrator.GenerateScript(options: MigrationsSqlGenerationOptions.Idempotent));
        Assert.Contains("idempotent migration scripts are not supported", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void MigrationsSqlGenerator_RejectsStandaloneForeignKeyChanges()
    {
        string dbPath = Path.Combine(_workspace, "generator.db");

        using var db = new MigrationRuntimeContext($"Data Source={dbPath}");
        IMigrationsSqlGenerator generator = db.GetService<IMigrationsSqlGenerator>();

        var addForeignKey = new AddForeignKeyOperation
        {
            Name = "FK_Posts_Blogs_BlogId",
            Table = "Posts",
            Columns = ["BlogId"],
            PrincipalTable = "Blogs",
            PrincipalColumns = ["Id"],
        };

        var dropForeignKey = new DropForeignKeyOperation
        {
            Name = "FK_Posts_Blogs_BlogId",
            Table = "Posts",
        };

        var addError = Assert.Throws<NotSupportedException>(() => generator.Generate([addForeignKey], model: null));
        var dropError = Assert.Throws<NotSupportedException>(() => generator.Generate([dropForeignKey], model: null));

        Assert.Contains("standalone foreign key changes", addError.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("standalone foreign key changes", dropError.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void MigrationsSqlGenerator_KeepsCompositeForeignKeysAndStandaloneKeyChangesRejected()
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

        var compositeForeignKeyError = Assert.Throws<NotSupportedException>(
            () => generator.Generate([createTable], model: null));
        Assert.Contains("composite foreign keys", compositeForeignKeyError.Message, StringComparison.OrdinalIgnoreCase);

        var addPrimaryKey = new AddPrimaryKeyOperation
        {
            Name = "PK_child",
            Table = "child",
            Columns = ["TenantId", "OrderId"],
        };
        var addUnique = new AddUniqueConstraintOperation
        {
            Name = "AK_child_tenant_order",
            Table = "child",
            Columns = ["TenantId", "OrderId"],
        };

        Assert.Contains(
            "standalone primary key changes",
            Assert.Throws<NotSupportedException>(() => generator.Generate([addPrimaryKey], model: null)).Message,
            StringComparison.OrdinalIgnoreCase);
        Assert.Contains(
            "unique constraints",
            Assert.Throws<NotSupportedException>(() => generator.Generate([addUnique], model: null)).Message,
            StringComparison.OrdinalIgnoreCase);
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

    private sealed class MigrationRuntimeContext(string connectionString) : DbContext
    {
        public DbSet<MigrationBlog> Blogs => Set<MigrationBlog>();

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.UseCSharpDb(connectionString);
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
