using CSharpDB.Data;
using CSharpDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace CSharpDB.EntityFrameworkCore.Tools.Fixtures;

public sealed class ScratchSchemaMismatchFixtureContext(
    DbContextOptions<ScratchSchemaMismatchFixtureContext> options)
    : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ScratchSchemaMismatchRow>(entity =>
        {
            entity.ToTable("scratch_schema_mismatch_rows");
            entity.HasKey(row => row.Id)
                .HasName("pk_scratch_schema_mismatch_rows");
            entity.Property(row => row.Id)
                .HasColumnType("INTEGER");
        });
    }
}

public sealed class ScratchSchemaMismatchFixtureContextFactory
    : IDesignTimeDbContextFactory<
        ScratchSchemaMismatchFixtureContext>
{
    public ScratchSchemaMismatchFixtureContext CreateDbContext(
        string[] args)
    {
        var options =
            new DbContextOptionsBuilder<
                ScratchSchemaMismatchFixtureContext>()
                .UseCSharpDb(
                    "Data Source=:memory:;Pooling=false")
                .Options;
        return new ScratchSchemaMismatchFixtureContext(options);
    }
}

public sealed class ScratchRoundTripMismatchFixtureContext(
    DbContextOptions<ScratchRoundTripMismatchFixtureContext> options)
    : DbContext(options)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ScratchRoundTripMismatchRow>(entity =>
        {
            entity.ToTable("scratch_roundtrip_mismatch_rows");
            entity.HasKey(row => row.Id)
                .HasName("pk_scratch_roundtrip_mismatch_rows");
            entity.Property(row => row.Id)
                .HasColumnType("INTEGER");
            entity.Property(row => row.Value)
                .HasColumnType("TEXT");
        });
    }
}

public sealed class ScratchRoundTripMismatchFixtureContextFactory
    : IDesignTimeDbContextFactory<
        ScratchRoundTripMismatchFixtureContext>
{
    public ScratchRoundTripMismatchFixtureContext CreateDbContext(
        string[] args)
    {
        var options =
            new DbContextOptionsBuilder<
                ScratchRoundTripMismatchFixtureContext>()
                .UseCSharpDb(
                    "Data Source=:memory:;Pooling=false")
                .Options;
        return new ScratchRoundTripMismatchFixtureContext(options);
    }
}

public sealed class ScratchSentinelIsolationFixtureContext(
    DbContextOptions<ScratchSentinelIsolationFixtureContext> options)
    : DbContext(options)
{
    public const string SentinelPathEnvironmentVariable =
        "CSHARPDB_EF_SCRATCH_SENTINEL_PATH";

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ScratchSentinelIsolationRow>(entity =>
        {
            entity.ToTable("scratch_sentinel_rows");
            entity.HasKey(row => row.Id)
                .HasName("pk_scratch_sentinel_rows");
            entity.Property(row => row.Id)
                .HasColumnType("INTEGER");
            entity.Property(row => row.Value)
                .HasColumnType("TEXT")
                .IsRequired();
        });
    }
}

public sealed class ScratchSentinelIsolationFixtureContextFactory
    : IDesignTimeDbContextFactory<
        ScratchSentinelIsolationFixtureContext>
{
    public ScratchSentinelIsolationFixtureContext CreateDbContext(
        string[] args)
    {
        string sentinelPath = Environment.GetEnvironmentVariable(
                ScratchSentinelIsolationFixtureContext
                    .SentinelPathEnvironmentVariable) ??
            throw new InvalidOperationException(
                "The scratch sentinel path is unavailable.");
        if (string.IsNullOrWhiteSpace(sentinelPath))
        {
            throw new InvalidOperationException(
                "The scratch sentinel path is unavailable.");
        }

        var connectionString = new CSharpDbConnectionStringBuilder
        {
            DataSource = sentinelPath,
            Pooling = false,
        };
        var options =
            new DbContextOptionsBuilder<
                ScratchSentinelIsolationFixtureContext>()
                .UseCSharpDb(connectionString.ConnectionString)
                .Options;
        return new ScratchSentinelIsolationFixtureContext(options);
    }
}

public sealed class ScratchSqlGeneratorOverrideFixtureContext(
    DbContextOptions<ScratchSqlGeneratorOverrideFixtureContext> options)
    : DbContext(options);

public sealed class ScratchSqlGeneratorOverrideFixtureContextFactory
    : IDesignTimeDbContextFactory<
        ScratchSqlGeneratorOverrideFixtureContext>
{
    public ScratchSqlGeneratorOverrideFixtureContext CreateDbContext(
        string[] args)
    {
        var options =
            new DbContextOptionsBuilder<
                ScratchSqlGeneratorOverrideFixtureContext>()
                .UseCSharpDb(
                    "Data Source=:memory:;Pooling=false")
                .ReplaceService<
                    IMigrationsSqlGenerator,
                    ScratchSqlGeneratorOverride>()
                .Options;
        return new ScratchSqlGeneratorOverrideFixtureContext(
            options);
    }
}

public sealed class ScratchSqlGeneratorOverride
    : IMigrationsSqlGenerator
{
    public IReadOnlyList<MigrationCommand> Generate(
        IReadOnlyList<MigrationOperation> operations,
        IModel? model = null,
        MigrationsSqlGenerationOptions options =
            MigrationsSqlGenerationOptions.Default) =>
        throw new InvalidOperationException(
            "A configured SQL-generator override must not run.");
}

public sealed class ScratchSqlGenerationDependencyOverrideFixtureContext(
    DbContextOptions<
        ScratchSqlGenerationDependencyOverrideFixtureContext> options)
    : DbContext(options);

public sealed class
    ScratchSqlGenerationDependencyOverrideFixtureContextFactory
    : IDesignTimeDbContextFactory<
        ScratchSqlGenerationDependencyOverrideFixtureContext>
{
    public ScratchSqlGenerationDependencyOverrideFixtureContext
        CreateDbContext(string[] args)
    {
        var options =
            new DbContextOptionsBuilder<
                ScratchSqlGenerationDependencyOverrideFixtureContext>()
                .UseCSharpDb(
                    "Data Source=:memory:;Pooling=false")
                .ReplaceService<
                    ISqlGenerationHelper,
                    ScratchSqlGenerationHelperOverride>()
                .Options;
        return new
            ScratchSqlGenerationDependencyOverrideFixtureContext(
                options);
    }
}

public sealed class ScratchSqlGenerationHelperOverride(
    RelationalSqlGenerationHelperDependencies dependencies)
    : RelationalSqlGenerationHelper(dependencies);

public sealed class ScratchCustomOptionsExtensionFixtureContext(
    DbContextOptions<ScratchCustomOptionsExtensionFixtureContext> options)
    : DbContext(options);

public sealed class ScratchCustomOptionsExtensionFixtureContextFactory
    : IDesignTimeDbContextFactory<
        ScratchCustomOptionsExtensionFixtureContext>
{
    public ScratchCustomOptionsExtensionFixtureContext CreateDbContext(
        string[] args)
    {
        var builder =
            new DbContextOptionsBuilder<
                ScratchCustomOptionsExtensionFixtureContext>()
                .UseCSharpDb(
                    "Data Source=:memory:;Pooling=false");
        ((IDbContextOptionsBuilderInfrastructure)builder)
            .AddOrUpdateExtension(
                new ScratchCustomServiceOptionsExtension());
        return new ScratchCustomOptionsExtensionFixtureContext(
            builder.Options);
    }
}

public sealed class ScratchCustomServiceOptionsExtension
    : IDbContextOptionsExtension
{
    private DbContextOptionsExtensionInfo? _info;

    public DbContextOptionsExtensionInfo Info =>
        _info ??= new ExtensionInfo(this);

    public void ApplyServices(IServiceCollection services) =>
        services.AddSingleton<
            ISqlGenerationHelper,
            ScratchSqlGenerationHelperOverride>();

    public void Validate(IDbContextOptions options)
    {
    }

    private sealed class ExtensionInfo(
        IDbContextOptionsExtension extension)
        : DbContextOptionsExtensionInfo(extension)
    {
        public override bool IsDatabaseProvider => false;

        public override string LogFragment =>
            "using scratch-custom-services ";

        public override int GetServiceProviderHashCode() => 1;

        public override void PopulateDebugInfo(
            IDictionary<string, string> debugInfo) =>
            debugInfo["ScratchCustomServices"] = "1";

        public override bool ShouldUseSameServiceProvider(
            DbContextOptionsExtensionInfo other) =>
            other is ExtensionInfo;
    }
}

public sealed class ScratchSchemaMismatchRow
{
    public long Id { get; set; }
}

public sealed class ScratchRoundTripMismatchRow
{
    public long Id { get; set; }

    public string? Value { get; set; }
}

public sealed class ScratchSentinelIsolationRow
{
    public long Id { get; set; }

    public required string Value { get; set; }
}

[DbContext(typeof(ScratchSchemaMismatchFixtureContext))]
[Migration("202607250101_ScratchSchemaMismatch")]
public sealed class ScratchSchemaMismatchMigration : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateTable(
            name: "scratch_schema_mismatch_rows",
            columns: table => new
            {
                Id = table.Column<long>(
                    type: "INTEGER",
                    nullable: false),
                UnexpectedValue = table.Column<string>(
                    type: "TEXT",
                    nullable: true),
            },
            constraints: table =>
            {
                table.PrimaryKey(
                    "pk_scratch_schema_mismatch_rows",
                    row => row.Id);
            });
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropTable(
            name: "scratch_schema_mismatch_rows");
    }

    protected override void BuildTargetModel(
        ModelBuilder modelBuilder)
    {
        modelBuilder.HasAnnotation(
            "ProductVersion",
            "10.0.10");
        modelBuilder.Entity<ScratchSchemaMismatchRow>(entity =>
        {
            entity.ToTable("scratch_schema_mismatch_rows");
            entity.HasKey(row => row.Id)
                .HasName("pk_scratch_schema_mismatch_rows");
            entity.Property(row => row.Id)
                .HasColumnType("INTEGER");
        });
    }
}

[DbContext(typeof(ScratchRoundTripMismatchFixtureContext))]
[Migration("202607250102_ScratchRoundTripMismatch")]
public sealed class ScratchRoundTripMismatchMigration : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateTable(
            name: "scratch_roundtrip_mismatch_rows",
            columns: table => new
            {
                Id = table.Column<long>(
                    type: "INTEGER",
                    nullable: false),
                Value = table.Column<string>(
                    type: "TEXT",
                    nullable: true),
            },
            constraints: table =>
            {
                table.PrimaryKey(
                    "pk_scratch_roundtrip_mismatch_rows",
                    row => row.Id);
            });
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropColumn(
            name: "Value",
            table: "scratch_roundtrip_mismatch_rows");
    }

    protected override void BuildTargetModel(
        ModelBuilder modelBuilder)
    {
        modelBuilder.HasAnnotation(
            "ProductVersion",
            "10.0.10");
        modelBuilder.Entity<ScratchRoundTripMismatchRow>(entity =>
        {
            entity.ToTable("scratch_roundtrip_mismatch_rows");
            entity.HasKey(row => row.Id)
                .HasName("pk_scratch_roundtrip_mismatch_rows");
            entity.Property(row => row.Id)
                .HasColumnType("INTEGER");
            entity.Property(row => row.Value)
                .HasColumnType("TEXT");
        });
    }
}

[DbContext(typeof(ScratchSentinelIsolationFixtureContext))]
[Migration("202607250103_ScratchSentinelIsolation")]
public sealed class ScratchSentinelIsolationMigration : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateTable(
            name: "scratch_sentinel_rows",
            columns: table => new
            {
                Id = table.Column<long>(
                    type: "INTEGER",
                    nullable: false),
                Value = table.Column<string>(
                    type: "TEXT",
                    nullable: false),
            },
            constraints: table =>
            {
                table.PrimaryKey(
                    "pk_scratch_sentinel_rows",
                    row => row.Id);
            });
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropTable(
            name: "scratch_sentinel_rows");
    }

    protected override void BuildTargetModel(
        ModelBuilder modelBuilder)
    {
        modelBuilder.HasAnnotation(
            "ProductVersion",
            "10.0.10");
        modelBuilder.Entity<ScratchSentinelIsolationRow>(entity =>
        {
            entity.ToTable("scratch_sentinel_rows");
            entity.HasKey(row => row.Id)
                .HasName("pk_scratch_sentinel_rows");
            entity.Property(row => row.Id)
                .HasColumnType("INTEGER");
            entity.Property(row => row.Value)
                .HasColumnType("TEXT")
                .IsRequired();
        });
    }
}
