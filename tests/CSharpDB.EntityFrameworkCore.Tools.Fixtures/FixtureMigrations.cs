using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

namespace CSharpDB.EntityFrameworkCore.Tools.Fixtures;

[DbContext(typeof(FixtureContext))]
[Migration("202607250001_InitialCreate")]
public sealed class InitialCreate : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        Console.Out.WriteLine("TOP-SECRET-EF-FIXTURE");

        if (string.Equals(
                ActiveProvider,
                "CSharpDB.EntityFrameworkCore",
                StringComparison.Ordinal))
        {
            migrationBuilder.CreateTable(
                name: "widgets",
                columns: table => new
                {
                    Id = table.Column<long>(
                        type: "INTEGER",
                        nullable: false),
                    Name = table.Column<string>(
                        type: "TEXT",
                        nullable: false),
                },
                constraints: table =>
                {
                    table.PrimaryKey(
                        "pk_widgets",
                        widget => widget.Id);
                });
            return;
        }

        migrationBuilder.EnsureSchema("wrong_provider_branch");
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropTable(name: "widgets");
    }

    protected override void BuildTargetModel(
        ModelBuilder modelBuilder)
    {
        modelBuilder.HasAnnotation(
            "ProductVersion",
            "10.0.10");

        modelBuilder.Entity<FixtureWidget>(entity =>
        {
            entity.ToTable("widgets");
            entity.HasKey(widget => widget.Id)
                .HasName("pk_widgets");
            entity.Property(widget => widget.Id)
                .HasColumnType("INTEGER");
            entity.Property(widget => widget.Name)
                .HasColumnType("TEXT")
                .IsRequired();
        });
    }
}

[DbContext(typeof(FixtureContext))]
[Migration("202607250002_AddTagAndIndex")]
public sealed class AddTagAndIndex : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.AddColumn<string>(
            name: "Tag",
            table: "widgets",
            type: "TEXT",
            nullable: true);
        migrationBuilder.CreateIndex(
            name: "ix_widgets_name",
            table: "widgets",
            column: "Name");
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropIndex(
            name: "ix_widgets_name",
            table: "widgets");
        migrationBuilder.DropColumn(
            name: "Tag",
            table: "widgets");
    }

    protected override void BuildTargetModel(
        ModelBuilder modelBuilder)
    {
        modelBuilder.HasAnnotation(
            "ProductVersion",
            "10.0.10");

        modelBuilder.Entity<FixtureWidget>(entity =>
        {
            entity.ToTable("widgets");
            entity.HasKey(widget => widget.Id)
                .HasName("pk_widgets");
            entity.Property(widget => widget.Id)
                .HasColumnType("INTEGER");
            entity.Property(widget => widget.Name)
                .HasColumnType("TEXT")
                .IsRequired();
            entity.Property(widget => widget.Tag)
                .HasColumnType("TEXT");
            entity.HasIndex(widget => widget.Name)
                .HasDatabaseName("ix_widgets_name");
        });
    }
}

[DbContext(typeof(UnsupportedFixtureContext))]
[Migration("202607250003_UnsupportedSchema")]
public sealed class UnsupportedSchema : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.EnsureSchema("unsupported");
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropSchema("unsupported");
    }
}

[DbContext(typeof(HostedFixtureContext))]
[Migration("202607250004_HostedInitialCreate")]
public sealed class HostedInitialCreate : Migration
{
    protected override void Up(
        MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateTable(
            name: "hosted_rows",
            columns: table => new
            {
                Id = table.Column<long>(
                    type: "INTEGER",
                    nullable: false),
            },
            constraints: table =>
            {
                table.PrimaryKey(
                    "pk_hosted_rows",
                    row => row.Id);
            });
    }

    protected override void Down(
        MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropTable(
            name: "hosted_rows");
    }
}

[DbContext(typeof(RawSqlFixtureContext))]
[Migration("202607250005_RawSql")]
public sealed class RawSql : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.Sql(
            """
            CREATE TABLE raw_values (
                integer_value INTEGER NOT NULL,
                real_value REAL,
                text_value TEXT COLLATE NOCASE,
                blob_value BLOB
            );
            """);
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
    }
}

[DbContext(typeof(TargetModelFixtureContext))]
[Migration("202607250006_TargetModelInitial")]
public sealed class TargetModelInitial : Migration
{
    protected override void Up(
        MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateTable(
            name: "target_model_rows",
            columns: table => new
            {
                Id = table.Column<long>(
                    type: "INTEGER",
                    nullable: false),
                LegacyAmount = table.Column<decimal>(
                    type: "INTEGER",
                    precision: 18,
                    scale: 2,
                    nullable: false),
            },
            constraints: _ =>
            {
            });
        migrationBuilder.AddPrimaryKey(
            name: "pk_target_model_rows",
            table: "target_model_rows",
            column: "LegacyAmount");
    }

    protected override void Down(
        MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropTable(
            name: "target_model_rows");
    }

    protected override void BuildTargetModel(
        ModelBuilder modelBuilder)
    {
        modelBuilder.HasAnnotation(
            "ProductVersion",
            "10.0.10");

        modelBuilder.Entity<TargetModelFixtureRow>(
            entity =>
            {
                entity.ToTable("target_model_rows");
                entity.Property<long>("Id")
                    .HasColumnType("INTEGER");
                entity.Property<decimal>("LegacyAmount")
                    .HasPrecision(18, 2)
                    .HasColumnType("INTEGER");
                entity.HasKey("LegacyAmount")
                    .HasName("pk_target_model_rows");
            });
    }
}

[DbContext(typeof(TargetModelFixtureContext))]
[Migration("202607250007_ReplaceLegacyPrimaryKey")]
public sealed class ReplaceLegacyPrimaryKey : Migration
{
    protected override void Up(
        MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropPrimaryKey(
            name: "pk_target_model_rows",
            table: "target_model_rows");
        migrationBuilder.DropColumn(
            name: "LegacyAmount",
            table: "target_model_rows");
        migrationBuilder.AddPrimaryKey(
            name: "pk_target_model_rows",
            table: "target_model_rows",
            column: "Id");
    }

    protected override void Down(
        MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropPrimaryKey(
            name: "pk_target_model_rows",
            table: "target_model_rows");
        migrationBuilder.AddColumn<decimal>(
            name: "LegacyAmount",
            table: "target_model_rows",
            type: "INTEGER",
            precision: 18,
            scale: 2,
            nullable: false);
        migrationBuilder.AddPrimaryKey(
            name: "pk_target_model_rows",
            table: "target_model_rows",
            column: "LegacyAmount");
    }

    protected override void BuildTargetModel(
        ModelBuilder modelBuilder)
    {
        modelBuilder.HasAnnotation(
            "ProductVersion",
            "10.0.10");

        modelBuilder.Entity<TargetModelFixtureRow>(
            entity =>
            {
                entity.ToTable("target_model_rows");
                entity.Property<long>("Id")
                    .HasColumnType("INTEGER");
                entity.HasKey("Id")
                    .HasName("pk_target_model_rows");
            });
    }
}
