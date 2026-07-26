using CSharpDB.Migration;

namespace CSharpDB.Migration.SqlServer.Tests;

public sealed class SqlServerRetainedBindingTests
{
    [Fact]
    public void BindingUsesSourceColumnOrderAndRejectsHeapScans()
    {
        MigrationCatalog catalog =
            SqlServerRetainedTestFixture
                .CreateCaptureCatalog();

        SqlServerRetainedSourceBinding binding =
            SqlServerRetainedBinding.Create(
                catalog,
                new SqlServerRetainedCaptureOptions());
        SqlServerRetainedTableBinding available =
            binding.Tables.Single(table =>
                table.CatalogObject.SourceName == "Good");
        SqlServerRetainedTableBinding heap =
            binding.Tables.Single(table =>
                table.CatalogObject.SourceName == "Heap");

        Assert.True(available.IsAvailable);
        Assert.Equal(
            ["Id", "Amount", "Name", "Rate"],
            available.Columns.Select(column =>
                column.CatalogObject.SourceName));
        Assert.Equal(
            "primary",
            Assert.IsType<SqlServerRetainedOrderBinding>(
                available.Order).Kind);
        Assert.Equal(
            ["Id"],
            available.Order.Columns.Select(column =>
                column.CatalogObject.SourceName));
        Assert.False(heap.IsAvailable);
        Assert.Equal(
            SqlServerRetainedAvailabilityReasons
                .StableOrder,
            heap.AvailabilityReason);
    }

    [Fact]
    public void BindingFallsBackFromUnsafePrimaryKeyToSafeUniqueConstraint()
    {
        MigrationCatalog catalog =
            SqlServerRetainedTestFixture.BuildCatalog(
                [
                    SqlServerTestSnapshot.OrdinaryTable(
                        100,
                        1,
                        "Fallback"),
                ],
                [
                    SqlServerTestSnapshot.Column(
                        100,
                        1,
                        "NullablePrimary",
                        "int",
                        "int",
                        4,
                        10,
                        nullable: true),
                    SqlServerTestSnapshot.Column(
                        100,
                        2,
                        "SafeUnique",
                        "bigint",
                        "bigint",
                        8,
                        19,
                        nullable: false),
                ],
                [
                    new SqlServerKeyMetadata(
                        1_000,
                        100,
                        "PK_Fallback",
                        "PK",
                        1,
                        false),
                    new SqlServerKeyMetadata(
                        1_001,
                        100,
                        "UQ_Fallback",
                        "UQ",
                        2,
                        false),
                ],
                [
                    SqlServerTestSnapshot.Index(
                        100,
                        1,
                        "PK_Fallback",
                        unique: true,
                        primaryKey: true),
                    SqlServerTestSnapshot.Index(
                        100,
                        2,
                        "UQ_Fallback",
                        unique: true,
                        uniqueConstraint: true),
                ],
                [
                    SqlServerTestSnapshot.IndexColumn(
                        100,
                        1,
                        1,
                        1,
                        1),
                    SqlServerTestSnapshot.IndexColumn(
                        100,
                        2,
                        1,
                        2,
                        1),
                ]);

        SqlServerRetainedTableBinding table =
            Assert.Single(
                SqlServerRetainedBinding.Create(
                        catalog,
                        new SqlServerRetainedCaptureOptions())
                    .Tables);

        Assert.True(table.IsAvailable);
        Assert.Equal(
            "unique",
            Assert.IsType<SqlServerRetainedOrderBinding>(
                table.Order).Kind);
        Assert.Equal(
            "SafeUnique",
            Assert.Single(table.Order.Columns)
                .CatalogObject.SourceName);
    }

    [Fact]
    public void BindingRejectsSpecialColumnsAndMissingStableKeys()
    {
        SqlServerTableMetadata[] tables =
            Enumerable.Range(1, 7)
                .Select(index =>
                    SqlServerTestSnapshot.OrdinaryTable(
                        index * 100,
                        1,
                        $"T{index}"))
                .ToArray();
        SqlServerColumnMetadata[] columns =
        [
            SqlServerTestSnapshot.Column(
                100, 1, "Id", "int", "int", 4, 10,
                nullable: false, isIdentity: true),
            SqlServerTestSnapshot.Column(
                200, 1, "Id", "int", "int", 4, 10,
                nullable: false, hasDefault: true),
            SqlServerTestSnapshot.Column(
                300, 1, "Id", "int", "int", 4, 10,
                nullable: false, isComputed: true),
            SqlServerTestSnapshot.Column(
                400, 1, "Version", "timestamp", "timestamp",
                8, 0, nullable: false),
            SqlServerTestSnapshot.Column(
                500, 1, "Alias", "AliasType", "int", 4, 10,
                nullable: false, typeSchema: "dbo"),
            SqlServerTestSnapshot.Column(
                600, 1, "Hidden", "int", "int", 4, 10,
                nullable: false, isHidden: true),
            SqlServerTestSnapshot.Column(
                700, 1, "Id", "int", "int", 4, 10,
                nullable: false),
        ];

        SqlServerRetainedSourceBinding binding =
            SqlServerRetainedBinding.Create(
                SqlServerRetainedTestFixture
                    .BuildCatalog(
                        tables,
                        columns),
                new SqlServerRetainedCaptureOptions());

        Assert.All(
            binding.Tables,
            table => Assert.False(table.IsAvailable));
        Assert.Equal(
            SqlServerRetainedAvailabilityReasons
                .StableOrder,
            binding.Tables.Single(table =>
                    table.CatalogObject.SourceName == "T7")
                .AvailabilityReason);
        Assert.Equal(
            6,
            binding.Tables.Count(table =>
                table.AvailabilityReason !=
                SqlServerRetainedAvailabilityReasons
                    .StableOrder));
    }

    [Fact]
    public void BindingFailsClosedForEnabledOrUnprovenRowLevelSecurity()
    {
        SqlServerTableMetadata[] tables =
        [
            SqlServerTestSnapshot.OrdinaryTable(
                100,
                1,
                "Clear"),
            SqlServerTestSnapshot.OrdinaryTable(
                    200,
                    1,
                    "Filtered")
                with
                {
                    HasEnabledRowLevelSecurityFilter =
                        true,
                },
            SqlServerTestSnapshot.OrdinaryTable(
                    300,
                    1,
                    "Unproven")
                with
                {
                    IsRowLevelSecurityInventoryComplete =
                        false,
                },
        ];
        SqlServerColumnMetadata[] columns =
            tables.Select(table =>
                    SqlServerTestSnapshot.Column(
                        table.ObjectId,
                        1,
                        "Id",
                        "int",
                        "int",
                        4,
                        10,
                        nullable: false))
                .ToArray();
        SqlServerKeyMetadata[] keys =
            tables.Select(table =>
                    new SqlServerKeyMetadata(
                        table.ObjectId + 1_000,
                        table.ObjectId,
                        $"PK_{table.Name}",
                        "PK",
                        1,
                        false))
                .ToArray();
        SqlServerIndexMetadata[] indexes =
            tables.Select(table =>
                    SqlServerTestSnapshot.Index(
                        table.ObjectId,
                        1,
                        $"PK_{table.Name}",
                        unique: true,
                        primaryKey: true))
                .ToArray();
        SqlServerIndexColumnMetadata[] indexColumns =
            tables.Select(table =>
                    SqlServerTestSnapshot.IndexColumn(
                        table.ObjectId,
                        1,
                        1,
                        1,
                        1))
                .ToArray();
        MigrationCatalog catalog =
            SqlServerRetainedTestFixture.BuildCatalog(
                tables,
                columns,
                keys,
                indexes,
                indexColumns);

        MigrationCatalogObject filteredObject =
            catalog.Objects.Single(item =>
                item.Kind == MigrationObjectKind.Table &&
                item.SourceName == "Filtered");
        MigrationCatalogObject unprovenObject =
            catalog.Objects.Single(item =>
                item.Kind == MigrationObjectKind.Table &&
                item.SourceName == "Unproven");
        Assert.Equal(
            "true",
            SqlServerRetainedTestFixture.Facet(
                filteredObject,
                SqlServerCatalogBuilder
                    .RowLevelSecurityFilterFacet));
        Assert.Equal(
            "false",
            SqlServerRetainedTestFixture.Facet(
                unprovenObject,
                SqlServerCatalogBuilder
                    .RowLevelSecurityInventoryCompleteFacet));
        Assert.Contains(
            catalog.Diagnostics,
            static item =>
                item.RuleId ==
                "MIG-SQLSERVER-RLS-FILTER-UNSUPPORTED-001");
        Assert.Contains(
            catalog.Diagnostics,
            static item =>
                item.RuleId ==
                "MIG-SQLSERVER-RLS-INVENTORY-INCOMPLETE-001");

        SqlServerRetainedSourceBinding binding =
            SqlServerRetainedBinding.Create(
                catalog,
                new SqlServerRetainedCaptureOptions());
        Assert.True(
            binding.Tables.Single(table =>
                    table.CatalogObject.SourceName == "Clear")
                .IsAvailable);
        SqlServerRetainedTableBinding filtered =
            binding.Tables.Single(table =>
                table.CatalogObject.SourceName == "Filtered");
        Assert.False(filtered.IsAvailable);
        Assert.Equal(
            SqlServerRetainedAvailabilityReasons
                .RowLevelSecurity,
            filtered.AvailabilityReason);
        SqlServerRetainedTableBinding unproven =
            binding.Tables.Single(table =>
                table.CatalogObject.SourceName == "Unproven");
        Assert.False(unproven.IsAvailable);
        Assert.Equal(
            SqlServerRetainedAvailabilityReasons
                .RowLevelSecurityInventory,
            unproven.AvailabilityReason);
    }

    [Fact]
    public void ReadSqlQuotesVerifiedNamesAndOrdersByTheFullKey()
    {
        MigrationCatalog catalog =
            SqlServerRetainedTestFixture.BuildCatalog(
                [
                    SqlServerTestSnapshot.OrdinaryTable(
                        100,
                        7,
                        "Odd]Table"),
                ],
                [
                    SqlServerTestSnapshot.Column(
                        100,
                        1,
                        "First]Key",
                        "int",
                        "int",
                        4,
                        10,
                        nullable: false),
                    SqlServerTestSnapshot.Column(
                        100,
                        2,
                        "SecondKey",
                        "bigint",
                        "bigint",
                        8,
                        19,
                        nullable: false),
                ],
                [
                    new SqlServerKeyMetadata(
                        1_000,
                        100,
                        "PK_Odd",
                        "PK",
                        1,
                        false),
                ],
                [
                    SqlServerTestSnapshot.Index(
                        100,
                        1,
                        "PK_Odd",
                        unique: true,
                        primaryKey: true),
                ],
                [
                    SqlServerTestSnapshot.IndexColumn(
                        100, 1, 1, 1, 1),
                    SqlServerTestSnapshot.IndexColumn(
                        100, 1, 2, 2, 2),
                ],
                [
                    new SqlServerSchemaMetadata(
                        7,
                        "d]bo",
                        HasViewDefinition: true),
                ]);
        SqlServerRetainedTableBinding table =
            Assert.Single(
                SqlServerRetainedBinding.Create(
                        catalog,
                        new SqlServerRetainedCaptureOptions())
                    .AvailableTables);

        string sql =
            SqlServerRetainedReadSql.Build(table);
        SqlServerRetainedReadCommand command =
            SqlServerRetainedReadSql.CreateCommand(
                table,
                new SqlServerRetainedCaptureOptions
                {
                    RowCommandTimeoutSeconds = 7_200,
                });

        Assert.Contains(
            "CONVERT(bigint, DATALENGTH([First]]Key])), [First]]Key]",
            sql,
            StringComparison.Ordinal);
        Assert.Contains(
            "FROM [d]]bo].[Odd]]Table]",
            sql,
            StringComparison.Ordinal);
        Assert.EndsWith(
            "ORDER BY [First]]Key] ASC, [SecondKey] ASC;",
            sql,
            StringComparison.Ordinal);
        Assert.Equal(
            "[d]]bo].[Odd]]Table]",
            SqlServerRetainedReadSql.QualifiedName(table));
        Assert.Equal(sql, command.CommandText);
        Assert.Equal(7_200, command.CommandTimeoutSeconds);
        Assert.Equal(
            "[d]]bo].[Odd]]Table]",
            command.QualifiedName);
        Assert.Equal(100, command.ExpectedObjectId);
        Assert.Throws<SqlServerMigrationException>(
            () => SqlServerRetainedReadSql
                .QuoteIdentifier("bad\uD800name"));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(86_401)]
    public void CaptureOptionsRejectInvalidRowCommandTimeout(
        int timeoutSeconds)
    {
        var options =
            new SqlServerRetainedCaptureOptions
            {
                RowCommandTimeoutSeconds =
                    timeoutSeconds,
            };

        Assert.Throws<ArgumentOutOfRangeException>(
            options.Validate);
    }

    [Fact]
    public void CaptureOptionsClassifyRetainedEnvelopeMinimaAsCaptureLimits()
    {
        Assert.Throws<SqlServerRetainedCaptureLimitException>(
            new SqlServerRetainedCaptureOptions
            {
                MaxPackageBytes =
                    SqlServerRetainedCaptureOptions
                        .MinimumPackageBytes -
                    1,
            }.Validate);
        Assert.Throws<SqlServerRetainedCaptureLimitException>(
            new SqlServerRetainedCaptureOptions
            {
                MaxValueBytes = 1,
                MaxRowBytes =
                    SqlServerRetainedCaptureOptions
                        .MinimumRowBytes -
                    1,
            }.Validate);

        new SqlServerRetainedCaptureOptions
        {
            MaxPackageBytes =
                SqlServerRetainedCaptureOptions
                    .MinimumPackageBytes,
            MaxValueBytes = 1,
            MaxRowBytes =
                SqlServerRetainedCaptureOptions
                    .MinimumRowBytes,
        }.Validate();
    }
}
