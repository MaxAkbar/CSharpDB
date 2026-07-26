using CSharpDB.Migration;
using CSharpDB.Migration.MySql;

namespace CSharpDB.Migration.MySql.Tests;

public sealed class MySqlRetainedBindingTests
{
    [Fact]
    public void BindingUsesOrdinalColumnsUnsignedPrimaryAndRejectsUnsafeTables()
    {
        MySqlRetainedSourceBinding binding =
            MySqlRetainedBinding.Create(
                MySqlRetainedTestFixture
                    .CreateCaptureCatalog(),
                new MySqlRetainedCaptureOptions());

        MySqlRetainedTableBinding available =
            binding.Tables.Single(table =>
                table.CatalogObject.SourceName == "Good");
        MySqlRetainedTableBinding noKey =
            binding.Tables.Single(table =>
                table.CatalogObject.SourceName == "NoKey");
        MySqlRetainedTableBinding partitioned =
            binding.Tables.Single(table =>
                table.CatalogObject.SourceName ==
                "Partitioned");

        Assert.True(available.IsAvailable);
        Assert.Equal(
            [
                "Id",
                "TinyOne",
                "Amount",
                "Rate",
                "Name",
                "Payload",
                "BusinessDate",
                "CreatedAt",
            ],
            available.Columns.Select(column =>
                column.CatalogObject.SourceName));
        Assert.Equal(
            MySqlScalarCodecKind.UnsignedInteger,
            available.Columns[0].Codec);
        Assert.Equal(
            MySqlScalarCodecKind.SignedInteger,
            available.Columns[1].Codec);
        Assert.Equal(
            "primary",
            Assert.IsType<MySqlRetainedOrderBinding>(
                available.Order).Kind);
        Assert.Equal(
            ["Id"],
            available.Order.Columns.Select(column =>
                column.CatalogObject.SourceName));
        Assert.False(noKey.IsAvailable);
        Assert.Equal(
            MySqlRetainedAvailabilityReasons.StableOrder,
            noKey.AvailabilityReason);
        Assert.False(partitioned.IsAvailable);
        Assert.Equal(
            MySqlRetainedAvailabilityReasons.TableShape,
            partitioned.AvailabilityReason);
    }

    [Fact]
    public void BindingFallsBackFromTextPrimaryToIntegerUniqueKey()
    {
        MigrationCatalog catalog =
            MySqlRetainedTestFixture.BuildCatalog(
                [MySqlTestSnapshot.Table("Fallback")],
                [
                    MySqlTestSnapshot.Column(
                        "Fallback",
                        1,
                        "TextPrimary",
                        "varchar",
                        nullable: false,
                        characterMaximumLength: 20,
                        characterSetName: "utf8mb4",
                        collationName:
                            "utf8mb4_0900_ai_ci"),
                    MySqlTestSnapshot.Column(
                        "Fallback",
                        2,
                        "IntegerUnique",
                        "bigint",
                        nullable: false),
                ],
                [
                    MySqlTestSnapshot.Key(
                        "Fallback",
                        "PRIMARY",
                        "PRIMARY KEY"),
                    MySqlTestSnapshot.Key(
                        "Fallback",
                        "UQ_Fallback"),
                ],
                [
                    MySqlTestSnapshot.KeyColumn(
                        "Fallback",
                        "PRIMARY",
                        1,
                        "TextPrimary"),
                    MySqlTestSnapshot.KeyColumn(
                        "Fallback",
                        "UQ_Fallback",
                        1,
                        "IntegerUnique"),
                ],
                [
                    MySqlTestSnapshot.Index(
                        "Fallback",
                        "PRIMARY",
                        unique: true),
                    MySqlTestSnapshot.Index(
                        "Fallback",
                        "UQ_Fallback",
                        unique: true),
                ],
                [
                    MySqlTestSnapshot.IndexPart(
                        "Fallback",
                        "PRIMARY",
                        1,
                        columnName: "TextPrimary"),
                    MySqlTestSnapshot.IndexPart(
                        "Fallback",
                        "UQ_Fallback",
                        1,
                        columnName: "IntegerUnique"),
                ]);

        MySqlRetainedTableBinding table =
            Assert.Single(
                MySqlRetainedBinding.Create(
                        catalog,
                        new MySqlRetainedCaptureOptions())
                    .Tables);

        Assert.True(table.IsAvailable);
        Assert.Equal(
            "unique",
            Assert.IsType<MySqlRetainedOrderBinding>(
                table.Order).Kind);
        Assert.Equal(
            "IntegerUnique",
            Assert.Single(table.Order.Columns)
                .CatalogObject.SourceName);
    }

    [Theory]
    [InlineData("HASH", true, "A", null)]
    [InlineData("BTREE", false, "A", null)]
    [InlineData("BTREE", true, "D", null)]
    [InlineData("BTREE", true, "A", 4L)]
    public void BindingRejectsNonExactBackingIndex(
        string indexType,
        bool visible,
        string sortDirection,
        long? prefixLength)
    {
        MigrationCatalog catalog =
            MySqlRetainedTestFixture.BuildCatalog(
                [MySqlTestSnapshot.Table("Unsafe")],
                [
                    MySqlTestSnapshot.Column(
                        "Unsafe",
                        1,
                        "Id",
                        "int",
                        nullable: false),
                ],
                [
                    MySqlTestSnapshot.Key(
                        "Unsafe",
                        "PRIMARY",
                        "PRIMARY KEY"),
                ],
                [
                    MySqlTestSnapshot.KeyColumn(
                        "Unsafe",
                        "PRIMARY",
                        1,
                        "Id"),
                ],
                [
                    MySqlTestSnapshot.Index(
                        "Unsafe",
                        "PRIMARY",
                        unique: true,
                        indexType: indexType,
                        visible: visible),
                ],
                [
                    MySqlTestSnapshot.IndexPart(
                        "Unsafe",
                        "PRIMARY",
                        1,
                        columnName: "Id",
                        sortDirection: sortDirection,
                        prefixLength: prefixLength),
                ]);

        MySqlRetainedTableBinding table =
            Assert.Single(
                MySqlRetainedBinding.Create(
                        catalog,
                        new MySqlRetainedCaptureOptions())
                    .Tables);

        Assert.False(table.IsAvailable);
        Assert.Equal(
            MySqlRetainedAvailabilityReasons.StableOrder,
            table.AvailabilityReason);
    }

    [Theory]
    [InlineData("time")]
    [InlineData("timestamp")]
    [InlineData("json")]
    [InlineData("bit")]
    [InlineData("year")]
    [InlineData("enum")]
    [InlineData("set")]
    [InlineData("geometry")]
    public void BindingRejectsExcludedScalarFamilies(
        string dataType)
    {
        MigrationCatalog catalog =
            CatalogWithExtraColumn(
                MySqlTestSnapshot.Column(
                    "Unsupported",
                    2,
                    "Value",
                    dataType,
                    nullable: false));

        MySqlRetainedTableBinding table =
            Assert.Single(
                MySqlRetainedBinding.Create(
                        catalog,
                        new MySqlRetainedCaptureOptions())
                    .Tables);

        Assert.False(table.IsAvailable);
        Assert.Equal(
            MySqlRetainedAvailabilityReasons.ScalarType,
            table.AvailabilityReason);
    }

    [Fact]
    public void BindingRejectsGeneratedInvisibleAndZerofillColumns()
    {
        MySqlColumnMetadata[] special =
        [
            MySqlTestSnapshot.Column(
                "Generated",
                2,
                "Value",
                "int",
                nullable: false,
                generated: true,
                generationKind:
                    "VIRTUAL GENERATED",
                generationExpression: "`Id` + 1"),
            MySqlTestSnapshot.Column(
                "Invisible",
                2,
                "Value",
                "int",
                nullable: false,
                invisible: true),
            MySqlTestSnapshot.Column(
                "Zerofill",
                2,
                "Value",
                "int",
                nullable: false,
                zerofill: true),
        ];

        foreach (MySqlColumnMetadata column in special)
        {
            MigrationCatalog catalog =
                CatalogWithExtraColumn(
                    column,
                    column.TableName);
            MySqlRetainedTableBinding table =
                Assert.Single(
                    MySqlRetainedBinding.Create(
                            catalog,
                            new MySqlRetainedCaptureOptions())
                        .Tables);
            Assert.False(table.IsAvailable);
            Assert.Equal(
                MySqlRetainedAvailabilityReasons.ColumnShape,
                table.AvailabilityReason);
        }
    }

    [Fact]
    public void BindingRequiresExactDirectSchemaSelectProof()
    {
        foreach (MySqlMetadataVisibilityProof proof in
                 new[]
                 {
                     MySqlMetadataVisibilityProof.Unproven,
                     MySqlTestSnapshot
                         .MetadataVisibilityProof(
                             select: false),
                     MySqlTestSnapshot
                         .MetadataVisibilityProof(
                             select: false,
                             granteeMatched: false),
                     MySqlTestSnapshot
                         .MetadataVisibilityProof(
                             select: true,
                             accountFormatSupported:
                                 false,
                             granteeMatched: false),
                 })
        {
            MySqlMigrationException error =
                Assert.Throws<MySqlMigrationException>(
                    () => MySqlRetainedBinding.Create(
                        MySqlRetainedTestFixture
                            .CreateCaptureCatalog(
                                includeUnavailable: false,
                                proof),
                        new MySqlRetainedCaptureOptions()));
            Assert.DoesNotContain(
                "SourceDb",
                error.ToString(),
                StringComparison.Ordinal);
        }
    }

    [Fact]
    public void RowSqlUsesFixedQuotedProjectionAndIntegerOrder()
    {
        MySqlRetainedTableBinding table =
            MySqlRetainedBinding.Create(
                    MySqlRetainedTestFixture
                        .CreateCaptureCatalog(
                            includeUnavailable: false),
                    new MySqlRetainedCaptureOptions())
                .AvailableTables
                .Single();

        string sql = MySqlRetainedReadSql.Build(table);
        MySqlRetainedReadCommand command =
            MySqlRetainedReadSql.CreateCommand(
                table,
                new MySqlRetainedCaptureOptions());

        Assert.StartsWith(
            "SELECT OCTET_LENGTH(`Id`), CASE WHEN " +
            "OCTET_LENGTH(`Id`) <= @max_value_bytes " +
            "THEN `Id` ELSE NULL END, ",
            sql,
            StringComparison.Ordinal);
        Assert.Contains(
            "OCTET_LENGTH(CONVERT(`Name` USING utf8mb4)), " +
            "CASE WHEN OCTET_LENGTH(CONVERT(`Name` USING utf8mb4)) " +
            "<= @max_value_bytes THEN `Name` ELSE NULL END",
            sql,
            StringComparison.Ordinal);
        Assert.Contains(
            " FROM `SourceDb`.`Good` ORDER BY `Id` ASC;",
            sql,
            StringComparison.Ordinal);
        Assert.Contains(
            "OCTET_LENGTH(CONVERT(`Name` USING utf8mb4))",
            command.PreflightCommandText,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "CASE WHEN",
            command.PreflightCommandText,
            StringComparison.Ordinal);
        Assert.Equal("SourceDb", command.DatabaseName);
        Assert.Equal("Good", command.TableName);
        Assert.Contains(
            "BINARY DATABASE() = BINARY @database_name",
            MySqlRetainedReadSql.IdentityQuery,
            StringComparison.Ordinal);
        Assert.Contains(
            "p.PARTITION_NAME IS NOT NULL",
            MySqlRetainedReadSql.IdentityQuery,
            StringComparison.Ordinal);
        Assert.Equal(
            "`a``b;c`",
            MySqlRetainedReadSql.QuoteIdentifier(
                "a`b;c"));
    }

    [Fact]
    public void CaptureOptionsRejectImpossibleEnvelopeMinimaWithTypedLimit()
    {
        Assert.Throws<MySqlRetainedCaptureLimitException>(
            () => new MySqlRetainedCaptureOptions
            {
                MaxRowBytes =
                    MySqlRetainedCaptureOptions
                        .MinimumRowBytes - 1,
            }.Validate());
        Assert.Throws<MySqlRetainedCaptureLimitException>(
            () => new MySqlRetainedCaptureOptions
            {
                MaxPackageBytes =
                    MySqlRetainedCaptureOptions
                        .MinimumPackageBytes - 1,
            }.Validate());
    }

    private static MigrationCatalog CatalogWithExtraColumn(
        MySqlColumnMetadata extra,
        string tableName = "Unsupported") =>
        MySqlRetainedTestFixture.BuildCatalog(
            [MySqlTestSnapshot.Table(tableName)],
            [
                MySqlTestSnapshot.Column(
                    tableName,
                    1,
                    "Id",
                    "int",
                    nullable: false),
                extra,
            ],
            [
                MySqlTestSnapshot.Key(
                    tableName,
                    "PRIMARY",
                    "PRIMARY KEY"),
            ],
            [
                MySqlTestSnapshot.KeyColumn(
                    tableName,
                    "PRIMARY",
                    1,
                    "Id"),
            ],
            [
                MySqlTestSnapshot.Index(
                    tableName,
                    "PRIMARY",
                    unique: true),
            ],
            [
                MySqlTestSnapshot.IndexPart(
                    tableName,
                    "PRIMARY",
                    1,
                    columnName: "Id"),
            ]);
}
