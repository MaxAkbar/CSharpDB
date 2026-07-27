using CSharpDB.Migration;
using CSharpDB.Migration.MySql;

namespace CSharpDB.Migration.MySql.Tests;

internal static class MySqlRetainedTestFixture
{
    internal static MigrationCatalog CreateCaptureCatalog(
        bool includeUnavailable = true,
        MySqlMetadataVisibilityProof? proof = null)
    {
        var tables = new List<MySqlTableMetadata>
        {
            MySqlTestSnapshot.Table("Good"),
        };
        var columns = new List<MySqlColumnMetadata>
        {
            MySqlTestSnapshot.Column(
                "Good",
                1,
                "Id",
                "bigint",
                nullable: false,
                unsigned: true),
            MySqlTestSnapshot.Column(
                "Good",
                2,
                "TinyOne",
                "tinyint",
                nullable: false,
                tinyIntOne: true),
            MySqlTestSnapshot.Column(
                "Good",
                3,
                "Amount",
                "decimal",
                nullable: false,
                numericPrecision: 65,
                numericScale: 30),
            MySqlTestSnapshot.Column(
                "Good",
                4,
                "Rate",
                "float",
                nullable: false),
            MySqlTestSnapshot.Column(
                "Good",
                5,
                "Name",
                "varchar",
                nullable: false,
                characterMaximumLength: 100,
                characterSetName: "utf8mb4",
                collationName:
                    "utf8mb4_0900_ai_ci"),
            MySqlTestSnapshot.Column(
                "Good",
                6,
                "Payload",
                "varbinary",
                nullable: false,
                characterMaximumLength: 100),
            MySqlTestSnapshot.Column(
                "Good",
                7,
                "BusinessDate",
                "date",
                nullable: false),
            MySqlTestSnapshot.Column(
                "Good",
                8,
                "CreatedAt",
                "datetime",
                nullable: false,
                dateTimePrecision: 6),
        };
        if (includeUnavailable)
        {
            tables.Add(MySqlTestSnapshot.Table("NoKey"));
            columns.Add(
                MySqlTestSnapshot.Column(
                    "NoKey",
                    1,
                    "Id",
                    "int",
                    nullable: false));
            tables.Add(
                MySqlTestSnapshot.Table(
                    "Partitioned",
                    partitioned: true));
            columns.Add(
                MySqlTestSnapshot.Column(
                    "Partitioned",
                    1,
                    "Id",
                    "int",
                    nullable: false));
        }

        return BuildCatalog(
            tables,
            columns,
            [
                MySqlTestSnapshot.Key(
                    "Good",
                    "PRIMARY",
                    "PRIMARY KEY"),
            ],
            [
                MySqlTestSnapshot.KeyColumn(
                    "Good",
                    "PRIMARY",
                    1,
                    "Id"),
            ],
            [
                MySqlTestSnapshot.Index(
                    "Good",
                    "PRIMARY",
                    unique: true),
            ],
            [
                MySqlTestSnapshot.IndexPart(
                    "Good",
                    "PRIMARY",
                    1,
                    columnName: "Id"),
            ],
            proof ?? MySqlTestSnapshot
                .MetadataVisibilityProof(
                    showView: false,
                    trigger: false,
                    execute: false));
    }

    internal static MigrationCatalog BuildCatalog(
        IReadOnlyList<MySqlTableMetadata> tables,
        IReadOnlyList<MySqlColumnMetadata> columns,
        IReadOnlyList<MySqlKeyMetadata>? keys = null,
        IReadOnlyList<MySqlKeyColumnMetadata>?
            keyColumns = null,
        IReadOnlyList<MySqlIndexMetadata>? indexes = null,
        IReadOnlyList<MySqlIndexPartMetadata>?
            indexParts = null,
        MySqlMetadataVisibilityProof? proof = null)
    {
        MySqlCatalogSnapshot snapshot =
            MySqlTestSnapshot.Create(
                tables: tables,
                columns: columns,
                keys: keys ?? [],
                keyColumns: keyColumns ?? [],
                indexes: indexes ?? [],
                indexParts: indexParts ?? [],
                metadataVisibilityProof:
                    proof ??
                    MySqlTestSnapshot
                        .MetadataVisibilityProof(
                            showView: false,
                            trigger: false,
                            execute: false));
        return MySqlCatalogBuilder.Build(
            snapshot,
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion =
                    CSharpDbCapabilityCatalogLoader
                        .CurrentTargetVersion,
                IncludeProfile = false,
            },
            MySqlInspectionLimits.Default,
            CancellationToken.None);
    }

    internal static string? Facet(
        MigrationCatalogObject item,
        string name) =>
        item.Facets.SingleOrDefault(facet =>
            string.Equals(
                facet.Name,
                name,
                StringComparison.Ordinal))?.Value;
}
