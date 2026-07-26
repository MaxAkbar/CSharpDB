using CSharpDB.Migration;

namespace CSharpDB.Migration.SqlServer.Tests;

internal static class SqlServerRetainedTestFixture
{
    internal static MigrationCatalog CreateCaptureCatalog(
        bool includeHeap = true)
    {
        var tables =
            new List<SqlServerTableMetadata>
            {
                SqlServerTestSnapshot.OrdinaryTable(
                    100,
                    1,
                    "Good"),
            };
        var columns =
            new List<SqlServerColumnMetadata>
            {
                SqlServerTestSnapshot.Column(
                    100,
                    1,
                    "Id",
                    "int",
                    "int",
                    4,
                    10,
                    nullable: false),
                SqlServerTestSnapshot.Column(
                    100,
                    2,
                    "Amount",
                    "decimal",
                    "decimal",
                    17,
                    38,
                    scale: 4,
                    nullable: false),
                SqlServerTestSnapshot.Column(
                    100,
                    3,
                    "Name",
                    "nvarchar",
                    "nvarchar",
                    200,
                    0,
                    nullable: false,
                    collation:
                        "Latin1_General_100_CI_AS_SC_UTF8"),
                SqlServerTestSnapshot.Column(
                    100,
                    4,
                    "Rate",
                    "real",
                    "real",
                    4,
                    24,
                    nullable: false),
            };
        if (includeHeap)
        {
            tables.Add(
                SqlServerTestSnapshot.OrdinaryTable(
                    200,
                    1,
                    "Heap"));
            columns.Add(
                SqlServerTestSnapshot.Column(
                    200,
                    1,
                    "Id",
                    "int",
                    "int",
                    4,
                    10,
                    nullable: false));
        }

        return BuildCatalog(
            tables,
            columns,
            [
                new SqlServerKeyMetadata(
                    ObjectId: 1_000,
                    ParentObjectId: 100,
                    Name: "PK_Good",
                    Type: "PK",
                    UniqueIndexId: 1,
                    IsSystemNamed: false),
            ],
            [
                SqlServerTestSnapshot.Index(
                    100,
                    1,
                    "PK_Good",
                    unique: true,
                    primaryKey: true),
            ],
            [
                SqlServerTestSnapshot.IndexColumn(
                    100,
                    1,
                    1,
                    1,
                    keyOrdinal: 1),
            ]);
    }

    internal static MigrationCatalog BuildCatalog(
        IReadOnlyList<SqlServerTableMetadata> tables,
        IReadOnlyList<SqlServerColumnMetadata> columns,
        IReadOnlyList<SqlServerKeyMetadata>? keys = null,
        IReadOnlyList<SqlServerIndexMetadata>? indexes = null,
        IReadOnlyList<SqlServerIndexColumnMetadata>?
            indexColumns = null,
        IReadOnlyList<SqlServerSchemaMetadata>? schemas = null)
    {
        SqlServerCatalogSnapshot snapshot =
            SqlServerTestSnapshot.Create(
                schemas:
                    schemas ??
                    [new SqlServerSchemaMetadata(
                        1,
                        "dbo",
                        HasViewDefinition: true)],
                tables: tables,
                columns: columns,
                keys: keys ?? [],
                indexes: indexes ?? [],
                indexColumns: indexColumns ?? []);
        return SqlServerCatalogBuilder.Build(
            snapshot,
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion =
                    CSharpDbCapabilityCatalogLoader
                        .CurrentTargetVersion,
                IncludeProfile = false,
            },
            SqlServerInspectionLimits.Default,
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
