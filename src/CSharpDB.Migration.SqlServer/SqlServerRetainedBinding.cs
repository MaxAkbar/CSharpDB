using System.Collections.ObjectModel;
using System.Globalization;
using CSharpDB.Migration;

namespace CSharpDB.Migration.SqlServer;

internal static class SqlServerRetainedAvailabilityReasons
{
    internal const string Available = "available";
    internal const string TableShape = "table-shape-unsupported";
    internal const string ColumnShape = "column-shape-unsupported";
    internal const string ScalarType = "scalar-type-unsupported";
    internal const string StableOrder = "stable-order-key-unavailable";
    internal const string RowLevelSecurity =
        "row-level-security-filter-enabled";
    internal const string RowLevelSecurityInventory =
        "row-level-security-inventory-incomplete";
}

internal sealed record SqlServerRetainedColumnBinding
{
    internal required MigrationCatalogObject CatalogObject { get; init; }

    internal required int SqlServerColumnId { get; init; }

    internal required bool IsSupported { get; init; }

    internal required string AvailabilityReason { get; init; }

    internal SqlServerScalarCodecKind? Codec { get; init; }

    internal int? BinaryWidth { get; init; }

    internal bool Nullable { get; init; }
}

internal sealed record SqlServerRetainedOrderBinding
{
    internal required MigrationCatalogObject CatalogObject { get; init; }

    internal required string Kind { get; init; }

    internal required IReadOnlyList<SqlServerRetainedColumnBinding>
        Columns
    { get; init; }
}

internal sealed record SqlServerRetainedTableBinding
{
    internal required MigrationCatalogObject CatalogObject { get; init; }

    internal required int SqlServerObjectId { get; init; }

    internal required IReadOnlyList<SqlServerRetainedColumnBinding>
        Columns
    { get; init; }

    internal required bool IsAvailable { get; init; }

    internal required string AvailabilityReason { get; init; }

    internal SqlServerRetainedOrderBinding? Order { get; init; }
}

internal sealed record SqlServerRetainedSourceBinding
{
    internal required MigrationCatalog AnalyzerCatalog { get; init; }

    internal required MigrationCatalogObject Database { get; init; }

    internal required IReadOnlyList<SqlServerRetainedTableBinding>
        Tables
    { get; init; }

    internal IReadOnlyList<SqlServerRetainedTableBinding>
        AvailableTables =>
        Tables.Where(static table => table.IsAvailable).ToArray();
}

internal static class SqlServerRetainedBinding
{
    internal static SqlServerRetainedSourceBinding Create(
        MigrationCatalog catalog,
        SqlServerRetainedCaptureOptions options)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        MigrationContractValidator.ValidateCatalog(catalog);
        if (catalog.Source.Kind != MigrationSourceKind.SqlServer)
        {
            throw new ArgumentException(
                "The retained SQL Server binding requires a SQL Server catalog.",
                nameof(catalog));
        }

        MigrationCatalogObject database = catalog.Objects.Single(
            static item =>
                item.Kind == MigrationObjectKind.Database);
        if (!string.Equals(
                Facet(database, "sqlServerCatalogContract"),
                SqlServerCatalogBuilder.CatalogContract,
                StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "The SQL Server analyzer catalog contract is unsupported.",
                nameof(catalog));
        }

        MigrationCatalogObject[] tables = catalog.Objects
            .Where(static item =>
                item.Kind == MigrationObjectKind.Table)
            .OrderBy(static item => item.ObjectId, StringComparer.Ordinal)
            .ToArray();
        if (tables.Length > options.MaxTables)
        {
            throw new SqlServerRetainedCaptureLimitException(
                "The SQL Server retained capture exceeds its table-count bound.");
        }

        IReadOnlyDictionary<string, MigrationCatalogObject> objects =
            catalog.Objects.ToDictionary(
                static item => item.ObjectId,
                StringComparer.Ordinal);
        var bindings = new List<SqlServerRetainedTableBinding>(
            tables.Length);
        foreach (MigrationCatalogObject table in tables)
        {
            MigrationCatalogObject[] catalogColumns = catalog.Objects
                .Where(item =>
                    item.Kind == MigrationObjectKind.Column &&
                    string.Equals(
                        item.ParentObjectId,
                        table.ObjectId,
                        StringComparison.Ordinal))
                .OrderBy(item =>
                    PositiveIntFacet(
                        item,
                        "sqlServerColumnId"))
                .ThenBy(static item =>
                    item.ObjectId,
                    StringComparer.Ordinal)
                .ToArray();
            if (catalogColumns.Length == 0 ||
                catalogColumns.Length > options.MaxColumnsPerTable)
            {
                throw new SqlServerRetainedCaptureLimitException(
                    "A SQL Server retained table exceeds its column-count bound or has no columns.");
            }

            SqlServerRetainedColumnBinding[] columns = catalogColumns
                .Select(CreateColumn)
                .ToArray();
            bool ordinaryTable = IsOrdinaryTable(table);
            string reason =
                BooleanFacet(
                    table,
                    SqlServerCatalogBuilder
                        .RowLevelSecurityFilterFacet)
                    ? SqlServerRetainedAvailabilityReasons
                        .RowLevelSecurity
                    : !BooleanFacet(
                        table,
                        SqlServerCatalogBuilder
                            .RowLevelSecurityInventoryCompleteFacet)
                        ? SqlServerRetainedAvailabilityReasons
                            .RowLevelSecurityInventory
                        : ordinaryTable
                            ? columns.Any(static column =>
                                !column.IsSupported &&
                                column.AvailabilityReason ==
                                SqlServerRetainedAvailabilityReasons
                                    .ScalarType)
                                ? SqlServerRetainedAvailabilityReasons
                                    .ScalarType
                                : columns.Any(static column =>
                                    !column.IsSupported)
                                    ? SqlServerRetainedAvailabilityReasons
                                        .ColumnShape
                                    : SqlServerRetainedAvailabilityReasons
                                        .Available
                            : SqlServerRetainedAvailabilityReasons
                                .TableShape;

            SqlServerRetainedOrderBinding? order = reason ==
                SqlServerRetainedAvailabilityReasons.Available
                    ? SelectOrder(table, columns, catalog, objects)
                    : null;
            if (reason == SqlServerRetainedAvailabilityReasons.Available &&
                order is null)
            {
                reason =
                    SqlServerRetainedAvailabilityReasons.StableOrder;
            }

            bindings.Add(new SqlServerRetainedTableBinding
            {
                CatalogObject = table,
                SqlServerObjectId = PositiveIntFacet(
                    table,
                    "sqlServerObjectId"),
                Columns = Array.AsReadOnly(columns),
                IsAvailable = reason ==
                    SqlServerRetainedAvailabilityReasons.Available,
                AvailabilityReason = reason,
                Order = order,
            });
        }

        return new SqlServerRetainedSourceBinding
        {
            AnalyzerCatalog = catalog,
            Database = database,
            Tables = bindings.AsReadOnly(),
        };
    }

    private static SqlServerRetainedColumnBinding CreateColumn(
        MigrationCatalogObject column)
    {
        bool nullable = BooleanFacet(column, "nullable");
        bool ordinary =
            !BooleanFacet(column, "identity") &&
            !BooleanFacet(column, "rowVersion") &&
            !BooleanFacet(column, "sqlServerUserDefinedType") &&
            !BooleanFacet(column, "sqlServerSparse") &&
            !BooleanFacet(column, "sqlServerColumnSet") &&
            !BooleanFacet(column, "sqlServerHidden") &&
            !BooleanFacet(column, "sqlServerComputed") &&
            !BooleanFacet(column, "sqlServerFileStream") &&
            !BooleanFacet(column, "sqlServerMasked") &&
            Facet(column, "sqlServerEncryptionType") is null &&
            string.Equals(
                Facet(column, "sqlServerXmlCollectionId"),
                "0",
                StringComparison.Ordinal) &&
            string.Equals(
                Facet(column, "sqlServerGeneratedAlwaysType"),
                "NOT_APPLICABLE",
                StringComparison.Ordinal) &&
            !BooleanFacet(column, "hasDefault") &&
            Facet(column, "defaultKind") is null &&
            Facet(column, "defaultExpression") is null;

        string systemType =
            Facet(column, "sqlServerSystemTypeName") ??
            string.Empty;
        SqlServerScalarCodecKind codec = default;
        int? binaryWidth = null;
        bool supportedType = ordinary &&
            SqlServerScalarCodec.TryResolve(
                systemType,
                ByteFacet(column, "sqlServerPrecision"),
                out codec,
                out binaryWidth);
        string reason = !ordinary
            ? SqlServerRetainedAvailabilityReasons.ColumnShape
            : supportedType
                ? SqlServerRetainedAvailabilityReasons.Available
                : SqlServerRetainedAvailabilityReasons.ScalarType;
        return new SqlServerRetainedColumnBinding
        {
            CatalogObject = column,
            SqlServerColumnId = PositiveIntFacet(
                column,
                "sqlServerColumnId"),
            IsSupported = supportedType,
            AvailabilityReason = reason,
            Codec = supportedType ? codec : null,
            BinaryWidth = supportedType ? binaryWidth : null,
            Nullable = nullable,
        };
    }

    private static SqlServerRetainedOrderBinding? SelectOrder(
        MigrationCatalogObject table,
        IReadOnlyList<SqlServerRetainedColumnBinding> columns,
        MigrationCatalog catalog,
        IReadOnlyDictionary<string, MigrationCatalogObject> objects)
    {
        IReadOnlyDictionary<string, SqlServerRetainedColumnBinding>
            columnsById = columns.ToDictionary(
                static item => item.CatalogObject.ObjectId,
                StringComparer.Ordinal);
        MigrationCatalogObject[] keys = catalog.Objects
            .Where(item =>
                item.Kind == MigrationObjectKind.Key &&
                string.Equals(
                    item.ParentObjectId,
                    table.ObjectId,
                    StringComparison.Ordinal))
            .OrderBy(static item =>
                string.Equals(
                    Facet(item, "kind"),
                    "primary",
                    StringComparison.Ordinal)
                    ? 0
                    : 1)
            .ThenBy(static item =>
                IntFacet(item, "sqlServerUniqueIndexId"))
            .ThenBy(static item => item.ObjectId, StringComparer.Ordinal)
            .ToArray();

        foreach (MigrationCatalogObject key in keys)
        {
            string? kind = Facet(key, "kind");
            if (kind is not ("primary" or "unique") ||
                !BooleanFacet(key, "sqlServerMembershipComplete") ||
                !BooleanFacet(key, "sqlServerUnique") ||
                BooleanFacet(key, "sqlServerDisabled") ||
                BooleanFacet(key, "sqlServerHypothetical") ||
                BooleanFacet(key, "sqlServerIgnoreDuplicateKey") ||
                BooleanFacet(key, "sqlServerHasFilter") ||
                BooleanFacet(key, "sqlServerPartitioned") ||
                IntFacet(key, "sqlServerIndexTypeCode") is not (1 or 2))
            {
                continue;
            }

            MigrationObjectReference[] members = key.Members
                .Where(static member =>
                    member.Role ==
                    MigrationObjectReferenceRoles.Column)
                .OrderBy(static member => member.Ordinal)
                .ToArray();
            if (members.Length == 0 ||
                !members.Select(static member => member.Ordinal)
                    .SequenceEqual(Enumerable.Range(0, members.Length)) ||
                members.Select(static member => member.ObjectId)
                    .Distinct(StringComparer.Ordinal).Count() != members.Length)
            {
                continue;
            }

            var ordered = new List<SqlServerRetainedColumnBinding>(
                members.Length);
            bool safe = true;
            foreach (MigrationObjectReference member in members)
            {
                if (!objects.TryGetValue(
                        member.ObjectId,
                        out MigrationCatalogObject? memberObject) ||
                    !string.Equals(
                        memberObject.ParentObjectId,
                        table.ObjectId,
                        StringComparison.Ordinal) ||
                    !columnsById.TryGetValue(
                        member.ObjectId,
                        out SqlServerRetainedColumnBinding? column) ||
                    !column.IsSupported ||
                    column.Nullable ||
                    column.Codec !=
                    SqlServerScalarCodecKind.SignedInteger)
                {
                    safe = false;
                    break;
                }
                ordered.Add(column);
            }
            if (!safe)
                continue;

            return new SqlServerRetainedOrderBinding
            {
                CatalogObject = key,
                Kind = kind,
                Columns = new ReadOnlyCollection<
                    SqlServerRetainedColumnBinding>(ordered),
            };
        }
        return null;
    }

    private static bool IsOrdinaryTable(
        MigrationCatalogObject table) =>
        !BooleanFacet(table, "sqlServerMemoryOptimized") &&
        string.Equals(
            Facet(table, "sqlServerDurability"),
            "SCHEMA_AND_DATA",
            StringComparison.Ordinal) &&
        !BooleanFacet(table, "sqlServerFileTable") &&
        string.Equals(
            Facet(table, "sqlServerTemporalType"),
            "NON_TEMPORAL_TABLE",
            StringComparison.Ordinal) &&
        !BooleanFacet(table, "sqlServerGraphNode") &&
        !BooleanFacet(table, "sqlServerGraphEdge");

    private static string? Facet(
        MigrationCatalogObject item,
        string name) =>
        item.Facets.FirstOrDefault(facet =>
            string.Equals(
                facet.Name,
                name,
                StringComparison.Ordinal))?.Value;

    private static bool BooleanFacet(
        MigrationCatalogObject item,
        string name) =>
        string.Equals(
            Facet(item, name),
            "true",
            StringComparison.Ordinal);

    private static byte ByteFacet(
        MigrationCatalogObject item,
        string name) =>
        byte.TryParse(
            Facet(item, name),
            NumberStyles.None,
            CultureInfo.InvariantCulture,
            out byte value)
            ? value
            : throw new SqlServerMigrationException(
                "The SQL Server catalog contains an invalid scalar facet.");

    private static int PositiveIntFacet(
        MigrationCatalogObject item,
        string name)
    {
        int value = IntFacet(item, name);
        return value > 0
            ? value
            : throw new SqlServerMigrationException(
                "The SQL Server catalog contains a nonpositive identity facet.");
    }

    private static int IntFacet(
        MigrationCatalogObject item,
        string name) =>
        int.TryParse(
            Facet(item, name),
            NumberStyles.None,
            CultureInfo.InvariantCulture,
            out int value)
            ? value
            : throw new SqlServerMigrationException(
                "The SQL Server catalog contains an invalid integer facet.");
}
