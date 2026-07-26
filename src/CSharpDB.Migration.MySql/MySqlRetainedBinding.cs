using System.Collections.ObjectModel;
using System.Globalization;

namespace CSharpDB.Migration.MySql;

internal static class MySqlRetainedAvailabilityReasons
{
    internal const string Available = "available";
    internal const string TableShape = "table-shape-unsupported";
    internal const string ColumnShape = "column-shape-unsupported";
    internal const string ScalarType = "scalar-type-unsupported";
    internal const string StableOrder = "stable-order-key-unavailable";
}

internal sealed record MySqlRetainedColumnBinding
{
    internal required MigrationCatalogObject CatalogObject { get; init; }

    internal required int OrdinalPosition { get; init; }

    internal required bool IsSupported { get; init; }

    internal required string AvailabilityReason { get; init; }

    internal MySqlScalarCodecKind? Codec { get; init; }

    internal bool Nullable { get; init; }
}

internal sealed record MySqlRetainedOrderBinding
{
    internal required MigrationCatalogObject CatalogObject { get; init; }

    internal required string Kind { get; init; }

    internal required IReadOnlyList<MySqlRetainedColumnBinding>
        Columns
    { get; init; }
}

internal sealed record MySqlRetainedTableBinding
{
    internal required MigrationCatalogObject CatalogObject { get; init; }

    internal required IReadOnlyList<MySqlRetainedColumnBinding>
        Columns
    { get; init; }

    internal required bool IsAvailable { get; init; }

    internal required string AvailabilityReason { get; init; }

    internal MySqlRetainedOrderBinding? Order { get; init; }
}

internal sealed record MySqlRetainedSourceBinding
{
    internal required MigrationCatalog AnalyzerCatalog { get; init; }

    internal required MigrationCatalogObject Database { get; init; }

    internal required IReadOnlyList<MySqlRetainedTableBinding>
        Tables
    { get; init; }

    internal IReadOnlyList<MySqlRetainedTableBinding>
        AvailableTables =>
        Tables.Where(static table => table.IsAvailable).ToArray();
}

internal static class MySqlRetainedBinding
{
    internal static MySqlRetainedSourceBinding Create(
        MigrationCatalog catalog,
        MySqlRetainedCaptureOptions options)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        MigrationContractValidator.ValidateCatalog(catalog);
        if (catalog.Source.Kind != MigrationSourceKind.MySql)
        {
            throw new ArgumentException(
                "The retained MySQL binding requires a MySQL catalog.",
                nameof(catalog));
        }

        MigrationCatalogObject database = catalog.Objects.Single(
            static item =>
                item.Kind == MigrationObjectKind.Database);
        if (!string.Equals(
                Facet(database, "mysqlCatalogContract"),
                MySqlCatalogBuilder.CatalogContract,
                StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "The MySQL analyzer catalog contract is unsupported.",
                nameof(catalog));
        }
        if (!BooleanFacet(
                database,
                "mysqlMetadataVisibilityProofAttempted") ||
            !BooleanFacet(
                database,
                "mysqlMetadataVisibilityAccountFormatSupported") ||
            !BooleanFacet(
                database,
                "mysqlMetadataVisibilityGranteeMatched") ||
            !BooleanFacet(
                database,
                "mysqlDirectSchemaSelect"))
        {
            throw new MySqlMigrationException(
                "Retained MySQL capture requires a direct schema-level SELECT grant for the authenticated account.");
        }

        MigrationCatalogObject[] tables = catalog.Objects
            .Where(static item =>
                item.Kind == MigrationObjectKind.Table)
            .OrderBy(static item =>
                item.ObjectId,
                StringComparer.Ordinal)
            .ToArray();
        if (tables.Length > options.MaxTables)
        {
            throw new MySqlRetainedCaptureLimitException(
                "The MySQL retained capture exceeds its table-count bound.");
        }

        IReadOnlyDictionary<string, MigrationCatalogObject> objects =
            catalog.Objects.ToDictionary(
                static item => item.ObjectId,
                StringComparer.Ordinal);
        var bindings = new List<MySqlRetainedTableBinding>(
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
                        "mysqlOrdinalPosition"))
                .ThenBy(static item =>
                    item.ObjectId,
                    StringComparer.Ordinal)
                .ToArray();
            if (catalogColumns.Length == 0 ||
                catalogColumns.Length >
                options.MaxColumnsPerTable)
            {
                throw new MySqlRetainedCaptureLimitException(
                    "A MySQL retained table exceeds its column-count bound or has no columns.");
            }

            MySqlRetainedColumnBinding[] columns =
                catalogColumns.Select(CreateColumn).ToArray();
            string reason = !IsOrdinaryTable(table)
                ? MySqlRetainedAvailabilityReasons.TableShape
                : columns.Any(static column =>
                    !column.IsSupported &&
                    column.AvailabilityReason ==
                    MySqlRetainedAvailabilityReasons.ScalarType)
                    ? MySqlRetainedAvailabilityReasons.ScalarType
                    : columns.Any(static column =>
                        !column.IsSupported)
                        ? MySqlRetainedAvailabilityReasons.ColumnShape
                        : MySqlRetainedAvailabilityReasons.Available;

            MySqlRetainedOrderBinding? order = reason ==
                MySqlRetainedAvailabilityReasons.Available
                    ? SelectOrder(
                        table,
                        columns,
                        catalog,
                        objects)
                    : null;
            if (reason ==
                    MySqlRetainedAvailabilityReasons.Available &&
                order is null)
            {
                reason =
                    MySqlRetainedAvailabilityReasons.StableOrder;
            }

            bindings.Add(new MySqlRetainedTableBinding
            {
                CatalogObject = table,
                Columns = Array.AsReadOnly(columns),
                IsAvailable = reason ==
                    MySqlRetainedAvailabilityReasons.Available,
                AvailabilityReason = reason,
                Order = order,
            });
        }

        return new MySqlRetainedSourceBinding
        {
            AnalyzerCatalog = catalog,
            Database = database,
            Tables = bindings.AsReadOnly(),
        };
    }

    private static MySqlRetainedColumnBinding CreateColumn(
        MigrationCatalogObject column)
    {
        bool ordinary =
            !BooleanFacet(column, "mysqlGenerated") &&
            !BooleanFacet(column, "mysqlInvisible") &&
            !BooleanFacet(column, "mysqlZerofill");
        string dataType =
            Facet(column, "mysqlDataType") ?? string.Empty;
        MySqlScalarCodecKind codec = default;
        bool supportedType = ordinary &&
            MySqlScalarCodec.TryResolve(
                dataType,
                BooleanFacet(column, "mysqlUnsigned"),
                out codec);
        string reason = !ordinary
            ? MySqlRetainedAvailabilityReasons.ColumnShape
            : supportedType
                ? MySqlRetainedAvailabilityReasons.Available
                : MySqlRetainedAvailabilityReasons.ScalarType;
        return new MySqlRetainedColumnBinding
        {
            CatalogObject = column,
            OrdinalPosition = PositiveIntFacet(
                column,
                "mysqlOrdinalPosition"),
            IsSupported = supportedType,
            AvailabilityReason = reason,
            Codec = supportedType ? codec : null,
            Nullable = BooleanFacet(column, "nullable"),
        };
    }

    private static MySqlRetainedOrderBinding? SelectOrder(
        MigrationCatalogObject table,
        IReadOnlyList<MySqlRetainedColumnBinding> columns,
        MigrationCatalog catalog,
        IReadOnlyDictionary<string, MigrationCatalogObject> objects)
    {
        IReadOnlyDictionary<string, MySqlRetainedColumnBinding>
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
                item.ObjectId,
                StringComparer.Ordinal)
            .ToArray();

        foreach (MigrationCatalogObject key in keys)
        {
            string? kind = Facet(key, "kind");
            if (kind is not ("primary" or "unique") ||
                !BooleanFacet(
                    key,
                    "mysqlMembershipComplete") ||
                !BooleanFacet(
                    key,
                    "mysqlBackingIndexMatched"))
            {
                continue;
            }

            MigrationObjectReference[] members = key.Members
                .Where(static member =>
                    member.Role ==
                    MigrationObjectReferenceRoles.Column)
                .OrderBy(static member =>
                    member.Ordinal)
                .ToArray();
            if (members.Length == 0 ||
                !members.Select(static member =>
                        member.Ordinal)
                    .SequenceEqual(
                        Enumerable.Range(0, members.Length)) ||
                members.Select(static member =>
                        member.ObjectId)
                    .Distinct(StringComparer.Ordinal)
                    .Count() != members.Length)
            {
                continue;
            }

            var ordered =
                new List<MySqlRetainedColumnBinding>(
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
                        out MySqlRetainedColumnBinding? column) ||
                    !column.IsSupported ||
                    column.Nullable ||
                    column.Codec is not (
                        MySqlScalarCodecKind.SignedInteger or
                        MySqlScalarCodecKind.UnsignedInteger))
                {
                    safe = false;
                    break;
                }
                ordered.Add(column);
            }
            if (!safe)
                continue;

            return new MySqlRetainedOrderBinding
            {
                CatalogObject = key,
                Kind = kind,
                Columns =
                    new ReadOnlyCollection<
                        MySqlRetainedColumnBinding>(
                        ordered),
            };
        }

        return null;
    }

    private static bool IsOrdinaryTable(
        MigrationCatalogObject table) =>
        string.Equals(
            Facet(table, "mysqlTableType"),
            "BASE TABLE",
            StringComparison.Ordinal) &&
        string.Equals(
            Facet(table, "mysqlEngine"),
            "InnoDB",
            StringComparison.Ordinal) &&
        !BooleanFacet(table, "mysqlPartitioned");

    internal static string? Facet(
        MigrationCatalogObject item,
        string name) =>
        item.Facets.FirstOrDefault(facet =>
            string.Equals(
                facet.Name,
                name,
                StringComparison.Ordinal))?.Value;

    internal static bool BooleanFacet(
        MigrationCatalogObject item,
        string name) =>
        string.Equals(
            Facet(item, name),
            "true",
            StringComparison.Ordinal);

    private static int PositiveIntFacet(
        MigrationCatalogObject item,
        string name)
    {
        if (int.TryParse(
                Facet(item, name),
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out int value) &&
            value > 0)
        {
            return value;
        }

        throw new MySqlMigrationException(
            "The MySQL catalog contains an invalid positive integer facet.");
    }
}
