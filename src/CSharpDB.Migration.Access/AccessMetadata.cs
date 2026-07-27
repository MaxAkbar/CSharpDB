using System.Collections.ObjectModel;
using System.Data.OleDb;
using System.Runtime.Versioning;

namespace CSharpDB.Migration.Access;

internal sealed record AccessInspectionLimits
{
    internal static AccessInspectionLimits Default { get; } =
        new();

    internal int MaxSchemaObjects { get; init; } =
        32_768;

    internal int MaxTables { get; init; } = 8_192;

    internal int MaxColumnsPerTable { get; init; } =
        2_048;

    internal int MaxIndexesPerTable { get; init; } =
        2_048;

    internal int MaxKeyColumns { get; init; } = 64;

    internal int MaxForeignKeys { get; init; } =
        16_384;

    internal int MaxForeignKeyColumns { get; init; } =
        65_536;

    internal long MaxCatalogTextBytes { get; init; } =
        64L * 1024 * 1024;

    internal void Validate()
    {
        if (MaxSchemaObjects <= 0 ||
            MaxTables <= 0 ||
            MaxColumnsPerTable <= 0 ||
            MaxIndexesPerTable <= 0 ||
            MaxKeyColumns <= 0 ||
            MaxForeignKeys <= 0 ||
            MaxForeignKeyColumns <= 0 ||
            MaxCatalogTextBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(AccessInspectionLimits));
        }
    }
}

internal sealed record AccessCatalogSnapshot
{
    internal required string SourceContentDigest
    {
        get;
        init;
    }

    internal required string ProviderId { get; init; }

    internal required string ProviderVersion { get; init; }

    internal required string SourceVersion { get; init; }

    internal required string SourceName { get; init; }

    internal required string SourceExtension { get; init; }

    internal IReadOnlyList<AccessTableMetadata> Tables
    {
        get;
        init;
    } = [];

    internal IReadOnlyList<AccessSchemaObjectMetadata>
        UnsupportedObjects
    { get; init; } = [];

    internal IReadOnlyList<AccessForeignKeyMetadata>
        ForeignKeys
    { get; init; } = [];
}

internal sealed record AccessSchemaObjectMetadata(
    string Name,
    string Type);

internal sealed record AccessTableMetadata
{
    internal required string Name { get; init; }

    internal IReadOnlyList<AccessColumnMetadata> Columns
    {
        get;
        init;
    } = [];

    internal IReadOnlyList<string> PrimaryKeyColumns
    {
        get;
        init;
    } = [];

    internal IReadOnlyList<AccessIndexMetadata> Indexes
    {
        get;
        init;
    } = [];
}

internal sealed record AccessColumnMetadata
{
    internal required string Name { get; init; }

    internal int Ordinal { get; init; }

    internal OleDbType ProviderType { get; init; }

    internal bool Nullable { get; init; }

    internal long? MaximumLength { get; init; }

    internal int? Precision { get; init; }

    internal int? Scale { get; init; }

    internal bool HasDefault { get; init; }

    internal string? DefaultDigest { get; init; }
}

internal sealed record AccessIndexMetadata
{
    internal required string Name { get; init; }

    internal bool Unique { get; init; }

    internal bool Primary { get; init; }

    internal IReadOnlyList<string> Columns { get; init; } =
        [];
}

internal sealed record AccessForeignKeyColumnMetadata(
    string SourceColumn,
    string ReferencedColumn,
    int Ordinal);

internal sealed record AccessForeignKeyMetadata
{
    internal required string Name { get; init; }

    internal required string SourceTable { get; init; }

    internal required string ReferencedTable { get; init; }

    internal string? ReferencedKeyName { get; init; }

    internal required string UpdateRule { get; init; }

    internal required string DeleteRule { get; init; }

    internal IReadOnlyList<
        AccessForeignKeyColumnMetadata> Columns
    { get; init; } = [];
}

internal enum AccessScalarCodecKind
{
    SignedInteger,
    UnsignedInteger,
    Boolean,
    Decimal,
    FloatingPoint,
    Text,
    Binary,
    Guid,
    DateTime,
}

internal readonly record struct AccessTypeSemantics(
    string LogicalType,
    AccessScalarCodecKind Codec);

[SupportedOSPlatform("windows")]
internal static class AccessTypeCatalog
{
    internal static bool TryResolve(
        OleDbType providerType,
        out AccessTypeSemantics semantics)
    {
        switch (providerType)
        {
            case OleDbType.TinyInt:
            case OleDbType.SmallInt:
            case OleDbType.Integer:
            case OleDbType.BigInt:
                semantics = new(
                    "SignedInteger",
                    AccessScalarCodecKind.SignedInteger);
                return true;
            case OleDbType.UnsignedTinyInt:
            case OleDbType.UnsignedSmallInt:
            case OleDbType.UnsignedInt:
            case OleDbType.UnsignedBigInt:
                semantics = new(
                    "UnsignedInteger",
                    AccessScalarCodecKind.UnsignedInteger);
                return true;
            case OleDbType.Boolean:
                semantics = new(
                    "Boolean",
                    AccessScalarCodecKind.Boolean);
                return true;
            case OleDbType.Currency:
            case OleDbType.Decimal:
            case OleDbType.Numeric:
            case OleDbType.VarNumeric:
                semantics = new(
                    "Decimal",
                    AccessScalarCodecKind.Decimal);
                return true;
            case OleDbType.Single:
            case OleDbType.Double:
                semantics = new(
                    "FloatingPoint",
                    AccessScalarCodecKind.FloatingPoint);
                return true;
            case OleDbType.BSTR:
            case OleDbType.Char:
            case OleDbType.VarChar:
            case OleDbType.LongVarChar:
            case OleDbType.WChar:
            case OleDbType.VarWChar:
            case OleDbType.LongVarWChar:
                semantics = new(
                    "Text",
                    AccessScalarCodecKind.Text);
                return true;
            case OleDbType.Binary:
            case OleDbType.VarBinary:
            case OleDbType.LongVarBinary:
                semantics = new(
                    "Binary",
                    AccessScalarCodecKind.Binary);
                return true;
            case OleDbType.Guid:
                semantics = new(
                    "Guid",
                    AccessScalarCodecKind.Guid);
                return true;
            case OleDbType.Date:
            case OleDbType.DBDate:
            case OleDbType.DBTime:
            case OleDbType.DBTimeStamp:
            case OleDbType.Filetime:
                semantics = new(
                    "DateTime",
                    AccessScalarCodecKind.DateTime);
                return true;
            default:
                semantics = default;
                return false;
        }
    }

    internal static string NativeType(
        OleDbType providerType) =>
        "access:" + providerType;
}

internal sealed record AccessColumnBinding
{
    internal required AccessColumnMetadata Metadata
    {
        get;
        init;
    }

    internal required CSharpDB.Migration.MigrationCatalogObject
        CatalogObject
    { get; init; }

    internal AccessScalarCodecKind? Codec { get; init; }

    internal bool IsSupported => Codec.HasValue;
}

internal sealed record AccessTableBinding
{
    internal required AccessTableMetadata Metadata
    {
        get;
        init;
    }

    internal required CSharpDB.Migration.MigrationCatalogObject
        CatalogObject
    { get; init; }

    internal IReadOnlyList<AccessColumnBinding> Columns
    {
        get;
        init;
    } = [];

    internal IReadOnlyList<AccessColumnBinding>
        PrimaryKeyColumns
    { get; init; } = [];

    internal bool IsDataAvailable =>
        PrimaryKeyColumns.Count > 0 &&
        Columns.Count > 0 &&
        Columns.All(static column =>
            column.IsSupported) &&
        PrimaryKeyColumns.All(static column =>
            column.IsSupported);
}

internal sealed record AccessCatalogBinding
{
    internal required Migration.MigrationCatalog Catalog
    {
        get;
        init;
    }

    internal required Migration.MigrationCatalogObject Database
    {
        get;
        init;
    }

    internal IReadOnlyList<AccessTableBinding> Tables
    {
        get;
        init;
    } = [];

    internal IReadOnlyList<AccessTableBinding>
        AvailableTables =>
        new ReadOnlyCollection<AccessTableBinding>(
            Tables.Where(static table =>
                    table.IsDataAvailable)
                .ToArray());
}
