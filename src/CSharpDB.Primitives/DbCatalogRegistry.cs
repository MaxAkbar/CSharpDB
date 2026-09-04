using System.Collections.ObjectModel;

namespace CSharpDB.Primitives;

/// <summary>
/// Describes how an internal physical table name is matched.
/// </summary>
public enum DbInternalTableMatchKind
{
    Exact,
    Prefix,
}

/// <summary>
/// Describes a database table that is owned by CSharpDB or one of its features.
/// </summary>
/// <param name="Pattern">The exact table name or table-name prefix.</param>
/// <param name="MatchKind">How <paramref name="Pattern"/> is matched.</param>
/// <param name="Owner">The feature that owns the table.</param>
/// <param name="LogicalReplacement">The preferred logical catalog, when one exists.</param>
/// <param name="HideFromClientMetadata">Whether normal client table listings omit the table.</param>
/// <param name="HideFromSystemCatalog">Whether <c>sys.tables</c> and related catalogs omit the table.</param>
public sealed record DbInternalTableDescriptor(
    string Pattern,
    DbInternalTableMatchKind MatchKind,
    string Owner,
    string? LogicalReplacement = null,
    bool HideFromClientMetadata = true,
    bool HideFromSystemCatalog = true)
{
    public bool IsMatch(string? tableName)
    {
        if (string.IsNullOrEmpty(tableName))
            return false;

        return MatchKind switch
        {
            DbInternalTableMatchKind.Exact =>
                string.Equals(tableName, Pattern, StringComparison.OrdinalIgnoreCase),
            DbInternalTableMatchKind.Prefix =>
                tableName.StartsWith(Pattern, StringComparison.OrdinalIgnoreCase),
            _ => false,
        };
    }
}

/// <summary>
/// Shared classification and visibility policy for internal physical tables.
/// Only registered exact names and prefixes are internal. An otherwise unknown
/// underscore-prefixed table remains a user table until its owning feature is
/// added to this registry.
/// </summary>
public static class DbInternalTableRegistry
{
    private static readonly DbInternalTableDescriptor[] s_descriptorValues =
    [
        new("__procedures", DbInternalTableMatchKind.Exact, "Stored Procedures", "Procedures",
            HideFromClientMetadata: true, HideFromSystemCatalog: false),
        new("__saved_queries", DbInternalTableMatchKind.Exact, "Saved Queries", "sys.saved_queries",
            HideFromClientMetadata: true, HideFromSystemCatalog: false),
        new("__external_tables", DbInternalTableMatchKind.Exact, "External Tables", "sys.external_tables",
            HideFromClientMetadata: true, HideFromSystemCatalog: true),
        new("__data_model_diagrams", DbInternalTableMatchKind.Exact, "Data Modeler", "sys.diagrams",
            HideFromClientMetadata: true, HideFromSystemCatalog: true),
        new("__validation_rules", DbInternalTableMatchKind.Exact, "Data Hygiene", "sys.validation_rules",
            HideFromClientMetadata: false, HideFromSystemCatalog: true),

        new("__forms", DbInternalTableMatchKind.Exact, "Admin Forms",
            HideFromClientMetadata: false, HideFromSystemCatalog: false),
        new("__reports", DbInternalTableMatchKind.Exact, "Admin Reports",
            HideFromClientMetadata: false, HideFromSystemCatalog: false),
        new("__report_definition_chunks", DbInternalTableMatchKind.Exact, "Admin Reports",
            HideFromClientMetadata: false, HideFromSystemCatalog: false),
        new("__code_modules", DbInternalTableMatchKind.Exact, "Code Modules",
            HideFromClientMetadata: false, HideFromSystemCatalog: false),
        new("__code_module_builds", DbInternalTableMatchKind.Exact, "Code Modules",
            HideFromClientMetadata: false, HideFromSystemCatalog: false),
        new("__EFMigrationsHistory", DbInternalTableMatchKind.Exact, "Entity Framework Core",
            HideFromClientMetadata: false, HideFromSystemCatalog: false),
        new("__EFMigrationsLock", DbInternalTableMatchKind.Exact, "Entity Framework Core",
            HideFromClientMetadata: false, HideFromSystemCatalog: false),
        new("__csharpdb_type_probe", DbInternalTableMatchKind.Exact, "Admin Schema",
            HideFromClientMetadata: false, HideFromSystemCatalog: false),

        new("_col_", DbInternalTableMatchKind.Prefix, "Collections", "Collections",
            HideFromClientMetadata: true, HideFromSystemCatalog: false),
        new("_etl_", DbInternalTableMatchKind.Prefix, "ETL Pipelines",
            HideFromClientMetadata: false, HideFromSystemCatalog: false),
        new("_shard_", DbInternalTableMatchKind.Prefix, "Sharding",
            HideFromClientMetadata: false, HideFromSystemCatalog: false),
        new("__migrate_", DbInternalTableMatchKind.Prefix, "Foreign Key Migration",
            HideFromClientMetadata: false, HideFromSystemCatalog: false),
        new("__csharpdb_restore_", DbInternalTableMatchKind.Prefix, "Archive Restore",
            HideFromClientMetadata: false, HideFromSystemCatalog: false),
        new("__csharpdb_migration_", DbInternalTableMatchKind.Prefix, "CSharpDB Migration",
            HideFromClientMetadata: false, HideFromSystemCatalog: false),
    ];

    public static IReadOnlyList<DbInternalTableDescriptor> Descriptors { get; } =
        Array.AsReadOnly(s_descriptorValues);

    public static bool TryGet(string? tableName, out DbInternalTableDescriptor descriptor)
    {
        foreach (DbInternalTableDescriptor candidate in s_descriptorValues)
        {
            if (!candidate.IsMatch(tableName))
                continue;

            descriptor = candidate;
            return true;
        }

        descriptor = null!;
        return false;
    }

    public static bool IsInternalTable(string? tableName) =>
        TryGet(tableName, out _);

    /// <summary>
    /// Returns whether a table name occupies CSharpDB's reserved double-underscore
    /// namespace, even when no owning feature is registered for that exact name.
    /// Reserved-but-unregistered names are not product-owned internal tables and
    /// therefore do not appear in <c>sys.internal_tables</c>.
    /// </summary>
    public static bool IsReservedInternalTableName(string? tableName) =>
        tableName?.StartsWith("__", StringComparison.Ordinal) == true;

    public static bool IsHiddenFromClientMetadata(string? tableName) =>
        TryGet(tableName, out DbInternalTableDescriptor descriptor)
        && descriptor.HideFromClientMetadata;

    public static bool IsHiddenFromSystemCatalog(string? tableName) =>
        TryGet(tableName, out DbInternalTableDescriptor descriptor)
        && descriptor.HideFromSystemCatalog;

    public static string? ResolveLogicalReplacement(string? tableName)
    {
        if (!TryGet(tableName, out DbInternalTableDescriptor descriptor))
            return null;

        if (descriptor.MatchKind == DbInternalTableMatchKind.Prefix
            && string.Equals(descriptor.Pattern, "_col_", StringComparison.OrdinalIgnoreCase)
            && tableName!.Length > descriptor.Pattern.Length)
        {
            return "collection:" + tableName[descriptor.Pattern.Length..];
        }

        return descriptor.LogicalReplacement;
    }
}

/// <summary>
/// Describes a read-only virtual system catalog and its default Admin query.
/// </summary>
public sealed record DbSystemCatalogDescriptor(
    string Name,
    string Description,
    string DefaultSql);

/// <summary>
/// Canonical inventory of CSharpDB virtual system catalogs.
/// </summary>
public static class DbSystemCatalogRegistry
{
    private static readonly DbSystemCatalogDescriptor[] s_descriptorValues =
    [
        new("sys.tables", "tables", "SELECT * FROM sys.tables ORDER BY table_name;"),
        new("sys.columns", "columns", "SELECT * FROM sys.columns ORDER BY table_name, ordinal_position;"),
        new("sys.indexes", "indexes", "SELECT * FROM sys.indexes ORDER BY table_name, index_name, ordinal_position;"),
        new("sys.foreign_keys", "foreign keys", "SELECT * FROM sys.foreign_keys ORDER BY table_name, constraint_name, ordinal_position;"),
        new("sys.key_constraints", "primary and unique keys", "SELECT * FROM sys.key_constraints ORDER BY table_name, constraint_name, ordinal_position;"),
        new("sys.check_constraints", "check constraints", "SELECT * FROM sys.check_constraints ORDER BY table_name, constraint_name;"),
        new("sys.functions", "functions", "SELECT * FROM sys.functions ORDER BY canonical_name, signature;"),
        new("sys.views", "views", "SELECT * FROM sys.views ORDER BY view_name;"),
        new("sys.triggers", "triggers", "SELECT * FROM sys.triggers ORDER BY trigger_name;"),
        new("sys.objects", "all objects", "SELECT * FROM sys.objects ORDER BY object_type, object_name;"),
        new("sys.saved_queries", "saved queries", "SELECT * FROM sys.saved_queries ORDER BY name;"),
        new("sys.external_tables", "external tables", "SELECT * FROM sys.external_tables ORDER BY table_name;"),
        new("sys.diagrams", "diagrams", "SELECT * FROM sys.diagrams ORDER BY name;"),
        new("sys.validation_rules", "validation rules", "SELECT * FROM sys.validation_rules ORDER BY table_name, column_name, rule_name;"),
        new("sys.temp_tables", "temporary tables", "SELECT * FROM sys.temp_tables ORDER BY table_name;"),
        new("sys.temp_columns", "temporary columns", "SELECT * FROM sys.temp_columns ORDER BY table_name, ordinal_position;"),
        new("sys.table_stats", "table stats", "SELECT * FROM sys.table_stats ORDER BY table_name;"),
        new("sys.column_stats", "column stats", "SELECT * FROM sys.column_stats ORDER BY table_name, ordinal_position;"),
        new("sys.planner_histograms", "planner histograms", "SELECT * FROM sys.planner_histograms ORDER BY table_name, ordinal_position, bucket_index;"),
        new("sys.planner_heavy_hitters", "planner heavy hitters", "SELECT * FROM sys.planner_heavy_hitters ORDER BY table_name, ordinal_position, row_count DESC;"),
        new("sys.planner_index_prefix_stats", "planner prefix stats", "SELECT * FROM sys.planner_index_prefix_stats ORDER BY table_name, index_name, prefix_length;"),
        new("sys.internal_tables", "internal storage", "SELECT * FROM sys.internal_tables ORDER BY table_name;"),
    ];

    private static readonly IReadOnlyDictionary<string, DbSystemCatalogDescriptor> s_byName =
        BuildNameLookup();

    private static readonly IReadOnlyDictionary<string, string> s_underscoredAliases =
        BuildAliasLookup();

    private static readonly IReadOnlyList<string> s_canonicalNames =
        Array.AsReadOnly(s_descriptorValues.Select(static descriptor => descriptor.Name).ToArray());

    public static IReadOnlyList<DbSystemCatalogDescriptor> Descriptors { get; } =
        Array.AsReadOnly(s_descriptorValues);

    public static IReadOnlyList<DbSystemCatalogDescriptor> Catalogs => Descriptors;

    public static IReadOnlyList<string> CanonicalNames => s_canonicalNames;

    /// <summary>
    /// Maps each supported underscored alias to its canonical dotted name.
    /// </summary>
    public static IReadOnlyDictionary<string, string> UnderscoredAliases => s_underscoredAliases;

    public static bool TryNormalize(string? tableName, out string normalizedName)
    {
        if (!string.IsNullOrEmpty(tableName)
            && s_byName.TryGetValue(tableName, out DbSystemCatalogDescriptor? descriptor))
        {
            normalizedName = descriptor.Name;
            return true;
        }

        normalizedName = string.Empty;
        return false;
    }

    public static bool TryGet(string? tableName, out DbSystemCatalogDescriptor descriptor)
    {
        if (!string.IsNullOrEmpty(tableName)
            && s_byName.TryGetValue(tableName, out DbSystemCatalogDescriptor? match))
        {
            descriptor = match;
            return true;
        }

        descriptor = null!;
        return false;
    }

    public static string GetUnderscoredAlias(string canonicalName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(canonicalName);
        if (!canonicalName.StartsWith("sys.", StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException("A canonical system catalog name must start with 'sys.'.", nameof(canonicalName));

        return "sys_" + canonicalName[4..];
    }

    private static IReadOnlyDictionary<string, DbSystemCatalogDescriptor> BuildNameLookup()
    {
        var lookup = new Dictionary<string, DbSystemCatalogDescriptor>(StringComparer.OrdinalIgnoreCase);
        foreach (DbSystemCatalogDescriptor descriptor in s_descriptorValues)
        {
            if (!lookup.TryAdd(descriptor.Name, descriptor))
                throw new InvalidOperationException($"Duplicate system catalog name '{descriptor.Name}'.");

            string alias = GetUnderscoredAlias(descriptor.Name);
            if (!lookup.TryAdd(alias, descriptor))
                throw new InvalidOperationException($"Duplicate system catalog alias '{alias}'.");
        }

        return new ReadOnlyDictionary<string, DbSystemCatalogDescriptor>(lookup);
    }

    private static IReadOnlyDictionary<string, string> BuildAliasLookup()
    {
        var aliases = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (DbSystemCatalogDescriptor descriptor in s_descriptorValues)
            aliases.Add(GetUnderscoredAlias(descriptor.Name), descriptor.Name);

        return new ReadOnlyDictionary<string, string>(aliases);
    }
}
