using System.Collections.ObjectModel;

namespace CSharpDB.Migration.SqlServer;

/// <summary>
/// One immutable, value-only view of the metadata read from SQL Server. The
/// production reader and deterministic tests meet at this seam.
/// </summary>
internal sealed class SqlServerCatalogSnapshot
{
    public SqlServerCatalogSnapshot(
        string endpointDigest,
        string providerVersion,
        SqlServerInstanceMetadata instance,
        SqlServerDatabaseMetadata database,
        IEnumerable<SqlServerSchemaMetadata> schemas,
        IEnumerable<SqlServerTableMetadata> tables,
        IEnumerable<SqlServerColumnMetadata> columns)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(endpointDigest);
        ArgumentException.ThrowIfNullOrWhiteSpace(providerVersion);
        ArgumentNullException.ThrowIfNull(instance);
        ArgumentNullException.ThrowIfNull(database);
        ArgumentNullException.ThrowIfNull(schemas);
        ArgumentNullException.ThrowIfNull(tables);
        ArgumentNullException.ThrowIfNull(columns);

        EndpointDigest = endpointDigest;
        ProviderVersion = providerVersion;
        Instance = instance;
        Database = database;
        Schemas = new ReadOnlyCollection<SqlServerSchemaMetadata>(schemas.ToArray());
        Tables = new ReadOnlyCollection<SqlServerTableMetadata>(tables.ToArray());
        Columns = new ReadOnlyCollection<SqlServerColumnMetadata>(columns.ToArray());
    }

    public string EndpointDigest { get; }

    public string ProviderVersion { get; }

    public SqlServerInstanceMetadata Instance { get; }

    public SqlServerDatabaseMetadata Database { get; }

    public IReadOnlyList<SqlServerSchemaMetadata> Schemas { get; }

    public IReadOnlyList<SqlServerTableMetadata> Tables { get; }

    public IReadOnlyList<SqlServerColumnMetadata> Columns { get; }
}

internal sealed record SqlServerInstanceMetadata(
    string ProductVersion,
    int ProductMajorVersion,
    string ProductLevel,
    string Edition,
    int EngineEdition);

internal sealed record SqlServerDatabaseMetadata(
    int DatabaseId,
    string Name,
    short CompatibilityLevel,
    string? Collation,
    bool IsReadCommittedSnapshotOn,
    string SnapshotIsolationState,
    bool IsAutoCreateStatsOn,
    bool IsAutoUpdateStatsOn,
    bool IsAnsiNullDefaultOn,
    bool IsQuotedIdentifierOn,
    bool IsParameterizationForced,
    string Containment,
    bool IsTrustworthyOn,
    bool? IsSysAdmin,
    bool? IsDbOwner,
    bool? HasControl,
    bool? HasViewDefinition);

internal sealed record SqlServerSchemaMetadata(
    int SchemaId,
    string Name);

internal sealed record SqlServerTableMetadata(
    int ObjectId,
    int SchemaId,
    string Name,
    bool IsMemoryOptimized,
    string Durability,
    bool IsFileTable,
    string TemporalType,
    bool IsNode,
    bool IsEdge);

internal sealed record SqlServerColumnMetadata(
    int ObjectId,
    int ColumnId,
    string Name,
    string TypeSchema,
    string TypeName,
    string SystemTypeName,
    short MaxLength,
    byte Precision,
    byte Scale,
    string? Collation,
    bool IsNullable,
    bool IsSparse,
    bool IsColumnSet,
    bool IsHidden,
    bool IsComputed,
    bool IsFileStream,
    bool IsMasked,
    string? EncryptionType,
    int XmlCollectionId,
    string GeneratedAlwaysType,
    bool HasDefault,
    string? DefaultConstraintName,
    long? DefaultDefinitionBytes,
    string? DefaultDefinition,
    long? ComputedDefinitionBytes,
    string? ComputedDefinition,
    bool IsPersisted,
    bool IsIdentity,
    string? IdentitySeed,
    string? IdentityIncrement,
    bool IdentityNotForReplication);
