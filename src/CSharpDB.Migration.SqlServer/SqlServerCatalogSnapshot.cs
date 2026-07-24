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
        IEnumerable<SqlServerColumnMetadata> columns,
        IEnumerable<SqlServerKeyMetadata>? keys = null,
        IEnumerable<SqlServerIndexMetadata>? indexes = null,
        IEnumerable<SqlServerIndexColumnMetadata>? indexColumns = null,
        IEnumerable<SqlServerForeignKeyMetadata>? foreignKeys = null,
        IEnumerable<SqlServerForeignKeyColumnMetadata>? foreignKeyColumns = null,
        IEnumerable<SqlServerCheckMetadata>? checks = null,
        IEnumerable<SqlServerSequenceMetadata>? sequences = null,
        SqlServerPermissionAuditMetadata? permissionAuditBefore = null,
        SqlServerPermissionAuditMetadata? permissionAuditAfter = null)
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
        Keys = Copy(keys);
        Indexes = Copy(indexes);
        IndexColumns = Copy(indexColumns);
        ForeignKeys = Copy(foreignKeys);
        ForeignKeyColumns = Copy(foreignKeyColumns);
        Checks = Copy(checks);
        Sequences = Copy(sequences);
        PermissionAuditBefore = Copy(
            permissionAuditBefore ?? SqlServerPermissionAuditMetadata.NotAttempted);
        PermissionAuditAfter = Copy(
            permissionAuditAfter ?? SqlServerPermissionAuditMetadata.NotAttempted);
    }

    public string EndpointDigest { get; }

    public string ProviderVersion { get; }

    public SqlServerInstanceMetadata Instance { get; }

    public SqlServerDatabaseMetadata Database { get; }

    public IReadOnlyList<SqlServerSchemaMetadata> Schemas { get; }

    public IReadOnlyList<SqlServerTableMetadata> Tables { get; }

    public IReadOnlyList<SqlServerColumnMetadata> Columns { get; }

    public IReadOnlyList<SqlServerKeyMetadata> Keys { get; }

    public IReadOnlyList<SqlServerIndexMetadata> Indexes { get; }

    public IReadOnlyList<SqlServerIndexColumnMetadata> IndexColumns { get; }

    public IReadOnlyList<SqlServerForeignKeyMetadata> ForeignKeys { get; }

    public IReadOnlyList<SqlServerForeignKeyColumnMetadata> ForeignKeyColumns { get; }

    public IReadOnlyList<SqlServerCheckMetadata> Checks { get; }

    public IReadOnlyList<SqlServerSequenceMetadata> Sequences { get; }

    public SqlServerPermissionAuditMetadata PermissionAuditBefore { get; }

    public SqlServerPermissionAuditMetadata PermissionAuditAfter { get; }

    private static IReadOnlyList<T> Copy<T>(IEnumerable<T>? items) =>
        new ReadOnlyCollection<T>((items ?? []).ToArray());

    private static SqlServerPermissionAuditMetadata Copy(
        SqlServerPermissionAuditMetadata audit) =>
        new(
            Copy(audit.Tokens),
            Copy(audit.Denials),
            audit.Attempted);
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
    bool? HasViewDefinition,
    bool? HasViewSecurityDefinition = null);

internal sealed record SqlServerSchemaMetadata(
    int SchemaId,
    string Name,
    bool? HasViewDefinition = null);

internal sealed record SqlServerTableMetadata(
    int ObjectId,
    int SchemaId,
    string Name,
    bool IsMemoryOptimized,
    string Durability,
    bool IsFileTable,
    string TemporalType,
    bool IsNode,
    bool IsEdge,
    bool? HasViewDefinition = null);

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

internal sealed record SqlServerKeyMetadata(
    int ObjectId,
    int ParentObjectId,
    string Name,
    string Type,
    int UniqueIndexId,
    bool IsSystemNamed);

internal sealed record SqlServerIndexMetadata(
    int ObjectId,
    int IndexId,
    string Name,
    byte Type,
    string TypeDescription,
    bool IsUnique,
    int DataSpaceId,
    string? DataSpaceName,
    string? DataSpaceType,
    bool IgnoreDuplicateKey,
    bool IsPrimaryKey,
    bool IsUniqueConstraint,
    byte FillFactor,
    bool IsPadded,
    bool IsDisabled,
    bool IsHypothetical,
    bool AllowRowLocks,
    bool AllowPageLocks,
    bool HasFilter,
    long? FilterDefinitionBytes,
    string? FilterDefinition,
    int? CompressionDelay,
    bool SuppressDuplicateKeyMessages,
    bool OptimizeForSequentialKey);

internal sealed record SqlServerIndexColumnMetadata(
    int ObjectId,
    int IndexId,
    int IndexColumnId,
    int ColumnId,
    byte KeyOrdinal,
    byte PartitionOrdinal,
    bool IsDescending,
    bool IsIncluded);

internal sealed record SqlServerForeignKeyMetadata(
    int ObjectId,
    int ParentObjectId,
    int ReferencedObjectId,
    int KeyIndexId,
    string Name,
    bool IsDisabled,
    bool IsNotForReplication,
    bool IsNotTrusted,
    byte DeleteAction,
    string DeleteActionDescription,
    byte UpdateAction,
    string UpdateActionDescription,
    bool IsSystemNamed);

internal sealed record SqlServerForeignKeyColumnMetadata(
    int ConstraintObjectId,
    int ConstraintColumnId,
    int ParentObjectId,
    int ParentColumnId,
    int ReferencedObjectId,
    int ReferencedColumnId);

internal sealed record SqlServerCheckMetadata(
    int ObjectId,
    int ParentObjectId,
    string Name,
    int ParentColumnId,
    bool IsDisabled,
    bool IsNotForReplication,
    bool IsNotTrusted,
    long? DefinitionBytes,
    string? Definition,
    bool UsesDatabaseCollation,
    bool IsSystemNamed);

internal sealed record SqlServerSequenceMetadata(
    int ObjectId,
    int SchemaId,
    string Name,
    string TypeSchema,
    string TypeName,
    string SystemTypeName,
    byte Precision,
    byte Scale,
    string StartValue,
    string Increment,
    string? MinimumValue,
    string? MaximumValue,
    bool IsCycling,
    bool IsCached,
    int? CacheSize);

internal sealed record SqlServerUserTokenMetadata(
    int PrincipalId,
    string Type,
    string Usage);

internal sealed record SqlServerPermissionDenyMetadata(
    byte Class,
    int MajorId,
    int MinorId,
    string PermissionName,
    int GranteePrincipalId,
    string TokenUsage);

internal sealed record SqlServerPermissionAuditMetadata(
    IReadOnlyList<SqlServerUserTokenMetadata> Tokens,
    IReadOnlyList<SqlServerPermissionDenyMetadata> Denials,
    bool Attempted)
{
    public static SqlServerPermissionAuditMetadata NotAttempted { get; } =
        new(
            Array.Empty<SqlServerUserTokenMetadata>(),
            Array.Empty<SqlServerPermissionDenyMetadata>(),
            Attempted: false);
}
