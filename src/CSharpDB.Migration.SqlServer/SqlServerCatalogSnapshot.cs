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
        SqlServerPermissionAuditMetadata? permissionAuditAfter = null,
        IEnumerable<SqlServerViewMetadata>? views = null,
        IEnumerable<SqlServerViewColumnMetadata>? viewColumns = null,
        IEnumerable<SqlServerTriggerMetadata>? triggers = null,
        IEnumerable<SqlServerTriggerEventMetadata>? triggerEvents = null,
        IEnumerable<SqlServerRoutineMetadata>? routines = null,
        IEnumerable<SqlServerModuleMetadata>? modules = null,
        IEnumerable<SqlServerParameterMetadata>? parameters = null,
        SqlServerExpressionDependencyAuditMetadata? expressionDependencyAudit = null,
        IEnumerable<SqlServerFullTextCatalogMetadata>? fullTextCatalogs = null,
        IEnumerable<SqlServerFullTextStoplistMetadata>? fullTextStoplists = null,
        IEnumerable<SqlServerSearchPropertyListMetadata>? searchPropertyLists = null,
        IEnumerable<SqlServerFullTextIndexMetadata>? fullTextIndexes = null,
        IEnumerable<SqlServerFullTextIndexColumnMetadata>? fullTextIndexColumns = null,
        IEnumerable<SqlServerDataSpaceMetadata>? dataSpaces = null,
        IEnumerable<SqlServerPartitionSchemeMetadata>? partitionSchemes = null,
        IEnumerable<SqlServerPartitionSchemeDestinationMetadata>?
            partitionSchemeDestinations = null,
        IEnumerable<SqlServerPartitionFunctionMetadata>? partitionFunctions = null,
        IEnumerable<SqlServerPartitionParameterMetadata>? partitionParameters = null,
        IEnumerable<SqlServerPartitionRangeValueMetadata>? partitionRangeValues = null,
        IEnumerable<SqlServerIndexPartitionMetadata>? indexPartitions = null,
        IEnumerable<SqlServerXmlIndexMetadata>? xmlIndexes = null,
        IEnumerable<SqlServerSelectiveXmlIndexPathMetadata>?
            selectiveXmlIndexPaths = null,
        IEnumerable<SqlServerSpatialIndexMetadata>? spatialIndexes = null,
        IEnumerable<SqlServerSpatialIndexTessellationMetadata>?
            spatialIndexTessellations = null,
        IEnumerable<SqlServerHashIndexMetadata>? hashIndexes = null,
        IEnumerable<SqlServerJsonIndexMetadata>? jsonIndexes = null,
        IEnumerable<SqlServerJsonIndexPathMetadata>? jsonIndexPaths = null)
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
        Views = Copy(views);
        ViewColumns = Copy(viewColumns);
        Triggers = Copy(triggers);
        TriggerEvents = Copy(triggerEvents);
        Routines = Copy(routines);
        Modules = Copy(modules);
        Parameters = Copy(parameters);
        ExpressionDependencyAudit = Copy(
            expressionDependencyAudit ??
            SqlServerExpressionDependencyAuditMetadata.NotAttempted);
        FullTextCatalogs = Copy(fullTextCatalogs);
        FullTextStoplists = Copy(fullTextStoplists);
        SearchPropertyLists = Copy(searchPropertyLists);
        FullTextIndexes = Copy(fullTextIndexes);
        FullTextIndexColumns = Copy(fullTextIndexColumns);
        DataSpaces = Copy(dataSpaces);
        PartitionSchemes = Copy(partitionSchemes);
        PartitionSchemeDestinations = Copy(partitionSchemeDestinations);
        PartitionFunctions = Copy(partitionFunctions);
        PartitionParameters = Copy(partitionParameters);
        PartitionRangeValues = Copy(partitionRangeValues);
        IndexPartitions = Copy(indexPartitions);
        XmlIndexes = Copy(xmlIndexes);
        SelectiveXmlIndexPaths = Copy(selectiveXmlIndexPaths);
        SpatialIndexes = Copy(spatialIndexes);
        SpatialIndexTessellations = Copy(spatialIndexTessellations);
        HashIndexes = Copy(hashIndexes);
        JsonIndexes = Copy(jsonIndexes);
        JsonIndexPaths = Copy(jsonIndexPaths);
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

    public IReadOnlyList<SqlServerViewMetadata> Views { get; }

    public IReadOnlyList<SqlServerViewColumnMetadata> ViewColumns { get; }

    public IReadOnlyList<SqlServerTriggerMetadata> Triggers { get; }

    public IReadOnlyList<SqlServerTriggerEventMetadata> TriggerEvents { get; }

    public IReadOnlyList<SqlServerRoutineMetadata> Routines { get; }

    public IReadOnlyList<SqlServerModuleMetadata> Modules { get; }

    public IReadOnlyList<SqlServerParameterMetadata> Parameters { get; }

    public SqlServerExpressionDependencyAuditMetadata ExpressionDependencyAudit { get; }

    public IReadOnlyList<SqlServerFullTextCatalogMetadata> FullTextCatalogs { get; }

    public IReadOnlyList<SqlServerFullTextStoplistMetadata> FullTextStoplists { get; }

    public IReadOnlyList<SqlServerSearchPropertyListMetadata> SearchPropertyLists { get; }

    public IReadOnlyList<SqlServerFullTextIndexMetadata> FullTextIndexes { get; }

    public IReadOnlyList<SqlServerFullTextIndexColumnMetadata> FullTextIndexColumns { get; }

    public IReadOnlyList<SqlServerDataSpaceMetadata> DataSpaces { get; }

    public IReadOnlyList<SqlServerPartitionSchemeMetadata> PartitionSchemes { get; }

    public IReadOnlyList<SqlServerPartitionSchemeDestinationMetadata>
        PartitionSchemeDestinations
    { get; }

    public IReadOnlyList<SqlServerPartitionFunctionMetadata> PartitionFunctions { get; }

    public IReadOnlyList<SqlServerPartitionParameterMetadata> PartitionParameters { get; }

    public IReadOnlyList<SqlServerPartitionRangeValueMetadata> PartitionRangeValues { get; }

    public IReadOnlyList<SqlServerIndexPartitionMetadata> IndexPartitions { get; }

    public IReadOnlyList<SqlServerXmlIndexMetadata> XmlIndexes { get; }

    public IReadOnlyList<SqlServerSelectiveXmlIndexPathMetadata>
        SelectiveXmlIndexPaths
    { get; }

    public IReadOnlyList<SqlServerSpatialIndexMetadata> SpatialIndexes { get; }

    public IReadOnlyList<SqlServerSpatialIndexTessellationMetadata>
        SpatialIndexTessellations
    { get; }

    public IReadOnlyList<SqlServerHashIndexMetadata> HashIndexes { get; }

    public IReadOnlyList<SqlServerJsonIndexMetadata> JsonIndexes { get; }

    public IReadOnlyList<SqlServerJsonIndexPathMetadata> JsonIndexPaths { get; }

    private static IReadOnlyList<T> Copy<T>(IEnumerable<T>? items) =>
        new ReadOnlyCollection<T>((items ?? []).ToArray());

    private static SqlServerPermissionAuditMetadata Copy(
        SqlServerPermissionAuditMetadata audit) =>
        new(
            Copy(audit.Tokens),
            Copy(audit.Denials),
            audit.Attempted);

    private static SqlServerExpressionDependencyAuditMetadata Copy(
        SqlServerExpressionDependencyAuditMetadata audit) =>
        new(
            Copy(audit.Dependencies),
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
    bool? HasViewSecurityDefinition = null,
    bool? HasSelectSqlExpressionDependencies = null);

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
    bool? HasViewDefinition = null,
    int LobDataSpaceId = 0,
    int FileStreamDataSpaceId = 0);

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
    bool IsIncluded,
    byte? ColumnStoreOrderOrdinal = null,
    byte? DataClusteringOrdinal = null);

internal sealed record SqlServerXmlIndexMetadata(
    int ObjectId,
    int IndexId,
    int? UsingXmlIndexId,
    string? SecondaryType,
    string? SecondaryTypeDescription,
    byte XmlIndexType,
    string XmlIndexTypeDescription,
    int? PathId);

internal sealed record SqlServerSelectiveXmlIndexPathMetadata(
    int ObjectId,
    int IndexId,
    int PathId,
    int PathBytes,
    string Path,
    string Name,
    byte PathType,
    string PathTypeDescription,
    int? XmlComponentId,
    string? XQueryTypeDescription,
    bool? IsXQueryTypeInferred,
    short? XQueryMaximumLength,
    bool? IsXQueryMaximumLengthInferred,
    bool? IsNode,
    byte? SystemTypeId,
    int? UserTypeId,
    short? MaxLength,
    byte? Precision,
    byte? Scale,
    string? Collation,
    bool? IsSingleton);

internal sealed record SqlServerSpatialIndexMetadata(
    int ObjectId,
    int IndexId,
    byte SpatialIndexType,
    string SpatialIndexTypeDescription,
    string TessellationScheme);

internal sealed record SqlServerSpatialIndexTessellationMetadata(
    int ObjectId,
    int IndexId,
    string TessellationScheme,
    double? BoundingBoxXMin,
    double? BoundingBoxYMin,
    double? BoundingBoxXMax,
    double? BoundingBoxYMax,
    short? Level1Grid,
    string? Level1GridDescription,
    short? Level2Grid,
    string? Level2GridDescription,
    short? Level3Grid,
    string? Level3GridDescription,
    short? Level4Grid,
    string? Level4GridDescription,
    int? CellsPerObject);

internal sealed record SqlServerHashIndexMetadata(
    int ObjectId,
    int IndexId,
    int BucketCount);

internal sealed record SqlServerJsonIndexMetadata(
    int ObjectId,
    int IndexId,
    bool OptimizeForArraySearch);

internal sealed record SqlServerJsonIndexPathMetadata(
    int ObjectId,
    int IndexId,
    int PathOrdinal,
    int PathBytes,
    string Path);

internal sealed record SqlServerFullTextCatalogMetadata(
    int FullTextCatalogId,
    string Name,
    bool IsDefault,
    bool IsAccentSensitivityOn,
    int DataSpaceId);

internal sealed record SqlServerFullTextStoplistMetadata(
    int StoplistId,
    string Name);

internal sealed record SqlServerSearchPropertyListMetadata(
    int PropertyListId,
    string Name);

internal sealed record SqlServerFullTextIndexMetadata(
    int ObjectId,
    int UniqueIndexId,
    int? IndexVersion,
    int FullTextCatalogId,
    bool IsEnabled,
    string ChangeTrackingState,
    string ChangeTrackingStateDescription,
    int? StoplistId,
    int DataSpaceId,
    int? PropertyListId);

internal sealed record SqlServerFullTextIndexColumnMetadata(
    int ObjectId,
    int ColumnId,
    int? TypeColumnId,
    int LanguageId,
    bool StatisticalSemantics);

internal sealed record SqlServerDataSpaceMetadata(
    int DataSpaceId,
    string Name,
    string Type,
    string TypeDescription,
    bool IsDefault,
    bool IsSystem,
    bool? IsReadOnly);

internal sealed record SqlServerPartitionSchemeMetadata(
    int DataSpaceId,
    int FunctionId);

internal sealed record SqlServerPartitionSchemeDestinationMetadata(
    int PartitionSchemeId,
    int DestinationId,
    int DataSpaceId);

internal sealed record SqlServerPartitionFunctionMetadata(
    int FunctionId,
    string Name,
    int Fanout,
    bool BoundaryValueOnRight,
    bool IsSystem);

internal sealed record SqlServerPartitionParameterMetadata(
    int FunctionId,
    int ParameterId,
    string TypeSchema,
    string TypeName,
    string SystemTypeName,
    short MaxLength,
    byte Precision,
    byte Scale,
    string? Collation);

internal sealed record SqlServerPartitionRangeValueMetadata(
    int FunctionId,
    int BoundaryId,
    int ParameterId,
    bool IsNull,
    string? BaseType,
    int? MaxLength,
    byte? Precision,
    byte? Scale,
    string? Collation,
    int? ValueBytes,
    string? ValueHex);

internal sealed record SqlServerIndexPartitionMetadata(
    int ObjectId,
    int IndexId,
    int PartitionNumber,
    byte DataCompression,
    string DataCompressionDescription,
    bool? XmlCompression,
    string? XmlCompressionDescription,
    int? DefinitionDataSpaceId,
    int? StorageDataSpaceId);

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

internal sealed record SqlServerViewMetadata(
    int ObjectId,
    int SchemaId,
    string Name,
    bool IsReplicated,
    bool HasReplicationFilter,
    bool HasOpaqueMetadata,
    bool HasUncheckedAssemblyData,
    bool WithCheckOption,
    bool IsDateCorrelationView,
    bool IsIndexed,
    bool? HasViewDefinition,
    byte? LedgerViewType,
    string? LedgerViewTypeDescription,
    bool? IsDroppedLedgerView);

internal sealed record SqlServerViewColumnMetadata(
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
    bool IsAnsiPadded,
    bool IsHidden,
    bool IsMasked,
    string? EncryptionType,
    bool IsXmlDocument,
    int XmlCollectionId);

internal sealed record SqlServerTriggerMetadata(
    int ObjectId,
    int? SchemaId,
    byte ParentClass,
    string ParentClassDescription,
    int ParentObjectId,
    string Name,
    string Type,
    string TypeDescription,
    bool IsDisabled,
    bool IsNotForReplication,
    bool IsInsteadOfTrigger,
    bool? IsInsert,
    bool? IsUpdate,
    bool? IsDelete,
    bool? IsFirstInsert,
    bool? IsLastInsert,
    bool? IsFirstUpdate,
    bool? IsLastUpdate,
    bool? IsFirstDelete,
    bool? IsLastDelete,
    bool? HasViewDefinition);

internal sealed record SqlServerTriggerEventMetadata(
    int ObjectId,
    int Type,
    string TypeDescription,
    bool IsFirst,
    bool IsLast,
    int? EventGroupType,
    string? EventGroupTypeDescription);

internal sealed record SqlServerRoutineMetadata(
    int ObjectId,
    int SchemaId,
    string Name,
    string Type,
    string TypeDescription,
    bool? IsAutoExecuted,
    bool? IsExecutionReplicated,
    bool? IsReplicationSerializableOnly,
    bool? SkipsReplicationConstraints,
    bool? HasViewDefinition);

internal sealed record SqlServerModuleMetadata(
    int ObjectId,
    int SchemaId,
    int ParentObjectId,
    string Name,
    string ObjectType,
    string ObjectTypeDescription,
    long? DefinitionBytes,
    string? Definition,
    bool UsesAnsiNulls,
    bool UsesQuotedIdentifier,
    bool IsSchemaBound,
    bool UsesDatabaseCollation,
    bool IsRecompiled,
    bool NullOnNullInput,
    int? ExecuteAsPrincipalId,
    bool UsesNativeCompilation,
    bool IsInlineable,
    bool InlineType,
    bool? IsEncrypted);

internal sealed record SqlServerParameterMetadata(
    int ObjectId,
    int ParameterId,
    string Name,
    string TypeSchema,
    string TypeName,
    string SystemTypeName,
    short MaxLength,
    byte Precision,
    byte Scale,
    bool IsOutput,
    bool IsCursorReference,
    bool HasDefaultValue,
    bool IsXmlDocument,
    int XmlCollectionId,
    bool IsReadOnly,
    bool IsNullable,
    string? EncryptionType,
    bool IsUserDefined,
    bool IsAssemblyType,
    bool IsTableType);

internal sealed record SqlServerExpressionDependencyMetadata(
    int ReferencingId,
    int ReferencingMinorId,
    byte ReferencingClass,
    string ReferencingClassDescription,
    bool IsSchemaBoundReference,
    byte ReferencedClass,
    string ReferencedClassDescription,
    string? ReferencedServerName,
    string? ReferencedDatabaseName,
    string? ReferencedSchemaName,
    string ReferencedEntityName,
    int? ReferencedId,
    int ReferencedMinorId,
    bool IsCallerDependent,
    bool IsAmbiguous);

internal sealed record SqlServerExpressionDependencyAuditMetadata(
    IReadOnlyList<SqlServerExpressionDependencyMetadata> Dependencies,
    bool Attempted)
{
    public static SqlServerExpressionDependencyAuditMetadata NotAttempted { get; } =
        new(
            Array.Empty<SqlServerExpressionDependencyMetadata>(),
            Attempted: false);
}

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
