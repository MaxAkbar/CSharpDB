using CSharpDB.Migration.SqlServer;

namespace CSharpDB.Migration.SqlServer.Tests;

internal static class SqlServerTestSnapshot
{
    public const string SecretDefaultDefinition =
        "((N'Password=NeverPersistThis'))";

    public const string SecretCheckDefinition =
        "([Amount]>(0) AND N'CheckPassword=NeverPersistThis'<>N'')";

    public const string SecretFilterDefinition =
        "([Customer]<>N'FilterPassword=NeverPersistThis')";

    public const string SecretModuleDefinition =
        "CREATE VIEW [dbo].[OrderSummary] AS SELECT [Id], [Amount] " +
        "FROM [dbo].[Orders] WHERE N'ModulePassword=NeverPersistThis'<>N''";

    public const string SecretPartitionBoundary =
        "PartitionPassword=NeverPersistThis";

    public const string SecretPartitionBoundaryHex =
        "50006100720074006900740069006f006e00500061007300730077006f00720064003d004e00650076006500720050006500720073006900730074005400680069007300";

    public static SqlServerCatalogSnapshot Create(
        SqlServerInstanceMetadata? instance = null,
        SqlServerDatabaseMetadata? database = null,
        IEnumerable<SqlServerSchemaMetadata>? schemas = null,
        IEnumerable<SqlServerTableMetadata>? tables = null,
        IEnumerable<SqlServerColumnMetadata>? columns = null,
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
        IEnumerable<SqlServerFullTextIndexColumnMetadata>?
            fullTextIndexColumns = null,
        IEnumerable<SqlServerDataSpaceMetadata>? dataSpaces = null,
        IEnumerable<SqlServerPartitionSchemeMetadata>? partitionSchemes = null,
        IEnumerable<SqlServerPartitionSchemeDestinationMetadata>?
            partitionSchemeDestinations = null,
        IEnumerable<SqlServerPartitionFunctionMetadata>?
            partitionFunctions = null,
        IEnumerable<SqlServerPartitionParameterMetadata>?
            partitionParameters = null,
        IEnumerable<SqlServerPartitionRangeValueMetadata>?
            partitionRangeValues = null,
        IEnumerable<SqlServerIndexPartitionMetadata>? indexPartitions = null)
    {
        bool usesDefaultStructure =
            schemas is null &&
            tables is null &&
            columns is null;
        SqlServerDatabaseMetadata selectedDatabase = database ?? Database();
        SqlServerPermissionAuditMetadata defaultAudit = PermissionAudit();

        return new SqlServerCatalogSnapshot(
            "sha256:" + new string('a', 64),
            "7.0.2",
            instance ?? Instance(),
            selectedDatabase,
            schemas ?? Schemas(),
            tables ?? Tables(),
            columns ?? Columns(),
            keys ?? (usesDefaultStructure ? Keys() : []),
            indexes ?? (usesDefaultStructure ? Indexes() : []),
            indexColumns ?? (usesDefaultStructure ? IndexColumns() : []),
            foreignKeys ?? (usesDefaultStructure ? ForeignKeys() : []),
            foreignKeyColumns ??
                (usesDefaultStructure ? ForeignKeyColumns() : []),
            checks ?? (usesDefaultStructure ? Checks() : []),
            sequences ?? (usesDefaultStructure ? Sequences() : []),
            permissionAuditBefore ?? defaultAudit,
            permissionAuditAfter ?? defaultAudit,
            views ?? (usesDefaultStructure ? Views() : []),
            viewColumns ?? (usesDefaultStructure ? ViewColumns() : []),
            triggers ?? (usesDefaultStructure ? Triggers() : []),
            triggerEvents ?? (usesDefaultStructure ? TriggerEvents() : []),
            routines ?? (usesDefaultStructure ? Routines() : []),
            modules ?? (usesDefaultStructure ? Modules() : []),
            parameters ?? (usesDefaultStructure ? Parameters() : []),
            expressionDependencyAudit ??
                (usesDefaultStructure
                    ? ExpressionDependencyAudit()
                    : selectedDatabase.HasSelectSqlExpressionDependencies == true
                        ? new SqlServerExpressionDependencyAuditMetadata(
                            [],
                            Attempted: true)
                        : SqlServerExpressionDependencyAuditMetadata.NotAttempted),
            fullTextCatalogs ??
                (usesDefaultStructure ? FullTextCatalogs() : []),
            fullTextStoplists ??
                (usesDefaultStructure ? FullTextStoplists() : []),
            searchPropertyLists ??
                (usesDefaultStructure ? SearchPropertyLists() : []),
            fullTextIndexes ??
                (usesDefaultStructure ? FullTextIndexes() : []),
            fullTextIndexColumns ??
                (usesDefaultStructure ? FullTextIndexColumns() : []),
            dataSpaces ?? (usesDefaultStructure ? DataSpaces() : []),
            partitionSchemes ??
                (usesDefaultStructure ? PartitionSchemes() : []),
            partitionSchemeDestinations ??
                (usesDefaultStructure ? PartitionSchemeDestinations() : []),
            partitionFunctions ??
                (usesDefaultStructure ? PartitionFunctions() : []),
            partitionParameters ??
                (usesDefaultStructure ? PartitionParameters() : []),
            partitionRangeValues ??
                (usesDefaultStructure ? PartitionRangeValues() : []),
            indexPartitions ??
                (usesDefaultStructure ? IndexPartitions() : []));
    }

    public static SqlServerInstanceMetadata Instance() =>
        new(
            ProductVersion: "16.0.4175.1",
            ProductMajorVersion: 16,
            ProductLevel: "RTM",
            Edition: "Enterprise Edition",
            EngineEdition: 3);

    public static SqlServerDatabaseMetadata Database() =>
        new(
            DatabaseId: 7,
            Name: "MigrationFixture",
            CompatibilityLevel: 160,
            Collation: "Latin1_General_100_CI_AS_SC_UTF8",
            IsReadCommittedSnapshotOn: true,
            SnapshotIsolationState: "ON",
            IsAutoCreateStatsOn: true,
            IsAutoUpdateStatsOn: true,
            IsAnsiNullDefaultOn: false,
            IsQuotedIdentifierOn: true,
            IsParameterizationForced: false,
            Containment: "NONE",
            IsTrustworthyOn: false,
            IsSysAdmin: true,
            IsDbOwner: false,
            HasControl: false,
            HasViewDefinition: true,
            HasViewSecurityDefinition: true,
            HasSelectSqlExpressionDependencies: true);

    public static IReadOnlyList<SqlServerSchemaMetadata> Schemas() =>
    [
        new(1, "dbo", HasViewDefinition: true),
        new(5, "Sales", HasViewDefinition: true),
    ];

    public static IReadOnlyList<SqlServerTableMetadata> Tables() =>
    [
        OrdinaryTable(100, 1, "Orders", lobDataSpaceId: 1),
        OrdinaryTable(200, 5, "Archive", lobDataSpaceId: 1),
    ];

    public static SqlServerTableMetadata OrdinaryTable(
        int objectId,
        int schemaId,
        string name,
        int lobDataSpaceId = 0,
        int fileStreamDataSpaceId = 0) =>
        new(
            ObjectId: objectId,
            SchemaId: schemaId,
            Name: name,
            IsMemoryOptimized: false,
            Durability: "SCHEMA_AND_DATA",
            IsFileTable: false,
            TemporalType: "NON_TEMPORAL_TABLE",
            IsNode: false,
            IsEdge: false,
            HasViewDefinition: true,
            LobDataSpaceId: lobDataSpaceId,
            FileStreamDataSpaceId: fileStreamDataSpaceId);

    public static IReadOnlyList<SqlServerColumnMetadata> Columns() =>
    [
        Column(
            objectId: 100,
            columnId: 1,
            name: "Id",
            typeName: "int",
            systemTypeName: "int",
            maxLength: 4,
            precision: 10,
            nullable: false,
            identitySeed: "1",
            identityIncrement: "1"),
        Column(
            objectId: 100,
            columnId: 2,
            name: "Amount",
            typeName: "decimal",
            systemTypeName: "decimal",
            maxLength: 9,
            precision: 18,
            scale: 2,
            nullable: false,
            defaultConstraintName: "DF_Orders_Amount",
            defaultDefinition: SecretDefaultDefinition),
        Column(
            objectId: 100,
            columnId: 3,
            name: "Customer",
            typeName: "nvarchar",
            systemTypeName: "nvarchar",
            maxLength: 200,
            precision: 0,
            nullable: false,
            collation: "Latin1_General_100_CI_AS_SC_UTF8"),
        Column(
            objectId: 100,
            columnId: 4,
            name: "ComputedAmount",
            typeName: "decimal",
            systemTypeName: "decimal",
            maxLength: 9,
            precision: 18,
            scale: 2,
            nullable: true,
            isComputed: true,
            computedDefinition: "([Amount]*(2))",
            isPersisted: true),
        Column(
            objectId: 100,
            columnId: 5,
            name: "Version",
            typeName: "timestamp",
            systemTypeName: "timestamp",
            maxLength: 8,
            precision: 0,
            nullable: false),
        Column(
            objectId: 100,
            columnId: 6,
            name: "AliasCode",
            typeSchema: "dbo",
            typeName: "CustomerCode",
            systemTypeName: "nvarchar",
            maxLength: 40,
            precision: 0,
            nullable: false),
        Column(
            objectId: 100,
            columnId: 7,
            name: "XmlPayload",
            typeName: "xml",
            systemTypeName: "xml",
            maxLength: -1,
            precision: 0,
            nullable: true),
        Column(
            objectId: 100,
            columnId: 8,
            name: "OptionalCode",
            typeName: "nvarchar",
            systemTypeName: "nvarchar",
            maxLength: 40,
            precision: 0,
            nullable: true,
            collation: "Latin1_General_100_CI_AS_SC_UTF8"),
        Column(
            objectId: 200,
            columnId: 1,
            name: "ArchiveId",
            typeName: "bigint",
            systemTypeName: "bigint",
            maxLength: 8,
            precision: 19,
            nullable: false),
        Column(
            objectId: 200,
            columnId: 2,
            name: "OrderId",
            typeName: "int",
            systemTypeName: "int",
            maxLength: 4,
            precision: 10,
            nullable: false),
        Column(
            objectId: 200,
            columnId: 3,
            name: "Customer",
            typeName: "nvarchar",
            systemTypeName: "nvarchar",
            maxLength: 200,
            precision: 0,
            nullable: false,
            collation: "Latin1_General_100_CI_AS_SC_UTF8"),
        Column(
            objectId: 200,
            columnId: 4,
            name: "OptionalCode",
            typeName: "nvarchar",
            systemTypeName: "nvarchar",
            maxLength: 40,
            precision: 0,
            nullable: true,
            collation: "Latin1_General_100_CI_AS_SC_UTF8"),
    ];

    public static IReadOnlyList<SqlServerKeyMetadata> Keys() =>
    [
        new(
            ObjectId: 1_000,
            ParentObjectId: 100,
            Name: "PK_Orders",
            Type: "PK",
            UniqueIndexId: 1,
            IsSystemNamed: false),
        new(
            ObjectId: 1_001,
            ParentObjectId: 100,
            Name: "UQ_Orders_OptionalCode",
            Type: "UQ",
            UniqueIndexId: 2,
            IsSystemNamed: false),
    ];

    public static IReadOnlyList<SqlServerIndexMetadata> Indexes() =>
    [
        Index(
            objectId: 100,
            indexId: 1,
            name: "PK_Orders",
            unique: true,
            primaryKey: true,
            dataSpaceId: 1,
            dataSpaceName: "PRIMARY",
            dataSpaceType: "ROWS_FILEGROUP"),
        Index(
            objectId: 100,
            indexId: 2,
            name: "UQ_Orders_OptionalCode",
            unique: true,
            uniqueConstraint: true,
            dataSpaceId: 1,
            dataSpaceName: "PRIMARY",
            dataSpaceType: "ROWS_FILEGROUP"),
        Index(
            objectId: 100,
            indexId: 3,
            name: "IX_Orders_Customer",
            dataSpaceId: 1,
            dataSpaceName: "PRIMARY",
            dataSpaceType: "ROWS_FILEGROUP"),
        Index(
            objectId: 100,
            indexId: 4,
            name: "UX_Orders_Customer",
            unique: true,
            dataSpaceId: 1,
            dataSpaceName: "PRIMARY",
            dataSpaceType: "ROWS_FILEGROUP"),
        Index(
            objectId: 100,
            indexId: 5,
            name: "IX_Orders_Amount_Filtered",
            filterDefinition: SecretFilterDefinition,
            dataSpaceId: 10,
            dataSpaceName: "PS_Orders_Customer",
            dataSpaceType: "PARTITION_SCHEME"),
        Index(
            objectId: 200,
            indexId: 1,
            name: "CX_Archive_ArchiveId",
            type: 1,
            typeDescription: "CLUSTERED",
            dataSpaceId: 1,
            dataSpaceName: "PRIMARY",
            dataSpaceType: "ROWS_FILEGROUP"),
        Index(
            objectId: 5_000,
            indexId: 1,
            name: "CUX_OrderSummary_Id",
            type: 1,
            typeDescription: "CLUSTERED",
            unique: true,
            dataSpaceId: 1,
            dataSpaceName: "PRIMARY",
            dataSpaceType: "ROWS_FILEGROUP"),
    ];

    public static IReadOnlyList<SqlServerIndexColumnMetadata> IndexColumns() =>
    [
        IndexColumn(100, 1, 1, 1, keyOrdinal: 1),
        IndexColumn(100, 1, 2, 3, keyOrdinal: 2),
        IndexColumn(100, 2, 1, 8, keyOrdinal: 1),
        IndexColumn(100, 3, 1, 3, keyOrdinal: 1),
        IndexColumn(100, 4, 1, 3, keyOrdinal: 1),
        IndexColumn(
            100,
            5,
            1,
            2,
            keyOrdinal: 1,
            descending: true,
            partitionOrdinal: 1),
        IndexColumn(
            100,
            5,
            2,
            3,
            keyOrdinal: 0,
            included: true),
        IndexColumn(200, 1, 1, 1, keyOrdinal: 1),
        IndexColumn(5_000, 1, 1, 1, keyOrdinal: 1),
    ];

    public static IReadOnlyList<SqlServerFullTextCatalogMetadata>
        FullTextCatalogs() =>
    [
        new(
            FullTextCatalogId: 1,
            Name: "MigrationSearch",
            IsDefault: true,
            IsAccentSensitivityOn: true,
            DataSpaceId: 1),
    ];

    public static IReadOnlyList<SqlServerFullTextStoplistMetadata>
        FullTextStoplists() =>
    [
        new(
            StoplistId: 10,
            Name: "MigrationStoplist"),
    ];

    public static IReadOnlyList<SqlServerSearchPropertyListMetadata>
        SearchPropertyLists() =>
    [
        new(
            PropertyListId: 20,
            Name: "MigrationProperties"),
    ];

    public static IReadOnlyList<SqlServerFullTextIndexMetadata>
        FullTextIndexes() =>
    [
        new(
            ObjectId: 100,
            UniqueIndexId: 1,
            IndexVersion: null,
            FullTextCatalogId: 1,
            IsEnabled: true,
            ChangeTrackingState: "A",
            ChangeTrackingStateDescription: "AUTO",
            StoplistId: 10,
            DataSpaceId: 1,
            PropertyListId: 20),
    ];

    public static IReadOnlyList<SqlServerFullTextIndexColumnMetadata>
        FullTextIndexColumns() =>
    [
        new(
            ObjectId: 100,
            ColumnId: 3,
            TypeColumnId: null,
            LanguageId: 1033,
            StatisticalSemantics: true),
    ];

    public static IReadOnlyList<SqlServerDataSpaceMetadata> DataSpaces() =>
    [
        new(
            DataSpaceId: 1,
            Name: "PRIMARY",
            Type: "FG",
            TypeDescription: "ROWS_FILEGROUP",
            IsDefault: true,
            IsSystem: false,
            IsReadOnly: false),
        new(
            DataSpaceId: 10,
            Name: "PS_Orders_Customer",
            Type: "PS",
            TypeDescription: "PARTITION_SCHEME",
            IsDefault: false,
            IsSystem: false,
            IsReadOnly: null),
        new(
            DataSpaceId: 11,
            Name: "ARCHIVE",
            Type: "FG",
            TypeDescription: "ROWS_FILEGROUP",
            IsDefault: false,
            IsSystem: false,
            IsReadOnly: true),
    ];

    public static IReadOnlyList<SqlServerPartitionSchemeMetadata>
        PartitionSchemes() =>
    [
        new(
            DataSpaceId: 10,
            FunctionId: 30),
    ];

    public static IReadOnlyList<SqlServerPartitionSchemeDestinationMetadata>
        PartitionSchemeDestinations() =>
    [
        new(
            PartitionSchemeId: 10,
            DestinationId: 1,
            DataSpaceId: 1),
        new(
            PartitionSchemeId: 10,
            DestinationId: 2,
            DataSpaceId: 11),
    ];

    public static IReadOnlyList<SqlServerPartitionFunctionMetadata>
        PartitionFunctions() =>
    [
        new(
            FunctionId: 30,
            Name: "PF_Orders_Customer",
            Fanout: 2,
            BoundaryValueOnRight: true,
            IsSystem: false),
    ];

    public static IReadOnlyList<SqlServerPartitionParameterMetadata>
        PartitionParameters() =>
    [
        new(
            FunctionId: 30,
            ParameterId: 1,
            TypeSchema: "sys",
            TypeName: "nvarchar",
            SystemTypeName: "nvarchar",
            MaxLength: 200,
            Precision: 0,
            Scale: 0,
            Collation: "Latin1_General_100_CI_AS_SC_UTF8"),
    ];

    public static IReadOnlyList<SqlServerPartitionRangeValueMetadata>
        PartitionRangeValues() =>
    [
        new(
            FunctionId: 30,
            BoundaryId: 1,
            ParameterId: 1,
            IsNull: false,
            BaseType: "nvarchar",
            MaxLength: 68,
            Precision: 0,
            Scale: 0,
            Collation: "Latin1_General_100_CI_AS_SC_UTF8",
            ValueBytes: 68,
            ValueHex: SecretPartitionBoundaryHex),
    ];

    public static IReadOnlyList<SqlServerIndexPartitionMetadata>
        IndexPartitions() =>
    [
        IndexPartition(
            100,
            0,
            1,
            definitionDataSpaceId: 10,
            storageDataSpaceId: 1),
        IndexPartition(
            100,
            0,
            2,
            definitionDataSpaceId: 10,
            storageDataSpaceId: 11),
        IndexPartition(
            100,
            1,
            1,
            definitionDataSpaceId: 1,
            storageDataSpaceId: 1),
        IndexPartition(
            100,
            2,
            1,
            definitionDataSpaceId: 1,
            storageDataSpaceId: 1),
        IndexPartition(
            100,
            3,
            1,
            definitionDataSpaceId: 1,
            storageDataSpaceId: 1),
        IndexPartition(
            100,
            4,
            1,
            definitionDataSpaceId: 1,
            storageDataSpaceId: 1),
        IndexPartition(
            100,
            5,
            1,
            dataCompression: 1,
            dataCompressionDescription: "ROW",
            definitionDataSpaceId: 10,
            storageDataSpaceId: 1),
        IndexPartition(
            100,
            5,
            2,
            dataCompression: 2,
            dataCompressionDescription: "PAGE",
            xmlCompression: true,
            xmlCompressionDescription: "ON",
            definitionDataSpaceId: 10,
            storageDataSpaceId: 11),
        IndexPartition(
            200,
            1,
            1,
            definitionDataSpaceId: 1,
            storageDataSpaceId: 1),
        IndexPartition(
            5_000,
            1,
            1,
            definitionDataSpaceId: 1,
            storageDataSpaceId: 1),
    ];

    public static IReadOnlyList<SqlServerForeignKeyMetadata> ForeignKeys() =>
    [
        new(
            ObjectId: 2_000,
            ParentObjectId: 200,
            ReferencedObjectId: 100,
            KeyIndexId: 1,
            Name: "FK_Archive_Orders",
            IsDisabled: false,
            IsNotForReplication: false,
            IsNotTrusted: false,
            DeleteAction: 1,
            DeleteActionDescription: "CASCADE",
            UpdateAction: 0,
            UpdateActionDescription: "NO_ACTION",
            IsSystemNamed: false),
        new(
            ObjectId: 2_001,
            ParentObjectId: 200,
            ReferencedObjectId: 100,
            KeyIndexId: 2,
            Name: "FK_Archive_OptionalCode",
            IsDisabled: true,
            IsNotForReplication: true,
            IsNotTrusted: true,
            DeleteAction: 2,
            DeleteActionDescription: "SET_NULL",
            UpdateAction: 1,
            UpdateActionDescription: "CASCADE",
            IsSystemNamed: false),
        new(
            ObjectId: 2_002,
            ParentObjectId: 200,
            ReferencedObjectId: 100,
            KeyIndexId: 4,
            Name: "FK_Archive_Customer_UX",
            IsDisabled: false,
            IsNotForReplication: false,
            IsNotTrusted: false,
            DeleteAction: 0,
            DeleteActionDescription: "NO_ACTION",
            UpdateAction: 0,
            UpdateActionDescription: "NO_ACTION",
            IsSystemNamed: false),
    ];

    public static IReadOnlyList<SqlServerForeignKeyColumnMetadata>
        ForeignKeyColumns() =>
    [
        new(2_000, 1, 200, 2, 100, 1),
        new(2_000, 2, 200, 3, 100, 3),
        new(2_001, 1, 200, 4, 100, 8),
        new(2_002, 1, 200, 3, 100, 3),
    ];

    public static IReadOnlyList<SqlServerCheckMetadata> Checks() =>
    [
        Check(
            objectId: 3_000,
            parentObjectId: 100,
            name: "CK_Orders_Amount",
            parentColumnId: 2,
            definition: SecretCheckDefinition),
        Check(
            objectId: 3_001,
            parentObjectId: 200,
            name: "CK_Archive_Id",
            parentColumnId: 1,
            definition: "([ArchiveId]>(0))",
            disabled: true,
            notTrusted: true),
    ];

    public static IReadOnlyList<SqlServerSequenceMetadata> Sequences() =>
    [
        new(
            ObjectId: 4_000,
            SchemaId: 5,
            Name: "OrderSequence",
            TypeSchema: "sys",
            TypeName: "bigint",
            SystemTypeName: "bigint",
            Precision: 19,
            Scale: 0,
            StartValue: "100",
            Increment: "5",
            MinimumValue: "1",
            MaximumValue: "9223372036854775807",
            IsCycling: false,
            IsCached: true,
            CacheSize: 50),
        new(
            ObjectId: 4_001,
            SchemaId: 1,
            Name: "DescendingSequence",
            TypeSchema: "sys",
            TypeName: "int",
            SystemTypeName: "int",
            Precision: 10,
            Scale: 0,
            StartValue: "0",
            Increment: "-1",
            MinimumValue: "-2147483648",
            MaximumValue: "0",
            IsCycling: true,
            IsCached: false,
            CacheSize: null),
    ];

    public static IReadOnlyList<SqlServerViewMetadata> Views() =>
    [
        new(
            ObjectId: 5_000,
            SchemaId: 1,
            Name: "OrderSummary",
            IsReplicated: false,
            HasReplicationFilter: false,
            HasOpaqueMetadata: false,
            HasUncheckedAssemblyData: false,
            WithCheckOption: true,
            IsDateCorrelationView: false,
            IsIndexed: true,
            HasViewDefinition: true,
            LedgerViewType: 0,
            LedgerViewTypeDescription: "NON_LEDGER_VIEW",
            IsDroppedLedgerView: false),
    ];

    public static IReadOnlyList<SqlServerViewColumnMetadata> ViewColumns() =>
    [
        new(
            ObjectId: 5_000,
            ColumnId: 1,
            Name: "Id",
            TypeSchema: "sys",
            TypeName: "int",
            SystemTypeName: "int",
            MaxLength: 4,
            Precision: 10,
            Scale: 0,
            Collation: null,
            IsNullable: false,
            IsAnsiPadded: false,
            IsHidden: false,
            IsMasked: false,
            EncryptionType: null,
            IsXmlDocument: false,
            XmlCollectionId: 0),
        new(
            ObjectId: 5_000,
            ColumnId: 2,
            Name: "Amount",
            TypeSchema: "sys",
            TypeName: "decimal",
            SystemTypeName: "decimal",
            MaxLength: 9,
            Precision: 18,
            Scale: 2,
            Collation: null,
            IsNullable: false,
            IsAnsiPadded: false,
            IsHidden: false,
            IsMasked: false,
            EncryptionType: null,
            IsXmlDocument: false,
            XmlCollectionId: 0),
    ];

    public static IReadOnlyList<SqlServerTriggerMetadata> Triggers() =>
    [
        new(
            ObjectId: 6_000,
            SchemaId: 1,
            ParentClass: 1,
            ParentClassDescription: "OBJECT_OR_COLUMN",
            ParentObjectId: 100,
            Name: "TR_Orders_Audit",
            Type: "TR",
            TypeDescription: "SQL_TRIGGER",
            IsDisabled: false,
            IsNotForReplication: false,
            IsInsteadOfTrigger: false,
            IsInsert: true,
            IsUpdate: true,
            IsDelete: false,
            IsFirstInsert: true,
            IsLastInsert: false,
            IsFirstUpdate: false,
            IsLastUpdate: true,
            IsFirstDelete: false,
            IsLastDelete: false,
            HasViewDefinition: true),
        new(
            ObjectId: 6_001,
            SchemaId: null,
            ParentClass: 0,
            ParentClassDescription: "DATABASE",
            ParentObjectId: 0,
            Name: "TR_Database_Ddl",
            Type: "TR",
            TypeDescription: "SQL_TRIGGER",
            IsDisabled: true,
            IsNotForReplication: false,
            IsInsteadOfTrigger: false,
            IsInsert: null,
            IsUpdate: null,
            IsDelete: null,
            IsFirstInsert: null,
            IsLastInsert: null,
            IsFirstUpdate: null,
            IsLastUpdate: null,
            IsFirstDelete: null,
            IsLastDelete: null,
            HasViewDefinition: true),
    ];

    public static IReadOnlyList<SqlServerTriggerEventMetadata> TriggerEvents() =>
    [
        new(
            ObjectId: 6_000,
            Type: 1,
            TypeDescription: "INSERT",
            IsFirst: true,
            IsLast: false,
            EventGroupType: null,
            EventGroupTypeDescription: null),
        new(
            ObjectId: 6_000,
            Type: 2,
            TypeDescription: "UPDATE",
            IsFirst: false,
            IsLast: true,
            EventGroupType: null,
            EventGroupTypeDescription: null),
        new(
            ObjectId: 6_001,
            Type: 101,
            TypeDescription: "CREATE_TABLE",
            IsFirst: false,
            IsLast: false,
            EventGroupType: 100,
            EventGroupTypeDescription: "DDL_TABLE_EVENTS"),
    ];

    public static IReadOnlyList<SqlServerRoutineMetadata> Routines() =>
    [
        Routine(7_000, "usp_CycleA", "P", "SQL_STORED_PROCEDURE"),
        Routine(7_001, "usp_CycleB", "P", "SQL_STORED_PROCEDURE"),
        Routine(
            7_002,
            "ufn_OrderAmount",
            "FN",
            "SQL_SCALAR_FUNCTION",
            procedureFlags: null),
    ];

    public static IReadOnlyList<SqlServerModuleMetadata> Modules() =>
    [
        Module(
            objectId: 5_000,
            name: "OrderSummary",
            objectType: "V",
            objectTypeDescription: "VIEW",
            definition: SecretModuleDefinition,
            isSchemaBound: true),
        Module(
            objectId: 6_000,
            name: "TR_Orders_Audit",
            objectType: "TR",
            objectTypeDescription: "SQL_TRIGGER",
            parentObjectId: 100,
            definition:
                "CREATE TRIGGER [dbo].[TR_Orders_Audit] ON [dbo].[Orders] " +
                "AFTER INSERT, UPDATE AS SELECT 1"),
        Module(
            objectId: 6_001,
            schemaId: 0,
            name: "TR_Database_Ddl",
            objectType: "TR",
            objectTypeDescription: "SQL_TRIGGER",
            definition:
                "CREATE TRIGGER [TR_Database_Ddl] ON DATABASE " +
                "FOR CREATE_TABLE AS SELECT 1",
            isEncrypted: null),
        Module(
            objectId: 7_000,
            name: "usp_CycleA",
            objectType: "P",
            objectTypeDescription: "SQL_STORED_PROCEDURE",
            definition:
                "CREATE PROCEDURE [dbo].[usp_CycleA] AS " +
                "EXEC [dbo].[usp_CycleB]"),
        Module(
            objectId: 7_001,
            name: "usp_CycleB",
            objectType: "P",
            objectTypeDescription: "SQL_STORED_PROCEDURE",
            definition: null,
            isEncrypted: true),
        Module(
            objectId: 7_002,
            name: "ufn_OrderAmount",
            objectType: "FN",
            objectTypeDescription: "SQL_SCALAR_FUNCTION",
            definition:
                "CREATE FUNCTION [dbo].[ufn_OrderAmount](@OrderId int) " +
                "RETURNS int AS BEGIN RETURN @OrderId END",
            nullOnNullInput: true),
    ];

    public static IReadOnlyList<SqlServerParameterMetadata> Parameters() =>
    [
        Parameter(
            objectId: 7_000,
            parameterId: 1,
            name: "@MinimumAmount",
            typeName: "decimal",
            systemTypeName: "decimal",
            maxLength: 9,
            precision: 18,
            scale: 2),
        Parameter(
            objectId: 7_000,
            parameterId: 2,
            name: "@RowsChanged",
            typeName: "int",
            systemTypeName: "int",
            maxLength: 4,
            precision: 10,
            isOutput: true),
        Parameter(
            objectId: 7_002,
            parameterId: 0,
            name: string.Empty,
            typeName: "int",
            systemTypeName: "int",
            maxLength: 4,
            precision: 10,
            isOutput: true),
        Parameter(
            objectId: 7_002,
            parameterId: 1,
            name: "@OrderId",
            typeName: "int",
            systemTypeName: "int",
            maxLength: 4,
            precision: 10),
    ];

    public static SqlServerExpressionDependencyAuditMetadata
        ExpressionDependencyAudit() =>
        new(
            [
                Dependency(
                    referencingId: 5_000,
                    referencedSchemaName: "dbo",
                    referencedEntityName: "Orders",
                    referencedId: 100,
                    referencedMinorId: 1,
                    schemaBound: true),
                Dependency(
                    referencingId: 5_000,
                    referencedSchemaName: "dbo",
                    referencedEntityName: "Orders",
                    referencedId: 100,
                    referencedMinorId: 2,
                    schemaBound: true),
                Dependency(
                    referencingId: 7_000,
                    referencedSchemaName: "dbo",
                    referencedEntityName: "OrderSummary",
                    referencedId: 5_000),
                Dependency(
                    referencingId: 7_000,
                    referencedSchemaName: "dbo",
                    referencedEntityName: "usp_CycleB",
                    referencedId: 7_001),
                Dependency(
                    referencingId: 7_001,
                    referencedSchemaName: "dbo",
                    referencedEntityName: "usp_CycleA",
                    referencedId: 7_000),
                Dependency(
                    referencingId: 7_000,
                    referencedSchemaName: null,
                    referencedEntityName: "MissingRoutine",
                    referencedId: null,
                    callerDependent: true),
                Dependency(
                    referencingId: 7_000,
                    referencedServerName: "ExternalServer",
                    referencedDatabaseName: "ExternalDatabase",
                    referencedSchemaName: "dbo",
                    referencedEntityName: "RemoteRoutine",
                    referencedId: null),
            ],
            Attempted: true);

    public static SqlServerPermissionAuditMetadata PermissionAudit(
        IEnumerable<SqlServerUserTokenMetadata>? tokens = null,
        IEnumerable<SqlServerPermissionDenyMetadata>? denials = null,
        bool attempted = true) =>
        new(
            (tokens ??
             [
                 new SqlServerUserTokenMetadata(
                     0,
                     "SERVER ROLE",
                     "GRANT OR DENY"),
                 new SqlServerUserTokenMetadata(
                     1,
                     "SQL USER",
                     "GRANT OR DENY"),
             ]).ToArray(),
            (denials ?? []).ToArray(),
            Attempted: attempted);

    private static SqlServerRoutineMetadata Routine(
        int objectId,
        string name,
        string type,
        string typeDescription,
        bool? procedureFlags = false) =>
        new(
            ObjectId: objectId,
            SchemaId: 1,
            Name: name,
            Type: type,
            TypeDescription: typeDescription,
            IsAutoExecuted: procedureFlags,
            IsExecutionReplicated: procedureFlags,
            IsReplicationSerializableOnly: procedureFlags,
            SkipsReplicationConstraints: procedureFlags,
            HasViewDefinition: true);

    private static SqlServerModuleMetadata Module(
        int objectId,
        string name,
        string objectType,
        string objectTypeDescription,
        string? definition,
        int schemaId = 1,
        int parentObjectId = 0,
        bool nullOnNullInput = false,
        bool? isEncrypted = false,
        bool isSchemaBound = false) =>
        new(
            ObjectId: objectId,
            SchemaId: schemaId,
            ParentObjectId: parentObjectId,
            Name: name,
            ObjectType: objectType,
            ObjectTypeDescription: objectTypeDescription,
            DefinitionBytes:
                definition is null
                    ? null
                    : checked(definition.Length * 2L),
            Definition: definition,
            UsesAnsiNulls: true,
            UsesQuotedIdentifier: true,
            IsSchemaBound: isSchemaBound,
            UsesDatabaseCollation: false,
            IsRecompiled: false,
            NullOnNullInput: nullOnNullInput,
            ExecuteAsPrincipalId: null,
            UsesNativeCompilation: false,
            IsInlineable: false,
            InlineType: false,
            IsEncrypted: isEncrypted);

    private static SqlServerParameterMetadata Parameter(
        int objectId,
        int parameterId,
        string name,
        string typeName,
        string systemTypeName,
        short maxLength,
        byte precision,
        byte scale = 0,
        bool isOutput = false) =>
        new(
            ObjectId: objectId,
            ParameterId: parameterId,
            Name: name,
            TypeSchema: "sys",
            TypeName: typeName,
            SystemTypeName: systemTypeName,
            MaxLength: maxLength,
            Precision: precision,
            Scale: scale,
            IsOutput: isOutput,
            IsCursorReference: false,
            HasDefaultValue: false,
            IsXmlDocument: false,
            XmlCollectionId: 0,
            IsReadOnly: false,
            IsNullable: true,
            EncryptionType: null,
            IsUserDefined: false,
            IsAssemblyType: false,
            IsTableType: false);

    private static SqlServerExpressionDependencyMetadata Dependency(
        int referencingId,
        string? referencedSchemaName,
        string referencedEntityName,
        int? referencedId,
        int referencedMinorId = 0,
        bool schemaBound = false,
        bool callerDependent = false,
        string? referencedServerName = null,
        string? referencedDatabaseName = null) =>
        new(
            ReferencingId: referencingId,
            ReferencingMinorId: 0,
            ReferencingClass: 1,
            ReferencingClassDescription: "OBJECT_OR_COLUMN",
            IsSchemaBoundReference: schemaBound,
            ReferencedClass: 1,
            ReferencedClassDescription: "OBJECT_OR_COLUMN",
            ReferencedServerName: referencedServerName,
            ReferencedDatabaseName: referencedDatabaseName,
            ReferencedSchemaName: referencedSchemaName,
            ReferencedEntityName: referencedEntityName,
            ReferencedId: referencedId,
            ReferencedMinorId: referencedMinorId,
            IsCallerDependent: callerDependent,
            IsAmbiguous: false);

    public static SqlServerCatalogSnapshot CreateSupportedRelational()
    {
        IReadOnlyList<SqlServerColumnMetadata> columns =
        [
            Column(100, 1, "TenantId", "int", "int", 4, 10, nullable: false),
            Column(100, 2, "OrderId", "int", "int", 4, 10, nullable: false),
            Column(200, 1, "TenantId", "int", "int", 4, 10, nullable: false),
            Column(200, 2, "OrderId", "int", "int", 4, 10, nullable: false),
        ];
        IReadOnlyList<SqlServerIndexMetadata> indexes =
        [
            Index(
                objectId: 100,
                indexId: 1,
                name: "PK_Orders",
                unique: true,
                primaryKey: true),
            Index(
                objectId: 200,
                indexId: 1,
                name: "IX_Archive_OrderId"),
        ];

        return Create(
            schemas: [new SqlServerSchemaMetadata(1, "dbo", true)],
            tables:
            [
                OrdinaryTable(100, 1, "Orders"),
                OrdinaryTable(200, 1, "Archive"),
            ],
            columns: columns,
            keys:
            [
                new SqlServerKeyMetadata(
                    1_000,
                    100,
                    "PK_Orders",
                    "PK",
                    1,
                    false),
            ],
            indexes: indexes,
            indexColumns:
            [
                IndexColumn(100, 1, 1, 1, keyOrdinal: 1),
                IndexColumn(100, 1, 2, 2, keyOrdinal: 2),
                IndexColumn(200, 1, 1, 2, keyOrdinal: 1),
                IndexColumn(200, 1, 2, 0, keyOrdinal: 0),
            ],
            foreignKeys:
            [
                new SqlServerForeignKeyMetadata(
                    2_000,
                    200,
                    100,
                    1,
                    "FK_Archive_Orders",
                    false,
                    false,
                    false,
                    1,
                    "CASCADE",
                    0,
                    "NO_ACTION",
                    false),
            ],
            foreignKeyColumns:
            [
                new SqlServerForeignKeyColumnMetadata(
                    2_000,
                    1,
                    200,
                    1,
                    100,
                    1),
                new SqlServerForeignKeyColumnMetadata(
                    2_000,
                    2,
                    200,
                    2,
                    100,
                    2),
            ],
            checks: [],
            sequences: []);
    }

    public static SqlServerIndexMetadata Index(
        int objectId,
        int indexId,
        string name,
        byte type = 2,
        string typeDescription = "NONCLUSTERED",
        bool unique = false,
        bool primaryKey = false,
        bool uniqueConstraint = false,
        bool disabled = false,
        bool hypothetical = false,
        bool ignoreDuplicateKey = false,
        string? filterDefinition = null,
        int dataSpaceId = 0,
        string? dataSpaceName = null,
        string? dataSpaceType = null) =>
        new(
            ObjectId: objectId,
            IndexId: indexId,
            Name: name,
            Type: type,
            TypeDescription: typeDescription,
            IsUnique: unique,
            DataSpaceId: dataSpaceId,
            DataSpaceName: dataSpaceName,
            DataSpaceType: dataSpaceType,
            IgnoreDuplicateKey: ignoreDuplicateKey,
            IsPrimaryKey: primaryKey,
            IsUniqueConstraint: uniqueConstraint,
            FillFactor: 0,
            IsPadded: false,
            IsDisabled: disabled,
            IsHypothetical: hypothetical,
            AllowRowLocks: true,
            AllowPageLocks: true,
            HasFilter: filterDefinition is not null,
            FilterDefinitionBytes:
                filterDefinition is null
                    ? null
                    : checked(filterDefinition.Length * 2L),
            FilterDefinition: filterDefinition,
            CompressionDelay: null,
            SuppressDuplicateKeyMessages: false,
            OptimizeForSequentialKey: false);

    public static SqlServerIndexColumnMetadata IndexColumn(
        int objectId,
        int indexId,
        int indexColumnId,
        int columnId,
        byte keyOrdinal,
        bool descending = false,
        bool included = false,
        byte partitionOrdinal = 0) =>
        new(
            ObjectId: objectId,
            IndexId: indexId,
            IndexColumnId: indexColumnId,
            ColumnId: columnId,
            KeyOrdinal: keyOrdinal,
            PartitionOrdinal: partitionOrdinal,
            IsDescending: descending,
            IsIncluded: included);

    public static SqlServerIndexPartitionMetadata IndexPartition(
        int objectId,
        int indexId,
        int partitionNumber,
        byte dataCompression = 0,
        string dataCompressionDescription = "NONE",
        bool? xmlCompression = false,
        string? xmlCompressionDescription = "OFF",
        int? definitionDataSpaceId = null,
        int? storageDataSpaceId = null) =>
        new(
            ObjectId: objectId,
            IndexId: indexId,
            PartitionNumber: partitionNumber,
            DataCompression: dataCompression,
            DataCompressionDescription: dataCompressionDescription,
            XmlCompression: xmlCompression,
            XmlCompressionDescription: xmlCompressionDescription,
            DefinitionDataSpaceId: definitionDataSpaceId,
            StorageDataSpaceId: storageDataSpaceId);

    public static SqlServerCheckMetadata Check(
        int objectId,
        int parentObjectId,
        string name,
        int parentColumnId,
        string? definition,
        bool disabled = false,
        bool notForReplication = false,
        bool notTrusted = false) =>
        new(
            ObjectId: objectId,
            ParentObjectId: parentObjectId,
            Name: name,
            ParentColumnId: parentColumnId,
            IsDisabled: disabled,
            IsNotForReplication: notForReplication,
            IsNotTrusted: notTrusted,
            DefinitionBytes:
                definition is null
                    ? null
                    : checked(definition.Length * 2L),
            Definition: definition,
            UsesDatabaseCollation: false,
            IsSystemNamed: false);

    public static SqlServerColumnMetadata Column(
        int objectId,
        int columnId,
        string name,
        string typeName,
        string systemTypeName,
        short maxLength,
        byte precision,
        byte scale = 0,
        bool nullable = true,
        string typeSchema = "sys",
        string? collation = null,
        bool isSparse = false,
        bool isColumnSet = false,
        bool isHidden = false,
        bool isComputed = false,
        bool isFileStream = false,
        bool isMasked = false,
        string? encryptionType = null,
        int xmlCollectionId = 0,
        string generatedAlwaysType = "NOT_APPLICABLE",
        bool? hasDefault = null,
        string? defaultConstraintName = null,
        long? defaultDefinitionBytes = null,
        string? defaultDefinition = null,
        long? computedDefinitionBytes = null,
        string? computedDefinition = null,
        bool isPersisted = false,
        bool? isIdentity = null,
        string? identitySeed = null,
        string? identityIncrement = null,
        bool identityNotForReplication = false) =>
        new(
            ObjectId: objectId,
            ColumnId: columnId,
            Name: name,
            TypeSchema: typeSchema,
            TypeName: typeName,
            SystemTypeName: systemTypeName,
            MaxLength: maxLength,
            Precision: precision,
            Scale: scale,
            Collation: collation,
            IsNullable: nullable,
            IsSparse: isSparse,
            IsColumnSet: isColumnSet,
            IsHidden: isHidden,
            IsComputed: isComputed,
            IsFileStream: isFileStream,
            IsMasked: isMasked,
            EncryptionType: encryptionType,
            XmlCollectionId: xmlCollectionId,
            GeneratedAlwaysType: generatedAlwaysType,
            HasDefault:
                hasDefault ??
                (defaultConstraintName is not null ||
                 defaultDefinition is not null),
            DefaultConstraintName: defaultConstraintName,
            DefaultDefinitionBytes:
                defaultDefinition is null
                    ? defaultDefinitionBytes
                    : defaultDefinitionBytes ?? checked(defaultDefinition.Length * 2L),
            DefaultDefinition: defaultDefinition,
            ComputedDefinitionBytes:
                computedDefinition is null
                    ? computedDefinitionBytes
                    : computedDefinitionBytes ?? checked(computedDefinition.Length * 2L),
            ComputedDefinition: computedDefinition,
            IsPersisted: isPersisted,
            IsIdentity:
                isIdentity ??
                (identitySeed is not null ||
                 identityIncrement is not null),
            IdentitySeed: identitySeed,
            IdentityIncrement: identityIncrement,
            IdentityNotForReplication: identityNotForReplication);
}
