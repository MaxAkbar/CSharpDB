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
        SqlServerPermissionAuditMetadata? permissionAuditAfter = null)
    {
        bool usesDefaultStructure =
            schemas is null &&
            tables is null &&
            columns is null;
        SqlServerPermissionAuditMetadata defaultAudit = PermissionAudit();

        return new SqlServerCatalogSnapshot(
            "sha256:" + new string('a', 64),
            "7.0.2",
            instance ?? Instance(),
            database ?? Database(),
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
            permissionAuditAfter ?? defaultAudit);
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
            HasViewSecurityDefinition: true);

    public static IReadOnlyList<SqlServerSchemaMetadata> Schemas() =>
    [
        new(1, "dbo", HasViewDefinition: true),
        new(5, "Sales", HasViewDefinition: true),
    ];

    public static IReadOnlyList<SqlServerTableMetadata> Tables() =>
    [
        OrdinaryTable(100, 1, "Orders"),
        OrdinaryTable(200, 5, "Archive"),
    ];

    public static SqlServerTableMetadata OrdinaryTable(
        int objectId,
        int schemaId,
        string name) =>
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
            HasViewDefinition: true);

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
            primaryKey: true),
        Index(
            objectId: 100,
            indexId: 2,
            name: "UQ_Orders_OptionalCode",
            unique: true,
            uniqueConstraint: true),
        Index(
            objectId: 100,
            indexId: 3,
            name: "IX_Orders_Customer"),
        Index(
            objectId: 100,
            indexId: 4,
            name: "UX_Orders_Customer",
            unique: true),
        Index(
            objectId: 100,
            indexId: 5,
            name: "IX_Orders_Amount_Filtered",
            filterDefinition: SecretFilterDefinition),
        Index(
            objectId: 200,
            indexId: 1,
            name: "CX_Archive_ArchiveId",
            type: 1,
            typeDescription: "CLUSTERED"),
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
            descending: true),
        IndexColumn(
            100,
            5,
            2,
            3,
            keyOrdinal: 0,
            included: true),
        IndexColumn(200, 1, 1, 1, keyOrdinal: 1),
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
        string? filterDefinition = null) =>
        new(
            ObjectId: objectId,
            IndexId: indexId,
            Name: name,
            Type: type,
            TypeDescription: typeDescription,
            IsUnique: unique,
            DataSpaceId: 1,
            DataSpaceName: "PRIMARY",
            DataSpaceType: "ROWS_FILEGROUP",
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
        bool included = false) =>
        new(
            ObjectId: objectId,
            IndexId: indexId,
            IndexColumnId: indexColumnId,
            ColumnId: columnId,
            KeyOrdinal: keyOrdinal,
            PartitionOrdinal: 0,
            IsDescending: descending,
            IsIncluded: included);

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
