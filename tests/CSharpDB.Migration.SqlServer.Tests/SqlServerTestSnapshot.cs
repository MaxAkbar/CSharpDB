using CSharpDB.Migration.SqlServer;

namespace CSharpDB.Migration.SqlServer.Tests;

internal static class SqlServerTestSnapshot
{
    public const string SecretDefaultDefinition =
        "((N'Password=NeverPersistThis'))";

    public static SqlServerCatalogSnapshot Create(
        SqlServerInstanceMetadata? instance = null,
        SqlServerDatabaseMetadata? database = null,
        IEnumerable<SqlServerSchemaMetadata>? schemas = null,
        IEnumerable<SqlServerTableMetadata>? tables = null,
        IEnumerable<SqlServerColumnMetadata>? columns = null) =>
        new(
            "sha256:" + new string('a', 64),
            "7.0.2",
            instance ?? Instance(),
            database ?? Database(),
            schemas ?? Schemas(),
            tables ?? Tables(),
            columns ?? Columns());

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
            HasViewDefinition: false);

    public static IReadOnlyList<SqlServerSchemaMetadata> Schemas() =>
    [
        new(1, "dbo"),
        new(5, "Sales"),
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
            IsEdge: false);

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
            objectId: 200,
            columnId: 1,
            name: "ArchiveId",
            typeName: "bigint",
            systemTypeName: "bigint",
            maxLength: 8,
            precision: 19,
            nullable: false),
    ];

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
