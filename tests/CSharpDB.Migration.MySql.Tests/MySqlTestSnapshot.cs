using CSharpDB.Migration.MySql;

namespace CSharpDB.Migration.MySql.Tests;

internal static class MySqlTestSnapshot
{
    public const string SecretGenerationExpression =
        "(`Amount` * 2 /* NeverPersistThisMySqlExpression */)";
    public const string SecretCheckClause =
        "(`Amount` > 0 /* NeverPersistThisMySqlCheck */)";
    public const string SecretFunctionalIndexExpression =
        "(lower(`Customer`) /* NeverPersistThisMySqlIndexExpression */)";
    public const string SecretShowCreateMarker =
        "NeverPersistThisMySqlShowCreate";
    public const string SecretDefinerIdentity =
        "NeverPersistUser@NeverPersistHost";
    public const string SecretDefaultValue =
        "NeverPersistThisMySqlDefault";
    public const string SecretViewDefinition =
        "select `Orders`.`Id` AS `Id` from `SourceDb`.`Orders` " +
        "/* NeverPersistThisMySqlView */";
    public const string SecretTriggerStatement =
        "SET NEW.`Customer` = CONCAT(NEW.`Customer`, " +
        "'NeverPersistThisMySqlTrigger')";
    public const string SecretProcedureDefinition =
        "BEGIN SELECT 'NeverPersistThisMySqlProcedure'; END";
    public const string SecretFunctionDefinition =
        "RETURN CONCAT(value, 'NeverPersistThisMySqlFunction:" +
        SecretDefinerIdentity + "')";

    public static MySqlCatalogSnapshot Create(
        MySqlServerMetadata? server = null,
        MySqlSessionMetadata? session = null,
        MySqlDatabaseMetadata? database = null,
        IEnumerable<MySqlTableMetadata>? tables = null,
        IEnumerable<MySqlColumnMetadata>? columns = null,
        IEnumerable<MySqlTableDefinitionMetadata>? tableDefinitions = null,
        IEnumerable<MySqlKeyMetadata>? keys = null,
        IEnumerable<MySqlKeyColumnMetadata>? keyColumns = null,
        IEnumerable<MySqlForeignKeyMetadata>? foreignKeys = null,
        IEnumerable<MySqlForeignKeyColumnMetadata>? foreignKeyColumns = null,
        IEnumerable<MySqlCheckMetadata>? checks = null,
        IEnumerable<MySqlIndexMetadata>? indexes = null,
        IEnumerable<MySqlIndexPartMetadata>? indexParts = null,
        IEnumerable<MySqlViewMetadata>? views = null,
        IEnumerable<MySqlViewColumnMetadata>? viewColumns = null,
        IEnumerable<MySqlTriggerMetadata>? triggers = null,
        IEnumerable<MySqlRoutineMetadata>? routines = null,
        IEnumerable<MySqlRoutineParameterMetadata>? routineParameters = null,
        MySqlMetadataVisibilityProof? metadataVisibilityProof = null)
    {
        IReadOnlyList<MySqlTableMetadata> resolvedTables =
            tables?.ToArray() ?? Tables();
        IReadOnlyList<MySqlColumnMetadata> resolvedColumns =
            columns?.ToArray() ?? Columns();
        IReadOnlyList<MySqlViewMetadata> resolvedViews =
            views?.ToArray() ?? [];
        bool usesDefaultStructure = tables is null && columns is null;
        MySqlDatabaseMetadata resolvedDatabase =
            database ?? Database() with { ViewCount = resolvedViews.Count };

        return new(
            endpointDigest:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            providerVersion: "2.6.1",
            server ?? Server(),
            session ?? Session(),
            resolvedDatabase,
            resolvedTables,
            resolvedColumns,
            tableDefinitions ?? TableDefinitions(resolvedTables),
            keys ?? (usesDefaultStructure ? Keys() : []),
            keyColumns ?? (usesDefaultStructure ? KeyColumns() : []),
            foreignKeys ?? (usesDefaultStructure ? ForeignKeys() : []),
            foreignKeyColumns ??
                (usesDefaultStructure ? ForeignKeyColumns() : []),
            checks ?? (usesDefaultStructure ? Checks() : []),
            indexes ?? (usesDefaultStructure ? Indexes() : []),
            indexParts ?? (usesDefaultStructure ? IndexParts() : []),
            resolvedViews,
            viewColumns ?? [],
            triggers ?? [],
            routines ?? [],
            routineParameters ?? [],
            metadataVisibilityProof);
    }

    public static MySqlMetadataVisibilityProof
        MetadataVisibilityProof(
        bool select = true,
        bool showView = true,
        bool trigger = true,
        bool execute = true,
        bool attempted = true,
        bool accountFormatSupported = true,
        bool granteeMatched = true) =>
        new(
            attempted,
            accountFormatSupported,
            granteeMatched,
            select,
            showView,
            trigger,
            execute);

    public static MySqlServerMetadata Server() =>
        new(
            Version: "8.0.42",
            VersionComment: "MySQL Community Server - GPL",
            CharacterSetServer: "utf8mb4",
            CollationServer: "utf8mb4_0900_ai_ci",
            SystemTimeZone: "UTC",
            LowerCaseTableNames: 0,
            ShowGeneratedInvisiblePrimaryKey: true);

    public static MySqlSessionMetadata Session() =>
        new(
            SqlMode: "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION",
            CharacterSetConnection: "utf8mb4",
            CollationConnection: "utf8mb4_0900_ai_ci",
            TimeZone: "+00:00",
            SqlQuoteShowCreate: true,
            ExplicitDefaultsForTimestamp: true);

    public static MySqlDatabaseMetadata Database() =>
        new(
            Name: "SourceDb",
            DefaultCharacterSet: "utf8mb4",
            DefaultCollation: "utf8mb4_0900_ai_ci",
            ViewCount: 0);

    public static IReadOnlyList<MySqlTableMetadata> Tables() =>
    [
        Table("Orders"),
        Table("Archive"),
    ];

    public static IReadOnlyList<MySqlColumnMetadata> Columns() =>
    [
        Column("Orders", 1, "Id", "bigint", nullable: false),
        Column(
            "Orders",
            2,
            "UnsignedId",
            "bigint",
            nullable: false,
            unsigned: true),
        Column(
            "Orders",
            3,
            "Amount",
            "decimal",
            nullable: false,
            numericPrecision: 18,
            numericScale: 2),
        Column(
            "Orders",
            4,
            "Enabled",
            "tinyint",
            nullable: false,
            tinyIntOne: true),
        Column(
            "Orders",
            5,
            "CreatedAt",
            "datetime",
            nullable: false,
            dateTimePrecision: 6),
        Column(
            "Orders",
            6,
            "Customer",
            "varchar",
            characterMaximumLength: 100,
            characterSetName: "utf8mb4",
            collationName: "utf8mb4_0900_ai_ci"),
        Column(
            "Orders",
            7,
            "Payload",
            "varbinary",
            characterMaximumLength: 64),
        Column("Orders", 8, "Document", "json"),
        Column("Orders", 9, "Location", "geometry"),
        Column(
            "Orders",
            10,
            "GeneratedTotal",
            "decimal",
            numericPrecision: 18,
            numericScale: 2,
            generated: true,
            generationKind: "STORED GENERATED",
            generationExpression: SecretGenerationExpression),
        Column(
            "Orders",
            11,
            "HiddenCode",
            "varchar",
            characterMaximumLength: 32,
            characterSetName: "utf8mb4",
            collationName: "utf8mb4_0900_ai_ci",
            invisible: true),
        Column("Archive", 1, "ArchiveId", "int", nullable: false),
    ];

    public static IReadOnlyList<MySqlTableDefinitionMetadata> TableDefinitions(
        IEnumerable<MySqlTableMetadata>? tables = null) =>
        (tables ?? Tables())
            .Select(table => TableDefinition(table.SchemaName, table.Name))
            .ToArray();

    public static IReadOnlyList<MySqlKeyMetadata> Keys() =>
    [
        Key("Orders", "PRIMARY", "PRIMARY KEY"),
        Key("Orders", "UQ_Orders_Amount_Customer", "UNIQUE"),
        Key("Archive", "PRIMARY", "PRIMARY KEY"),
    ];

    public static IReadOnlyList<MySqlKeyColumnMetadata> KeyColumns() =>
    [
        KeyColumn("Orders", "PRIMARY", 1, "Id"),
        KeyColumn("Orders", "UQ_Orders_Amount_Customer", 1, "Amount"),
        KeyColumn("Orders", "UQ_Orders_Amount_Customer", 2, "Customer"),
        KeyColumn("Archive", "PRIMARY", 1, "ArchiveId"),
    ];

    public static IReadOnlyList<MySqlForeignKeyMetadata> ForeignKeys() =>
    [
        ForeignKey(
            tableName: "Archive",
            name: "FK_Archive_Orders",
            referencedTableName: "Orders",
            uniqueConstraintName: "PRIMARY",
            deleteRule: "CASCADE"),
    ];

    public static IReadOnlyList<MySqlForeignKeyColumnMetadata>
        ForeignKeyColumns() =>
    [
        ForeignKeyColumn(
            tableName: "Archive",
            constraintName: "FK_Archive_Orders",
            ordinal: 1,
            columnName: "ArchiveId",
            referencedTableName: "Orders",
            referencedColumnName: "Id",
            positionInUniqueConstraint: 1),
    ];

    public static IReadOnlyList<MySqlCheckMetadata> Checks() =>
    [
        Check(
            tableName: "Orders",
            name: "CK_Orders_Amount",
            clause: SecretCheckClause),
    ];

    public static IReadOnlyList<MySqlIndexMetadata> Indexes() =>
    [
        Index("Orders", "PRIMARY", unique: true),
        Index("Orders", "UQ_Orders_Amount_Customer", unique: true),
        Index("Orders", "IX_Orders_Amount_Customer"),
        Index("Archive", "PRIMARY", unique: true),
    ];

    public static IReadOnlyList<MySqlIndexPartMetadata> IndexParts() =>
    [
        IndexPart("Orders", "PRIMARY", 1, columnName: "Id"),
        IndexPart(
            "Orders",
            "UQ_Orders_Amount_Customer",
            1,
            columnName: "Amount"),
        IndexPart(
            "Orders",
            "UQ_Orders_Amount_Customer",
            2,
            columnName: "Customer"),
        IndexPart(
            "Orders",
            "IX_Orders_Amount_Customer",
            1,
            columnName: "Amount"),
        IndexPart(
            "Orders",
            "IX_Orders_Amount_Customer",
            2,
            columnName: "Customer"),
        IndexPart("Archive", "PRIMARY", 1, columnName: "ArchiveId"),
    ];

    public static MySqlCatalogSnapshot CreateSupportedRelational() =>
        Create(
            tables:
            [
                Table("Parent", collation: null),
                Table("Child", collation: null),
            ],
            columns:
            [
                Column("Parent", 1, "Id", "int", nullable: false),
                Column("Parent", 2, "Code", "int", nullable: false),
                Column("Child", 1, "Id", "int", nullable: false),
                Column("Child", 2, "ParentId", "int", nullable: false),
            ],
            keys:
            [
                Key("Parent", "PRIMARY", "PRIMARY KEY"),
                Key("Child", "PRIMARY", "PRIMARY KEY"),
                Key("Child", "UQ_Child_ParentId", "UNIQUE"),
            ],
            keyColumns:
            [
                KeyColumn("Parent", "PRIMARY", 1, "Id"),
                KeyColumn("Child", "PRIMARY", 1, "Id"),
                KeyColumn("Child", "UQ_Child_ParentId", 1, "ParentId"),
            ],
            foreignKeys:
            [
                ForeignKey(
                    tableName: "Child",
                    name: "FK_Child_Parent",
                    referencedTableName: "Parent",
                    uniqueConstraintName: "PRIMARY",
                    deleteRule: "CASCADE"),
            ],
            foreignKeyColumns:
            [
                ForeignKeyColumn(
                    tableName: "Child",
                    constraintName: "FK_Child_Parent",
                    ordinal: 1,
                    columnName: "ParentId",
                    referencedTableName: "Parent",
                    referencedColumnName: "Id",
                    positionInUniqueConstraint: 1),
            ],
            checks: [],
            indexes:
            [
                Index("Parent", "PRIMARY", unique: true),
                Index("Parent", "IX_Parent_Code"),
                Index("Child", "PRIMARY", unique: true),
                Index("Child", "UQ_Child_ParentId", unique: true),
            ],
            indexParts:
            [
                IndexPart("Parent", "PRIMARY", 1, columnName: "Id"),
                IndexPart("Parent", "IX_Parent_Code", 1, columnName: "Code"),
                IndexPart("Child", "PRIMARY", 1, columnName: "Id"),
                IndexPart(
                    "Child",
                    "UQ_Child_ParentId",
                    1,
                    columnName: "ParentId"),
            ]);

    public static MySqlCatalogSnapshot CreateProgrammableInventory() =>
        Create(
            database: Database() with { ViewCount = 2 },
            columns:
            [
                .. Columns(),
                Column(
                    "Archive",
                    2,
                    "LiteralDefault",
                    "varchar",
                    characterMaximumLength: 64,
                    characterSetName: "utf8mb4",
                    collationName: "utf8mb4_0900_ai_ci",
                    defaultValue: SecretDefaultValue),
                Column(
                    "Archive",
                    3,
                    "ExpressionDefault",
                    "timestamp",
                    nullable: false,
                    dateTimePrecision: 6,
                    defaultValue: "CURRENT_TIMESTAMP(6)",
                    defaultGenerated: true),
                Column(
                    "Archive",
                    4,
                    "UpdatedAt",
                    "timestamp",
                    nullable: false,
                    dateTimePrecision: 6,
                    defaultValue: "CURRENT_TIMESTAMP(6)",
                    defaultGenerated: true,
                    onUpdateCurrentTimestamp: true),
            ],
            views: Views(),
            viewColumns: ViewColumns(),
            triggers: Triggers(),
            routines: Routines(),
            routineParameters: RoutineParameters());

    public static IReadOnlyList<MySqlViewMetadata> Views() =>
    [
        View(
            "VisibleOrders",
            metadataVisible: true,
            definition: SecretViewDefinition,
            checkOption: "NONE",
            updatable: true,
            securityType: "DEFINER"),
        View(
            "FilteredOrders",
            metadataVisible: false,
            definition: null),
    ];

    public static IReadOnlyList<MySqlViewColumnMetadata> ViewColumns() =>
    [
        ViewColumn("VisibleOrders", 1, "Id", "bigint", nullable: false),
        ViewColumn(
            "VisibleOrders",
            2,
            "Customer",
            "varchar",
            characterMaximumLength: 100,
            characterSetName: "utf8mb4",
            collationName: "utf8mb4_0900_ai_ci"),
        ViewColumn("FilteredOrders", 1, "Id", "bigint", nullable: false),
    ];

    public static IReadOnlyList<MySqlTriggerMetadata> Triggers() =>
    [
        Trigger(
            "Orders",
            "TR_Orders_Customer",
            actionStatement: SecretTriggerStatement),
    ];

    public static IReadOnlyList<MySqlRoutineMetadata> Routines() =>
    [
        Routine(
            specificName: "RefreshArchive",
            name: "RefreshArchive",
            routineType: "PROCEDURE",
            definition: SecretProcedureDefinition),
        Routine(
            specificName: "NormalizeCustomer",
            name: "NormalizeCustomer",
            routineType: "FUNCTION",
            dataType: "varchar",
            dtdIdentifier: "varchar(100)",
            definition: SecretFunctionDefinition,
            deterministic: true,
            sqlDataAccess: "NO SQL"),
    ];

    public static IReadOnlyList<MySqlRoutineParameterMetadata>
        RoutineParameters() =>
    [
        RoutineParameter(
            specificName: "RefreshArchive",
            routineType: "PROCEDURE",
            ordinal: 1,
            mode: "IN",
            name: "cutoff_id",
            dataType: "bigint",
            dtdIdentifier: "bigint"),
        RoutineParameter(
            specificName: "NormalizeCustomer",
            routineType: "FUNCTION",
            ordinal: 0,
            mode: null,
            name: null,
            dataType: "varchar",
            dtdIdentifier: "varchar(100)",
            characterMaximumLength: 100,
            characterSetName: "utf8mb4",
            collationName: "utf8mb4_0900_ai_ci"),
        RoutineParameter(
            specificName: "NormalizeCustomer",
            routineType: "FUNCTION",
            ordinal: 1,
            mode: "IN",
            name: "value",
            dataType: "varchar",
            dtdIdentifier: "varchar(100)",
            characterMaximumLength: 100,
            characterSetName: "utf8mb4",
            collationName: "utf8mb4_0900_ai_ci"),
    ];

    public static MySqlTableMetadata Table(
        string name,
        string tableType = "BASE TABLE",
        string? engine = "InnoDB",
        string? collation = "utf8mb4_0900_ai_ci",
        string? createOptions = "",
        bool partitioned = false,
        string schemaName = "SourceDb") =>
        new(
            SchemaName: schemaName,
            Name: name,
            TableType: tableType,
            Engine: engine,
            TableCollation: collation,
            CreateOptions: createOptions,
            IsPartitioned: partitioned);

    public static MySqlTableDefinitionMetadata TableDefinition(
        string schemaName,
        string tableName,
        string? definition = null)
    {
        string resolvedDefinition = definition ??
            $"CREATE TABLE `{tableName}` (`id` int) " +
            $"/* {SecretShowCreateMarker}:{tableName} */";
        return new(
            SchemaName: schemaName,
            TableName: tableName,
            DefinitionBytes:
                System.Text.Encoding.UTF8.GetByteCount(resolvedDefinition),
            Definition: resolvedDefinition);
    }

    public static MySqlKeyMetadata Key(
        string tableName,
        string name,
        string constraintType = "UNIQUE",
        string schemaName = "SourceDb") =>
        new(
            SchemaName: schemaName,
            TableName: tableName,
            Name: name,
            ConstraintType: constraintType);

    public static MySqlKeyColumnMetadata KeyColumn(
        string tableName,
        string constraintName,
        int ordinal,
        string columnName,
        string schemaName = "SourceDb") =>
        new(
            SchemaName: schemaName,
            TableName: tableName,
            ConstraintName: constraintName,
            OrdinalPosition: ordinal,
            ColumnName: columnName);

    public static MySqlForeignKeyMetadata ForeignKey(
        string tableName,
        string name,
        string referencedTableName,
        string? uniqueConstraintName,
        string matchOption = "NONE",
        string updateRule = "NO ACTION",
        string deleteRule = "NO ACTION",
        string schemaName = "SourceDb",
        string referencedSchemaName = "SourceDb",
        string? uniqueConstraintSchemaName = "SourceDb") =>
        new(
            SchemaName: schemaName,
            TableName: tableName,
            Name: name,
            ReferencedSchemaName: referencedSchemaName,
            ReferencedTableName: referencedTableName,
            UniqueConstraintSchemaName: uniqueConstraintSchemaName,
            UniqueConstraintName: uniqueConstraintName,
            MatchOption: matchOption,
            UpdateRule: updateRule,
            DeleteRule: deleteRule);

    public static MySqlForeignKeyColumnMetadata ForeignKeyColumn(
        string tableName,
        string constraintName,
        int ordinal,
        string columnName,
        string referencedTableName,
        string referencedColumnName,
        int? positionInUniqueConstraint,
        string schemaName = "SourceDb",
        string referencedSchemaName = "SourceDb") =>
        new(
            SchemaName: schemaName,
            TableName: tableName,
            ConstraintName: constraintName,
            OrdinalPosition: ordinal,
            ColumnName: columnName,
            PositionInUniqueConstraint: positionInUniqueConstraint,
            ReferencedSchemaName: referencedSchemaName,
            ReferencedTableName: referencedTableName,
            ReferencedColumnName: referencedColumnName);

    public static MySqlCheckMetadata Check(
        string tableName,
        string name,
        string clause,
        bool enforced = true,
        string schemaName = "SourceDb") =>
        new(
            SchemaName: schemaName,
            TableName: tableName,
            Name: name,
            IsEnforced: enforced,
            ClauseBytes: System.Text.Encoding.UTF8.GetByteCount(clause),
            Clause: clause);

    public static MySqlIndexMetadata Index(
        string tableName,
        string name,
        bool unique = false,
        string indexType = "BTREE",
        bool visible = true,
        string schemaName = "SourceDb") =>
        new(
            SchemaName: schemaName,
            TableName: tableName,
            Name: name,
            IsUnique: unique,
            IndexType: indexType,
            IsVisible: visible);

    public static MySqlIndexPartMetadata IndexPart(
        string tableName,
        string indexName,
        int sequence,
        string? columnName = null,
        string? sortDirection = "A",
        long? prefixLength = null,
        string? expression = null,
        string schemaName = "SourceDb") =>
        new(
            SchemaName: schemaName,
            TableName: tableName,
            IndexName: indexName,
            Sequence: sequence,
            ColumnName: columnName,
            SortDirection: sortDirection,
            PrefixLength: prefixLength,
            ExpressionBytes:
                expression is null
                    ? null
                    : System.Text.Encoding.UTF8.GetByteCount(expression),
            Expression: expression);

    public static MySqlViewMetadata View(
        string name,
        bool metadataVisible,
        string? definition,
        string? checkOption = null,
        bool? updatable = null,
        string? securityType = null,
        string? characterSetClient = null,
        string? collationConnection = null,
        string schemaName = "SourceDb") =>
        new(
            SchemaName: schemaName,
            Name: name,
            MetadataVisible: metadataVisible,
            DefinitionBytes:
                definition is null
                    ? null
                    : System.Text.Encoding.UTF8.GetByteCount(definition),
            Definition: definition,
            CheckOption: checkOption,
            IsUpdatable: updatable,
            SecurityType: securityType,
            CharacterSetClient: characterSetClient ??
                (metadataVisible ? "utf8mb4" : null),
            CollationConnection: collationConnection ??
                (metadataVisible ? "utf8mb4_0900_ai_ci" : null));

    public static MySqlViewColumnMetadata ViewColumn(
        string viewName,
        int ordinal,
        string name,
        string dataType,
        bool nullable = true,
        string? characterSetName = null,
        string? collationName = null,
        long? characterMaximumLength = null,
        int? numericPrecision = null,
        int? numericScale = null,
        int? dateTimePrecision = null,
        string? columnType = null,
        string schemaName = "SourceDb")
    {
        string resolvedColumnType = columnType ?? FormatColumnType(
            dataType,
            characterMaximumLength,
            numericPrecision,
            numericScale,
            dateTimePrecision,
            unsigned: false,
            zerofill: false,
            tinyIntOne: false);
        return new(
            SchemaName: schemaName,
            ViewName: viewName,
            OrdinalPosition: ordinal,
            Name: name,
            DataType: dataType,
            IsNullable: nullable,
            CharacterSetName: characterSetName,
            CollationName: collationName,
            CharacterMaximumLength: characterMaximumLength,
            NumericPrecision: numericPrecision,
            NumericScale: numericScale,
            DateTimePrecision: dateTimePrecision,
            ColumnTypeBytes:
                System.Text.Encoding.UTF8.GetByteCount(resolvedColumnType),
            ColumnType: resolvedColumnType);
    }

    public static MySqlTriggerMetadata Trigger(
        string tableName,
        string name,
        string actionStatement,
        string eventManipulation = "UPDATE",
        int actionOrder = 1,
        string actionOrientation = "ROW",
        string actionTiming = "BEFORE",
        string schemaName = "SourceDb") =>
        new(
            SchemaName: schemaName,
            Name: name,
            EventManipulation: eventManipulation,
            EventObjectSchema: schemaName,
            EventObjectTable: tableName,
            ActionOrder: actionOrder,
            ActionStatementBytes:
                System.Text.Encoding.UTF8.GetByteCount(actionStatement),
            ActionStatement: actionStatement,
            ActionOrientation: actionOrientation,
            ActionTiming: actionTiming,
            SqlMode: "STRICT_TRANS_TABLES",
            CharacterSetClient: "utf8mb4",
            CollationConnection: "utf8mb4_0900_ai_ci",
            DatabaseCollation: "utf8mb4_0900_ai_ci");

    public static MySqlRoutineMetadata Routine(
        string specificName,
        string name,
        string routineType,
        string? definition,
        string? dataType = null,
        string? dtdIdentifier = null,
        bool deterministic = false,
        string sqlDataAccess = "CONTAINS SQL",
        string schemaName = "SourceDb") =>
        new(
            SchemaName: schemaName,
            SpecificName: specificName,
            Name: name,
            RoutineType: routineType,
            DataType: dataType,
            DtdIdentifierBytes:
                dtdIdentifier is null
                    ? null
                    : System.Text.Encoding.UTF8.GetByteCount(dtdIdentifier),
            DtdIdentifier: dtdIdentifier,
            RoutineBody: "SQL",
            DefinitionBytes:
                definition is null
                    ? null
                    : System.Text.Encoding.UTF8.GetByteCount(definition),
            Definition: definition,
            IsDeterministic: deterministic,
            SqlDataAccess: sqlDataAccess,
            SecurityType: "DEFINER",
            SqlMode: "STRICT_TRANS_TABLES",
            CharacterSetClient: "utf8mb4",
            CollationConnection: "utf8mb4_0900_ai_ci",
            DatabaseCollation: "utf8mb4_0900_ai_ci");

    public static MySqlRoutineParameterMetadata RoutineParameter(
        string specificName,
        string routineType,
        int ordinal,
        string? mode,
        string? name,
        string dataType,
        string dtdIdentifier,
        string? characterSetName = null,
        string? collationName = null,
        long? characterMaximumLength = null,
        int? numericPrecision = null,
        int? numericScale = null,
        int? dateTimePrecision = null,
        string schemaName = "SourceDb") =>
        new(
            SchemaName: schemaName,
            SpecificName: specificName,
            RoutineType: routineType,
            OrdinalPosition: ordinal,
            Mode: mode,
            Name: name,
            DataType: dataType,
            DtdIdentifierBytes:
                System.Text.Encoding.UTF8.GetByteCount(dtdIdentifier),
            DtdIdentifier: dtdIdentifier,
            CharacterSetName: characterSetName,
            CollationName: collationName,
            CharacterMaximumLength: characterMaximumLength,
            NumericPrecision: numericPrecision,
            NumericScale: numericScale,
            DateTimePrecision: dateTimePrecision);

    public static MySqlColumnMetadata Column(
        string tableName,
        int ordinal,
        string name,
        string dataType,
        bool nullable = true,
        string? characterSetName = null,
        string? collationName = null,
        long? characterMaximumLength = null,
        int? numericPrecision = null,
        int? numericScale = null,
        int? dateTimePrecision = null,
        bool unsigned = false,
        bool zerofill = false,
        bool tinyIntOne = false,
        bool autoIncrement = false,
        bool generated = false,
        string generationKind = "NEVER",
        string? generationExpression = null,
        bool invisible = false,
        string? defaultValue = null,
        bool defaultGenerated = false,
        bool onUpdateCurrentTimestamp = false,
        string schemaName = "SourceDb",
        string? columnType = null)
    {
        string resolvedColumnType = columnType ?? FormatColumnType(
            dataType,
            characterMaximumLength,
            numericPrecision,
            numericScale,
            dateTimePrecision,
            unsigned,
            zerofill,
            tinyIntOne);

        return new(
            SchemaName: schemaName,
            TableName: tableName,
            OrdinalPosition: ordinal,
            Name: name,
            DataType: dataType,
            ColumnTypeBytes:
                System.Text.Encoding.UTF8.GetByteCount(resolvedColumnType),
            ColumnType: resolvedColumnType,
            IsNullable: nullable,
            CharacterSetName: characterSetName,
            CollationName: collationName,
            CharacterMaximumLength: characterMaximumLength,
            NumericPrecision: numericPrecision,
            NumericScale: numericScale,
            DateTimePrecision: dateTimePrecision,
            IsUnsigned: unsigned,
            IsZerofill: zerofill,
            IsTinyIntOne: tinyIntOne,
            IsAutoIncrement: autoIncrement,
            IsGenerated: generated,
            GenerationKind: generationKind,
            GenerationExpressionBytes:
                generationExpression is null
                    ? null
                    : System.Text.Encoding.UTF8.GetByteCount(generationExpression),
            GenerationExpression: generationExpression,
            IsInvisible: invisible,
            DefaultBytes:
                defaultValue is null
                    ? null
                    : System.Text.Encoding.UTF8.GetByteCount(defaultValue),
            DefaultValue: defaultValue,
            IsDefaultGenerated: defaultGenerated,
            HasOnUpdateCurrentTimestamp: onUpdateCurrentTimestamp);
    }

    private static string FormatColumnType(
        string dataType,
        long? characterMaximumLength,
        int? numericPrecision,
        int? numericScale,
        int? dateTimePrecision,
        bool unsigned,
        bool zerofill,
        bool tinyIntOne)
    {
        string type = dataType.ToLowerInvariant();
        string formatted = type;
        if (tinyIntOne)
        {
            formatted = "tinyint(1)";
        }
        else if (type is "decimal" or "numeric" &&
                 numericPrecision is int precision &&
                 numericScale is int scale)
        {
            formatted = $"{type}({precision},{scale})";
        }
        else if (type is "char" or "varchar" or "binary" or "varbinary" &&
                 characterMaximumLength is long maxLength)
        {
            formatted = $"{type}({maxLength})";
        }
        else if (type is "time" or "datetime" or "timestamp" &&
                 dateTimePrecision is int fractionalSeconds)
        {
            formatted = $"{type}({fractionalSeconds})";
        }
        if (unsigned)
            formatted += " unsigned";
        if (zerofill)
            formatted += " zerofill";
        return formatted;
    }
}
