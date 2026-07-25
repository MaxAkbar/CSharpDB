using CSharpDB.Migration.MySql;

namespace CSharpDB.Migration.MySql.Tests;

internal static class MySqlTestSnapshot
{
    public const string SecretGenerationExpression =
        "(`Amount` * 2 /* NeverPersistThisMySqlExpression */)";

    public static MySqlCatalogSnapshot Create(
        MySqlServerMetadata? server = null,
        MySqlSessionMetadata? session = null,
        MySqlDatabaseMetadata? database = null,
        IEnumerable<MySqlTableMetadata>? tables = null,
        IEnumerable<MySqlColumnMetadata>? columns = null) =>
        new(
            endpointDigest:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            providerVersion: "2.6.1",
            server ?? Server(),
            session ?? Session(),
            database ?? Database(),
            tables ?? Tables(),
            columns ?? Columns());

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
            TimeZone: "+00:00");

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
            IsInvisible: invisible);
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
