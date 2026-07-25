using System.Collections.ObjectModel;

namespace CSharpDB.Migration.MySql;

/// <summary>
/// One immutable, value-only view of the bounded metadata read from MySQL.
/// The production reader and deterministic tests meet at this seam.
/// </summary>
internal sealed class MySqlCatalogSnapshot
{
    public MySqlCatalogSnapshot(
        string endpointDigest,
        string providerVersion,
        MySqlServerMetadata server,
        MySqlSessionMetadata session,
        MySqlDatabaseMetadata database,
        IEnumerable<MySqlTableMetadata> tables,
        IEnumerable<MySqlColumnMetadata> columns,
        IEnumerable<MySqlTableDefinitionMetadata>? tableDefinitions = null,
        IEnumerable<MySqlKeyMetadata>? keys = null,
        IEnumerable<MySqlKeyColumnMetadata>? keyColumns = null,
        IEnumerable<MySqlForeignKeyMetadata>? foreignKeys = null,
        IEnumerable<MySqlForeignKeyColumnMetadata>? foreignKeyColumns = null,
        IEnumerable<MySqlCheckMetadata>? checks = null,
        IEnumerable<MySqlIndexMetadata>? indexes = null,
        IEnumerable<MySqlIndexPartMetadata>? indexParts = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(endpointDigest);
        ArgumentException.ThrowIfNullOrWhiteSpace(providerVersion);
        ArgumentNullException.ThrowIfNull(server);
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(database);
        ArgumentNullException.ThrowIfNull(tables);
        ArgumentNullException.ThrowIfNull(columns);

        EndpointDigest = endpointDigest;
        ProviderVersion = providerVersion;
        Server = server;
        Session = session;
        Database = database;
        Tables = new ReadOnlyCollection<MySqlTableMetadata>(tables.ToArray());
        Columns = new ReadOnlyCollection<MySqlColumnMetadata>(columns.ToArray());
        TableDefinitions = ReadOnly(tableDefinitions);
        Keys = ReadOnly(keys);
        KeyColumns = ReadOnly(keyColumns);
        ForeignKeys = ReadOnly(foreignKeys);
        ForeignKeyColumns = ReadOnly(foreignKeyColumns);
        Checks = ReadOnly(checks);
        Indexes = ReadOnly(indexes);
        IndexParts = ReadOnly(indexParts);
    }

    public string EndpointDigest { get; }

    public string ProviderVersion { get; }

    public MySqlServerMetadata Server { get; }

    public MySqlSessionMetadata Session { get; }

    public MySqlDatabaseMetadata Database { get; }

    public IReadOnlyList<MySqlTableMetadata> Tables { get; }

    public IReadOnlyList<MySqlColumnMetadata> Columns { get; }

    public IReadOnlyList<MySqlTableDefinitionMetadata> TableDefinitions { get; }

    public IReadOnlyList<MySqlKeyMetadata> Keys { get; }

    public IReadOnlyList<MySqlKeyColumnMetadata> KeyColumns { get; }

    public IReadOnlyList<MySqlForeignKeyMetadata> ForeignKeys { get; }

    public IReadOnlyList<MySqlForeignKeyColumnMetadata> ForeignKeyColumns { get; }

    public IReadOnlyList<MySqlCheckMetadata> Checks { get; }

    public IReadOnlyList<MySqlIndexMetadata> Indexes { get; }

    public IReadOnlyList<MySqlIndexPartMetadata> IndexParts { get; }

    private static IReadOnlyList<T> ReadOnly<T>(IEnumerable<T>? values) =>
        new ReadOnlyCollection<T>((values ?? []).ToArray());
}

internal sealed record MySqlServerMetadata(
    string Version,
    string VersionComment,
    string CharacterSetServer,
    string CollationServer,
    string SystemTimeZone,
    int LowerCaseTableNames,
    bool? ShowGeneratedInvisiblePrimaryKey);

internal sealed record MySqlSessionMetadata(
    string SqlMode,
    string CharacterSetConnection,
    string CollationConnection,
    string TimeZone,
    bool? SqlQuoteShowCreate = null);

internal sealed record MySqlDatabaseMetadata(
    string Name,
    string DefaultCharacterSet,
    string DefaultCollation,
    int ViewCount);

internal sealed record MySqlTableMetadata(
    string SchemaName,
    string Name,
    string TableType,
    string? Engine,
    string? TableCollation,
    string? CreateOptions,
    bool IsPartitioned);

internal sealed record MySqlColumnMetadata(
    string SchemaName,
    string TableName,
    int OrdinalPosition,
    string Name,
    string DataType,
    long ColumnTypeBytes,
    string ColumnType,
    bool IsNullable,
    string? CharacterSetName,
    string? CollationName,
    long? CharacterMaximumLength,
    int? NumericPrecision,
    int? NumericScale,
    int? DateTimePrecision,
    bool IsUnsigned,
    bool IsZerofill,
    bool IsTinyIntOne,
    bool IsAutoIncrement,
    bool IsGenerated,
    string GenerationKind,
    long? GenerationExpressionBytes,
    string? GenerationExpression,
    bool IsInvisible);

internal sealed record MySqlTableDefinitionMetadata(
    string SchemaName,
    string TableName,
    long DefinitionBytes,
    string Definition);

internal sealed record MySqlKeyMetadata(
    string SchemaName,
    string TableName,
    string Name,
    string ConstraintType);

internal sealed record MySqlKeyColumnMetadata(
    string SchemaName,
    string TableName,
    string ConstraintName,
    int OrdinalPosition,
    string ColumnName);

internal sealed record MySqlForeignKeyMetadata(
    string SchemaName,
    string TableName,
    string Name,
    string ReferencedSchemaName,
    string ReferencedTableName,
    string? UniqueConstraintSchemaName,
    string? UniqueConstraintName,
    string MatchOption,
    string UpdateRule,
    string DeleteRule);

internal sealed record MySqlForeignKeyColumnMetadata(
    string SchemaName,
    string TableName,
    string ConstraintName,
    int OrdinalPosition,
    string ColumnName,
    int? PositionInUniqueConstraint,
    string ReferencedSchemaName,
    string ReferencedTableName,
    string ReferencedColumnName);

internal sealed record MySqlCheckMetadata(
    string SchemaName,
    string TableName,
    string Name,
    bool IsEnforced,
    long ClauseBytes,
    string Clause);

internal sealed record MySqlIndexMetadata(
    string SchemaName,
    string TableName,
    string Name,
    bool IsUnique,
    string IndexType,
    bool IsVisible);

internal sealed record MySqlIndexPartMetadata(
    string SchemaName,
    string TableName,
    string IndexName,
    int Sequence,
    string? ColumnName,
    string? SortDirection,
    long? PrefixLength,
    long? ExpressionBytes,
    string? Expression);
