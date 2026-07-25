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
        IEnumerable<MySqlColumnMetadata> columns)
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
    }

    public string EndpointDigest { get; }

    public string ProviderVersion { get; }

    public MySqlServerMetadata Server { get; }

    public MySqlSessionMetadata Session { get; }

    public MySqlDatabaseMetadata Database { get; }

    public IReadOnlyList<MySqlTableMetadata> Tables { get; }

    public IReadOnlyList<MySqlColumnMetadata> Columns { get; }
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
    string TimeZone);

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
