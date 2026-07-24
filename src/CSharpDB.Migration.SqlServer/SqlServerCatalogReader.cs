using System.Data;
using System.Globalization;
using System.Reflection;
using System.Text;
using Microsoft.Data.SqlClient;

namespace CSharpDB.Migration.SqlServer;

internal interface ISqlServerCatalogReader
{
    ValueTask<SqlServerCatalogSnapshot> ReadAsync(
        SqlServerInspectionLimits limits,
        CancellationToken cancellationToken);
}

internal sealed partial class SqlServerCatalogReader : ISqlServerCatalogReader
{
    private const int ConnectionTimeoutSeconds = 30;
    private const int CommandTimeoutSeconds = 30;

    internal const string ServerAndDatabaseQuery =
        """
        SELECT
            CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductVersion')),
            CONVERT(int, SERVERPROPERTY(N'ProductMajorVersion')),
            CONVERT(nvarchar(128), SERVERPROPERTY(N'ProductLevel')),
            CONVERT(nvarchar(256), SERVERPROPERTY(N'Edition')),
            CONVERT(int, SERVERPROPERTY(N'EngineEdition')),
            d.database_id,
            d.name,
            d.compatibility_level,
            d.collation_name,
            d.is_read_committed_snapshot_on,
            d.snapshot_isolation_state_desc,
            d.is_auto_create_stats_on,
            d.is_auto_update_stats_on,
            d.is_ansi_null_default_on,
            d.is_quoted_identifier_on,
            d.is_parameterization_forced,
            d.containment_desc,
            d.is_trustworthy_on,
            CONVERT(int, IS_SRVROLEMEMBER(N'sysadmin')),
            CONVERT(int, IS_MEMBER(N'db_owner')),
            CONVERT(int, HAS_PERMS_BY_NAME(DB_NAME(), N'DATABASE', N'CONTROL')),
            CONVERT(int, HAS_PERMS_BY_NAME(DB_NAME(), N'DATABASE', N'VIEW DEFINITION')),
            CASE
                WHEN CONVERT(int, SERVERPROPERTY(N'ProductMajorVersion')) >= 16
                THEN CONVERT(
                    int,
                    HAS_PERMS_BY_NAME(
                        DB_NAME(),
                        N'DATABASE',
                        N'VIEW SECURITY DEFINITION'))
                ELSE NULL
            END
        FROM sys.databases AS d
        WHERE d.database_id = DB_ID();
        """;

    internal const string SchemasQuery =
        """
        SELECT
            s.schema_id,
            s.name,
            CONVERT(
                int,
                HAS_PERMS_BY_NAME(
                    s.name,
                    N'SCHEMA',
                    N'VIEW DEFINITION'))
        FROM sys.schemas AS s
        LEFT JOIN sys.database_principals AS p
            ON p.principal_id = s.principal_id
        WHERE s.name NOT IN (N'sys', N'INFORMATION_SCHEMA')
          AND NOT (
              p.type = N'R'
              AND p.is_fixed_role = 1
              AND p.name = s.name
          )
        ORDER BY s.schema_id, s.name;
        """;

    internal const string TablesQuery =
        """
        SELECT
            t.object_id,
            t.schema_id,
            t.name,
            t.is_memory_optimized,
            t.durability_desc,
            t.is_filetable,
            t.temporal_type_desc,
            t.is_node,
            t.is_edge,
            CONVERT(
                int,
                HAS_PERMS_BY_NAME(
                    QUOTENAME(OBJECT_SCHEMA_NAME(t.object_id)) +
                        N'.' +
                        QUOTENAME(t.name),
                    N'OBJECT',
                    N'VIEW DEFINITION'))
        FROM sys.tables AS t
        WHERE t.is_ms_shipped = 0
        ORDER BY t.object_id;
        """;

    internal const string ColumnsQuery =
        """
        SELECT
            c.object_id,
            c.column_id,
            c.name,
            type_schema.name,
            user_type.name,
            COALESCE(system_type.name, user_type.name),
            c.max_length,
            c.precision,
            c.scale,
            c.collation_name,
            c.is_nullable,
            c.is_sparse,
            c.is_column_set,
            c.is_hidden,
            c.is_computed,
            c.is_filestream,
            c.is_masked,
            c.encryption_type_desc,
            c.xml_collection_id,
            c.generated_always_type_desc,
            CONVERT(bit, CASE WHEN c.default_object_id <> 0 THEN 1 ELSE 0 END),
            default_constraint.name,
            CONVERT(bigint, DATALENGTH(default_constraint.definition)),
            default_constraint.definition,
            CONVERT(bigint, DATALENGTH(computed_column.definition)),
            computed_column.definition,
            COALESCE(computed_column.is_persisted, CONVERT(bit, 0)),
            c.is_identity,
            CONVERT(nvarchar(128), identity_column.seed_value),
            CONVERT(nvarchar(128), identity_column.increment_value),
            COALESCE(identity_column.is_not_for_replication, CONVERT(bit, 0))
        FROM sys.columns AS c
        INNER JOIN sys.tables AS t
            ON t.object_id = c.object_id
           AND t.is_ms_shipped = 0
        INNER JOIN sys.types AS user_type
            ON user_type.user_type_id = c.user_type_id
        INNER JOIN sys.schemas AS type_schema
            ON type_schema.schema_id = user_type.schema_id
        LEFT JOIN sys.types AS system_type
            ON system_type.user_type_id = c.system_type_id
           AND system_type.is_user_defined = 0
        LEFT JOIN sys.default_constraints AS default_constraint
            ON default_constraint.object_id = c.default_object_id
        LEFT JOIN sys.computed_columns AS computed_column
            ON computed_column.object_id = c.object_id
           AND computed_column.column_id = c.column_id
        LEFT JOIN sys.identity_columns AS identity_column
            ON identity_column.object_id = c.object_id
           AND identity_column.column_id = c.column_id
        ORDER BY c.object_id, c.column_id;
        """;

    internal static IReadOnlyList<string> CommandTexts { get; } =
        Array.AsReadOnly(
        [
            ServerAndDatabaseQuery,
            SchemasQuery,
            TablesQuery,
            ColumnsQuery,
            UserTokensQuery,
            PermissionDenialsQuery,
            KeysQuery,
            IndexesQuery,
            IndexColumnsQuery,
            ForeignKeysQuery,
            ForeignKeyColumnsQuery,
            ChecksQuery,
            SequencesQuery,
        ]);

    private readonly string connectionString;
    private readonly string endpointDigest;

    public SqlServerCatalogReader(string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        SqlConnectionStringBuilder settings;
        try
        {
            settings = new SqlConnectionStringBuilder(connectionString);
        }
        catch (ArgumentException)
        {
            throw new ArgumentException(
                "The SQL Server connection settings are invalid.",
                nameof(connectionString));
        }

        if (string.IsNullOrWhiteSpace(settings.DataSource))
        {
            throw new ArgumentException(
                "The SQL Server connection settings must select a data source.",
                nameof(connectionString));
        }
        if (string.IsNullOrWhiteSpace(settings.InitialCatalog))
        {
            throw new ArgumentException(
                "The SQL Server connection settings must select one database.",
                nameof(connectionString));
        }
        if (!string.IsNullOrWhiteSpace(settings.AttachDBFilename) ||
            settings.UserInstance)
        {
            throw new ArgumentException(
                "The SQL Server analyzer cannot attach a database file or start a user instance.",
                nameof(connectionString));
        }

        try
        {
            endpointDigest = "sha256:" + SqlServerStableDigest.Text(
                "csharpdb-sqlserver-endpoint/v1",
                settings.DataSource.Trim().ToUpperInvariant());
        }
        catch (EncoderFallbackException)
        {
            throw new ArgumentException(
                "The SQL Server connection settings are invalid.",
                nameof(connectionString));
        }

        settings.ApplicationIntent = ApplicationIntent.ReadOnly;
        settings.ApplicationName = "CSharpDB Migration Analyzer";
        if (settings.ConnectTimeout is <= 0 or > ConnectionTimeoutSeconds)
            settings.ConnectTimeout = ConnectionTimeoutSeconds;
        settings.ConnectRetryCount = 0;
        settings.Pooling = false;
        settings.PersistSecurityInfo = false;
        settings.Enlist = false;
        settings.MultipleActiveResultSets = false;
        this.connectionString = settings.ConnectionString;
        Policy = new SqlServerConnectionPolicy(
            settings.ApplicationIntent.ToString(),
            settings.Pooling,
            settings.PersistSecurityInfo,
            settings.Enlist,
            settings.MultipleActiveResultSets,
            settings.Encrypt.ToString(),
            settings.TrustServerCertificate,
            settings.ConnectTimeout,
            settings.ConnectRetryCount);
    }

    internal SqlServerConnectionPolicy Policy { get; }

    internal string EndpointDigest => endpointDigest;

    public async ValueTask<SqlServerCatalogSnapshot> ReadAsync(
        SqlServerInspectionLimits limits,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(limits);
        limits.Validate();
        cancellationToken.ThrowIfCancellationRequested();

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        var budget = new ReaderBudget(limits);
        (SqlServerInstanceMetadata instance, SqlServerDatabaseMetadata database) =
            await ReadServerAndDatabaseAsync(
                    connection,
                    budget,
                    cancellationToken)
                .ConfigureAwait(false);
        SqlServerPermissionAuditMetadata permissionAuditBefore =
            await ReadPermissionAuditAsync(
                    connection,
                    instance,
                    database,
                    budget,
                    limits,
                    cancellationToken)
                .ConfigureAwait(false);
        IReadOnlyList<SqlServerSchemaMetadata> schemas = await ReadSchemasAsync(
                connection,
                budget,
                limits,
                cancellationToken)
            .ConfigureAwait(false);
        IReadOnlyList<SqlServerTableMetadata> tables = await ReadTablesAsync(
                connection,
                budget,
                limits,
                cancellationToken)
            .ConfigureAwait(false);
        IReadOnlyList<SqlServerColumnMetadata> columns = await ReadColumnsAsync(
                connection,
                budget,
                limits,
                cancellationToken)
            .ConfigureAwait(false);
        IReadOnlyList<SqlServerKeyMetadata> keys = await ReadKeysAsync(
                connection,
                budget,
                limits,
                cancellationToken)
            .ConfigureAwait(false);
        IReadOnlyList<SqlServerIndexMetadata> indexes = await ReadIndexesAsync(
                connection,
                budget,
                limits,
                cancellationToken)
            .ConfigureAwait(false);
        IReadOnlyList<SqlServerIndexColumnMetadata> indexColumns =
            await ReadIndexColumnsAsync(
                    connection,
                    budget,
                    limits,
                    cancellationToken)
                .ConfigureAwait(false);
        IReadOnlyList<SqlServerForeignKeyMetadata> foreignKeys =
            await ReadForeignKeysAsync(
                    connection,
                    budget,
                    limits,
                    cancellationToken)
                .ConfigureAwait(false);
        IReadOnlyList<SqlServerForeignKeyColumnMetadata> foreignKeyColumns =
            await ReadForeignKeyColumnsAsync(
                    connection,
                    budget,
                    limits,
                    cancellationToken)
                .ConfigureAwait(false);
        IReadOnlyList<SqlServerCheckMetadata> checks = await ReadChecksAsync(
                connection,
                budget,
                limits,
                cancellationToken)
            .ConfigureAwait(false);
        IReadOnlyList<SqlServerSequenceMetadata> sequences =
            await ReadSequencesAsync(
                    connection,
                    budget,
                    limits,
                    cancellationToken)
                .ConfigureAwait(false);
        SqlServerPermissionAuditMetadata permissionAuditAfter =
            await ReadPermissionAuditAsync(
                    connection,
                    instance,
                    database,
                    budget,
                    limits,
                    cancellationToken)
                .ConfigureAwait(false);

        return new SqlServerCatalogSnapshot(
            endpointDigest,
            ProviderVersion(),
            instance,
            database,
            schemas,
            tables,
            columns,
            keys,
            indexes,
            indexColumns,
            foreignKeys,
            foreignKeyColumns,
            checks,
            sequences,
            permissionAuditBefore,
            permissionAuditAfter);
    }

    private static async ValueTask<(
        SqlServerInstanceMetadata Instance,
        SqlServerDatabaseMetadata Database)> ReadServerAndDatabaseAsync(
        SqlConnection connection,
        ReaderBudget budget,
        CancellationToken cancellationToken)
    {
        await using SqlCommand command = Command(connection, ServerAndDatabaseQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            throw new SqlServerMigrationException(
                "SQL Server did not return metadata for the selected database.");
        }

        string productVersion = RequiredString(reader, 0, budget);
        var instance = new SqlServerInstanceMetadata(
            productVersion,
            RequiredInt32(reader, 1),
            RequiredString(reader, 2, budget),
            RequiredString(reader, 3, budget),
            RequiredInt32(reader, 4));
        var database = new SqlServerDatabaseMetadata(
            RequiredInt32(reader, 5),
            RequiredString(reader, 6, budget, isName: true),
            RequiredInt16(reader, 7),
            OptionalString(reader, 8, budget),
            RequiredBoolean(reader, 9),
            RequiredString(reader, 10, budget),
            RequiredBoolean(reader, 11),
            RequiredBoolean(reader, 12),
            RequiredBoolean(reader, 13),
            RequiredBoolean(reader, 14),
            RequiredBoolean(reader, 15),
            RequiredString(reader, 16, budget),
            RequiredBoolean(reader, 17),
            OptionalBoolean(reader, 18),
            OptionalBoolean(reader, 19),
            OptionalBoolean(reader, 20),
            OptionalBoolean(reader, 21),
            OptionalBoolean(reader, 22));

        if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            throw new SqlServerMigrationException(
                "SQL Server returned ambiguous selected-database metadata.");
        }
        return (instance, database);
    }

    private static async ValueTask<IReadOnlyList<SqlServerSchemaMetadata>> ReadSchemasAsync(
        SqlConnection connection,
        ReaderBudget budget,
        SqlServerInspectionLimits limits,
        CancellationToken cancellationToken)
    {
        var schemas = new List<SqlServerSchemaMetadata>();
        await using SqlCommand command = Command(connection, SchemasQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (schemas.Count == limits.MaxSchemas)
                throw LimitExceeded("schema count");
            schemas.Add(new SqlServerSchemaMetadata(
                RequiredInt32(reader, 0),
                RequiredString(reader, 1, budget, isName: true),
                OptionalBoolean(reader, 2)));
        }
        return schemas.AsReadOnly();
    }

    private static async ValueTask<IReadOnlyList<SqlServerTableMetadata>> ReadTablesAsync(
        SqlConnection connection,
        ReaderBudget budget,
        SqlServerInspectionLimits limits,
        CancellationToken cancellationToken)
    {
        var tables = new List<SqlServerTableMetadata>();
        await using SqlCommand command = Command(connection, TablesQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (tables.Count == limits.MaxTables)
                throw LimitExceeded("table count");
            tables.Add(new SqlServerTableMetadata(
                RequiredInt32(reader, 0),
                RequiredInt32(reader, 1),
                RequiredString(reader, 2, budget, isName: true),
                RequiredBoolean(reader, 3),
                RequiredString(reader, 4, budget),
                RequiredBoolean(reader, 5),
                RequiredString(reader, 6, budget),
                RequiredBoolean(reader, 7),
                RequiredBoolean(reader, 8),
                OptionalBoolean(reader, 9)));
        }
        return tables.AsReadOnly();
    }

    private static async ValueTask<IReadOnlyList<SqlServerColumnMetadata>> ReadColumnsAsync(
        SqlConnection connection,
        ReaderBudget budget,
        SqlServerInspectionLimits limits,
        CancellationToken cancellationToken)
    {
        var columns = new List<SqlServerColumnMetadata>();
        await using SqlCommand command = Command(connection, ColumnsQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (columns.Count == limits.MaxColumns)
                throw LimitExceeded("column count");
            int objectId = RequiredInt32(reader, 0);
            int columnId = RequiredInt32(reader, 1);
            string name = RequiredString(reader, 2, budget, isName: true);
            string typeSchema = RequiredString(reader, 3, budget, isName: true);
            string typeName = RequiredString(reader, 4, budget, isName: true);
            string systemTypeName = RequiredString(reader, 5, budget, isName: true);
            short maxLength = RequiredInt16(reader, 6);
            byte precision = RequiredByte(reader, 7);
            byte scale = RequiredByte(reader, 8);
            string? collation = OptionalString(reader, 9, budget);
            bool isNullable = RequiredBoolean(reader, 10);
            bool isSparse = RequiredBoolean(reader, 11);
            bool isColumnSet = RequiredBoolean(reader, 12);
            bool isHidden = RequiredBoolean(reader, 13);
            bool isComputed = RequiredBoolean(reader, 14);
            bool isFileStream = RequiredBoolean(reader, 15);
            bool isMasked = RequiredBoolean(reader, 16);
            string? encryptionType = OptionalString(reader, 17, budget);
            int xmlCollectionId = RequiredInt32(reader, 18);
            string generatedAlwaysType = RequiredString(reader, 19, budget);
            bool hasDefault = RequiredBoolean(reader, 20);
            string? defaultConstraintName =
                OptionalString(reader, 21, budget, isName: true);
            long? defaultDefinitionBytes = OptionalInt64(reader, 22);
            string? defaultDefinition = OptionalExpression(
                reader,
                23,
                defaultDefinitionBytes,
                budget);
            long? computedDefinitionBytes = OptionalInt64(reader, 24);
            string? computedDefinition = OptionalExpression(
                reader,
                25,
                computedDefinitionBytes,
                budget);
            bool isPersisted = RequiredBoolean(reader, 26);
            bool isIdentity = RequiredBoolean(reader, 27);
            string? identitySeed = OptionalString(reader, 28, budget);
            string? identityIncrement = OptionalString(reader, 29, budget);
            bool identityNotForReplication = RequiredBoolean(reader, 30);

            columns.Add(new SqlServerColumnMetadata(
                objectId,
                columnId,
                name,
                typeSchema,
                typeName,
                systemTypeName,
                maxLength,
                precision,
                scale,
                collation,
                isNullable,
                isSparse,
                isColumnSet,
                isHidden,
                isComputed,
                isFileStream,
                isMasked,
                encryptionType,
                xmlCollectionId,
                generatedAlwaysType,
                hasDefault,
                defaultConstraintName,
                defaultDefinitionBytes,
                defaultDefinition,
                computedDefinitionBytes,
                computedDefinition,
                isPersisted,
                isIdentity,
                identitySeed,
                identityIncrement,
                identityNotForReplication));
        }
        return columns.AsReadOnly();
    }

    private static SqlCommand Command(SqlConnection connection, string commandText) =>
        new(commandText, connection)
        {
            CommandType = CommandType.Text,
            CommandTimeout = CommandTimeoutSeconds,
        };

    private static string RequiredString(
        SqlDataReader reader,
        int ordinal,
        ReaderBudget budget,
        bool isName = false)
    {
        if (reader.IsDBNull(ordinal))
            throw InvalidProviderMetadata();
        string value = reader.GetString(ordinal);
        budget.Add(value, isName);
        return value;
    }

    private static string? OptionalString(
        SqlDataReader reader,
        int ordinal,
        ReaderBudget budget,
        bool isName = false)
    {
        if (reader.IsDBNull(ordinal))
            return null;
        string value = reader.GetString(ordinal);
        budget.Add(value, isName);
        return value;
    }

    private static string? OptionalExpression(
        SqlDataReader reader,
        int valueOrdinal,
        long? sourceBytes,
        ReaderBudget budget)
    {
        if (sourceBytes is null)
        {
            if (!reader.IsDBNull(valueOrdinal))
                throw InvalidProviderMetadata();
            return null;
        }
        if (sourceBytes < 0)
            throw InvalidProviderMetadata();
        budget.ReserveExpression(sourceBytes.Value);
        if (reader.IsDBNull(valueOrdinal))
            throw InvalidProviderMetadata();
        string value = reader.GetString(valueOrdinal);
        budget.AddExpression(value);
        return value;
    }

    private static int RequiredInt32(SqlDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
            throw InvalidProviderMetadata();
        return Convert.ToInt32(reader.GetValue(ordinal), CultureInfo.InvariantCulture);
    }

    private static short RequiredInt16(SqlDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
            throw InvalidProviderMetadata();
        return Convert.ToInt16(reader.GetValue(ordinal), CultureInfo.InvariantCulture);
    }

    private static byte RequiredByte(SqlDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
            throw InvalidProviderMetadata();
        return Convert.ToByte(reader.GetValue(ordinal), CultureInfo.InvariantCulture);
    }

    private static long? OptionalInt64(SqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal)
            ? null
            : Convert.ToInt64(reader.GetValue(ordinal), CultureInfo.InvariantCulture);

    private static int? OptionalInt32(SqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal)
            ? null
            : Convert.ToInt32(reader.GetValue(ordinal), CultureInfo.InvariantCulture);

    private static bool RequiredBoolean(SqlDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
            throw InvalidProviderMetadata();
        return Convert.ToBoolean(reader.GetValue(ordinal), CultureInfo.InvariantCulture);
    }

    private static bool? OptionalBoolean(SqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal)
            ? null
            : Convert.ToBoolean(reader.GetValue(ordinal), CultureInfo.InvariantCulture);

    private static SqlServerMigrationException InvalidProviderMetadata() =>
        new("SQL Server returned incomplete or invalid catalog metadata.");

    private static SqlServerMigrationException LimitExceeded(string category) =>
        new($"SQL Server inspection exceeded the fixed {category} limit.");

    private static string ProviderVersion()
    {
        Assembly assembly = typeof(SqlConnection).Assembly;
        return assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?
                   .InformationalVersion?
                   .Split('+', 2)[0]
               ?? assembly.GetName().Version?.ToString()
               ?? "unknown";
    }

    private sealed class ReaderBudget
    {
        private static readonly UTF8Encoding s_utf8 =
            new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

        private readonly SqlServerInspectionLimits limits;
        private long metadataBytes;
        private long expressionStorageBytes;
        private int structuralRows;
        private int permissionRows;

        public ReaderBudget(SqlServerInspectionLimits limits)
        {
            this.limits = limits;
        }

        public void Add(string value, bool isName = false)
        {
            int bytes;
            try
            {
                bytes = s_utf8.GetByteCount(value);
            }
            catch (EncoderFallbackException)
            {
                throw new SqlServerMigrationException(
                    "SQL Server metadata contains invalid Unicode.");
            }
            if (isName && bytes > limits.MaxNameBytes)
                throw LimitExceeded("identifier byte");
            metadataBytes = checked(metadataBytes + bytes);
            if (metadataBytes > limits.MaxMetadataBytes)
                throw LimitExceeded("metadata byte");
        }

        public void AddExpression(string value)
        {
            int bytes;
            try
            {
                bytes = s_utf8.GetByteCount(value);
            }
            catch (EncoderFallbackException)
            {
                throw new SqlServerMigrationException(
                    "SQL Server metadata contains invalid Unicode.");
            }
            metadataBytes = checked(metadataBytes + bytes);
            if (metadataBytes > limits.MaxMetadataBytes)
                throw LimitExceeded("metadata byte");
        }

        public void ReserveExpression(long sourceBytes)
        {
            if (sourceBytes > limits.MaxExpressionBytes)
                throw LimitExceeded("expression byte");
            expressionStorageBytes = checked(expressionStorageBytes + sourceBytes);
            if (expressionStorageBytes > limits.MaxExpressionBytesTotal)
                throw LimitExceeded("aggregate expression byte");
        }

        public void AddStructuralRow()
        {
            structuralRows = checked(structuralRows + 1);
            if (structuralRows > limits.MaxStructuralRowsTotal)
                throw LimitExceeded("aggregate structural row");
        }

        public void AddPermissionRow()
        {
            permissionRows = checked(permissionRows + 1);
            if (permissionRows > limits.MaxPermissionRowsTotal)
                throw LimitExceeded("aggregate permission row");
        }
    }
}

internal sealed record SqlServerConnectionPolicy(
    string ApplicationIntent,
    bool Pooling,
    bool PersistSecurityInfo,
    bool Enlist,
    bool MultipleActiveResultSets,
    string Encrypt,
    bool TrustServerCertificate,
    int ConnectTimeout,
    int ConnectRetryCount);
