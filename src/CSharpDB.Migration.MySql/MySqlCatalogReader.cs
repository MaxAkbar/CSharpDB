using System.Data;
using System.Globalization;
using System.Reflection;
using System.Text;
using MySqlConnector;

namespace CSharpDB.Migration.MySql;

internal sealed partial class MySqlCatalogReader : IMySqlCatalogReader
{
    private const uint ConnectionTimeoutSeconds = 30;
    private const uint CommandTimeoutSeconds = 30;
    private const int CancellationTimeoutSeconds = 5;

    internal const string ServerAndDatabaseQuery =
        """
        SELECT
            @@version,
            @@version_comment,
            @@character_set_server,
            @@collation_server,
            @@system_time_zone,
            @@lower_case_table_names,
            @@session.sql_mode,
            @@session.character_set_connection,
            @@session.collation_connection,
            @@session.time_zone,
            @@session.character_set_database,
            @@session.collation_database,
            (
                SELECT s.SCHEMA_NAME
                FROM INFORMATION_SCHEMA.SCHEMATA AS s
                WHERE s.SCHEMA_NAME = DATABASE()
                ORDER BY
                    (BINARY s.SCHEMA_NAME = BINARY DATABASE()) DESC,
                    BINARY s.SCHEMA_NAME
                LIMIT 1
            ),
            @@session.sql_quote_show_create,
            @@session.explicit_defaults_for_timestamp;
        """;

    internal const string TablesQuery =
        """
        SELECT
            TABLE_SCHEMA,
            TABLE_NAME,
            TABLE_TYPE,
            ENGINE,
            TABLE_COLLATION,
            CREATE_OPTIONS
        FROM INFORMATION_SCHEMA.TABLES
        WHERE BINARY TABLE_SCHEMA = BINARY @database_name
        ORDER BY
            BINARY TABLE_SCHEMA,
            BINARY TABLE_NAME,
            BINARY TABLE_TYPE;
        """;

    internal const string GeneratedInvisiblePrimaryKeyVisibilityQuery =
        """
        SELECT
            @@session.show_gipk_in_create_table_and_information_schema;
        """;

    internal const string ColumnsQuery =
        """
        SELECT
            c.TABLE_SCHEMA,
            c.TABLE_NAME,
            c.ORDINAL_POSITION,
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.IS_NULLABLE,
            c.CHARACTER_SET_NAME,
            c.COLLATION_NAME,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            c.DATETIME_PRECISION,
            OCTET_LENGTH(c.COLUMN_TYPE),
            c.COLUMN_TYPE,
            c.EXTRA,
            OCTET_LENGTH(c.GENERATION_EXPRESSION),
            c.GENERATION_EXPRESSION,
            OCTET_LENGTH(c.COLUMN_DEFAULT),
            c.COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS AS c
        INNER JOIN INFORMATION_SCHEMA.TABLES AS t
            ON BINARY t.TABLE_SCHEMA = BINARY c.TABLE_SCHEMA
           AND BINARY t.TABLE_NAME = BINARY c.TABLE_NAME
        WHERE BINARY c.TABLE_SCHEMA = BINARY @database_name
          AND t.TABLE_TYPE = 'BASE TABLE'
        ORDER BY
            BINARY c.TABLE_SCHEMA,
            BINARY c.TABLE_NAME,
            c.ORDINAL_POSITION,
            BINARY c.COLUMN_NAME;
        """;

    internal static IReadOnlyList<string> CommandTexts { get; } =
        Array.AsReadOnly(
        [
            ServerAndDatabaseQuery,
            GeneratedInvisiblePrimaryKeyVisibilityQuery,
            TablesQuery,
            ColumnsQuery,
            KeysQuery,
            KeyColumnsQuery,
            ForeignKeysQuery,
            ForeignKeyColumnsQuery,
            ChecksQuery,
            IndexesQuery,
            LegacyIndexesQuery,
            UnqualifiedIndexesQuery,
            ViewsQuery,
            ViewColumnsQuery,
            TriggersQuery,
            RoutinesQuery,
            RoutineParametersQuery,
        ]);

    private readonly string connectionString;
    private readonly string endpointDigest;

    public MySqlCatalogReader(string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        MySqlConnectionStringBuilder settings;
        try
        {
            settings = new MySqlConnectionStringBuilder(connectionString);
        }
        catch (ArgumentException)
        {
            throw new ArgumentException(
                "The MySQL connection settings are invalid.",
                nameof(connectionString));
        }

        if (string.IsNullOrWhiteSpace(settings.Server))
        {
            throw new ArgumentException(
                "The MySQL connection settings must select a server.",
                nameof(connectionString));
        }
        if (string.IsNullOrWhiteSpace(settings.Database))
        {
            throw new ArgumentException(
                "The MySQL connection settings must select one database.",
                nameof(connectionString));
        }
        string server = settings.Server.Trim();
        if (server.Contains(',', StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "The MySQL connection settings must select one server.",
                nameof(connectionString));
        }
        bool localTransport =
            settings.ConnectionProtocol is MySqlConnectionProtocol.Pipe or
                MySqlConnectionProtocol.UnixSocket or
                MySqlConnectionProtocol.SharedMemory ||
            settings.ConnectionProtocol == MySqlConnectionProtocol.Sockets &&
            server.IndexOfAny(['/', '\\']) >= 0;
        if (!localTransport &&
            settings.SslMode is MySqlSslMode.None or MySqlSslMode.Preferred)
        {
            throw new ArgumentException(
                "The MySQL connection settings must require TLS for a TCP server.",
                nameof(connectionString));
        }

        try
        {
            endpointDigest = "sha256:" + MySqlStableDigest.Text(
                "csharpdb-mysql-endpoint/v1",
                settings.ConnectionProtocol.ToString(),
                NormalizeServerIdentity(
                    server,
                    settings.ConnectionProtocol),
                settings.Port.ToString(CultureInfo.InvariantCulture),
                settings.PipeName);
        }
        catch (EncoderFallbackException)
        {
            throw new ArgumentException(
                "The MySQL connection settings are invalid.",
                nameof(connectionString));
        }

        settings.Pooling = false;
        settings.AllowLoadLocalInfile = false;
        settings.AllowUserVariables = false;
        settings.AutoEnlist = false;
        settings.PersistSecurityInfo = false;
        if (settings.ConnectionTimeout is 0 or > ConnectionTimeoutSeconds)
            settings.ConnectionTimeout = ConnectionTimeoutSeconds;
        if (settings.DefaultCommandTimeout is 0 or > CommandTimeoutSeconds)
            settings.DefaultCommandTimeout = CommandTimeoutSeconds;
        if (settings.CancellationTimeout is <= 0 or > CancellationTimeoutSeconds)
            settings.CancellationTimeout = CancellationTimeoutSeconds;

        this.connectionString = settings.ConnectionString;
        Policy = new MySqlConnectionPolicy(
            settings.Pooling,
            settings.AllowLoadLocalInfile,
            settings.AllowUserVariables,
            settings.AutoEnlist,
            settings.PersistSecurityInfo,
            settings.ConnectionTimeout,
            settings.DefaultCommandTimeout,
            settings.CancellationTimeout,
            settings.SslMode.ToString());
    }

    internal MySqlConnectionPolicy Policy { get; }

    internal string EndpointDigest => endpointDigest;

    public async ValueTask<MySqlCatalogSnapshot> ReadAsync(
        MySqlInspectionLimits limits,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(limits);
        limits.Validate();
        cancellationToken.ThrowIfCancellationRequested();

        await using var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        if (string.IsNullOrWhiteSpace(connection.Database))
            throw InvalidProviderMetadata();

        var budget = new ReaderBudget(limits);
        (
            MySqlServerMetadata server,
            MySqlSessionMetadata session,
            string defaultCharacterSet,
            string defaultCollation,
            string selectedDatabase
        ) = await ReadServerAndDatabaseAsync(
                connection,
                budget,
            cancellationToken)
            .ConfigureAwait(false);
        bool? showGeneratedInvisiblePrimaryKey =
            ShouldReadGeneratedInvisiblePrimaryKeyVisibility(server)
                ? await ReadGeneratedInvisiblePrimaryKeyVisibilityAsync(
                        connection,
                        cancellationToken)
                    .ConfigureAwait(false)
                : null;
        server = server with
        {
            ShowGeneratedInvisiblePrimaryKey =
                showGeneratedInvisiblePrimaryKey,
        };
        (
            IReadOnlyList<MySqlTableMetadata> tables,
            IReadOnlyList<MySqlViewMetadata> visibleViews
        ) =
            await ReadTablesAsync(
                    connection,
                    selectedDatabase,
                    budget,
                    limits,
                    cancellationToken)
                .ConfigureAwait(false);
        IReadOnlyList<MySqlColumnMetadata> columns = await ReadColumnsAsync(
                connection,
                selectedDatabase,
                tables,
                budget,
                limits,
                cancellationToken)
            .ConfigureAwait(false);
        IReadOnlyList<MySqlViewMetadata> views = await ReadViewsAsync(
                connection,
                selectedDatabase,
                visibleViews,
                budget,
                limits,
                cancellationToken)
            .ConfigureAwait(false);
        IReadOnlyList<MySqlViewColumnMetadata> viewColumns =
            await ReadViewColumnsAsync(
                    connection,
                    selectedDatabase,
                    views,
                    budget,
                    limits,
                    cancellationToken)
                .ConfigureAwait(false);
        IReadOnlyList<MySqlTriggerMetadata> triggers =
            await ReadTriggersAsync(
                    connection,
                    selectedDatabase,
                    tables,
                    budget,
                    limits,
                    cancellationToken)
                .ConfigureAwait(false);
        IReadOnlyList<MySqlRoutineMetadata> routines =
            await ReadRoutinesAsync(
                    connection,
                    selectedDatabase,
                    budget,
                    limits,
                    cancellationToken)
                .ConfigureAwait(false);
        IReadOnlyList<MySqlRoutineParameterMetadata> routineParameters =
            await ReadRoutineParametersAsync(
                    connection,
                    selectedDatabase,
                    routines,
                    budget,
                    limits,
                    cancellationToken)
                .ConfigureAwait(false);
        IReadOnlyList<MySqlKeyMetadata> keys = await ReadKeysAsync(
                connection,
                selectedDatabase,
                tables,
                budget,
                limits,
                cancellationToken)
            .ConfigureAwait(false);
        IReadOnlyList<MySqlKeyColumnMetadata> keyColumns =
            await ReadKeyColumnsAsync(
                    connection,
                    selectedDatabase,
                    tables,
                    columns,
                    keys,
                    budget,
                    limits,
                    cancellationToken)
                .ConfigureAwait(false);
        IReadOnlyList<MySqlForeignKeyMetadata> foreignKeys =
            await ReadForeignKeysAsync(
                    connection,
                    selectedDatabase,
                    tables,
                    budget,
                    limits,
                    cancellationToken)
                .ConfigureAwait(false);
        IReadOnlyList<MySqlForeignKeyColumnMetadata> foreignKeyColumns =
            await ReadForeignKeyColumnsAsync(
                    connection,
                    selectedDatabase,
                    tables,
                    columns,
                    foreignKeys,
                    budget,
                    limits,
                    cancellationToken)
                .ConfigureAwait(false);
        IReadOnlyList<MySqlCheckMetadata> checks =
            ShouldReadCheckConstraints(server)
                ? await ReadChecksAsync(
                        connection,
                        selectedDatabase,
                        tables,
                        budget,
                        limits,
                        cancellationToken)
                    .ConfigureAwait(false)
                : [];
        (
            IReadOnlyList<MySqlIndexMetadata> indexes,
            IReadOnlyList<MySqlIndexPartMetadata> indexParts
        ) = await ReadIndexesAsync(
                connection,
                selectedDatabase,
                tables,
                columns,
                server,
                budget,
                limits,
                cancellationToken)
            .ConfigureAwait(false);
        IReadOnlyList<MySqlTableDefinitionMetadata> tableDefinitions =
            await ReadTableDefinitionsAsync(
                    connection,
                    tables,
                    budget,
                    limits,
                    cancellationToken)
                .ConfigureAwait(false);

        return new MySqlCatalogSnapshot(
            endpointDigest,
            ProviderVersion(),
            server,
            session,
            new MySqlDatabaseMetadata(
                selectedDatabase,
                defaultCharacterSet,
                defaultCollation,
                views.Count),
            tables,
            columns,
            tableDefinitions,
            keys,
            keyColumns,
            foreignKeys,
            foreignKeyColumns,
            checks,
            indexes,
            indexParts,
            views,
            viewColumns,
            triggers,
            routines,
            routineParameters);
    }

    private async ValueTask<(
        MySqlServerMetadata Server,
        MySqlSessionMetadata Session,
        string DefaultCharacterSet,
        string DefaultCollation,
        string SelectedDatabase)> ReadServerAndDatabaseAsync(
        MySqlConnection connection,
        ReaderBudget budget,
        CancellationToken cancellationToken)
    {
        await using MySqlCommand command = Command(
            connection,
            ServerAndDatabaseQuery);
        await using MySqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            throw InvalidProviderMetadata();

        string version = RequiredString(reader, 0, budget);
        string versionComment = RequiredString(reader, 1, budget);
        string characterSetServer = RequiredString(reader, 2, budget);
        string collationServer = RequiredString(reader, 3, budget);
        string systemTimeZone = RequiredString(reader, 4, budget);
        int lowerCaseTableNames = RequiredInt32(reader, 5);
        if (lowerCaseTableNames is < 0 or > 2)
            throw InvalidProviderMetadata();
        string sqlMode = RequiredString(reader, 6, budget);
        string characterSetConnection = RequiredString(reader, 7, budget);
        string collationConnection = RequiredString(reader, 8, budget);
        string timeZone = RequiredString(reader, 9, budget);
        string defaultCharacterSet = RequiredString(reader, 10, budget);
        string defaultCollation = RequiredString(reader, 11, budget);
        string selectedDatabase = RequiredString(
            reader,
            12,
            budget,
            isName: true);
        bool sqlQuoteShowCreate = RequiredBoolean(reader, 13);
        bool explicitDefaultsForTimestamp = RequiredBoolean(reader, 14);

        if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            throw InvalidProviderMetadata();

        return (
            new MySqlServerMetadata(
                version,
                versionComment,
                characterSetServer,
                collationServer,
                systemTimeZone,
                lowerCaseTableNames,
                ShowGeneratedInvisiblePrimaryKey: null),
            new MySqlSessionMetadata(
                sqlMode,
                characterSetConnection,
                collationConnection,
                timeZone,
                sqlQuoteShowCreate,
                explicitDefaultsForTimestamp),
            defaultCharacterSet,
            defaultCollation,
            selectedDatabase);
    }

    private static async ValueTask<bool>
        ReadGeneratedInvisiblePrimaryKeyVisibilityAsync(
            MySqlConnection connection,
            CancellationToken cancellationToken)
    {
        await using MySqlCommand command = Command(
            connection,
            GeneratedInvisiblePrimaryKeyVisibilityQuery);
        object? value = await command.ExecuteScalarAsync(cancellationToken)
            .ConfigureAwait(false);
        if (value is null || value is DBNull)
            throw InvalidProviderMetadata();
        int numeric = Convert.ToInt32(value, CultureInfo.InvariantCulture);
        return numeric switch
        {
            0 => false,
            1 => true,
            _ => throw InvalidProviderMetadata(),
        };
    }

    private async ValueTask<(
        IReadOnlyList<MySqlTableMetadata> Tables,
        IReadOnlyList<MySqlViewMetadata> Views)> ReadTablesAsync(
        MySqlConnection connection,
        string selectedDatabase,
        ReaderBudget budget,
        MySqlInspectionLimits limits,
        CancellationToken cancellationToken)
    {
        await using MySqlCommand command = Command(connection, TablesQuery);
        AddDatabaseParameter(command, selectedDatabase);
        await using MySqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        var tables = new List<MySqlTableMetadata>();
        var views = new List<MySqlViewMetadata>();
        var identities = new HashSet<string>(StringComparer.Ordinal);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            budget.AddStructuralRow();
            string schemaName = RequiredString(reader, 0, budget, isName: true);
            string name = RequiredString(reader, 1, budget, isName: true);
            string tableType = RequiredString(reader, 2, budget);
            string? engine = OptionalString(reader, 3, budget);
            string? tableCollation = OptionalString(reader, 4, budget);
            string? createOptions = OptionalString(reader, 5, budget);
            if (!string.Equals(
                    schemaName,
                    selectedDatabase,
                    StringComparison.Ordinal))
            {
                throw InvalidProviderMetadata();
            }

            string identity = schemaName + "\0" + name;
            if (!identities.Add(identity))
                throw InvalidProviderMetadata();

            if (string.Equals(tableType, "VIEW", StringComparison.Ordinal))
            {
                if (views.Count >= limits.MaxViews)
                    throw LimitExceeded("view");
                views.Add(new MySqlViewMetadata(
                    schemaName,
                    name,
                    MetadataVisible: false,
                    DefinitionBytes: null,
                    Definition: null,
                    CheckOption: null,
                    IsUpdatable: null,
                    SecurityType: null,
                    CharacterSetClient: null,
                    CollationConnection: null));
                continue;
            }
            if (!string.Equals(
                    tableType,
                    "BASE TABLE",
                    StringComparison.Ordinal))
            {
                throw InvalidProviderMetadata();
            }
            if (tables.Count >= limits.MaxTables)
                throw LimitExceeded("table");

            tables.Add(
                new MySqlTableMetadata(
                    schemaName,
                    name,
                    tableType,
                    engine,
                    tableCollation,
                    createOptions,
                    HasToken(createOptions, "partitioned")));
        }
        return (tables.AsReadOnly(), views.AsReadOnly());
    }

    private async ValueTask<IReadOnlyList<MySqlColumnMetadata>> ReadColumnsAsync(
        MySqlConnection connection,
        string selectedDatabase,
        IReadOnlyList<MySqlTableMetadata> tables,
        ReaderBudget budget,
        MySqlInspectionLimits limits,
        CancellationToken cancellationToken)
    {
        var tableIdentities = new HashSet<string>(
            tables.Select(static table => table.SchemaName + "\0" + table.Name),
            StringComparer.Ordinal);
        var columnIdentities = new HashSet<string>(StringComparer.Ordinal);
        var columnNameIdentities = new HashSet<string>(StringComparer.Ordinal);
        await using MySqlCommand command = Command(connection, ColumnsQuery);
        AddDatabaseParameter(command, selectedDatabase);
        await using MySqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        var columns = new List<MySqlColumnMetadata>();
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (columns.Count >= limits.MaxColumns)
                throw LimitExceeded("column");
            budget.AddStructuralRow();

            string schemaName = RequiredString(reader, 0, budget, isName: true);
            string tableName = RequiredString(reader, 1, budget, isName: true);
            int ordinalPosition = RequiredInt32(reader, 2);
            if (ordinalPosition <= 0 ||
                !tableIdentities.Contains(schemaName + "\0" + tableName))
            {
                throw InvalidProviderMetadata();
            }
            string name = RequiredString(reader, 3, budget, isName: true);
            string dataType = RequiredString(reader, 4, budget);
            bool isNullable = ParseYesNo(
                RequiredString(reader, 5, budget));
            string? characterSetName = OptionalString(reader, 6, budget);
            string? collationName = OptionalString(reader, 7, budget);
            long? characterMaximumLength = OptionalInt64(reader, 8);
            int? numericPrecision = OptionalInt32(reader, 9);
            int? numericScale = OptionalInt32(reader, 10);
            int? dateTimePrecision = OptionalInt32(reader, 11);
            long? sourceColumnTypeBytes = OptionalInt64(reader, 12);
            budget.PreflightColumnType(sourceColumnTypeBytes);
            if (reader.IsDBNull(13))
                throw InvalidProviderMetadata();
            string columnType = reader.GetString(13);
            long columnTypeBytes = budget.ValidateColumnType(
                columnType,
                sourceColumnTypeBytes);
            string extra = RequiredString(reader, 14, budget);
            long? sourceExpressionBytes = OptionalInt64(reader, 15);
            bool hasGenerationExpression = !reader.IsDBNull(16);
            if (hasGenerationExpression && sourceExpressionBytes is null)
                throw InvalidProviderMetadata();
            budget.PreflightExpression(sourceExpressionBytes);
            string? generationExpression = hasGenerationExpression
                ? reader.GetString(16)
                : null;
            long? sourceDefaultBytes = OptionalInt64(reader, 17);
            bool hasDefaultValue = !reader.IsDBNull(18);
            if (hasDefaultValue && sourceDefaultBytes is null)
                throw InvalidProviderMetadata();
            budget.PreflightExpression(sourceDefaultBytes);
            string? defaultValue = hasDefaultValue
                ? reader.GetString(18)
                : null;

            string generationKind = GenerationKind(extra);
            bool isGenerated = !string.Equals(
                generationKind,
                "NEVER",
                StringComparison.Ordinal);
            long? expressionBytes = null;
            if (string.IsNullOrEmpty(generationExpression))
            {
                generationExpression = null;
                if (isGenerated || sourceExpressionBytes is not (null or 0))
                    throw InvalidProviderMetadata();
            }
            else
            {
                if (!isGenerated)
                    throw InvalidProviderMetadata();
                expressionBytes = budget.AddExpression(
                    generationExpression,
                    sourceExpressionBytes);
            }

            long? defaultBytes = null;
            if (defaultValue is null)
            {
                if (sourceDefaultBytes is not null)
                    throw InvalidProviderMetadata();
            }
            else
            {
                defaultBytes = budget.AddExpression(
                    defaultValue,
                    sourceDefaultBytes);
            }

            string columnIdentity =
                schemaName + "\0" + tableName + "\0" +
                ordinalPosition.ToString(CultureInfo.InvariantCulture);
            string columnNameIdentity =
                schemaName + "\0" + tableName + "\0" + name;
            if (!columnIdentities.Add(columnIdentity) ||
                !columnNameIdentities.Add(columnNameIdentity))
                throw InvalidProviderMetadata();

            bool numeric = IsNumericType(dataType);
            columns.Add(
                new MySqlColumnMetadata(
                    schemaName,
                    tableName,
                    ordinalPosition,
                    name,
                    dataType,
                    columnTypeBytes,
                    columnType,
                    isNullable,
                    characterSetName,
                    collationName,
                    characterMaximumLength,
                    numericPrecision,
                    numericScale,
                    dateTimePrecision,
                    numeric && HasToken(columnType, "unsigned"),
                    numeric && HasToken(columnType, "zerofill"),
                    IsTinyIntOne(dataType, columnType),
                    HasToken(extra, "auto_increment"),
                    isGenerated,
                    generationKind,
                    expressionBytes,
                    generationExpression,
                    HasToken(extra, "invisible"),
                    defaultBytes,
                    defaultValue,
                    HasToken(extra, "DEFAULT_GENERATED"),
                    HasSequence(
                        extra,
                        "on",
                        "update",
                        "CURRENT_TIMESTAMP")));
        }
        return columns.AsReadOnly();
    }

    private static void AddDatabaseParameter(
        MySqlCommand command,
        string selectedDatabase)
    {
        MySqlParameter parameter = command.Parameters.Add(
            "@database_name",
            MySqlDbType.VarChar);
        parameter.Value = selectedDatabase;
    }

    private static MySqlCommand Command(
        MySqlConnection connection,
        string commandText) =>
        new(commandText, connection)
        {
            CommandType = CommandType.Text,
            CommandTimeout = checked((int)CommandTimeoutSeconds),
        };

    private static string RequiredString(
        MySqlDataReader reader,
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
        MySqlDataReader reader,
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

    private static int RequiredInt32(MySqlDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
            throw InvalidProviderMetadata();
        return Convert.ToInt32(
            reader.GetValue(ordinal),
            CultureInfo.InvariantCulture);
    }

    private static long? OptionalInt64(MySqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal)
            ? null
            : Convert.ToInt64(
                reader.GetValue(ordinal),
                CultureInfo.InvariantCulture);

    private static int? OptionalInt32(MySqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal)
            ? null
            : Convert.ToInt32(
                reader.GetValue(ordinal),
                CultureInfo.InvariantCulture);

    private static bool RequiredBoolean(MySqlDataReader reader, int ordinal)
    {
        int value = RequiredInt32(reader, ordinal);
        return value switch
        {
            0 => false,
            1 => true,
            _ => throw InvalidProviderMetadata(),
        };
    }

    private static bool ParseYesNo(string value) =>
        value switch
        {
            "YES" => true,
            "NO" => false,
            _ => throw InvalidProviderMetadata(),
        };

    private static string GenerationKind(string extra)
    {
        if (HasPhrase(extra, "STORED", "GENERATED"))
            return "STORED GENERATED";
        if (HasPhrase(extra, "VIRTUAL", "GENERATED"))
            return "VIRTUAL GENERATED";
        return "NEVER";
    }

    private static bool HasPhrase(
        string value,
        string first,
        string second)
    {
        string[] tokens = Tokens(value);
        for (int index = 0; index + 1 < tokens.Length; index++)
        {
            if (string.Equals(
                    tokens[index],
                    first,
                    StringComparison.OrdinalIgnoreCase) &&
                string.Equals(
                    tokens[index + 1],
                    second,
                    StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }
        return false;
    }

    private static bool HasSequence(
        string value,
        params string[] expected)
    {
        string[] tokens = Tokens(value);
        if (expected.Length == 0 || expected.Length > tokens.Length)
            return false;
        for (int start = 0; start <= tokens.Length - expected.Length; start++)
        {
            bool matches = true;
            for (int offset = 0; offset < expected.Length; offset++)
            {
                if (!SequenceTokenEquals(
                        tokens[start + offset],
                        expected[offset]))
                {
                    matches = false;
                    break;
                }
            }
            if (matches)
                return true;
        }
        return false;
    }

    private static bool SequenceTokenEquals(
        string actual,
        string expected)
    {
        if (string.Equals(
                actual,
                expected,
                StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }
        const string currentTimestamp = "CURRENT_TIMESTAMP";
        if (!string.Equals(
                expected,
                currentTimestamp,
                StringComparison.OrdinalIgnoreCase) ||
            !actual.StartsWith(
                currentTimestamp + "(",
                StringComparison.OrdinalIgnoreCase) ||
            !actual.EndsWith(')'))
        {
            return false;
        }

        string precision = actual[(currentTimestamp.Length + 1)..^1];
        return precision.Length == 0 ||
               precision.Length == 1 && precision[0] is >= '0' and <= '6';
    }

    private static bool HasToken(string? value, string expected) =>
        value is not null &&
        Tokens(value).Any(
            token => string.Equals(
                token,
                expected,
                StringComparison.OrdinalIgnoreCase));

    private static string[] Tokens(string value) =>
        value.Split(
            (char[]?)null,
            StringSplitOptions.RemoveEmptyEntries |
            StringSplitOptions.TrimEntries);

    private static bool IsNumericType(string dataType) =>
        dataType.Equals("tinyint", StringComparison.OrdinalIgnoreCase) ||
        dataType.Equals("smallint", StringComparison.OrdinalIgnoreCase) ||
        dataType.Equals("mediumint", StringComparison.OrdinalIgnoreCase) ||
        dataType.Equals("int", StringComparison.OrdinalIgnoreCase) ||
        dataType.Equals("integer", StringComparison.OrdinalIgnoreCase) ||
        dataType.Equals("bigint", StringComparison.OrdinalIgnoreCase) ||
        dataType.Equals("decimal", StringComparison.OrdinalIgnoreCase) ||
        dataType.Equals("numeric", StringComparison.OrdinalIgnoreCase) ||
        dataType.Equals("float", StringComparison.OrdinalIgnoreCase) ||
        dataType.Equals("double", StringComparison.OrdinalIgnoreCase) ||
        dataType.Equals("real", StringComparison.OrdinalIgnoreCase);

    private static bool IsTinyIntOne(string dataType, string columnType)
    {
        if (!dataType.Equals("tinyint", StringComparison.OrdinalIgnoreCase) ||
            !columnType.StartsWith("tinyint(1)", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }
        return columnType.Length == "tinyint(1)".Length ||
               char.IsWhiteSpace(columnType["tinyint(1)".Length]);
    }

    private static string NormalizeServerIdentity(
        string server,
        MySqlConnectionProtocol protocol) =>
        protocol == MySqlConnectionProtocol.Sockets &&
        server.IndexOfAny(['/', '\\']) < 0
            ? server.ToUpperInvariant()
            : server;

    private static bool ShouldReadGeneratedInvisiblePrimaryKeyVisibility(
        MySqlServerMetadata server)
    {
        string identity = string.Concat(
            server.Version,
            " ",
            server.VersionComment).ToLowerInvariant();
        if (!identity.Contains("mysql", StringComparison.Ordinal) ||
            identity.Contains("mariadb", StringComparison.Ordinal) ||
            identity.Contains("aurora", StringComparison.Ordinal) ||
            identity.Contains("percona", StringComparison.Ordinal) ||
            identity.Contains("tidb", StringComparison.Ordinal) ||
            identity.Contains("vitess", StringComparison.Ordinal) ||
            identity.Contains("heatwave", StringComparison.Ordinal))
        {
            return false;
        }

        string numeric = server.Version.Split('-', 2)[0];
        return Version.TryParse(numeric, out Version? version) &&
               (version.Major > 8 ||
                version.Major == 8 &&
                (version.Minor > 0 ||
                 version.Minor == 0 && version.Build >= 30));
    }

    private static MySqlMigrationException InvalidProviderMetadata() =>
        new("MySQL returned incomplete or invalid catalog metadata.");

    private static MySqlMigrationException LimitExceeded(string category) =>
        new($"MySQL inspection exceeded the fixed {category} limit.");

    private static string ProviderVersion()
    {
        Assembly assembly = typeof(MySqlConnection).Assembly;
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
        private readonly MySqlInspectionLimits limits;
        private long definitionBytes;
        private long expressionBytes;
        private long metadataBytes;
        private int structuralRows;

        public ReaderBudget(MySqlInspectionLimits limits)
        {
            this.limits = limits;
        }

        public int Add(string value, bool isName = false)
        {
            int bytes = ByteCount(value);
            if (isName && bytes > limits.MaxNameBytes)
                throw LimitExceeded("identifier byte");
            AddMetadata(bytes);
            return bytes;
        }

        public void PreflightColumnType(long? sourceBytes)
        {
            if (sourceBytes is null or < 0)
                throw InvalidProviderMetadata();
            if (sourceBytes > limits.MaxColumnTypeBytes)
                throw LimitExceeded("column type byte");
        }

        public long ValidateColumnType(
            string value,
            long? sourceBytes)
        {
            int bytes = ByteCount(value);
            PreflightColumnType(sourceBytes);
            if (sourceBytes != bytes)
                throw InvalidProviderMetadata();
            AddMetadata(bytes);
            return bytes;
        }

        public void PreflightExpression(long? sourceBytes)
        {
            if (sourceBytes is null)
                return;
            if (sourceBytes < 0)
                throw InvalidProviderMetadata();
            if (sourceBytes > limits.MaxExpressionBytes)
                throw LimitExceeded("expression byte");
            if (sourceBytes >
                limits.MaxExpressionBytesTotal - expressionBytes)
            {
                throw LimitExceeded("aggregate expression byte");
            }
        }

        public long AddExpression(string value, long? sourceBytes)
        {
            PreflightExpression(sourceBytes);
            int bytes = ByteCount(value);
            if (sourceBytes is null || sourceBytes.Value != bytes)
            {
                throw InvalidProviderMetadata();
            }
            expressionBytes = checked(expressionBytes + bytes);
            AddMetadata(bytes);
            return bytes;
        }

        public void PreflightDefinition(long? sourceBytes)
        {
            if (sourceBytes is null)
                return;
            if (sourceBytes < 0)
                throw InvalidProviderMetadata();
            if (sourceBytes > limits.MaxDefinitionBytes)
                throw LimitExceeded("definition byte");
            if (sourceBytes >
                limits.MaxDefinitionBytesTotal - definitionBytes)
            {
                throw LimitExceeded("aggregate definition byte");
            }
        }

        public long AddDefinition(string value, long? sourceBytes)
        {
            PreflightDefinition(sourceBytes);
            int bytes = ByteCount(value);
            if (sourceBytes is null || sourceBytes < 0 ||
                sourceBytes.Value != bytes)
            {
                throw InvalidProviderMetadata();
            }
            definitionBytes = checked(definitionBytes + bytes);
            AddMetadata(bytes);
            return bytes;
        }

        public void AddStructuralRow()
        {
            structuralRows = checked(structuralRows + 1);
            if (structuralRows > limits.MaxStructuralRowsTotal)
                throw LimitExceeded("aggregate structural row");
        }

        private static int ByteCount(string value)
        {
            try
            {
                return s_utf8.GetByteCount(value);
            }
            catch (EncoderFallbackException)
            {
                throw new MySqlMigrationException(
                    "MySQL metadata contains invalid Unicode.");
            }
        }

        private void AddMetadata(int bytes)
        {
            metadataBytes = checked(metadataBytes + bytes);
            if (metadataBytes > limits.MaxMetadataBytes)
                throw LimitExceeded("metadata byte");
        }
    }
}

internal sealed record MySqlConnectionPolicy(
    bool Pooling,
    bool AllowLoadLocalInfile,
    bool AllowUserVariables,
    bool AutoEnlist,
    bool PersistSecurityInfo,
    uint ConnectionTimeout,
    uint DefaultCommandTimeout,
    int CancellationTimeout,
    string SslMode);
