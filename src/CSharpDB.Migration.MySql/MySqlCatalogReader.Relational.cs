using System.Data;
using System.Globalization;
using MySqlConnector;

namespace CSharpDB.Migration.MySql;

internal sealed partial class MySqlCatalogReader
{
    internal const string KeysQuery =
        """
        SELECT
            tc.TABLE_SCHEMA,
            tc.TABLE_NAME,
            tc.CONSTRAINT_NAME,
            tc.CONSTRAINT_TYPE
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
        INNER JOIN INFORMATION_SCHEMA.TABLES AS t
            ON BINARY t.TABLE_SCHEMA = BINARY tc.TABLE_SCHEMA
           AND BINARY t.TABLE_NAME = BINARY tc.TABLE_NAME
        WHERE BINARY tc.TABLE_SCHEMA = BINARY @database_name
          AND t.TABLE_TYPE = 'BASE TABLE'
          AND tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')
        ORDER BY
            BINARY tc.TABLE_SCHEMA,
            BINARY tc.TABLE_NAME,
            BINARY tc.CONSTRAINT_NAME,
            BINARY tc.CONSTRAINT_TYPE;
        """;

    internal const string KeyColumnsQuery =
        """
        SELECT
            kcu.TABLE_SCHEMA,
            kcu.TABLE_NAME,
            kcu.CONSTRAINT_NAME,
            kcu.ORDINAL_POSITION,
            kcu.COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
        INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
            ON BINARY tc.CONSTRAINT_SCHEMA = BINARY kcu.CONSTRAINT_SCHEMA
           AND BINARY tc.TABLE_SCHEMA = BINARY kcu.TABLE_SCHEMA
           AND BINARY tc.TABLE_NAME = BINARY kcu.TABLE_NAME
           AND BINARY tc.CONSTRAINT_NAME = BINARY kcu.CONSTRAINT_NAME
        WHERE BINARY kcu.TABLE_SCHEMA = BINARY @database_name
          AND tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')
        ORDER BY
            BINARY kcu.TABLE_SCHEMA,
            BINARY kcu.TABLE_NAME,
            BINARY kcu.CONSTRAINT_NAME,
            kcu.ORDINAL_POSITION,
            BINARY kcu.COLUMN_NAME;
        """;

    internal const string ForeignKeysQuery =
        """
        SELECT
            rc.CONSTRAINT_SCHEMA,
            rc.TABLE_NAME,
            rc.CONSTRAINT_NAME,
            kcu.REFERENCED_TABLE_SCHEMA,
            rc.REFERENCED_TABLE_NAME,
            rc.UNIQUE_CONSTRAINT_SCHEMA,
            rc.UNIQUE_CONSTRAINT_NAME,
            rc.MATCH_OPTION,
            rc.UPDATE_RULE,
            rc.DELETE_RULE
        FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS rc
        INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
            ON BINARY tc.CONSTRAINT_SCHEMA = BINARY rc.CONSTRAINT_SCHEMA
           AND BINARY tc.TABLE_SCHEMA = BINARY rc.CONSTRAINT_SCHEMA
           AND BINARY tc.TABLE_NAME = BINARY rc.TABLE_NAME
           AND BINARY tc.CONSTRAINT_NAME = BINARY rc.CONSTRAINT_NAME
        INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
            ON BINARY kcu.CONSTRAINT_SCHEMA = BINARY rc.CONSTRAINT_SCHEMA
           AND BINARY kcu.TABLE_SCHEMA = BINARY rc.CONSTRAINT_SCHEMA
           AND BINARY kcu.TABLE_NAME = BINARY rc.TABLE_NAME
           AND BINARY kcu.CONSTRAINT_NAME = BINARY rc.CONSTRAINT_NAME
           AND kcu.ORDINAL_POSITION = 1
        WHERE BINARY rc.CONSTRAINT_SCHEMA = BINARY @database_name
          AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
        ORDER BY
            BINARY rc.CONSTRAINT_SCHEMA,
            BINARY rc.TABLE_NAME,
            BINARY rc.CONSTRAINT_NAME;
        """;

    internal const string ForeignKeyColumnsQuery =
        """
        SELECT
            kcu.TABLE_SCHEMA,
            kcu.TABLE_NAME,
            kcu.CONSTRAINT_NAME,
            kcu.ORDINAL_POSITION,
            kcu.COLUMN_NAME,
            kcu.POSITION_IN_UNIQUE_CONSTRAINT,
            kcu.REFERENCED_TABLE_SCHEMA,
            kcu.REFERENCED_TABLE_NAME,
            kcu.REFERENCED_COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
        INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
            ON BINARY tc.CONSTRAINT_SCHEMA = BINARY kcu.CONSTRAINT_SCHEMA
           AND BINARY tc.TABLE_SCHEMA = BINARY kcu.TABLE_SCHEMA
           AND BINARY tc.TABLE_NAME = BINARY kcu.TABLE_NAME
           AND BINARY tc.CONSTRAINT_NAME = BINARY kcu.CONSTRAINT_NAME
        WHERE BINARY kcu.TABLE_SCHEMA = BINARY @database_name
          AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
        ORDER BY
            BINARY kcu.TABLE_SCHEMA,
            BINARY kcu.TABLE_NAME,
            BINARY kcu.CONSTRAINT_NAME,
            kcu.ORDINAL_POSITION,
            BINARY kcu.COLUMN_NAME;
        """;

    internal const string ChecksQuery =
        """
        SELECT
            tc.CONSTRAINT_SCHEMA,
            tc.TABLE_NAME,
            tc.CONSTRAINT_NAME,
            tc.ENFORCED,
            OCTET_LENGTH(cc.CHECK_CLAUSE),
            cc.CHECK_CLAUSE
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
        INNER JOIN INFORMATION_SCHEMA.CHECK_CONSTRAINTS AS cc
            ON BINARY cc.CONSTRAINT_SCHEMA = BINARY tc.CONSTRAINT_SCHEMA
           AND BINARY cc.CONSTRAINT_NAME = BINARY tc.CONSTRAINT_NAME
        WHERE BINARY tc.CONSTRAINT_SCHEMA = BINARY @database_name
          AND tc.CONSTRAINT_TYPE = 'CHECK'
        ORDER BY
            BINARY tc.CONSTRAINT_SCHEMA,
            BINARY tc.TABLE_NAME,
            BINARY tc.CONSTRAINT_NAME;
        """;

    internal const string IndexesQuery =
        """
        SELECT
            s.TABLE_SCHEMA,
            s.TABLE_NAME,
            s.INDEX_NAME,
            s.NON_UNIQUE,
            s.INDEX_TYPE,
            s.IS_VISIBLE,
            s.SEQ_IN_INDEX,
            s.COLUMN_NAME,
            s.COLLATION,
            s.SUB_PART,
            OCTET_LENGTH(s.EXPRESSION),
            s.EXPRESSION
        FROM INFORMATION_SCHEMA.STATISTICS AS s
        INNER JOIN INFORMATION_SCHEMA.TABLES AS t
            ON BINARY t.TABLE_SCHEMA = BINARY s.TABLE_SCHEMA
           AND BINARY t.TABLE_NAME = BINARY s.TABLE_NAME
        WHERE BINARY s.TABLE_SCHEMA = BINARY @database_name
          AND t.TABLE_TYPE = 'BASE TABLE'
        ORDER BY
            BINARY s.TABLE_SCHEMA,
            BINARY s.TABLE_NAME,
            BINARY s.INDEX_NAME,
            s.SEQ_IN_INDEX;
        """;

    internal const string LegacyIndexesQuery =
        """
        SELECT
            s.TABLE_SCHEMA,
            s.TABLE_NAME,
            s.INDEX_NAME,
            s.NON_UNIQUE,
            s.INDEX_TYPE,
            s.IS_VISIBLE,
            s.SEQ_IN_INDEX,
            s.COLUMN_NAME,
            s.COLLATION,
            s.SUB_PART,
            CONVERT(NULL, SIGNED),
            CONVERT(NULL, CHAR)
        FROM INFORMATION_SCHEMA.STATISTICS AS s
        INNER JOIN INFORMATION_SCHEMA.TABLES AS t
            ON BINARY t.TABLE_SCHEMA = BINARY s.TABLE_SCHEMA
           AND BINARY t.TABLE_NAME = BINARY s.TABLE_NAME
        WHERE BINARY s.TABLE_SCHEMA = BINARY @database_name
          AND t.TABLE_TYPE = 'BASE TABLE'
        ORDER BY
            BINARY s.TABLE_SCHEMA,
            BINARY s.TABLE_NAME,
            BINARY s.INDEX_NAME,
            s.SEQ_IN_INDEX;
        """;

    internal const string UnqualifiedIndexesQuery =
        """
        SELECT
            s.TABLE_SCHEMA,
            s.TABLE_NAME,
            s.INDEX_NAME,
            s.NON_UNIQUE,
            s.INDEX_TYPE,
            'YES',
            s.SEQ_IN_INDEX,
            s.COLUMN_NAME,
            s.COLLATION,
            s.SUB_PART,
            CONVERT(NULL, SIGNED),
            CONVERT(NULL, CHAR)
        FROM INFORMATION_SCHEMA.STATISTICS AS s
        INNER JOIN INFORMATION_SCHEMA.TABLES AS t
            ON BINARY t.TABLE_SCHEMA = BINARY s.TABLE_SCHEMA
           AND BINARY t.TABLE_NAME = BINARY s.TABLE_NAME
        WHERE BINARY s.TABLE_SCHEMA = BINARY @database_name
          AND t.TABLE_TYPE = 'BASE TABLE'
        ORDER BY
            BINARY s.TABLE_SCHEMA,
            BINARY s.TABLE_NAME,
            BINARY s.INDEX_NAME,
            s.SEQ_IN_INDEX;
        """;

    private async ValueTask<IReadOnlyList<MySqlKeyMetadata>> ReadKeysAsync(
        CatalogReadContext context,
        string selectedDatabase,
        IReadOnlyList<MySqlTableMetadata> tables,
        ReaderBudget budget,
        MySqlInspectionLimits limits,
        CancellationToken cancellationToken)
    {
        HashSet<string> tableIdentities = TableIdentities(tables);
        var identities = new HashSet<string>(StringComparer.Ordinal);
        var keys = new List<MySqlKeyMetadata>();
        await using MySqlCommand command = Command(context, KeysQuery);
        AddDatabaseParameter(command, selectedDatabase);
        await using MySqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (keys.Count == limits.MaxKeys)
                throw LimitExceeded("key count");
            budget.AddStructuralRow();

            string schemaName = RequiredString(reader, 0, budget, isName: true);
            string tableName = RequiredString(reader, 1, budget, isName: true);
            string name = RequiredString(reader, 2, budget, isName: true);
            string constraintType = RequiredString(reader, 3, budget);
            string tableIdentity = TableIdentity(schemaName, tableName);
            if (!tableIdentities.Contains(tableIdentity) ||
                constraintType is not ("PRIMARY KEY" or "UNIQUE") ||
                !identities.Add(ChildIdentity(tableIdentity, name)))
            {
                throw InvalidProviderMetadata();
            }

            keys.Add(new MySqlKeyMetadata(
                schemaName,
                tableName,
                name,
                constraintType));
        }
        return keys.AsReadOnly();
    }

    private async ValueTask<IReadOnlyList<MySqlKeyColumnMetadata>>
        ReadKeyColumnsAsync(
            CatalogReadContext context,
            string selectedDatabase,
            IReadOnlyList<MySqlTableMetadata> tables,
            IReadOnlyList<MySqlColumnMetadata> columns,
            IReadOnlyList<MySqlKeyMetadata> keys,
            ReaderBudget budget,
            MySqlInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        HashSet<string> tableIdentities = TableIdentities(tables);
        HashSet<string> columnIdentities = ColumnNameIdentities(columns);
        var keyIdentities = new HashSet<string>(
            keys.Select(static key => ChildIdentity(
                TableIdentity(key.SchemaName, key.TableName),
                key.Name)),
            StringComparer.Ordinal);
        var rowIdentities = new HashSet<string>(StringComparer.Ordinal);
        var result = new List<MySqlKeyColumnMetadata>();
        await using MySqlCommand command = Command(context, KeyColumnsQuery);
        AddDatabaseParameter(command, selectedDatabase);
        await using MySqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (result.Count == limits.MaxKeyColumns)
                throw LimitExceeded("key-column count");
            budget.AddStructuralRow();

            string schemaName = RequiredString(reader, 0, budget, isName: true);
            string tableName = RequiredString(reader, 1, budget, isName: true);
            string constraintName =
                RequiredString(reader, 2, budget, isName: true);
            int ordinalPosition = RequiredInt32(reader, 3);
            string columnName = RequiredString(reader, 4, budget, isName: true);
            string tableIdentity = TableIdentity(schemaName, tableName);
            string keyIdentity = ChildIdentity(tableIdentity, constraintName);
            if (ordinalPosition <= 0 ||
                !tableIdentities.Contains(tableIdentity) ||
                !keyIdentities.Contains(keyIdentity) ||
                !columnIdentities.Contains(ChildIdentity(
                    tableIdentity,
                    columnName)) ||
                !rowIdentities.Add(PositionIdentity(
                    keyIdentity,
                    ordinalPosition)))
            {
                throw InvalidProviderMetadata();
            }

            result.Add(new MySqlKeyColumnMetadata(
                schemaName,
                tableName,
                constraintName,
                ordinalPosition,
                columnName));
        }
        return result.AsReadOnly();
    }

    private async ValueTask<IReadOnlyList<MySqlForeignKeyMetadata>>
        ReadForeignKeysAsync(
            CatalogReadContext context,
            string selectedDatabase,
            IReadOnlyList<MySqlTableMetadata> tables,
            ReaderBudget budget,
            MySqlInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        HashSet<string> tableIdentities = TableIdentities(tables);
        var identities = new HashSet<string>(StringComparer.Ordinal);
        var result = new List<MySqlForeignKeyMetadata>();
        await using MySqlCommand command = Command(context, ForeignKeysQuery);
        AddDatabaseParameter(command, selectedDatabase);
        await using MySqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (result.Count == limits.MaxForeignKeys)
                throw LimitExceeded("foreign-key count");
            budget.AddStructuralRow();

            string schemaName = RequiredString(reader, 0, budget, isName: true);
            string tableName = RequiredString(reader, 1, budget, isName: true);
            string name = RequiredString(reader, 2, budget, isName: true);
            string referencedSchemaName =
                RequiredString(reader, 3, budget, isName: true);
            string referencedTableName =
                RequiredString(reader, 4, budget, isName: true);
            string? uniqueConstraintSchemaName =
                OptionalString(reader, 5, budget, isName: true);
            string? uniqueConstraintName =
                OptionalString(reader, 6, budget, isName: true);
            string matchOption = RequiredString(reader, 7, budget);
            string updateRule = RequiredString(reader, 8, budget);
            string deleteRule = RequiredString(reader, 9, budget);
            string tableIdentity = TableIdentity(schemaName, tableName);
            string referencedTableIdentity =
                TableIdentity(referencedSchemaName, referencedTableName);
            if (!tableIdentities.Contains(tableIdentity) ||
                !identities.Add(ChildIdentity(tableIdentity, name)) ||
                string.Equals(
                    referencedSchemaName,
                    selectedDatabase,
                    StringComparison.Ordinal) &&
                !tableIdentities.Contains(referencedTableIdentity))
            {
                throw InvalidProviderMetadata();
            }

            result.Add(new MySqlForeignKeyMetadata(
                schemaName,
                tableName,
                name,
                referencedSchemaName,
                referencedTableName,
                uniqueConstraintSchemaName,
                uniqueConstraintName,
                matchOption,
                updateRule,
                deleteRule));
        }
        return result.AsReadOnly();
    }

    private async ValueTask<IReadOnlyList<MySqlForeignKeyColumnMetadata>>
        ReadForeignKeyColumnsAsync(
            CatalogReadContext context,
            string selectedDatabase,
            IReadOnlyList<MySqlTableMetadata> tables,
            IReadOnlyList<MySqlColumnMetadata> columns,
            IReadOnlyList<MySqlForeignKeyMetadata> foreignKeys,
            ReaderBudget budget,
            MySqlInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        HashSet<string> tableIdentities = TableIdentities(tables);
        HashSet<string> columnIdentities = ColumnNameIdentities(columns);
        Dictionary<string, MySqlForeignKeyMetadata> foreignKeyByIdentity =
            foreignKeys.ToDictionary(
                static foreignKey => ChildIdentity(
                    TableIdentity(
                        foreignKey.SchemaName,
                        foreignKey.TableName),
                    foreignKey.Name),
                StringComparer.Ordinal);
        var rowIdentities = new HashSet<string>(StringComparer.Ordinal);
        var result = new List<MySqlForeignKeyColumnMetadata>();
        await using MySqlCommand command = Command(
            context,
            ForeignKeyColumnsQuery);
        AddDatabaseParameter(command, selectedDatabase);
        await using MySqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (result.Count == limits.MaxForeignKeyColumns)
                throw LimitExceeded("foreign-key-column count");
            budget.AddStructuralRow();

            string schemaName = RequiredString(reader, 0, budget, isName: true);
            string tableName = RequiredString(reader, 1, budget, isName: true);
            string constraintName =
                RequiredString(reader, 2, budget, isName: true);
            int ordinalPosition = RequiredInt32(reader, 3);
            string columnName = RequiredString(reader, 4, budget, isName: true);
            int? positionInUniqueConstraint = OptionalInt32(reader, 5);
            string referencedSchemaName =
                RequiredString(reader, 6, budget, isName: true);
            string referencedTableName =
                RequiredString(reader, 7, budget, isName: true);
            string referencedColumnName =
                RequiredString(reader, 8, budget, isName: true);
            string tableIdentity = TableIdentity(schemaName, tableName);
            string foreignKeyIdentity =
                ChildIdentity(tableIdentity, constraintName);
            string referencedTableIdentity =
                TableIdentity(referencedSchemaName, referencedTableName);
            if (ordinalPosition <= 0 ||
                positionInUniqueConstraint is <= 0 ||
                !tableIdentities.Contains(tableIdentity) ||
                !columnIdentities.Contains(ChildIdentity(
                    tableIdentity,
                    columnName)) ||
                !foreignKeyByIdentity.TryGetValue(
                    foreignKeyIdentity,
                    out MySqlForeignKeyMetadata? foreignKey) ||
                !string.Equals(
                    foreignKey.ReferencedSchemaName,
                    referencedSchemaName,
                    StringComparison.Ordinal) ||
                !string.Equals(
                    foreignKey.ReferencedTableName,
                    referencedTableName,
                    StringComparison.Ordinal) ||
                string.Equals(
                    referencedSchemaName,
                    selectedDatabase,
                    StringComparison.Ordinal) &&
                (!tableIdentities.Contains(referencedTableIdentity) ||
                 !columnIdentities.Contains(ChildIdentity(
                     referencedTableIdentity,
                     referencedColumnName))) ||
                !rowIdentities.Add(PositionIdentity(
                    foreignKeyIdentity,
                    ordinalPosition)))
            {
                throw InvalidProviderMetadata();
            }

            result.Add(new MySqlForeignKeyColumnMetadata(
                schemaName,
                tableName,
                constraintName,
                ordinalPosition,
                columnName,
                positionInUniqueConstraint,
                referencedSchemaName,
                referencedTableName,
                referencedColumnName));
        }
        ValidateContiguousChildren(
            foreignKeyByIdentity.Keys,
            result.Select(static item => (
                Parent: ChildIdentity(
                    TableIdentity(item.SchemaName, item.TableName),
                    item.ConstraintName),
                Position: item.OrdinalPosition)));
        return result.AsReadOnly();
    }

    private async ValueTask<IReadOnlyList<MySqlCheckMetadata>> ReadChecksAsync(
        CatalogReadContext context,
        string selectedDatabase,
        IReadOnlyList<MySqlTableMetadata> tables,
        ReaderBudget budget,
        MySqlInspectionLimits limits,
        CancellationToken cancellationToken)
    {
        HashSet<string> tableIdentities = TableIdentities(tables);
        var identities = new HashSet<string>(StringComparer.Ordinal);
        var result = new List<MySqlCheckMetadata>();
        await using MySqlCommand command = Command(context, ChecksQuery);
        AddDatabaseParameter(command, selectedDatabase);
        await using MySqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (result.Count == limits.MaxChecks)
                throw LimitExceeded("check-constraint count");
            budget.AddStructuralRow();

            string schemaName = RequiredString(reader, 0, budget, isName: true);
            string tableName = RequiredString(reader, 1, budget, isName: true);
            string name = RequiredString(reader, 2, budget, isName: true);
            bool isEnforced = ParseYesNo(RequiredString(reader, 3, budget));
            long? sourceClauseBytes = OptionalInt64(reader, 4);
            budget.PreflightExpression(sourceClauseBytes);
            if (reader.IsDBNull(5))
                throw InvalidProviderMetadata();
            string clause = reader.GetString(5);
            long clauseBytes = budget.AddExpression(clause, sourceClauseBytes);
            string tableIdentity = TableIdentity(schemaName, tableName);
            if (!tableIdentities.Contains(tableIdentity) ||
                !identities.Add(ChildIdentity(tableIdentity, name)))
            {
                throw InvalidProviderMetadata();
            }

            result.Add(new MySqlCheckMetadata(
                schemaName,
                tableName,
                name,
                isEnforced,
                clauseBytes,
                clause));
        }
        return result.AsReadOnly();
    }

    private async ValueTask<(
        IReadOnlyList<MySqlIndexMetadata> Indexes,
        IReadOnlyList<MySqlIndexPartMetadata> Parts)> ReadIndexesAsync(
        CatalogReadContext context,
        string selectedDatabase,
        IReadOnlyList<MySqlTableMetadata> tables,
        IReadOnlyList<MySqlColumnMetadata> columns,
        MySqlServerMetadata server,
        ReaderBudget budget,
        MySqlInspectionLimits limits,
        CancellationToken cancellationToken)
    {
        HashSet<string> tableIdentities = TableIdentities(tables);
        HashSet<string> columnIdentities = ColumnNameIdentities(columns);
        var indexByIdentity =
            new Dictionary<string, MySqlIndexMetadata>(StringComparer.Ordinal);
        var rowIdentities = new HashSet<string>(StringComparer.Ordinal);
        var indexes = new List<MySqlIndexMetadata>();
        var parts = new List<MySqlIndexPartMetadata>();
        await using MySqlCommand command = Command(
            context,
            SelectIndexesQuery(server));
        AddDatabaseParameter(command, selectedDatabase);
        await using MySqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (parts.Count == limits.MaxIndexParts)
                throw LimitExceeded("index-part count");
            budget.AddStructuralRow();

            string schemaName = RequiredString(reader, 0, budget, isName: true);
            string tableName = RequiredString(reader, 1, budget, isName: true);
            string indexName = RequiredString(reader, 2, budget, isName: true);
            bool isUnique = !RequiredBoolean(reader, 3);
            string indexType = RequiredString(reader, 4, budget);
            bool isVisible = ParseYesNo(RequiredString(reader, 5, budget));
            int sequence = RequiredInt32(reader, 6);
            string? columnName =
                OptionalString(reader, 7, budget, isName: true);
            string? sortDirection = OptionalString(reader, 8, budget);
            long? prefixLength = OptionalInt64(reader, 9);
            long? sourceExpressionBytes = OptionalInt64(reader, 10);
            budget.PreflightExpression(sourceExpressionBytes);
            string? expression = reader.IsDBNull(11)
                ? null
                : reader.GetString(11);
            long? expressionBytes = null;
            if (expression is not null)
            {
                expressionBytes = budget.AddExpression(
                    expression,
                    sourceExpressionBytes);
            }
            else if (sourceExpressionBytes is not null)
            {
                throw InvalidProviderMetadata();
            }

            string tableIdentity = TableIdentity(schemaName, tableName);
            string indexIdentity = ChildIdentity(tableIdentity, indexName);
            if (sequence <= 0 ||
                prefixLength is <= 0 ||
                !tableIdentities.Contains(tableIdentity) ||
                (columnName is null) == (expression is null) ||
                columnName is not null &&
                !columnIdentities.Contains(ChildIdentity(
                    tableIdentity,
                    columnName)) ||
                sortDirection is not (null or "A" or "D") ||
                !rowIdentities.Add(PositionIdentity(indexIdentity, sequence)))
            {
                throw InvalidProviderMetadata();
            }

            var index = new MySqlIndexMetadata(
                schemaName,
                tableName,
                indexName,
                isUnique,
                indexType,
                isVisible);
            if (indexByIdentity.TryGetValue(
                    indexIdentity,
                    out MySqlIndexMetadata? existing))
            {
                if (existing != index)
                    throw InvalidProviderMetadata();
            }
            else
            {
                if (indexes.Count == limits.MaxIndexes)
                    throw LimitExceeded("index count");
                indexByIdentity.Add(indexIdentity, index);
                indexes.Add(index);
            }

            parts.Add(new MySqlIndexPartMetadata(
                schemaName,
                tableName,
                indexName,
                sequence,
                columnName,
                sortDirection,
                prefixLength,
                expressionBytes,
                expression));
        }
        ValidateContiguousChildren(
            indexByIdentity.Keys,
            parts.Select(static item => (
                Parent: ChildIdentity(
                    TableIdentity(item.SchemaName, item.TableName),
                    item.IndexName),
                Position: item.Sequence)));
        return (indexes.AsReadOnly(), parts.AsReadOnly());
    }

    private static async ValueTask<
        IReadOnlyList<MySqlTableDefinitionMetadata>> ReadTableDefinitionsAsync(
        CatalogReadContext context,
        IReadOnlyList<MySqlTableMetadata> tables,
        ReaderBudget budget,
        MySqlInspectionLimits limits,
        CancellationToken cancellationToken)
    {
        if (tables.Count > limits.MaxTableDefinitions)
            throw LimitExceeded("table-definition count");

        var result = new List<MySqlTableDefinitionMetadata>(tables.Count);
        var identities = new HashSet<string>(StringComparer.Ordinal);
        foreach (MySqlTableMetadata table in tables)
        {
            cancellationToken.ThrowIfCancellationRequested();
            budget.AddStructuralRow();
            await using MySqlCommand command = Command(
                context,
                BuildShowCreateTableCommand(
                    table.SchemaName,
                    table.Name));
            await using MySqlDataReader reader = await command.ExecuteReaderAsync(
                    CommandBehavior.SequentialAccess |
                    CommandBehavior.SingleResult,
                    cancellationToken)
                .ConfigureAwait(false);
            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ||
                reader.FieldCount != 2)
            {
                throw InvalidProviderMetadata();
            }
            string returnedName =
                RequiredString(reader, 0, budget, isName: true);
            if (!string.Equals(
                    returnedName,
                    table.Name,
                    StringComparison.Ordinal) ||
                reader.IsDBNull(1))
            {
                throw InvalidProviderMetadata();
            }
            string definition = await ReadBoundedTextAsync(
                    reader,
                    ordinal: 1,
                    limits.MaxDefinitionBytes,
                    cancellationToken)
                .ConfigureAwait(false);
            long definitionBytes = budget.AddDefinition(
                definition,
                StrictUtf8ByteCount(definition));
            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false) ||
                !identities.Add(TableIdentity(
                    table.SchemaName,
                    table.Name)))
            {
                throw InvalidProviderMetadata();
            }
            result.Add(new MySqlTableDefinitionMetadata(
                table.SchemaName,
                table.Name,
                definitionBytes,
                definition));
        }
        return result.AsReadOnly();
    }

    internal static string BuildShowCreateTableCommand(
        string schemaName,
        string tableName) =>
        $"SHOW CREATE TABLE {QuoteIdentifier(schemaName)}." +
        $"{QuoteIdentifier(tableName)};";

    internal static string QuoteIdentifier(string identifier)
    {
        ArgumentException.ThrowIfNullOrEmpty(identifier);
        return "`" +
               identifier.Replace("`", "``", StringComparison.Ordinal) +
               "`";
    }

    internal static string SelectIndexesQuery(MySqlServerMetadata server) =>
        IsOracleMySqlAtLeast(server, new Version(8, 0, 13))
            ? IndexesQuery
            : IsOracleMySqlAtLeast(server, new Version(8, 0))
                ? LegacyIndexesQuery
                : UnqualifiedIndexesQuery;

    internal static bool ShouldReadCheckConstraints(
        MySqlServerMetadata server) =>
        IsOracleMySqlAtLeast(server, new Version(8, 0, 16));

    private static bool IsOracleMySqlAtLeast(
        MySqlServerMetadata server,
        Version minimum)
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
            identity.Contains("vitess", StringComparison.Ordinal))
        {
            return false;
        }

        string numeric = server.Version.Split('-', 2)[0];
        return Version.TryParse(numeric, out Version? version) &&
               version >= minimum;
    }

    private static HashSet<string> TableIdentities(
        IEnumerable<MySqlTableMetadata> tables) =>
        new(
            tables.Select(static table =>
                TableIdentity(table.SchemaName, table.Name)),
            StringComparer.Ordinal);

    private static HashSet<string> ColumnNameIdentities(
        IEnumerable<MySqlColumnMetadata> columns) =>
        new(
            columns.Select(static column => ChildIdentity(
                TableIdentity(column.SchemaName, column.TableName),
                column.Name)),
            StringComparer.Ordinal);

    private static string TableIdentity(string schemaName, string tableName) =>
        schemaName + "\0" + tableName;

    private static string ChildIdentity(string parent, string name) =>
        parent + "\0" + name;

    private static string PositionIdentity(string parent, int position) =>
        parent + "\0" + position.ToString(CultureInfo.InvariantCulture);

    private static void ValidateContiguousChildren(
        IEnumerable<string> expectedParents,
        IEnumerable<(string Parent, int Position)> children)
    {
        Dictionary<string, int[]> positions = children
            .GroupBy(static child => child.Parent, StringComparer.Ordinal)
            .ToDictionary(
                static group => group.Key,
                static group => group
                    .Select(static child => child.Position)
                    .Order()
                    .ToArray(),
                StringComparer.Ordinal);
        foreach (string parent in expectedParents)
        {
            if (!positions.Remove(parent, out int[]? actual) ||
                actual.Length == 0)
            {
                throw InvalidProviderMetadata();
            }
            for (int index = 0; index < actual.Length; index++)
            {
                if (actual[index] != index + 1)
                    throw InvalidProviderMetadata();
            }
        }
        if (positions.Count != 0)
            throw InvalidProviderMetadata();
    }

    private static long StrictUtf8ByteCount(string value)
    {
        try
        {
            return new System.Text.UTF8Encoding(
                encoderShouldEmitUTF8Identifier: false,
                throwOnInvalidBytes: true).GetByteCount(value);
        }
        catch (System.Text.EncoderFallbackException)
        {
            throw new MySqlMigrationException(
                "MySQL metadata contains invalid Unicode.");
        }
    }

    private static async ValueTask<string> ReadBoundedTextAsync(
        MySqlDataReader reader,
        int ordinal,
        int maximumBytes,
        CancellationToken cancellationToken)
    {
        using TextReader textReader = reader.GetTextReader(ordinal);
        var buffer = new char[8192];
        var value = new System.Text.StringBuilder(
            Math.Min(maximumBytes, buffer.Length));
        while (true)
        {
            int read = await textReader.ReadAsync(
                    buffer.AsMemory(),
                    cancellationToken)
                .ConfigureAwait(false);
            if (read == 0)
                break;
            if (value.Length > maximumBytes - read)
                throw LimitExceeded("definition byte");
            value.Append(buffer, 0, read);
        }
        return value.ToString();
    }
}
