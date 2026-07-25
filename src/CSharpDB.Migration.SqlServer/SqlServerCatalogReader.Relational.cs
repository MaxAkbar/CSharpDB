using System.Data;
using Microsoft.Data.SqlClient;

namespace CSharpDB.Migration.SqlServer;

internal sealed partial class SqlServerCatalogReader
{
    internal const string UserTokensQuery =
        """
        SELECT
            ut.principal_id,
            ut.type,
            ut.usage
        FROM sys.user_token AS ut
        ORDER BY ut.principal_id, ut.type, ut.usage;
        """;

    internal const string PermissionDenialsQuery =
        """
        SELECT
            dp.class,
            dp.major_id,
            dp.minor_id,
            dp.permission_name,
            dp.grantee_principal_id,
            ut.usage
        FROM sys.database_permissions AS dp
        INNER JOIN sys.user_token AS ut
            ON ut.principal_id = dp.grantee_principal_id
        WHERE dp.state = N'D'
        ORDER BY
            dp.class,
            dp.major_id,
            dp.minor_id,
            dp.permission_name,
            dp.grantee_principal_id,
            ut.usage;
        """;

    internal const string KeysQuery =
        """
        SELECT
            kc.object_id,
            kc.parent_object_id,
            kc.name,
            kc.type,
            kc.unique_index_id,
            kc.is_system_named
        FROM sys.key_constraints AS kc
        INNER JOIN sys.tables AS t
            ON t.object_id = kc.parent_object_id
        WHERE t.is_ms_shipped = 0
        ORDER BY kc.parent_object_id, kc.object_id;
        """;

    internal const string IndexesQuery =
        """
        SELECT
            i.object_id,
            i.index_id,
            i.name,
            i.type,
            i.type_desc,
            i.is_unique,
            i.data_space_id,
            ds.name,
            ds.type_desc,
            i.ignore_dup_key,
            i.is_primary_key,
            i.is_unique_constraint,
            i.fill_factor,
            i.is_padded,
            i.is_disabled,
            i.is_hypothetical,
            i.allow_row_locks,
            i.allow_page_locks,
            i.has_filter,
            CONVERT(bigint, DATALENGTH(i.filter_definition)),
            i.filter_definition,
            i.compression_delay,
            i.suppress_dup_key_messages,
            i.optimize_for_sequential_key
        FROM sys.indexes AS i
        INNER JOIN sys.objects AS o
            ON o.object_id = i.object_id
        LEFT JOIN sys.data_spaces AS ds
            ON ds.data_space_id = i.data_space_id
        WHERE o.is_ms_shipped = 0
          AND o.type IN (N'U', N'V')
          AND i.index_id > 0
        ORDER BY i.object_id, i.index_id;
        """;

    internal const string IndexColumnsQuery =
        """
        SELECT
            ic.object_id,
            ic.index_id,
            ic.index_column_id,
            ic.column_id,
            ic.key_ordinal,
            ic.partition_ordinal,
            ic.is_descending_key,
            ic.is_included_column,
            CONVERT(tinyint, NULL),
            CONVERT(tinyint, NULL)
        FROM sys.index_columns AS ic
        INNER JOIN sys.indexes AS i
            ON i.object_id = ic.object_id
           AND i.index_id = ic.index_id
        INNER JOIN sys.objects AS o
            ON o.object_id = ic.object_id
        WHERE o.is_ms_shipped = 0
          AND o.type IN (N'U', N'V')
          AND i.index_id > 0
        ORDER BY ic.object_id, ic.index_id, ic.index_column_id;
        """;

    internal const string IndexColumnsV16Query =
        """
        SELECT
            ic.object_id,
            ic.index_id,
            ic.index_column_id,
            ic.column_id,
            ic.key_ordinal,
            ic.partition_ordinal,
            ic.is_descending_key,
            ic.is_included_column,
            ic.column_store_order_ordinal,
            CONVERT(tinyint, NULL)
        FROM sys.index_columns AS ic
        INNER JOIN sys.indexes AS i
            ON i.object_id = ic.object_id
           AND i.index_id = ic.index_id
        INNER JOIN sys.objects AS o
            ON o.object_id = ic.object_id
        WHERE o.is_ms_shipped = 0
          AND o.type IN (N'U', N'V')
          AND i.index_id > 0
        ORDER BY ic.object_id, ic.index_id, ic.index_column_id;
        """;

    internal const string IndexColumnsV17Query =
        """
        SELECT
            ic.object_id,
            ic.index_id,
            ic.index_column_id,
            ic.column_id,
            ic.key_ordinal,
            ic.partition_ordinal,
            ic.is_descending_key,
            ic.is_included_column,
            ic.column_store_order_ordinal,
            ic.data_clustering_ordinal
        FROM sys.index_columns AS ic
        INNER JOIN sys.indexes AS i
            ON i.object_id = ic.object_id
           AND i.index_id = ic.index_id
        INNER JOIN sys.objects AS o
            ON o.object_id = ic.object_id
        WHERE o.is_ms_shipped = 0
          AND o.type IN (N'U', N'V')
          AND i.index_id > 0
        ORDER BY ic.object_id, ic.index_id, ic.index_column_id;
        """;

    internal const string ForeignKeysQuery =
        """
        SELECT
            fk.object_id,
            fk.parent_object_id,
            fk.referenced_object_id,
            fk.key_index_id,
            fk.name,
            fk.is_disabled,
            fk.is_not_for_replication,
            fk.is_not_trusted,
            fk.delete_referential_action,
            fk.delete_referential_action_desc,
            fk.update_referential_action,
            fk.update_referential_action_desc,
            fk.is_system_named
        FROM sys.foreign_keys AS fk
        INNER JOIN sys.tables AS t
            ON t.object_id = fk.parent_object_id
        WHERE t.is_ms_shipped = 0
        ORDER BY fk.parent_object_id, fk.object_id;
        """;

    internal const string ForeignKeyColumnsQuery =
        """
        SELECT
            fkc.constraint_object_id,
            fkc.constraint_column_id,
            fkc.parent_object_id,
            fkc.parent_column_id,
            fkc.referenced_object_id,
            fkc.referenced_column_id
        FROM sys.foreign_key_columns AS fkc
        INNER JOIN sys.tables AS t
            ON t.object_id = fkc.parent_object_id
        WHERE t.is_ms_shipped = 0
        ORDER BY fkc.constraint_object_id, fkc.constraint_column_id;
        """;

    internal const string ChecksQuery =
        """
        SELECT
            cc.object_id,
            cc.parent_object_id,
            cc.name,
            cc.parent_column_id,
            cc.is_disabled,
            cc.is_not_for_replication,
            cc.is_not_trusted,
            CONVERT(bigint, DATALENGTH(cc.definition)),
            cc.definition,
            cc.uses_database_collation,
            cc.is_system_named
        FROM sys.check_constraints AS cc
        INNER JOIN sys.tables AS t
            ON t.object_id = cc.parent_object_id
        WHERE t.is_ms_shipped = 0
        ORDER BY cc.parent_object_id, cc.object_id;
        """;

    internal const string SequencesQuery =
        """
        SELECT
            seq.object_id,
            seq.schema_id,
            seq.name,
            type_schema.name,
            user_type.name,
            COALESCE(system_type.name, user_type.name),
            seq.precision,
            seq.scale,
            CONVERT(varchar(40), CONVERT(decimal(38, 0), seq.start_value)),
            CONVERT(varchar(40), CONVERT(decimal(38, 0), seq.increment)),
            CONVERT(varchar(40), CONVERT(decimal(38, 0), seq.minimum_value)),
            CONVERT(varchar(40), CONVERT(decimal(38, 0), seq.maximum_value)),
            seq.is_cycling,
            seq.is_cached,
            seq.cache_size
        FROM sys.sequences AS seq
        INNER JOIN sys.types AS user_type
            ON user_type.user_type_id = seq.user_type_id
        INNER JOIN sys.schemas AS type_schema
            ON type_schema.schema_id = user_type.schema_id
        LEFT JOIN sys.types AS system_type
            ON system_type.user_type_id = seq.system_type_id
           AND system_type.is_user_defined = 0
        WHERE seq.is_ms_shipped = 0
        ORDER BY seq.object_id;
        """;

    private static async ValueTask<SqlServerPermissionAuditMetadata>
        ReadPermissionAuditAsync(
            SqlConnection connection,
            SqlServerInstanceMetadata instance,
            SqlServerDatabaseMetadata database,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        if (!CanReadCompletePermissionAudit(instance, database))
            return SqlServerPermissionAuditMetadata.NotAttempted;

        IReadOnlyList<SqlServerUserTokenMetadata> tokens =
            await ReadUserTokensAsync(
                    connection,
                    budget,
                    limits,
                    cancellationToken)
                .ConfigureAwait(false);
        IReadOnlyList<SqlServerPermissionDenyMetadata> denials =
            await ReadPermissionDenialsAsync(
                    connection,
                    budget,
                    limits,
                    cancellationToken)
                .ConfigureAwait(false);
        return new SqlServerPermissionAuditMetadata(
            tokens,
            denials,
            Attempted: true);
    }

    private static bool CanReadCompletePermissionAudit(
        SqlServerInstanceMetadata instance,
        SqlServerDatabaseMetadata database)
    {
        if (database.IsSysAdmin is true ||
            database.IsDbOwner is true ||
            database.HasControl is true)
        {
            return true;
        }

        return instance.ProductMajorVersion >= 16
            ? database.HasViewSecurityDefinition is true
            : database.HasViewDefinition is true;
    }

    private static async ValueTask<IReadOnlyList<SqlServerUserTokenMetadata>>
        ReadUserTokensAsync(
            SqlConnection connection,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var tokens = new List<SqlServerUserTokenMetadata>();
        await using SqlCommand command = Command(connection, UserTokensQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (tokens.Count == limits.MaxUserTokens)
                throw LimitExceeded("user-token count");
            budget.AddPermissionRow();
            tokens.Add(new SqlServerUserTokenMetadata(
                RequiredInt32(reader, 0),
                RequiredString(reader, 1, budget),
                RequiredString(reader, 2, budget)));
        }
        return tokens.AsReadOnly();
    }

    private static async ValueTask<IReadOnlyList<SqlServerPermissionDenyMetadata>>
        ReadPermissionDenialsAsync(
            SqlConnection connection,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var denials = new List<SqlServerPermissionDenyMetadata>();
        await using SqlCommand command = Command(connection, PermissionDenialsQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (denials.Count == limits.MaxPermissionDenials)
                throw LimitExceeded("permission-denial count");
            budget.AddPermissionRow();
            denials.Add(new SqlServerPermissionDenyMetadata(
                RequiredByte(reader, 0),
                RequiredInt32(reader, 1),
                RequiredInt32(reader, 2),
                RequiredString(reader, 3, budget),
                RequiredInt32(reader, 4),
                RequiredString(reader, 5, budget)));
        }
        return denials.AsReadOnly();
    }

    private static async ValueTask<IReadOnlyList<SqlServerKeyMetadata>> ReadKeysAsync(
        SqlConnection connection,
        ReaderBudget budget,
        SqlServerInspectionLimits limits,
        CancellationToken cancellationToken)
    {
        var keys = new List<SqlServerKeyMetadata>();
        await using SqlCommand command = Command(connection, KeysQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (keys.Count == limits.MaxKeys)
                throw LimitExceeded("key count");
            budget.AddStructuralRow();
            keys.Add(new SqlServerKeyMetadata(
                RequiredInt32(reader, 0),
                RequiredInt32(reader, 1),
                RequiredString(reader, 2, budget, isName: true),
                RequiredString(reader, 3, budget),
                RequiredInt32(reader, 4),
                RequiredBoolean(reader, 5)));
        }
        return keys.AsReadOnly();
    }

    private static async ValueTask<IReadOnlyList<SqlServerIndexMetadata>>
        ReadIndexesAsync(
            SqlConnection connection,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var indexes = new List<SqlServerIndexMetadata>();
        await using SqlCommand command = Command(connection, IndexesQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (indexes.Count == limits.MaxIndexes)
                throw LimitExceeded("index count");
            budget.AddStructuralRow();

            int objectId = RequiredInt32(reader, 0);
            int indexId = RequiredInt32(reader, 1);
            string name = RequiredString(reader, 2, budget, isName: true);
            byte type = RequiredByte(reader, 3);
            string typeDescription = RequiredString(reader, 4, budget);
            bool isUnique = RequiredBoolean(reader, 5);
            int dataSpaceId = RequiredInt32(reader, 6);
            string? dataSpaceName =
                OptionalString(reader, 7, budget, isName: true);
            string? dataSpaceType = OptionalString(reader, 8, budget);
            bool ignoreDuplicateKey = RequiredBoolean(reader, 9);
            bool isPrimaryKey = RequiredBoolean(reader, 10);
            bool isUniqueConstraint = RequiredBoolean(reader, 11);
            byte fillFactor = RequiredByte(reader, 12);
            bool isPadded = RequiredBoolean(reader, 13);
            bool isDisabled = RequiredBoolean(reader, 14);
            bool isHypothetical = RequiredBoolean(reader, 15);
            bool allowRowLocks = RequiredBoolean(reader, 16);
            bool allowPageLocks = RequiredBoolean(reader, 17);
            bool hasFilter = RequiredBoolean(reader, 18);
            long? filterDefinitionBytes = OptionalInt64(reader, 19);
            string? filterDefinition = OptionalExpression(
                reader,
                20,
                filterDefinitionBytes,
                budget);
            int? compressionDelay = OptionalInt32(reader, 21);
            bool suppressDuplicateKeyMessages = RequiredBoolean(reader, 22);
            bool optimizeForSequentialKey = RequiredBoolean(reader, 23);

            indexes.Add(new SqlServerIndexMetadata(
                objectId,
                indexId,
                name,
                type,
                typeDescription,
                isUnique,
                dataSpaceId,
                dataSpaceName,
                dataSpaceType,
                ignoreDuplicateKey,
                isPrimaryKey,
                isUniqueConstraint,
                fillFactor,
                isPadded,
                isDisabled,
                isHypothetical,
                allowRowLocks,
                allowPageLocks,
                hasFilter,
                filterDefinitionBytes,
                filterDefinition,
                compressionDelay,
                suppressDuplicateKeyMessages,
                optimizeForSequentialKey));
        }
        return indexes.AsReadOnly();
    }

    private static async ValueTask<IReadOnlyList<SqlServerIndexColumnMetadata>>
        ReadIndexColumnsAsync(
            SqlConnection connection,
            SqlServerInstanceMetadata instance,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var columns = new List<SqlServerIndexColumnMetadata>();
        string commandText = instance.ProductMajorVersion switch
        {
            >= 17 => IndexColumnsV17Query,
            >= 16 => IndexColumnsV16Query,
            _ => IndexColumnsQuery,
        };
        await using SqlCommand command = Command(connection, commandText);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (columns.Count == limits.MaxIndexColumns)
                throw LimitExceeded("index-column count");
            budget.AddStructuralRow();
            columns.Add(new SqlServerIndexColumnMetadata(
                RequiredInt32(reader, 0),
                RequiredInt32(reader, 1),
                RequiredInt32(reader, 2),
                RequiredInt32(reader, 3),
                RequiredByte(reader, 4),
                RequiredByte(reader, 5),
                RequiredBoolean(reader, 6),
                RequiredBoolean(reader, 7),
                OptionalByte(reader, 8),
                OptionalByte(reader, 9)));
        }
        return columns.AsReadOnly();
    }

    private static async ValueTask<IReadOnlyList<SqlServerForeignKeyMetadata>>
        ReadForeignKeysAsync(
            SqlConnection connection,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var foreignKeys = new List<SqlServerForeignKeyMetadata>();
        await using SqlCommand command = Command(connection, ForeignKeysQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (foreignKeys.Count == limits.MaxForeignKeys)
                throw LimitExceeded("foreign-key count");
            budget.AddStructuralRow();
            foreignKeys.Add(new SqlServerForeignKeyMetadata(
                RequiredInt32(reader, 0),
                RequiredInt32(reader, 1),
                RequiredInt32(reader, 2),
                RequiredInt32(reader, 3),
                RequiredString(reader, 4, budget, isName: true),
                RequiredBoolean(reader, 5),
                RequiredBoolean(reader, 6),
                RequiredBoolean(reader, 7),
                RequiredByte(reader, 8),
                RequiredString(reader, 9, budget),
                RequiredByte(reader, 10),
                RequiredString(reader, 11, budget),
                RequiredBoolean(reader, 12)));
        }
        return foreignKeys.AsReadOnly();
    }

    private static async ValueTask<
        IReadOnlyList<SqlServerForeignKeyColumnMetadata>> ReadForeignKeyColumnsAsync(
            SqlConnection connection,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var columns = new List<SqlServerForeignKeyColumnMetadata>();
        await using SqlCommand command = Command(connection, ForeignKeyColumnsQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (columns.Count == limits.MaxForeignKeyColumns)
                throw LimitExceeded("foreign-key-column count");
            budget.AddStructuralRow();
            columns.Add(new SqlServerForeignKeyColumnMetadata(
                RequiredInt32(reader, 0),
                RequiredInt32(reader, 1),
                RequiredInt32(reader, 2),
                RequiredInt32(reader, 3),
                RequiredInt32(reader, 4),
                RequiredInt32(reader, 5)));
        }
        return columns.AsReadOnly();
    }

    private static async ValueTask<IReadOnlyList<SqlServerCheckMetadata>>
        ReadChecksAsync(
            SqlConnection connection,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var checks = new List<SqlServerCheckMetadata>();
        await using SqlCommand command = Command(connection, ChecksQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (checks.Count == limits.MaxChecks)
                throw LimitExceeded("check-constraint count");
            budget.AddStructuralRow();

            int objectId = RequiredInt32(reader, 0);
            int parentObjectId = RequiredInt32(reader, 1);
            string name = RequiredString(reader, 2, budget, isName: true);
            int parentColumnId = RequiredInt32(reader, 3);
            bool isDisabled = RequiredBoolean(reader, 4);
            bool isNotForReplication = RequiredBoolean(reader, 5);
            bool isNotTrusted = RequiredBoolean(reader, 6);
            long? definitionBytes = OptionalInt64(reader, 7);
            string? definition =
                OptionalExpression(reader, 8, definitionBytes, budget);
            bool usesDatabaseCollation = RequiredBoolean(reader, 9);
            bool isSystemNamed = RequiredBoolean(reader, 10);

            checks.Add(new SqlServerCheckMetadata(
                objectId,
                parentObjectId,
                name,
                parentColumnId,
                isDisabled,
                isNotForReplication,
                isNotTrusted,
                definitionBytes,
                definition,
                usesDatabaseCollation,
                isSystemNamed));
        }
        return checks.AsReadOnly();
    }

    private static async ValueTask<IReadOnlyList<SqlServerSequenceMetadata>>
        ReadSequencesAsync(
            SqlConnection connection,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var sequences = new List<SqlServerSequenceMetadata>();
        await using SqlCommand command = Command(connection, SequencesQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (sequences.Count == limits.MaxSequences)
                throw LimitExceeded("sequence count");
            budget.AddStructuralRow();
            sequences.Add(new SqlServerSequenceMetadata(
                RequiredInt32(reader, 0),
                RequiredInt32(reader, 1),
                RequiredString(reader, 2, budget, isName: true),
                RequiredString(reader, 3, budget, isName: true),
                RequiredString(reader, 4, budget, isName: true),
                RequiredString(reader, 5, budget, isName: true),
                RequiredByte(reader, 6),
                RequiredByte(reader, 7),
                RequiredString(reader, 8, budget),
                RequiredString(reader, 9, budget),
                OptionalString(reader, 10, budget),
                OptionalString(reader, 11, budget),
                RequiredBoolean(reader, 12),
                RequiredBoolean(reader, 13),
                OptionalInt32(reader, 14)));
        }
        return sequences.AsReadOnly();
    }
}
