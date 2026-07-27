using System.Data;
using Microsoft.Data.SqlClient;

namespace CSharpDB.Migration.SqlServer;

internal sealed partial class SqlServerCatalogReader
{
    internal const string ViewsQuery =
        """
        SELECT
            v.object_id,
            v.schema_id,
            v.name,
            v.is_replicated,
            v.has_replication_filter,
            v.has_opaque_metadata,
            v.has_unchecked_assembly_data,
            v.with_check_option,
            v.is_date_correlation_view,
            CONVERT(
                bit,
                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM sys.indexes AS i
                        WHERE i.object_id = v.object_id
                          AND i.index_id > 0)
                    THEN 1
                    ELSE 0
                END),
            CONVERT(
                int,
                HAS_PERMS_BY_NAME(
                    QUOTENAME(s.name) + N'.' + QUOTENAME(v.name),
                    N'OBJECT',
                    N'VIEW DEFINITION'))
        FROM sys.views AS v
        INNER JOIN sys.schemas AS s
            ON s.schema_id = v.schema_id
        WHERE v.is_ms_shipped = 0
        ORDER BY v.object_id;
        """;

    internal const string LedgerViewsQuery =
        """
        SELECT
            v.object_id,
            v.ledger_view_type,
            v.ledger_view_type_desc,
            v.is_dropped_ledger_view
        FROM sys.views AS v
        WHERE v.is_ms_shipped = 0
        ORDER BY v.object_id;
        """;

    internal const string ViewColumnsQuery =
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
            c.is_ansi_padded,
            c.is_hidden,
            c.is_masked,
            c.encryption_type_desc,
            c.is_xml_document,
            c.xml_collection_id
        FROM sys.columns AS c
        INNER JOIN sys.views AS v
            ON v.object_id = c.object_id
           AND v.is_ms_shipped = 0
        INNER JOIN sys.types AS user_type
            ON user_type.user_type_id = c.user_type_id
        INNER JOIN sys.schemas AS type_schema
            ON type_schema.schema_id = user_type.schema_id
        LEFT JOIN sys.types AS system_type
            ON system_type.user_type_id = c.system_type_id
           AND system_type.is_user_defined = 0
        ORDER BY c.object_id, c.column_id;
        """;

    internal const string TriggersQuery =
        """
        SELECT
            tr.object_id,
            o.schema_id,
            tr.parent_class,
            tr.parent_class_desc,
            tr.parent_id,
            tr.name,
            tr.type,
            tr.type_desc,
            tr.is_disabled,
            tr.is_not_for_replication,
            tr.is_instead_of_trigger,
            CONVERT(
                int,
                OBJECTPROPERTYEX(
                    tr.object_id,
                    N'ExecIsInsertTrigger')),
            CONVERT(
                int,
                OBJECTPROPERTYEX(
                    tr.object_id,
                    N'ExecIsUpdateTrigger')),
            CONVERT(
                int,
                OBJECTPROPERTYEX(
                    tr.object_id,
                    N'ExecIsDeleteTrigger')),
            CONVERT(
                int,
                OBJECTPROPERTYEX(
                    tr.object_id,
                    N'ExecIsFirstInsertTrigger')),
            CONVERT(
                int,
                OBJECTPROPERTYEX(
                    tr.object_id,
                    N'ExecIsLastInsertTrigger')),
            CONVERT(
                int,
                OBJECTPROPERTYEX(
                    tr.object_id,
                    N'ExecIsFirstUpdateTrigger')),
            CONVERT(
                int,
                OBJECTPROPERTYEX(
                    tr.object_id,
                    N'ExecIsLastUpdateTrigger')),
            CONVERT(
                int,
                OBJECTPROPERTYEX(
                    tr.object_id,
                    N'ExecIsFirstDeleteTrigger')),
            CONVERT(
                int,
                OBJECTPROPERTYEX(
                    tr.object_id,
                    N'ExecIsLastDeleteTrigger')),
            CASE
                WHEN o.schema_id IS NULL
                THEN NULL
                ELSE CONVERT(
                    int,
                    HAS_PERMS_BY_NAME(
                        QUOTENAME(s.name) +
                            N'.' +
                            QUOTENAME(tr.name),
                        N'OBJECT',
                        N'VIEW DEFINITION'))
            END
        FROM sys.triggers AS tr
        LEFT JOIN sys.objects AS o
            ON o.object_id = tr.object_id
        LEFT JOIN sys.schemas AS s
            ON s.schema_id = o.schema_id
        WHERE tr.is_ms_shipped = 0
        ORDER BY tr.parent_class, tr.parent_id, tr.object_id;
        """;

    internal const string TriggerEventsQuery =
        """
        SELECT
            trigger_event.object_id,
            trigger_event.type,
            trigger_event.type_desc,
            trigger_event.is_first,
            trigger_event.is_last,
            trigger_event.event_group_type,
            trigger_event.event_group_type_desc
        FROM sys.trigger_events AS trigger_event
        INNER JOIN sys.triggers AS tr
            ON tr.object_id = trigger_event.object_id
        WHERE tr.is_ms_shipped = 0
        ORDER BY trigger_event.object_id, trigger_event.type;
        """;

    internal const string RoutinesQuery =
        """
        SELECT
            o.object_id,
            o.schema_id,
            o.name,
            RTRIM(o.type),
            o.type_desc,
            p.is_auto_executed,
            p.is_execution_replicated,
            p.is_repl_serializable_only,
            p.skips_repl_constraints,
            CONVERT(
                int,
                HAS_PERMS_BY_NAME(
                    QUOTENAME(s.name) + N'.' + QUOTENAME(o.name),
                    N'OBJECT',
                    N'VIEW DEFINITION'))
        FROM sys.objects AS o
        INNER JOIN sys.schemas AS s
            ON s.schema_id = o.schema_id
        LEFT JOIN sys.procedures AS p
            ON p.object_id = o.object_id
        WHERE o.is_ms_shipped = 0
          AND o.type IN (
              N'P',
              N'PC',
              N'RF',
              N'X',
              N'FN',
              N'FS',
              N'FT',
              N'IF',
              N'TF',
              N'AF')
        ORDER BY o.object_id;
        """;

    internal const string ModulesQuery =
        """
        SELECT
            sm.object_id,
            module_object.schema_id,
            module_object.parent_object_id,
            module_object.name,
            RTRIM(module_object.type),
            module_object.type_desc,
            CONVERT(bigint, DATALENGTH(sm.definition)),
            sm.definition,
            sm.uses_ansi_nulls,
            sm.uses_quoted_identifier,
            sm.is_schema_bound,
            sm.uses_database_collation,
            sm.is_recompiled,
            sm.null_on_null_input,
            sm.execute_as_principal_id,
            sm.uses_native_compilation,
            sm.is_inlineable,
            sm.inline_type,
            CONVERT(
                int,
                OBJECTPROPERTYEX(
                    sm.object_id,
                    N'IsEncrypted'))
        FROM sys.sql_modules AS sm
        INNER JOIN (
            SELECT
                o.object_id,
                o.schema_id,
                o.parent_object_id,
                o.name,
                o.type,
                o.type_desc,
                o.is_ms_shipped
            FROM sys.objects AS o

            UNION ALL

            SELECT
                tr.object_id,
                CONVERT(int, 0) AS schema_id,
                tr.parent_id AS parent_object_id,
                tr.name,
                tr.type,
                tr.type_desc,
                tr.is_ms_shipped
            FROM sys.triggers AS tr
            WHERE tr.parent_class = 0
        ) AS module_object
            ON module_object.object_id = sm.object_id
        WHERE module_object.is_ms_shipped = 0
          AND module_object.type IN (
              N'V',
              N'TR',
              N'P',
              N'RF',
              N'FN',
              N'IF',
              N'TF',
              N'R',
              N'D')
        ORDER BY sm.object_id;
        """;

    internal const string ParametersQuery =
        """
        SELECT
            p.object_id,
            p.parameter_id,
            COALESCE(p.name, N''),
            type_schema.name,
            user_type.name,
            COALESCE(system_type.name, user_type.name),
            p.max_length,
            p.precision,
            p.scale,
            p.is_output,
            p.is_cursor_ref,
            p.has_default_value,
            p.is_xml_document,
            p.xml_collection_id,
            p.is_readonly,
            p.is_nullable,
            p.encryption_type_desc,
            user_type.is_user_defined,
            user_type.is_assembly_type,
            user_type.is_table_type
        FROM sys.parameters AS p
        INNER JOIN sys.objects AS o
            ON o.object_id = p.object_id
        INNER JOIN sys.types AS user_type
            ON user_type.user_type_id = p.user_type_id
        INNER JOIN sys.schemas AS type_schema
            ON type_schema.schema_id = user_type.schema_id
        LEFT JOIN sys.types AS system_type
            ON system_type.user_type_id = p.system_type_id
           AND system_type.is_user_defined = 0
        WHERE o.is_ms_shipped = 0
          AND o.type IN (
              N'P',
              N'PC',
              N'RF',
              N'X',
              N'FN',
              N'FS',
              N'FT',
              N'IF',
              N'TF',
              N'AF')
        ORDER BY p.object_id, p.parameter_id;
        """;

    internal const string ExpressionDependenciesQuery =
        """
        SELECT
            dependency.referencing_id,
            dependency.referencing_minor_id,
            dependency.referencing_class,
            dependency.referencing_class_desc,
            dependency.is_schema_bound_reference,
            dependency.referenced_class,
            dependency.referenced_class_desc,
            dependency.referenced_server_name,
            dependency.referenced_database_name,
            dependency.referenced_schema_name,
            dependency.referenced_entity_name,
            dependency.referenced_id,
            dependency.referenced_minor_id,
            dependency.is_caller_dependent,
            dependency.is_ambiguous
        FROM sys.sql_expression_dependencies AS dependency
        ORDER BY
            dependency.referencing_class,
            dependency.referencing_id,
            dependency.referencing_minor_id,
            dependency.referenced_class,
            dependency.referenced_id,
            dependency.referenced_minor_id,
            dependency.referenced_server_name,
            dependency.referenced_database_name,
            dependency.referenced_schema_name,
            dependency.referenced_entity_name;
        """;

    private static async ValueTask<IReadOnlyList<SqlServerViewMetadata>>
        ReadViewsAsync(
            CatalogReadContext context,
            SqlServerInstanceMetadata instance,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var views = new List<SqlServerViewMetadata>();
        await using (SqlCommand command = Command(context, ViewsQuery))
        await using (SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                if (views.Count == limits.MaxViews)
                    throw LimitExceeded("view count");
                budget.AddStructuralRow();
                views.Add(new SqlServerViewMetadata(
                    RequiredInt32(reader, 0),
                    RequiredInt32(reader, 1),
                    RequiredString(reader, 2, budget, isName: true),
                    RequiredBoolean(reader, 3),
                    RequiredBoolean(reader, 4),
                    RequiredBoolean(reader, 5),
                    RequiredBoolean(reader, 6),
                    RequiredBoolean(reader, 7),
                    RequiredBoolean(reader, 8),
                    RequiredBoolean(reader, 9),
                    OptionalBoolean(reader, 10),
                    LedgerViewType: null,
                    LedgerViewTypeDescription: null,
                    IsDroppedLedgerView: null));
            }
        }

        if (instance.ProductMajorVersion < 16)
            return views.AsReadOnly();

        var ledgerFacts =
            new Dictionary<int, (byte Type, string Description, bool IsDropped)>();
        await using (SqlCommand command = Command(context, LedgerViewsQuery))
        await using (SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                if (ledgerFacts.Count == limits.MaxViews)
                    throw LimitExceeded("ledger-view count");
                budget.AddStructuralRow();
                int objectId = RequiredInt32(reader, 0);
                if (!ledgerFacts.TryAdd(
                        objectId,
                        (
                            RequiredByte(reader, 1),
                            RequiredString(reader, 2, budget),
                            RequiredBoolean(reader, 3))))
                {
                    throw InvalidProviderMetadata();
                }
            }
        }

        if (ledgerFacts.Count != views.Count)
            throw InvalidProviderMetadata();

        for (int index = 0; index < views.Count; index++)
        {
            SqlServerViewMetadata view = views[index];
            if (!ledgerFacts.TryGetValue(
                    view.ObjectId,
                    out (byte Type, string Description, bool IsDropped) ledger))
            {
                throw InvalidProviderMetadata();
            }

            views[index] = view with
            {
                LedgerViewType = ledger.Type,
                LedgerViewTypeDescription = ledger.Description,
                IsDroppedLedgerView = ledger.IsDropped,
            };
        }

        return views.AsReadOnly();
    }

    private static async ValueTask<IReadOnlyList<SqlServerViewColumnMetadata>>
        ReadViewColumnsAsync(
            CatalogReadContext context,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var columns = new List<SqlServerViewColumnMetadata>();
        await using SqlCommand command = Command(context, ViewColumnsQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (columns.Count == limits.MaxViewColumns)
                throw LimitExceeded("view-column count");
            budget.AddStructuralRow();
            columns.Add(new SqlServerViewColumnMetadata(
                RequiredInt32(reader, 0),
                RequiredInt32(reader, 1),
                RequiredString(reader, 2, budget, isName: true),
                RequiredString(reader, 3, budget, isName: true),
                RequiredString(reader, 4, budget, isName: true),
                RequiredString(reader, 5, budget, isName: true),
                RequiredInt16(reader, 6),
                RequiredByte(reader, 7),
                RequiredByte(reader, 8),
                OptionalString(reader, 9, budget),
                RequiredBoolean(reader, 10),
                RequiredBoolean(reader, 11),
                RequiredBoolean(reader, 12),
                RequiredBoolean(reader, 13),
                OptionalString(reader, 14, budget),
                RequiredBoolean(reader, 15),
                RequiredInt32(reader, 16)));
        }
        return columns.AsReadOnly();
    }

    private static async ValueTask<IReadOnlyList<SqlServerTriggerMetadata>>
        ReadTriggersAsync(
            CatalogReadContext context,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var triggers = new List<SqlServerTriggerMetadata>();
        await using SqlCommand command = Command(context, TriggersQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (triggers.Count == limits.MaxTriggers)
                throw LimitExceeded("trigger count");
            budget.AddStructuralRow();
            triggers.Add(new SqlServerTriggerMetadata(
                RequiredInt32(reader, 0),
                OptionalInt32(reader, 1),
                RequiredByte(reader, 2),
                RequiredString(reader, 3, budget),
                RequiredInt32(reader, 4),
                RequiredString(reader, 5, budget, isName: true),
                RequiredString(reader, 6, budget),
                RequiredString(reader, 7, budget),
                RequiredBoolean(reader, 8),
                RequiredBoolean(reader, 9),
                RequiredBoolean(reader, 10),
                OptionalBoolean(reader, 11),
                OptionalBoolean(reader, 12),
                OptionalBoolean(reader, 13),
                OptionalBoolean(reader, 14),
                OptionalBoolean(reader, 15),
                OptionalBoolean(reader, 16),
                OptionalBoolean(reader, 17),
                OptionalBoolean(reader, 18),
                OptionalBoolean(reader, 19),
                OptionalBoolean(reader, 20)));
        }
        return triggers.AsReadOnly();
    }

    private static async ValueTask<IReadOnlyList<SqlServerTriggerEventMetadata>>
        ReadTriggerEventsAsync(
            CatalogReadContext context,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var events = new List<SqlServerTriggerEventMetadata>();
        await using SqlCommand command = Command(context, TriggerEventsQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (events.Count == limits.MaxTriggerEvents)
                throw LimitExceeded("trigger-event count");
            budget.AddStructuralRow();
            events.Add(new SqlServerTriggerEventMetadata(
                RequiredInt32(reader, 0),
                RequiredInt32(reader, 1),
                RequiredString(reader, 2, budget),
                RequiredBoolean(reader, 3),
                RequiredBoolean(reader, 4),
                OptionalInt32(reader, 5),
                OptionalString(reader, 6, budget)));
        }
        return events.AsReadOnly();
    }

    private static async ValueTask<IReadOnlyList<SqlServerRoutineMetadata>>
        ReadRoutinesAsync(
            CatalogReadContext context,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var routines = new List<SqlServerRoutineMetadata>();
        await using SqlCommand command = Command(context, RoutinesQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (routines.Count == limits.MaxRoutines)
                throw LimitExceeded("routine count");
            budget.AddStructuralRow();
            routines.Add(new SqlServerRoutineMetadata(
                RequiredInt32(reader, 0),
                RequiredInt32(reader, 1),
                RequiredString(reader, 2, budget, isName: true),
                RequiredString(reader, 3, budget),
                RequiredString(reader, 4, budget),
                OptionalBoolean(reader, 5),
                OptionalBoolean(reader, 6),
                OptionalBoolean(reader, 7),
                OptionalBoolean(reader, 8),
                OptionalBoolean(reader, 9)));
        }
        return routines.AsReadOnly();
    }

    private static async ValueTask<IReadOnlyList<SqlServerModuleMetadata>>
        ReadModulesAsync(
            CatalogReadContext context,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var modules = new List<SqlServerModuleMetadata>();
        await using SqlCommand command = Command(context, ModulesQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (modules.Count == limits.MaxModules)
                throw LimitExceeded("module count");
            budget.AddStructuralRow();

            int objectId = RequiredInt32(reader, 0);
            int schemaId = RequiredInt32(reader, 1);
            int parentObjectId = RequiredInt32(reader, 2);
            string name = RequiredString(reader, 3, budget, isName: true);
            string objectType = RequiredString(reader, 4, budget);
            string objectTypeDescription = RequiredString(reader, 5, budget);
            long? definitionBytes = OptionalInt64(reader, 6);
            string? definition =
                OptionalExpression(reader, 7, definitionBytes, budget);

            modules.Add(new SqlServerModuleMetadata(
                objectId,
                schemaId,
                parentObjectId,
                name,
                objectType,
                objectTypeDescription,
                definitionBytes,
                definition,
                RequiredBoolean(reader, 8),
                RequiredBoolean(reader, 9),
                RequiredBoolean(reader, 10),
                RequiredBoolean(reader, 11),
                RequiredBoolean(reader, 12),
                RequiredBoolean(reader, 13),
                OptionalInt32(reader, 14),
                RequiredBoolean(reader, 15),
                RequiredBoolean(reader, 16),
                RequiredBoolean(reader, 17),
                OptionalBoolean(reader, 18)));
        }
        return modules.AsReadOnly();
    }

    private static async ValueTask<IReadOnlyList<SqlServerParameterMetadata>>
        ReadParametersAsync(
            CatalogReadContext context,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var parameters = new List<SqlServerParameterMetadata>();
        await using SqlCommand command = Command(context, ParametersQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (parameters.Count == limits.MaxParameters)
                throw LimitExceeded("parameter count");
            budget.AddStructuralRow();
            int objectId = RequiredInt32(reader, 0);
            int parameterId = RequiredInt32(reader, 1);
            string name = RequiredString(reader, 2, budget, isName: true);
            if ((parameterId == 0) != (name.Length == 0))
                throw InvalidProviderMetadata();
            parameters.Add(new SqlServerParameterMetadata(
                objectId,
                parameterId,
                name,
                RequiredString(reader, 3, budget, isName: true),
                RequiredString(reader, 4, budget, isName: true),
                RequiredString(reader, 5, budget, isName: true),
                RequiredInt16(reader, 6),
                RequiredByte(reader, 7),
                RequiredByte(reader, 8),
                RequiredBoolean(reader, 9),
                RequiredBoolean(reader, 10),
                RequiredBoolean(reader, 11),
                RequiredBoolean(reader, 12),
                RequiredInt32(reader, 13),
                RequiredBoolean(reader, 14),
                RequiredBoolean(reader, 15),
                OptionalString(reader, 16, budget),
                RequiredBoolean(reader, 17),
                RequiredBoolean(reader, 18),
                RequiredBoolean(reader, 19)));
        }
        return parameters.AsReadOnly();
    }

    private static async ValueTask<SqlServerExpressionDependencyAuditMetadata>
        ReadExpressionDependencyAuditAsync(
            CatalogReadContext context,
            SqlServerDatabaseMetadata database,
            ReaderBudget budget,
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        if (database.HasSelectSqlExpressionDependencies is not true)
            return SqlServerExpressionDependencyAuditMetadata.NotAttempted;

        var dependencies = new List<SqlServerExpressionDependencyMetadata>();
        await using SqlCommand command = Command(
            context,
            ExpressionDependenciesQuery);
        await using SqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (dependencies.Count == limits.MaxExpressionDependencies)
                throw LimitExceeded("expression-dependency count");
            budget.AddStructuralRow();
            dependencies.Add(new SqlServerExpressionDependencyMetadata(
                RequiredInt32(reader, 0),
                RequiredInt32(reader, 1),
                RequiredByte(reader, 2),
                RequiredString(reader, 3, budget),
                RequiredBoolean(reader, 4),
                RequiredByte(reader, 5),
                RequiredString(reader, 6, budget),
                OptionalString(reader, 7, budget, isName: true),
                OptionalString(reader, 8, budget, isName: true),
                OptionalString(reader, 9, budget, isName: true),
                RequiredString(reader, 10, budget, isName: true),
                OptionalInt32(reader, 11),
                RequiredInt32(reader, 12),
                RequiredBoolean(reader, 13),
                RequiredBoolean(reader, 14)));
        }
        return new SqlServerExpressionDependencyAuditMetadata(
            dependencies.AsReadOnly(),
            Attempted: true);
    }
}
