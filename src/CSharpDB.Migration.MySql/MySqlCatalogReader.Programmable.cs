using System.Data;
using System.Globalization;
using MySqlConnector;

namespace CSharpDB.Migration.MySql;

internal sealed partial class MySqlCatalogReader
{
    internal const string ViewsQuery =
        """
        SELECT
            v.TABLE_SCHEMA,
            v.TABLE_NAME,
            OCTET_LENGTH(v.VIEW_DEFINITION),
            v.VIEW_DEFINITION,
            v.CHECK_OPTION,
            v.IS_UPDATABLE,
            v.SECURITY_TYPE,
            v.CHARACTER_SET_CLIENT,
            v.COLLATION_CONNECTION
        FROM INFORMATION_SCHEMA.VIEWS AS v
        WHERE BINARY v.TABLE_SCHEMA = BINARY @database_name
        ORDER BY
            BINARY v.TABLE_SCHEMA,
            BINARY v.TABLE_NAME;
        """;

    internal const string ViewColumnsQuery =
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
            c.COLUMN_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS AS c
        INNER JOIN INFORMATION_SCHEMA.TABLES AS t
            ON BINARY t.TABLE_SCHEMA = BINARY c.TABLE_SCHEMA
           AND BINARY t.TABLE_NAME = BINARY c.TABLE_NAME
        WHERE BINARY c.TABLE_SCHEMA = BINARY @database_name
          AND t.TABLE_TYPE = 'VIEW'
        ORDER BY
            BINARY c.TABLE_SCHEMA,
            BINARY c.TABLE_NAME,
            c.ORDINAL_POSITION,
            BINARY c.COLUMN_NAME;
        """;

    internal const string TriggersQuery =
        """
        SELECT
            tr.TRIGGER_SCHEMA,
            tr.TRIGGER_NAME,
            tr.EVENT_MANIPULATION,
            tr.EVENT_OBJECT_SCHEMA,
            tr.EVENT_OBJECT_TABLE,
            tr.ACTION_ORDER,
            OCTET_LENGTH(tr.ACTION_STATEMENT),
            tr.ACTION_STATEMENT,
            tr.ACTION_ORIENTATION,
            tr.ACTION_TIMING,
            tr.SQL_MODE,
            tr.CHARACTER_SET_CLIENT,
            tr.COLLATION_CONNECTION,
            tr.DATABASE_COLLATION
        FROM INFORMATION_SCHEMA.TRIGGERS AS tr
        WHERE BINARY tr.TRIGGER_SCHEMA = BINARY @database_name
        ORDER BY
            BINARY tr.TRIGGER_SCHEMA,
            BINARY tr.EVENT_OBJECT_SCHEMA,
            BINARY tr.EVENT_OBJECT_TABLE,
            BINARY tr.EVENT_MANIPULATION,
            BINARY tr.ACTION_TIMING,
            tr.ACTION_ORDER,
            BINARY tr.TRIGGER_NAME;
        """;

    internal const string RoutinesQuery =
        """
        SELECT
            r.ROUTINE_SCHEMA,
            r.SPECIFIC_NAME,
            r.ROUTINE_NAME,
            r.ROUTINE_TYPE,
            r.DATA_TYPE,
            OCTET_LENGTH(r.DTD_IDENTIFIER),
            r.DTD_IDENTIFIER,
            r.ROUTINE_BODY,
            OCTET_LENGTH(r.ROUTINE_DEFINITION),
            r.ROUTINE_DEFINITION,
            r.IS_DETERMINISTIC,
            r.SQL_DATA_ACCESS,
            r.SECURITY_TYPE,
            r.SQL_MODE,
            r.CHARACTER_SET_CLIENT,
            r.COLLATION_CONNECTION,
            r.DATABASE_COLLATION
        FROM INFORMATION_SCHEMA.ROUTINES AS r
        WHERE BINARY r.ROUTINE_SCHEMA = BINARY @database_name
        ORDER BY
            BINARY r.ROUTINE_SCHEMA,
            BINARY r.SPECIFIC_NAME,
            BINARY r.ROUTINE_TYPE,
            BINARY r.ROUTINE_NAME;
        """;

    internal const string RoutineParametersQuery =
        """
        SELECT
            p.SPECIFIC_SCHEMA,
            p.SPECIFIC_NAME,
            p.ROUTINE_TYPE,
            p.ORDINAL_POSITION,
            p.PARAMETER_MODE,
            p.PARAMETER_NAME,
            p.DATA_TYPE,
            OCTET_LENGTH(p.DTD_IDENTIFIER),
            p.DTD_IDENTIFIER,
            p.CHARACTER_SET_NAME,
            p.COLLATION_NAME,
            p.CHARACTER_MAXIMUM_LENGTH,
            p.NUMERIC_PRECISION,
            p.NUMERIC_SCALE,
            p.DATETIME_PRECISION
        FROM INFORMATION_SCHEMA.PARAMETERS AS p
        WHERE BINARY p.SPECIFIC_SCHEMA = BINARY @database_name
        ORDER BY
            BINARY p.SPECIFIC_SCHEMA,
            BINARY p.SPECIFIC_NAME,
            BINARY p.ROUTINE_TYPE,
            p.ORDINAL_POSITION,
            BINARY COALESCE(p.PARAMETER_NAME, '');
        """;

    private async ValueTask<IReadOnlyList<MySqlViewMetadata>> ReadViewsAsync(
        CatalogReadContext context,
        string selectedDatabase,
        IReadOnlyList<MySqlViewMetadata> visibleViews,
        ReaderBudget budget,
        MySqlInspectionLimits limits,
        CancellationToken cancellationToken)
    {
        var views = visibleViews.ToArray();
        var indexesByIdentity = new Dictionary<string, int>(
            visibleViews.Count,
            StringComparer.Ordinal);
        for (int index = 0; index < visibleViews.Count; index++)
        {
            MySqlViewMetadata view = visibleViews[index];
            if (!indexesByIdentity.TryAdd(
                    ObjectIdentity(view.SchemaName, view.Name),
                    index))
            {
                throw InvalidProviderMetadata();
            }
        }

        var enriched = new HashSet<string>(StringComparer.Ordinal);
        await using MySqlCommand command = Command(context, ViewsQuery);
        AddDatabaseParameter(command, selectedDatabase);
        await using MySqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            budget.AddStructuralRow();
            string schemaName = RequiredString(reader, 0, budget, isName: true);
            string name = RequiredString(reader, 1, budget, isName: true);
            string identity = ObjectIdentity(schemaName, name);
            if (!string.Equals(
                    schemaName,
                    selectedDatabase,
                    StringComparison.Ordinal) ||
                !indexesByIdentity.TryGetValue(identity, out int index) ||
                !enriched.Add(identity))
            {
                throw InvalidProviderMetadata();
            }

            long? sourceDefinitionBytes = OptionalInt64(reader, 2);
            bool hasDefinition = !reader.IsDBNull(3);
            if (hasDefinition && sourceDefinitionBytes is null)
                throw InvalidProviderMetadata();
            budget.PreflightDefinition(sourceDefinitionBytes);
            string? definition = hasDefinition
                ? reader.GetString(3)
                : null;
            long? definitionBytes = null;
            if (definition is null)
            {
                if (sourceDefinitionBytes is not null)
                    throw InvalidProviderMetadata();
            }
            else
            {
                definitionBytes = budget.AddDefinition(
                    definition,
                    sourceDefinitionBytes);
            }

            views[index] = new MySqlViewMetadata(
                schemaName,
                name,
                MetadataVisible: true,
                definitionBytes,
                definition,
                RequiredString(reader, 4, budget),
                ParseYesNo(RequiredString(reader, 5, budget)),
                RequiredString(reader, 6, budget),
                RequiredString(reader, 7, budget),
                RequiredString(reader, 8, budget));
        }

        if (views.Length > limits.MaxViews)
            throw LimitExceeded("view");
        return Array.AsReadOnly(views);
    }

    private async ValueTask<IReadOnlyList<MySqlViewColumnMetadata>>
        ReadViewColumnsAsync(
            CatalogReadContext context,
            string selectedDatabase,
            IReadOnlyList<MySqlViewMetadata> views,
            ReaderBudget budget,
            MySqlInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var viewIdentities = new HashSet<string>(
            views.Select(static view =>
                ObjectIdentity(view.SchemaName, view.Name)),
            StringComparer.Ordinal);
        var ordinalIdentities = new HashSet<string>(StringComparer.Ordinal);
        var nameIdentities = new HashSet<string>(StringComparer.Ordinal);
        var columns = new List<MySqlViewColumnMetadata>();

        await using MySqlCommand command = Command(
            context,
            ViewColumnsQuery);
        AddDatabaseParameter(command, selectedDatabase);
        await using MySqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (columns.Count >= limits.MaxViewColumns)
                throw LimitExceeded("view column");
            budget.AddStructuralRow();

            string schemaName = RequiredString(reader, 0, budget, isName: true);
            string viewName = RequiredString(reader, 1, budget, isName: true);
            int ordinalPosition = RequiredInt32(reader, 2);
            string name = RequiredString(reader, 3, budget, isName: true);
            if (!string.Equals(
                    schemaName,
                    selectedDatabase,
                    StringComparison.Ordinal) ||
                ordinalPosition <= 0 ||
                !viewIdentities.Contains(ObjectIdentity(schemaName, viewName)))
            {
                throw InvalidProviderMetadata();
            }

            string dataType = RequiredString(reader, 4, budget);
            bool isNullable = ParseYesNo(RequiredString(reader, 5, budget));
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

            string viewIdentity = ObjectIdentity(schemaName, viewName);
            string ordinalIdentity = string.Concat(
                viewIdentity,
                "\0",
                ordinalPosition.ToString(CultureInfo.InvariantCulture));
            string nameIdentity = string.Concat(viewIdentity, "\0", name);
            if (!ordinalIdentities.Add(ordinalIdentity) ||
                !nameIdentities.Add(nameIdentity))
            {
                throw InvalidProviderMetadata();
            }

            columns.Add(new MySqlViewColumnMetadata(
                schemaName,
                viewName,
                ordinalPosition,
                name,
                dataType,
                isNullable,
                characterSetName,
                collationName,
                characterMaximumLength,
                numericPrecision,
                numericScale,
                dateTimePrecision,
                columnTypeBytes,
                columnType));
        }

        return columns.AsReadOnly();
    }

    private async ValueTask<IReadOnlyList<MySqlTriggerMetadata>>
        ReadTriggersAsync(
            CatalogReadContext context,
            string selectedDatabase,
            IReadOnlyList<MySqlTableMetadata> tables,
            ReaderBudget budget,
            MySqlInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var tableIdentities = new HashSet<string>(
            tables.Select(static table =>
                ObjectIdentity(table.SchemaName, table.Name)),
            StringComparer.Ordinal);
        var identities = new HashSet<string>(StringComparer.Ordinal);
        var triggers = new List<MySqlTriggerMetadata>();

        await using MySqlCommand command = Command(context, TriggersQuery);
        AddDatabaseParameter(command, selectedDatabase);
        await using MySqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (triggers.Count >= limits.MaxTriggers)
                throw LimitExceeded("trigger");
            budget.AddStructuralRow();

            string schemaName = RequiredString(reader, 0, budget, isName: true);
            string name = RequiredString(reader, 1, budget, isName: true);
            string eventManipulation = RequiredString(reader, 2, budget);
            string eventObjectSchema = RequiredString(
                reader,
                3,
                budget,
                isName: true);
            string eventObjectTable = RequiredString(
                reader,
                4,
                budget,
                isName: true);
            int actionOrder = RequiredInt32(reader, 5);
            if (!string.Equals(
                    schemaName,
                    selectedDatabase,
                    StringComparison.Ordinal) ||
                !string.Equals(
                    eventObjectSchema,
                    selectedDatabase,
                    StringComparison.Ordinal) ||
                !tableIdentities.Contains(ObjectIdentity(
                    eventObjectSchema,
                    eventObjectTable)) ||
                actionOrder <= 0 ||
                eventManipulation is not ("INSERT" or "UPDATE" or "DELETE") ||
                !identities.Add(ObjectIdentity(schemaName, name)))
            {
                throw InvalidProviderMetadata();
            }

            long? sourceStatementBytes = OptionalInt64(reader, 6);
            if (sourceStatementBytes is null || reader.IsDBNull(7))
                throw InvalidProviderMetadata();
            budget.PreflightDefinition(sourceStatementBytes);
            string actionStatement = reader.GetString(7);
            long actionStatementBytes = budget.AddDefinition(
                actionStatement,
                sourceStatementBytes);
            string actionOrientation = RequiredString(reader, 8, budget);
            string actionTiming = RequiredString(reader, 9, budget);
            if (!string.Equals(
                    actionOrientation,
                    "ROW",
                    StringComparison.Ordinal) ||
                actionTiming is not ("BEFORE" or "AFTER"))
            {
                throw InvalidProviderMetadata();
            }

            triggers.Add(new MySqlTriggerMetadata(
                schemaName,
                name,
                eventManipulation,
                eventObjectSchema,
                eventObjectTable,
                actionOrder,
                actionStatementBytes,
                actionStatement,
                actionOrientation,
                actionTiming,
                RequiredString(reader, 10, budget),
                RequiredString(reader, 11, budget),
                RequiredString(reader, 12, budget),
                RequiredString(reader, 13, budget)));
        }

        return triggers.AsReadOnly();
    }

    private async ValueTask<IReadOnlyList<MySqlRoutineMetadata>>
        ReadRoutinesAsync(
            CatalogReadContext context,
            string selectedDatabase,
            ReaderBudget budget,
            MySqlInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var identities = new HashSet<string>(StringComparer.Ordinal);
        var routines = new List<MySqlRoutineMetadata>();

        await using MySqlCommand command = Command(context, RoutinesQuery);
        AddDatabaseParameter(command, selectedDatabase);
        await using MySqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (routines.Count >= limits.MaxRoutines)
                throw LimitExceeded("routine");
            budget.AddStructuralRow();

            string schemaName = RequiredString(reader, 0, budget, isName: true);
            string specificName = RequiredString(reader, 1, budget, isName: true);
            string name = RequiredString(reader, 2, budget, isName: true);
            string routineType = RequiredString(reader, 3, budget);
            if (!string.Equals(
                    schemaName,
                    selectedDatabase,
                    StringComparison.Ordinal) ||
                routineType is not ("PROCEDURE" or "FUNCTION") ||
                !identities.Add(RoutineIdentity(
                    schemaName,
                    specificName,
                    routineType)))
            {
                throw InvalidProviderMetadata();
            }

            string? dataType = OptionalString(reader, 4, budget);
            long? sourceDtdBytes = OptionalInt64(reader, 5);
            bool hasDtdIdentifier = !reader.IsDBNull(6);
            if (hasDtdIdentifier)
            {
                if (sourceDtdBytes is null)
                    throw InvalidProviderMetadata();
                budget.PreflightColumnType(sourceDtdBytes);
            }
            string? dtdIdentifier = hasDtdIdentifier
                ? reader.GetString(6)
                : null;
            long? dtdIdentifierBytes = null;
            if (string.IsNullOrEmpty(dataType) &&
                string.IsNullOrEmpty(dtdIdentifier) &&
                sourceDtdBytes is null or 0)
            {
                dataType = null;
                dtdIdentifier = null;
                sourceDtdBytes = null;
            }
            else
            {
                if (string.IsNullOrEmpty(dataType) ||
                    string.IsNullOrEmpty(dtdIdentifier) ||
                    sourceDtdBytes is null)
                {
                    throw InvalidProviderMetadata();
                }
                dtdIdentifierBytes = budget.ValidateColumnType(
                    dtdIdentifier,
                    sourceDtdBytes);
            }

            string routineBody = RequiredString(reader, 7, budget);
            if (!string.Equals(routineBody, "SQL", StringComparison.Ordinal))
                throw InvalidProviderMetadata();

            long? sourceDefinitionBytes = OptionalInt64(reader, 8);
            bool hasDefinition = !reader.IsDBNull(9);
            if (hasDefinition && sourceDefinitionBytes is null)
                throw InvalidProviderMetadata();
            budget.PreflightDefinition(sourceDefinitionBytes);
            string? definition = hasDefinition
                ? reader.GetString(9)
                : null;
            long? definitionBytes = null;
            if (definition is null)
            {
                if (sourceDefinitionBytes is not null)
                    throw InvalidProviderMetadata();
            }
            else
            {
                definitionBytes = budget.AddDefinition(
                    definition,
                    sourceDefinitionBytes);
            }

            routines.Add(new MySqlRoutineMetadata(
                schemaName,
                specificName,
                name,
                routineType,
                dataType,
                dtdIdentifierBytes,
                dtdIdentifier,
                routineBody,
                definitionBytes,
                definition,
                ParseYesNo(RequiredString(reader, 10, budget)),
                RequiredString(reader, 11, budget),
                RequiredString(reader, 12, budget),
                RequiredString(reader, 13, budget),
                RequiredString(reader, 14, budget),
                RequiredString(reader, 15, budget),
                RequiredString(reader, 16, budget)));
        }

        return routines.AsReadOnly();
    }

    private async ValueTask<IReadOnlyList<MySqlRoutineParameterMetadata>>
        ReadRoutineParametersAsync(
            CatalogReadContext context,
            string selectedDatabase,
            IReadOnlyList<MySqlRoutineMetadata> routines,
            ReaderBudget budget,
            MySqlInspectionLimits limits,
            CancellationToken cancellationToken)
    {
        var routineIdentities = new HashSet<string>(
            routines.Select(static routine => RoutineIdentity(
                routine.SchemaName,
                routine.SpecificName,
                routine.RoutineType)),
            StringComparer.Ordinal);
        var parameterIdentities = new HashSet<string>(StringComparer.Ordinal);
        var parameters = new List<MySqlRoutineParameterMetadata>();

        await using MySqlCommand command = Command(
            context,
            RoutineParametersQuery);
        AddDatabaseParameter(command, selectedDatabase);
        await using MySqlDataReader reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                cancellationToken)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (parameters.Count >= limits.MaxRoutineParameters)
                throw LimitExceeded("routine parameter");
            budget.AddStructuralRow();

            string schemaName = RequiredString(reader, 0, budget, isName: true);
            string specificName = RequiredString(reader, 1, budget, isName: true);
            string routineType = RequiredString(reader, 2, budget);
            int ordinalPosition = RequiredInt32(reader, 3);
            string? mode = OptionalString(reader, 4, budget);
            string? name = OptionalString(reader, 5, budget, isName: true);
            string routineIdentity = RoutineIdentity(
                schemaName,
                specificName,
                routineType);
            if (!string.Equals(
                    schemaName,
                    selectedDatabase,
                    StringComparison.Ordinal) ||
                routineType is not ("PROCEDURE" or "FUNCTION") ||
                ordinalPosition < 0 ||
                !routineIdentities.Contains(routineIdentity))
            {
                throw InvalidProviderMetadata();
            }

            if (ordinalPosition == 0)
            {
                if (routineType != "FUNCTION" ||
                    mode is not null ||
                    name is not null)
                {
                    throw InvalidProviderMetadata();
                }
            }
            else if (string.IsNullOrEmpty(name) ||
                     mode is not ("IN" or "OUT" or "INOUT"))
            {
                throw InvalidProviderMetadata();
            }

            string dataType = RequiredString(reader, 6, budget);
            long? sourceDtdBytes = OptionalInt64(reader, 7);
            budget.PreflightColumnType(sourceDtdBytes);
            if (reader.IsDBNull(8))
                throw InvalidProviderMetadata();
            string dtdIdentifier = reader.GetString(8);
            if (string.IsNullOrEmpty(dtdIdentifier))
                throw InvalidProviderMetadata();
            long dtdIdentifierBytes = budget.ValidateColumnType(
                dtdIdentifier,
                sourceDtdBytes);

            string parameterIdentity = string.Concat(
                routineIdentity,
                "\0",
                ordinalPosition.ToString(CultureInfo.InvariantCulture));
            if (!parameterIdentities.Add(parameterIdentity))
                throw InvalidProviderMetadata();

            parameters.Add(new MySqlRoutineParameterMetadata(
                schemaName,
                specificName,
                routineType,
                ordinalPosition,
                mode,
                name,
                dataType,
                dtdIdentifierBytes,
                dtdIdentifier,
                OptionalString(reader, 9, budget),
                OptionalString(reader, 10, budget),
                OptionalInt64(reader, 11),
                OptionalInt32(reader, 12),
                OptionalInt32(reader, 13),
                OptionalInt32(reader, 14)));
        }

        return parameters.AsReadOnly();
    }

    private static string ObjectIdentity(string schemaName, string name) =>
        string.Concat(schemaName, "\0", name);

    private static string RoutineIdentity(
        string schemaName,
        string specificName,
        string routineType) =>
        string.Concat(schemaName, "\0", specificName, "\0", routineType);
}
