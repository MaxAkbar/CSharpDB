using System.Text;
using System.Text.RegularExpressions;
using System.Text.Json;
using CSharpDB.Client.Models;

namespace CSharpDB.DevOps;

public static partial class SchemaScriptRenderer
{
    public static string RenderCreateTable(TableSchema schema)
    {
        var sql = new StringBuilder();
        sql.Append("CREATE TABLE ").Append(Identifier(schema.TableName)).AppendLine(" (");

        for (int i = 0; i < schema.Columns.Count; i++)
        {
            bool hasTrailingItems = i < schema.Columns.Count - 1 || schema.ForeignKeys.Count > 0;
            sql.Append("  ")
                .Append(RenderColumn(schema.Columns[i]))
                .AppendLine(hasTrailingItems ? "," : string.Empty);
        }

        for (int i = 0; i < schema.ForeignKeys.Count; i++)
        {
            ForeignKeyDefinition foreignKey = schema.ForeignKeys[i];
            bool hasTrailingItems = i < schema.ForeignKeys.Count - 1;
            sql.Append("  CONSTRAINT ")
                .Append(Identifier(foreignKey.ConstraintName))
                .Append(" FOREIGN KEY (")
                .Append(Identifier(foreignKey.ColumnName))
                .Append(") REFERENCES ")
                .Append(Identifier(foreignKey.ReferencedTableName))
                .Append(" (")
                .Append(Identifier(foreignKey.ReferencedColumnName))
                .Append(')');

            if (foreignKey.OnDelete == ForeignKeyOnDeleteAction.Cascade)
                sql.Append(" ON DELETE CASCADE");

            sql.AppendLine(hasTrailingItems ? "," : string.Empty);
        }

        sql.Append(");");
        return sql.ToString();
    }

    public static string RenderColumn(ColumnDefinition column)
    {
        var sql = new StringBuilder();
        sql.Append(Identifier(column.Name))
            .Append(' ')
            .Append(column.Type.ToString().ToUpperInvariant());

        if (column.IsPrimaryKey)
            sql.Append(" PRIMARY KEY");
        if (column.IsIdentity)
            sql.Append(" IDENTITY");
        if (!column.Nullable)
            sql.Append(" NOT NULL");
        if (!string.IsNullOrWhiteSpace(column.Collation))
            sql.Append(" COLLATE ").Append(Identifier(column.Collation));

        return sql.ToString();
    }

    public static string RenderCreateIndex(IndexSchema index)
    {
        var sql = new StringBuilder();
        sql.Append("CREATE ");
        if (index.IsUnique)
            sql.Append("UNIQUE ");

        sql.Append("INDEX ")
            .Append(Identifier(index.IndexName))
            .Append(" ON ")
            .Append(Identifier(index.TableName))
            .Append(" (");

        for (int i = 0; i < index.Columns.Count; i++)
        {
            if (i > 0)
                sql.Append(", ");

            sql.Append(Identifier(index.Columns[i]));
            if (i < index.ColumnCollations.Count && !string.IsNullOrWhiteSpace(index.ColumnCollations[i]))
                sql.Append(" COLLATE ").Append(Identifier(index.ColumnCollations[i]!));
        }

        sql.Append(");");
        return sql.ToString();
    }

    public static string RenderCreateView(ViewDefinition view)
        => $"CREATE VIEW {Identifier(view.Name)} AS{Environment.NewLine}{view.Sql.Trim().TrimEnd(';')};";

    public static string RenderCreateTrigger(TriggerSchema trigger)
    {
        string timing = trigger.Timing.ToString().ToUpperInvariant();
        string eventName = trigger.Event.ToString().ToUpperInvariant();
        return $"""
            CREATE TRIGGER {Identifier(trigger.TriggerName)} {timing} {eventName} ON {Identifier(trigger.TableName)}
            BEGIN
              {trigger.BodySql.Trim().TrimEnd(';')};
            END;
            """;
    }

    public static string RenderSnapshotScript(SchemaSnapshot snapshot, SchemaScriptOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        options ??= new SchemaScriptOptions();

        string? objectName = string.IsNullOrWhiteSpace(options.ObjectName) ? null : options.ObjectName.Trim();

        var script = new StringBuilder();
        script.AppendLine("-- CSharpDB schema script");
        script.AppendLine($"-- Source: {snapshot.Target.DisplayName}");
        script.AppendLine($"-- Scope: {options.Scope}");
        if (options.ObjectKind is not null)
            script.AppendLine($"-- Object type: {options.ObjectKind}");
        if (objectName is not null)
            script.AppendLine($"-- Object: {objectName}");
        if (options.ObjectKind == SchemaObjectKind.Table && objectName is not null)
        {
            script.AppendLine($"-- Include indexes: {options.IncludeIndexes}");
            script.AppendLine($"-- Include triggers: {options.IncludeTriggers}");
            script.AppendLine($"-- Include related views: {options.IncludeRelatedViews}");
            script.AppendLine($"-- Include related procedures: {options.IncludeRelatedProcedures}");
        }

        script.AppendLine($"-- Generated UTC: {DateTime.UtcNow:O}");
        script.AppendLine();

        bool wroteAny = objectName is null
            ? AppendWholeSnapshot(script, snapshot)
            : AppendObjectSnapshot(script, snapshot, options, objectName);

        if (!wroteAny)
            script.AppendLine("-- No schema objects found for this scope.");

        return script.ToString();
    }

    private static bool AppendWholeSnapshot(StringBuilder script, SchemaSnapshot snapshot)
    {
        bool wroteAny = false;
        foreach (TableSchema table in snapshot.Tables.OrderBy(table => table.TableName, StringComparer.OrdinalIgnoreCase))
        {
            script.AppendLine(RenderCreateTable(table));
            script.AppendLine();
            wroteAny = true;
        }

        foreach (IndexSchema index in snapshot.Indexes
                     .OrderBy(index => index.TableName, StringComparer.OrdinalIgnoreCase)
                     .ThenBy(index => index.IndexName, StringComparer.OrdinalIgnoreCase))
        {
            script.AppendLine(RenderCreateIndex(index));
            script.AppendLine();
            wroteAny = true;
        }

        foreach (ViewDefinition view in snapshot.Views.OrderBy(view => view.Name, StringComparer.OrdinalIgnoreCase))
        {
            script.AppendLine(RenderCreateView(view));
            script.AppendLine();
            wroteAny = true;
        }

        foreach (TriggerSchema trigger in snapshot.Triggers
                     .OrderBy(trigger => trigger.TableName, StringComparer.OrdinalIgnoreCase)
                     .ThenBy(trigger => trigger.TriggerName, StringComparer.OrdinalIgnoreCase))
        {
            script.AppendLine(RenderCreateTrigger(trigger).Trim());
            script.AppendLine();
            wroteAny = true;
        }

        foreach (ProcedureDefinition procedure in snapshot.Procedures.OrderBy(procedure => procedure.Name, StringComparer.OrdinalIgnoreCase))
            wroteAny |= AppendProcedure(script, procedure);

        return wroteAny;
    }

    private static bool AppendObjectSnapshot(
        StringBuilder script,
        SchemaSnapshot snapshot,
        SchemaScriptOptions options,
        string objectName)
    {
        return options.ObjectKind switch
        {
            SchemaObjectKind.Table => AppendTableObject(script, snapshot, options, objectName),
            SchemaObjectKind.Index => AppendIndexObject(script, snapshot, objectName),
            SchemaObjectKind.View => AppendViewObject(script, snapshot, objectName),
            SchemaObjectKind.Trigger => AppendTriggerObject(script, snapshot, objectName),
            SchemaObjectKind.Procedure => AppendProcedureObject(script, snapshot, objectName),
            null => AppendDetectedObject(script, snapshot, options, objectName),
            _ => false,
        };
    }

    private static bool AppendDetectedObject(
        StringBuilder script,
        SchemaSnapshot snapshot,
        SchemaScriptOptions options,
        string objectName)
    {
        if (snapshot.Tables.Any(table => table.TableName.Equals(objectName, StringComparison.OrdinalIgnoreCase)))
            return AppendTableObject(script, snapshot, options, objectName);
        if (snapshot.Indexes.Any(index => index.IndexName.Equals(objectName, StringComparison.OrdinalIgnoreCase)))
            return AppendIndexObject(script, snapshot, objectName);
        if (snapshot.Views.Any(view => view.Name.Equals(objectName, StringComparison.OrdinalIgnoreCase)))
            return AppendViewObject(script, snapshot, objectName);
        if (snapshot.Triggers.Any(trigger => trigger.TriggerName.Equals(objectName, StringComparison.OrdinalIgnoreCase)))
            return AppendTriggerObject(script, snapshot, objectName);
        if (snapshot.Procedures.Any(procedure => procedure.Name.Equals(objectName, StringComparison.OrdinalIgnoreCase)))
            return AppendProcedureObject(script, snapshot, objectName);

        return false;
    }

    private static bool AppendTableObject(
        StringBuilder script,
        SchemaSnapshot snapshot,
        SchemaScriptOptions options,
        string tableName)
    {
        TableSchema? table = snapshot.Tables.FirstOrDefault(table => table.TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase));
        if (table is null)
            return false;

        bool wroteAny = true;
        script.AppendLine(RenderCreateTable(table));
        script.AppendLine();

        if (options.IncludeIndexes)
        {
            foreach (IndexSchema index in snapshot.Indexes
                         .Where(index => index.TableName.Equals(table.TableName, StringComparison.OrdinalIgnoreCase))
                         .OrderBy(index => index.IndexName, StringComparer.OrdinalIgnoreCase))
            {
                script.AppendLine(RenderCreateIndex(index));
                script.AppendLine();
            }
        }

        if (options.IncludeTriggers)
        {
            foreach (TriggerSchema trigger in snapshot.Triggers
                         .Where(trigger => trigger.TableName.Equals(table.TableName, StringComparison.OrdinalIgnoreCase))
                         .OrderBy(trigger => trigger.TriggerName, StringComparer.OrdinalIgnoreCase))
            {
                script.AppendLine(RenderCreateTrigger(trigger).Trim());
                script.AppendLine();
            }
        }

        if (options.IncludeRelatedViews)
        {
            foreach (ViewDefinition view in snapshot.Views
                         .Where(view => TextReferencesName(view.Sql, table.TableName))
                         .OrderBy(view => view.Name, StringComparer.OrdinalIgnoreCase))
            {
                script.AppendLine(RenderCreateView(view));
                script.AppendLine();
            }
        }

        if (options.IncludeRelatedProcedures)
        {
            foreach (ProcedureDefinition procedure in snapshot.Procedures
                         .Where(procedure => TextReferencesName(procedure.BodySql, table.TableName))
                         .OrderBy(procedure => procedure.Name, StringComparer.OrdinalIgnoreCase))
            {
                AppendProcedure(script, procedure);
            }
        }

        return wroteAny;
    }

    private static bool AppendIndexObject(StringBuilder script, SchemaSnapshot snapshot, string indexName)
    {
        IndexSchema? index = snapshot.Indexes.FirstOrDefault(index => index.IndexName.Equals(indexName, StringComparison.OrdinalIgnoreCase));
        if (index is null)
            return false;

        script.AppendLine(RenderCreateIndex(index));
        script.AppendLine();
        return true;
    }

    private static bool AppendViewObject(StringBuilder script, SchemaSnapshot snapshot, string viewName)
    {
        ViewDefinition? view = snapshot.Views.FirstOrDefault(view => view.Name.Equals(viewName, StringComparison.OrdinalIgnoreCase));
        if (view is null)
            return false;

        script.AppendLine(RenderCreateView(view));
        script.AppendLine();
        return true;
    }

    private static bool AppendTriggerObject(StringBuilder script, SchemaSnapshot snapshot, string triggerName)
    {
        TriggerSchema? trigger = snapshot.Triggers.FirstOrDefault(trigger => trigger.TriggerName.Equals(triggerName, StringComparison.OrdinalIgnoreCase));
        if (trigger is null)
            return false;

        script.AppendLine(RenderCreateTrigger(trigger).Trim());
        script.AppendLine();
        return true;
    }

    private static bool AppendProcedureObject(StringBuilder script, SchemaSnapshot snapshot, string procedureName)
    {
        ProcedureDefinition? procedure = snapshot.Procedures.FirstOrDefault(procedure => procedure.Name.Equals(procedureName, StringComparison.OrdinalIgnoreCase));
        return procedure is not null && AppendProcedure(script, procedure);
    }

    private static bool AppendProcedure(StringBuilder script, ProcedureDefinition procedure)
    {
        script.AppendLine($"-- Procedure '{procedure.Name}' is a client catalog object and is not emitted as executable SQL in V1.");
        AppendCommentedBlock(script, JsonSerializer.Serialize(procedure, SchemaDevOpsJson.Options));
        script.AppendLine();
        return true;
    }

    private static bool TextReferencesName(string text, string name)
        => Regex.IsMatch(
            text,
            $@"(?<![A-Za-z0-9_]){Regex.Escape(name)}(?![A-Za-z0-9_])",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    public static string RenderDeployScript(SchemaDiffReport report)
    {
        ArgumentNullException.ThrowIfNull(report);

        var script = new StringBuilder();
        script.AppendLine("-- CSharpDB schema compare deployment preview");
        script.AppendLine($"-- Source: {report.Source.DisplayName}");
        script.AppendLine($"-- Target: {report.Target.DisplayName}");
        script.AppendLine($"-- Generated UTC: {report.GeneratedUtc:O}");
        script.AppendLine("-- Review warnings before executing this script.");
        script.AppendLine();

        foreach (SchemaDiffChange change in report.Changes)
        {
            if (!string.IsNullOrWhiteSpace(change.Warning))
                script.AppendLine($"-- WARNING: {change.Warning}");

            switch (change.ChangeKind)
            {
                case SchemaChangeKind.Added when change.ObjectKind == SchemaObjectKind.Column
                    && change.SourceDefinition is not null
                    && !string.IsNullOrWhiteSpace(change.ParentName):
                    script.Append("ALTER TABLE ")
                        .Append(Identifier(change.ParentName))
                        .Append(" ADD COLUMN ")
                        .Append(change.SourceDefinition.Trim())
                        .AppendLine(";");
                    script.AppendLine();
                    break;
                case SchemaChangeKind.Added when change.SourceDefinition is not null
                    && change.ObjectKind is SchemaObjectKind.Table or SchemaObjectKind.Index or SchemaObjectKind.View or SchemaObjectKind.Trigger:
                    script.AppendLine(change.SourceDefinition.Trim());
                    script.AppendLine();
                    break;
                case SchemaChangeKind.Removed when change.ObjectKind == SchemaObjectKind.Table:
                    script.AppendLine($"-- Destructive: target-only table {change.Name}");
                    script.AppendLine($"-- DROP TABLE {Identifier(change.Name)};");
                    script.AppendLine();
                    break;
                case SchemaChangeKind.Removed when change.ObjectKind == SchemaObjectKind.Index:
                    script.AppendLine($"-- Target-only index {change.Name}");
                    script.AppendLine($"-- DROP INDEX {Identifier(change.Name)};");
                    script.AppendLine();
                    break;
                case SchemaChangeKind.Changed:
                    script.AppendLine($"-- Changed {change.ObjectKind.ToString().ToLowerInvariant()}: {change.Name}");
                    if (change.SourceDefinition is not null)
                    {
                        script.AppendLine("-- Desired source definition:");
                        AppendCommentedBlock(script, change.SourceDefinition);
                    }
                    script.AppendLine();
                    break;
                default:
                    script.AppendLine($"-- {change.ChangeKind} {change.ObjectKind}: {change.Name}");
                    script.AppendLine("-- No SQL emitted for this change in V1.");
                    script.AppendLine();
                    break;
            }
        }

        if (report.Changes.Count == 0)
            script.AppendLine("-- No schema changes detected.");

        return script.ToString();
    }

    internal static string Identifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new ArgumentException("Identifier is required.", nameof(identifier));
        if (!SafeIdentifierRegex().IsMatch(identifier))
            throw new InvalidOperationException($"Identifier '{identifier}' cannot be emitted in a SQL script because quoted identifiers are not supported.");

        return identifier;
    }

    private static void AppendCommentedBlock(StringBuilder builder, string text)
    {
        foreach (string line in text.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n'))
            builder.AppendLine($"--   {line}");
    }

    [GeneratedRegex("^[A-Za-z_][A-Za-z0-9_]*$")]
    private static partial Regex SafeIdentifierRegex();
}

internal static class SchemaDevOpsJson
{
    internal static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = true,
    };
}
