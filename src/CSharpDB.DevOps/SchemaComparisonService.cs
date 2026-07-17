using System.Text.Json;
using CSharpDB.Client.Models;
using ClientColumnDefinition = CSharpDB.Client.Models.ColumnDefinition;
using ClientForeignKeyDefinition = CSharpDB.Client.Models.ForeignKeyDefinition;

namespace CSharpDB.DevOps;

public sealed class SchemaComparisonService
{
    public async Task<SchemaDiffReport> CompareAsync(
        ISchemaCompareTarget source,
        ISchemaCompareTarget target,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(target);

        SchemaSnapshot sourceSnapshot = await source.LoadSchemaAsync(ct);
        SchemaSnapshot targetSnapshot = await target.LoadSchemaAsync(ct);
        return Compare(sourceSnapshot, targetSnapshot);
    }

    public SchemaDiffReport Compare(SchemaSnapshot source, SchemaSnapshot target)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(target);

        var changes = new List<SchemaDiffChange>();

        CompareTables(source.Tables, target.Tables, changes);
        CompareIndexes(source.Indexes, target.Indexes, changes);
        CompareViews(source.Views, target.Views, changes);
        CompareTriggers(source.Triggers, target.Triggers, changes);
        CompareProcedures(source.Procedures, target.Procedures, changes);

        IReadOnlyList<string> warnings = changes
            .Where(change => !string.IsNullOrWhiteSpace(change.Warning))
            .Select(change => change.Warning!)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        return new SchemaDiffReport
        {
            Source = source.Target,
            Target = target.Target,
            Changes = changes,
            Warnings = warnings,
            Summary = BuildSummary(changes),
        };
    }

    private static void CompareTables(
        IReadOnlyList<TableSchema> sourceTables,
        IReadOnlyList<TableSchema> targetTables,
        List<SchemaDiffChange> changes)
    {
        var sourceByName = sourceTables.ToDictionary(table => table.TableName, StringComparer.OrdinalIgnoreCase);
        var targetByName = targetTables.ToDictionary(table => table.TableName, StringComparer.OrdinalIgnoreCase);

        foreach (TableSchema source in sourceTables.OrderBy(table => table.TableName, StringComparer.OrdinalIgnoreCase))
        {
            if (!targetByName.TryGetValue(source.TableName, out TableSchema? target))
            {
                changes.Add(new SchemaDiffChange
                {
                    ObjectKind = SchemaObjectKind.Table,
                    ChangeKind = SchemaChangeKind.Added,
                    Name = source.TableName,
                    SourceDefinition = SchemaScriptRenderer.RenderCreateTable(source),
                });
                continue;
            }

            CompareColumns(source, target, changes);
            CompareForeignKeys(source, target, changes);
            CompareLogicalConstraints(source, target, changes);
        }

        foreach (TableSchema target in targetTables.OrderBy(table => table.TableName, StringComparer.OrdinalIgnoreCase))
        {
            if (sourceByName.ContainsKey(target.TableName))
                continue;

            changes.Add(new SchemaDiffChange
            {
                ObjectKind = SchemaObjectKind.Table,
                ChangeKind = SchemaChangeKind.Removed,
                Name = target.TableName,
                TargetDefinition = SchemaScriptRenderer.RenderCreateTable(target),
                IsDestructive = true,
                Warning = $"Target table '{target.TableName}' would be dropped to match the source.",
            });
        }
    }

    private static void CompareColumns(TableSchema sourceTable, TableSchema targetTable, List<SchemaDiffChange> changes)
    {
        var sourceByName = sourceTable.Columns.ToDictionary(column => column.Name, StringComparer.OrdinalIgnoreCase);
        var targetByName = targetTable.Columns.ToDictionary(column => column.Name, StringComparer.OrdinalIgnoreCase);

        foreach (ClientColumnDefinition sourceColumn in sourceTable.Columns)
        {
            if (!targetByName.TryGetValue(sourceColumn.Name, out ClientColumnDefinition? targetColumn))
            {
                bool risky = !sourceColumn.Nullable;
                changes.Add(new SchemaDiffChange
                {
                    ObjectKind = SchemaObjectKind.Column,
                    ChangeKind = SchemaChangeKind.Added,
                    Name = $"{sourceTable.TableName}.{sourceColumn.Name}",
                    ParentName = sourceTable.TableName,
                    SourceDefinition = SchemaScriptRenderer.RenderColumn(sourceColumn),
                    Warning = risky
                        ? $"Column '{sourceTable.TableName}.{sourceColumn.Name}' is NOT NULL and may require a table rebuild or data backfill."
                        : null,
                });
                continue;
            }

            if (!ColumnEquals(sourceColumn, targetColumn))
            {
                changes.Add(new SchemaDiffChange
                {
                    ObjectKind = SchemaObjectKind.Column,
                    ChangeKind = SchemaChangeKind.Changed,
                    Name = $"{sourceTable.TableName}.{sourceColumn.Name}",
                    ParentName = sourceTable.TableName,
                    SourceDefinition = SchemaScriptRenderer.RenderColumn(sourceColumn),
                    TargetDefinition = SchemaScriptRenderer.RenderColumn(targetColumn),
                    IsDestructive = IsColumnChangeDestructive(sourceColumn, targetColumn),
                    Warning = $"Column '{sourceTable.TableName}.{sourceColumn.Name}' differs and may require a table rebuild.",
                    Details = DiffColumnDetails(sourceColumn, targetColumn),
                });
            }
        }

        foreach (ClientColumnDefinition targetColumn in targetTable.Columns)
        {
            if (sourceByName.ContainsKey(targetColumn.Name))
                continue;

            changes.Add(new SchemaDiffChange
            {
                ObjectKind = SchemaObjectKind.Column,
                ChangeKind = SchemaChangeKind.Removed,
                Name = $"{targetTable.TableName}.{targetColumn.Name}",
                ParentName = targetTable.TableName,
                TargetDefinition = SchemaScriptRenderer.RenderColumn(targetColumn),
                IsDestructive = true,
                Warning = $"Target column '{targetTable.TableName}.{targetColumn.Name}' would be dropped to match the source.",
            });
        }
    }

    private static void CompareForeignKeys(TableSchema sourceTable, TableSchema targetTable, List<SchemaDiffChange> changes)
    {
        var sourceByName = sourceTable.ForeignKeys.ToDictionary(foreignKey => foreignKey.ConstraintName, StringComparer.OrdinalIgnoreCase);
        var targetByName = targetTable.ForeignKeys.ToDictionary(foreignKey => foreignKey.ConstraintName, StringComparer.OrdinalIgnoreCase);

        foreach (ClientForeignKeyDefinition sourceForeignKey in sourceTable.ForeignKeys)
        {
            if (!targetByName.TryGetValue(sourceForeignKey.ConstraintName, out ClientForeignKeyDefinition? targetForeignKey))
            {
                changes.Add(new SchemaDiffChange
                {
                    ObjectKind = SchemaObjectKind.ForeignKey,
                    ChangeKind = SchemaChangeKind.Added,
                    Name = $"{sourceTable.TableName}.{sourceForeignKey.ConstraintName}",
                    ParentName = sourceTable.TableName,
                    SourceDefinition = RenderForeignKey(sourceForeignKey),
                    Warning = $"Foreign key '{sourceForeignKey.ConstraintName}' requires schema migration support before it can be applied automatically.",
                });
                continue;
            }

            if (!ForeignKeyEquals(sourceForeignKey, targetForeignKey))
            {
                changes.Add(new SchemaDiffChange
                {
                    ObjectKind = SchemaObjectKind.ForeignKey,
                    ChangeKind = SchemaChangeKind.Changed,
                    Name = $"{sourceTable.TableName}.{sourceForeignKey.ConstraintName}",
                    ParentName = sourceTable.TableName,
                    SourceDefinition = RenderForeignKey(sourceForeignKey),
                    TargetDefinition = RenderForeignKey(targetForeignKey),
                    Warning = $"Foreign key '{sourceForeignKey.ConstraintName}' differs and requires migration review.",
                });
            }
        }

        foreach (ClientForeignKeyDefinition targetForeignKey in targetTable.ForeignKeys)
        {
            if (sourceByName.ContainsKey(targetForeignKey.ConstraintName))
                continue;

            changes.Add(new SchemaDiffChange
            {
                ObjectKind = SchemaObjectKind.ForeignKey,
                ChangeKind = SchemaChangeKind.Removed,
                Name = $"{targetTable.TableName}.{targetForeignKey.ConstraintName}",
                ParentName = targetTable.TableName,
                TargetDefinition = RenderForeignKey(targetForeignKey),
                IsDestructive = true,
                Warning = $"Target foreign key '{targetForeignKey.ConstraintName}' would be removed to match the source.",
            });
        }
    }

    private static void CompareLogicalConstraints(
        TableSchema sourceTable,
        TableSchema targetTable,
        List<SchemaDiffChange> changes)
    {
        bool keysEqual = ConstraintSignatures(sourceTable.KeyConstraints, KeyConstraintSignature)
            .SequenceEqual(
                ConstraintSignatures(targetTable.KeyConstraints, KeyConstraintSignature),
                StringComparer.Ordinal);
        bool checksEqual = ConstraintSignatures(sourceTable.CheckConstraints, CheckConstraintSignature)
            .SequenceEqual(
                ConstraintSignatures(targetTable.CheckConstraints, CheckConstraintSignature),
                StringComparer.Ordinal);
        if (keysEqual && checksEqual)
            return;

        var details = new Dictionary<string, string>();
        if (!keysEqual)
            details["keyConstraints"] = $"{targetTable.KeyConstraints.Count} -> {sourceTable.KeyConstraints.Count}";
        if (!checksEqual)
            details["checkConstraints"] = $"{targetTable.CheckConstraints.Count} -> {sourceTable.CheckConstraints.Count}";

        changes.Add(new SchemaDiffChange
        {
            ObjectKind = SchemaObjectKind.Table,
            ChangeKind = SchemaChangeKind.Changed,
            Name = sourceTable.TableName,
            SourceDefinition = SchemaScriptRenderer.RenderCreateTable(sourceTable),
            TargetDefinition = SchemaScriptRenderer.RenderCreateTable(targetTable),
            Warning = $"Table '{sourceTable.TableName}' has logical key or check constraint differences that require migration review.",
            Details = details,
        });
    }

    private static void CompareIndexes(
        IReadOnlyList<IndexSchema> sourceIndexes,
        IReadOnlyList<IndexSchema> targetIndexes,
        List<SchemaDiffChange> changes)
    {
        var sourceByName = sourceIndexes.ToDictionary(index => index.IndexName, StringComparer.OrdinalIgnoreCase);
        var targetByName = targetIndexes.ToDictionary(index => index.IndexName, StringComparer.OrdinalIgnoreCase);

        foreach (IndexSchema source in sourceIndexes.OrderBy(index => index.IndexName, StringComparer.OrdinalIgnoreCase))
        {
            if (!targetByName.TryGetValue(source.IndexName, out IndexSchema? target))
            {
                changes.Add(new SchemaDiffChange
                {
                    ObjectKind = SchemaObjectKind.Index,
                    ChangeKind = SchemaChangeKind.Added,
                    Name = source.IndexName,
                    ParentName = source.TableName,
                    SourceDefinition = SchemaScriptRenderer.RenderCreateIndex(source),
                });
                continue;
            }

            if (!IndexEquals(source, target))
            {
                changes.Add(new SchemaDiffChange
                {
                    ObjectKind = SchemaObjectKind.Index,
                    ChangeKind = SchemaChangeKind.Changed,
                    Name = source.IndexName,
                    ParentName = source.TableName,
                    SourceDefinition = SchemaScriptRenderer.RenderCreateIndex(source),
                    TargetDefinition = SchemaScriptRenderer.RenderCreateIndex(target),
                    Warning = $"Index '{source.IndexName}' differs and should be reviewed before recreate.",
                });
            }
        }

        foreach (IndexSchema target in targetIndexes.OrderBy(index => index.IndexName, StringComparer.OrdinalIgnoreCase))
        {
            if (sourceByName.ContainsKey(target.IndexName))
                continue;

            changes.Add(new SchemaDiffChange
            {
                ObjectKind = SchemaObjectKind.Index,
                ChangeKind = SchemaChangeKind.Removed,
                Name = target.IndexName,
                ParentName = target.TableName,
                TargetDefinition = SchemaScriptRenderer.RenderCreateIndex(target),
                Warning = $"Target index '{target.IndexName}' would be dropped to match the source.",
            });
        }
    }

    private static void CompareViews(
        IReadOnlyList<ViewDefinition> sourceViews,
        IReadOnlyList<ViewDefinition> targetViews,
        List<SchemaDiffChange> changes)
        => CompareNamedObjects(
            sourceViews,
            targetViews,
            view => view.Name,
            SchemaObjectKind.View,
            view => SchemaScriptRenderer.RenderCreateView(view),
            ViewEquals,
            changes);

    private static void CompareTriggers(
        IReadOnlyList<TriggerSchema> sourceTriggers,
        IReadOnlyList<TriggerSchema> targetTriggers,
        List<SchemaDiffChange> changes)
        => CompareNamedObjects(
            sourceTriggers,
            targetTriggers,
            trigger => trigger.TriggerName,
            SchemaObjectKind.Trigger,
            SchemaScriptRenderer.RenderCreateTrigger,
            TriggerEquals,
            changes);

    private static void CompareProcedures(
        IReadOnlyList<ProcedureDefinition> sourceProcedures,
        IReadOnlyList<ProcedureDefinition> targetProcedures,
        List<SchemaDiffChange> changes)
        => CompareNamedObjects(
            sourceProcedures,
            targetProcedures,
            procedure => procedure.Name,
            SchemaObjectKind.Procedure,
            RenderProcedure,
            ProcedureEquals,
            changes);

    private static void CompareNamedObjects<T>(
        IReadOnlyList<T> sourceItems,
        IReadOnlyList<T> targetItems,
        Func<T, string> getName,
        SchemaObjectKind kind,
        Func<T, string> render,
        Func<T, T, bool> equals,
        List<SchemaDiffChange> changes)
    {
        var sourceByName = sourceItems.ToDictionary(getName, StringComparer.OrdinalIgnoreCase);
        var targetByName = targetItems.ToDictionary(getName, StringComparer.OrdinalIgnoreCase);

        foreach (T source in sourceItems.OrderBy(getName, StringComparer.OrdinalIgnoreCase))
        {
            string name = getName(source);
            if (!targetByName.TryGetValue(name, out T? target))
            {
                changes.Add(new SchemaDiffChange
                {
                    ObjectKind = kind,
                    ChangeKind = SchemaChangeKind.Added,
                    Name = name,
                    SourceDefinition = render(source),
                    Warning = kind == SchemaObjectKind.Procedure
                        ? $"Procedure '{name}' is a client catalog object and cannot be emitted as SQL in V1."
                        : null,
                });
                continue;
            }

            if (!equals(source, target))
            {
                changes.Add(new SchemaDiffChange
                {
                    ObjectKind = kind,
                    ChangeKind = SchemaChangeKind.Changed,
                    Name = name,
                    SourceDefinition = render(source),
                    TargetDefinition = render(target),
                    Warning = kind == SchemaObjectKind.Procedure
                        ? $"Procedure '{name}' differs and requires client catalog deployment."
                        : $"{kind} '{name}' differs and should be reviewed before recreate.",
                });
            }
        }

        foreach (T target in targetItems.OrderBy(getName, StringComparer.OrdinalIgnoreCase))
        {
            string name = getName(target);
            if (sourceByName.ContainsKey(name))
                continue;

            changes.Add(new SchemaDiffChange
            {
                ObjectKind = kind,
                ChangeKind = SchemaChangeKind.Removed,
                Name = name,
                TargetDefinition = render(target),
                Warning = $"{kind} '{name}' exists only in the target.",
            });
        }
    }

    private static SchemaDiffSummary BuildSummary(IReadOnlyList<SchemaDiffChange> changes)
        => new()
        {
            TotalChanges = changes.Count,
            DestructiveChanges = changes.Count(change => change.IsDestructive),
            TableChanges = changes.Count(change => change.ObjectKind == SchemaObjectKind.Table),
            ColumnChanges = changes.Count(change => change.ObjectKind == SchemaObjectKind.Column),
            ForeignKeyChanges = changes.Count(change => change.ObjectKind == SchemaObjectKind.ForeignKey),
            IndexChanges = changes.Count(change => change.ObjectKind == SchemaObjectKind.Index),
            ViewChanges = changes.Count(change => change.ObjectKind == SchemaObjectKind.View),
            TriggerChanges = changes.Count(change => change.ObjectKind == SchemaObjectKind.Trigger),
            ProcedureChanges = changes.Count(change => change.ObjectKind == SchemaObjectKind.Procedure),
        };

    private static bool ColumnEquals(ClientColumnDefinition left, ClientColumnDefinition right)
        => left.Type == right.Type
           && left.Nullable == right.Nullable
           && left.IsPrimaryKey == right.IsPrimaryKey
           && left.IsIdentity == right.IsIdentity
           && string.Equals(left.Collation ?? string.Empty, right.Collation ?? string.Empty, StringComparison.OrdinalIgnoreCase)
           && string.Equals(
               NormalizeOptionalSql(left.DefaultSql),
               NormalizeOptionalSql(right.DefaultSql),
               StringComparison.Ordinal);

    private static bool IsColumnChangeDestructive(ClientColumnDefinition source, ClientColumnDefinition target)
        => source.Type != target.Type
           || !source.Nullable && target.Nullable
           || source.IsPrimaryKey != target.IsPrimaryKey
           || source.IsIdentity != target.IsIdentity;

    private static IReadOnlyDictionary<string, string> DiffColumnDetails(ClientColumnDefinition source, ClientColumnDefinition target)
    {
        var details = new Dictionary<string, string>();
        AddIfDifferent(details, "type", source.Type.ToString(), target.Type.ToString(), StringComparison.Ordinal);
        AddIfDifferent(details, "nullable", source.Nullable.ToString(), target.Nullable.ToString(), StringComparison.Ordinal);
        AddIfDifferent(details, "primaryKey", source.IsPrimaryKey.ToString(), target.IsPrimaryKey.ToString(), StringComparison.Ordinal);
        AddIfDifferent(details, "identity", source.IsIdentity.ToString(), target.IsIdentity.ToString(), StringComparison.Ordinal);
        AddIfDifferent(details, "collation", source.Collation ?? string.Empty, target.Collation ?? string.Empty, StringComparison.OrdinalIgnoreCase);
        AddIfDifferent(
            details,
            "defaultSql",
            NormalizeOptionalSql(source.DefaultSql),
            NormalizeOptionalSql(target.DefaultSql),
            StringComparison.Ordinal);
        return details;
    }

    private static void AddIfDifferent(
        Dictionary<string, string> details,
        string key,
        string source,
        string target,
        StringComparison comparison)
    {
        if (!string.Equals(source, target, comparison))
            details[key] = $"{target} -> {source}";
    }

    private static bool ForeignKeyEquals(ClientForeignKeyDefinition left, ClientForeignKeyDefinition right)
        => ResolveChildColumns(left).SequenceEqual(
               ResolveChildColumns(right),
               StringComparer.OrdinalIgnoreCase)
           && string.Equals(left.ReferencedTableName, right.ReferencedTableName, StringComparison.OrdinalIgnoreCase)
           && ResolveReferencedColumns(left).SequenceEqual(
               ResolveReferencedColumns(right),
               StringComparer.OrdinalIgnoreCase)
           && left.OnDelete == right.OnDelete;

    private static IReadOnlyList<string> ResolveChildColumns(ClientForeignKeyDefinition foreignKey)
        => foreignKey.ColumnNames.Count > 0
            ? foreignKey.ColumnNames
            : [foreignKey.ColumnName];

    private static IReadOnlyList<string> ResolveReferencedColumns(ClientForeignKeyDefinition foreignKey)
        => foreignKey.ReferencedColumnNames.Count > 0
            ? foreignKey.ReferencedColumnNames
            : [foreignKey.ReferencedColumnName];

    private static bool IndexEquals(IndexSchema left, IndexSchema right)
        => string.Equals(left.TableName, right.TableName, StringComparison.OrdinalIgnoreCase)
           && left.IsUnique == right.IsUnique
           && left.Columns.SequenceEqual(right.Columns, StringComparer.OrdinalIgnoreCase)
           && NormalizeCollations(left.ColumnCollations).SequenceEqual(NormalizeCollations(right.ColumnCollations), StringComparer.OrdinalIgnoreCase);

    private static IEnumerable<string> NormalizeCollations(IReadOnlyList<string?> collations)
        => collations.Select(collation => collation ?? string.Empty);

    private static IEnumerable<string> ConstraintSignatures<T>(
        IReadOnlyList<T> constraints,
        Func<T, string> signature)
        => constraints.Select(signature).OrderBy(value => value, StringComparer.Ordinal);

    private static string KeyConstraintSignature(KeyConstraintDefinition key)
        => string.Join(
            '\u001f',
            key.Kind.ToString(),
            NormalizeIdentifier(key.ConstraintName),
            string.Join('\u001e', key.Columns.Select(NormalizeIdentifier)),
            NormalizeIdentifier(key.BackingIndexName));

    private static string CheckConstraintSignature(CheckConstraintDefinition check)
        => string.Join(
            '\u001f',
            NormalizeIdentifier(check.ConstraintName),
            NormalizeIdentifier(check.ColumnName),
            NormalizeSql(check.ExpressionSql));

    private static string NormalizeIdentifier(string? value)
        => (value ?? string.Empty).Trim().ToUpperInvariant();

    private static string NormalizeOptionalSql(string? sql)
        => string.IsNullOrWhiteSpace(sql) ? string.Empty : NormalizeSql(sql);

    private static bool ViewEquals(ViewDefinition left, ViewDefinition right)
        => string.Equals(NormalizeSql(left.Sql), NormalizeSql(right.Sql), StringComparison.Ordinal);

    private static bool TriggerEquals(TriggerSchema left, TriggerSchema right)
        => string.Equals(left.TableName, right.TableName, StringComparison.OrdinalIgnoreCase)
           && left.Timing == right.Timing
           && left.Event == right.Event
           && string.Equals(NormalizeSql(left.BodySql), NormalizeSql(right.BodySql), StringComparison.Ordinal);

    private static bool ProcedureEquals(ProcedureDefinition left, ProcedureDefinition right)
        => string.Equals(NormalizeSql(left.BodySql), NormalizeSql(right.BodySql), StringComparison.Ordinal)
           && left.IsEnabled == right.IsEnabled
           && string.Equals(left.Description ?? string.Empty, right.Description ?? string.Empty, StringComparison.Ordinal)
           && JsonSerializer.Serialize(left.Parameters, SchemaDevOpsJson.Options)
              == JsonSerializer.Serialize(right.Parameters, SchemaDevOpsJson.Options);

    private static string NormalizeSql(string sql)
        => sql.Trim().TrimEnd(';').Trim();

    private static string RenderForeignKey(ClientForeignKeyDefinition foreignKey)
        => $"{foreignKey.ConstraintName}: ({string.Join(", ", ResolveChildColumns(foreignKey))}) -> " +
           $"{foreignKey.ReferencedTableName}.({string.Join(", ", ResolveReferencedColumns(foreignKey))}) " +
           $"ON DELETE {foreignKey.OnDelete}";

    private static string RenderProcedure(ProcedureDefinition procedure)
        => JsonSerializer.Serialize(procedure, SchemaDevOpsJson.Options);
}
