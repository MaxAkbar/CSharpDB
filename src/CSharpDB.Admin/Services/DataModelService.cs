using System.Globalization;
using System.Text;
using CSharpDB.Admin.Models;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.ImportExport.TableArchives;
using CSharpDB.Sql;
using ArchiveColumn = CSharpDB.ImportExport.Models.TableArchiveColumn;
using ArchiveForeignKey = CSharpDB.ImportExport.Models.TableArchiveForeignKey;
using InternalTableRegistry = CSharpDB.Primitives.DbInternalTableRegistry;
using PrimitiveForeignKeyOnDeleteAction = CSharpDB.Primitives.ForeignKeyOnDeleteAction;

namespace CSharpDB.Admin.Services;

public interface IDataModelService
{
    Task<DataModelState?> RefreshModelAsync(DataModelState state, CancellationToken ct = default) => Task.FromResult<DataModelState?>(null);
    Task<DataModelChangePlan> ReviewChangesAsync(DataModelState state, CancellationToken ct = default) => throw new NotSupportedException("Schema review is unavailable for this client.");
    Task<DataModelApplyResult> ApplyReviewedChangesAsync(DataModelState state, DataModelChangePlan plan, CancellationToken ct = default) => throw new NotSupportedException("Transactional schema application is unavailable for this client.");
    Task<IReadOnlyList<DataModelDataCheckResult>> CheckDataAsync(DataModelState state, string tableName, CancellationToken ct = default) => throw new NotSupportedException("Data checks are unavailable for this client.");
    Task<DataModelState> BuildModelAsync(string? seedSourceName = null, int autoLayoutLimit = DataModelGraphBuilder.DefaultAutoLayoutLimit, CancellationToken ct = default);
    Task<DataModelState> BuildSelectionAsync(IReadOnlyCollection<string> sourceNames, DataModelSelectionMode selectionMode = DataModelSelectionMode.Exact, CancellationToken ct = default);
    Task<IReadOnlyList<DataModelSourceOption>> GetSourceOptionsAsync(CancellationToken ct = default);
    string BuildPreviewSql(DataModelState state);
    string BuildPendingOperationsPreview(DataModelState state);
    QueryDesignerState ToQueryDesignerState(DataModelState state);
}

public interface IDataModelDiagramService
{
    Task<IReadOnlyList<DataModelDiagramSummary>> GetDiagramsAsync(CancellationToken ct = default);
    Task<DataModelState?> LoadDiagramAsync(string name, CancellationToken ct = default);
    Task SaveDiagramAsync(string name, DataModelState state, CancellationToken ct = default);
    Task RenameDiagramAsync(string existingName, string newName, DataModelState state, CancellationToken ct = default);
    Task DeleteDiagramAsync(string name, CancellationToken ct = default);
    Task<DataModelApplyResult> ApplyPendingOperationsAsync(DataModelState state, CancellationToken ct = default);
}

public sealed partial class DataModelService(ICSharpDbClient client) : IDataModelService, IDataModelDiagramService
{
    private const string DiagramTableName = "__data_model_diagrams";
    private const string DiagramNameIndexName = "idx___data_model_diagrams_name";

    public async Task<DataModelState> BuildModelAsync(
        string? seedSourceName = null,
        int autoLayoutLimit = DataModelGraphBuilder.DefaultAutoLayoutLimit,
        CancellationToken ct = default)
    {
        string fingerprintBeforeLoad = await ReadSchemaFingerprintAsync(null, ct);
        IReadOnlyList<DataModelSourceMetadata> sources = await LoadSourcesAsync(ct);
        DataModelState state = DataModelGraphBuilder.Build(sources, seedSourceName, autoLayoutLimit);
        state.SchemaSnapshotUtc = DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture);
        state.SchemaFingerprint = await ReadSchemaFingerprintAsync(null, ct);
        if (state.SchemaFingerprint != fingerprintBeforeLoad) throw new InvalidOperationException("Schema changed while loading. Refresh the model.");
        return state;
    }

    public async Task<DataModelState> BuildSelectionAsync(
        IReadOnlyCollection<string> sourceNames,
        DataModelSelectionMode selectionMode = DataModelSelectionMode.Exact,
        CancellationToken ct = default)
    {
        string fingerprintBeforeLoad = await ReadSchemaFingerprintAsync(null, ct);
        IReadOnlyList<DataModelSourceMetadata> sources = await LoadSourcesAsync(ct);
        DataModelState state = DataModelGraphBuilder.BuildSelection(sources, sourceNames, selectionMode);
        state.SchemaSnapshotUtc = DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture);
        state.SchemaFingerprint = await ReadSchemaFingerprintAsync(null, ct);
        if (state.SchemaFingerprint != fingerprintBeforeLoad) throw new InvalidOperationException("Schema changed while loading. Refresh the model.");
        return state;
    }

    public async Task<IReadOnlyList<DataModelSourceOption>> GetSourceOptionsAsync(CancellationToken ct = default)
    {
        IReadOnlyList<DataModelSourceMetadata> sources = await LoadSourcesAsync(ct);
        return sources
            .Select(static source => new DataModelSourceOption
            {
                Name = source.TableName,
                Kind = source.Kind,
                SourceTableName = source.SourceTableName,
            })
            .OrderBy(static option => option.Kind)
            .ThenBy(static option => option.Name, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public async Task<IReadOnlyList<DataModelDiagramSummary>> GetDiagramsAsync(CancellationToken ct = default)
    {
        await EnsureDiagramCatalogAsync(ct);
        SqlExecutionResult result = await client.ExecuteSqlAsync(
            $"""
            SELECT id, name, diagram_json, created_utc, updated_utc
            FROM {DiagramTableName}
            ORDER BY name;
            """,
            ct);

        ThrowIfSqlError(result);
        var diagrams = new List<DataModelDiagramSummary>();
        foreach (object?[] row in result.Rows ?? [])
        {
            if (row.Length < 5 || row[1] is null)
                continue;

            string name = Convert.ToString(row[1], CultureInfo.InvariantCulture) ?? string.Empty;
            if (string.IsNullOrWhiteSpace(name))
                continue;

            string json = Convert.ToString(row[2], CultureInfo.InvariantCulture) ?? string.Empty;
            int sourceCount = 0;
            int pendingCount = 0;
            try
            {
                DataModelState? state = DataModelGraphBuilder.DeserializeState(json);
                sourceCount = state?.Nodes.Count ?? 0;
                pendingCount = state?.PendingOperations.Count ?? 0;
            }
            catch
            {
            }

            diagrams.Add(new DataModelDiagramSummary
            {
                Id = row[0] is null ? 0 : Convert.ToInt64(row[0], CultureInfo.InvariantCulture),
                Name = name,
                CreatedUtc = Convert.ToString(row[3], CultureInfo.InvariantCulture) ?? string.Empty,
                UpdatedUtc = Convert.ToString(row[4], CultureInfo.InvariantCulture) ?? string.Empty,
                SourceCount = sourceCount,
                PendingOperationCount = pendingCount,
            });
        }

        return diagrams;
    }

    public async Task<DataModelState?> LoadDiagramAsync(string name, CancellationToken ct = default)
    {
        await EnsureDiagramCatalogAsync(ct);
        string normalized = NormalizeDiagramName(name);
        SqlExecutionResult result = await client.ExecuteSqlAsync(
            $"""
            SELECT diagram_json
            FROM {DiagramTableName}
            WHERE name = {FormatSqlStringLiteral(normalized)}
            LIMIT 1;
            """,
            ct);

        ThrowIfSqlError(result);
        object?[]? row = result.Rows?.FirstOrDefault();
        if (row is null || row.Length == 0 || row[0] is null)
            return null;

        string json = Convert.ToString(row[0], CultureInfo.InvariantCulture) ?? string.Empty;
        DataModelState? saved = DataModelGraphBuilder.DeserializeState(json);
        if (saved is null)
            return null;

        saved.DiagramName = normalized;
        saved.SavedLayoutName = normalized;
        var restored = (await RefreshModelAsync(saved, ct))!;
        if (restored.PendingOperations.Count > 0) restored.SchemaFingerprint = saved.SchemaFingerprint;
        return restored;
    }

    public async Task<DataModelState?> RefreshModelAsync(DataModelState state, CancellationToken ct = default)
    {
        string baseline = await ReadSchemaFingerprintAsync(null, ct);
        var refreshed = DataModelGraphBuilder.BuildFromDiagramState(await LoadSourcesAsync(ct), state);
        if (await ReadSchemaFingerprintAsync(null, ct) != baseline)
            throw new InvalidOperationException("Schema changed while refreshing. Try again.");
        refreshed.SchemaFingerprint = baseline;
        refreshed.SchemaSnapshotUtc = DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture);
        return refreshed;
    }

    public async Task SaveDiagramAsync(string name, DataModelState state, CancellationToken ct = default)
    {
        await EnsureDiagramCatalogAsync(ct);
        string normalized = NormalizeDiagramName(name);
        state.Version = 5;
        state.DiagramName = normalized;
        state.SavedLayoutName = normalized;
        state.SchemaSnapshotUtc ??= DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture);
        string json = DataModelGraphBuilder.SerializeState(state);
        string now = DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture);

        bool exists = await DiagramExistsAsync(normalized, ct);
        SqlExecutionResult result = exists
            ? await client.ExecuteSqlAsync(
                $"""
                UPDATE {DiagramTableName}
                SET diagram_json = {FormatSqlStringLiteral(json)},
                    updated_utc = {FormatSqlStringLiteral(now)}
                WHERE name = {FormatSqlStringLiteral(normalized)};
                """,
                ct)
            : await client.ExecuteSqlAsync(
                $"""
                INSERT INTO {DiagramTableName} (name, diagram_json, created_utc, updated_utc)
                VALUES ({FormatSqlStringLiteral(normalized)}, {FormatSqlStringLiteral(json)}, {FormatSqlStringLiteral(now)}, {FormatSqlStringLiteral(now)});
                """,
                ct);

        ThrowIfSqlError(result);
    }

    public async Task RenameDiagramAsync(string existingName, string newName, DataModelState state, CancellationToken ct = default)
    {
        await EnsureDiagramCatalogAsync(ct);
        string existing = NormalizeDiagramName(existingName);
        string normalized = NormalizeDiagramName(newName);
        if (!string.Equals(existing, normalized, StringComparison.OrdinalIgnoreCase) && await DiagramExistsAsync(normalized, ct))
            throw new InvalidOperationException($"A diagram named '{normalized}' already exists.");

        state.DiagramName = normalized;
        state.SavedLayoutName = normalized;
        string json = DataModelGraphBuilder.SerializeState(state);
        string now = DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture);
        SqlExecutionResult result = await client.ExecuteSqlAsync(
            $"""
            UPDATE {DiagramTableName}
            SET name = {FormatSqlStringLiteral(normalized)},
                diagram_json = {FormatSqlStringLiteral(json)},
                updated_utc = {FormatSqlStringLiteral(now)}
            WHERE name = {FormatSqlStringLiteral(existing)};
            """,
            ct);

        ThrowIfSqlError(result);
    }

    public async Task DeleteDiagramAsync(string name, CancellationToken ct = default)
    {
        await EnsureDiagramCatalogAsync(ct);
        SqlExecutionResult result = await client.ExecuteSqlAsync(
            $"DELETE FROM {DiagramTableName} WHERE name = {FormatSqlStringLiteral(NormalizeDiagramName(name))};",
            ct);
        ThrowIfSqlError(result);
    }

    public QueryDesignerState ToQueryDesignerState(DataModelState state) =>
        DataModelGraphBuilder.ToQueryDesignerState(state);

    public string BuildPreviewSql(DataModelState state)
    {
        if (state.Nodes.Count == 0) return "-- Add model sources to preview schema DDL.";
        var sql = new StringBuilder("-- Preview only. Review before running in a query tab.\n");
        foreach (var node in state.Nodes.OrderBy(node => node.Name, StringComparer.OrdinalIgnoreCase))
        {
            if (node.Kind == DataModelNodeKind.ExternalTable)
            {
                sql.AppendLine($"-- Read-only external table: {node.Name}");
                continue;
            }
            var schema = new TableSchema { TableName = node.Name, SchemaId = node.SchemaId,
                Columns = node.Columns.Select(ToColumnDefinition).ToArray(), KeyConstraints = node.Keys, CheckConstraints = node.Checks };
            sql.AppendLine(CSharpDB.DevOps.SchemaScriptRenderer.RenderCreateTable(schema));
            foreach (var index in node.Indexes.Where(index => !index.IsEngineManaged))
                sql.AppendLine(CSharpDB.DevOps.SchemaScriptRenderer.RenderCreateIndex(new IndexSchema
                { IndexName = index.IndexName, TableName = node.Name, Columns = index.Columns, IsUnique = index.IsUnique, ColumnCollations = index.ColumnCollations }));
        }
        foreach (var relationship in state.Relationships.Where(relationship => relationship.Kind != DataModelRelationshipKind.ExternalArchiveForeignKey && relationship.IsResolved))
            sql.AppendLine(BuildForeignKeySql(new DataModelPendingOperation { Id = relationship.Id, TableName = relationship.LeftTable,
                ConstraintName = relationship.ConstraintName, ColumnNames = relationship.EffectiveColumnPairs.Select(pair => pair.ChildColumn).ToList(),
                ReferencedTableName = relationship.RightTable, ReferencedColumnNames = relationship.EffectiveColumnPairs.Select(pair => pair.ParentColumn).ToList(),
                OnDelete = relationship.OnDelete ?? "RESTRICT", OnUpdate = relationship.OnUpdate ?? "RESTRICT" }));
        return sql.ToString().TrimEnd();
    }

    private static ColumnDefinition ToColumnDefinition(DataModelColumn column)
    {
        var parsed = ((CreateTableStatement)Parser.Parse($"CREATE TABLE probe (value {NormalizeTypeLabel(column.TypeLabel)});")).Columns[0];
        var type = parsed.DeclaredType;
        return new ColumnDefinition { SchemaId = column.SchemaId, Name = column.Name, Type = type.StorageType switch
            { CSharpDB.Primitives.DbType.Integer => DbType.Integer, CSharpDB.Primitives.DbType.Real => DbType.Real,
                CSharpDB.Primitives.DbType.Text => DbType.Text, CSharpDB.Primitives.DbType.Blob => DbType.Blob, _ => DbType.Decimal },
            DeclaredType = new SqlTypeDescriptor { Kind = (SqlTypeKind)(int)type.Kind, Length = type.Length, Precision = type.Precision,
                Scale = type.Scale, FractionalSecondsPrecision = type.FractionalSecondsPrecision },
            IsPrimaryKey = column.IsPrimaryKey, IsIdentity = column.IsIdentity, IsRowVersion = column.IsRowVersion,
            Nullable = column.Nullable, DefaultSql = column.DefaultSql, Collation = column.Collation };
    }

    public string BuildPendingOperationsPreview(DataModelState state)
    {
        if (state.PendingOperations.Count == 0)
            return "-- No pending schema changes.";

        var sb = new StringBuilder();
        sb.AppendLine("-- Pending schema changes. Apply from the diagram toolbar after review.");
        foreach (DataModelPendingOperation operation in state.PendingOperations)
        {
            sb.AppendLine();
            sb.AppendLine("-- " + DescribeOperation(operation));
            sb.AppendLine(BuildOperationSql(operation));
        }

        return sb.ToString().TrimEnd();
    }

    public async Task<DataModelApplyResult> ApplyPendingOperationsAsync(DataModelState state, CancellationToken ct = default)
    {
        if (state.PendingOperations.Count == 0)
            return new DataModelApplyResult { Succeeded = true, Messages = ["No pending schema changes."] };

        return await ApplyReviewedChangesAsync(state, await ReviewChangesAsync(state, ct), ct);
    }

    private static void ApplyOperationToState(
        DataModelState state,
        DataModelPendingOperation operation)
    {
        switch (operation.Kind)
        {
            case DataModelPendingOperationKind.CreateTable:
                foreach (DataModelNode node in state.Nodes.Where(node => string.Equals(node.Name, operation.TableName, StringComparison.OrdinalIgnoreCase)))
                {
                    node.IsDraft = false;
                    node.Warnings.RemoveAll(static warning => warning.Contains("Pending create table", StringComparison.OrdinalIgnoreCase));
                }
                break;

            case DataModelPendingOperationKind.DropTable:
                state.Nodes.RemoveAll(node => string.Equals(node.Name, operation.TableName, StringComparison.OrdinalIgnoreCase));
                DataModelGroups.Normalize(state);
                state.Relationships.RemoveAll(relationship =>
                    string.Equals(relationship.LeftTable, operation.TableName, StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(relationship.RightTable, operation.TableName, StringComparison.OrdinalIgnoreCase));
                break;

            case DataModelPendingOperationKind.RenameTable:
                string newTableName = RequireValue(operation.NewTableName, "new table name");
                foreach (DataModelNode node in state.Nodes.Where(node => string.Equals(node.Name, operation.TableName, StringComparison.OrdinalIgnoreCase)))
                    node.Name = newTableName;
                foreach (DataModelRelationship relationship in state.Relationships)
                {
                    if (string.Equals(relationship.LeftTable, operation.TableName, StringComparison.OrdinalIgnoreCase))
                        relationship.LeftTable = newTableName;
                    if (string.Equals(relationship.RightTable, operation.TableName, StringComparison.OrdinalIgnoreCase))
                        relationship.RightTable = newTableName;
                }
                break;

            case DataModelPendingOperationKind.AddColumn:
                DataModelNode? addNode = FindStateNode(state, operation.TableName);
                if (addNode is not null && !addNode.Columns.Any(column => string.Equals(column.Name, operation.ColumnName, StringComparison.OrdinalIgnoreCase)))
                {
                    string normalizedColumnType = NormalizeTypeLabel(operation.ColumnType);
                    bool isRowVersion = string.Equals(
                        normalizedColumnType,
                        "ROWVERSION",
                        StringComparison.Ordinal);
                    addNode.Columns.Add(new DataModelColumn
                    {
                        Name = RequireValue(operation.ColumnName, "column name"),
                        TypeLabel = normalizedColumnType,
                        IsRowVersion = isRowVersion,
                        Nullable = !operation.NotNull && !isRowVersion,
                    });
                }
                break;

            case DataModelPendingOperationKind.DropColumn:
                DataModelNode? dropNode = FindStateNode(state, operation.TableName);
                if (dropNode is not null)
                    dropNode.Columns.RemoveAll(column => string.Equals(column.Name, operation.ColumnName, StringComparison.OrdinalIgnoreCase));
                state.Relationships.RemoveAll(relationship =>
                    (string.Equals(relationship.LeftTable, operation.TableName, StringComparison.OrdinalIgnoreCase) &&
                     string.Equals(relationship.LeftColumn, operation.ColumnName, StringComparison.OrdinalIgnoreCase)) ||
                    (string.Equals(relationship.RightTable, operation.TableName, StringComparison.OrdinalIgnoreCase) &&
                     string.Equals(relationship.RightColumn, operation.ColumnName, StringComparison.OrdinalIgnoreCase)));
                break;

            case DataModelPendingOperationKind.RenameColumn:
                DataModelNode? renameNode = FindStateNode(state, operation.TableName);
                DataModelColumn? column = renameNode?.Columns.FirstOrDefault(column => string.Equals(column.Name, operation.ColumnName, StringComparison.OrdinalIgnoreCase));
                string newColumnName = RequireValue(operation.NewColumnName, "new column name");
                if (column is not null)
                    column.Name = newColumnName;
                foreach (DataModelRelationship relationship in state.Relationships)
                {
                    if (string.Equals(relationship.LeftTable, operation.TableName, StringComparison.OrdinalIgnoreCase) &&
                        string.Equals(relationship.LeftColumn, operation.ColumnName, StringComparison.OrdinalIgnoreCase))
                        relationship.LeftColumn = newColumnName;
                    if (string.Equals(relationship.RightTable, operation.TableName, StringComparison.OrdinalIgnoreCase) &&
                        string.Equals(relationship.RightColumn, operation.ColumnName, StringComparison.OrdinalIgnoreCase))
                        relationship.RightColumn = newColumnName;
                }
                break;


            case DataModelPendingOperationKind.DropForeignKey:
                state.Relationships.RemoveAll(relationship =>
                    !string.IsNullOrWhiteSpace(operation.ConstraintName) &&
                    string.Equals(relationship.ConstraintName ?? relationship.Id, operation.ConstraintName, StringComparison.OrdinalIgnoreCase));
                break;
        }
    }

    private static DataModelNode? FindStateNode(DataModelState state, string tableName) =>
        state.Nodes.FirstOrDefault(node => string.Equals(node.Name, tableName, StringComparison.OrdinalIgnoreCase));


    private static string BuildOperationSql(DataModelPendingOperation operation) => operation.Kind switch
    {
        DataModelPendingOperationKind.CreateTable => BuildCreateTableSql(operation),
        DataModelPendingOperationKind.DropTable => $"DROP TABLE {FormatIdentifier(operation.TableName)};",
        DataModelPendingOperationKind.RenameTable => $"ALTER TABLE {FormatIdentifier(operation.TableName)} RENAME TO {FormatIdentifier(RequireValue(operation.NewTableName, "new table name"))};",
        DataModelPendingOperationKind.AddColumn => $"ALTER TABLE {FormatIdentifier(operation.TableName)} ADD COLUMN {BuildColumnSql(operation)};",
        DataModelPendingOperationKind.DropColumn => $"ALTER TABLE {FormatIdentifier(operation.TableName)} DROP COLUMN {FormatIdentifier(RequireValue(operation.ColumnName, "column name"))};",
        DataModelPendingOperationKind.RenameColumn => $"ALTER TABLE {FormatIdentifier(operation.TableName)} RENAME COLUMN {FormatIdentifier(RequireValue(operation.ColumnName, "column name"))} TO {FormatIdentifier(RequireValue(operation.NewColumnName, "new column name"))};",
        DataModelPendingOperationKind.AddForeignKey => BuildForeignKeySql(operation),
        DataModelPendingOperationKind.DropForeignKey => $"ALTER TABLE {FormatIdentifier(operation.TableName)} DROP CONSTRAINT {FormatIdentifier(RequireValue(operation.ConstraintName, "constraint name"))};",
        _ => BuildExtendedOperationSql(operation),
    };

    private static string BuildCreateTableSql(DataModelPendingOperation operation)
    {
        IReadOnlyList<DataModelColumn> columns = operation.Columns.Count > 0
            ? operation.Columns
            :
            [
                new DataModelColumn
                {
                    Name = "Id",
                    TypeLabel = "INTEGER",
                    IsPrimaryKey = true,
                    IsIdentity = true,
                    Nullable = false,
                },
            ];

        return SchemaColumnRules.RenderCreateTable(operation.TableName, columns);
    }

    private static string BuildColumnSql(DataModelPendingOperation operation) =>
        SchemaColumnRules.RenderColumn(DataModelSchemaProjection.ColumnFromOperation(operation));

    private static string DescribeOperation(DataModelPendingOperation operation)
    {
        if (!string.IsNullOrWhiteSpace(operation.Description))
            return operation.Description;

        return operation.Kind switch
        {
            DataModelPendingOperationKind.CreateTable => $"Create table {operation.TableName}",
            DataModelPendingOperationKind.DropTable => $"Drop table {operation.TableName}",
            DataModelPendingOperationKind.RenameTable => $"Rename table {operation.TableName} to {operation.NewTableName}",
            DataModelPendingOperationKind.AddColumn => $"Add column {operation.TableName}.{operation.ColumnName}",
            DataModelPendingOperationKind.DropColumn => $"Drop column {operation.TableName}.{operation.ColumnName}",
            DataModelPendingOperationKind.RenameColumn => $"Rename column {operation.TableName}.{operation.ColumnName} to {operation.NewColumnName}",
            DataModelPendingOperationKind.AddForeignKey => $"Add relationship {operation.TableName}.{operation.ColumnName} -> {operation.ReferencedTableName}.{operation.ReferencedColumnName}",
            DataModelPendingOperationKind.DropForeignKey => $"Drop relationship {operation.ConstraintName}",
            _ => operation.Kind.ToString(),
        };
    }

    private async Task<IReadOnlyList<DataModelSourceMetadata>> LoadSourcesAsync(CancellationToken ct)
    {
        var sources = new List<DataModelSourceMetadata>();
        IReadOnlyList<string> tableNames = await client.GetTableNamesAsync(ct);
        IReadOnlyList<IndexSchema> indexes = await client.GetIndexesAsync(ct);
        IReadOnlyList<TriggerSchema> triggers = await client.GetTriggersAsync(ct);
        IReadOnlyList<ViewDefinition> views = await client.GetViewsAsync(ct);

        foreach (string tableName in tableNames.Where(static name => !IsSystemTableName(name)).OrderBy(static name => name, StringComparer.OrdinalIgnoreCase))
        {
            TableSchema? schema = await client.GetTableSchemaAsync(tableName, ct);
            if (schema is null)
                continue;

            sources.Add(new DataModelSourceMetadata
            {
                SchemaId = schema.SchemaId,
                Keys = EffectiveKeys(schema),
                Checks = schema.CheckConstraints,
                Dependencies = views.Where(view => ReferencesIdentifier(view.Sql, schema.TableName)).Select(view => new DataModelDependency("View", view.Name, view.Sql))
                    .Concat(triggers.Where(trigger => string.Equals(trigger.TableName, schema.TableName, StringComparison.OrdinalIgnoreCase) || ReferencesIdentifier(trigger.BodySql, schema.TableName))
                        .Select(trigger => new DataModelDependency("Trigger", trigger.TriggerName, trigger.BodySql))).ToArray(),
                TableName = schema.TableName,
                Kind = DataModelNodeKind.Table,
                Columns = schema.Columns.Select(MapColumn).ToArray(),
                ForeignKeys = schema.ForeignKeys.Select(MapForeignKey).ToArray(),
                Indexes = indexes
                    .Where(index => string.Equals(index.TableName, schema.TableName, StringComparison.OrdinalIgnoreCase))
                    .Select(index => MapIndex(index, schema))
                    .Concat(schema.ForeignKeys.Where(key => !string.IsNullOrWhiteSpace(key.SupportingIndexName)).Select(key => new DataModelIndexMetadata
                    { IndexName = key.SupportingIndexName, Columns = key.ColumnNames.Count > 0 ? key.ColumnNames : [key.ColumnName], IsEngineManaged = true }))
                    .Concat(schema.KeyConstraints.Where(key => !string.IsNullOrWhiteSpace(key.BackingIndexName)).Select(key => new DataModelIndexMetadata
                    { IndexName = key.BackingIndexName!, Columns = key.Columns, IsUnique = true, IsEngineManaged = true }))
                    .DistinctBy(index => index.IndexName, StringComparer.OrdinalIgnoreCase).ToArray(),
                TriggerCount = triggers.Count(trigger => string.Equals(trigger.TableName, schema.TableName, StringComparison.OrdinalIgnoreCase)),
            });
        }

        foreach (ExternalTableRegistration registration in await LoadExternalRegistrationsAsync(ct))
        {
            sources.Add(await LoadExternalSourceAsync(registration, ct));
        }

        return sources;
    }

    private static IReadOnlyList<KeyConstraintDefinition> EffectiveKeys(TableSchema schema)
    {
        if (schema.KeyConstraints.Any(key => key.Kind == KeyConstraintKind.PrimaryKey)) return schema.KeyConstraints;
        string[] primary = schema.Columns.Where(column => column.IsPrimaryKey).Select(column => column.Name).ToArray();
        return primary.Length == 0 ? schema.KeyConstraints : schema.KeyConstraints.Concat([new KeyConstraintDefinition
        { Kind = KeyConstraintKind.PrimaryKey, Columns = primary }]).ToArray();
    }

    private async Task<DataModelSourceMetadata> LoadExternalSourceAsync(
        ExternalTableRegistration registration,
        CancellationToken ct)
    {
        string resolvedPath = ResolveExternalPath(registration.Path);
        var warnings = new List<string>();
        try
        {
            var (schema, manifest) = await TableArchiveReader.ReadMetadataAsync(resolvedPath, ct);
            return new DataModelSourceMetadata
            {
                // Archive identities belong to the source database, not this registration.
                Keys = schema.KeyConstraints.Select(key => new KeyConstraintDefinition { ConstraintName = key.ConstraintName,
                    Kind = (KeyConstraintKind)(int)key.Kind, Columns = key.Columns }).ToArray(),
                Checks = schema.CheckConstraints.Select(check => new CheckConstraintDefinition { ConstraintName = check.ConstraintName,
                    ColumnName = check.ColumnName, ExpressionSql = check.ExpressionSql }).ToArray(),
                TableName = registration.TableName,
                Kind = DataModelNodeKind.ExternalTable,
                SourceTableName = string.IsNullOrWhiteSpace(registration.SourceTableName) ? schema.TableName : registration.SourceTableName,
                ArchivePath = registration.Path,
                ArchiveCreatedUtc = manifest.CreatedUtc.ToString("O", CultureInfo.InvariantCulture),
                RowCount = registration.RowCount,
                Columns = schema.Columns.Select(MapArchiveColumn).ToArray(),
                ForeignKeys = schema.ForeignKeys.Select(MapArchiveForeignKey).ToArray(),
            };
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or InvalidDataException or System.Text.Json.JsonException)
        {
            warnings.Add($"Could not read archive metadata from '{registration.Path}': {ex.Message}");
            return new DataModelSourceMetadata
            {
                TableName = registration.TableName,
                Kind = DataModelNodeKind.ExternalTable,
                SourceTableName = registration.SourceTableName,
                ArchivePath = registration.Path,
                RowCount = registration.RowCount,
                Warnings = warnings,
            };
        }
    }

    private async Task<IReadOnlyList<ExternalTableRegistration>> LoadExternalRegistrationsAsync(CancellationToken ct)
    {
        SqlExecutionResult result = await client.ExecuteSqlAsync(
            """
            SELECT table_name, path, source_table_name, row_count, created_utc
            FROM sys.external_tables
            ORDER BY table_name;
            """,
            ct);

        if (!string.IsNullOrWhiteSpace(result.Error) || result.Rows is null)
            return Array.Empty<ExternalTableRegistration>();

        var registrations = new List<ExternalTableRegistration>();
        foreach (object?[] row in result.Rows)
        {
            if (row.Length < 4 || row[0] is null)
                continue;

            registrations.Add(new ExternalTableRegistration(
                Convert.ToString(row[0], CultureInfo.InvariantCulture) ?? string.Empty,
                Convert.ToString(row[1], CultureInfo.InvariantCulture) ?? string.Empty,
                Convert.ToString(row[2], CultureInfo.InvariantCulture) ?? string.Empty,
                row[3] is null ? 0L : Convert.ToInt64(row[3], CultureInfo.InvariantCulture),
                row.Length >= 5 ? Convert.ToString(row[4], CultureInfo.InvariantCulture) : null));
        }

        return registrations;
    }

    private string ResolveExternalPath(string path)
    {
        if (Path.IsPathRooted(path))
            return path;

        string? databaseFolder = string.IsNullOrWhiteSpace(client.DataSource)
            ? null
            : Path.GetDirectoryName(client.DataSource);

        return Path.GetFullPath(Path.Combine(databaseFolder ?? Directory.GetCurrentDirectory(), path));
    }

    private static DataModelColumnMetadata MapColumn(ColumnDefinition column) => new()
    {
        SchemaId = column.SchemaId,
        Name = column.Name,
        TypeLabel = column.IsRowVersion ? "ROWVERSION" : column.EffectiveType.ToSql(),
        IsPrimaryKey = column.IsPrimaryKey,
        IsIdentity = column.IsIdentity,
        IsRowVersion = column.IsRowVersion,
        Nullable = column.Nullable,
        Collation = column.Collation,
        DefaultSql = column.DefaultSql,
    };

    private static DataModelColumnMetadata MapArchiveColumn(ArchiveColumn column) => new()
    {
        Name = column.Name,
        TypeLabel = column.IsRowVersion
            ? "ROWVERSION"
            : (column.DeclaredType ??
                CSharpDB.Primitives.SqlTypeDescriptor.FromLegacy(column.Type)).ToSql(),
        IsPrimaryKey = column.IsPrimaryKey,
        IsIdentity = column.IsIdentity,
        IsRowVersion = column.IsRowVersion,
        Nullable = column.Nullable,
        Collation = column.Collation,
        DefaultSql = column.DefaultSql,
    };

    private static DataModelForeignKeyMetadata MapForeignKey(ForeignKeyDefinition foreignKey) => new()
    {
        SchemaId = foreignKey.SchemaId,
        ConstraintName = foreignKey.ConstraintName,
        ColumnName = foreignKey.ColumnName,
        ColumnNames = foreignKey.ColumnNames.Count > 0
            ? foreignKey.ColumnNames
            : [foreignKey.ColumnName],
        ReferencedTableName = foreignKey.ReferencedTableName,
        ReferencedColumnName = foreignKey.ReferencedColumnName,
        ReferencedColumnNames = foreignKey.ReferencedColumnNames.Count > 0
            ? foreignKey.ReferencedColumnNames
            : [foreignKey.ReferencedColumnName],
        OnDelete = FormatReferentialAction(foreignKey.OnDelete),
        OnUpdate = FormatReferentialAction(foreignKey.OnUpdate),
    };

    private static DataModelForeignKeyMetadata MapArchiveForeignKey(ArchiveForeignKey foreignKey) => new()
    {
        ConstraintName = foreignKey.ConstraintName,
        ColumnName = foreignKey.ColumnName,
        ColumnNames = foreignKey.ColumnNames.Count > 0
            ? foreignKey.ColumnNames
            : [foreignKey.ColumnName],
        ReferencedTableName = foreignKey.ReferencedTableName,
        ReferencedColumnName = foreignKey.ReferencedColumnName,
        ReferencedColumnNames = foreignKey.ReferencedColumnNames.Count > 0
            ? foreignKey.ReferencedColumnNames
            : [foreignKey.ReferencedColumnName],
        OnDelete = FormatReferentialAction(foreignKey.OnDelete),
        OnUpdate = FormatReferentialAction(foreignKey.OnUpdate),
    };

    private static DataModelIndexMetadata MapIndex(IndexSchema index, TableSchema schema) => new()
    {
        ColumnCollations = index.ColumnCollations,
        IsEngineManaged = schema.ForeignKeys.Any(key => key.SupportingIndexName == index.IndexName) || schema.KeyConstraints.Any(key => key.BackingIndexName == index.IndexName),
        IndexName = index.IndexName,
        Columns = index.Columns,
        IsUnique = index.IsUnique,
    };

    private static bool IsSystemTableName(string name) =>
        InternalTableRegistry.IsInternalTable(name)
        || name.StartsWith("sys.", StringComparison.OrdinalIgnoreCase);

    private static string FormatIdentifier(string identifier)
    {
        if (!string.IsNullOrWhiteSpace(identifier)
            && (char.IsLetter(identifier[0]) || identifier[0] == '_')
            && identifier.All(static c => char.IsLetterOrDigit(c) || c == '_'))
        {
            return identifier;
        }

        return "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
    }

    private static string FormatSqlStringLiteral(string value) =>
        "'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";

    private async Task EnsureDiagramCatalogAsync(CancellationToken ct)
    {
        ThrowIfSqlError(await client.ExecuteSqlAsync(
            $"""
            CREATE TABLE IF NOT EXISTS {DiagramTableName} (
                id BIGINT PRIMARY KEY IDENTITY,
                name TEXT NOT NULL,
                diagram_json TEXT NOT NULL,
                created_utc TEXT NOT NULL,
                updated_utc TEXT NOT NULL
            );
            """,
            ct));

        ThrowIfSqlError(await client.ExecuteSqlAsync(
            $"""
            CREATE UNIQUE INDEX IF NOT EXISTS {DiagramNameIndexName}
            ON {DiagramTableName} (name);
            """,
            ct));
    }

    private async Task<bool> DiagramExistsAsync(string name, CancellationToken ct)
    {
        SqlExecutionResult result = await client.ExecuteSqlAsync(
            $"SELECT COUNT(*) FROM {DiagramTableName} WHERE name = {FormatSqlStringLiteral(name)};",
            ct);
        ThrowIfSqlError(result);
        object? value = result.Rows?.FirstOrDefault()?.FirstOrDefault();
        return value is not null && Convert.ToInt64(value, CultureInfo.InvariantCulture) > 0;
    }

    private static void ThrowIfSqlError(SqlExecutionResult result)
    {
        if (!string.IsNullOrWhiteSpace(result.Error))
            throw new InvalidOperationException(result.Error);
    }

    private static string NormalizeDiagramName(string name)
    {
        string trimmed = name.Trim();
        if (string.IsNullOrWhiteSpace(trimmed))
            throw new ArgumentException("Diagram name is required.", nameof(name));
        return trimmed;
    }

    private static string RequireValue(string? value, string name)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new InvalidOperationException($"Diagram operation requires {name}.");
        return value.Trim();
    }

    private static string NormalizeTypeLabel(string value) => SchemaColumnRules.NormalizeType(value);

    private static ForeignKeyOnDeleteAction ParseOnDeleteAction(string? value)
        => ParseReferentialAction(value);

    private static ForeignKeyOnDeleteAction ParseReferentialAction(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return ForeignKeyOnDeleteAction.Restrict;

        string normalized = value.Trim().Replace("_", " ", StringComparison.Ordinal)
            .Replace("-", " ", StringComparison.Ordinal);
        return normalized.ToUpperInvariant() switch
        {
            "RESTRICT" => ForeignKeyOnDeleteAction.Restrict,
            "CASCADE" => ForeignKeyOnDeleteAction.Cascade,
            "NO ACTION" or "NOACTION" => ForeignKeyOnDeleteAction.NoAction,
            "SET NULL" or "SETNULL" => ForeignKeyOnDeleteAction.SetNull,
            "SET DEFAULT" or "SETDEFAULT" => ForeignKeyOnDeleteAction.SetDefault,
            _ => throw new InvalidOperationException(
                $"Unsupported foreign key referential action '{value}'."),
        };
    }

    private static string FormatReferentialAction(
        ForeignKeyOnDeleteAction action) =>
        action switch
        {
            ForeignKeyOnDeleteAction.Restrict => "RESTRICT",
            ForeignKeyOnDeleteAction.Cascade => "CASCADE",
            ForeignKeyOnDeleteAction.NoAction => "NO ACTION",
            ForeignKeyOnDeleteAction.SetNull => "SET NULL",
            ForeignKeyOnDeleteAction.SetDefault => "SET DEFAULT",
            _ => throw new InvalidOperationException(
                $"Unsupported foreign key referential action '{action}'."),
        };

    private static string FormatReferentialAction(
        PrimitiveForeignKeyOnDeleteAction action) =>
        action switch
        {
            PrimitiveForeignKeyOnDeleteAction.Restrict => "RESTRICT",
            PrimitiveForeignKeyOnDeleteAction.Cascade => "CASCADE",
            PrimitiveForeignKeyOnDeleteAction.NoAction => "NO ACTION",
            PrimitiveForeignKeyOnDeleteAction.SetNull => "SET NULL",
            PrimitiveForeignKeyOnDeleteAction.SetDefault => "SET DEFAULT",
            _ => throw new InvalidOperationException(
                $"Unsupported archived foreign key referential action '{action}'."),
        };

    private sealed record ExternalTableRegistration(
        string TableName,
        string Path,
        string SourceTableName,
        long RowCount,
        string? CreatedUtc);
}
