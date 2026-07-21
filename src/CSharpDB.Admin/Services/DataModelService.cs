using System.Globalization;
using System.Text;
using CSharpDB.Admin.Models;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.ImportExport.TableArchives;
using ArchiveColumn = CSharpDB.ImportExport.Models.TableArchiveColumn;
using ArchiveForeignKey = CSharpDB.ImportExport.Models.TableArchiveForeignKey;

namespace CSharpDB.Admin.Services;

public interface IDataModelService
{
    Task<DataModelState> BuildModelAsync(string? seedSourceName = null, int autoLayoutLimit = DataModelGraphBuilder.DefaultAutoLayoutLimit, CancellationToken ct = default);
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

public sealed class DataModelService(ICSharpDbClient client) : IDataModelService, IDataModelDiagramService
{
    private const string DiagramTableName = "__data_model_diagrams";
    private const string DiagramNameIndexName = "idx___data_model_diagrams_name";

    public async Task<DataModelState> BuildModelAsync(
        string? seedSourceName = null,
        int autoLayoutLimit = DataModelGraphBuilder.DefaultAutoLayoutLimit,
        CancellationToken ct = default)
    {
        IReadOnlyList<DataModelSourceMetadata> sources = await LoadSourcesAsync(ct);
        DataModelState state = DataModelGraphBuilder.Build(sources, seedSourceName, autoLayoutLimit);
        state.SchemaSnapshotUtc = DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture);
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
        IReadOnlyList<DataModelSourceMetadata> sources = await LoadSourcesAsync(ct);
        return DataModelGraphBuilder.BuildFromDiagramState(sources, saved);
    }

    public async Task SaveDiagramAsync(string name, DataModelState state, CancellationToken ct = default)
    {
        await EnsureDiagramCatalogAsync(ct);
        string normalized = NormalizeDiagramName(name);
        state.Version = 1;
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
        if (state.Nodes.Count == 0)
            return "-- Add model sources to preview schema DDL.";

        var sb = new StringBuilder();
        sb.AppendLine("-- Preview only. Review before running in a query tab.");
        foreach (DataModelNode node in state.Nodes.OrderBy(static node => node.Kind).ThenBy(static node => node.Name, StringComparer.OrdinalIgnoreCase))
        {
            sb.AppendLine();
            if (node.Kind == DataModelNodeKind.ExternalTable)
            {
                sb.Append("CREATE EXTERNAL TABLE ")
                    .Append(FormatIdentifier(node.Name))
                    .Append(" FROM ")
                    .Append(FormatSqlStringLiteral(node.ArchivePath ?? string.Empty))
                    .AppendLine(";");
                continue;
            }

            sb.Append("CREATE TABLE ").Append(FormatIdentifier(node.Name)).AppendLine(" (");
            for (int i = 0; i < node.Columns.Count; i++)
            {
                DataModelColumn column = node.Columns[i];
                sb.Append("    ")
                    .Append(FormatIdentifier(column.Name))
                    .Append(' ')
                    .Append(column.TypeLabel);

                if (column.IsRowVersion)
                    sb.Append(" ROWVERSION");
                if (column.IsPrimaryKey)
                    sb.Append(" PRIMARY KEY");
                if (column.IsIdentity)
                    sb.Append(" IDENTITY");
                if (!column.Nullable && !column.IsPrimaryKey)
                    sb.Append(" NOT NULL");
                if (!string.IsNullOrWhiteSpace(column.Collation))
                    sb.Append(" COLLATE ").Append(column.Collation);

                if (i < node.Columns.Count - 1)
                    sb.Append(',');
                sb.AppendLine();
            }

            sb.AppendLine(");");
        }

        IReadOnlyList<DataModelRelationship> resolvedRelationships = state.Relationships
            .Where(static relationship => relationship.IsResolved)
            .OrderBy(static relationship => relationship.LeftTable, StringComparer.OrdinalIgnoreCase)
            .ThenBy(static relationship => relationship.LeftColumn, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        if (resolvedRelationships.Count > 0)
        {
            sb.AppendLine();
            sb.AppendLine("-- Relationships");
            foreach (DataModelRelationship relationship in resolvedRelationships)
            {
                sb.Append("-- ")
                    .Append(relationship.LeftTable)
                    .Append('.')
                    .Append(relationship.LeftColumn)
                    .Append(" -> ")
                    .Append(relationship.RightTable)
                    .Append('.')
                    .Append(relationship.RightColumn);
                if (!string.IsNullOrWhiteSpace(relationship.OnDelete))
                    sb.Append(" ON DELETE ").Append(relationship.OnDelete);
                sb.AppendLine();
            }
        }

        return sb.ToString().TrimEnd();
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

        var messages = new List<string>();
        foreach (DataModelPendingOperation operation in state.PendingOperations.ToArray())
        {
            await ApplyOperationAsync(operation, ct);
            ApplyOperationToState(state, operation);
            messages.Add(DescribeOperation(operation));
        }

        state.PendingOperations.Clear();
        state.SchemaSnapshotUtc = DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture);
        if (!string.IsNullOrWhiteSpace(state.DiagramName))
            await SaveDiagramAsync(state.DiagramName, state, ct);

        return new DataModelApplyResult
        {
            Succeeded = true,
            Messages = messages,
        };
    }

    private static void ApplyOperationToState(DataModelState state, DataModelPendingOperation operation)
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
                    addNode.Columns.Add(new DataModelColumn
                    {
                        Name = RequireValue(operation.ColumnName, "column name"),
                        TypeLabel = NormalizeTypeLabel(operation.ColumnType),
                        Nullable = !operation.NotNull,
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

            case DataModelPendingOperationKind.AddForeignKey:
                state.Relationships.RemoveAll(relationship => string.Equals(relationship.Id, operation.Id, StringComparison.Ordinal));
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

    private async Task ApplyOperationAsync(DataModelPendingOperation operation, CancellationToken ct)
    {
        switch (operation.Kind)
        {
            case DataModelPendingOperationKind.CreateTable:
                ThrowIfSqlError(await client.ExecuteSqlAsync(BuildOperationSql(operation), ct));
                break;

            case DataModelPendingOperationKind.DropTable:
                await client.DropTableAsync(operation.TableName, ct);
                break;

            case DataModelPendingOperationKind.RenameTable:
                await client.RenameTableAsync(operation.TableName, RequireValue(operation.NewTableName, "new table name"), ct);
                break;

            case DataModelPendingOperationKind.AddColumn:
                await client.AddColumnAsync(
                    operation.TableName,
                    RequireValue(operation.ColumnName, "column name"),
                    ParseClientDbType(operation.ColumnType),
                    operation.NotNull,
                    ct);
                break;

            case DataModelPendingOperationKind.DropColumn:
                await client.DropColumnAsync(operation.TableName, RequireValue(operation.ColumnName, "column name"), ct);
                break;

            case DataModelPendingOperationKind.RenameColumn:
                await client.RenameColumnAsync(
                    operation.TableName,
                    RequireValue(operation.ColumnName, "column name"),
                    RequireValue(operation.NewColumnName, "new column name"),
                    ct);
                break;

            case DataModelPendingOperationKind.AddForeignKey:
                ForeignKeyMigrationResult migration = await client.MigrateForeignKeysAsync(
                    new ForeignKeyMigrationRequest
                    {
                        ValidateOnly = false,
                        Constraints =
                        [
                            new ForeignKeyMigrationConstraintSpec
                            {
                                TableName = operation.TableName,
                                ColumnName = RequireValue(operation.ColumnName, "column name"),
                                ReferencedTableName = RequireValue(operation.ReferencedTableName, "referenced table"),
                                ReferencedColumnName = operation.ReferencedColumnName,
                                OnDelete = ParseOnDeleteAction(operation.OnDelete),
                            },
                        ],
                    },
                    ct);
                if (!migration.Succeeded)
                    throw new InvalidOperationException($"Foreign key migration failed with {migration.ViolationCount} violation(s).");
                break;

            case DataModelPendingOperationKind.DropForeignKey:
                ThrowIfSqlError(await client.ExecuteSqlAsync(BuildOperationSql(operation), ct));
                break;

            default:
                throw new InvalidOperationException($"Unsupported diagram operation '{operation.Kind}'.");
        }
    }

    private static string BuildOperationSql(DataModelPendingOperation operation) => operation.Kind switch
    {
        DataModelPendingOperationKind.CreateTable => BuildCreateTableSql(operation),
        DataModelPendingOperationKind.DropTable => $"DROP TABLE {FormatIdentifier(operation.TableName)};",
        DataModelPendingOperationKind.RenameTable => $"ALTER TABLE {FormatIdentifier(operation.TableName)} RENAME TO {FormatIdentifier(RequireValue(operation.NewTableName, "new table name"))};",
        DataModelPendingOperationKind.AddColumn => $"ALTER TABLE {FormatIdentifier(operation.TableName)} ADD COLUMN {BuildColumnSql(operation)};",
        DataModelPendingOperationKind.DropColumn => $"ALTER TABLE {FormatIdentifier(operation.TableName)} DROP COLUMN {FormatIdentifier(RequireValue(operation.ColumnName, "column name"))};",
        DataModelPendingOperationKind.RenameColumn => $"ALTER TABLE {FormatIdentifier(operation.TableName)} RENAME COLUMN {FormatIdentifier(RequireValue(operation.ColumnName, "column name"))} TO {FormatIdentifier(RequireValue(operation.NewColumnName, "new column name"))};",
        DataModelPendingOperationKind.AddForeignKey => $"-- Uses validated foreign-key migration: {FormatIdentifier(operation.TableName)}.{FormatIdentifier(RequireValue(operation.ColumnName, "column name"))} -> {FormatIdentifier(RequireValue(operation.ReferencedTableName, "referenced table"))}.{FormatIdentifier(RequireValue(operation.ReferencedColumnName, "referenced column"))}",
        DataModelPendingOperationKind.DropForeignKey => $"ALTER TABLE {FormatIdentifier(operation.TableName)} DROP CONSTRAINT {FormatIdentifier(RequireValue(operation.ConstraintName, "constraint name"))};",
        _ => "-- Unsupported diagram operation.",
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

        var sb = new StringBuilder()
            .Append("CREATE TABLE ")
            .Append(FormatIdentifier(operation.TableName))
            .AppendLine(" (");
        for (int i = 0; i < columns.Count; i++)
        {
            DataModelColumn column = columns[i];
            sb.Append("    ")
                .Append(FormatIdentifier(column.Name))
                .Append(' ')
                .Append(NormalizeTypeLabel(column.TypeLabel));
            if (column.IsRowVersion)
                sb.Append(" ROWVERSION");
            if (column.IsPrimaryKey)
                sb.Append(" PRIMARY KEY");
            if (column.IsIdentity)
                sb.Append(" IDENTITY");
            if (!column.Nullable && !column.IsPrimaryKey)
                sb.Append(" NOT NULL");
            if (!string.IsNullOrWhiteSpace(column.Collation))
                sb.Append(" COLLATE ").Append(column.Collation);
            if (i < columns.Count - 1)
                sb.Append(',');
            sb.AppendLine();
        }

        sb.Append(");");
        return sb.ToString();
    }

    private static string BuildColumnSql(DataModelPendingOperation operation)
    {
        string column = $"{FormatIdentifier(RequireValue(operation.ColumnName, "column name"))} {NormalizeTypeLabel(operation.ColumnType)}";
        return operation.NotNull ? column + " NOT NULL" : column;
    }

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

        foreach (string tableName in tableNames.Where(static name => !IsSystemTableName(name)).OrderBy(static name => name, StringComparer.OrdinalIgnoreCase))
        {
            TableSchema? schema = await client.GetTableSchemaAsync(tableName, ct);
            if (schema is null)
                continue;

            sources.Add(new DataModelSourceMetadata
            {
                TableName = schema.TableName,
                Kind = DataModelNodeKind.Table,
                Columns = schema.Columns.Select(MapColumn).ToArray(),
                ForeignKeys = schema.ForeignKeys.Select(MapForeignKey).ToArray(),
                Indexes = indexes
                    .Where(index => string.Equals(index.TableName, schema.TableName, StringComparison.OrdinalIgnoreCase))
                    .Select(MapIndex)
                    .ToArray(),
                TriggerCount = triggers.Count(trigger => string.Equals(trigger.TableName, schema.TableName, StringComparison.OrdinalIgnoreCase)),
            });
        }

        foreach (ExternalTableRegistration registration in await LoadExternalRegistrationsAsync(ct))
        {
            sources.Add(await LoadExternalSourceAsync(registration, ct));
        }

        return sources;
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
        Name = column.Name,
        TypeLabel = column.Type.ToString().ToUpperInvariant(),
        IsPrimaryKey = column.IsPrimaryKey,
        IsIdentity = column.IsIdentity,
        IsRowVersion = column.IsRowVersion,
        Nullable = column.Nullable,
        Collation = column.Collation,
    };

    private static DataModelColumnMetadata MapArchiveColumn(ArchiveColumn column) => new()
    {
        Name = column.Name,
        TypeLabel = column.Type.ToString().ToUpperInvariant(),
        IsPrimaryKey = column.IsPrimaryKey,
        IsIdentity = column.IsIdentity,
        IsRowVersion = column.IsRowVersion,
        Nullable = column.Nullable,
        Collation = column.Collation,
    };

    private static DataModelForeignKeyMetadata MapForeignKey(ForeignKeyDefinition foreignKey) => new()
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
        OnDelete = foreignKey.OnDelete.ToString().ToUpperInvariant(),
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
        OnDelete = foreignKey.OnDelete.ToString().ToUpperInvariant(),
    };

    private static DataModelIndexMetadata MapIndex(IndexSchema index) => new()
    {
        IndexName = index.IndexName,
        Columns = index.Columns,
        IsUnique = index.IsUnique,
    };

    private static bool IsSystemTableName(string name) =>
        name.StartsWith("_", StringComparison.Ordinal)
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
                id INTEGER PRIMARY KEY IDENTITY,
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

    private static string NormalizeTypeLabel(string value)
    {
        string normalized = value.Trim().ToUpperInvariant();
        return normalized switch
        {
            "INT" => "INTEGER",
            "INTEGER" or "REAL" or "TEXT" or "BLOB" => normalized,
            _ => "TEXT",
        };
    }

    private static DbType ParseClientDbType(string value) => NormalizeTypeLabel(value) switch
    {
        "INTEGER" => DbType.Integer,
        "REAL" => DbType.Real,
        "BLOB" => DbType.Blob,
        _ => DbType.Text,
    };

    private static ForeignKeyOnDeleteAction ParseOnDeleteAction(string? value)
    {
        return string.Equals(value, "CASCADE", StringComparison.OrdinalIgnoreCase)
            ? ForeignKeyOnDeleteAction.Cascade
            : ForeignKeyOnDeleteAction.Restrict;
    }

    private sealed record ExternalTableRegistration(
        string TableName,
        string Path,
        string SourceTableName,
        long RowCount,
        string? CreatedUtc);
}
