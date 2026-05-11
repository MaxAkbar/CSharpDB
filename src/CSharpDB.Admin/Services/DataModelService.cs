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
    Task<IReadOnlyList<SavedQueryDefinition>> GetSavedLayoutsAsync(CancellationToken ct = default);
    Task<DataModelState?> LoadLayoutAsync(string fullName, CancellationToken ct = default);
    Task SaveLayoutAsync(string name, DataModelState state, CancellationToken ct = default);
    Task DeleteLayoutAsync(string fullName, CancellationToken ct = default);
    string BuildPreviewSql(DataModelState state);
    QueryDesignerState ToQueryDesignerState(DataModelState state);
}

public sealed class DataModelService(ICSharpDbClient client) : IDataModelService
{
    public const string LayoutPrefix = "__data_model_layout:";

    public async Task<DataModelState> BuildModelAsync(
        string? seedSourceName = null,
        int autoLayoutLimit = DataModelGraphBuilder.DefaultAutoLayoutLimit,
        CancellationToken ct = default)
    {
        IReadOnlyList<DataModelSourceMetadata> sources = await LoadSourcesAsync(ct);
        return DataModelGraphBuilder.Build(sources, seedSourceName, autoLayoutLimit);
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

    public async Task<IReadOnlyList<SavedQueryDefinition>> GetSavedLayoutsAsync(CancellationToken ct = default)
    {
        IReadOnlyList<SavedQueryDefinition> saved = await client.GetSavedQueriesAsync(ct);
        return saved
            .Where(static query => query.Name.StartsWith(LayoutPrefix, StringComparison.Ordinal))
            .OrderBy(static query => query.Name, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public async Task<DataModelState?> LoadLayoutAsync(string fullName, CancellationToken ct = default)
    {
        SavedQueryDefinition? saved = await client.GetSavedQueryAsync(fullName, ct);
        if (saved is null)
            return null;

        DataModelState? state = DataModelGraphBuilder.DeserializeState(saved.SqlText);
        if (state is not null)
            state.SavedLayoutName = fullName.StartsWith(LayoutPrefix, StringComparison.Ordinal)
                ? fullName[LayoutPrefix.Length..]
                : fullName;

        return state;
    }

    public async Task SaveLayoutAsync(string name, DataModelState state, CancellationToken ct = default)
    {
        string trimmed = name.Trim();
        if (string.IsNullOrWhiteSpace(trimmed))
            throw new ArgumentException("Layout name is required.", nameof(name));

        state.SavedLayoutName = trimmed;
        await client.UpsertSavedQueryAsync(LayoutPrefix + trimmed, DataModelGraphBuilder.SerializeState(state), ct);
    }

    public Task DeleteLayoutAsync(string fullName, CancellationToken ct = default) =>
        client.DeleteSavedQueryAsync(fullName, ct);

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
        Nullable = column.Nullable,
        Collation = column.Collation,
    };

    private static DataModelColumnMetadata MapArchiveColumn(ArchiveColumn column) => new()
    {
        Name = column.Name,
        TypeLabel = column.Type.ToString().ToUpperInvariant(),
        IsPrimaryKey = column.IsPrimaryKey,
        IsIdentity = column.IsIdentity,
        Nullable = column.Nullable,
        Collation = column.Collation,
    };

    private static DataModelForeignKeyMetadata MapForeignKey(ForeignKeyDefinition foreignKey) => new()
    {
        ConstraintName = foreignKey.ConstraintName,
        ColumnName = foreignKey.ColumnName,
        ReferencedTableName = foreignKey.ReferencedTableName,
        ReferencedColumnName = foreignKey.ReferencedColumnName,
        OnDelete = foreignKey.OnDelete.ToString().ToUpperInvariant(),
    };

    private static DataModelForeignKeyMetadata MapArchiveForeignKey(ArchiveForeignKey foreignKey) => new()
    {
        ConstraintName = foreignKey.ConstraintName,
        ColumnName = foreignKey.ColumnName,
        ReferencedTableName = foreignKey.ReferencedTableName,
        ReferencedColumnName = foreignKey.ReferencedColumnName,
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

    private sealed record ExternalTableRegistration(
        string TableName,
        string Path,
        string SourceTableName,
        long RowCount,
        string? CreatedUtc);
}
