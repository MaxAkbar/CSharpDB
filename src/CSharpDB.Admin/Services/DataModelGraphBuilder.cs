using System.Text.Json;
using CSharpDB.Admin.Models;

namespace CSharpDB.Admin.Services;

public static class DataModelGraphBuilder
{
    public const int DefaultAutoLayoutLimit = 35;

    public static DataModelState Build(
        IReadOnlyList<DataModelSourceMetadata> sources,
        string? seedSourceName = null,
        int autoLayoutLimit = DefaultAutoLayoutLimit)
    {
        var orderedSources = sources
            .OrderBy(static source => source.Kind)
            .ThenBy(static source => source.TableName, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var state = new DataModelState();
        IReadOnlySet<string> selectedNames = SelectSourceNames(orderedSources, seedSourceName, autoLayoutLimit, state.Warnings);
        int index = 0;
        foreach (DataModelSourceMetadata source in orderedSources.Where(source => selectedNames.Contains(source.TableName)))
        {
            state.Nodes.Add(CreateNode(source, index++));
        }

        AddRelationships(orderedSources, state, selectedNames);
        return state;
    }

    public static DataModelState BuildFromDiagramState(
        IReadOnlyList<DataModelSourceMetadata> sources,
        DataModelState savedState)
    {
        var orderedSources = sources
            .OrderBy(static source => source.Kind)
            .ThenBy(static source => source.TableName, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var state = new DataModelState
        {
            Version = savedState.Version <= 0 ? 1 : savedState.Version,
            DiagramName = savedState.DiagramName ?? savedState.SavedLayoutName,
            SavedLayoutName = savedState.SavedLayoutName,
            SchemaSnapshotUtc = savedState.SchemaSnapshotUtc,
            ViewportX = savedState.ViewportX,
            ViewportY = savedState.ViewportY,
            Scale = savedState.Scale <= 0 ? 1 : savedState.Scale,
            PendingOperations = savedState.PendingOperations.Select(CloneOperation).ToList(),
        };

        var selectedNames = new HashSet<string>(
            savedState.Nodes.Select(static node => node.Name),
            StringComparer.OrdinalIgnoreCase);

        int index = 0;
        foreach (DataModelNode savedNode in savedState.Nodes)
        {
            DataModelSourceMetadata? source = FindSource(orderedSources, savedNode.Name);
            if (source is null)
            {
                if (savedNode.IsDraft || state.PendingOperations.Any(operation =>
                        operation.Kind == DataModelPendingOperationKind.CreateTable &&
                        string.Equals(operation.TableName, savedNode.Name, StringComparison.OrdinalIgnoreCase)))
                {
                    state.Nodes.Add(CloneNode(savedNode));
                }
                else
                {
                    state.Warnings.Add($"Diagram source '{savedNode.Name}' no longer exists in the database.");
                }

                continue;
            }

            DataModelNode node = CreateNode(source, index++);
            node.X = savedNode.X;
            node.Y = savedNode.Y;
            node.IsCollapsed = savedNode.IsCollapsed;
            state.Nodes.Add(node);
        }

        AddRelationships(orderedSources, state, selectedNames);
        foreach (DataModelRelationship relationship in savedState.Relationships.Where(static relationship => relationship.Kind == DataModelRelationshipKind.Draft))
        {
            if (!state.Relationships.Any(existing => string.Equals(existing.Id, relationship.Id, StringComparison.Ordinal)))
                state.Relationships.Add(CloneRelationship(relationship));
        }

        foreach (string warning in savedState.Warnings)
        {
            if (!state.Warnings.Contains(warning, StringComparer.OrdinalIgnoreCase))
                state.Warnings.Add(warning);
        }

        return state;
    }

    public static QueryDesignerState ToQueryDesignerState(DataModelState state)
    {
        var selectedNames = new HashSet<string>(
            state.Nodes.Select(static node => node.Name),
            StringComparer.OrdinalIgnoreCase);

        var designer = new QueryDesignerState
        {
            Tables = state.Nodes
                .OrderBy(static node => node.Y)
                .ThenBy(static node => node.X)
                .Select(static node => new DesignerTableNode
                {
                    TableName = node.Name,
                    X = node.X,
                    Y = node.Y,
                    Columns = node.Columns.Select(static column => new DesignerColumn
                    {
                        Name = column.Name,
                        TypeLabel = column.TypeLabel,
                        IsPrimaryKey = column.IsPrimaryKey,
                        IsSelected = true,
                    }).ToList(),
                })
                .ToList(),
            Joins = state.Relationships
                .Where(relationship =>
                    relationship.IsResolved &&
                    selectedNames.Contains(relationship.LeftTable) &&
                    selectedNames.Contains(relationship.RightTable) &&
                    !string.IsNullOrWhiteSpace(relationship.LeftColumn) &&
                    !string.IsNullOrWhiteSpace(relationship.RightColumn))
                .Select(static relationship => new DesignerJoin
                {
                    LeftTable = relationship.RightTable,
                    LeftColumn = relationship.RightColumn,
                    RightTable = relationship.LeftTable,
                    RightColumn = relationship.LeftColumn,
                    JoinType = DesignerJoinType.Inner,
                })
                .ToList(),
        };

        foreach (DesignerTableNode table in designer.Tables)
        {
            foreach (DesignerColumn column in table.Columns)
            {
                designer.GridRows.Add(new DesignerGridRow
                {
                    TableName = table.TableName,
                    ColumnExpr = column.Name,
                    Output = true,
                });
            }
        }

        return designer;
    }

    public static string SerializeState(DataModelState state) =>
        JsonSerializer.Serialize(state);

    public static DataModelState? DeserializeState(string json) =>
        JsonSerializer.Deserialize<DataModelState>(json);

    private static IReadOnlySet<string> SelectSourceNames(
        IReadOnlyList<DataModelSourceMetadata> sources,
        string? seedSourceName,
        int autoLayoutLimit,
        List<string> warnings)
    {
        if (string.IsNullOrWhiteSpace(seedSourceName))
        {
            if (sources.Count <= autoLayoutLimit)
                return sources.Select(static source => source.TableName).ToHashSet(StringComparer.OrdinalIgnoreCase);

            warnings.Add($"Database has {sources.Count} model sources. Add individual tables or use Load All to render the full model.");
            return new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }

        DataModelSourceMetadata? seed = FindSource(sources, seedSourceName);
        if (seed is null)
        {
            warnings.Add($"Source '{seedSourceName}' was not found.");
            return new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }

        var selected = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { seed.TableName };
        foreach (DataModelForeignKeyMetadata foreignKey in seed.ForeignKeys)
        {
            DataModelSourceMetadata? referenced = FindSource(sources, foreignKey.ReferencedTableName);
            if (referenced is not null)
                selected.Add(referenced.TableName);
        }

        foreach (DataModelSourceMetadata source in sources)
        {
            if (source.ForeignKeys.Any(foreignKey => SourceMatches(seed, foreignKey.ReferencedTableName)))
                selected.Add(source.TableName);
        }

        return selected;
    }

    private static DataModelNode CreateNode(DataModelSourceMetadata source, int index)
    {
        int column = index % 4;
        int row = index / 4;
        var foreignKeyColumns = source.ForeignKeys
            .Select(static foreignKey => foreignKey.ColumnName)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        var indexedColumns = source.Indexes
            .SelectMany(static indexMetadata => indexMetadata.Columns)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        return new DataModelNode
        {
            Name = source.TableName,
            Kind = source.Kind,
            X = 20 + column * 250,
            Y = 20 + row * 260,
            SourceTableName = source.SourceTableName,
            ArchivePath = source.ArchivePath,
            ArchiveCreatedUtc = source.ArchiveCreatedUtc,
            RowCount = source.RowCount,
            IndexCount = source.Indexes.Count,
            TriggerCount = source.TriggerCount,
            Warnings = source.Warnings.ToList(),
            Columns = source.Columns.Select(columnMetadata => new DataModelColumn
            {
                Name = columnMetadata.Name,
                TypeLabel = columnMetadata.TypeLabel,
                IsPrimaryKey = columnMetadata.IsPrimaryKey,
                IsIdentity = columnMetadata.IsIdentity,
                Nullable = columnMetadata.Nullable,
                Collation = columnMetadata.Collation,
                IsForeignKey = foreignKeyColumns.Contains(columnMetadata.Name),
                IsIndexed = indexedColumns.Contains(columnMetadata.Name),
            }).ToList(),
        };
    }

    private static void AddRelationships(
        IReadOnlyList<DataModelSourceMetadata> sources,
        DataModelState state,
        IReadOnlySet<string> selectedNames)
    {
        var selectedSources = sources
            .Where(source => selectedNames.Contains(source.TableName))
            .ToArray();

        foreach (DataModelSourceMetadata source in selectedSources)
        {
            foreach (DataModelForeignKeyMetadata foreignKey in source.ForeignKeys)
            {
                DataModelSourceMetadata? referenced = FindSource(selectedSources, foreignKey.ReferencedTableName);
                bool resolved = referenced is not null;
                string rightTable = referenced?.TableName ?? foreignKey.ReferencedTableName;
                string id = $"{source.TableName}:{foreignKey.ColumnName}->{rightTable}:{foreignKey.ReferencedColumnName}";
                var relationship = new DataModelRelationship
                {
                    Id = id,
                    LeftTable = source.TableName,
                    LeftColumn = foreignKey.ColumnName,
                    RightTable = rightTable,
                    RightColumn = foreignKey.ReferencedColumnName,
                    Kind = source.Kind == DataModelNodeKind.ExternalTable
                        ? DataModelRelationshipKind.ExternalArchiveForeignKey
                        : DataModelRelationshipKind.PhysicalForeignKey,
                    IsResolved = resolved,
                    ConstraintName = foreignKey.ConstraintName,
                    OnDelete = foreignKey.OnDelete,
                    Warning = resolved
                        ? null
                        : $"Relationship target '{foreignKey.ReferencedTableName}' is not on the canvas.",
                };

                state.Relationships.Add(relationship);
                if (!resolved && relationship.Warning is not null)
                    state.Warnings.Add($"{source.TableName}.{foreignKey.ColumnName}: {relationship.Warning}");
            }
        }
    }

    private static DataModelSourceMetadata? FindSource(
        IEnumerable<DataModelSourceMetadata> sources,
        string sourceName)
    {
        return sources.FirstOrDefault(source => SourceMatches(source, sourceName));
    }

    private static bool SourceMatches(DataModelSourceMetadata source, string sourceName)
    {
        return string.Equals(source.TableName, sourceName, StringComparison.OrdinalIgnoreCase)
               || (!string.IsNullOrWhiteSpace(source.SourceTableName)
                   && string.Equals(source.SourceTableName, sourceName, StringComparison.OrdinalIgnoreCase));
    }

    private static DataModelNode CloneNode(DataModelNode node) => new()
    {
        Name = node.Name,
        Kind = node.Kind,
        X = node.X,
        Y = node.Y,
        IsCollapsed = node.IsCollapsed,
        IsDraft = node.IsDraft,
        SourceTableName = node.SourceTableName,
        ArchivePath = node.ArchivePath,
        ArchiveCreatedUtc = node.ArchiveCreatedUtc,
        RowCount = node.RowCount,
        IndexCount = node.IndexCount,
        TriggerCount = node.TriggerCount,
        Warnings = node.Warnings.ToList(),
        Columns = node.Columns.Select(static column => new DataModelColumn
        {
            Name = column.Name,
            TypeLabel = column.TypeLabel,
            IsPrimaryKey = column.IsPrimaryKey,
            IsIdentity = column.IsIdentity,
            Nullable = column.Nullable,
            Collation = column.Collation,
            IsForeignKey = column.IsForeignKey,
            IsIndexed = column.IsIndexed,
        }).ToList(),
    };

    private static DataModelRelationship CloneRelationship(DataModelRelationship relationship) => new()
    {
        Id = relationship.Id,
        LeftTable = relationship.LeftTable,
        LeftColumn = relationship.LeftColumn,
        RightTable = relationship.RightTable,
        RightColumn = relationship.RightColumn,
        Kind = relationship.Kind,
        IsResolved = relationship.IsResolved,
        ConstraintName = relationship.ConstraintName,
        OnDelete = relationship.OnDelete,
        Warning = relationship.Warning,
    };

    private static DataModelPendingOperation CloneOperation(DataModelPendingOperation operation) => new()
    {
        Id = operation.Id,
        Kind = operation.Kind,
        TableName = operation.TableName,
        NewTableName = operation.NewTableName,
        ColumnName = operation.ColumnName,
        NewColumnName = operation.NewColumnName,
        ColumnType = operation.ColumnType,
        NotNull = operation.NotNull,
        Columns = operation.Columns.Select(static column => new DataModelColumn
        {
            Name = column.Name,
            TypeLabel = column.TypeLabel,
            IsPrimaryKey = column.IsPrimaryKey,
            IsIdentity = column.IsIdentity,
            Nullable = column.Nullable,
            Collation = column.Collation,
            IsForeignKey = column.IsForeignKey,
            IsIndexed = column.IsIndexed,
        }).ToList(),
        ReferencedTableName = operation.ReferencedTableName,
        ReferencedColumnName = operation.ReferencedColumnName,
        ConstraintName = operation.ConstraintName,
        OnDelete = operation.OnDelete,
        Description = operation.Description,
    };
}
