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

        var warnings = new List<string>();
        IReadOnlySet<string> selectedNames = SelectSourceNames(orderedSources, seedSourceName, autoLayoutLimit, warnings);
        DataModelState state = BuildSelection(orderedSources, selectedNames, DataModelSelectionMode.Exact);
        state.Warnings.AddRange(warnings);
        return state;
    }

    public static DataModelState BuildSelection(
        IReadOnlyList<DataModelSourceMetadata> sources,
        IReadOnlyCollection<string> sourceNames,
        DataModelSelectionMode selectionMode = DataModelSelectionMode.Exact)
    {
        var orderedSources = sources
            .OrderBy(static source => source.Kind)
            .ThenBy(static source => source.TableName, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        var selectedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (string requestedName in sourceNames.Where(static name => !string.IsNullOrWhiteSpace(name)))
        {
            DataModelSourceMetadata? source = FindSource(orderedSources, requestedName);
            if (source is not null)
                selectedNames.Add(source.TableName);
        }

        if (selectionMode == DataModelSelectionMode.IncludeDirectlyRelated)
        {
            foreach (DataModelSourceMetadata selected in orderedSources.Where(source => selectedNames.Contains(source.TableName)).ToArray())
            {
                foreach (DataModelForeignKeyMetadata foreignKey in selected.ForeignKeys)
                {
                    DataModelSourceMetadata? parent = FindSource(orderedSources, foreignKey.ReferencedTableName);
                    if (parent is not null)
                        selectedNames.Add(parent.TableName);
                }

                foreach (DataModelSourceMetadata candidate in orderedSources)
                {
                    if (candidate.ForeignKeys.Any(foreignKey => SourceMatches(selected, foreignKey.ReferencedTableName)))
                        selectedNames.Add(candidate.TableName);
                }
            }
        }

        var state = new DataModelState();
        int index = 0;
        foreach (DataModelSourceMetadata source in orderedSources.Where(source => selectedNames.Contains(source.TableName)))
            state.Nodes.Add(CreateNode(source, index++));

        AddRelationships(orderedSources, state, selectedNames);
        state.SchemaContext = BuildSchemaContext(orderedSources);
        return state;
    }

    private static DataModelState BuildSchemaContext(IReadOnlyList<DataModelSourceMetadata> sources)
    {
        var context = new DataModelState { Nodes = sources.Select((source, index) => CreateNode(source, index)).ToList() };
        AddRelationships(sources, context, sources.Select(source => source.TableName).ToHashSet(StringComparer.OrdinalIgnoreCase));
        return context;
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
            SchemaContext = BuildSchemaContext(orderedSources),
            Version = 5,
            SchemaFingerprint = savedState.SchemaFingerprint,
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
            DataModelSourceMetadata? source = savedNode.SchemaId != Guid.Empty
                ? orderedSources.FirstOrDefault(item => item.SchemaId == savedNode.SchemaId)
                : FindSource(orderedSources, savedNode.Name);
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
            node.GroupId = savedNode.GroupId;
            node.DetailLevel = savedState.Version <= 1
                ? (savedNode.IsCollapsed ? DataModelNodeDetailLevel.Collapsed : DataModelNodeDetailLevel.All)
                : savedNode.IsCollapsed
                    ? DataModelNodeDetailLevel.Collapsed
                    : savedNode.DetailLevel;
            node.IsCollapsed = node.DetailLevel == DataModelNodeDetailLevel.Collapsed;
            state.Nodes.Add(node);
        }

        AddRelationships(orderedSources, state, state.Nodes.Select(node => node.Name).ToHashSet(StringComparer.OrdinalIgnoreCase));
        foreach (DataModelRelationship relationship in savedState.Relationships.Where(static relationship => relationship.Kind == DataModelRelationshipKind.Draft))
        {
            if (!state.Relationships.Any(existing => string.Equals(existing.Id, relationship.Id, StringComparison.Ordinal)))
                state.Relationships.Add(CloneRelationship(relationship));
        }

        if (savedState.Version >= 3)
            state.Warnings.AddRange(PreserveConnectorLayouts(savedState.Relationships, state.Relationships));
        else
            foreach (DataModelRelationship relationship in state.Relationships)
                relationship.ConnectorLayout = null;

        foreach (string warning in savedState.Warnings)
        {
            if (!state.Warnings.Contains(warning, StringComparer.OrdinalIgnoreCase))
                state.Warnings.Add(warning);
        }

        if (savedState.Version >= 4) DataModelGroups.Preserve(savedState, state);
        UpdateDraftCardinalities(state);
        return state;
    }

    public static void UpdateDraftCardinalities(DataModelState state)
    {
        foreach (var relationship in state.Relationships.Where(relationship => relationship.Kind == DataModelRelationshipKind.Draft))
        {
            var child = state.Nodes.FirstOrDefault(node => node.Name.Equals(relationship.LeftTable, StringComparison.OrdinalIgnoreCase));
            if (child is null) continue;
            var columns = relationship.EffectiveColumnPairs.Select(pair => pair.ChildColumn).ToArray();
            bool unique = child.Keys.Any(key => SameColumns(key.Columns, columns)) || child.Indexes.Any(index => index.IsUnique && SameColumns(index.Columns, columns)) ||
                SameColumns(child.Columns.Where(column => column.IsPrimaryKey).Select(column => column.Name).ToArray(), columns) ||
                columns.Length == 1 && child.Columns.Any(column => column.Name.Equals(columns[0], StringComparison.OrdinalIgnoreCase) && column.IsUnique);
            bool optional = child.Columns.Any(column => columns.Contains(column.Name, StringComparer.OrdinalIgnoreCase) && column.Nullable && !column.IsPrimaryKey);
            relationship.ChildColumnIsUnique = unique;
            relationship.ReferencedEndCardinality = optional ? DataModelCardinality.ZeroOrOne : DataModelCardinality.One;
            relationship.ReferencingEndCardinality = unique ? DataModelCardinality.ZeroOrOne : DataModelCardinality.ZeroOrMany;
        }
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
                .SelectMany(static relationship => relationship.EffectiveColumnPairs.Select(pair => new DesignerJoin
                {
                    LeftTable = relationship.RightTable,
                    LeftColumn = pair.ParentColumn,
                    RightTable = relationship.LeftTable,
                    RightColumn = pair.ChildColumn,
                    JoinType = DesignerJoinType.Inner,
                }))
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

    public static string SerializeState(DataModelState state)
    {
        state.Version = 5;
        DataModelGroups.Normalize(state);
        NormalizeConnectorLayouts(state);
        foreach (DataModelNode node in state.Nodes)
        {
            if (node.IsCollapsed)
                node.DetailLevel = DataModelNodeDetailLevel.Collapsed;
            node.IsCollapsed = node.DetailLevel == DataModelNodeDetailLevel.Collapsed;
        }
        return JsonSerializer.Serialize(state);
    }

    public static DataModelState? DeserializeState(string json)
    {
        DataModelState? state = JsonSerializer.Deserialize<DataModelState>(json);
        if (state is null)
            return null;

        if (state.Version <= 1)
        {
            foreach (DataModelNode node in state.Nodes)
            {
                node.DetailLevel = node.IsCollapsed
                    ? DataModelNodeDetailLevel.Collapsed
                    : DataModelNodeDetailLevel.All;
            }
        }

        if (state.Version <= 2)
            foreach (DataModelRelationship relationship in state.Relationships)
                relationship.ConnectorLayout = null;

        if (state.Version <= 3)
        {
            state.Groups = [];
            foreach (var node in state.Nodes) node.GroupId = null;
        }
        state.Version = 5;
        DataModelGroups.Normalize(state);
        NormalizeConnectorLayouts(state);
        foreach (DataModelNode node in state.Nodes)
            node.IsCollapsed = node.DetailLevel == DataModelNodeDetailLevel.Collapsed;
        return state;
    }

    /// <summary>
    /// Restores visual routes after live metadata replaces relationship instances. Matches must be
    /// one-to-one: an ambiguous identity never silently assigns a route to a different foreign key.
    /// </summary>
    public static IReadOnlyList<string> PreserveConnectorLayouts(
        IEnumerable<DataModelRelationship> previousRelationships,
        IEnumerable<DataModelRelationship> targetRelationships)
    {
        DataModelRelationship[] previous = previousRelationships
            .Where(static relationship => relationship.ConnectorLayout is not null)
            .Select(CloneRelationship)
            .ToArray();
        DataModelRelationship[] targets = targetRelationships.ToArray();
        var warnings = new List<string>();
        var assignments = new List<(DataModelRelationship Previous, DataModelRelationship Target)>();
        var ambiguousTargets = new HashSet<DataModelRelationship>();

        foreach (DataModelRelationship relationship in previous)
        {
            DataModelRelationship[] matches = relationship.SchemaId != Guid.Empty
                ? targets.Where(target => target.SchemaId == relationship.SchemaId).ToArray()
                : string.IsNullOrWhiteSpace(relationship.Id)
                ? []
                : targets.Where(target => string.Equals(target.Id, relationship.Id, StringComparison.Ordinal)).ToArray();
            if (matches.Length == 0 && !string.IsNullOrWhiteSpace(relationship.ConstraintName))
                matches = targets.Where(target =>
                    SameName(target.LeftTable, relationship.LeftTable) &&
                    SameName(target.ConstraintName, relationship.ConstraintName)).ToArray();
            if (matches.Length == 0)
                matches = targets.Where(target =>
                    SameName(target.LeftTable, relationship.LeftTable) &&
                    SameName(target.LeftColumn, relationship.LeftColumn) &&
                    SameName(target.RightTable, relationship.RightTable) &&
                    SameName(target.RightColumn, relationship.RightColumn)).ToArray();

            if (matches.Length == 1)
                assignments.Add((relationship, matches[0]));
            else
            {
                foreach (DataModelRelationship target in matches)
                {
                    target.ConnectorLayout = null;
                    ambiguousTargets.Add(target);
                }
                warnings.Add(ConnectorRestoreWarning(relationship, matches.Length > 1));
            }
        }

        foreach (var group in assignments.GroupBy(static assignment => assignment.Target))
        {
            if (group.Count() == 1 && !ambiguousTargets.Contains(group.Key))
                group.Key.ConnectorLayout = CloneConnectorLayout(group.First().Previous.ConnectorLayout);
            else
            {
                group.Key.ConnectorLayout = null;
                foreach (var assignment in group)
                    warnings.Add(ConnectorRestoreWarning(assignment.Previous, ambiguous: true));
            }
        }

        return warnings.Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
    }

    public static DataModelConnectorLayout? CloneConnectorLayout(DataModelConnectorLayout? layout) =>
        layout is null || !IsValidConnectorLayout(layout) ? null : new DataModelConnectorLayout
        {
            ParentSide = layout.ParentSide,
            ChildSide = layout.ChildSide,
            Waypoints = layout.Waypoints.Select(static waypoint => new DataModelConnectorWaypoint
            {
                Id = waypoint.Id,
                X = waypoint.X,
                Y = waypoint.Y,
            }).ToList(),
        };

    public static bool IsValidConnectorLayout(DataModelConnectorLayout? layout)
    {
        if (layout is null)
            return true;
        if (!Enum.IsDefined(layout.ParentSide) || !Enum.IsDefined(layout.ChildSide) ||
            layout.Waypoints is null || layout.Waypoints.Count is < 1 or > 128)
            return false;

        var ids = new HashSet<string>(StringComparer.Ordinal);
        return layout.Waypoints.All(waypoint => waypoint is not null &&
            !string.IsNullOrWhiteSpace(waypoint.Id) && ids.Add(waypoint.Id) &&
            double.IsFinite(waypoint.X) && double.IsFinite(waypoint.Y) && waypoint.X >= 4 && waypoint.Y >= 4);
    }

    private static void NormalizeConnectorLayouts(DataModelState state)
    {
        foreach (DataModelRelationship relationship in state.Relationships.Where(static relationship =>
                     !IsValidConnectorLayout(relationship.ConnectorLayout)))
        {
            relationship.ConnectorLayout = null;
            string warning = $"Custom connector route for '{relationship.LeftTable}.{relationship.LeftColumn}' contains invalid layout data; automatic routing is used.";
            if (!state.Warnings.Contains(warning, StringComparer.OrdinalIgnoreCase))
                state.Warnings.Add(warning);
        }
    }

    private static bool SameName(string? left, string? right) =>
        string.Equals(left, right, StringComparison.OrdinalIgnoreCase);

    private static string ConnectorRestoreWarning(DataModelRelationship relationship, bool ambiguous) =>
        $"Custom connector route for '{relationship.LeftTable}.{relationship.LeftColumn} → {relationship.RightTable}.{relationship.RightColumn}' " +
        (ambiguous ? "could not be restored because the relationship match is ambiguous; automatic routing is used."
            : "could not be restored because the relationship no longer exists; automatic routing is used.");

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
            .SelectMany(static foreignKey => foreignKey.ColumnNames.Count > 0 ? foreignKey.ColumnNames : [foreignKey.ColumnName])
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        var indexedColumns = source.Indexes
            .SelectMany(static indexMetadata => indexMetadata.Columns)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        var uniqueColumns = source.Indexes
            .Where(static indexMetadata => indexMetadata.IsUnique && indexMetadata.Columns.Count == 1)
            .Select(static indexMetadata => indexMetadata.Columns[0])
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        uniqueColumns.UnionWith(source.Keys.Where(key => key.Columns.Count == 1).Select(key => key.Columns[0]));
        if (source.Keys.Count == 0 && source.Columns.Count(column => column.IsPrimaryKey) == 1)
            uniqueColumns.UnionWith(source.Columns.Where(column => column.IsPrimaryKey).Select(column => column.Name));
        var primaryColumns = source.Keys.Where(key => key.Kind == CSharpDB.Client.Models.KeyConstraintKind.PrimaryKey)
            .SelectMany(key => key.Columns).ToHashSet(StringComparer.OrdinalIgnoreCase);
        return new DataModelNode
        {
            SchemaId = source.SchemaId,
            Keys = source.Keys.ToList(),
            Checks = source.Checks.ToList(),
            Indexes = source.Indexes.ToList(),
            Dependencies = source.Dependencies.ToList(),
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
                SchemaId = columnMetadata.SchemaId,
                Name = columnMetadata.Name,
                TypeLabel = columnMetadata.TypeLabel,
                IsPrimaryKey = columnMetadata.IsPrimaryKey || primaryColumns.Contains(columnMetadata.Name),
                IsIdentity = columnMetadata.IsIdentity,
                IsRowVersion = columnMetadata.IsRowVersion,
                Nullable = columnMetadata.Nullable && !columnMetadata.IsPrimaryKey && !primaryColumns.Contains(columnMetadata.Name),
                Collation = columnMetadata.Collation,
                DefaultSql = columnMetadata.DefaultSql,
                IsForeignKey = foreignKeyColumns.Contains(columnMetadata.Name),
                IsIndexed = indexedColumns.Contains(columnMetadata.Name),
                IsUnique = uniqueColumns.Contains(columnMetadata.Name),
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
                var childColumns = foreignKey.ColumnNames.Count > 0 ? foreignKey.ColumnNames : [foreignKey.ColumnName];
                var parentColumns = foreignKey.ReferencedColumnNames.Count > 0 ? foreignKey.ReferencedColumnNames : [foreignKey.ReferencedColumnName];
                bool childColumnIsUnique = source.Keys.Any(key => SameColumns(key.Columns, childColumns)) || source.Indexes.Any(index =>
                    index.IsUnique && SameColumns(index.Columns, childColumns)) ||
                    (source.Keys.Count == 0 && SameColumns(source.Columns.Where(column => column.IsPrimaryKey).Select(column => column.Name).ToArray(), childColumns));
                bool optional = source.Columns.Any(column => childColumns.Contains(column.Name, StringComparer.OrdinalIgnoreCase) && column.Nullable && !column.IsPrimaryKey &&
                    !source.Keys.Any(key => key.Kind == CSharpDB.Client.Models.KeyConstraintKind.PrimaryKey && key.Columns.Contains(column.Name, StringComparer.OrdinalIgnoreCase)));
                string rightTable = referenced?.TableName ?? foreignKey.ReferencedTableName;
                string id = foreignKey.SchemaId != Guid.Empty ? foreignKey.SchemaId.ToString("N") :
                    $"{source.TableName}:{foreignKey.ConstraintName}:{string.Join(',', childColumns)}->{rightTable}:{string.Join(',', parentColumns)}";
                var relationship = new DataModelRelationship
                {
                    Id = id,
                    SchemaId = foreignKey.SchemaId,
                    ColumnPairs = childColumns.Zip(parentColumns, (child, parent) => new DataModelColumnPair(child, parent)).ToList(),
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
                    OnUpdate = foreignKey.OnUpdate,
                    Warning = resolved
                        ? null
                        : $"Relationship target '{foreignKey.ReferencedTableName}' is not on the canvas.",
                    ReferencedEndCardinality = optional
                        ? DataModelCardinality.ZeroOrOne
                        : DataModelCardinality.One,
                    ReferencingEndCardinality = childColumnIsUnique
                        ? DataModelCardinality.ZeroOrOne
                        : DataModelCardinality.ZeroOrMany,
                    ChildColumnIsUnique = childColumnIsUnique,
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

    private static bool SameColumns(IReadOnlyList<string> left, IReadOnlyList<string> right) =>
        left.Count > 0 && left.Count == right.Count && left.ToHashSet(StringComparer.OrdinalIgnoreCase).SetEquals(right);

    private static bool SourceMatches(DataModelSourceMetadata source, string sourceName)
    {
        return string.Equals(source.TableName, sourceName, StringComparison.OrdinalIgnoreCase)
               || (!string.IsNullOrWhiteSpace(source.SourceTableName)
                   && string.Equals(source.SourceTableName, sourceName, StringComparison.OrdinalIgnoreCase));
    }

    private static DataModelNode CloneNode(DataModelNode node) => new()
    {
        SchemaId = node.SchemaId,
        Keys = node.Keys.ToList(), Checks = node.Checks.ToList(), Indexes = node.Indexes.ToList(), Dependencies = node.Dependencies.ToList(),
        Name = node.Name,
        GroupId = node.GroupId,
        Kind = node.Kind,
        X = node.X,
        Y = node.Y,
        IsCollapsed = node.IsCollapsed,
        DetailLevel = node.DetailLevel,
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
            Checks = column.Checks.ToList(),
            SchemaId = column.SchemaId,
            Name = column.Name,
            TypeLabel = column.TypeLabel,
            IsPrimaryKey = column.IsPrimaryKey,
            IsIdentity = column.IsIdentity,
            IsRowVersion = column.IsRowVersion,
            Nullable = column.Nullable,
            Collation = column.Collation,
            DefaultSql = column.DefaultSql,
            IsForeignKey = column.IsForeignKey,
            IsIndexed = column.IsIndexed,
            IsUnique = column.IsUnique,
        }).ToList(),
    };

    private static DataModelRelationship CloneRelationship(DataModelRelationship relationship) => new()
    {
        SchemaId = relationship.SchemaId,
        ColumnPairs = relationship.ColumnPairs.ToList(),
        Id = relationship.Id,
        LeftTable = relationship.LeftTable,
        LeftColumn = relationship.LeftColumn,
        RightTable = relationship.RightTable,
        RightColumn = relationship.RightColumn,
        Kind = relationship.Kind,
        IsResolved = relationship.IsResolved,
        ConstraintName = relationship.ConstraintName,
        OnDelete = relationship.OnDelete,
        OnUpdate = relationship.OnUpdate,
        Warning = relationship.Warning,
        ReferencedEndCardinality = relationship.ReferencedEndCardinality,
        ReferencingEndCardinality = relationship.ReferencingEndCardinality,
        ChildColumnIsUnique = relationship.ChildColumnIsUnique,
        ConnectorLayout = CloneConnectorLayout(relationship.ConnectorLayout),
    };

    private static DataModelPendingOperation CloneOperation(DataModelPendingOperation operation) => new()
    {
        ColumnNames = operation.ColumnNames.ToList(), ReferencedColumnNames = operation.ReferencedColumnNames.ToList(),
        ColumnCollations = operation.ColumnCollations.ToList(), ExpressionSql = operation.ExpressionSql,
        CheckExpressionSql = operation.CheckExpressionSql,
        Collation = operation.Collation, IndexName = operation.IndexName, IsUnique = operation.IsUnique,
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
            Checks = column.Checks.ToList(),
            SchemaId = column.SchemaId,
            Name = column.Name,
            TypeLabel = column.TypeLabel,
            IsPrimaryKey = column.IsPrimaryKey,
            IsIdentity = column.IsIdentity,
            IsRowVersion = column.IsRowVersion,
            Nullable = column.Nullable,
            Collation = column.Collation,
            DefaultSql = column.DefaultSql,
            IsForeignKey = column.IsForeignKey,
            IsIndexed = column.IsIndexed,
            IsUnique = column.IsUnique,
        }).ToList(),
        ReferencedTableName = operation.ReferencedTableName,
        ReferencedColumnName = operation.ReferencedColumnName,
        ConstraintName = operation.ConstraintName,
        OnDelete = operation.OnDelete,
        OnUpdate = operation.OnUpdate,
        Description = operation.Description,
    };
}
