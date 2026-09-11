using System.Text.Json;
using CSharpDB.Admin.Models;
using CSharpDB.Client.Models;

namespace CSharpDB.Admin.Services;

/// <summary>Pure, ordered preview of pending DDL. Never changes the baseline, layout, or pending SQL.</summary>
public sealed class DataModelSchemaProjection
{
    public DataModelState State { get; }
    private readonly Dictionary<DataModelNode, string?> _tables = [];
    private readonly Dictionary<DataModelNode, string> _originalNames = [];
    private readonly Dictionary<DataModelColumn, string?> _columns = [];
    private readonly HashSet<DataModelColumn> _changedTypes = [];
    public List<string> Errors { get; } = [];
    private static bool Same(string? left, string? right) => string.Equals(left, right, StringComparison.OrdinalIgnoreCase);
    public static T Copy<T>(T value) => JsonSerializer.Deserialize<T>(JsonSerializer.Serialize(value))!;

    public DataModelSchemaProjection(DataModelState baseline)
    {
        State = Copy(baseline.SchemaContext ?? baseline);
        State.Nodes.RemoveAll(node => node.IsDraft);
        State.Relationships.RemoveAll(relationship => relationship.Kind == DataModelRelationshipKind.Draft);
        State.PendingOperations.Clear();
        foreach (var node in State.Nodes)
        {
            var inspected = (baseline.SchemaContext ?? baseline).Nodes.FirstOrDefault(source =>
                source.Kind == node.Kind && (node.SchemaId != Guid.Empty ? source.SchemaId == node.SchemaId : Same(source.Name, node.Name)));
            node.Dependencies = inspected?.Dependencies.ToList() ?? [];
            node.DependencyWarnings = inspected?.DependencyWarnings.ToList() ?? [];
            _tables[node] = node.Name;
            _originalNames[node] = node.Name;
            if (!node.Keys.Any(key => key.Kind == KeyConstraintKind.PrimaryKey) && node.Columns.Any(column => column.IsPrimaryKey))
                node.Keys.Add(new() { Kind = KeyConstraintKind.PrimaryKey, Columns = node.Columns.Where(column => column.IsPrimaryKey).Select(column => column.Name).ToArray() });
            foreach (var column in node.Columns) _columns[column] = column.Name;
        }
    }

    public static DataModelSchemaProjection ForEditing(DataModelState baseline)
    {
        var preview = new DataModelSchemaProjection(baseline);
        foreach (var operation in baseline.PendingOperations)
        {
            try { preview.Apply(operation); }
            catch (Exception ex) when (ex is InvalidOperationException or CSharpDB.Primitives.CSharpDbException)
            { preview.Errors.Add($"{operation.Kind} ({operation.TableName}): {ex.Message}"); }
        }
        return preview;
    }

    public DataModelNode? Find(string name) => State.Nodes.FirstOrDefault(node => Same(node.Name, name));
    public DataModelNode? FromCanvas(DataModelNode node) => State.Nodes.FirstOrDefault(candidate =>
        node.SchemaId != Guid.Empty && candidate.SchemaId == node.SchemaId || Same(_originalNames.GetValueOrDefault(candidate), node.Name) || Same(candidate.Name, node.Name));
    public string OriginalName(DataModelNode node) => _originalNames.GetValueOrDefault(node, node.Name);
    public string? LiveName(DataModelNode node) => _tables.GetValueOrDefault(node);
    public string? LiveColumn(DataModelColumn column) => _changedTypes.Contains(column) ? null : _columns.GetValueOrDefault(column);
    public static string[] ChildColumns(DataModelPendingOperation op) => op.ColumnNames.Count > 0 ? op.ColumnNames.ToArray() : [op.ColumnName ?? ""];
    public static string[] ParentColumns(DataModelPendingOperation op) => op.ReferencedColumnNames.Count > 0 ? op.ReferencedColumnNames.ToArray() : [op.ReferencedColumnName ?? ""];

    public void Apply(DataModelPendingOperation op)
    {
        var node = Find(op.TableName);
        var column = node?.Columns.FirstOrDefault(column => Same(column.Name, op.ColumnName));
        switch (op.Kind)
        {
            case DataModelPendingOperationKind.CreateTable:
                // A same-name live object stays live so review can diagnose the collision.
                if (node is not null) return;
                node = new() { Name = op.TableName, IsDraft = true, Columns = Copy(op.Columns) };
                if (node.Columns.Count == 0) node.Columns.Add(new() { Name = "Id", TypeLabel = "INTEGER", IsPrimaryKey = true, IsIdentity = true, Nullable = false });
                State.Nodes.Add(node); _tables[node] = null; _originalNames[node] = op.TableName;
                foreach (var item in node.Columns) { _columns[item] = null; node.Checks.AddRange(item.Checks); }
                if (node.Columns.Any(item => item.IsPrimaryKey)) node.Keys.Add(new() { Kind = KeyConstraintKind.PrimaryKey, Columns = node.Columns.Where(item => item.IsPrimaryKey).Select(item => item.Name).ToArray() });
                break;
            case DataModelPendingOperationKind.DropTable:
                if (node is null) return;
                State.Nodes.Remove(node);
                State.Relationships.RemoveAll(r => Same(r.LeftTable, op.TableName) || Same(r.RightTable, op.TableName));
                break;
            case DataModelPendingOperationKind.RenameTable:
                if (node is null) return;
                node.Name = op.NewTableName ?? node.Name;
                foreach (var r in State.Relationships) { if (Same(r.LeftTable, op.TableName)) r.LeftTable = node.Name; if (Same(r.RightTable, op.TableName)) r.RightTable = node.Name; }
                break;
            case DataModelPendingOperationKind.AddColumn:
                if (node is null || column is not null) return;
                column = ColumnFromOperation(op); node.Columns.Add(column); _columns[column] = null; node.Checks.AddRange(column.Checks);
                break;
            case DataModelPendingOperationKind.DropColumn:
                if (column is not null) node!.Columns.Remove(column);
                break;
            case DataModelPendingOperationKind.RenameColumn:
                if (column is null) return;
                string old = column.Name, renamed = op.NewColumnName ?? old;
                column.Name = renamed;
                node!.Checks = node.Checks.Select(check => RenameCheck(check, old, renamed)).ToList();
                foreach (var item in node.Columns) item.Checks = item.Checks.Select(check => RenameCheck(check, old, renamed)).ToList();
                node!.Keys = node.Keys.Select(key => new KeyConstraintDefinition { SchemaId = key.SchemaId, ConstraintName = key.ConstraintName, Kind = key.Kind,
                    BackingIndexName = key.BackingIndexName, Columns = key.Columns.Select(name => Same(name, old) ? renamed : name).ToArray() }).ToList();
                node.Indexes = node.Indexes.Select(index => new DataModelIndexMetadata { IndexName = index.IndexName, IsUnique = index.IsUnique, IsEngineManaged = index.IsEngineManaged,
                    ColumnCollations = index.ColumnCollations, Columns = index.Columns.Select(name => Same(name, old) ? renamed : name).ToArray() }).ToList();
                foreach (var r in State.Relationships)
                {
                    r.ColumnPairs = r.EffectiveColumnPairs.Select(pair => new DataModelColumnPair(Same(r.LeftTable, node.Name) && Same(pair.ChildColumn, old) ? renamed : pair.ChildColumn,
                        Same(r.RightTable, node.Name) && Same(pair.ParentColumn, old) ? renamed : pair.ParentColumn)).ToList();
                    r.LeftColumn = r.ColumnPairs[0].ChildColumn; r.RightColumn = r.ColumnPairs[0].ParentColumn;
                }
                break;
            case DataModelPendingOperationKind.AlterColumnType:
                if (column is not null) { column.TypeLabel = SchemaColumnRules.NormalizeType(op.ColumnType); _changedTypes.Add(column); }
                break;
            case DataModelPendingOperationKind.SetDefault: if (column is not null) column.DefaultSql = op.ExpressionSql; break;
            case DataModelPendingOperationKind.DropDefault: if (column is not null) column.DefaultSql = null; break;
            case DataModelPendingOperationKind.SetNotNull: if (column is not null) column.Nullable = false; break;
            case DataModelPendingOperationKind.DropNotNull: if (column is not null) column.Nullable = true; break;
            case DataModelPendingOperationKind.SetCollation: if (column is not null) column.Collation = op.Collation; break;
            case DataModelPendingOperationKind.DropCollation: if (column is not null) column.Collation = null; break;
            case DataModelPendingOperationKind.AddPrimaryKey:
            case DataModelPendingOperationKind.AddUniqueKey:
                node?.Keys.Add(new() { ConstraintName = op.ConstraintName, Kind = op.Kind == DataModelPendingOperationKind.AddPrimaryKey ? KeyConstraintKind.PrimaryKey : KeyConstraintKind.Unique, Columns = ChildColumns(op) });
                break;
            case DataModelPendingOperationKind.DropPrimaryKey:
                RemoveKey(node, key => key.Kind == KeyConstraintKind.PrimaryKey); break;
            case DataModelPendingOperationKind.DropConstraint:
                RemoveKey(node, key => Same(key.ConstraintName, op.ConstraintName));
                node?.Checks.RemoveAll(check => Same(check.ConstraintName, op.ConstraintName));
                State.Relationships.RemoveAll(r => Same(r.LeftTable, op.TableName) && Same(r.ConstraintName, op.ConstraintName));
                break;
            case DataModelPendingOperationKind.AddCheck:
                node?.Checks.Add(new() { ConstraintName = op.ConstraintName, ExpressionSql = op.ExpressionSql ?? "" }); break;
            case DataModelPendingOperationKind.CreateIndex:
                node?.Indexes.Add(new() { IndexName = op.IndexName ?? "", IsUnique = op.IsUnique, Columns = ChildColumns(op), ColumnCollations = op.ColumnCollations.ToArray() }); break;
            case DataModelPendingOperationKind.DropIndex: node?.Indexes.RemoveAll(index => Same(index.IndexName, op.IndexName)); break;
            case DataModelPendingOperationKind.AddForeignKey:
                var pairs = ChildColumns(op).Zip(ParentColumns(op), (child, parent) => new DataModelColumnPair(child, parent)).ToList();
                if (pairs.Count == 0) return;
                State.Relationships.Add(new() { Id = op.Id, LeftTable = op.TableName, RightTable = op.ReferencedTableName ?? "", ColumnPairs = pairs,
                    LeftColumn = pairs[0].ChildColumn, RightColumn = pairs[0].ParentColumn, Kind = DataModelRelationshipKind.Draft,
                    ConstraintName = op.ConstraintName, OnDelete = op.OnDelete, OnUpdate = op.OnUpdate }); break;
            case DataModelPendingOperationKind.DropForeignKey:
                State.Relationships.RemoveAll(r => Same(r.LeftTable, op.TableName) && Same(r.ConstraintName, op.ConstraintName)); break;
        }
        if (node is not null) RecomputeKeys(node);
        DataModelGraphBuilder.UpdateDraftCardinalities(State);
    }

    private static void RemoveKey(DataModelNode? node, Predicate<KeyConstraintDefinition> predicate)
    {
        if (node is null) return;
        foreach (var key in node.Keys.Where(key => predicate(key)).ToArray())
        {
            node.Indexes.RemoveAll(index => Same(index.IndexName, key.BackingIndexName));
            if (key.Kind == KeyConstraintKind.PrimaryKey) foreach (var column in node.Columns.Where(column => key.Columns.Contains(column.Name, StringComparer.OrdinalIgnoreCase))) column.IsPrimaryKey = false;
            node.Keys.Remove(key);
        }
    }

    private static CheckConstraintDefinition RenameCheck(CheckConstraintDefinition check, string oldName, string newName)
    {
        var renamed = CSharpDB.Execution.QueryPlanner.PreviewCheckColumnRename(new()
        { SchemaId = check.SchemaId, ConstraintName = check.ConstraintName, ColumnName = check.ColumnName, ExpressionSql = check.ExpressionSql }, oldName, newName);
        return new() { SchemaId = renamed.SchemaId, ConstraintName = renamed.ConstraintName, ColumnName = renamed.ColumnName, ExpressionSql = renamed.ExpressionSql };
    }

    private static void RecomputeKeys(DataModelNode node)
    {
        foreach (var column in node.Columns)
        {
            column.IsPrimaryKey = node.Keys.Any(key => key.Kind == KeyConstraintKind.PrimaryKey && key.Columns.Contains(column.Name, StringComparer.OrdinalIgnoreCase));
            if (column.IsPrimaryKey) column.Nullable = false;
            column.IsUnique = node.Keys.Any(key => key.Columns.Count == 1 && Same(key.Columns[0], column.Name)) || node.Indexes.Any(index => index.IsUnique && index.Columns.Count == 1 && Same(index.Columns[0], column.Name));
        }
    }

    public static DataModelColumn ColumnFromOperation(DataModelPendingOperation op)
    {
        string type = SchemaColumnRules.NormalizeType(op.ColumnType);
        var column = new DataModelColumn { Name = op.ColumnName ?? "", TypeLabel = type, Nullable = !op.NotNull && type != "ROWVERSION", IsRowVersion = type == "ROWVERSION",
            DefaultSql = string.IsNullOrWhiteSpace(op.ExpressionSql) ? null : op.ExpressionSql, Collation = string.IsNullOrWhiteSpace(op.Collation) ? null : op.Collation };
        if (!string.IsNullOrWhiteSpace(op.CheckExpressionSql)) column.Checks.Add(new() { ConstraintName = op.ConstraintName, ColumnName = column.Name, ExpressionSql = op.CheckExpressionSql });
        return column;
    }
}
