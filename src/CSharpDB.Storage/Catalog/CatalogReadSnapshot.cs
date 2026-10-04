using System.Collections.Immutable;
using CSharpDB.Primitives;

namespace CSharpDB.Storage.Catalog;

internal readonly record struct CatalogReadTable(
    TableSchema Schema,
    uint RootPage,
    IReadOnlyList<ForeignKeyDefinition> ForeignKeys,
    IReadOnlyList<TableForeignKeyReference> ReferencingForeignKeys);

internal readonly record struct CatalogReadIndex(IndexSchema Schema, uint RootPage);

/// <summary>
/// Detached catalog metadata for one committed snapshot. It owns no pager,
/// cursor, table tree, index store, or reader lease. Published instances are read-only.
/// </summary>
internal sealed class CatalogReadSnapshot
{
    internal CatalogReadSnapshot(CatalogReadMetadata metadata, ulong rowVersionHighWater,
        uint pageCount, uint schemaRootPage, uint freelistHead, uint changeCounter)
    {
        Metadata = metadata;
        RowVersionHighWater = rowVersionHighWater;
        PageCount = pageCount;
        SchemaRootPage = schemaRootPage;
        FreelistHead = freelistHead;
        ChangeCounter = changeCounter;
    }

    internal CatalogReadMetadata Metadata { get; }
    internal long SchemaVersion => Metadata.SchemaVersion;
    internal ulong RowVersionHighWater { get; }
    internal uint PageCount { get; }
    internal uint SchemaRootPage { get; }
    internal uint FreelistHead { get; }
    internal uint ChangeCounter { get; }
    internal void ApplyHeader(Pager pager) => pager.ApplySnapshotHeader(PageCount, SchemaRootPage, FreelistHead, ChangeCounter);
    internal SchemaCatalog CreateReaderView(Pager pager) => new(this, pager);
    internal bool TryGetReadTable(string name, out CatalogReadTable table) => Metadata.TryGetReadTable(name, out table);
    internal TableSchema? GetTable(string name) => Metadata.GetTable(name);
    internal uint GetTableRootPage(string name) => Metadata.GetTableRootPage(name);
    internal IReadOnlyCollection<string> GetTableNames() => Metadata.GetTableNames();
    internal IReadOnlyList<ForeignKeyDefinition> GetForeignKeysForTable(string name) => Metadata.GetForeignKeysForTable(name);
    internal IReadOnlyList<TableForeignKeyReference> GetReferencingForeignKeys(string name) => Metadata.GetReferencingForeignKeys(name);
    internal TableStatistics? GetTableStatistics(string name) => Metadata.GetTableStatistics(name);
    internal IReadOnlyCollection<TableStatistics> GetTableStatistics() => Metadata.GetTableStatistics();
    internal ColumnStatistics? GetColumnStatistics(string table, string column) => Metadata.GetColumnStatistics(table, column);
    internal IReadOnlyCollection<ColumnStatistics> GetColumnStatistics(string table) => Metadata.GetColumnStatistics(table);
    internal IReadOnlyCollection<ColumnStatistics> GetColumnStatistics() => Metadata.GetColumnStatistics();
    internal IReadOnlyCollection<ColumnDistributionStatistics> GetColumnDistributionStatistics() => Metadata.GetColumnDistributionStatistics();
    internal IReadOnlyCollection<IndexPrefixStatistics> GetIndexPrefixStatistics() => Metadata.GetIndexPrefixStatistics();
    internal bool TryGetEstimatedTableRowCount(string table, out long count) => Metadata.TryGetEstimatedTableRowCount(table, out count);
    internal bool TryGetExactTableRowCount(string table, out long count) => Metadata.TryGetExactTableRowCount(table, out count);
    internal bool TryGetFreshColumnStatistics(string table, string column, out ColumnStatistics statistics)
        => Metadata.TryGetFreshColumnStatistics(table, column, out statistics);
    internal bool TryGetFreshColumnDistributionStatistics(string table, string column, out ColumnDistributionStatistics statistics)
        => Metadata.TryGetFreshColumnDistributionStatistics(table, column, out statistics);
    internal bool TryGetFreshIndexPrefixStatistics(string name, out IndexPrefixStatistics statistics)
        => Metadata.TryGetFreshIndexPrefixStatistics(name, out statistics);
    internal BTree GetTableTree(string table, Pager pager) => Metadata.GetTableTree(table, pager);
    internal ValueTask<long> GetExactTableRowCountAsync(string table, Pager pager, CancellationToken ct)
        => Metadata.GetExactTableRowCountAsync(table, pager, ct);
    internal IndexSchema? GetIndex(string name) => Metadata.GetIndex(name);
    internal IReadOnlyCollection<IndexSchema> GetIndexes() => Metadata.GetIndexes();
    internal IReadOnlyList<IndexSchema> GetIndexesForTable(string table) => Metadata.GetIndexesForTable(table);
    internal IReadOnlyList<IndexSchema> GetSqlIndexesForTable(string table) => Metadata.GetSqlIndexesForTable(table);
    internal IIndexStore GetIndexStore(string name, Pager pager) => Metadata.GetIndexStore(name, pager);
    internal string? GetViewSql(string name) => Metadata.GetViewSql(name);
    internal IReadOnlyCollection<string> GetViewNames() => Metadata.GetViewNames();
    internal bool IsView(string name) => Metadata.IsView(name);
    internal TriggerSchema? GetTrigger(string name) => Metadata.GetTrigger(name);
    internal IReadOnlyCollection<TriggerSchema> GetTriggers() => Metadata.GetTriggers();
    internal IReadOnlyList<TriggerSchema> GetTriggersForTable(string table) => Metadata.GetTriggersForTable(table);
}

// Header-only commits share these detached maps without recopying their fields.
internal sealed class CatalogReadMetadata
{
    private readonly ImmutableDictionary<string, CatalogReadTable> _tables;
    private readonly ImmutableDictionary<string, CatalogReadIndex> _indexes;
    private readonly ImmutableDictionary<string, string> _views;
    private readonly ImmutableDictionary<string, TriggerSchema> _triggers;
    private readonly ImmutableDictionary<string, TableStatistics> _tableStatistics;
    private readonly ImmutableHashSet<string> _exactTableRowCounts;
    private readonly ImmutableDictionary<string, ColumnStatistics> _columnStatistics;
    private readonly ImmutableDictionary<string, ColumnDistributionStatistics> _columnDistributions;
    private readonly ImmutableDictionary<string, IndexPrefixStatistics> _indexPrefixes;
    private readonly Dictionary<string, IndexSchema[]> _indexesByTable;
    private readonly Dictionary<string, IndexSchema[]> _sqlIndexesByTable;
    private readonly Dictionary<string, TriggerSchema[]> _triggersByTable;
    private Dictionary<string, ColumnStatistics[]>? _columnsByTable;
    private readonly IndexSchema[] _allIndexes;
    private readonly TriggerSchema[] _allTriggers;
    private TableStatistics[]? _allTableStatistics;
    private ColumnStatistics[]? _allColumnStatistics;
    private ColumnDistributionStatistics[]? _allColumnDistributions;
    private IndexPrefixStatistics[]? _allIndexPrefixes;
    private readonly IIndexProvider _indexProvider;
    private string[]? _tableNames;
    private string[]? _viewNames;

    internal CatalogReadMetadata(
        CatalogReadMetadata? previous,
        long schemaVersion,
        IIndexProvider indexProvider,
        Dictionary<string, TableSchema> tables,
        Dictionary<string, uint> tableRoots,
        Func<string, IReadOnlyList<ForeignKeyDefinition>> getForeignKeys,
        Func<string, IReadOnlyList<TableForeignKeyReference>> getReferencingForeignKeys,
        Dictionary<string, IndexSchema> indexes,
        Dictionary<string, uint> indexRoots,
        Dictionary<string, string> views,
        Dictionary<string, TriggerSchema> triggers,
        Dictionary<string, TableStatistics> tableStatistics,
        HashSet<string> exactTableRowCounts,
        bool trustExactRowCounts,
        Dictionary<string, ColumnStatistics> columnStatistics,
        Dictionary<string, ColumnDistributionStatistics> columnDistributions,
        Dictionary<string, IndexPrefixStatistics> indexPrefixes)
    {
        SchemaVersion = schemaVersion;
        _indexProvider = indexProvider;

        bool sameLogicalSchema = previous is not null && previous.SchemaVersion == schemaVersion &&
            previous._tables.Count == tables.Count;
        if (sameLogicalSchema)
        {
            foreach (var pair in tables)
            {
                if (!previous!._tables.TryGetValue(pair.Key, out var oldTable) ||
                    !SameTableDefinition(oldTable.Schema, pair.Value))
                {
                    sameLogicalSchema = false;
                    break;
                }
            }
        }

        var capturedTables = previous?._tables ?? EmptyMap<CatalogReadTable>();
        foreach (var pair in tables)
        {
            bool found = capturedTables.TryGetValue(pair.Key, out var oldTable);
            TableSchema schema = found && SameTableDefinition(oldTable.Schema, pair.Value) &&
                oldTable.Schema.NextRowId == pair.Value.NextRowId
                    ? oldTable.Schema
                    : CloneTable(pair.Value);
            var foreignKeys = found && sameLogicalSchema ? oldTable.ForeignKeys : getForeignKeys(pair.Key);
            var references = found && sameLogicalSchema ? oldTable.ReferencingForeignKeys : getReferencingForeignKeys(pair.Key);
            var table = new CatalogReadTable(schema, tableRoots[pair.Key], foreignKeys, references);
            if (!found || table != oldTable)
                capturedTables = capturedTables.SetItem(pair.Key, table);
        }
        _tables = RemoveMissing(capturedTables, tables);

        var capturedIndexes = previous?._indexes ?? EmptyMap<CatalogReadIndex>();
        bool sameIndexSchemas = previous is not null && capturedIndexes.Count == indexes.Count;
        foreach (var pair in indexes)
        {
            bool found = capturedIndexes.TryGetValue(pair.Key, out var oldIndex);
            sameIndexSchemas &= found && ReferenceEquals(oldIndex.Schema, pair.Value);
            var index = new CatalogReadIndex(pair.Value, indexRoots[pair.Key]);
            if (!found || index != oldIndex)
                capturedIndexes = capturedIndexes.SetItem(pair.Key, index);
        }
        _indexes = RemoveMissing(capturedIndexes, indexes);
        _views = CaptureMap(previous?._views, views);
        _triggers = CaptureMap(previous?._triggers, triggers);
        _tableStatistics = CaptureMap(previous?._tableStatistics, tableStatistics);
        _exactTableRowCounts = trustExactRowCounts
            ? CaptureSet(previous?._exactTableRowCounts, exactTableRowCounts)
            : ImmutableHashSet.Create<string>(StringComparer.OrdinalIgnoreCase);
        _columnStatistics = CaptureMap(previous?._columnStatistics, columnStatistics);
        _columnDistributions = CaptureMap(previous?._columnDistributions, columnDistributions);
        _indexPrefixes = CaptureMap(previous?._indexPrefixes, indexPrefixes);

        if (sameIndexSchemas)
        {
            _allIndexes = previous!._allIndexes;
            _indexesByTable = previous._indexesByTable;
            _sqlIndexesByTable = previous._sqlIndexesByTable;
        }
        else
        {
            _allIndexes = indexes.Values.ToArray();
            _indexesByTable = GroupByTable(_allIndexes, entry => entry.TableName);
            _sqlIndexesByTable = GroupByTable(_allIndexes.Where(entry => entry.Kind is IndexKind.Sql or IndexKind.ConstraintInternal), entry => entry.TableName);
        }
        if (ReferenceEquals(_triggers, previous?._triggers))
        {
            _allTriggers = previous!._allTriggers;
            _triggersByTable = previous._triggersByTable;
        }
        else
        {
            _allTriggers = triggers.Values.ToArray();
            _triggersByTable = GroupByTable(_allTriggers, entry => entry.TableName);
        }
        if (ReferenceEquals(_columnStatistics, previous?._columnStatistics))
        {
            _allColumnStatistics = Volatile.Read(ref previous!._allColumnStatistics);
            _columnsByTable = Volatile.Read(ref previous._columnsByTable);
        }
        else
        {
            _allColumnStatistics = columnStatistics.Values.ToArray();
            _columnsByTable = GroupByTable(_allColumnStatistics, entry => entry.TableName);
        }
        if (ReferenceEquals(_tableStatistics, previous?._tableStatistics))
            _allTableStatistics = Volatile.Read(ref previous!._allTableStatistics);
        if (ReferenceEquals(_columnDistributions, previous?._columnDistributions))
            _allColumnDistributions = Volatile.Read(ref previous!._allColumnDistributions);
        if (ReferenceEquals(_indexPrefixes, previous?._indexPrefixes))
            _allIndexPrefixes = Volatile.Read(ref previous!._allIndexPrefixes);
        if (ReferenceEquals(_tables, previous?._tables))
            _tableNames = Volatile.Read(ref previous!._tableNames);
        if (ReferenceEquals(_views, previous?._views))
            _viewNames = Volatile.Read(ref previous!._viewNames);
    }

    internal CatalogReadMetadata CaptureChanges(
        CatalogReadChanges changes,
        Dictionary<string, TableSchema> tables,
        Dictionary<string, uint> tableRoots,
        Dictionary<string, uint> indexRoots,
        Dictionary<string, TableStatistics> tableStatistics,
        HashSet<string> exactTableRowCounts,
        Dictionary<string, ColumnStatistics> columnStatistics)
    {
        var capturedTables = _tables;
        foreach (string name in changes.Tables)
        {
            CatalogReadTable previous = _tables[name];
            TableSchema live = tables[name];
            TableSchema schema = SameTableDefinition(previous.Schema, live) && previous.Schema.NextRowId == live.NextRowId
                ? previous.Schema : CloneTable(live);
            var table = new CatalogReadTable(schema, tableRoots[name], previous.ForeignKeys, previous.ReferencingForeignKeys);
            if (table != previous)
                capturedTables = capturedTables.SetItem(name, table);
        }
        var capturedIndexes = _indexes;
        foreach (string name in changes.Indexes)
        {
            CatalogReadIndex previous = _indexes[name];
            uint root = indexRoots[name];
            if (previous.RootPage != root)
                capturedIndexes = capturedIndexes.SetItem(name, new(previous.Schema, root));
        }
        var capturedStatistics = CaptureChangedMap(_tableStatistics, tableStatistics, changes.TableStatistics);
        var capturedExactCounts = _exactTableRowCounts;
        foreach (string name in changes.TableStatistics)
            capturedExactCounts = exactTableRowCounts.Contains(name)
                ? capturedExactCounts.Add(name) : capturedExactCounts.Remove(name);
        var capturedColumns = CaptureChangedMap(_columnStatistics, columnStatistics, changes.ColumnStatistics);

        if (ReferenceEquals(capturedTables, _tables) && ReferenceEquals(capturedIndexes, _indexes) &&
            ReferenceEquals(capturedStatistics, _tableStatistics) && ReferenceEquals(capturedExactCounts, _exactTableRowCounts) &&
            ReferenceEquals(capturedColumns, _columnStatistics))
            return this;

        return new CatalogReadMetadata(this, capturedTables, capturedIndexes, capturedStatistics, capturedExactCounts, capturedColumns);
    }

    private CatalogReadMetadata(CatalogReadMetadata previous,
        ImmutableDictionary<string, CatalogReadTable> tables,
        ImmutableDictionary<string, CatalogReadIndex> indexes,
        ImmutableDictionary<string, TableStatistics> tableStatistics,
        ImmutableHashSet<string> exactTableRowCounts,
        ImmutableDictionary<string, ColumnStatistics> columnStatistics)
    {
        SchemaVersion = previous.SchemaVersion;
        _tables = tables;
        _indexes = indexes;
        _tableStatistics = tableStatistics;
        _exactTableRowCounts = exactTableRowCounts;
        _columnStatistics = columnStatistics;
        _views = previous._views;
        _triggers = previous._triggers;
        _columnDistributions = previous._columnDistributions;
        _indexPrefixes = previous._indexPrefixes;
        _indexesByTable = previous._indexesByTable;
        _sqlIndexesByTable = previous._sqlIndexesByTable;
        _triggersByTable = previous._triggersByTable;
        _allIndexes = previous._allIndexes;
        _allTriggers = previous._allTriggers;
        _indexProvider = previous._indexProvider;
        _tableNames = Volatile.Read(ref previous._tableNames);
        _viewNames = Volatile.Read(ref previous._viewNames);
        _allColumnDistributions = Volatile.Read(ref previous._allColumnDistributions);
        _allIndexPrefixes = Volatile.Read(ref previous._allIndexPrefixes);
        if (ReferenceEquals(tableStatistics, previous._tableStatistics))
            _allTableStatistics = Volatile.Read(ref previous._allTableStatistics);
        if (ReferenceEquals(columnStatistics, previous._columnStatistics))
        {
            _allColumnStatistics = Volatile.Read(ref previous._allColumnStatistics);
            _columnsByTable = Volatile.Read(ref previous._columnsByTable);
        }
    }

    private static ImmutableDictionary<string, T> CaptureChangedMap<T>(
        ImmutableDictionary<string, T> captured, Dictionary<string, T> source, HashSet<string> changes) where T : class
    {
        foreach (string name in changes)
        {
            if (!source.TryGetValue(name, out T? value))
                captured = captured.Remove(name);
            else if (!captured.TryGetValue(name, out T? previous) || !SameCapturedValue(previous, value))
                captured = captured.SetItem(name, value);
        }
        return captured;
    }

    internal long SchemaVersion { get; }
    internal bool TryGetReadTable(string tableName, out CatalogReadTable table)
        => _tables.TryGetValue(tableName, out table);

    internal TableSchema? GetTable(string tableName)
        => _tables.TryGetValue(tableName, out var table) ? table.Schema : null;

    internal uint GetTableRootPage(string tableName)
        => _tables.TryGetValue(tableName, out var table)
            ? table.RootPage
            : throw new CSharpDbException(ErrorCode.TableNotFound, $"Table '{tableName}' not found.");

    internal IReadOnlyCollection<string> GetTableNames() => GetArray(ref _tableNames, _tables.Keys);
    internal IReadOnlyList<ForeignKeyDefinition> GetForeignKeysForTable(string tableName)
        => _tables.TryGetValue(tableName, out var table) ? table.ForeignKeys : Array.Empty<ForeignKeyDefinition>();
    internal IReadOnlyList<TableForeignKeyReference> GetReferencingForeignKeys(string tableName)
        => _tables.TryGetValue(tableName, out var table) ? table.ReferencingForeignKeys : Array.Empty<TableForeignKeyReference>();
    internal TableStatistics? GetTableStatistics(string tableName)
        => _tableStatistics.GetValueOrDefault(tableName);
    internal IReadOnlyCollection<TableStatistics> GetTableStatistics() => GetArray(ref _allTableStatistics, _tableStatistics.Values);
    internal ColumnStatistics? GetColumnStatistics(string tableName, string columnName)
        => _columnStatistics.GetValueOrDefault(ColumnKey(tableName, columnName));
    internal IReadOnlyCollection<ColumnStatistics> GetColumnStatistics(string tableName)
    {
        var grouped = Volatile.Read(ref _columnsByTable);
        if (grouped is null)
        {
            grouped = GroupByTable(GetColumnStatistics(), entry => entry.TableName);
            grouped = Interlocked.CompareExchange(ref _columnsByTable, grouped, null) ?? grouped;
        }
        return grouped.GetValueOrDefault(tableName) ?? Array.Empty<ColumnStatistics>();
    }
    internal IReadOnlyCollection<ColumnStatistics> GetColumnStatistics() => GetArray(ref _allColumnStatistics, _columnStatistics.Values);
    internal IReadOnlyCollection<ColumnDistributionStatistics> GetColumnDistributionStatistics() => GetArray(ref _allColumnDistributions, _columnDistributions.Values);
    internal IReadOnlyCollection<IndexPrefixStatistics> GetIndexPrefixStatistics() => GetArray(ref _allIndexPrefixes, _indexPrefixes.Values);

    internal bool TryGetEstimatedTableRowCount(string tableName, out long count)
    {
        if (_tableStatistics.TryGetValue(tableName, out var statistics))
        {
            count = statistics.RowCount;
            return true;
        }
        count = 0;
        return false;
    }

    internal bool TryGetExactTableRowCount(string tableName, out long count)
    {
        count = 0;
        return _exactTableRowCounts.Contains(tableName) && TryGetEstimatedTableRowCount(tableName, out count);
    }

    internal bool TryGetFreshColumnStatistics(string tableName, string columnName, out ColumnStatistics statistics)
    {
        if (_columnStatistics.TryGetValue(ColumnKey(tableName, columnName), out statistics!) && !statistics.IsStale)
            return true;
        statistics = null!;
        return false;
    }

    internal bool TryGetFreshColumnDistributionStatistics(string tableName, string columnName, out ColumnDistributionStatistics statistics)
    {
        if (_columnDistributions.TryGetValue(ColumnKey(tableName, columnName), out statistics!) &&
            TryGetFreshColumnStatistics(tableName, columnName, out _))
            return true;
        statistics = null!;
        return false;
    }

    internal bool TryGetFreshIndexPrefixStatistics(string indexName, out IndexPrefixStatistics statistics)
    {
        if (_indexPrefixes.TryGetValue(indexName, out statistics!) &&
            _tableStatistics.TryGetValue(statistics.TableName, out var table) && !table.HasStaleColumns)
            return true;
        statistics = null!;
        return false;
    }

    internal BTree GetTableTree(string tableName, Pager pager)
    {
        var tree = new BTree(pager, GetTableRootPage(tableName), tableName);
        if (TryGetEstimatedTableRowCount(tableName, out long count))
            tree.SetCachedEntryCount(count);
        return tree;
    }

    internal ValueTask<long> GetExactTableRowCountAsync(string tableName, Pager pager, CancellationToken ct)
        => TryGetExactTableRowCount(tableName, out long count)
            ? ValueTask.FromResult(count)
            : GetTableTree(tableName, pager).CountEntriesExactAsync(ct);

    internal IndexSchema? GetIndex(string indexName)
        => _indexes.TryGetValue(indexName, out var index) ? index.Schema : null;
    internal IReadOnlyCollection<IndexSchema> GetIndexes() => _allIndexes;
    internal IReadOnlyList<IndexSchema> GetIndexesForTable(string tableName)
        => _indexesByTable.GetValueOrDefault(tableName) ?? Array.Empty<IndexSchema>();
    internal IReadOnlyList<IndexSchema> GetSqlIndexesForTable(string tableName)
        => _sqlIndexesByTable.GetValueOrDefault(tableName) ?? Array.Empty<IndexSchema>();

    internal IIndexStore GetIndexStore(string indexName, Pager pager)
    {
        if (!_indexes.TryGetValue(indexName, out var index))
            throw new CSharpDbException(ErrorCode.TableNotFound, $"Index '{indexName}' not found.");
        IIndexStore store = _indexProvider.CreateIndexStore(pager, index.RootPage, index.Schema.IndexName);
        return index.Schema.Kind is IndexKind.Collection or IndexKind.Sql or IndexKind.FullTextInternal
            ? new OverflowingIndexStore(store, pager)
            : store;
    }

    internal string? GetViewSql(string viewName) => _views.GetValueOrDefault(viewName);
    internal IReadOnlyCollection<string> GetViewNames() => GetArray(ref _viewNames, _views.Keys);
    internal bool IsView(string name) => _views.ContainsKey(name);
    internal TriggerSchema? GetTrigger(string triggerName) => _triggers.GetValueOrDefault(triggerName);
    internal IReadOnlyCollection<TriggerSchema> GetTriggers() => _allTriggers;
    internal IReadOnlyList<TriggerSchema> GetTriggersForTable(string tableName)
        => _triggersByTable.GetValueOrDefault(tableName) ?? Array.Empty<TriggerSchema>();

    private static string ColumnKey(string tableName, string columnName) => $"{tableName}\u001F{columnName}";
    private static Dictionary<string, T[]> GroupByTable<T>(IEnumerable<T> entries, Func<T, string> tableName)
        => entries.GroupBy(tableName, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(group => group.Key, group => group.ToArray(), StringComparer.OrdinalIgnoreCase);

    private static ImmutableDictionary<string, T> EmptyMap<T>()
        => ImmutableDictionary.Create<string, T>(StringComparer.OrdinalIgnoreCase);

    private static ImmutableDictionary<string, T> CaptureMap<T>(
        ImmutableDictionary<string, T>? previous, Dictionary<string, T> source) where T : class
    {
        var captured = previous ?? EmptyMap<T>();
        foreach (var pair in source)
        {
            if (!captured.TryGetValue(pair.Key, out T? oldValue) ||
                !SameCapturedValue(oldValue, pair.Value))
                captured = captured.SetItem(pair.Key, pair.Value);
        }
        return RemoveMissing(captured, source);
    }

    private static bool SameCapturedValue<T>(T left, T right) where T : class
    {
        if (ReferenceEquals(left, right)) return true;
        return (left, right) switch
        {
            (TableStatistics a, TableStatistics b) =>
                a.TableName == b.TableName && a.RowCount == b.RowCount &&
                a.RowCountIsExact == b.RowCountIsExact && a.HasStaleColumns == b.HasStaleColumns &&
                a.LastPersistedChangeCounter == b.LastPersistedChangeCounter,
            (ColumnStatistics a, ColumnStatistics b) =>
                a.TableName == b.TableName && a.ColumnName == b.ColumnName &&
                a.DistinctCount == b.DistinctCount && a.NonNullCount == b.NonNullCount &&
                a.IsStale == b.IsStale && a.MinValue.Type == b.MinValue.Type &&
                a.MaxValue.Type == b.MaxValue.Type && a.MinValue == b.MinValue && a.MaxValue == b.MaxValue,
            _ => EqualityComparer<T>.Default.Equals(left, right),
        };
    }

    private static ImmutableDictionary<string, TCaptured> RemoveMissing<TCaptured, TSource>(
        ImmutableDictionary<string, TCaptured> captured, Dictionary<string, TSource> source)
    {
        foreach (var pair in captured)
            if (!source.ContainsKey(pair.Key))
                captured = captured.Remove(pair.Key);
        return captured;
    }

    private static ImmutableHashSet<string> CaptureSet(ImmutableHashSet<string>? previous, HashSet<string> source)
    {
        var captured = previous ?? ImmutableHashSet.Create<string>(StringComparer.OrdinalIgnoreCase);
        foreach (string value in source)
            captured = captured.Add(value);
        foreach (string value in captured)
            if (!source.Contains(value))
                captured = captured.Remove(value);
        return captured;
    }

    private static bool SameTableDefinition(TableSchema left, TableSchema right)
        => left.SchemaId == right.SchemaId && left.TableName == right.TableName &&
            ReferenceEquals(left.Columns, right.Columns) &&
            ReferenceEquals(left.ForeignKeys, right.ForeignKeys) &&
            ReferenceEquals(left.CheckConstraints, right.CheckConstraints) &&
            ReferenceEquals(left.KeyConstraints, right.KeyConstraints) &&
            SameMappings(left.QualifiedMappings, right.QualifiedMappings);

    private static bool SameMappings(Dictionary<string, int>? left, Dictionary<string, int>? right)
    {
        if (ReferenceEquals(left, right)) return true;
        if (left is null || right is null || left.Count != right.Count) return false;
        foreach (var pair in left)
            if (!right.TryGetValue(pair.Key, out int ordinal) || ordinal != pair.Value)
                return false;
        return true;
    }

    private static TableSchema CloneTable(TableSchema schema) => new()
    {
        SchemaId = schema.SchemaId,
        TableName = schema.TableName,
        Columns = schema.Columns,
        ForeignKeys = schema.ForeignKeys,
        CheckConstraints = schema.CheckConstraints,
        KeyConstraints = schema.KeyConstraints,
        NextRowId = schema.NextRowId,
        QualifiedMappings = schema.QualifiedMappings is null
            ? null
            : new Dictionary<string, int>(schema.QualifiedMappings, schema.QualifiedMappings.Comparer),
    };

    private static T[] GetArray<T>(ref T[]? cache, IEnumerable<T> source)
    {
        T[]? array = Volatile.Read(ref cache);
        if (array is not null) return array;
        array = source.ToArray();
        return Interlocked.CompareExchange(ref cache, array, null) ?? array;
    }

}

// Separate from persistence dirty queues: entries survive flushes and are consumed
// only by a successful coherent capture. Overflow switches to a full capture.
internal sealed class CatalogReadChanges
{
    internal const int MaximumEntries = 128;
    internal readonly HashSet<string> Tables = new(StringComparer.OrdinalIgnoreCase);
    internal readonly HashSet<string> Indexes = new(StringComparer.OrdinalIgnoreCase);
    internal readonly HashSet<string> TableStatistics = new(StringComparer.OrdinalIgnoreCase);
    internal readonly HashSet<string> ColumnStatistics = new(StringComparer.OrdinalIgnoreCase);
    internal int Count => Tables.Count + Indexes.Count + TableStatistics.Count + ColumnStatistics.Count;
    internal void Clear()
    {
        Tables.Clear();
        Indexes.Clear();
        TableStatistics.Clear();
        ColumnStatistics.Clear();
    }
}
