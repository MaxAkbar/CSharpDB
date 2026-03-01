using System.Buffers.Binary;
using System.Text;
using CSharpDB.Core;

namespace CSharpDB.Storage;

/// <summary>
/// The schema catalog stores table metadata in a B+tree.
/// Key = hash of table name, Value = serialized TableSchema.
/// Also maintains an in-memory cache of all schemas.
///
/// Index metadata is stored in a separate sub-tree whose root page is
/// stored as a sentinel entry (key = long.MaxValue) in the main catalog tree.
///
/// View definitions are stored as sentinel entry (key = long.MaxValue - 1)
/// pointing to a sub-tree where each entry maps view name hash to SQL text.
/// </summary>
public sealed class SchemaCatalog
{
    private const long IndexCatalogSentinel = long.MaxValue;
    private const long ViewCatalogSentinel = long.MaxValue - 1;
    private const long TriggerCatalogSentinel = long.MaxValue - 2;

    private readonly Pager _pager;
    private BTree? _catalogTree;
    private readonly Dictionary<string, TableSchema> _cache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, uint> _tableRootPages = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, BTree> _tableTrees = new(StringComparer.OrdinalIgnoreCase);

    // Index catalog
    private BTree? _indexCatalogTree;
    private readonly Dictionary<string, IndexSchema> _indexCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, uint> _indexRootPages = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, BTree> _indexTrees = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, IndexSchema[]> _indexesByTable = new(StringComparer.OrdinalIgnoreCase);

    // View catalog
    private BTree? _viewCatalogTree;
    private readonly Dictionary<string, string> _viewCache = new(StringComparer.OrdinalIgnoreCase); // viewName -> SQL

    // Trigger catalog
    private BTree? _triggerCatalogTree;
    private readonly Dictionary<string, TriggerSchema> _triggerCache = new(StringComparer.OrdinalIgnoreCase);

    private SchemaCatalog(Pager pager)
    {
        _pager = pager;
    }

    public static async ValueTask<SchemaCatalog> CreateAsync(Pager pager, CancellationToken ct = default)
    {
        var catalog = new SchemaCatalog(pager);

        if (pager.SchemaRootPage != PageConstants.NullPageId)
        {
            catalog._catalogTree = new BTree(pager, pager.SchemaRootPage);
            await catalog.LoadAllAsync(ct);
        }

        return catalog;
    }

    private async ValueTask EnsureCatalogTreeAsync(CancellationToken ct = default)
    {
        if (_catalogTree != null) return;

        uint rootPage = await BTree.CreateNewAsync(_pager, ct);
        _pager.SchemaRootPage = rootPage;
        _catalogTree = new BTree(_pager, rootPage);
    }

    private async ValueTask EnsureIndexCatalogTreeAsync(CancellationToken ct = default)
    {
        if (_indexCatalogTree != null) return;

        await EnsureCatalogTreeAsync(ct);
        uint rootPage = await BTree.CreateNewAsync(_pager, ct);
        _indexCatalogTree = new BTree(_pager, rootPage);

        // Store the root page as sentinel in the main catalog
        var payload = new byte[4];
        BitConverter.TryWriteBytes(payload, rootPage);
        // Delete existing sentinel if any, then insert
        try { await _catalogTree!.DeleteAsync(IndexCatalogSentinel, ct); } catch { }
        await _catalogTree!.InsertAsync(IndexCatalogSentinel, payload, ct);
    }

    private async ValueTask EnsureViewCatalogTreeAsync(CancellationToken ct = default)
    {
        if (_viewCatalogTree != null) return;

        await EnsureCatalogTreeAsync(ct);
        uint rootPage = await BTree.CreateNewAsync(_pager, ct);
        _viewCatalogTree = new BTree(_pager, rootPage);

        // Store the root page as sentinel in the main catalog
        var payload = new byte[4];
        BitConverter.TryWriteBytes(payload, rootPage);
        try { await _catalogTree!.DeleteAsync(ViewCatalogSentinel, ct); } catch { }
        await _catalogTree!.InsertAsync(ViewCatalogSentinel, payload, ct);
    }

    private async ValueTask EnsureTriggerCatalogTreeAsync(CancellationToken ct = default)
    {
        if (_triggerCatalogTree != null) return;

        await EnsureCatalogTreeAsync(ct);
        uint rootPage = await BTree.CreateNewAsync(_pager, ct);
        _triggerCatalogTree = new BTree(_pager, rootPage);

        var payload = new byte[4];
        BitConverter.TryWriteBytes(payload, rootPage);
        try { await _catalogTree!.DeleteAsync(TriggerCatalogSentinel, ct); } catch { }
        await _catalogTree!.InsertAsync(TriggerCatalogSentinel, payload, ct);
    }

    private async ValueTask LoadAllAsync(CancellationToken ct = default)
    {
        var cursor = _catalogTree!.CreateCursor();
        while (await cursor.MoveNextAsync(ct))
        {
            if (cursor.CurrentKey == IndexCatalogSentinel)
            {
                uint indexRootPage = BinaryPrimitives.ReadUInt32LittleEndian(cursor.CurrentValue.Span);
                _indexCatalogTree = new BTree(_pager, indexRootPage);
                continue;
            }

            if (cursor.CurrentKey == ViewCatalogSentinel)
            {
                uint viewRootPage = BinaryPrimitives.ReadUInt32LittleEndian(cursor.CurrentValue.Span);
                _viewCatalogTree = new BTree(_pager, viewRootPage);
                continue;
            }

            if (cursor.CurrentKey == TriggerCatalogSentinel)
            {
                uint triggerRootPage = BinaryPrimitives.ReadUInt32LittleEndian(cursor.CurrentValue.Span);
                _triggerCatalogTree = new BTree(_pager, triggerRootPage);
                continue;
            }

            var data = cursor.CurrentValue;
            // Data format: [4 bytes root page ID] [schema bytes]
            uint rootPage = BinaryPrimitives.ReadUInt32LittleEndian(data.Span);
            var schema = SchemaSerializer.Deserialize(data.Span[4..]);
            _cache[schema.TableName] = schema;
            _tableRootPages[schema.TableName] = rootPage;
        }

        // Load index entries
        if (_indexCatalogTree != null)
        {
            var idxCursor = _indexCatalogTree.CreateCursor();
            while (await idxCursor.MoveNextAsync(ct))
            {
                var data = idxCursor.CurrentValue;
                uint rootPage = BinaryPrimitives.ReadUInt32LittleEndian(data.Span);
                var indexSchema = SchemaSerializer.DeserializeIndex(data.Span[4..]);
                _indexCache[indexSchema.IndexName] = indexSchema;
                _indexRootPages[indexSchema.IndexName] = rootPage;
                AddIndexToTableCache(indexSchema);
            }
        }

        // Load view entries
        if (_viewCatalogTree != null)
        {
            var viewCursor = _viewCatalogTree.CreateCursor();
            while (await viewCursor.MoveNextAsync(ct))
            {
                var data = viewCursor.CurrentValue;
                string viewName = ReadLengthPrefixedString(data.Span, 0, out int offset);
                string sql = ReadLengthPrefixedString(data.Span, offset, out _);
                _viewCache[viewName] = sql;
            }
        }

        // Load trigger entries
        if (_triggerCatalogTree != null)
        {
            var trigCursor = _triggerCatalogTree.CreateCursor();
            while (await trigCursor.MoveNextAsync(ct))
            {
                var triggerSchema = SchemaSerializer.DeserializeTrigger(trigCursor.CurrentValue.Span);
                _triggerCache[triggerSchema.TriggerName] = triggerSchema;
            }
        }
    }

    // ============ TABLE operations ============

    public TableSchema? GetTable(string tableName)
    {
        _cache.TryGetValue(tableName, out var schema);
        return schema;
    }

    public uint GetTableRootPage(string tableName)
    {
        if (_tableRootPages.TryGetValue(tableName, out var rootPage))
            return rootPage;
        throw new CSharpDbException(ErrorCode.TableNotFound, $"Table '{tableName}' not found.");
    }

    public IReadOnlyCollection<string> GetTableNames() => _cache.Keys;

    /// <summary>
    /// Persist table/index root page changes caused by B+tree root splits.
    /// Uses cached tree instances and only rewrites catalog entries when a root ID changed.
    /// </summary>
    public async ValueTask PersistRootPageChangesAsync(string tableName, CancellationToken ct = default)
    {
        await PersistTableRootPageChangeAsync(tableName, ct);

        foreach (var idx in GetIndexesForTable(tableName))
            await PersistIndexRootPageChangeAsync(idx.IndexName, ct);
    }

    public async ValueTask CreateTableAsync(TableSchema schema, CancellationToken ct = default)
    {
        if (_cache.ContainsKey(schema.TableName))
            throw new CSharpDbException(ErrorCode.TableAlreadyExists, $"Table '{schema.TableName}' already exists.");

        await EnsureCatalogTreeAsync(ct);

        // Create a new B+tree for the table's data
        uint tableRootPage = await BTree.CreateNewAsync(_pager, ct);

        // Serialize: [rootPage:4 bytes] [schema bytes]
        var schemaBytes = SchemaSerializer.Serialize(schema);
        var payload = new byte[4 + schemaBytes.Length];
        BitConverter.TryWriteBytes(payload, tableRootPage);
        schemaBytes.CopyTo(payload.AsSpan(4));

        long key = SchemaSerializer.TableNameToKey(schema.TableName);
        await _catalogTree!.InsertAsync(key, payload, ct);
        _pager.SchemaRootPage = _catalogTree.RootPageId;

        _cache[schema.TableName] = schema;
        _tableRootPages[schema.TableName] = tableRootPage;
        _tableTrees[schema.TableName] = new BTree(_pager, tableRootPage);
    }

    public async ValueTask DropTableAsync(string tableName, CancellationToken ct = default)
    {
        if (!_cache.ContainsKey(tableName))
            throw new CSharpDbException(ErrorCode.TableNotFound, $"Table '{tableName}' not found.");

        // Also drop all indexes on this table
        var indexesToDrop = GetIndexesForTable(tableName);
        foreach (var idx in indexesToDrop)
            await DropIndexAsync(idx.IndexName, ct);

        long key = SchemaSerializer.TableNameToKey(tableName);
        await _catalogTree!.DeleteAsync(key, ct);
        _pager.SchemaRootPage = _catalogTree.RootPageId;

        _cache.Remove(tableName);
        _tableRootPages.Remove(tableName);
        _tableTrees.Remove(tableName);
    }

    /// <summary>
    /// Updates the schema for an existing table while keeping the same data root page.
    /// Used by ALTER TABLE operations.
    /// </summary>
    public async ValueTask UpdateTableSchemaAsync(string oldTableName, TableSchema newSchema, CancellationToken ct = default)
    {
        if (!_tableRootPages.TryGetValue(oldTableName, out uint rootPage))
            throw new CSharpDbException(ErrorCode.TableNotFound, $"Table '{oldTableName}' not found.");

        // Delete old catalog entry
        long oldKey = SchemaSerializer.TableNameToKey(oldTableName);
        await _catalogTree!.DeleteAsync(oldKey, ct);
        _cache.Remove(oldTableName);
        _tableRootPages.Remove(oldTableName);

        // Insert new catalog entry with same root page
        var schemaBytes = SchemaSerializer.Serialize(newSchema);
        var payload = new byte[4 + schemaBytes.Length];
        BitConverter.TryWriteBytes(payload, rootPage);
        schemaBytes.CopyTo(payload.AsSpan(4));

        long newKey = SchemaSerializer.TableNameToKey(newSchema.TableName);
        await _catalogTree!.InsertAsync(newKey, payload, ct);
        _pager.SchemaRootPage = _catalogTree.RootPageId;

        _cache[newSchema.TableName] = newSchema;
        _tableRootPages[newSchema.TableName] = rootPage;

        if (_tableTrees.Remove(oldTableName, out var existingTree))
            _tableTrees[newSchema.TableName] = existingTree;
    }

    /// <summary>
    /// Get the B+tree for a table's data.
    /// </summary>
    public BTree GetTableTree(string tableName)
    {
        if (_tableTrees.TryGetValue(tableName, out var tree))
            return tree;

        uint rootPage = GetTableRootPage(tableName);
        tree = new BTree(_pager, rootPage);
        _tableTrees[tableName] = tree;
        return tree;
    }

    /// <summary>
    /// Get the B+tree for a table's data, using a specified pager.
    /// Used by snapshot readers to route reads through a snapshot pager.
    /// </summary>
    public BTree GetTableTree(string tableName, Pager pager)
    {
        if (ReferenceEquals(pager, _pager))
            return GetTableTree(tableName);

        uint rootPage = GetTableRootPage(tableName);
        return new BTree(pager, rootPage);
    }

    // ============ INDEX operations ============

    public IndexSchema? GetIndex(string indexName)
    {
        _indexCache.TryGetValue(indexName, out var schema);
        return schema;
    }

    public IReadOnlyCollection<IndexSchema> GetIndexes() => _indexCache.Values.ToArray();

    public IReadOnlyList<IndexSchema> GetIndexesForTable(string tableName)
    {
        if (_indexesByTable.TryGetValue(tableName, out var indexes))
            return indexes;

        return Array.Empty<IndexSchema>();
    }

    public BTree GetIndexTree(string indexName)
    {
        if (_indexTrees.TryGetValue(indexName, out var tree))
            return tree;

        if (_indexRootPages.TryGetValue(indexName, out uint rootPage))
        {
            tree = new BTree(_pager, rootPage);
            _indexTrees[indexName] = tree;
            return tree;
        }

        throw new CSharpDbException(ErrorCode.TableNotFound, $"Index '{indexName}' not found.");
    }

    /// <summary>
    /// Get the B+tree for an index, using a specified pager.
    /// Used by snapshot readers to route reads through a snapshot pager.
    /// </summary>
    public BTree GetIndexTree(string indexName, Pager pager)
    {
        if (ReferenceEquals(pager, _pager))
            return GetIndexTree(indexName);

        if (_indexRootPages.TryGetValue(indexName, out uint rootPage))
            return new BTree(pager, rootPage);
        throw new CSharpDbException(ErrorCode.TableNotFound, $"Index '{indexName}' not found.");
    }

    public async ValueTask CreateIndexAsync(IndexSchema schema, CancellationToken ct = default)
    {
        if (_indexCache.ContainsKey(schema.IndexName))
            throw new CSharpDbException(ErrorCode.TableAlreadyExists, $"Index '{schema.IndexName}' already exists.");

        await EnsureIndexCatalogTreeAsync(ct);

        // Create a new B+tree for the index data
        uint indexRootPage = await BTree.CreateNewAsync(_pager, ct);

        // Serialize: [rootPage:4 bytes] [index schema bytes]
        var indexBytes = SchemaSerializer.SerializeIndex(schema);
        var payload = new byte[4 + indexBytes.Length];
        BitConverter.TryWriteBytes(payload, indexRootPage);
        indexBytes.CopyTo(payload.AsSpan(4));

        long key = SchemaSerializer.IndexNameToKey(schema.IndexName);
        await _indexCatalogTree!.InsertAsync(key, payload, ct);

        _indexCache[schema.IndexName] = schema;
        _indexRootPages[schema.IndexName] = indexRootPage;
        _indexTrees[schema.IndexName] = new BTree(_pager, indexRootPage);
        AddIndexToTableCache(schema);
    }

    public async ValueTask DropIndexAsync(string indexName, CancellationToken ct = default)
    {
        if (!_indexCache.TryGetValue(indexName, out var schema))
            throw new CSharpDbException(ErrorCode.TableNotFound, $"Index '{indexName}' not found.");

        long key = SchemaSerializer.IndexNameToKey(indexName);
        await _indexCatalogTree!.DeleteAsync(key, ct);

        _indexCache.Remove(indexName);
        _indexRootPages.Remove(indexName);
        _indexTrees.Remove(indexName);
        RemoveIndexFromTableCache(schema);
    }

    // ============ VIEW operations ============

    public string? GetViewSql(string viewName)
    {
        _viewCache.TryGetValue(viewName, out var sql);
        return sql;
    }

    public IReadOnlyCollection<string> GetViewNames() => _viewCache.Keys.ToArray();

    public bool IsView(string name) => _viewCache.ContainsKey(name);

    public async ValueTask CreateViewAsync(string viewName, string sql, CancellationToken ct = default)
    {
        if (_viewCache.ContainsKey(viewName))
            throw new CSharpDbException(ErrorCode.TableAlreadyExists, $"View '{viewName}' already exists.");

        // Views must not conflict with table names
        if (_cache.ContainsKey(viewName))
            throw new CSharpDbException(ErrorCode.TableAlreadyExists, $"A table named '{viewName}' already exists.");

        await EnsureViewCatalogTreeAsync(ct);

        // Serialize: [nameLen:4][nameUtf8][sqlLen:4][sqlUtf8]
        var payload = WriteLengthPrefixedStrings(viewName, sql);
        long key = SchemaSerializer.ViewNameToKey(viewName);
        await _viewCatalogTree!.InsertAsync(key, payload, ct);

        _viewCache[viewName] = sql;
    }

    public async ValueTask DropViewAsync(string viewName, CancellationToken ct = default)
    {
        if (!_viewCache.ContainsKey(viewName))
            throw new CSharpDbException(ErrorCode.TableNotFound, $"View '{viewName}' not found.");

        long key = SchemaSerializer.ViewNameToKey(viewName);
        await _viewCatalogTree!.DeleteAsync(key, ct);

        _viewCache.Remove(viewName);
    }

    // ============ TRIGGER operations ============

    public TriggerSchema? GetTrigger(string triggerName)
    {
        _triggerCache.TryGetValue(triggerName, out var schema);
        return schema;
    }

    public IReadOnlyCollection<TriggerSchema> GetTriggers() => _triggerCache.Values.ToArray();

    public IReadOnlyList<TriggerSchema> GetTriggersForTable(string tableName)
    {
        var result = new List<TriggerSchema>();
        foreach (var trig in _triggerCache.Values)
        {
            if (string.Equals(trig.TableName, tableName, StringComparison.OrdinalIgnoreCase))
                result.Add(trig);
        }
        return result;
    }

    public async ValueTask CreateTriggerAsync(TriggerSchema schema, CancellationToken ct = default)
    {
        if (_triggerCache.ContainsKey(schema.TriggerName))
            throw new CSharpDbException(ErrorCode.TriggerAlreadyExists, $"Trigger '{schema.TriggerName}' already exists.");

        await EnsureTriggerCatalogTreeAsync(ct);

        var payload = SchemaSerializer.SerializeTrigger(schema);
        long key = SchemaSerializer.TriggerNameToKey(schema.TriggerName);
        await _triggerCatalogTree!.InsertAsync(key, payload, ct);

        _triggerCache[schema.TriggerName] = schema;
    }

    public async ValueTask DropTriggerAsync(string triggerName, CancellationToken ct = default)
    {
        if (!_triggerCache.ContainsKey(triggerName))
            throw new CSharpDbException(ErrorCode.TriggerNotFound, $"Trigger '{triggerName}' not found.");

        long key = SchemaSerializer.TriggerNameToKey(triggerName);
        await _triggerCatalogTree!.DeleteAsync(key, ct);

        _triggerCache.Remove(triggerName);
    }

    // ============ Helpers ============

    private static byte[] WriteLengthPrefixedStrings(string s1, string s2)
    {
        var b1 = Encoding.UTF8.GetBytes(s1);
        var b2 = Encoding.UTF8.GetBytes(s2);
        var result = new byte[4 + b1.Length + 4 + b2.Length];
        BitConverter.TryWriteBytes(result.AsSpan(0), b1.Length);
        b1.CopyTo(result.AsSpan(4));
        BitConverter.TryWriteBytes(result.AsSpan(4 + b1.Length), b2.Length);
        b2.CopyTo(result.AsSpan(4 + b1.Length + 4));
        return result;
    }

    private static string ReadLengthPrefixedString(ReadOnlySpan<byte> data, int pos, out int newPos)
    {
        int len = BitConverter.ToInt32(data[pos..(pos + 4)]);
        string s = Encoding.UTF8.GetString(data[(pos + 4)..(pos + 4 + len)]);
        newPos = pos + 4 + len;
        return s;
    }

    private void AddIndexToTableCache(IndexSchema schema)
    {
        if (_indexesByTable.TryGetValue(schema.TableName, out var existing))
        {
            var updated = new IndexSchema[existing.Length + 1];
            Array.Copy(existing, updated, existing.Length);
            updated[^1] = schema;
            _indexesByTable[schema.TableName] = updated;
            return;
        }

        _indexesByTable[schema.TableName] = new[] { schema };
    }

    private void RemoveIndexFromTableCache(IndexSchema schema)
    {
        if (!_indexesByTable.TryGetValue(schema.TableName, out var existing))
            return;

        if (existing.Length == 1)
        {
            _indexesByTable.Remove(schema.TableName);
            return;
        }

        int removeAt = -1;
        for (int i = 0; i < existing.Length; i++)
        {
            if (string.Equals(existing[i].IndexName, schema.IndexName, StringComparison.OrdinalIgnoreCase))
            {
                removeAt = i;
                break;
            }
        }

        if (removeAt < 0)
            return;

        var updated = new IndexSchema[existing.Length - 1];
        if (removeAt > 0)
            Array.Copy(existing, 0, updated, 0, removeAt);
        if (removeAt < existing.Length - 1)
            Array.Copy(existing, removeAt + 1, updated, removeAt, existing.Length - removeAt - 1);
        _indexesByTable[schema.TableName] = updated;
    }

    private async ValueTask PersistTableRootPageChangeAsync(string tableName, CancellationToken ct)
    {
        if (!_tableTrees.TryGetValue(tableName, out var tree))
            return;

        if (!_tableRootPages.TryGetValue(tableName, out uint persistedRootPage))
            return;

        uint currentRootPage = tree.RootPageId;
        if (currentRootPage == persistedRootPage)
            return;

        var schema = _cache[tableName];
        var schemaBytes = SchemaSerializer.Serialize(schema);
        var payload = new byte[4 + schemaBytes.Length];
        BitConverter.TryWriteBytes(payload, currentRootPage);
        schemaBytes.CopyTo(payload.AsSpan(4));

        long key = SchemaSerializer.TableNameToKey(tableName);
        await _catalogTree!.DeleteAsync(key, ct);
        await _catalogTree.InsertAsync(key, payload, ct);

        _tableRootPages[tableName] = currentRootPage;
        _pager.SchemaRootPage = _catalogTree.RootPageId;
    }

    private async ValueTask PersistIndexRootPageChangeAsync(string indexName, CancellationToken ct)
    {
        if (!_indexTrees.TryGetValue(indexName, out var tree))
            return;

        if (!_indexRootPages.TryGetValue(indexName, out uint persistedRootPage))
            return;

        uint currentRootPage = tree.RootPageId;
        if (currentRootPage == persistedRootPage)
            return;

        var schema = _indexCache[indexName];
        var schemaBytes = SchemaSerializer.SerializeIndex(schema);
        var payload = new byte[4 + schemaBytes.Length];
        BitConverter.TryWriteBytes(payload, currentRootPage);
        schemaBytes.CopyTo(payload.AsSpan(4));

        long key = SchemaSerializer.IndexNameToKey(indexName);
        await _indexCatalogTree!.DeleteAsync(key, ct);
        await _indexCatalogTree.InsertAsync(key, payload, ct);

        _indexRootPages[indexName] = currentRootPage;
    }
}
