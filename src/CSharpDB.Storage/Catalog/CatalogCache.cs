using CSharpDB.Primitives;

namespace CSharpDB.Storage.Catalog;

/// <summary>
/// In-memory cache/projection for catalog state.
/// </summary>
internal sealed class CatalogCache
{
    public Dictionary<string, TableSchema> Tables { get; } = new(StringComparer.OrdinalIgnoreCase);
    public Dictionary<string, uint> TableRootPages { get; } = new(StringComparer.OrdinalIgnoreCase);
    public Dictionary<string, BTree> TableTrees { get; } = new(StringComparer.OrdinalIgnoreCase);
    public Dictionary<string, ForeignKeyDefinition[]> ForeignKeysByTable { get; } = new(StringComparer.OrdinalIgnoreCase);
    public Dictionary<string, TableForeignKeyReference[]> ReferencingForeignKeysByParentTable { get; } = new(StringComparer.OrdinalIgnoreCase);

    public Dictionary<string, IndexSchema> Indexes { get; } = new(StringComparer.OrdinalIgnoreCase);
    public Dictionary<string, uint> IndexRootPages { get; } = new(StringComparer.OrdinalIgnoreCase);
    public Dictionary<string, IIndexStore> IndexStores { get; } = new(StringComparer.OrdinalIgnoreCase);
    public Dictionary<string, IndexSchema[]> IndexesByTable { get; } = new(StringComparer.OrdinalIgnoreCase);

    public Dictionary<string, string> Views { get; } = new(StringComparer.OrdinalIgnoreCase);
    public Dictionary<string, TriggerSchema> Triggers { get; } = new(StringComparer.OrdinalIgnoreCase);
    public Dictionary<string, TriggerSchema[]> TriggersByTable { get; } = new(StringComparer.OrdinalIgnoreCase);

    public void AddIndexToTable(IndexSchema schema)
    {
        if (IndexesByTable.TryGetValue(schema.TableName, out var existing))
        {
            var updated = new IndexSchema[existing.Length + 1];
            Array.Copy(existing, updated, existing.Length);
            updated[^1] = schema;
            IndexesByTable[schema.TableName] = updated;
            return;
        }

        IndexesByTable[schema.TableName] = new[] { schema };
    }

    public void RemoveIndexFromTable(IndexSchema schema)
    {
        if (!IndexesByTable.TryGetValue(schema.TableName, out var existing))
            return;

        if (existing.Length == 1)
        {
            IndexesByTable.Remove(schema.TableName);
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
        IndexesByTable[schema.TableName] = updated;
    }

    public void AddTriggerToTable(TriggerSchema schema)
    {
        if (TriggersByTable.TryGetValue(schema.TableName, out var existing))
        {
            var updated = new TriggerSchema[existing.Length + 1];
            Array.Copy(existing, updated, existing.Length);
            updated[^1] = schema;
            TriggersByTable[schema.TableName] = updated;
            return;
        }

        TriggersByTable[schema.TableName] = new[] { schema };
    }

    public void RemoveTriggerFromTable(TriggerSchema schema)
    {
        if (!TriggersByTable.TryGetValue(schema.TableName, out var existing))
            return;

        if (existing.Length == 1)
        {
            TriggersByTable.Remove(schema.TableName);
            return;
        }

        int removeAt = -1;
        for (int i = 0; i < existing.Length; i++)
        {
            if (string.Equals(existing[i].TriggerName, schema.TriggerName, StringComparison.OrdinalIgnoreCase))
            {
                removeAt = i;
                break;
            }
        }

        if (removeAt < 0)
            return;

        var updated = new TriggerSchema[existing.Length - 1];
        if (removeAt > 0)
            Array.Copy(existing, 0, updated, 0, removeAt);
        if (removeAt < existing.Length - 1)
            Array.Copy(existing, removeAt + 1, updated, removeAt, existing.Length - removeAt - 1);
        TriggersByTable[schema.TableName] = updated;
    }
}
