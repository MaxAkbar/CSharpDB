using CSharpDB.Core;

namespace CSharpDB.Storage.Serialization;

/// <summary>
/// Adapter that preserves current static SchemaSerializer behavior.
/// </summary>
public sealed class DefaultSchemaSerializer : ISchemaSerializer
{
    public byte[] Serialize(TableSchema schema) => SchemaSerializer.Serialize(schema);

    public TableSchema Deserialize(ReadOnlySpan<byte> data) => SchemaSerializer.Deserialize(data);

    public byte[] SerializeIndex(IndexSchema index) => SchemaSerializer.SerializeIndex(index);

    public IndexSchema DeserializeIndex(ReadOnlySpan<byte> data) => SchemaSerializer.DeserializeIndex(data);

    public long TableNameToKey(string tableName) => SchemaSerializer.TableNameToKey(tableName);

    public long IndexNameToKey(string indexName) => SchemaSerializer.IndexNameToKey(indexName);

    public long ViewNameToKey(string viewName) => SchemaSerializer.ViewNameToKey(viewName);

    public long TriggerNameToKey(string triggerName) => SchemaSerializer.TriggerNameToKey(triggerName);

    public byte[] SerializeTrigger(TriggerSchema trigger) => SchemaSerializer.SerializeTrigger(trigger);

    public TriggerSchema DeserializeTrigger(ReadOnlySpan<byte> data) => SchemaSerializer.DeserializeTrigger(data);
}
