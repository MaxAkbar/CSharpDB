using CSharpDB.Primitives;

namespace CSharpDB.Storage.Serialization;

/// <summary>
/// Abstraction over schema serialization and catalog key mapping.
/// </summary>
public interface ISchemaSerializer
{
    byte[] Serialize(TableSchema schema);
    TableSchema Deserialize(ReadOnlySpan<byte> data);

    byte[] SerializeIndex(IndexSchema index);
    IndexSchema DeserializeIndex(ReadOnlySpan<byte> data);

    long TableNameToKey(string tableName);
    long IndexNameToKey(string indexName);
    long ViewNameToKey(string viewName);
    long TriggerNameToKey(string triggerName);

    byte[] SerializeTrigger(TriggerSchema trigger);
    TriggerSchema DeserializeTrigger(ReadOnlySpan<byte> data);
}
