namespace CSharpDB.Storage.Serialization;

/// <summary>
/// Provides serializers used by storage/engine components.
/// </summary>
public interface ISerializerProvider
{
    IRecordSerializer RecordSerializer { get; }
    ISchemaSerializer SchemaSerializer { get; }
}
