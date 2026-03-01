namespace CSharpDB.Storage.Serialization;

/// <summary>
/// Default serializer provider that keeps current binary formats unchanged.
/// </summary>
public sealed class DefaultSerializerProvider : ISerializerProvider
{
    public IRecordSerializer RecordSerializer { get; init; } = new DefaultRecordSerializer();

    public ISchemaSerializer SchemaSerializer { get; init; } = new DefaultSchemaSerializer();
}
