using System.Text.Json.Serialization;

namespace CSharpDB.Engine;

[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    WriteIndented = true)]
[JsonSerializable(typeof(DatabaseBackupManifest))]
internal sealed partial class EngineJsonContext : JsonSerializerContext
{
}
