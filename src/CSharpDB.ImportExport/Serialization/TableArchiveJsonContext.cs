using System.Text.Json.Serialization;
using CSharpDB.ImportExport.Models;

namespace CSharpDB.ImportExport.Serialization;

[JsonSourceGenerationOptions(GenerationMode = JsonSourceGenerationMode.Metadata)]
[JsonSerializable(typeof(TableArchiveSchema))]
[JsonSerializable(typeof(TableArchiveManifest))]
[JsonSerializable(typeof(TableArchiveSchemaDocument))]
[JsonSerializable(typeof(TableArchiveManifestDocument))]
internal partial class TableArchiveJsonContext : JsonSerializerContext
{
}
