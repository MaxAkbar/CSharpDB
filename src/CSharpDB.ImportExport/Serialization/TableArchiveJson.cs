using System.Text.Json;
using System.Text.Json.Serialization;

namespace CSharpDB.ImportExport.Serialization;

internal static class TableArchiveJson
{
    public static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = true,
        Converters =
        {
            new JsonStringEnumConverter(JsonNamingPolicy.CamelCase),
        },
    };
}
