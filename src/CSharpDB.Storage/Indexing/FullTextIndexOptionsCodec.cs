using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Primitives;

namespace CSharpDB.Storage.Indexing;

internal static class FullTextIndexOptionsCodec
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private static readonly FullTextIndexOptionsJsonContext JsonContext = new(SerializerOptions);

    public static string Serialize(FullTextIndexOptions? options)
        => JsonSerializer.Serialize(options ?? new FullTextIndexOptions(), JsonContext.FullTextIndexOptions);

    public static FullTextIndexOptions Deserialize(string? payload)
    {
        if (string.IsNullOrWhiteSpace(payload))
            return new FullTextIndexOptions();

        try
        {
            return JsonSerializer.Deserialize(payload, JsonContext.FullTextIndexOptions)
                ?? new FullTextIndexOptions();
        }
        catch (JsonException)
        {
            return new FullTextIndexOptions();
        }
    }
}

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(FullTextIndexOptions))]
internal sealed partial class FullTextIndexOptionsJsonContext : JsonSerializerContext
{
}
