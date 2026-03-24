using System.Text.Json;
using CSharpDB.Primitives;

namespace CSharpDB.Storage.Indexing;

internal static class FullTextIndexOptionsCodec
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    public static string Serialize(FullTextIndexOptions? options)
        => JsonSerializer.Serialize(options ?? new FullTextIndexOptions(), SerializerOptions);

    public static FullTextIndexOptions Deserialize(string? payload)
    {
        if (string.IsNullOrWhiteSpace(payload))
            return new FullTextIndexOptions();

        return JsonSerializer.Deserialize<FullTextIndexOptions>(payload, SerializerOptions)
            ?? new FullTextIndexOptions();
    }
}
