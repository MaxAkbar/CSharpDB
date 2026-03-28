using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Primitives;

namespace CSharpDB.Execution;

internal enum SqlIndexStorageMode
{
    Hashed = 0,
    OrderedText = 1,
}

internal sealed class SqlIndexOptions
{
    public string? Storage { get; init; }
}

internal static class SqlIndexOptionsCodec
{
    private const string OrderedTextStorage = "ordered_text";

    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private static readonly SqlIndexOptionsJsonContext JsonContext = new(SerializerOptions);

    public static string? CreateDefaultOptionsJson(TableSchema schema, ReadOnlySpan<int> indexColumnIndices)
        => indexColumnIndices.Length == 1 &&
           indexColumnIndices[0] >= 0 &&
           indexColumnIndices[0] < schema.Columns.Count &&
           schema.Columns[indexColumnIndices[0]].Type == DbType.Text
            ? Serialize(new SqlIndexOptions { Storage = OrderedTextStorage })
            : null;

    public static SqlIndexStorageMode Resolve(IndexSchema index, TableSchema schema)
    {
        if (index.Kind != IndexKind.Sql ||
            index.Columns.Count != 1)
        {
            return SqlIndexStorageMode.Hashed;
        }

        int columnIndex = schema.GetColumnIndex(index.Columns[0]);
        if (columnIndex < 0 ||
            columnIndex >= schema.Columns.Count ||
            schema.Columns[columnIndex].Type != DbType.Text)
        {
            return SqlIndexStorageMode.Hashed;
        }

        SqlIndexOptions options = Deserialize(index.OptionsJson);
        return string.Equals(options.Storage, OrderedTextStorage, StringComparison.Ordinal)
            ? SqlIndexStorageMode.OrderedText
            : SqlIndexStorageMode.Hashed;
    }

    private static string Serialize(SqlIndexOptions options)
        => JsonSerializer.Serialize(options, JsonContext.SqlIndexOptions);

    private static SqlIndexOptions Deserialize(string? payload)
    {
        if (string.IsNullOrWhiteSpace(payload))
            return new SqlIndexOptions();

        try
        {
            return JsonSerializer.Deserialize(payload, JsonContext.SqlIndexOptions)
                ?? new SqlIndexOptions();
        }
        catch (JsonException)
        {
            return new SqlIndexOptions();
        }
    }
}

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(SqlIndexOptions))]
internal sealed partial class SqlIndexOptionsJsonContext : JsonSerializerContext
{
}
