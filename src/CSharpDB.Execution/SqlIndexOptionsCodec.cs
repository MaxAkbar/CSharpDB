using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Primitives;

namespace CSharpDB.Execution;

internal enum SqlIndexStorageMode
{
    Hashed = 0,
    OrderedText = 1,
    HashedTrailingInteger = 2,
}

internal sealed class SqlIndexOptions
{
    public string? Storage { get; init; }
}

internal static class SqlIndexOptionsCodec
{
    private const string OrderedTextStorage = "ordered_text";
    private const string HashedTrailingIntegerStorage = "hashed_trailing_integer";

    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private static readonly SqlIndexOptionsJsonContext JsonContext = new(SerializerOptions);

    public static string? CreateDefaultOptionsJson(
        TableSchema schema,
        ReadOnlySpan<int> indexColumnIndices,
        ReadOnlySpan<string?> indexColumnCollations = default)
        => ShouldDefaultToOrderedTextStorage(schema, indexColumnIndices, indexColumnCollations)
            ? Serialize(new SqlIndexOptions { Storage = OrderedTextStorage })
            : ShouldDefaultToTrailingIntegerHashedStorage(schema, indexColumnIndices)
                ? Serialize(new SqlIndexOptions { Storage = HashedTrailingIntegerStorage })
                : null;

    public static SqlIndexStorageMode Resolve(IndexSchema index, TableSchema schema)
    {
        if (index.Kind != IndexKind.Sql)
            return SqlIndexStorageMode.Hashed;

        SqlIndexOptions options = Deserialize(index.OptionsJson);
        if (string.Equals(options.Storage, OrderedTextStorage, StringComparison.Ordinal))
        {
            if (index.Columns.Count != 1)
                return SqlIndexStorageMode.Hashed;

            int columnIndex = schema.GetColumnIndex(index.Columns[0]);
            return columnIndex >= 0 &&
                   columnIndex < schema.Columns.Count &&
                   schema.Columns[columnIndex].Type == DbType.Text
                ? SqlIndexStorageMode.OrderedText
                : SqlIndexStorageMode.Hashed;
        }

        if (string.Equals(options.Storage, HashedTrailingIntegerStorage, StringComparison.Ordinal))
        {
            if (index.Columns.Count <= 1)
                return SqlIndexStorageMode.Hashed;

            int trailingColumnIndex = schema.GetColumnIndex(index.Columns[^1]);
            return trailingColumnIndex >= 0 &&
                   trailingColumnIndex < schema.Columns.Count &&
                   schema.Columns[trailingColumnIndex].Type == DbType.Integer
                ? SqlIndexStorageMode.HashedTrailingInteger
                : SqlIndexStorageMode.Hashed;
        }

        return SqlIndexStorageMode.Hashed;
    }

    private static string Serialize(SqlIndexOptions options)
        => JsonSerializer.Serialize(options, JsonContext.SqlIndexOptions);

    private static bool ShouldDefaultToOrderedTextStorage(
        TableSchema schema,
        ReadOnlySpan<int> indexColumnIndices,
        ReadOnlySpan<string?> indexColumnCollations)
    {
        if (indexColumnIndices.Length != 1)
            return false;

        int columnIndex = indexColumnIndices[0];
        if (columnIndex < 0 ||
            columnIndex >= schema.Columns.Count ||
            schema.Columns[columnIndex].Type != DbType.Text)
        {
            return false;
        }

        string? effectiveCollation = indexColumnCollations.IsEmpty || indexColumnCollations[0] == null
            ? schema.Columns[columnIndex].Collation
            : indexColumnCollations[0];
        return !CollationSupport.IsBinaryOrDefault(effectiveCollation);
    }

    private static bool ShouldDefaultToTrailingIntegerHashedStorage(
        TableSchema schema,
        ReadOnlySpan<int> indexColumnIndices)
    {
        if (indexColumnIndices.Length <= 1)
            return false;

        int trailingColumnIndex = indexColumnIndices[^1];
        return trailingColumnIndex >= 0 &&
               trailingColumnIndex < schema.Columns.Count &&
               schema.Columns[trailingColumnIndex].Type == DbType.Integer;
    }

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
