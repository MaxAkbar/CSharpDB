using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Primitives;

namespace CSharpDB.ImportExport.Serialization;

internal static class TableArchiveJson
{
    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = true,
        Converters =
        {
            new JsonStringEnumConverter<DbType>(JsonNamingPolicy.CamelCase, allowIntegerValues: false),
            new JsonStringEnumConverter<SqlTypeKind>(JsonNamingPolicy.CamelCase, allowIntegerValues: false),
            new JsonStringEnumConverter<ForeignKeyOnDeleteAction>(JsonNamingPolicy.CamelCase, allowIntegerValues: false),
            new JsonStringEnumConverter<KeyConstraintKind>(JsonNamingPolicy.CamelCase, allowIntegerValues: false),
        },
    };

    public static TableArchiveJsonContext Context { get; } = new(Options);
}
