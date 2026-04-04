namespace CSharpDB.Admin.Forms.Models;

public sealed record FormFieldDefinition(
    string Name,
    FieldDataType DataType,
    bool IsNullable,
    bool IsReadOnly,
    string? DisplayName = null,
    string? Description = null,
    int? MaxLength = null,
    decimal? Min = null,
    decimal? Max = null,
    string? Regex = null,
    IReadOnlyList<EnumChoice>? Choices = null,
    IReadOnlyDictionary<string, object?>? Metadata = null);
