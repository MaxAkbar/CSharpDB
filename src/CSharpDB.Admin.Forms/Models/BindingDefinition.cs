namespace CSharpDB.Admin.Forms.Models;

public sealed record BindingDefinition(
    string FieldName,
    string Mode,
    string? ConverterId = null,
    bool UpdateOnChange = true);
