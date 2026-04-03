namespace CSharpDB.Admin.Forms.Models;

public sealed record FormForeignKeyDefinition(
    string Name,
    IReadOnlyList<string> LocalFields,
    string ReferencedTable,
    IReadOnlyList<string> ReferencedFields);
