namespace CSharpDB.Admin.Forms.Models;

public sealed record FormTableDefinition(
    string TableName,
    string SourceSchemaSignature,
    IReadOnlyList<FormFieldDefinition> Fields,
    IReadOnlyList<string> PrimaryKey,
    IReadOnlyList<FormForeignKeyDefinition> ForeignKeys,
    IReadOnlyDictionary<string, object?>? Metadata = null);
