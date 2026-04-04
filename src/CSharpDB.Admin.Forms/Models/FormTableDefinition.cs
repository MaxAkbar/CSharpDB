namespace CSharpDB.Admin.Forms.Models;

public sealed record FormTableDefinition(
    string TableName,
    string SourceSchemaSignature,
    IReadOnlyList<FormFieldDefinition> Fields,
    IReadOnlyList<string> PrimaryKey,
    IReadOnlyList<FormForeignKeyDefinition> ForeignKeys,
    FormSourceKind SourceKind = FormSourceKind.Table,
    IReadOnlyDictionary<string, object?>? Metadata = null)
{
    public bool HasSinglePrimaryKey => PrimaryKey.Count == 1;

    public bool SupportsWriteOperations => SourceKind == FormSourceKind.Table;
}
