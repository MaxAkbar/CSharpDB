namespace CSharpDB.Admin.Forms.Models;

public sealed record FormAttachmentTableBinding(
    string TableName,
    string ForeignKeyField,
    string BlobField,
    string? FileNameField = null,
    string? ContentTypeField = null,
    string? FileSizeField = null,
    string? ControlIdField = null,
    string? ControlId = null);
