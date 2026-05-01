namespace CSharpDB.Admin.Forms.Models;

public sealed record FormAttachmentValue(
    byte[]? Bytes,
    string? FileName,
    string? ContentType,
    long? FileSize,
    bool ClearExisting)
{
    public static string GetRecordKey(string controlId)
        => $"__formAttachment:{controlId}";

    public static FormAttachmentValue FromFile(byte[] bytes, string fileName, string? contentType, long fileSize)
        => new(bytes, fileName, contentType, fileSize, ClearExisting: false);

    public static FormAttachmentValue Clear()
        => new(null, null, null, null, ClearExisting: true);
}
