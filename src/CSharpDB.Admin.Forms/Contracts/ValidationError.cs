namespace CSharpDB.Admin.Forms.Contracts;

public sealed record ValidationError(string FieldName, string RuleId, string Message);
