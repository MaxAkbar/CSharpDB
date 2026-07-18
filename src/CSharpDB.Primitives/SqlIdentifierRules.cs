namespace CSharpDB.Primitives;

/// <summary>
/// Identifier contract for CSharpDB SQL.
/// Unquoted identifiers are compared case-insensitively by catalogs. Double
/// quotes delimit identifiers and an embedded double quote is escaped as "".
/// </summary>
public static class SqlIdentifierRules
{
    public const int MaxLength = 128;

    public static void Validate(string identifier, string description = "Identifier")
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(identifier);

        if (identifier.Length > MaxLength)
        {
            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                $"{description} exceeds the maximum length of {MaxLength} characters.");
        }

        if (identifier.IndexOf('\0') >= 0)
        {
            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                $"{description} cannot contain a NUL character.");
        }
    }

    public static string Escape(string identifier)
    {
        Validate(identifier);
        return identifier.Replace("\"", "\"\"", StringComparison.Ordinal);
    }

    public static string Quote(string identifier) => $"\"{Escape(identifier)}\"";
}
