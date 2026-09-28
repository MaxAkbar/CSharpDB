using System.Collections.Frozen;
using CSharpDB.Sql;

namespace CSharpDB.Admin.Helpers;

/// <summary>
/// Shared editor vocabulary. Reserved spellings follow the tokenizer; contextual
/// words remain identifiers in the grammar and are listed here for editor use.
/// </summary>
internal static class SqlKeywordCatalog
{
    private static readonly string[] ContextualKeywords =
    [
        // Supported expressions, constraints, ALTER TABLE clauses and windows.
        "ALL", "CAST", "CHECK", "DEFAULT", "MATCH", "SIMPLE", "RESTRICT",
        "NO", "ACTION", "WHEN", "COLLATION", "TYPE", "RESEED",
        "WINDOW", "OVER", "PARTITION", "ROWS", "UNBOUNDED", "PRECEDING",
        "FOLLOWING", "CURRENT",

        // Type names and qualifiers accepted by ParseSqlTypeDescriptor.
        "BOOL", "BOOLEAN", "TINYINT", "SMALLINT", "BIGINT", "DECIMAL", "NUMERIC",
        "CHAR", "CHARACTER", "NVARCHAR", "NCHAR", "CLOB", "BINARY", "VARBINARY",
        "UUID", "GUID", "UNIQUEIDENTIFIER", "DATE", "TIME", "TIMESTAMP",
        "DATETIME", "DATETIME2", "DATETIMEOFFSET", "ROWVERSION", "INTERVAL",
        "JSON", "XML", "BIT", "VARBIT", "PRECISION", "VARYING", "ZONE",
        "YEAR", "MONTH", "DAY", "SECOND",

        // Procedure commands handled by the Admin query tab before native SQL.
        "EXEC", "EXECUTE",
    ];

    public static IReadOnlySet<string> CompletionKeywords { get; } =
        Tokenizer.ReservedKeywords
            // WITH RECURSIVE is recognized only to report an unsupported feature.
            .Where(static keyword => keyword != "RECURSIVE")
            .Concat(ContextualKeywords)
            .ToFrozenSet(StringComparer.OrdinalIgnoreCase);

    public static IReadOnlySet<string> Keywords { get; } =
        Tokenizer.ReservedKeywords
            .Concat(ContextualKeywords)
            // Preserve existing highlighting/formatting of legacy SQL vocabulary
            // without advertising these unsupported native constructs in completion.
            .Concat(["COMMIT", "ROLLBACK", "TRANSACTION", "ELSE", "CASE", "THEN",
                "INSTEAD", "OF", "TRUE", "FALSE"])
            .ToFrozenSet(StringComparer.OrdinalIgnoreCase);
}
