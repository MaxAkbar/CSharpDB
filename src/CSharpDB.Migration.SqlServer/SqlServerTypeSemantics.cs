namespace CSharpDB.Migration.SqlServer;

/// <summary>
/// Provider type facts shared by live catalog inspection and standalone
/// ScriptDom lowering. This class describes SQL Server source semantics only;
/// target representation remains owned by the standard migration type mapper.
/// </summary>
internal static class SqlServerTypeSemantics
{
    internal static string LogicalType(string systemTypeName) =>
        systemTypeName.ToLowerInvariant() switch
        {
            "bigint" or "int" or "smallint" or "tinyint" =>
                "signedInteger",
            "bit" => "boolean",
            "decimal" or "numeric" or "money" or "smallmoney" =>
                "decimal",
            "float" or "real" => "floatingPoint",
            "char" or "varchar" or "nchar" or "nvarchar" or "text" or
                "ntext" or "sysname" => "text",
            "binary" or "varbinary" or "image" => "binary",
            "uniqueidentifier" => "guid",
            "date" => "date",
            "time" => "time",
            "datetime" or "datetime2" or "smalldatetime" => "dateTime",
            "datetimeoffset" => "dateTimeOffset",
            "timestamp" or "rowversion" => "rowVersion",
            "json" => "json",
            _ => "native",
        };

    internal static bool IsRowVersion(string systemTypeName) =>
        systemTypeName.Equals(
            "timestamp",
            StringComparison.OrdinalIgnoreCase) ||
        systemTypeName.Equals(
            "rowversion",
            StringComparison.OrdinalIgnoreCase);
}
