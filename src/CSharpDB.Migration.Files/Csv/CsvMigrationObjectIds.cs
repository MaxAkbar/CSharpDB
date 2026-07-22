using System.Globalization;

namespace CSharpDB.Migration.Files.Csv;

/// <summary>Stable object identifiers emitted by the CSV migration adapter.</summary>
public static class CsvMigrationObjectIds
{
    public const string MainNamespace = "csv:namespace:main";

    public const string Table = "csv:table:0";

    public static string Column(int columnIndex)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(columnIndex);
        return "csv:column:" + columnIndex.ToString(CultureInfo.InvariantCulture);
    }

    internal static bool TryParseColumn(string objectId, out int columnIndex)
    {
        const string prefix = "csv:column:";
        columnIndex = -1;
        if (objectId is null ||
            !objectId.StartsWith(prefix, StringComparison.Ordinal) ||
            !int.TryParse(
                objectId.AsSpan(prefix.Length),
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out int parsed) ||
            parsed < 0 ||
            !string.Equals(objectId, Column(parsed), StringComparison.Ordinal))
        {
            return false;
        }

        columnIndex = parsed;
        return true;
    }
}
