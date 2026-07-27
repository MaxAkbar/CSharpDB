using System.Globalization;

namespace CSharpDB.Migration.Files.Json;

/// <summary>Stable object identifiers emitted by the JSON table adapter.</summary>
public static class JsonMigrationObjectIds
{
    public const string MainNamespace = "json:namespace:main";

    public const string Table = "json:table:0";

    public static string Column(int columnIndex)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(columnIndex);
        return "json:column:" +
            columnIndex.ToString(CultureInfo.InvariantCulture);
    }

    internal static bool TryParseColumn(
        string objectId,
        out int columnIndex)
    {
        const string prefix = "json:column:";
        columnIndex = -1;
        if (objectId is null ||
            !objectId.StartsWith(prefix, StringComparison.Ordinal) ||
            !int.TryParse(
                objectId.AsSpan(prefix.Length),
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out int parsed) ||
            parsed < 0 ||
            !string.Equals(
                objectId,
                Column(parsed),
                StringComparison.Ordinal))
        {
            return false;
        }

        columnIndex = parsed;
        return true;
    }
}
