using System.Globalization;
using System.Text;

namespace CSharpDB.Admin.Helpers;

internal static class AdminTabDomIds
{
    internal static string TabButtonId(string tabId)
        => "admin-tab-u" + Encode(tabId);

    internal static string PanelId(string tabId)
        => "admin-panel-u" + Encode(tabId);

    private static string Encode(string tabId)
    {
        ArgumentNullException.ThrowIfNull(tabId);

        var encoded = new StringBuilder(tabId.Length * 4);
        foreach (char character in tabId)
            encoded.Append(((int)character).ToString("x4", CultureInfo.InvariantCulture));

        return encoded.ToString();
    }
}
