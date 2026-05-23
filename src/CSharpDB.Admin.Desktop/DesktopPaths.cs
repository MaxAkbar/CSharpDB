using System;
using System.IO;

namespace CSharpDB.Admin.Desktop;

internal static class DesktopPaths
{
    private const string VendorFolder = "CSharpDB";
    private const string AppFolder = "Studio";

    public static string BaseDirectory { get; } = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
        VendorFolder,
        AppFolder);

    public static string DataDirectory { get; } = Path.Combine(BaseDirectory, "Data");

    public static string LogDirectory { get; } = Path.Combine(BaseDirectory, "Logs");

    public static string WebViewDataDirectory { get; } = Path.Combine(BaseDirectory, "WebView2");

    public static string DefaultDatabasePath { get; } = Path.Combine(DataDirectory, "csharpdb-studio.db");

    public static void EnsureCreated()
    {
        Directory.CreateDirectory(DataDirectory);
        Directory.CreateDirectory(LogDirectory);
        Directory.CreateDirectory(WebViewDataDirectory);
    }
}
