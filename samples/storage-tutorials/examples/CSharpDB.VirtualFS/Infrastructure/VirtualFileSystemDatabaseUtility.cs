internal static class VirtualFileSystemDatabaseUtility
{
    public static void DeleteDatabaseFiles(string databasePath)
    {
        var directory = Path.GetDirectoryName(databasePath);
        var rootDirectory = string.IsNullOrWhiteSpace(directory) ? "." : directory;
        var filePrefix = Path.GetFileNameWithoutExtension(databasePath);

        foreach (var file in Directory.GetFiles(rootDirectory, $"{filePrefix}.*"))
        {
            File.Delete(file);
        }
    }
}
