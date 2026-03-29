using System.Text;
using CSharpDB.DataGen.Specs;

namespace CSharpDB.DataGen.Output;

public static class CsvWriter
{
    public static async Task WriteRowsAsync(
        string path,
        SqlTableSpec table,
        IEnumerable<IReadOnlyDictionary<string, object?>> rows,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(table);
        ArgumentNullException.ThrowIfNull(rows);

        string fullPath = Path.GetFullPath(path);
        string? directory = Path.GetDirectoryName(fullPath);
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        await using var stream = File.Create(fullPath);
        await using var writer = new StreamWriter(stream, new UTF8Encoding(false));

        await writer.WriteLineAsync(string.Join(",", SqlSpecBuilder.GetCsvHeaders(table).Select(static value => Escape(value))));
        foreach (IReadOnlyDictionary<string, object?> row in rows)
        {
            ct.ThrowIfCancellationRequested();
            IReadOnlyList<string> values = SqlSpecBuilder.GetCsvValues(table, row);
            await writer.WriteLineAsync(string.Join(",", values.Select(static value => Escape(value))));
        }
    }

    public static async Task WriteTextAsync(string path, string content, CancellationToken ct = default)
    {
        string fullPath = Path.GetFullPath(path);
        string? directory = Path.GetDirectoryName(fullPath);
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        await File.WriteAllTextAsync(fullPath, content, new UTF8Encoding(false), ct);
    }

    private static string Escape(string value)
    {
        if (value.IndexOfAny([',', '"', '\r', '\n']) < 0)
            return value;

        return "\"" + value.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
    }
}
