using System.Text;
using System.Text.Json;

namespace CSharpDB.DataGen.Output;

public static class JsonlWriter
{
    private static readonly JsonSerializerOptions s_compactOptions = new()
    {
        WriteIndented = false,
    };

    private static readonly JsonSerializerOptions s_indentedOptions = new()
    {
        WriteIndented = true,
    };

    public static async Task WriteJsonLinesAsync<T>(string path, IEnumerable<T> items, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(items);

        string fullPath = Path.GetFullPath(path);
        string? directory = Path.GetDirectoryName(fullPath);
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        await using var stream = File.Create(fullPath);
        await using var writer = new StreamWriter(stream, new UTF8Encoding(false));

        foreach (T item in items)
        {
            ct.ThrowIfCancellationRequested();
            await writer.WriteLineAsync(JsonSerializer.Serialize(item, s_compactOptions));
        }
    }

    public static async Task WriteJsonAsync<T>(string path, T value, CancellationToken ct = default)
    {
        string fullPath = Path.GetFullPath(path);
        string? directory = Path.GetDirectoryName(fullPath);
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        await using var stream = File.Create(fullPath);
        await JsonSerializer.SerializeAsync(stream, value, s_indentedOptions, ct);
    }
}
