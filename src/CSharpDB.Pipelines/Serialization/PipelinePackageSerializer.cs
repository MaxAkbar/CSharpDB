using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Pipelines.Models;

namespace CSharpDB.Pipelines.Serialization;

public static class PipelinePackageSerializer
{
    private static readonly JsonSerializerOptions s_options = new()
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Converters =
        {
            new JsonStringEnumConverter(JsonNamingPolicy.CamelCase),
        },
    };

    public static string Serialize(PipelinePackageDefinition package)
    {
        ArgumentNullException.ThrowIfNull(package);
        return JsonSerializer.Serialize(package, s_options);
    }

    public static PipelinePackageDefinition Deserialize(string json)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(json);

        var package = JsonSerializer.Deserialize<PipelinePackageDefinition>(json, s_options);
        return package ?? throw new InvalidOperationException("Pipeline package JSON did not deserialize into a package definition.");
    }

    public static async Task<PipelinePackageDefinition> LoadFromFileAsync(string path, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        await using var stream = File.OpenRead(path);
        var package = await JsonSerializer.DeserializeAsync<PipelinePackageDefinition>(stream, s_options, ct);
        return package ?? throw new InvalidOperationException($"Pipeline package file '{path}' did not deserialize into a package definition.");
    }

    public static async Task SaveToFileAsync(PipelinePackageDefinition package, string path, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(package);
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        string? directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        await using var stream = File.Create(path);
        await JsonSerializer.SerializeAsync(stream, package, s_options, ct);
    }
}
