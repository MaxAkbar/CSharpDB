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
            new ObjectDictionaryConverter(),
            new JsonStringEnumConverter(JsonNamingPolicy.CamelCase),
        },
    };

    public static string Serialize(PipelinePackageDefinition package)
    {
        ArgumentNullException.ThrowIfNull(package);
        return JsonSerializer.Serialize(PipelineAutomationMetadata.NormalizeForExport(package), s_options);
    }

    public static PipelinePackageDefinition Deserialize(string json)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(json);

        var package = JsonSerializer.Deserialize<PipelinePackageDefinition>(json, s_options)
            ?? throw new InvalidOperationException("Pipeline package JSON did not deserialize into a package definition.");
        return PipelineAutomationMetadata.NormalizeForExport(package);
    }

    public static async Task<PipelinePackageDefinition> LoadFromFileAsync(string path, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        await using var stream = File.OpenRead(path);
        var package = await JsonSerializer.DeserializeAsync<PipelinePackageDefinition>(stream, s_options, ct)
            ?? throw new InvalidOperationException($"Pipeline package file '{path}' did not deserialize into a package definition.");
        return PipelineAutomationMetadata.NormalizeForExport(package);
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
        await JsonSerializer.SerializeAsync(stream, PipelineAutomationMetadata.NormalizeForExport(package), s_options, ct);
    }
}
