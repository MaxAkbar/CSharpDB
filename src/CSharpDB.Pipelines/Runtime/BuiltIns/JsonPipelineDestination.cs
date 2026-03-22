using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Pipelines.Models;

namespace CSharpDB.Pipelines.Runtime.BuiltIns;

public sealed class JsonPipelineDestination : IPipelineDestination
{
    private readonly PipelineDestinationDefinition _definition;
    private readonly List<Dictionary<string, object?>> _rows = [];
    private string? _resolvedPath;

    private static readonly JsonSerializerOptions s_options = new()
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    public JsonPipelineDestination(PipelineDestinationDefinition definition)
    {
        _definition = definition;
    }

    public Task InitializeAsync(PipelineExecutionContext context, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(_definition.Path))
        {
            throw new InvalidOperationException("JSON destination path is required.");
        }

        _resolvedPath = PipelineFilePathResolver.ResolveOutputPath(_definition.Path);

        string? directory = Path.GetDirectoryName(_resolvedPath);
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        _rows.Clear();
        return Task.CompletedTask;
    }

    public Task WriteBatchAsync(PipelineRowBatch batch, PipelineExecutionContext context, CancellationToken ct = default)
    {
        _rows.AddRange(batch.Rows.Select(row => new Dictionary<string, object?>(row, StringComparer.OrdinalIgnoreCase)));
        return Task.CompletedTask;
    }

    public async Task CompleteAsync(PipelineExecutionContext context, CancellationToken ct = default)
    {
        string path = _resolvedPath ?? PipelineFilePathResolver.ResolveOutputPath(_definition.Path!);
        await using var stream = File.Create(path);
        await JsonSerializer.SerializeAsync(stream, _rows, s_options, ct);
    }
}
