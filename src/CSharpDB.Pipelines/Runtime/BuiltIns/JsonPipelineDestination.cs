using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Pipelines.Models;

namespace CSharpDB.Pipelines.Runtime.BuiltIns;

public sealed class JsonPipelineDestination : IPipelineDestination
{
    private readonly PipelineDestinationDefinition _definition;
    private readonly List<Dictionary<string, object?>> _rows = [];

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

        string? directory = Path.GetDirectoryName(_definition.Path);
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
        await using var stream = File.Create(_definition.Path!);
        await JsonSerializer.SerializeAsync(stream, _rows, s_options, ct);
    }
}
