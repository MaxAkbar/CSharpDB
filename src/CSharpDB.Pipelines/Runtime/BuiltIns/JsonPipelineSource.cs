using System.Text.Json;
using CSharpDB.Pipelines.Models;

namespace CSharpDB.Pipelines.Runtime.BuiltIns;

public sealed class JsonPipelineSource : IPipelineSource
{
    private readonly PipelineSourceDefinition _definition;
    private string? _resolvedPath;

    public JsonPipelineSource(PipelineSourceDefinition definition)
    {
        _definition = definition;
    }

    public Task OpenAsync(PipelineExecutionContext context, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(_definition.Path))
        {
            throw new InvalidOperationException("JSON source path is required.");
        }

        _resolvedPath = PipelineFilePathResolver.ResolveExistingFile(_definition.Path);
        if (!File.Exists(_resolvedPath))
        {
            throw new FileNotFoundException("JSON source file was not found.", _definition.Path);
        }

        return Task.CompletedTask;
    }

    public async IAsyncEnumerable<PipelineRowBatch> ReadBatchesAsync(
        PipelineExecutionContext context,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        string path = _resolvedPath ?? PipelineFilePathResolver.ResolveExistingFile(_definition.Path!);
        string json = await File.ReadAllTextAsync(path, ct);
        using var document = JsonDocument.Parse(json);

        if (document.RootElement.ValueKind != JsonValueKind.Array)
        {
            throw new InvalidOperationException("JSON source must contain a top-level array of objects.");
        }

        int batchSize = context.Package.Options.BatchSize;
        long batchNumber = 0;
        long rowNumber = 0;
        long batchStartRowNumber = 1;
        var batch = new List<Dictionary<string, object?>>(batchSize);

        foreach (JsonElement element in document.RootElement.EnumerateArray())
        {
            ct.ThrowIfCancellationRequested();
            if (element.ValueKind != JsonValueKind.Object)
            {
                throw new InvalidOperationException("JSON source array items must be objects.");
            }

            if (batch.Count == 0)
            {
                batchStartRowNumber = rowNumber + 1;
            }

            batch.Add(CoerceObject(element));
            rowNumber++;

            if (batch.Count >= batchSize)
            {
                batchNumber++;
                if (context.Checkpoint is null || batchNumber > context.Checkpoint.BatchNumber)
                    yield return CreateBatch(batchNumber, batchStartRowNumber, batch);
                batch = new List<Dictionary<string, object?>>(batchSize);
            }
        }

        if (batch.Count > 0)
        {
            batchNumber++;
            if (context.Checkpoint is null || batchNumber > context.Checkpoint.BatchNumber)
                yield return CreateBatch(batchNumber, batchStartRowNumber, batch);
        }
    }

    private static Dictionary<string, object?> CoerceObject(JsonElement element)
    {
        var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (JsonProperty property in element.EnumerateObject())
        {
            row[property.Name] = property.Value.ValueKind switch
            {
                JsonValueKind.Null => null,
                JsonValueKind.String => property.Value.GetString(),
                JsonValueKind.Number => property.Value.TryGetInt64(out long longValue) ? longValue : property.Value.GetDouble(),
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                _ => property.Value.GetRawText(),
            };
        }

        return row;
    }

    private static PipelineRowBatch CreateBatch(long batchNumber, long batchStartRowNumber, List<Dictionary<string, object?>> batch) => new()
    {
        BatchNumber = batchNumber,
        StartingRowNumber = batchStartRowNumber,
        Rows = batch.ToArray(),
    };
}
