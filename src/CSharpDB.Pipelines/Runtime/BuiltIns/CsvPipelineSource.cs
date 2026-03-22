using CSharpDB.Pipelines.Models;

namespace CSharpDB.Pipelines.Runtime.BuiltIns;

public sealed class CsvPipelineSource : IPipelineSource
{
    private readonly PipelineSourceDefinition _definition;
    private string? _resolvedPath;

    public CsvPipelineSource(PipelineSourceDefinition definition)
    {
        _definition = definition;
    }

    public Task OpenAsync(PipelineExecutionContext context, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(_definition.Path))
        {
            throw new InvalidOperationException("CSV source path is required.");
        }

        _resolvedPath = PipelineFilePathResolver.ResolveExistingFile(_definition.Path);
        if (!File.Exists(_resolvedPath))
        {
            throw new FileNotFoundException("CSV source file was not found.", _definition.Path);
        }

        return Task.CompletedTask;
    }

    public async IAsyncEnumerable<PipelineRowBatch> ReadBatchesAsync(
        PipelineExecutionContext context,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        string path = _resolvedPath ?? PipelineFilePathResolver.ResolveExistingFile(_definition.Path!);
        using var reader = new StreamReader(path);

        string[] headers = [];
        bool headerInitialized = false;
        long batchNumber = 0;
        long rowNumber = 0;
        int batchSize = context.Package.Options.BatchSize;
        var batch = new List<Dictionary<string, object?>>(batchSize);
        long batchStartRowNumber = 1;

        while (true)
        {
            ct.ThrowIfCancellationRequested();
            string? line = await reader.ReadLineAsync(ct);
            if (line is null)
            {
                break;
            }

            if (!headerInitialized)
            {
                string[] parsed = CsvSupport.ParseLine(line);
                if (_definition.HasHeaderRow)
                {
                    headers = parsed;
                    headerInitialized = true;
                    continue;
                }

                headers = Enumerable.Range(0, parsed.Length).Select(i => $"column{i + 1}").ToArray();
                headerInitialized = true;
                batch.Add(CreateRow(headers, parsed));
                rowNumber++;
                continue;
            }

            if (batch.Count == 0)
            {
                batchStartRowNumber = rowNumber + 1;
            }

            batch.Add(CreateRow(headers, CsvSupport.ParseLine(line)));
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

    private static Dictionary<string, object?> CreateRow(string[] headers, string[] values)
    {
        var row = new Dictionary<string, object?>(headers.Length, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < headers.Length; i++)
        {
            row[headers[i]] = i < values.Length ? values[i] : null;
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
