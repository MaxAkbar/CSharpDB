using CSharpDB.Pipelines.Models;

namespace CSharpDB.Pipelines.Runtime.BuiltIns;

public sealed class CsvPipelineDestination : IPipelineDestination
{
    private readonly PipelineDestinationDefinition _definition;
    private string[]? _headers;
    private bool _headerWritten;
    private string? _resolvedPath;

    public CsvPipelineDestination(PipelineDestinationDefinition definition)
    {
        _definition = definition;
    }

    public Task InitializeAsync(PipelineExecutionContext context, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(_definition.Path))
        {
            throw new InvalidOperationException("CSV destination path is required.");
        }

        _resolvedPath = PipelineFilePathResolver.ResolveOutputPath(_definition.Path);

        string? directory = Path.GetDirectoryName(_resolvedPath);
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        if (_definition.Overwrite && File.Exists(_resolvedPath))
        {
            File.Delete(_resolvedPath);
        }

        return Task.CompletedTask;
    }

    public async Task WriteBatchAsync(PipelineRowBatch batch, PipelineExecutionContext context, CancellationToken ct = default)
    {
        if (batch.Rows.Count == 0)
        {
            return;
        }

        _headers ??= batch.Rows[0].Keys.ToArray();

        string path = _resolvedPath ?? PipelineFilePathResolver.ResolveOutputPath(_definition.Path!);
        await using var stream = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.Read);
        await using var writer = new StreamWriter(stream);

        if (!_headerWritten)
        {
            await writer.WriteLineAsync(string.Join(",", _headers.Select(CsvSupport.FormatValue)));
            _headerWritten = true;
        }

        foreach (var row in batch.Rows)
        {
            ct.ThrowIfCancellationRequested();
            string line = string.Join(",", _headers.Select(header => CsvSupport.FormatValue(row.TryGetValue(header, out var value) ? value : null)));
            await writer.WriteLineAsync(line);
        }
    }

    public Task CompleteAsync(PipelineExecutionContext context, CancellationToken ct = default)
        => Task.CompletedTask;
}
