using CSharpDB.Pipelines.Models;

namespace CSharpDB.Pipelines.Runtime.BuiltIns;

public sealed class CsvPipelineDestination : IPipelineDestination
{
    private readonly PipelineDestinationDefinition _definition;
    private string[]? _headers;
    private bool _headerWritten;

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

        string? directory = Path.GetDirectoryName(_definition.Path);
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        if (_definition.Overwrite && File.Exists(_definition.Path))
        {
            File.Delete(_definition.Path);
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

        await using var stream = new FileStream(_definition.Path!, FileMode.Append, FileAccess.Write, FileShare.Read);
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
