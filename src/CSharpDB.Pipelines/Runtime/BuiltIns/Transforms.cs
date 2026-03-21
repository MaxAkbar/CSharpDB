using CSharpDB.Pipelines.Models;

namespace CSharpDB.Pipelines.Runtime.BuiltIns;

public sealed class SelectPipelineTransform : IPipelineTransform
{
    private readonly string[] _columns;

    public SelectPipelineTransform(PipelineTransformDefinition definition)
    {
        _columns = definition.SelectColumns?.ToArray()
            ?? throw new InvalidOperationException("Select transform requires columns.");
    }

    public string Name => "select";

    public ValueTask<PipelineRowBatch> TransformAsync(PipelineRowBatch batch, PipelineExecutionContext context, CancellationToken ct = default)
    {
        var rows = batch.Rows
            .Select(row => (Dictionary<string, object?>)_columns.ToDictionary(column => column, column => row.TryGetValue(column, out var value) ? value : null, StringComparer.OrdinalIgnoreCase))
            .ToArray();

        return ValueTask.FromResult(PipelineBatchFactory.CreateBatch(batch, rows));
    }
}

public sealed class RenamePipelineTransform : IPipelineTransform
{
    private readonly IReadOnlyList<PipelineRenameMapping> _mappings;

    public RenamePipelineTransform(PipelineTransformDefinition definition)
    {
        _mappings = definition.RenameMappings
            ?? throw new InvalidOperationException("Rename transform requires mappings.");
    }

    public string Name => "rename";

    public ValueTask<PipelineRowBatch> TransformAsync(PipelineRowBatch batch, PipelineExecutionContext context, CancellationToken ct = default)
    {
        var rows = batch.Rows.Select(row =>
        {
            var output = new Dictionary<string, object?>(row, StringComparer.OrdinalIgnoreCase);
            foreach (var mapping in _mappings)
            {
                if (output.TryGetValue(mapping.Source, out var value))
                {
                    output.Remove(mapping.Source);
                    output[mapping.Target] = value;
                }
            }

            return output;
        }).ToArray();

        return ValueTask.FromResult(PipelineBatchFactory.CreateBatch(batch, rows));
    }
}

public sealed class CastPipelineTransform : IPipelineTransform
{
    private readonly IReadOnlyList<PipelineCastMapping> _mappings;

    public CastPipelineTransform(PipelineTransformDefinition definition)
    {
        _mappings = definition.CastMappings
            ?? throw new InvalidOperationException("Cast transform requires mappings.");
    }

    public string Name => "cast";

    public ValueTask<PipelineRowBatch> TransformAsync(PipelineRowBatch batch, PipelineExecutionContext context, CancellationToken ct = default)
    {
        var rows = new List<Dictionary<string, object?>>(batch.Rows.Count);
        for (int rowIndex = 0; rowIndex < batch.Rows.Count; rowIndex++)
        {
            var row = batch.Rows[rowIndex];
            var output = new Dictionary<string, object?>(row, StringComparer.OrdinalIgnoreCase);
            foreach (var mapping in _mappings)
            {
                if (output.TryGetValue(mapping.Column, out var value))
                {
                    try
                    {
                        output[mapping.Column] = TransformSupport.ConvertValue(value, mapping.TargetType);
                    }
                    catch (Exception ex)
                    {
                        long rowNumber = batch.StartingRowNumber + rowIndex;
                        throw new InvalidOperationException(
                            $"Cast transform failed for column '{mapping.Column}' to '{mapping.TargetType}' at row {rowNumber} (batch {batch.BatchNumber}). {ex.Message}",
                            ex);
                    }
                }
            }

            rows.Add(output);
        }

        return ValueTask.FromResult(PipelineBatchFactory.CreateBatch(batch, rows.ToArray()));
    }
}

public sealed class FilterPipelineTransform : IPipelineTransform
{
    private readonly string _expression;

    public FilterPipelineTransform(PipelineTransformDefinition definition)
    {
        _expression = definition.FilterExpression
            ?? throw new InvalidOperationException("Filter transform requires an expression.");
    }

    public string Name => "filter";

    public ValueTask<PipelineRowBatch> TransformAsync(PipelineRowBatch batch, PipelineExecutionContext context, CancellationToken ct = default)
    {
        var rows = batch.Rows
            .Where(row => TransformSupport.EvaluateFilter(_expression, row))
            .Select(row => new Dictionary<string, object?>(row, StringComparer.OrdinalIgnoreCase))
            .ToArray();

        return ValueTask.FromResult(PipelineBatchFactory.CreateBatch(batch, rows));
    }
}

public sealed class DerivePipelineTransform : IPipelineTransform
{
    private readonly IReadOnlyList<PipelineDerivedColumn> _columns;

    public DerivePipelineTransform(PipelineTransformDefinition definition)
    {
        _columns = definition.DerivedColumns
            ?? throw new InvalidOperationException("Derive transform requires derived columns.");
    }

    public string Name => "derive";

    public ValueTask<PipelineRowBatch> TransformAsync(PipelineRowBatch batch, PipelineExecutionContext context, CancellationToken ct = default)
    {
        var rows = batch.Rows.Select(row =>
        {
            var output = new Dictionary<string, object?>(row, StringComparer.OrdinalIgnoreCase);
            foreach (var column in _columns)
            {
                output[column.Name] = TransformSupport.EvaluateDerivedExpression(column.Expression, output);
            }

            return output;
        }).ToArray();

        return ValueTask.FromResult(PipelineBatchFactory.CreateBatch(batch, rows));
    }
}

public sealed class DeduplicatePipelineTransform : IPipelineTransform
{
    private readonly string[] _keys;
    private readonly HashSet<string> _seen = new(StringComparer.Ordinal);

    public DeduplicatePipelineTransform(PipelineTransformDefinition definition)
    {
        _keys = definition.DeduplicateKeys?.ToArray()
            ?? throw new InvalidOperationException("Deduplicate transform requires keys.");
    }

    public string Name => "deduplicate";

    public ValueTask<PipelineRowBatch> TransformAsync(PipelineRowBatch batch, PipelineExecutionContext context, CancellationToken ct = default)
    {
        var output = new List<Dictionary<string, object?>>();
        foreach (var row in batch.Rows)
        {
            string key = string.Join("|", _keys.Select(column => row.TryGetValue(column, out var value) ? value?.ToString() ?? "<null>" : "<missing>"));
            if (_seen.Add(key))
            {
                output.Add(new Dictionary<string, object?>(row, StringComparer.OrdinalIgnoreCase));
            }
        }

        return ValueTask.FromResult(PipelineBatchFactory.CreateBatch(batch, output.ToArray()));
    }
}

file static class PipelineBatchFactory
{
    public static PipelineRowBatch CreateBatch(PipelineRowBatch batch, IReadOnlyList<Dictionary<string, object?>> rows) => new()
    {
        BatchNumber = batch.BatchNumber,
        StartingRowNumber = batch.StartingRowNumber,
        Rows = rows,
    };
}
