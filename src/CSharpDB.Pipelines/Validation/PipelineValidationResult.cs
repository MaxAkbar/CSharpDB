namespace CSharpDB.Pipelines.Validation;

public sealed class PipelineValidationResult
{
    public bool IsValid => Errors.Count == 0;

    public IReadOnlyList<PipelineValidationIssue> Errors { get; }

    public PipelineValidationResult(IEnumerable<PipelineValidationIssue> errors)
    {
        Errors = errors.ToArray();
    }

    public static PipelineValidationResult Success { get; } = new([]);
}

public sealed class PipelineValidationIssue
{
    public required string Code { get; init; }
    public required string Path { get; init; }
    public required string Message { get; init; }
}
