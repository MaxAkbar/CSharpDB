using CSharpDB.Pipelines.Models;

namespace CSharpDB.Pipelines.Runtime.BuiltIns;

public sealed class DefaultPipelineComponentFactory : IPipelineComponentFactory
{
    public IPipelineSource CreateSource(PipelineSourceDefinition definition) => definition.Kind switch
    {
        PipelineSourceKind.CsvFile => new CsvPipelineSource(definition),
        PipelineSourceKind.JsonFile => new JsonPipelineSource(definition),
        PipelineSourceKind.CSharpDbTable => throw new NotSupportedException("CSharpDB table sources are not implemented yet."),
        PipelineSourceKind.SqlQuery => throw new NotSupportedException("SQL query sources are not implemented yet."),
        _ => throw new ArgumentOutOfRangeException(nameof(definition)),
    };

    public IReadOnlyList<IPipelineTransform> CreateTransforms(IReadOnlyList<PipelineTransformDefinition> definitions)
    {
        ArgumentNullException.ThrowIfNull(definitions);
        return definitions.Select(CreateTransform).ToArray();
    }

    public IPipelineDestination CreateDestination(PipelineDestinationDefinition definition) => definition.Kind switch
    {
        PipelineDestinationKind.CsvFile => new CsvPipelineDestination(definition),
        PipelineDestinationKind.JsonFile => new JsonPipelineDestination(definition),
        PipelineDestinationKind.CSharpDbTable => throw new NotSupportedException("CSharpDB table destinations are not implemented yet."),
        _ => throw new ArgumentOutOfRangeException(nameof(definition)),
    };

    private static IPipelineTransform CreateTransform(PipelineTransformDefinition definition) => definition.Kind switch
    {
        PipelineTransformKind.Select => new SelectPipelineTransform(definition),
        PipelineTransformKind.Rename => new RenamePipelineTransform(definition),
        PipelineTransformKind.Cast => new CastPipelineTransform(definition),
        PipelineTransformKind.Filter => new FilterPipelineTransform(definition),
        PipelineTransformKind.Derive => new DerivePipelineTransform(definition),
        PipelineTransformKind.Deduplicate => new DeduplicatePipelineTransform(definition),
        _ => throw new ArgumentOutOfRangeException(nameof(definition)),
    };
}
