using CSharpDB.Primitives;

namespace CSharpDB.Pipelines.Models;

public static class PipelineAutomationMetadata
{
    private const string Surface = "pipelines";

    public static PipelinePackageDefinition NormalizeForExport(PipelinePackageDefinition package)
    {
        ArgumentNullException.ThrowIfNull(package);

        DbAutomationMetadata metadata = Build(package);
        return WithAutomation(package, metadata.IsEmpty ? null : metadata);
    }

    public static DbAutomationMetadata Build(PipelinePackageDefinition package)
    {
        ArgumentNullException.ThrowIfNull(package);

        var builder = new DbAutomationMetadataBuilder();
        for (int i = 0; i < package.Hooks.Count; i++)
        {
            PipelineCommandHookDefinition hook = package.Hooks[i];
            builder.AddCommand(hook.CommandName, Surface, $"hooks[{i}].{hook.Event}");
        }

        for (int i = 0; i < package.Transforms.Count; i++)
        {
            PipelineTransformDefinition transform = package.Transforms[i];
            string transformLocation = $"transforms[{i}]";
            if (transform.Kind == PipelineTransformKind.Filter)
                AddScalarFunctions(builder, transform.FilterExpression, $"{transformLocation}.filterExpression");

            if (transform.Kind == PipelineTransformKind.Derive && transform.DerivedColumns is not null)
            {
                for (int columnIndex = 0; columnIndex < transform.DerivedColumns.Count; columnIndex++)
                {
                    PipelineDerivedColumn column = transform.DerivedColumns[columnIndex];
                    AddScalarFunctions(builder, column.Expression, $"{transformLocation}.derivedColumns[{columnIndex}].expression");
                }
            }
        }

        return builder.Build();
    }

    private static PipelinePackageDefinition WithAutomation(PipelinePackageDefinition package, DbAutomationMetadata? automation)
        => new()
        {
            Name = package.Name,
            Version = package.Version,
            Description = package.Description,
            Source = package.Source,
            Transforms = package.Transforms,
            Destination = package.Destination,
            Options = package.Options,
            Incremental = package.Incremental,
            Hooks = package.Hooks,
            Automation = automation,
        };

    private static void AddScalarFunctions(DbAutomationMetadataBuilder builder, string? expression, string location)
    {
        foreach (DbAutomationScalarFunctionCall call in DbAutomationExpressionInspector.FindScalarFunctionCalls(expression))
            builder.AddScalarFunction(call.Name, call.Arity, Surface, location);
    }
}
