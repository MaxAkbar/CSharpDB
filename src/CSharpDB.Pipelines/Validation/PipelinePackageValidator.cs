using CSharpDB.Pipelines.Models;

namespace CSharpDB.Pipelines.Validation;

public static class PipelinePackageValidator
{
    public static PipelineValidationResult Validate(PipelinePackageDefinition package)
    {
        ArgumentNullException.ThrowIfNull(package);

        var errors = new List<PipelineValidationIssue>();

        if (string.IsNullOrWhiteSpace(package.Name))
        {
            errors.Add(Error("pipeline.name.required", "name", "Pipeline name is required."));
        }

        if (string.IsNullOrWhiteSpace(package.Version))
        {
            errors.Add(Error("pipeline.version.required", "version", "Pipeline version is required."));
        }

        ValidateSource(package.Source, errors);
        ValidateDestination(package.Destination, errors);
        ValidateOptions(package.Options, errors);
        ValidateIncremental(package.Incremental, errors);
        ValidateTransforms(package.Transforms, errors);
        ValidateHooks(package.Hooks, errors);

        return errors.Count == 0
            ? PipelineValidationResult.Success
            : new PipelineValidationResult(errors);
    }

    private static void ValidateSource(PipelineSourceDefinition source, List<PipelineValidationIssue> errors)
    {
        ArgumentNullException.ThrowIfNull(source);

        switch (source.Kind)
        {
            case PipelineSourceKind.CsvFile:
            case PipelineSourceKind.JsonFile:
                if (string.IsNullOrWhiteSpace(source.Path))
                {
                    errors.Add(Error("pipeline.source.path.required", "source.path", "File-based sources require a path."));
                }
                break;

            case PipelineSourceKind.CSharpDbTable:
                if (string.IsNullOrWhiteSpace(source.TableName))
                {
                    errors.Add(Error("pipeline.source.table.required", "source.tableName", "CSharpDB table sources require a table name."));
                }
                break;

            case PipelineSourceKind.SqlQuery:
                if (string.IsNullOrWhiteSpace(source.QueryText))
                {
                    errors.Add(Error("pipeline.source.query.required", "source.queryText", "SQL query sources require query text."));
                }
                break;
        }
    }

    private static void ValidateDestination(PipelineDestinationDefinition destination, List<PipelineValidationIssue> errors)
    {
        ArgumentNullException.ThrowIfNull(destination);

        switch (destination.Kind)
        {
            case PipelineDestinationKind.CSharpDbTable:
                if (string.IsNullOrWhiteSpace(destination.TableName))
                {
                    errors.Add(Error("pipeline.destination.table.required", "destination.tableName", "CSharpDB destinations require a table name."));
                }
                break;

            case PipelineDestinationKind.CsvFile:
            case PipelineDestinationKind.JsonFile:
                if (string.IsNullOrWhiteSpace(destination.Path))
                {
                    errors.Add(Error("pipeline.destination.path.required", "destination.path", "File destinations require a path."));
                }
                break;
        }
    }

    private static void ValidateOptions(PipelineExecutionOptions options, List<PipelineValidationIssue> errors)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (options.BatchSize <= 0)
        {
            errors.Add(Error("pipeline.options.batchSize.invalid", "options.batchSize", "Batch size must be greater than zero."));
        }

        if (options.CheckpointInterval <= 0)
        {
            errors.Add(Error("pipeline.options.checkpointInterval.invalid", "options.checkpointInterval", "Checkpoint interval must be greater than zero."));
        }

        if (options.MaxRejects < 0)
        {
            errors.Add(Error("pipeline.options.maxRejects.invalid", "options.maxRejects", "Max rejects cannot be negative."));
        }

        if (options.ErrorMode == PipelineErrorMode.FailFast && options.MaxRejects > 0)
        {
            errors.Add(Error("pipeline.options.maxRejects.unsupported", "options.maxRejects", "Max rejects only applies when error mode is SkipBadRows."));
        }
    }

    private static void ValidateIncremental(PipelineIncrementalOptions? incremental, List<PipelineValidationIssue> errors)
    {
        if (incremental is null)
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(incremental.WatermarkColumn))
        {
            errors.Add(Error("pipeline.incremental.watermark.required", "incremental.watermarkColumn", "Incremental pipelines require a watermark column."));
        }
    }

    private static void ValidateTransforms(IReadOnlyList<PipelineTransformDefinition> transforms, List<PipelineValidationIssue> errors)
    {
        ArgumentNullException.ThrowIfNull(transforms);

        for (int i = 0; i < transforms.Count; i++)
        {
            var transform = transforms[i];
            string path = $"transforms[{i}]";

            switch (transform.Kind)
            {
                case PipelineTransformKind.Select:
                    if (transform.SelectColumns is null || transform.SelectColumns.Count == 0)
                    {
                        errors.Add(Error("pipeline.transform.select.columns.required", $"{path}.selectColumns", "Select transforms require at least one column."));
                    }
                    break;

                case PipelineTransformKind.Rename:
                    if (transform.RenameMappings is null || transform.RenameMappings.Count == 0)
                    {
                        errors.Add(Error("pipeline.transform.rename.mappings.required", $"{path}.renameMappings", "Rename transforms require at least one mapping."));
                        break;
                    }

                    for (int mappingIndex = 0; mappingIndex < transform.RenameMappings.Count; mappingIndex++)
                    {
                        var mapping = transform.RenameMappings[mappingIndex];
                        if (string.IsNullOrWhiteSpace(mapping.Source) || string.IsNullOrWhiteSpace(mapping.Target))
                        {
                            errors.Add(Error("pipeline.transform.rename.mapping.invalid", $"{path}.renameMappings[{mappingIndex}]", "Rename mappings require both source and target names."));
                        }
                    }
                    break;

                case PipelineTransformKind.Cast:
                    if (transform.CastMappings is null || transform.CastMappings.Count == 0)
                    {
                        errors.Add(Error("pipeline.transform.cast.mappings.required", $"{path}.castMappings", "Cast transforms require at least one cast mapping."));
                        break;
                    }

                    for (int mappingIndex = 0; mappingIndex < transform.CastMappings.Count; mappingIndex++)
                    {
                        var mapping = transform.CastMappings[mappingIndex];
                        if (string.IsNullOrWhiteSpace(mapping.Column))
                        {
                            errors.Add(Error("pipeline.transform.cast.column.required", $"{path}.castMappings[{mappingIndex}].column", "Cast mappings require a column name."));
                        }
                    }
                    break;

                case PipelineTransformKind.Filter:
                    if (string.IsNullOrWhiteSpace(transform.FilterExpression))
                    {
                        errors.Add(Error("pipeline.transform.filter.expression.required", $"{path}.filterExpression", "Filter transforms require an expression."));
                    }
                    else
                    {
                        ValidateFunctionSyntax(transform.FilterExpression, $"{path}.filterExpression", errors);
                    }
                    break;

                case PipelineTransformKind.Derive:
                    if (transform.DerivedColumns is null || transform.DerivedColumns.Count == 0)
                    {
                        errors.Add(Error("pipeline.transform.derive.columns.required", $"{path}.derivedColumns", "Derive transforms require at least one derived column."));
                        break;
                    }

                    for (int derivedIndex = 0; derivedIndex < transform.DerivedColumns.Count; derivedIndex++)
                    {
                        var derived = transform.DerivedColumns[derivedIndex];
                        if (string.IsNullOrWhiteSpace(derived.Name) || string.IsNullOrWhiteSpace(derived.Expression))
                        {
                            errors.Add(Error("pipeline.transform.derive.column.invalid", $"{path}.derivedColumns[{derivedIndex}]", "Derived columns require both a name and an expression."));
                        }
                        else
                        {
                            ValidateFunctionSyntax(derived.Expression, $"{path}.derivedColumns[{derivedIndex}].expression", errors);
                        }
                    }
                    break;

                case PipelineTransformKind.Deduplicate:
                    if (transform.DeduplicateKeys is null || transform.DeduplicateKeys.Count == 0)
                    {
                        errors.Add(Error("pipeline.transform.deduplicate.keys.required", $"{path}.deduplicateKeys", "Deduplicate transforms require at least one key column."));
                    }
                    break;
            }
        }
    }

    private static void ValidateHooks(IReadOnlyList<PipelineCommandHookDefinition>? hooks, List<PipelineValidationIssue> errors)
    {
        if (hooks is null)
            return;

        for (int i = 0; i < hooks.Count; i++)
        {
            PipelineCommandHookDefinition hook = hooks[i];
            string path = $"hooks[{i}]";

            if (string.IsNullOrWhiteSpace(hook.CommandName))
            {
                errors.Add(Error("pipeline.hook.command.required", $"{path}.commandName", "Pipeline command hooks require a command name."));
            }

            if (hook.Arguments is null)
                continue;

            foreach (string key in hook.Arguments.Keys)
            {
                if (string.IsNullOrWhiteSpace(key))
                {
                    errors.Add(Error("pipeline.hook.argument.name.required", $"{path}.arguments", "Pipeline command hook arguments require non-empty names."));
                    break;
                }
            }
        }
    }

    private static void ValidateFunctionSyntax(string expression, string path, List<PipelineValidationIssue> errors)
    {
        bool inString = false;
        for (int i = 0; i < expression.Length; i++)
        {
            if (expression[i] == '\'')
            {
                inString = !inString;
                continue;
            }

            if (inString)
                continue;

            if (!IsIdentifierStart(expression[i]))
                continue;

            int nameStart = i;
            i++;
            while (i < expression.Length && IsIdentifierPart(expression[i]))
                i++;

            int cursor = i;
            while (cursor < expression.Length && char.IsWhiteSpace(expression[cursor]))
                cursor++;

            if (cursor >= expression.Length || expression[cursor] != '(')
                continue;

            if (!TryValidateFunctionArguments(expression, cursor, out int closeParen))
            {
                errors.Add(Error(
                    "pipeline.expression.function.syntax",
                    path,
                    $"Function call '{expression[nameStart..Math.Min(expression.Length, cursor + 1)]}...' is malformed."));
                return;
            }

            i = closeParen;
        }
    }

    private static bool TryValidateFunctionArguments(string expression, int openParen, out int closeParen)
    {
        closeParen = -1;
        int depth = 0;
        bool inString = false;
        bool expectingArgument = true;
        bool sawArgument = false;

        for (int i = openParen; i < expression.Length; i++)
        {
            char ch = expression[i];
            if (ch == '\'')
            {
                inString = !inString;
                expectingArgument = false;
                sawArgument = true;
                continue;
            }

            if (inString)
                continue;

            if (ch == '(')
            {
                depth++;
                if (depth > 1)
                {
                    expectingArgument = false;
                    sawArgument = true;
                }
                continue;
            }

            if (ch == ')')
            {
                depth--;
                if (depth < 0)
                    return false;

                if (depth == 0)
                {
                    closeParen = i;
                    return !inString && (!expectingArgument || !sawArgument);
                }

                continue;
            }

            if (ch == ',' && depth == 1)
            {
                if (expectingArgument)
                    return false;

                expectingArgument = true;
                continue;
            }

            if (!char.IsWhiteSpace(ch))
            {
                expectingArgument = false;
                sawArgument = true;
            }
        }

        return false;
    }

    private static bool IsIdentifierStart(char value)
        => char.IsLetter(value) || value == '_';

    private static bool IsIdentifierPart(char value)
        => char.IsLetterOrDigit(value) || value == '_';

    private static PipelineValidationIssue Error(string code, string path, string message) => new()
    {
        Code = code,
        Path = path,
        Message = message,
    };
}
