namespace CSharpDB.Primitives;

public sealed record AutomationValidationResult(
    bool Succeeded,
    IReadOnlyList<AutomationValidationIssue> Issues);

public sealed record AutomationValidationIssue(
    AutomationValidationSeverity Severity,
    AutomationCallbackKind CallbackKind,
    string Name,
    string Surface,
    string Location,
    string Message,
    int? ExpectedArity = null);

public sealed record AutomationManifestValidationOptions(bool RequireMetadata = false)
{
    public static AutomationManifestValidationOptions Default { get; } = new();
}

public enum AutomationValidationSeverity
{
    Warning,
    Error,
}

public enum AutomationCallbackKind
{
    Unknown,
    ScalarFunction,
    Command,
    ValidationRule,
}

public static class AutomationManifestValidator
{
    private const string UnknownSurface = "unknown";
    private const string UnknownLocation = "$";

    public static AutomationValidationResult Validate(
        DbAutomationMetadata? metadata,
        DbFunctionRegistry functions,
        DbCommandRegistry commands)
        => Validate(metadata, functions, commands, DbValidationRuleRegistry.Empty, AutomationManifestValidationOptions.Default);

    public static AutomationValidationResult Validate(
        DbAutomationMetadata? metadata,
        DbFunctionRegistry functions,
        DbCommandRegistry commands,
        DbValidationRuleRegistry validationRules)
        => Validate(metadata, functions, commands, validationRules, AutomationManifestValidationOptions.Default);

    public static AutomationValidationResult Validate(
        DbAutomationMetadata? metadata,
        DbFunctionRegistry functions,
        DbCommandRegistry commands,
        AutomationManifestValidationOptions? options)
        => Validate(metadata, functions, commands, DbValidationRuleRegistry.Empty, options);

    public static AutomationValidationResult Validate(
        DbAutomationMetadata? metadata,
        DbFunctionRegistry functions,
        DbCommandRegistry commands,
        DbValidationRuleRegistry validationRules,
        AutomationManifestValidationOptions? options)
    {
        ArgumentNullException.ThrowIfNull(functions);
        ArgumentNullException.ThrowIfNull(commands);
        ArgumentNullException.ThrowIfNull(validationRules);

        options ??= AutomationManifestValidationOptions.Default;
        var issues = new List<AutomationValidationIssue>();

        if (metadata is null)
        {
            if (options.RequireMetadata)
            {
                issues.Add(new AutomationValidationIssue(
                    AutomationValidationSeverity.Error,
                    AutomationCallbackKind.Unknown,
                    string.Empty,
                    UnknownSurface,
                    UnknownLocation,
                    "Automation metadata is missing. Export or build automation metadata before validating callbacks."));
            }

            return CreateResult(issues);
        }

        AddDuplicateCommandIssues(metadata.Commands, issues);
        AddDuplicateScalarFunctionIssues(metadata.ScalarFunctions, issues);
        AddDuplicateValidationRuleIssues(metadata.ValidationRules, issues);
        ValidateCommands(metadata.Commands, commands, issues);
        ValidateScalarFunctions(metadata.ScalarFunctions, functions, issues);
        ValidateValidationRules(metadata.ValidationRules, validationRules, issues);

        return CreateResult(issues);
    }

    private static void AddDuplicateCommandIssues(
        IReadOnlyList<DbAutomationCommandReference>? references,
        List<AutomationValidationIssue> issues)
    {
        if (references is null || references.Count == 0)
            return;

        var occurrences = new Dictionary<string, DuplicateOccurrence>(StringComparer.OrdinalIgnoreCase);
        foreach (DbAutomationCommandReference reference in references)
        {
            string name = NormalizeName(reference.Name);
            string surface = NormalizeSurface(reference.Surface);
            string location = NormalizeLocation(reference.Location);
            string key = CreateKey(name, surface, location);

            occurrences[key] = occurrences.TryGetValue(key, out DuplicateOccurrence? occurrence)
                ? occurrence.Increment()
                : new DuplicateOccurrence(name, surface, location, Count: 1, Arity: null);
        }

        foreach (DuplicateOccurrence occurrence in occurrences.Values
            .Where(static occurrence => occurrence.Count > 1)
            .OrderBy(static occurrence => occurrence.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(static occurrence => occurrence.Surface, StringComparer.OrdinalIgnoreCase)
            .ThenBy(static occurrence => occurrence.Location, StringComparer.OrdinalIgnoreCase))
        {
            issues.Add(new AutomationValidationIssue(
                AutomationValidationSeverity.Warning,
                AutomationCallbackKind.Command,
                occurrence.Name,
                occurrence.Surface,
                occurrence.Location,
                $"Command '{occurrence.Name}' is referenced {occurrence.Count} times at {occurrence.Surface}:{occurrence.Location}. Remove duplicate metadata entries."));
        }
    }

    private static void AddDuplicateScalarFunctionIssues(
        IReadOnlyList<DbAutomationScalarFunctionReference>? references,
        List<AutomationValidationIssue> issues)
    {
        if (references is null || references.Count == 0)
            return;

        var occurrences = new Dictionary<string, DuplicateOccurrence>(StringComparer.OrdinalIgnoreCase);
        foreach (DbAutomationScalarFunctionReference reference in references)
        {
            string name = NormalizeName(reference.Name);
            string surface = NormalizeSurface(reference.Surface);
            string location = NormalizeLocation(reference.Location);
            string key = CreateKey(name, reference.Arity.ToString(System.Globalization.CultureInfo.InvariantCulture), surface, location);

            occurrences[key] = occurrences.TryGetValue(key, out DuplicateOccurrence? occurrence)
                ? occurrence.Increment()
                : new DuplicateOccurrence(name, surface, location, Count: 1, reference.Arity);
        }

        foreach (DuplicateOccurrence occurrence in occurrences.Values
            .Where(static occurrence => occurrence.Count > 1)
            .OrderBy(static occurrence => occurrence.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(static occurrence => occurrence.Arity)
            .ThenBy(static occurrence => occurrence.Surface, StringComparer.OrdinalIgnoreCase)
            .ThenBy(static occurrence => occurrence.Location, StringComparer.OrdinalIgnoreCase))
        {
            issues.Add(new AutomationValidationIssue(
                AutomationValidationSeverity.Warning,
                AutomationCallbackKind.ScalarFunction,
                occurrence.Name,
                occurrence.Surface,
                occurrence.Location,
                $"Scalar function '{occurrence.Name}' with arity {occurrence.Arity} is referenced {occurrence.Count} times at {occurrence.Surface}:{occurrence.Location}. Remove duplicate metadata entries.",
                occurrence.Arity));
        }
    }

    private static void AddDuplicateValidationRuleIssues(
        IReadOnlyList<DbAutomationValidationRuleReference>? references,
        List<AutomationValidationIssue> issues)
    {
        if (references is null || references.Count == 0)
            return;

        var occurrences = new Dictionary<string, DuplicateOccurrence>(StringComparer.OrdinalIgnoreCase);
        foreach (DbAutomationValidationRuleReference reference in references)
        {
            string name = NormalizeName(reference.Name);
            string surface = NormalizeSurface(reference.Surface);
            string location = NormalizeLocation(reference.Location);
            string key = CreateKey(name, surface, location);

            occurrences[key] = occurrences.TryGetValue(key, out DuplicateOccurrence? occurrence)
                ? occurrence.Increment()
                : new DuplicateOccurrence(name, surface, location, Count: 1, Arity: null);
        }

        foreach (DuplicateOccurrence occurrence in occurrences.Values
            .Where(static occurrence => occurrence.Count > 1)
            .OrderBy(static occurrence => occurrence.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(static occurrence => occurrence.Surface, StringComparer.OrdinalIgnoreCase)
            .ThenBy(static occurrence => occurrence.Location, StringComparer.OrdinalIgnoreCase))
        {
            issues.Add(new AutomationValidationIssue(
                AutomationValidationSeverity.Warning,
                AutomationCallbackKind.ValidationRule,
                occurrence.Name,
                occurrence.Surface,
                occurrence.Location,
                $"Validation rule '{occurrence.Name}' is referenced {occurrence.Count} times at {occurrence.Surface}:{occurrence.Location}. Remove duplicate metadata entries."));
        }
    }

    private static void ValidateCommands(
        IReadOnlyList<DbAutomationCommandReference>? references,
        DbCommandRegistry commands,
        List<AutomationValidationIssue> issues)
    {
        if (references is null || references.Count == 0)
            return;

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (DbAutomationCommandReference reference in references)
        {
            string name = NormalizeName(reference.Name);
            string surface = NormalizeSurface(reference.Surface);
            string location = NormalizeLocation(reference.Location);
            if (!seen.Add(CreateKey(name, surface, location)))
                continue;

            if (string.IsNullOrWhiteSpace(name))
            {
                issues.Add(new AutomationValidationIssue(
                    AutomationValidationSeverity.Error,
                    AutomationCallbackKind.Command,
                    name,
                    surface,
                    location,
                    $"Command reference at {surface}:{location} has no command name."));
                continue;
            }

            if (!commands.ContainsCommandName(name))
            {
                issues.Add(new AutomationValidationIssue(
                    AutomationValidationSeverity.Error,
                    AutomationCallbackKind.Command,
                    name,
                    surface,
                    location,
                    $"Command '{name}' is referenced by {surface} at {location}, but it is not registered in the host command registry."));
            }
        }
    }

    private static void ValidateScalarFunctions(
        IReadOnlyList<DbAutomationScalarFunctionReference>? references,
        DbFunctionRegistry functions,
        List<AutomationValidationIssue> issues)
    {
        if (references is null || references.Count == 0)
            return;

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (DbAutomationScalarFunctionReference reference in references)
        {
            string name = NormalizeName(reference.Name);
            string surface = NormalizeSurface(reference.Surface);
            string location = NormalizeLocation(reference.Location);
            string key = CreateKey(name, reference.Arity.ToString(System.Globalization.CultureInfo.InvariantCulture), surface, location);
            if (!seen.Add(key))
                continue;

            if (string.IsNullOrWhiteSpace(name))
            {
                issues.Add(new AutomationValidationIssue(
                    AutomationValidationSeverity.Error,
                    AutomationCallbackKind.ScalarFunction,
                    name,
                    surface,
                    location,
                    $"Scalar function reference at {surface}:{location} has no function name.",
                    reference.Arity));
                continue;
            }

            if (reference.Arity < 0)
            {
                issues.Add(new AutomationValidationIssue(
                    AutomationValidationSeverity.Error,
                    AutomationCallbackKind.ScalarFunction,
                    name,
                    surface,
                    location,
                    $"Scalar function '{name}' is referenced by {surface} at {location} with invalid arity {reference.Arity}. Arity must be zero or greater.",
                    reference.Arity));
                continue;
            }

            if (functions.TryGetScalar(name, reference.Arity, out _))
                continue;

            if (functions.ContainsScalarName(name))
            {
                string registeredArities = FormatRegisteredArities(functions, name);
                issues.Add(new AutomationValidationIssue(
                    AutomationValidationSeverity.Error,
                    AutomationCallbackKind.ScalarFunction,
                    name,
                    surface,
                    location,
                    $"Scalar function '{name}' is referenced by {surface} at {location} with arity {reference.Arity}, but the host registry has {registeredArities}. Register it with arity {reference.Arity} or update the metadata expression.",
                    reference.Arity));
                continue;
            }

            issues.Add(new AutomationValidationIssue(
                AutomationValidationSeverity.Error,
                AutomationCallbackKind.ScalarFunction,
                name,
                surface,
                location,
                $"Scalar function '{name}' is referenced by {surface} at {location} with arity {reference.Arity}, but it is not registered in the host function registry.",
                reference.Arity));
        }
    }

    private static void ValidateValidationRules(
        IReadOnlyList<DbAutomationValidationRuleReference>? references,
        DbValidationRuleRegistry validationRules,
        List<AutomationValidationIssue> issues)
    {
        if (references is null || references.Count == 0)
            return;

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (DbAutomationValidationRuleReference reference in references)
        {
            string name = NormalizeName(reference.Name);
            string surface = NormalizeSurface(reference.Surface);
            string location = NormalizeLocation(reference.Location);
            if (!seen.Add(CreateKey(name, surface, location)))
                continue;

            if (string.IsNullOrWhiteSpace(name))
            {
                issues.Add(new AutomationValidationIssue(
                    AutomationValidationSeverity.Error,
                    AutomationCallbackKind.ValidationRule,
                    name,
                    surface,
                    location,
                    $"Validation rule reference at {surface}:{location} has no rule name."));
                continue;
            }

            if (!validationRules.ContainsRuleName(name))
            {
                issues.Add(new AutomationValidationIssue(
                    AutomationValidationSeverity.Error,
                    AutomationCallbackKind.ValidationRule,
                    name,
                    surface,
                    location,
                    $"Validation rule '{name}' is referenced by {surface} at {location}, but it is not registered in the host validation rule registry."));
            }
        }
    }

    private static AutomationValidationResult CreateResult(IReadOnlyList<AutomationValidationIssue> issues)
        => new(
            !issues.Any(static issue => issue.Severity == AutomationValidationSeverity.Error),
            issues.ToArray());

    private static string FormatRegisteredArities(DbFunctionRegistry functions, string name)
    {
        int[] arities = functions.ScalarFunctions
            .Where(function => string.Equals(function.Name, name, StringComparison.OrdinalIgnoreCase))
            .Select(static function => function.Arity)
            .Order()
            .ToArray();

        return arities.Length == 1
            ? $"arity {arities[0]}"
            : $"arities {string.Join(", ", arities)}";
    }

    private static string NormalizeName(string? value)
        => value?.Trim() ?? string.Empty;

    private static string NormalizeSurface(string? value)
        => string.IsNullOrWhiteSpace(value) ? UnknownSurface : value.Trim();

    private static string NormalizeLocation(string? value)
        => string.IsNullOrWhiteSpace(value) ? UnknownLocation : value.Trim();

    private static string CreateKey(params string[] parts)
        => string.Join('\u001f', parts);

    private sealed record DuplicateOccurrence(
        string Name,
        string Surface,
        string Location,
        int Count,
        int? Arity)
    {
        public DuplicateOccurrence Increment()
            => this with { Count = Count + 1 };
    }
}
