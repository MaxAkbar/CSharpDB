namespace CSharpDB.Primitives;

public enum DbValidationRuleScope
{
    Field = 0,
    Form = 1,
}

public delegate ValueTask<DbValidationRuleResult> DbValidationRuleDelegate(
    DbValidationRuleContext context,
    CancellationToken ct);

public sealed record DbValidationFailure(
    string? FieldName,
    string Message,
    string? RuleId = null);

public sealed record DbValidationRuleResult(
    bool Succeeded,
    IReadOnlyList<DbValidationFailure>? Failures = null,
    string? Message = null)
{
    public static DbValidationRuleResult Success(string? message = null)
        => new(true, Failures: [], message);

    public static DbValidationRuleResult Failure(string message, string? fieldName = null, string? ruleId = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(message);
        return new(false, [new DbValidationFailure(fieldName, message, ruleId)], message);
    }

    public static DbValidationRuleResult Failure(IEnumerable<DbValidationFailure> failures, string? message = null)
    {
        ArgumentNullException.ThrowIfNull(failures);
        DbValidationFailure[] failureList = failures
            .Where(static failure => !string.IsNullOrWhiteSpace(failure.Message))
            .ToArray();

        return new(false, failureList, message);
    }
}

public sealed record DbValidationRuleContext(
    string RuleName,
    DbValidationRuleScope Scope,
    IReadOnlyDictionary<string, DbValue> Record,
    IReadOnlyDictionary<string, DbValue> Parameters,
    IReadOnlyDictionary<string, string> Metadata)
{
    public string? FormId { get; init; }
    public string? FormName { get; init; }
    public string? TableName { get; init; }
    public string? ControlId { get; init; }
    public string? FieldName { get; init; }
    public DbValue Value { get; init; } = DbValue.Null;
    public string? FallbackMessage { get; init; }

    public static DbValidationRuleContext Create(
        string ruleName,
        DbValidationRuleScope scope,
        IReadOnlyDictionary<string, DbValue>? record = null,
        IReadOnlyDictionary<string, DbValue>? parameters = null,
        IReadOnlyDictionary<string, string>? metadata = null)
        => new(
            ruleName,
            scope,
            record ?? EmptyDbValueDictionary.Instance,
            parameters ?? EmptyDbValueDictionary.Instance,
            metadata ?? EmptyStringDictionary.Instance);

    private static class EmptyDbValueDictionary
    {
        public static readonly IReadOnlyDictionary<string, DbValue> Instance =
            new Dictionary<string, DbValue>(StringComparer.OrdinalIgnoreCase);
    }

    private static class EmptyStringDictionary
    {
        public static readonly IReadOnlyDictionary<string, string> Instance =
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    }
}

public sealed record DbValidationRuleOptions(
    string? Description = null,
    TimeSpan? Timeout = null,
    bool IsLongRunning = false,
    IReadOnlyList<DbExtensionCapabilityRequest>? AdditionalCapabilities = null,
    IReadOnlyDictionary<string, string>? Metadata = null);

public sealed class DbValidationRuleDefinition
{
    private const string ValidationTimeoutDataKey = "CSharpDB.ValidationRuleTimedOut";
    private readonly DbValidationRuleDelegate _invoke;

    internal DbValidationRuleDefinition(
        string name,
        DbValidationRuleOptions options,
        DbValidationRuleDelegate invoke)
    {
        Name = name;
        Options = options;
        Descriptor = DbHostCallbackDescriptorFactory.CreateValidationRule(name, options);
        _invoke = invoke;
    }

    public string Name { get; }

    public DbValidationRuleOptions Options { get; }

    public DbHostCallbackDescriptor Descriptor { get; }

    public ValueTask<DbValidationRuleResult> InvokeAsync(
        DbValidationRuleContext context,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (DbCallbackDiagnostics.IsInvocationEnabled)
            return InvokeWithDiagnosticsAsync(context, ct);

        return InvokeCoreAsync(context, ct);
    }

    public ValueTask<DbValidationRuleResult> InvokeAsync(
        DbValidationRuleContext context,
        DbExtensionPolicy policy,
        DbExtensionHostMode hostMode = DbExtensionHostMode.Embedded,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(policy);

        DbExtensionPolicyDecision decision = DbExtensionPolicyEvaluator.Evaluate(
            Descriptor,
            policy,
            hostMode);

        if (!decision.Allowed)
        {
            DbCallbackDiagnostics.WritePolicyDeniedInvocation(Descriptor, context.Metadata, decision);
            throw new DbCallbackPolicyException(Descriptor, decision);
        }

        if (DbCallbackDiagnostics.IsInvocationEnabled)
            return InvokeWithDiagnosticsAsync(context, ct, decision.Timeout, decision);

        return InvokeCoreAsync(context, ct, decision.Timeout);
    }

    private ValueTask<DbValidationRuleResult> InvokeCoreAsync(
        DbValidationRuleContext context,
        CancellationToken ct,
        TimeSpan? timeoutOverride = null)
    {
        TimeSpan? timeout = timeoutOverride ?? Options.Timeout;
        return timeout is { } value
            ? InvokeWithTimeoutAsync(context, value, ct)
            : _invoke(context, ct);
    }

    private async ValueTask<DbValidationRuleResult> InvokeWithDiagnosticsAsync(
        DbValidationRuleContext context,
        CancellationToken ct,
        TimeSpan? timeoutOverride = null,
        DbExtensionPolicyDecision? policyDecision = null)
    {
        long started = DbCallbackDiagnostics.GetTimestamp();
        try
        {
            DbValidationRuleResult result = await InvokeCoreAsync(context, ct, timeoutOverride).ConfigureAwait(false);
            DbCallbackDiagnostics.WriteValidationInvocation(
                Name,
                context.Metadata,
                DbCallbackDiagnostics.GetElapsedTime(started),
                succeeded: result.Succeeded,
                timedOut: false,
                canceled: false,
                result.Message,
                exceptionMessage: null,
                policyDecision: policyDecision);
            return result;
        }
        catch (TimeoutException ex) when (IsValidationTimeoutException(ex))
        {
            DbCallbackDiagnostics.WriteValidationInvocation(
                Name,
                context.Metadata,
                DbCallbackDiagnostics.GetElapsedTime(started),
                succeeded: false,
                timedOut: true,
                canceled: false,
                resultMessage: null,
                exceptionMessage: ex.Message,
                policyDecision: policyDecision,
                errorCode: "Timeout",
                exceptionType: ex.GetType().FullName);
            throw;
        }
        catch (OperationCanceledException ex)
        {
            DbCallbackDiagnostics.WriteValidationInvocation(
                Name,
                context.Metadata,
                DbCallbackDiagnostics.GetElapsedTime(started),
                succeeded: false,
                timedOut: false,
                canceled: true,
                resultMessage: null,
                exceptionMessage: ex.Message,
                policyDecision: policyDecision,
                errorCode: "Canceled",
                exceptionType: ex.GetType().FullName);
            throw;
        }
        catch (Exception ex)
        {
            DbCallbackDiagnostics.WriteValidationInvocation(
                Name,
                context.Metadata,
                DbCallbackDiagnostics.GetElapsedTime(started),
                succeeded: false,
                timedOut: false,
                canceled: false,
                resultMessage: null,
                exceptionMessage: ex.Message,
                policyDecision: policyDecision,
                errorCode: "Exception",
                exceptionType: ex.GetType().FullName);
            throw;
        }
    }

    private async ValueTask<DbValidationRuleResult> InvokeWithTimeoutAsync(
        DbValidationRuleContext context,
        TimeSpan timeout,
        CancellationToken ct)
    {
        using var timeoutCts = new CancellationTokenSource(timeout);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);

        try
        {
            ValueTask<DbValidationRuleResult> invocation = _invoke(context, linkedCts.Token);
            if (invocation.IsCompletedSuccessfully)
                return invocation.Result;

            return await invocation.AsTask().WaitAsync(linkedCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException ex) when (timeoutCts.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            throw CreateTimeoutException(timeout, ex);
        }
    }

    private TimeoutException CreateTimeoutException(TimeSpan timeout, Exception inner)
    {
        var exception = new TimeoutException($"Validation rule '{Name}' timed out after {FormatTimeout(timeout)}.", inner);
        exception.Data[ValidationTimeoutDataKey] = true;
        return exception;
    }

    private static bool IsValidationTimeoutException(TimeoutException exception)
        => exception.Data[ValidationTimeoutDataKey] is true;

    private static string FormatTimeout(TimeSpan timeout)
        => timeout.TotalMilliseconds < 1000
            ? $"{timeout.TotalMilliseconds:0.###}ms"
            : $"{timeout.TotalSeconds:0.###}s";
}

public sealed class DbValidationRuleRegistry
{
    private readonly Dictionary<string, DbValidationRuleDefinition> _rules;
    private readonly DbValidationRuleDefinition[] _ruleList;
    private readonly DbHostCallbackDescriptor[] _callbackList;

    public static DbValidationRuleRegistry Empty { get; } = new();

    private DbValidationRuleRegistry()
    {
        _rules = new Dictionary<string, DbValidationRuleDefinition>(StringComparer.OrdinalIgnoreCase);
        _ruleList = [];
        _callbackList = [];
    }

    internal DbValidationRuleRegistry(Dictionary<string, DbValidationRuleDefinition> rules)
    {
        _rules = rules;
        _ruleList = rules.Values
            .OrderBy(static definition => definition.Name, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        _callbackList = _ruleList
            .Select(static definition => definition.Descriptor)
            .ToArray();
    }

    public IReadOnlyCollection<DbValidationRuleDefinition> Rules => _ruleList;

    public IReadOnlyCollection<DbHostCallbackDescriptor> Callbacks => _callbackList;

    public static DbValidationRuleRegistry Create(Action<DbValidationRuleRegistryBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var builder = new DbValidationRuleRegistryBuilder();
        configure(builder);
        return builder.Build();
    }

    public bool TryGetRule(string name, out DbValidationRuleDefinition definition)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        if (_rules.TryGetValue(name, out definition!))
            return true;

        definition = null!;
        return false;
    }

    public bool ContainsRuleName(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        return _rules.ContainsKey(name);
    }
}

public sealed class DbValidationRuleRegistryBuilder
{
    private readonly Dictionary<string, DbValidationRuleDefinition> _rules =
        new(StringComparer.OrdinalIgnoreCase);

    public DbValidationRuleRegistryBuilder AddRule(
        string name,
        DbValidationRuleOptions? options,
        DbValidationRuleDelegate invoke)
    {
        string normalizedName = ValidateRuleName(name);
        ValidateRuleOptions(options);
        ArgumentNullException.ThrowIfNull(invoke);

        if (_rules.ContainsKey(normalizedName))
            throw new ArgumentException($"Validation rule '{name}' is already registered.", nameof(name));

        _rules.Add(
            normalizedName,
            new DbValidationRuleDefinition(
                normalizedName,
                options ?? new DbValidationRuleOptions(),
                invoke));
        return this;
    }

    public DbValidationRuleRegistryBuilder AddRule(
        string name,
        DbValidationRuleDelegate invoke)
        => AddRule(name, options: null, invoke);

    public DbValidationRuleRegistryBuilder AddRule(
        string name,
        DbValidationRuleOptions? options,
        Func<DbValidationRuleContext, DbValidationRuleResult> invoke)
    {
        ArgumentNullException.ThrowIfNull(invoke);
        return AddRule(
            name,
            options,
            (context, _) => ValueTask.FromResult(invoke(context)));
    }

    public DbValidationRuleRegistryBuilder AddRule(
        string name,
        Func<DbValidationRuleContext, DbValidationRuleResult> invoke)
        => AddRule(name, options: null, invoke);

    public DbValidationRuleRegistry Build()
    {
        if (_rules.Count == 0)
            return DbValidationRuleRegistry.Empty;

        return new DbValidationRuleRegistry(new Dictionary<string, DbValidationRuleDefinition>(_rules, StringComparer.OrdinalIgnoreCase));
    }

    private static string ValidateRuleName(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        string trimmed = name.Trim();
        if (!IsIdentifierStart(trimmed[0]))
            throw new ArgumentException($"Validation rule name '{name}' is not a valid identifier.", nameof(name));

        for (int i = 1; i < trimmed.Length; i++)
        {
            char ch = trimmed[i];
            if (!char.IsLetterOrDigit(ch) && ch != '_')
                throw new ArgumentException($"Validation rule name '{name}' is not a valid identifier.", nameof(name));
        }

        return trimmed;
    }

    private static void ValidateRuleOptions(DbValidationRuleOptions? options)
    {
        if (options?.Timeout is { } timeout && timeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(options), timeout, "Validation rule timeout must be greater than zero.");
    }

    private static bool IsIdentifierStart(char value)
        => char.IsLetter(value) || value == '_';
}
