namespace CSharpDB.Primitives;

public delegate DbValue DbScalarFunctionDelegate(
    DbScalarFunctionContext context,
    ReadOnlySpan<DbValue> arguments);

public sealed record DbScalarFunctionContext(string FunctionName)
{
    public IReadOnlyDictionary<string, string> Metadata { get; init; } = EmptyStringDictionary.Instance;

    internal static DbScalarFunctionContext Create(
        string functionName,
        IReadOnlyDictionary<string, string>? metadata)
        => new(functionName)
        {
            Metadata = metadata ?? EmptyStringDictionary.Instance,
        };

    private static class EmptyStringDictionary
    {
        public static readonly IReadOnlyDictionary<string, string> Instance =
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    }
}

public sealed record DbScalarFunctionOptions(
    DbType? ReturnType = null,
    bool IsDeterministic = false,
    bool NullPropagating = false,
    string? Description = null,
    IReadOnlyList<DbExtensionCapabilityRequest>? AdditionalCapabilities = null,
    IReadOnlyDictionary<string, string>? Metadata = null,
    bool CanRunWithoutFrom = false);

public sealed class DbScalarFunctionDefinition
{
    private readonly DbScalarFunctionDelegate _invoke;

    internal DbScalarFunctionDefinition(
        string name,
        int arity,
        DbScalarFunctionOptions options,
        DbScalarFunctionDelegate invoke)
    {
        Name = name;
        Arity = arity;
        Options = options;
        Descriptor = DbHostCallbackDescriptorFactory.CreateScalar(name, arity, options);
        _invoke = invoke;
    }

    public string Name { get; }

    public int Arity { get; }

    public DbScalarFunctionOptions Options { get; }

    public DbHostCallbackDescriptor Descriptor { get; }

    public DbValue Invoke(ReadOnlySpan<DbValue> arguments)
        => Invoke(arguments, metadata: null);

    public DbValue Invoke(
        ReadOnlySpan<DbValue> arguments,
        IReadOnlyDictionary<string, string>? metadata)
        => InvokeCore(arguments, metadata, policyDecision: null);

    public DbValue Invoke(
        ReadOnlySpan<DbValue> arguments,
        IReadOnlyDictionary<string, string>? metadata,
        DbExtensionPolicy policy,
        DbExtensionHostMode hostMode = DbExtensionHostMode.Embedded)
    {
        ArgumentNullException.ThrowIfNull(policy);

        DbExtensionPolicyDecision decision = DbExtensionPolicyEvaluator.Evaluate(
            Descriptor,
            policy,
            hostMode);
        if (!decision.Allowed)
        {
            DbCallbackDiagnostics.WritePolicyDeniedInvocation(Descriptor, metadata, decision);
            throw new DbCallbackPolicyException(Descriptor, decision);
        }

        return InvokeCore(arguments, metadata, decision);
    }

    private DbValue InvokeCore(
        ReadOnlySpan<DbValue> arguments,
        IReadOnlyDictionary<string, string>? metadata,
        DbExtensionPolicyDecision? policyDecision)
    {
        DbScalarFunctionContext context = DbScalarFunctionContext.Create(Name, metadata);
        if (!DbCallbackDiagnostics.IsInvocationEnabled)
            return _invoke(context, arguments);

        long started = DbCallbackDiagnostics.GetTimestamp();
        try
        {
            DbValue result = _invoke(context, arguments);
            DbCallbackDiagnostics.WriteScalarInvocation(
                Name,
                Arity,
                context.Metadata,
                DbCallbackDiagnostics.GetElapsedTime(started),
                succeeded: true,
                canceled: false,
                exceptionMessage: null,
                policyDecision: policyDecision);
            return result;
        }
        catch (OperationCanceledException ex)
        {
            DbCallbackDiagnostics.WriteScalarInvocation(
                Name,
                Arity,
                context.Metadata,
                DbCallbackDiagnostics.GetElapsedTime(started),
                succeeded: false,
                canceled: true,
                exceptionMessage: ex.Message,
                policyDecision: policyDecision,
                errorCode: "Canceled",
                exceptionType: ex.GetType().FullName);
            throw;
        }
        catch (Exception ex)
        {
            DbCallbackDiagnostics.WriteScalarInvocation(
                Name,
                Arity,
                context.Metadata,
                DbCallbackDiagnostics.GetElapsedTime(started),
                succeeded: false,
                canceled: false,
                exceptionMessage: ex.Message,
                policyDecision: policyDecision,
                errorCode: "Exception",
                exceptionType: ex.GetType().FullName);
            throw;
        }
    }
}

public sealed class DbFunctionRegistry
{
    private readonly Dictionary<string, Dictionary<int, DbScalarFunctionDefinition>> _scalarFunctions;
    private readonly DbScalarFunctionDefinition[] _scalarFunctionList;
    private readonly DbHostCallbackDescriptor[] _callbackList;

    public static DbFunctionRegistry Empty { get; } = new();

    private DbFunctionRegistry()
    {
        _scalarFunctions = new Dictionary<string, Dictionary<int, DbScalarFunctionDefinition>>(StringComparer.OrdinalIgnoreCase);
        _scalarFunctionList = [];
        _callbackList = [];
    }

    internal DbFunctionRegistry(Dictionary<string, Dictionary<int, DbScalarFunctionDefinition>> scalarFunctions)
    {
        _scalarFunctions = scalarFunctions;
        _scalarFunctionList = scalarFunctions.Values
            .SelectMany(static byArity => byArity.Values)
            .OrderBy(static definition => definition.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(static definition => definition.Arity)
            .ToArray();
        _callbackList = _scalarFunctionList
            .Select(static definition => definition.Descriptor)
            .ToArray();
    }

    public IReadOnlyCollection<DbScalarFunctionDefinition> ScalarFunctions => _scalarFunctionList;

    public IReadOnlyCollection<DbHostCallbackDescriptor> Callbacks => _callbackList;

    public static DbFunctionRegistry Create(Action<DbFunctionRegistryBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var builder = new DbFunctionRegistryBuilder();
        configure(builder);
        return builder.Build();
    }

    public bool TryGetScalar(string name, int arity, out DbScalarFunctionDefinition definition)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        if (_scalarFunctions.TryGetValue(name, out var byArity) &&
            byArity.TryGetValue(arity, out definition!))
        {
            return true;
        }

        definition = null!;
        return false;
    }

    public bool ContainsScalarName(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        return _scalarFunctions.ContainsKey(name);
    }
}

public sealed class DbFunctionRegistryBuilder
{
    private static readonly HashSet<string> s_reservedFunctionNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "TEXT",
        "COUNT",
        "SUM",
        "AVG",
        "MIN",
        "MAX",
    };

    private readonly Dictionary<string, Dictionary<int, DbScalarFunctionDefinition>> _scalarFunctions =
        new(StringComparer.OrdinalIgnoreCase);

    public DbFunctionRegistryBuilder AddScalar(
        string name,
        int arity,
        DbScalarFunctionOptions? options,
        DbScalarFunctionDelegate invoke)
    {
        string normalizedName = ValidateFunctionName(name);
        ArgumentOutOfRangeException.ThrowIfNegative(arity);
        ArgumentNullException.ThrowIfNull(invoke);

        if (s_reservedFunctionNames.Contains(normalizedName))
            throw new ArgumentException($"Function name '{name}' is reserved and cannot be overridden.", nameof(name));

        if (_scalarFunctions.ContainsKey(normalizedName))
            throw new ArgumentException($"Scalar function '{name}' is already registered.", nameof(name));

        var byArity = new Dictionary<int, DbScalarFunctionDefinition>();
        _scalarFunctions.Add(normalizedName, byArity);

        byArity.Add(
            arity,
            new DbScalarFunctionDefinition(
                normalizedName,
                arity,
                options ?? new DbScalarFunctionOptions(),
                invoke));
        return this;
    }

    public DbFunctionRegistryBuilder AddScalar(
        string name,
        int arity,
        DbScalarFunctionDelegate invoke)
        => AddScalar(name, arity, options: null, invoke);

    public DbFunctionRegistry Build()
    {
        if (_scalarFunctions.Count == 0)
            return DbFunctionRegistry.Empty;

        var copy = new Dictionary<string, Dictionary<int, DbScalarFunctionDefinition>>(StringComparer.OrdinalIgnoreCase);
        foreach ((string name, Dictionary<int, DbScalarFunctionDefinition> byArity) in _scalarFunctions)
            copy[name] = new Dictionary<int, DbScalarFunctionDefinition>(byArity);

        return new DbFunctionRegistry(copy);
    }

    internal static bool IsReservedFunctionName(string name)
        => s_reservedFunctionNames.Contains(name);

    private static string ValidateFunctionName(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        string trimmed = name.Trim();
        if (!IsIdentifierStart(trimmed[0]))
            throw new ArgumentException($"Function name '{name}' is not a valid SQL identifier.", nameof(name));

        for (int i = 1; i < trimmed.Length; i++)
        {
            char ch = trimmed[i];
            if (!char.IsLetterOrDigit(ch) && ch != '_')
                throw new ArgumentException($"Function name '{name}' is not a valid SQL identifier.", nameof(name));
        }

        return trimmed;
    }

    private static bool IsIdentifierStart(char value)
        => char.IsLetter(value) || value == '_';
}
