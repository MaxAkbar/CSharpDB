namespace CSharpDB.Primitives;

public delegate ValueTask<DbCommandResult> DbCommandDelegate(
    DbCommandContext context,
    CancellationToken ct);

public sealed record DbCommandContext(
    string CommandName,
    IReadOnlyDictionary<string, DbValue> Arguments,
    IReadOnlyDictionary<string, string> Metadata);

public sealed record DbCommandOptions(
    string? Description = null,
    TimeSpan? Timeout = null,
    bool IsLongRunning = false,
    IReadOnlyList<DbExtensionCapabilityRequest>? AdditionalCapabilities = null,
    IReadOnlyDictionary<string, string>? Metadata = null);

public sealed record DbCommandResult(
    bool Succeeded,
    string? Message = null,
    DbValue Value = default)
{
    public static DbCommandResult Success(string? message = null, DbValue value = default)
        => new(true, message, value);

    public static DbCommandResult Failure(string message, DbValue value = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(message);
        return new(false, message, value);
    }
}

public sealed class DbCommandDefinition
{
    private const string CommandTimeoutDataKey = "CSharpDB.CommandTimedOut";
    private readonly DbCommandDelegate _invoke;

    internal DbCommandDefinition(
        string name,
        DbCommandOptions options,
        DbCommandDelegate invoke)
    {
        Name = name;
        Options = options;
        Descriptor = DbHostCallbackDescriptorFactory.CreateCommand(name, options);
        _invoke = invoke;
    }

    public string Name { get; }

    public DbCommandOptions Options { get; }

    public DbHostCallbackDescriptor Descriptor { get; }

    public ValueTask<DbCommandResult> InvokeAsync(
        IReadOnlyDictionary<string, DbValue>? arguments = null,
        IReadOnlyDictionary<string, string>? metadata = null,
        CancellationToken ct = default)
    {
        var context = new DbCommandContext(
            Name,
            arguments ?? EmptyDbValueDictionary.Instance,
            metadata ?? EmptyStringDictionary.Instance);
        if (DbCallbackDiagnostics.IsInvocationEnabled)
            return InvokeWithDiagnosticsAsync(context, ct);

        return InvokeCoreAsync(context, ct);
    }

    private ValueTask<DbCommandResult> InvokeCoreAsync(
        DbCommandContext context,
        CancellationToken ct)
    {
        return Options.Timeout is { } timeout
            ? InvokeWithTimeoutAsync(context, timeout, ct)
            : _invoke(context, ct);
    }

    private async ValueTask<DbCommandResult> InvokeWithDiagnosticsAsync(
        DbCommandContext context,
        CancellationToken ct)
    {
        long started = DbCallbackDiagnostics.GetTimestamp();
        try
        {
            DbCommandResult result = await InvokeCoreAsync(context, ct).ConfigureAwait(false);
            DbCallbackDiagnostics.WriteCommandInvocation(
                Name,
                context.Metadata,
                DbCallbackDiagnostics.GetElapsedTime(started),
                succeeded: result.Succeeded,
                timedOut: false,
                canceled: false,
                result.Message,
                exceptionMessage: null);
            return result;
        }
        catch (TimeoutException ex) when (IsCommandTimeoutException(ex))
        {
            DbCallbackDiagnostics.WriteCommandInvocation(
                Name,
                context.Metadata,
                DbCallbackDiagnostics.GetElapsedTime(started),
                succeeded: false,
                timedOut: true,
                canceled: false,
                resultMessage: null,
                ex.Message);
            throw;
        }
        catch (OperationCanceledException ex)
        {
            DbCallbackDiagnostics.WriteCommandInvocation(
                Name,
                context.Metadata,
                DbCallbackDiagnostics.GetElapsedTime(started),
                succeeded: false,
                timedOut: false,
                canceled: true,
                resultMessage: null,
                ex.Message);
            throw;
        }
        catch (Exception ex)
        {
            DbCallbackDiagnostics.WriteCommandInvocation(
                Name,
                context.Metadata,
                DbCallbackDiagnostics.GetElapsedTime(started),
                succeeded: false,
                timedOut: false,
                canceled: false,
                resultMessage: null,
                ex.Message);
            throw;
        }
    }

    private async ValueTask<DbCommandResult> InvokeWithTimeoutAsync(
        DbCommandContext context,
        TimeSpan timeout,
        CancellationToken ct)
    {
        using var timeoutCts = new CancellationTokenSource(timeout);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);

        try
        {
            ValueTask<DbCommandResult> invocation = _invoke(context, linkedCts.Token);
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
        var exception = new TimeoutException($"Command '{Name}' timed out after {FormatTimeout(timeout)}.", inner);
        exception.Data[CommandTimeoutDataKey] = true;
        return exception;
    }

    private static bool IsCommandTimeoutException(TimeoutException exception)
        => exception.Data[CommandTimeoutDataKey] is true;

    private static string FormatTimeout(TimeSpan timeout)
        => timeout.TotalMilliseconds < 1000
            ? $"{timeout.TotalMilliseconds:0.###}ms"
            : $"{timeout.TotalSeconds:0.###}s";

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

public sealed class DbCommandRegistry
{
    private readonly Dictionary<string, DbCommandDefinition> _commands;
    private readonly DbCommandDefinition[] _commandList;
    private readonly DbHostCallbackDescriptor[] _callbackList;

    public static DbCommandRegistry Empty { get; } = new();

    private DbCommandRegistry()
    {
        _commands = new Dictionary<string, DbCommandDefinition>(StringComparer.OrdinalIgnoreCase);
        _commandList = [];
        _callbackList = [];
    }

    internal DbCommandRegistry(Dictionary<string, DbCommandDefinition> commands)
    {
        _commands = commands;
        _commandList = commands.Values
            .OrderBy(static definition => definition.Name, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        _callbackList = _commandList
            .Select(static definition => definition.Descriptor)
            .ToArray();
    }

    public IReadOnlyCollection<DbCommandDefinition> Commands => _commandList;

    public IReadOnlyCollection<DbHostCallbackDescriptor> Callbacks => _callbackList;

    public static DbCommandRegistry Create(Action<DbCommandRegistryBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var builder = new DbCommandRegistryBuilder();
        configure(builder);
        return builder.Build();
    }

    public bool TryGetCommand(string name, out DbCommandDefinition definition)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        if (_commands.TryGetValue(name, out definition!))
            return true;

        definition = null!;
        return false;
    }

    public bool ContainsCommandName(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        return _commands.ContainsKey(name);
    }
}

public sealed class DbCommandRegistryBuilder
{
    private readonly Dictionary<string, DbCommandDefinition> _commands =
        new(StringComparer.OrdinalIgnoreCase);

    public DbCommandRegistryBuilder AddCommand(
        string name,
        DbCommandOptions? options,
        DbCommandDelegate invoke)
    {
        string normalizedName = ValidateCommandName(name);
        ValidateCommandOptions(options);
        ArgumentNullException.ThrowIfNull(invoke);

        if (_commands.ContainsKey(normalizedName))
            throw new ArgumentException($"Command '{name}' is already registered.", nameof(name));

        _commands.Add(
            normalizedName,
            new DbCommandDefinition(
                normalizedName,
                options ?? new DbCommandOptions(),
                invoke));
        return this;
    }

    public DbCommandRegistryBuilder AddAsyncCommand(
        string name,
        DbCommandOptions? options,
        Func<DbCommandContext, CancellationToken, Task<DbCommandResult>> invoke)
    {
        ArgumentNullException.ThrowIfNull(invoke);
        return AddCommand(
            name,
            options,
            (context, ct) => new ValueTask<DbCommandResult>(invoke(context, ct)));
    }

    public DbCommandRegistryBuilder AddAsyncCommand(
        string name,
        Func<DbCommandContext, CancellationToken, Task<DbCommandResult>> invoke)
        => AddAsyncCommand(name, options: null, invoke);

    public DbCommandRegistryBuilder AddCommand(
        string name,
        DbCommandDelegate invoke)
        => AddCommand(name, options: null, invoke);

    public DbCommandRegistryBuilder AddCommand(
        string name,
        DbCommandOptions? options,
        Func<DbCommandContext, DbCommandResult> invoke)
    {
        ArgumentNullException.ThrowIfNull(invoke);
        return AddCommand(
            name,
            options,
            (context, _) => ValueTask.FromResult(invoke(context)));
    }

    public DbCommandRegistryBuilder AddCommand(
        string name,
        Func<DbCommandContext, DbCommandResult> invoke)
        => AddCommand(name, options: null, invoke);

    public DbCommandRegistry Build()
    {
        if (_commands.Count == 0)
            return DbCommandRegistry.Empty;

        return new DbCommandRegistry(new Dictionary<string, DbCommandDefinition>(_commands, StringComparer.OrdinalIgnoreCase));
    }

    private static string ValidateCommandName(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        string trimmed = name.Trim();
        if (!IsIdentifierStart(trimmed[0]))
            throw new ArgumentException($"Command name '{name}' is not a valid identifier.", nameof(name));

        for (int i = 1; i < trimmed.Length; i++)
        {
            char ch = trimmed[i];
            if (!char.IsLetterOrDigit(ch) && ch != '_')
                throw new ArgumentException($"Command name '{name}' is not a valid identifier.", nameof(name));
        }

        return trimmed;
    }

    private static void ValidateCommandOptions(DbCommandOptions? options)
    {
        if (options?.Timeout is { } timeout && timeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(options), timeout, "Command timeout must be greater than zero.");
    }

    private static bool IsIdentifierStart(char value)
        => char.IsLetter(value) || value == '_';
}
