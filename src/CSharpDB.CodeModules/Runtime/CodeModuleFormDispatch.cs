using System.Reflection;
using System.Runtime.Loader;
using CSharpDB.CodeModules.Runtime;

namespace CSharpDB.CodeModules;

public interface ICodeModuleFormEventDispatcher
{
    Task<CodeModuleFormDispatchResult> DispatchAsync(
        CodeModuleHandler handler,
        CodeModuleFormEventDispatchContext context,
        CancellationToken ct = default);
}

public sealed record CodeModuleFormEventDispatchContext(
    string? FormId,
    string? FormName,
    string? TableName,
    string EventName,
    IReadOnlyDictionary<string, object?>? Record,
    IReadOnlyDictionary<string, object?>? BindingArguments,
    IReadOnlyDictionary<string, object?>? RuntimeArguments,
    IReadOnlyDictionary<string, string>? Metadata,
    IFormCommandApi CommandApi,
    bool IsCancelable,
    string? ControlId = null,
    string? ControlType = null);

public sealed record CodeModuleFormDispatchResult(bool Succeeded, string? Message = null)
{
    public static CodeModuleFormDispatchResult Success(string? message = null) => new(true, message);

    public static CodeModuleFormDispatchResult Failure(string message)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(message);
        return new(false, message);
    }
}

public sealed class NullCodeModuleFormEventDispatcher : ICodeModuleFormEventDispatcher
{
    public static NullCodeModuleFormEventDispatcher Instance { get; } = new();

    private NullCodeModuleFormEventDispatcher()
    {
    }

    public Task<CodeModuleFormDispatchResult> DispatchAsync(
        CodeModuleHandler handler,
        CodeModuleFormEventDispatchContext context,
        CancellationToken ct = default)
        => Task.FromResult(CodeModuleFormDispatchResult.Failure(
            "C# code module execution is not configured for this host."));
}

public sealed class CodeModuleFormEventDispatcher(
    CSharpDbCodeModuleClient client,
    CodeModuleRuntimeOptions options) : ICodeModuleFormEventDispatcher
{
    public async Task<CodeModuleFormDispatchResult> DispatchAsync(
        CodeModuleHandler handler,
        CodeModuleFormEventDispatchContext context,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(context);

        if (!options.EnableInProcessExecution)
            return CodeModuleFormDispatchResult.Failure("C# code module execution is disabled by the host.");

        CodeModuleBuildResult build = await client.BuildAsync(ct);
        if (!build.Succeeded || build.AssemblyBytes is null)
        {
            string message = build.Diagnostics.FirstOrDefault(diagnostic => diagnostic.Severity == CodeModuleDiagnosticSeverity.Error)?.Message
                ?? "C# code modules failed to build.";
            return CodeModuleFormDispatchResult.Failure(message);
        }

        CodeModuleTrustState trust = await client.GetTrustStateAsync(build.ModuleSetHash, ct);
        if (!trust.IsTrusted)
        {
            return CodeModuleFormDispatchResult.Failure(
                $"C# code modules are not trusted locally for module set '{build.ModuleSetHash}'. Build and trust the current code modules before running this handler.");
        }

        AssemblyLoadContext loadContext = new($"csharpdb-code-modules-{Guid.NewGuid():N}", isCollectible: true);
        try
        {
            using var stream = new MemoryStream(build.AssemblyBytes);
            Assembly assembly = loadContext.LoadFromStream(stream);
            return await InvokeHandlerAsync(assembly, handler, context, ct);
        }
        finally
        {
            loadContext.Unload();
        }
    }

    private static async Task<CodeModuleFormDispatchResult> InvokeHandlerAsync(
        Assembly assembly,
        CodeModuleHandler handler,
        CodeModuleFormEventDispatchContext context,
        CancellationToken ct)
    {
        Type? moduleType = assembly.GetType(handler.TypeName, throwOnError: false, ignoreCase: false)
            ?? assembly.GetTypes().FirstOrDefault(type => string.Equals(type.Name, handler.TypeName, StringComparison.Ordinal));
        if (moduleType is null)
            return CodeModuleFormDispatchResult.Failure($"C# code module type '{handler.TypeName}' was not found.");

        if (!typeof(FormCodeModule).IsAssignableFrom(moduleType))
            return CodeModuleFormDispatchResult.Failure($"C# code module type '{handler.TypeName}' must derive from FormCodeModule.");

        MethodInfo? method = moduleType.GetMethod(
            handler.MethodName,
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        if (method is null)
            return CodeModuleFormDispatchResult.Failure($"C# code module handler '{handler.MethodName}' was not found on '{handler.TypeName}'.");

        object? instance = Activator.CreateInstance(moduleType);
        if (instance is not FormCodeModule formModule)
            return CodeModuleFormDispatchResult.Failure($"C# code module type '{handler.TypeName}' could not be created.");

        FormEventContext eventContext = CreateEventContext(context);
        formModule.Initialize(new FormCodeModuleRuntimeContext(
            new FormModuleRecord(ToMutableRecord(context.Record)),
            context.CommandApi,
            eventContext));

        try
        {
            object?[]? args = BuildMethodArguments(method, eventContext);
            object? result = method.Invoke(formModule, args);
            await AwaitResultAsync(result, ct);
        }
        catch (TargetInvocationException ex) when (ex.InnerException is not null)
        {
            return CodeModuleFormDispatchResult.Failure(
                $"C# code module handler '{handler.MethodName}' failed: {ex.InnerException.Message}");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return CodeModuleFormDispatchResult.Failure(
                $"C# code module handler '{handler.MethodName}' failed: {ex.Message}");
        }

        if (eventContext.Canceled)
            return CodeModuleFormDispatchResult.Failure(eventContext.Message ?? "The C# code module canceled the event.");

        return CodeModuleFormDispatchResult.Success(eventContext.Message);
    }

    private static IDictionary<string, object?> ToMutableRecord(IReadOnlyDictionary<string, object?>? record)
    {
        if (record is IDictionary<string, object?> mutable)
            return mutable;

        return record is null
            ? new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, object?>(record, StringComparer.OrdinalIgnoreCase);
    }

    private static FormEventContext CreateEventContext(CodeModuleFormEventDispatchContext context)
    {
        IReadOnlyDictionary<string, object?> arguments = MergeArguments(context.BindingArguments, context.RuntimeArguments);
        if (!string.IsNullOrWhiteSpace(context.ControlId))
        {
            return new FormControlEventContext(
                context.FormId,
                context.FormName,
                context.TableName,
                context.EventName,
                context.ControlId,
                context.ControlType,
                arguments,
                context.Metadata);
        }

        return context.IsCancelable
            ? new FormBeforeEventContext(
                context.FormId,
                context.FormName,
                context.TableName,
                context.EventName,
                arguments,
                context.Metadata)
            : new FormEventContext(
                context.FormId,
                context.FormName,
                context.TableName,
                context.EventName,
                arguments,
                context.Metadata);
    }

    private static IReadOnlyDictionary<string, object?> MergeArguments(
        IReadOnlyDictionary<string, object?>? bindingArguments,
        IReadOnlyDictionary<string, object?>? runtimeArguments)
    {
        if ((bindingArguments is null || bindingArguments.Count == 0) &&
            (runtimeArguments is null || runtimeArguments.Count == 0))
        {
            return new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        }

        var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        if (runtimeArguments is not null)
        {
            foreach ((string key, object? value) in runtimeArguments)
                result[key] = value;
        }

        if (bindingArguments is not null)
        {
            foreach ((string key, object? value) in bindingArguments)
                result[key] = value;
        }

        return result;
    }

    private static object?[]? BuildMethodArguments(MethodInfo method, FormEventContext context)
    {
        ParameterInfo[] parameters = method.GetParameters();
        if (parameters.Length == 0)
            return null;

        if (parameters.Length == 1 && parameters[0].ParameterType.IsInstanceOfType(context))
            return [context];

        throw new InvalidOperationException(
            $"C# code module handler '{method.Name}' must accept no parameters or one parameter assignable from '{context.GetType().Name}'.");
    }

    private static async Task AwaitResultAsync(object? result, CancellationToken ct)
    {
        switch (result)
        {
            case null:
                return;
            case Task task:
                await task.WaitAsync(ct);
                return;
            case ValueTask valueTask:
                await valueTask.AsTask().WaitAsync(ct);
                return;
            default:
                throw new InvalidOperationException(
                    $"C# code module handlers must return void, Task, or ValueTask. Actual return type was '{result.GetType().Name}'.");
        }
    }
}
