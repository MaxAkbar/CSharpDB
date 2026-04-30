using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class CallbackDiagnosticsTests
{
    [Fact]
    public void ScalarFunctionInvocation_EmitsDiagnosticEvent()
    {
        List<DbCallbackInvocationDiagnostic> diagnostics = [];
        using IDisposable subscription = DbCallbackDiagnostics.Listener.Subscribe(new CallbackObserver(diagnostics));
        DbFunctionRegistry registry = DbFunctionRegistry.Create(functions =>
            functions.AddScalar(
                "DiagBump",
                1,
                static (_, args) => DbValue.FromInteger(args[0].AsInteger + 1)));

        Assert.True(registry.TryGetScalar("DiagBump", 1, out DbScalarFunctionDefinition definition));
        DbValue result = definition.Invoke(
            [DbValue.FromInteger(41)],
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["surface"] = "SQL",
                ["location"] = "functions.DiagBump",
            });

        Assert.Equal(42, result.AsInteger);
        DbCallbackInvocationDiagnostic diagnostic = Assert.Single(
            diagnostics,
            diagnostic => diagnostic.Name == "DiagBump");
        Assert.Equal(AutomationCallbackKind.ScalarFunction, diagnostic.CallbackKind);
        Assert.Equal(1, diagnostic.Arity);
        Assert.Equal("SQL", diagnostic.Surface);
        Assert.Equal("functions.DiagBump", diagnostic.Location);
        Assert.True(diagnostic.Succeeded);
        Assert.False(diagnostic.TimedOut);
        Assert.False(diagnostic.Canceled);
        Assert.Null(diagnostic.ExceptionMessage);
        Assert.True(diagnostic.Elapsed >= TimeSpan.Zero);
    }

    [Fact]
    public async Task CommandInvocation_EmitsDiagnosticEventWithMetadataLocation()
    {
        List<DbCallbackInvocationDiagnostic> diagnostics = [];
        using IDisposable subscription = DbCallbackDiagnostics.Listener.Subscribe(new CallbackObserver(diagnostics));
        DbCommandRegistry registry = DbCommandRegistry.Create(commands =>
            commands.AddCommand(
                "DiagAudit",
                static _ => DbCommandResult.Success("ok")));

        Assert.True(registry.TryGetCommand("DiagAudit", out DbCommandDefinition definition));
        DbCommandResult result = await definition.InvokeAsync(
            metadata: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["surface"] = "AdminForms",
                ["event"] = "BeforeInsert",
                ["actionSequence"] = "PrepareCustomer",
                ["actionStep"] = "2",
            },
            ct: TestContext.Current.CancellationToken);

        Assert.True(result.Succeeded);
        DbCallbackInvocationDiagnostic diagnostic = Assert.Single(
            diagnostics,
            diagnostic => diagnostic.Name == "DiagAudit");
        Assert.Equal(AutomationCallbackKind.Command, diagnostic.CallbackKind);
        Assert.Null(diagnostic.Arity);
        Assert.Equal("AdminForms", diagnostic.Surface);
        Assert.Equal("BeforeInsert", diagnostic.EventName);
        Assert.Equal("actionSequences.PrepareCustomer.steps[2]", diagnostic.Location);
        Assert.True(diagnostic.Succeeded);
        Assert.Equal("ok", diagnostic.ResultMessage);
        Assert.Null(diagnostic.ExceptionMessage);
    }

    [Fact]
    public async Task CommandFailureResult_EmitsFailedDiagnosticWithoutException()
    {
        List<DbCallbackInvocationDiagnostic> diagnostics = [];
        using IDisposable subscription = DbCallbackDiagnostics.Listener.Subscribe(new CallbackObserver(diagnostics));
        DbCommandRegistry registry = DbCommandRegistry.Create(commands =>
            commands.AddCommand(
                "DiagReject",
                static _ => DbCommandResult.Failure("not allowed")));

        Assert.True(registry.TryGetCommand("DiagReject", out DbCommandDefinition definition));
        DbCommandResult result = await definition.InvokeAsync(ct: TestContext.Current.CancellationToken);

        Assert.False(result.Succeeded);
        DbCallbackInvocationDiagnostic diagnostic = Assert.Single(
            diagnostics,
            diagnostic => diagnostic.Name == "DiagReject");
        Assert.False(diagnostic.Succeeded);
        Assert.False(diagnostic.TimedOut);
        Assert.False(diagnostic.Canceled);
        Assert.Equal("not allowed", diagnostic.ResultMessage);
        Assert.Null(diagnostic.ExceptionMessage);
    }

    [Fact]
    public async Task CommandTimeout_EmitsTimedOutDiagnostic()
    {
        List<DbCallbackInvocationDiagnostic> diagnostics = [];
        using IDisposable subscription = DbCallbackDiagnostics.Listener.Subscribe(new CallbackObserver(diagnostics));
        DbCommandRegistry registry = DbCommandRegistry.Create(commands =>
            commands.AddAsyncCommand(
                "DiagSlow",
                new DbCommandOptions(Timeout: TimeSpan.FromMilliseconds(10)),
                static async (_, ct) =>
                {
                    await Task.Delay(TimeSpan.FromSeconds(5), ct);
                    return DbCommandResult.Success();
                }));

        Assert.True(registry.TryGetCommand("DiagSlow", out DbCommandDefinition definition));
        TimeoutException ex = await Assert.ThrowsAsync<TimeoutException>(async () =>
            await definition.InvokeAsync(ct: TestContext.Current.CancellationToken));

        Assert.Contains("timed out", ex.Message);
        DbCallbackInvocationDiagnostic diagnostic = Assert.Single(
            diagnostics,
            diagnostic => diagnostic.Name == "DiagSlow");
        Assert.False(diagnostic.Succeeded);
        Assert.True(diagnostic.TimedOut);
        Assert.False(diagnostic.Canceled);
        Assert.Contains("timed out", diagnostic.ExceptionMessage);
    }

    private sealed class CallbackObserver(List<DbCallbackInvocationDiagnostic> diagnostics)
        : IObserver<KeyValuePair<string, object?>>
    {
        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void OnNext(KeyValuePair<string, object?> value)
        {
            if (value.Key == DbCallbackDiagnostics.InvocationEventName &&
                value.Value is DbCallbackInvocationDiagnostic diagnostic)
            {
                diagnostics.Add(diagnostic);
            }
        }
    }
}
