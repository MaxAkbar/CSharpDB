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

    [Fact]
    public async Task CommandPolicyDenied_EmitsDiagnosticAndDoesNotInvokeCallback()
    {
        bool invoked = false;
        List<DbCallbackInvocationDiagnostic> diagnostics = [];
        using IDisposable subscription = DbCallbackDiagnostics.Listener.Subscribe(new CallbackObserver(diagnostics));
        DbCommandRegistry registry = DbCommandRegistry.Create(commands =>
            commands.AddCommand(
                "DiagNetwork",
                new DbCommandOptions(
                    AdditionalCapabilities:
                    [
                        new DbExtensionCapabilityRequest(DbExtensionCapability.Network),
                    ]),
                _ =>
                {
                    invoked = true;
                    return DbCommandResult.Success();
                }));
        var policy = new DbExtensionPolicy(
            AllowExtensions: true,
            Grants:
            [
                new DbExtensionCapabilityGrant(
                    DbExtensionCapability.Commands,
                    DbExtensionCapabilityGrantStatus.Granted),
            ]);

        Assert.True(registry.TryGetCommand("DiagNetwork", out DbCommandDefinition definition));
        DbCallbackPolicyException ex = await Assert.ThrowsAsync<DbCallbackPolicyException>(async () =>
            await definition.InvokeAsync(
                new Dictionary<string, DbValue>(),
                new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    ["surface"] = "AdminForms",
                    ["location"] = "controls.notify.events.Click",
                    ["correlationId"] = "corr-1",
                    ["ownerKind"] = "Form",
                    ["ownerId"] = "orders-form",
                    ["ownerName"] = "Orders",
                },
                policy,
                DbExtensionHostMode.Embedded,
                TestContext.Current.CancellationToken));

        Assert.False(invoked);
        Assert.Contains("No grant exists for capability 'Network'.", ex.Message);
        DbCallbackInvocationDiagnostic diagnostic = Assert.Single(
            diagnostics,
            diagnostic => diagnostic.Name == "DiagNetwork");
        Assert.False(diagnostic.Succeeded);
        Assert.Equal(false, diagnostic.PolicyAllowed);
        Assert.Equal("No grant exists for capability 'Network'.", diagnostic.PolicyDenialReason);
        Assert.Equal("PolicyDenied", diagnostic.ErrorCode);
        Assert.Equal(typeof(DbCallbackPolicyException).FullName, diagnostic.ExceptionType);
        Assert.Equal("corr-1", diagnostic.CorrelationId);
        Assert.Equal("Form", diagnostic.OwnerKind);
        Assert.Equal("orders-form", diagnostic.OwnerId);
        Assert.Equal("controls.notify.events.Click", diagnostic.Location);
        Assert.NotNull(diagnostic.StartedAtUtc);
    }

    [Fact]
    public async Task ValidationRuleInvocation_EmitsDiagnosticWithPolicyDecision()
    {
        List<DbCallbackInvocationDiagnostic> diagnostics = [];
        using IDisposable subscription = DbCallbackDiagnostics.Listener.Subscribe(new CallbackObserver(diagnostics));
        DbValidationRuleRegistry registry = DbValidationRuleRegistry.Create(rules =>
            rules.AddRule(
                "CreditLimit",
                static (_, _) => ValueTask.FromResult(DbValidationRuleResult.Failure("Credit limit exceeded.", "CreditLimit"))));

        Assert.True(registry.TryGetRule("CreditLimit", out DbValidationRuleDefinition definition));
        DbValidationRuleResult result = await definition.InvokeAsync(
            DbValidationRuleContext.Create(
                "CreditLimit",
                DbValidationRuleScope.Field,
                metadata: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    ["surface"] = "admin.forms",
                    ["location"] = "controls.credit.validationRules.CreditLimit",
                    ["correlationId"] = "validation-corr",
                    ["ownerKind"] = "Form",
                    ["ownerId"] = "orders-form",
                    ["ownerName"] = "Orders",
                }),
            DbExtensionPolicies.DefaultHostCallbackPolicy,
            DbExtensionHostMode.Embedded,
            TestContext.Current.CancellationToken);

        Assert.False(result.Succeeded);
        DbCallbackInvocationDiagnostic diagnostic = Assert.Single(
            diagnostics,
            diagnostic => diagnostic.Name == "CreditLimit");
        Assert.Equal(AutomationCallbackKind.ValidationRule, diagnostic.CallbackKind);
        Assert.Null(diagnostic.Arity);
        Assert.False(diagnostic.Succeeded);
        Assert.Equal(true, diagnostic.PolicyAllowed);
        Assert.Equal("admin.forms", diagnostic.Surface);
        Assert.Equal("controls.credit.validationRules.CreditLimit", diagnostic.Location);
        Assert.Equal("validation-corr", diagnostic.CorrelationId);
        Assert.Equal("Form", diagnostic.OwnerKind);
        Assert.Equal("orders-form", diagnostic.OwnerId);
        Assert.NotNull(diagnostic.StartedAtUtc);
    }

    [Fact]
    public void ScalarPolicyDenied_EmitsDiagnosticAndDoesNotInvokeCallback()
    {
        bool invoked = false;
        List<DbCallbackInvocationDiagnostic> diagnostics = [];
        using IDisposable subscription = DbCallbackDiagnostics.Listener.Subscribe(new CallbackObserver(diagnostics));
        DbFunctionRegistry registry = DbFunctionRegistry.Create(functions =>
            functions.AddScalar(
                "DiagSecret",
                0,
                new DbScalarFunctionOptions(
                    AdditionalCapabilities:
                    [
                        new DbExtensionCapabilityRequest(DbExtensionCapability.EnvironmentVariables),
                    ]),
                (_, _) =>
                {
                    invoked = true;
                    return DbValue.FromText("secret");
                }));
        var policy = new DbExtensionPolicy(
            AllowExtensions: true,
            Grants:
            [
                new DbExtensionCapabilityGrant(
                    DbExtensionCapability.ScalarFunctions,
                    DbExtensionCapabilityGrantStatus.Granted),
            ]);

        Assert.True(registry.TryGetScalar("DiagSecret", 0, out DbScalarFunctionDefinition definition));
        DbCallbackPolicyException ex = Assert.Throws<DbCallbackPolicyException>(() =>
            definition.Invoke(
                [],
                new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    ["surface"] = "Pipelines",
                    ["location"] = "transforms.functions.DiagSecret",
                },
                policy,
                DbExtensionHostMode.Embedded));

        Assert.False(invoked);
        Assert.Contains("No grant exists for capability 'EnvironmentVariables'.", ex.Message);
        DbCallbackInvocationDiagnostic diagnostic = Assert.Single(
            diagnostics,
            diagnostic => diagnostic.Name == "DiagSecret");
        Assert.False(diagnostic.Succeeded);
        Assert.Equal(false, diagnostic.PolicyAllowed);
        Assert.Equal("No grant exists for capability 'EnvironmentVariables'.", diagnostic.PolicyDenialReason);
        Assert.Equal("PolicyDenied", diagnostic.ErrorCode);
        Assert.Equal(typeof(DbCallbackPolicyException).FullName, diagnostic.ExceptionType);
        Assert.Equal("Pipelines", diagnostic.Surface);
        Assert.Equal("transforms.functions.DiagSecret", diagnostic.Location);
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
