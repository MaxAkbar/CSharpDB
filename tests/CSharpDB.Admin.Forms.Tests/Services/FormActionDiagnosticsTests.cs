using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Forms.Services;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Tests.Services;

public sealed class FormActionDiagnosticsTests
{
    [Fact]
    public async Task DispatchActionSequence_EmitsDiagnosticEvent()
    {
        List<FormActionInvocationDiagnostic> diagnostics = [];
        using IDisposable subscription = FormActionDiagnostics.Listener.Subscribe(new ActionObserver(diagnostics));
        var dispatcher = new DefaultFormEventDispatcher(DbCommandRegistry.Empty);
        FormDefinition form = CreateForm(
            new DbActionSequence(
            [
                new DbActionStep(DbActionKind.ShowMessage, Message: "Loaded."),
            ],
            Name: "NotifyLoad"));

        FormEventDispatchResult result = await dispatcher.DispatchAsync(
            form,
            FormEventKind.OnLoad,
            new Dictionary<string, object?> { ["Id"] = 42L },
            TestContext.Current.CancellationToken);

        Assert.True(result.Succeeded);
        FormActionInvocationDiagnostic diagnostic = Assert.Single(
            diagnostics,
            diagnostic => diagnostic.FormId == "orders-form" && diagnostic.ActionKind == DbActionKind.ShowMessage);
        Assert.Equal(DbActionKind.ShowMessage, diagnostic.ActionKind);
        Assert.Equal("orders-form", diagnostic.FormId);
        Assert.Equal("Orders", diagnostic.TableName);
        Assert.Equal("OnLoad", diagnostic.EventName);
        Assert.Equal("NotifyLoad", diagnostic.ActionSequenceName);
        Assert.Equal(0, diagnostic.StepIndex);
        Assert.Equal("actionSequences.NotifyLoad.steps[0]", diagnostic.Location);
        Assert.True(diagnostic.Succeeded);
        Assert.False(diagnostic.Canceled);
        Assert.Equal("Loaded.", diagnostic.ResultMessage);
        Assert.Null(diagnostic.ExceptionMessage);
        Assert.True(diagnostic.Elapsed >= TimeSpan.Zero);
    }

    private static FormDefinition CreateForm(DbActionSequence sequence)
        => new(
            "orders-form",
            "Orders Form",
            "Orders",
            DefinitionVersion: 1,
            SourceSchemaSignature: "sig:orders",
            Layout: new LayoutDefinition("absolute", 8, SnapToGrid: false, []),
            Controls: [],
            EventBindings:
            [
                new FormEventBinding(FormEventKind.OnLoad, string.Empty, ActionSequence: sequence),
            ]);

    private sealed class ActionObserver(List<FormActionInvocationDiagnostic> diagnostics)
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
            if (value.Key == FormActionDiagnostics.InvocationEventName &&
                value.Value is FormActionInvocationDiagnostic diagnostic)
            {
                diagnostics.Add(diagnostic);
            }
        }
    }
}
