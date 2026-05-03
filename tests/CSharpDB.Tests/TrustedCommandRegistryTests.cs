using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class TrustedCommandRegistryTests
{
    [Fact]
    public async Task Registry_ValidatesNamesCollisionsAndMetadata()
    {
        var registry = DbCommandRegistry.Create(commands =>
            commands.AddCommand(
                "RecalculateInventory",
                new DbCommandOptions(
                    "Rebuilds inventory summaries.",
                    Timeout: TimeSpan.FromSeconds(5),
                    IsLongRunning: true,
                    AdditionalCapabilities:
                    [
                        new DbExtensionCapabilityRequest(
                            DbExtensionCapability.ReadDatabase,
                            Reason: "Reads product inventory tables.",
                            Tables: ["Products", "Inventory"]),
                    ]),
                static context =>
                {
                    Assert.Equal("RecalculateInventory", context.CommandName);
                    Assert.Equal("AdminForms", context.Metadata["surface"]);
                    Assert.Equal(42, context.Arguments["ProductId"].AsInteger);
                    return DbCommandResult.Success("done", DbValue.FromText("ok"));
                }));

        Assert.True(registry.TryGetCommand("recalculateinventory", out DbCommandDefinition definition));
        Assert.Equal("Rebuilds inventory summaries.", definition.Options.Description);
        Assert.Equal(TimeSpan.FromSeconds(5), definition.Options.Timeout);
        Assert.True(definition.Options.IsLongRunning);
        Assert.Equal(AutomationCallbackKind.Command, definition.Descriptor.Kind);
        Assert.Equal(DbExtensionRuntimeKind.HostCallback, definition.Descriptor.Runtime);
        Assert.Equal("RecalculateInventory", definition.Descriptor.Name);
        Assert.Null(definition.Descriptor.Arity);
        Assert.Equal("Rebuilds inventory summaries.", definition.Descriptor.Description);
        Assert.Equal(TimeSpan.FromSeconds(5), definition.Descriptor.Timeout);
        Assert.True(definition.Descriptor.IsLongRunning);
        Assert.Equal(
            [DbExtensionCapability.Commands, DbExtensionCapability.ReadDatabase],
            definition.Descriptor.Capabilities.Select(static capability => capability.Name).ToArray());
        Assert.Same(definition.Descriptor, Assert.Single(registry.Callbacks));

        DbCommandResult result = await definition.InvokeAsync(
            new Dictionary<string, DbValue>(StringComparer.OrdinalIgnoreCase)
            {
                ["ProductId"] = DbValue.FromInteger(42),
            },
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["surface"] = "AdminForms",
            },
            TestContext.Current.CancellationToken);

        Assert.True(result.Succeeded);
        Assert.Equal("done", result.Message);
        Assert.Equal("ok", result.Value.AsText);

        Assert.Throws<ArgumentException>(() => DbCommandRegistry.Create(commands =>
        {
            commands.AddCommand("Dup", static _ => DbCommandResult.Success());
            commands.AddCommand("dup", static _ => DbCommandResult.Success());
        }));

        Assert.Throws<ArgumentException>(() => DbCommandRegistry.Create(commands =>
            commands.AddCommand("bad-name", static _ => DbCommandResult.Success())));
    }

    [Fact]
    public async Task Registry_InvokesAsyncCommands()
    {
        var registry = DbCommandRegistry.Create(commands =>
            commands.AddAsyncCommand(
                "AsyncCommand",
                static async (context, ct) =>
                {
                    await Task.Delay(1, ct);
                    return DbCommandResult.Success(context.Metadata["event"]);
                }));

        Assert.True(registry.TryGetCommand("AsyncCommand", out DbCommandDefinition definition));

        DbCommandResult result = await definition.InvokeAsync(
            metadata: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            {
                ["event"] = "AfterUpdate",
            },
            ct: TestContext.Current.CancellationToken);

        Assert.True(result.Succeeded);
        Assert.Equal("AfterUpdate", result.Message);
    }

    [Fact]
    public async Task Registry_AppliesCommandTimeout()
    {
        var registry = DbCommandRegistry.Create(commands =>
            commands.AddAsyncCommand(
                "SlowCommand",
                new DbCommandOptions(Timeout: TimeSpan.FromMilliseconds(10)),
                static async (_, ct) =>
                {
                    await Task.Delay(TimeSpan.FromSeconds(5), ct);
                    return DbCommandResult.Success();
                }));

        Assert.True(registry.TryGetCommand("SlowCommand", out DbCommandDefinition definition));

        TimeoutException ex = await Assert.ThrowsAsync<TimeoutException>(async () =>
            await definition.InvokeAsync(ct: TestContext.Current.CancellationToken));

        Assert.Contains("SlowCommand", ex.Message);
        Assert.Contains("timed out", ex.Message);
    }

    [Fact]
    public async Task Registry_DoesNotTreatDelegateTimeoutExceptionAsCommandTimeout()
    {
        var registry = DbCommandRegistry.Create(commands =>
            commands.AddAsyncCommand(
                "ServiceCommand",
                new DbCommandOptions(Timeout: TimeSpan.FromSeconds(5)),
                static (_, _) => Task.FromException<DbCommandResult>(
                    new TimeoutException("Downstream service timeout."))));

        Assert.True(registry.TryGetCommand("ServiceCommand", out DbCommandDefinition definition));

        TimeoutException ex = await Assert.ThrowsAsync<TimeoutException>(async () =>
            await definition.InvokeAsync(ct: TestContext.Current.CancellationToken));

        Assert.Equal("Downstream service timeout.", ex.Message);
    }

    [Fact]
    public void Registry_RejectsNonPositiveCommandTimeout()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => DbCommandRegistry.Create(commands =>
            commands.AddCommand(
                "InvalidTimeout",
                new DbCommandOptions(Timeout: TimeSpan.Zero),
                static _ => DbCommandResult.Success())));
    }

    [Fact]
    public void CommandArguments_ConvertObjectDictionariesAndLetConfiguredValuesOverrideRuntimeValues()
    {
        Dictionary<string, DbValue> arguments = DbCommandArguments.FromObjectDictionary(
            new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
            {
                ["Id"] = 7,
                ["IsActive"] = true,
                ["Total"] = 12.5m,
                ["JsonInteger"] = 3.0d,
                ["Name"] = "Alice",
                [""] = "ignored",
            },
            new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
            {
                ["Name"] = "Override",
                ["NullValue"] = null,
            });

        Assert.Equal(7, arguments["Id"].AsInteger);
        Assert.Equal(1, arguments["IsActive"].AsInteger);
        Assert.Equal(12.5, arguments["Total"].AsReal);
        Assert.Equal(3, arguments["JsonInteger"].AsInteger);
        Assert.Equal("Override", arguments["Name"].AsText);
        Assert.True(arguments["NullValue"].IsNull);
        Assert.False(arguments.ContainsKey(""));
    }
}
