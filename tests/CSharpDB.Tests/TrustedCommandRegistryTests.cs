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
                new DbCommandOptions("Rebuilds inventory summaries."),
                static context =>
                {
                    Assert.Equal("RecalculateInventory", context.CommandName);
                    Assert.Equal("AdminForms", context.Metadata["surface"]);
                    Assert.Equal(42, context.Arguments["ProductId"].AsInteger);
                    return DbCommandResult.Success("done", DbValue.FromText("ok"));
                }));

        Assert.True(registry.TryGetCommand("recalculateinventory", out DbCommandDefinition definition));
        Assert.Equal("Rebuilds inventory summaries.", definition.Options.Description);

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
            commands.AddCommand(
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
}
