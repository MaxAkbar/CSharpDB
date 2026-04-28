using CSharpDB.Primitives;

namespace CSharpDB.Tests;

public sealed class AutomationMetadataTests
{
    [Fact]
    public void Builder_SortsAndDeduplicatesReferences()
    {
        var builder = new DbAutomationMetadataBuilder();

        DbAutomationMetadata metadata = builder
            .AddCommand("Notify", "pipelines", "hooks[1]")
            .AddCommand(" notify ", "pipelines", "hooks[1]")
            .AddScalarFunction("Slugify", 1, "pipelines", "transforms[0]")
            .AddScalarFunction("slugify", 1, "pipelines", "transforms[0]")
            .Build();

        DbAutomationCommandReference command = Assert.Single(metadata.Commands!);
        Assert.Equal("Notify", command.Name);
        DbAutomationScalarFunctionReference function = Assert.Single(metadata.ScalarFunctions!);
        Assert.Equal("Slugify", function.Name);
        Assert.Equal(1, function.Arity);
    }

    [Fact]
    public void ExpressionInspector_FindsNestedScalarFunctionCalls()
    {
        IReadOnlyList<DbAutomationScalarFunctionCall> calls =
            DbAutomationExpressionInspector.FindScalarFunctionCalls(
                "=Normalize(Slugify(Name), 'IgnoreMe(1)', [AlsoIgnore(2)])",
                ignoredNames: ["SUM"]);

        Assert.Contains(calls, call => call.Name == "Normalize" && call.Arity == 3);
        Assert.Contains(calls, call => call.Name == "Slugify" && call.Arity == 1);
        Assert.DoesNotContain(calls, call => call.Name == "IgnoreMe");
        Assert.DoesNotContain(calls, call => call.Name == "AlsoIgnore");
    }

    [Fact]
    public void ExpressionInspector_IgnoresConfiguredFunctionNames()
    {
        IReadOnlyList<DbAutomationScalarFunctionCall> calls =
            DbAutomationExpressionInspector.FindScalarFunctionCalls("=SUM(LineTotal) + Tax(LineTotal)", ["SUM"]);

        DbAutomationScalarFunctionCall call = Assert.Single(calls);
        Assert.Equal("Tax", call.Name);
        Assert.Equal(1, call.Arity);
    }
}
