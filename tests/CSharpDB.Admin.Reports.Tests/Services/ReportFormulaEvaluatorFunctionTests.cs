using CSharpDB.Admin.Reports.Services;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Reports.Tests.Services;

public sealed class ReportFormulaEvaluatorFunctionTests
{
    [Fact]
    public void EvaluateNumeric_CallsRegisteredFunction()
    {
        var registry = DbFunctionRegistry.Create(functions =>
            functions.AddScalar(
                "Discount",
                2,
                new DbScalarFunctionOptions(DbType.Real, IsDeterministic: true, NullPropagating: true),
                static (_, args) => DbValue.FromReal(args[0].AsReal - args[1].AsReal)));

        double? value = ReportFormulaEvaluator.EvaluateNumeric("=Discount(Total, 2)", field => field switch
        {
            "Total" => 10.0,
            _ => null,
        }, registry);

        Assert.Equal(8.0, value);
    }

    [Fact]
    public void TryEvaluateScalar_ReturnsTextFunctionValue()
    {
        var registry = DbFunctionRegistry.Create(functions =>
            functions.AddScalar(
                "Labelize",
                1,
                new DbScalarFunctionOptions(DbType.Text, IsDeterministic: true, NullPropagating: true),
                static (_, args) => DbValue.FromText($"Item: {args[0].AsText}")));

        bool evaluated = ReportFormulaEvaluator.TryEvaluateScalar(
            "=Labelize(Name)",
            field => field == "Name" ? "Widget" : null,
            registry,
            out object? value);

        Assert.True(evaluated);
        Assert.Equal("Item: Widget", value);
    }
}
