using System.Globalization;
using CSharpDB.Admin.Forms.Evaluation;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Forms.Services;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Tests.Evaluation;

public class FormulaEvaluatorTests
{
    // ===== Basic Arithmetic =====

    [Fact]
    public void Addition()
    {
        var result = FormulaEvaluator.Evaluate("=2+3", _ => null);
        Assert.Equal(5.0, result);
    }

    [Fact]
    public void Subtraction()
    {
        var result = FormulaEvaluator.Evaluate("=10 - 4", _ => null);
        Assert.Equal(6.0, result);
    }

    [Fact]
    public void Multiplication()
    {
        var result = FormulaEvaluator.Evaluate("=3 * 7", _ => null);
        Assert.Equal(21.0, result);
    }

    [Fact]
    public void Division()
    {
        var result = FormulaEvaluator.Evaluate("=10/2", _ => null);
        Assert.Equal(5.0, result);
    }

    [Fact]
    public void OperatorPrecedence()
    {
        // 2 + 3 * 4 = 14 (not 20)
        var result = FormulaEvaluator.Evaluate("=2 + 3 * 4", _ => null);
        Assert.Equal(14.0, result);
    }

    [Fact]
    public void Parentheses()
    {
        // (2 + 3) * 4 = 20
        var result = FormulaEvaluator.Evaluate("=(2 + 3) * 4", _ => null);
        Assert.Equal(20.0, result);
    }

    [Fact]
    public void NegativeNumber()
    {
        var result = FormulaEvaluator.Evaluate("=-5 + 3", _ => null);
        Assert.Equal(-2.0, result);
    }

    [Fact]
    public void DecimalNumbers()
    {
        var result = FormulaEvaluator.Evaluate("=1.5 * 2.0", _ => null);
        Assert.Equal(3.0, result);
    }

    // ===== Field References =====

    [Fact]
    public void FieldReference()
    {
        var result = FormulaEvaluator.Evaluate("=Qty * Price", field => field switch
        {
            "Qty" => 5.0,
            "Price" => 10.0,
            _ => null
        });
        Assert.Equal(50.0, result);
    }

    [Fact]
    public void RegisteredScalarFunction()
    {
        var registry = DbFunctionRegistry.Create(functions =>
            functions.AddScalar(
                "Markup",
                2,
                new DbScalarFunctionOptions(DbType.Real, IsDeterministic: true, NullPropagating: true),
                static (_, args) => DbValue.FromReal(args[0].AsReal * args[1].AsReal)));

        var result = FormulaEvaluator.Evaluate("=Markup(Price, 1.25)", field => field switch
        {
            "Price" => 10.0,
            _ => null,
        }, registry);

        Assert.Equal(12.5, result);
    }

    [Fact]
    public void ComplexExpression_WithFields()
    {
        // (A + B) * C
        var result = FormulaEvaluator.Evaluate("=(A + B) * C", field => field switch
        {
            "A" => 2.0,
            "B" => 3.0,
            "C" => 10.0,
            _ => null
        });
        Assert.Equal(50.0, result);
    }

    [Fact]
    public void NestedExpression()
    {
        // A * (B + C) / D
        var result = FormulaEvaluator.Evaluate("=A * (B + C) / D", field => field switch
        {
            "A" => 6.0,
            "B" => 2.0,
            "C" => 3.0,
            "D" => 5.0,
            _ => null
        });
        Assert.Equal(6.0, result);
    }

    [Fact]
    public void FieldWithUnderscores()
    {
        var result = FormulaEvaluator.Evaluate("=unit_price * qty_ordered", field => field switch
        {
            "unit_price" => 9.99,
            "qty_ordered" => 3.0,
            _ => null
        });
        Assert.NotNull(result);
        Assert.Equal(29.97, result!.Value, 2);
    }

    // ===== Null Propagation =====

    [Fact]
    public void NullField_ReturnsNull()
    {
        var result = FormulaEvaluator.Evaluate("=Qty * Price", field => field switch
        {
            "Qty" => 5.0,
            "Price" => null,
            _ => null
        });
        Assert.Null(result);
    }

    [Fact]
    public void UnknownField_ReturnsNull()
    {
        var result = FormulaEvaluator.Evaluate("=Unknown + 5", _ => null);
        Assert.Null(result);
    }

    // ===== Division by Zero =====

    [Fact]
    public void DivisionByZero_ReturnsNull()
    {
        var result = FormulaEvaluator.Evaluate("=10 / 0", _ => null);
        Assert.Null(result);
    }

    [Fact]
    public void DivisionByZeroField_ReturnsNull()
    {
        var result = FormulaEvaluator.Evaluate("=A / B", field => field switch
        {
            "A" => 10.0,
            "B" => 0.0,
            _ => null
        });
        Assert.Null(result);
    }

    // ===== Invalid Formulas =====

    [Fact]
    public void EmptyFormula_ReturnsNull()
    {
        Assert.Null(FormulaEvaluator.Evaluate("", _ => null));
    }

    [Fact]
    public void NullFormula_ReturnsNull()
    {
        Assert.Null(FormulaEvaluator.Evaluate(null, _ => null));
    }

    [Fact]
    public void MissingEqualsPrefix_ReturnsNull()
    {
        Assert.Null(FormulaEvaluator.Evaluate("2+3", _ => null));
    }

    [Fact]
    public void JustEquals_ReturnsNull()
    {
        Assert.Null(FormulaEvaluator.Evaluate("=", _ => null));
    }

    // ===== Aggregate Parsing =====

    [Fact]
    public void TryParseAggregate_Sum()
    {
        var ok = FormulaEvaluator.TryParseAggregate("=SUM(OrderItems.LineTotal)", out var func, out var table, out var field);
        Assert.True(ok);
        Assert.Equal("SUM", func);
        Assert.Equal("OrderItems", table);
        Assert.Equal("LineTotal", field);
    }

    [Fact]
    public void TryParseAggregate_Count()
    {
        var ok = FormulaEvaluator.TryParseAggregate("=COUNT(Items.Qty)", out var func, out var table, out var field);
        Assert.True(ok);
        Assert.Equal("COUNT", func);
        Assert.Equal("Items", table);
        Assert.Equal("Qty", field);
    }

    [Fact]
    public void TryParseAggregate_Avg()
    {
        var ok = FormulaEvaluator.TryParseAggregate("=AVG(Scores.Value)", out var func, out var table, out var field);
        Assert.True(ok);
        Assert.Equal("AVG", func);
    }

    [Fact]
    public void TryParseAggregate_Min()
    {
        var ok = FormulaEvaluator.TryParseAggregate("=MIN(Prices.Amount)", out var func, out var _, out var _);
        Assert.True(ok);
        Assert.Equal("MIN", func);
    }

    [Fact]
    public void TryParseAggregate_Max()
    {
        var ok = FormulaEvaluator.TryParseAggregate("=MAX(Temps.Reading)", out var func, out var _, out var _);
        Assert.True(ok);
        Assert.Equal("MAX", func);
    }

    [Fact]
    public void TryParseAggregate_CaseInsensitive()
    {
        var ok = FormulaEvaluator.TryParseAggregate("=sum(Items.Total)", out var func, out var _, out var _);
        Assert.True(ok);
        Assert.Equal("SUM", func);
    }

    [Fact]
    public void TryParseAggregate_Invalid_NoDot()
    {
        var ok = FormulaEvaluator.TryParseAggregate("=SUM(Total)", out _, out _, out _);
        Assert.False(ok);
    }

    [Fact]
    public void TryParseAggregate_Invalid_NoParens()
    {
        var ok = FormulaEvaluator.TryParseAggregate("=SUM Items.Total", out _, out _, out _);
        Assert.False(ok);
    }

    [Fact]
    public void TryParseAggregate_Invalid_Empty()
    {
        Assert.False(FormulaEvaluator.TryParseAggregate("", out _, out _, out _));
        Assert.False(FormulaEvaluator.TryParseAggregate(null, out _, out _, out _));
    }

    // ===== Aggregate Evaluation =====

    [Fact]
    public void EvaluateAggregate_Sum()
    {
        var result = FormulaEvaluator.EvaluateAggregate("SUM", [10.0, 20.0, 30.0]);
        Assert.Equal(60.0, result);
    }

    [Fact]
    public void EvaluateAggregate_Sum_WithNulls()
    {
        var result = FormulaEvaluator.EvaluateAggregate("SUM", [10.0, null, 30.0]);
        Assert.Equal(40.0, result);
    }

    [Fact]
    public void EvaluateAggregate_Count()
    {
        var result = FormulaEvaluator.EvaluateAggregate("COUNT", [10.0, null, 30.0]);
        Assert.Equal(2.0, result);
    }

    [Fact]
    public void EvaluateAggregate_Avg()
    {
        var result = FormulaEvaluator.EvaluateAggregate("AVG", [10.0, 20.0, 30.0]);
        Assert.Equal(20.0, result);
    }

    [Fact]
    public void EvaluateAggregate_Min()
    {
        var result = FormulaEvaluator.EvaluateAggregate("MIN", [30.0, 10.0, 20.0]);
        Assert.Equal(10.0, result);
    }

    [Fact]
    public void EvaluateAggregate_Max()
    {
        var result = FormulaEvaluator.EvaluateAggregate("MAX", [30.0, 10.0, 20.0]);
        Assert.Equal(30.0, result);
    }

    [Fact]
    public void EvaluateAggregate_EmptyList_Sum_ReturnsZero()
    {
        var result = FormulaEvaluator.EvaluateAggregate("SUM", []);
        Assert.Equal(0.0, result);
    }

    [Fact]
    public void EvaluateAggregate_EmptyList_Avg_ReturnsNull()
    {
        var result = FormulaEvaluator.EvaluateAggregate("AVG", []);
        Assert.Null(result);
    }

    [Fact]
    public void EvaluateAggregate_AllNulls_ReturnsNull()
    {
        double?[] values = [null, null, null];
        var result = FormulaEvaluator.EvaluateAggregate("SUM", values);
        Assert.Null(result);
    }

    // ===== Access-style built-in functions =====

    [Fact]
    public void AccessBuiltIns_NullAndConditional()
    {
        Dictionary<string, object?> record = new(StringComparer.OrdinalIgnoreCase)
        {
            ["Qty"] = 3,
            ["Blank"] = "",
        };

        Assert.Equal("fallback", Eval("=Nz(Missing, 'fallback')", record));
        Assert.Equal("", Eval("=Nz(Missing)", record));
        Assert.True(AsBool(Eval("=IsNull(Missing)", record)));
        Assert.True(AsBool(Eval("=IsEmpty(Blank)", record)));
        Assert.Equal("high", Eval("=IIf(Qty > 2, 'high', 'low')", record));
        Assert.Equal("matched", Eval("=Switch(Qty = 1, 'one', Qty = 3, 'matched')", record));
        Assert.Equal("second", Eval("=Choose(2, 'first', 'second', 'third')", record));
    }

    [Fact]
    public void AccessBuiltIns_Text()
    {
        Assert.Equal(5, AsInt(Eval("=Len('Hello')")));
        Assert.Equal("Hel", Eval("=Left('Hello', 3)"));
        Assert.Equal("llo", Eval("=Right('Hello', 3)"));
        Assert.Equal("ell", Eval("=Mid('Hello', 2, 3)"));
        Assert.Equal("trim", Eval("=Trim('  trim  ')"));
        Assert.Equal("left  ", Eval("=LTrim('  left  ')"));
        Assert.Equal("  right", Eval("=RTrim('  right  ')"));
        Assert.Equal("HELLO", Eval("=UCase('Hello')"));
        Assert.Equal("hello", Eval("=LCase('Hello')"));
        Assert.Equal(3, AsInt(Eval("=InStr('Hello', 'l')")));
        Assert.Equal("HaLlo", Eval("=Replace('Hello', 'el', 'aL')"));
        Assert.Equal(0, AsInt(Eval("=StrComp('abc', 'ABC', 'text')")));
        Assert.Equal(12.75, AsDouble(Eval("=Val('12.75 kg')")));
    }

    [Fact]
    public void AccessBuiltIns_DateAndTime()
    {
        Assert.IsType<DateTime>(Eval("=Date()"));
        Assert.IsType<TimeSpan>(Eval("=Time()"));
        Assert.IsType<DateTime>(Eval("=Now()"));

        Assert.Equal(2026, AsInt(Eval("=Year('2026-05-02')")));
        Assert.Equal(5, AsInt(Eval("=Month('2026-05-02')")));
        Assert.Equal(2, AsInt(Eval("=Day('2026-05-02')")));
        Assert.Equal(14, AsInt(Eval("=Hour('2026-05-02 14:30:15')")));
        Assert.Equal(30, AsInt(Eval("=Minute('2026-05-02 14:30:15')")));
        Assert.Equal(15, AsInt(Eval("=Second('2026-05-02 14:30:15')")));

        Assert.Equal(new DateTime(2026, 5, 7), Eval("=DateAdd('d', 5, '2026-05-02')"));
        Assert.Equal(5, AsInt(Eval("=DateDiff('d', '2026-05-02', '2026-05-07')")));
        Assert.Equal(2, AsInt(Eval("=DatePart('q', '2026-05-02')")));
        Assert.Equal(new DateTime(2026, 5, 2), Eval("=DateSerial(2026, 5, 2)"));
        Assert.Equal(new TimeSpan(14, 30, 15), Eval("=TimeSerial(14, 30, 15)"));
        Assert.Equal(7, AsInt(Eval("=Weekday('2026-05-02')")));
        Assert.Equal("May", Eval("=MonthName(5, true)"));
    }

    [Fact]
    public void AccessBuiltIns_NumberAndConversion()
    {
        Assert.Equal(5.0, AsDouble(Eval("=Abs(-5)")));
        Assert.Equal(12.34, AsDouble(Eval("=Round(12.345, 2)")));
        Assert.Equal(-2.0, AsDouble(Eval("=Int(-1.2)")));
        Assert.Equal(-1.0, AsDouble(Eval("=Fix(-1.2)")));
        Assert.Equal(-1, AsInt(Eval("=Sgn(-12)")));
        Assert.Equal("42", Eval("=CStr(42)"));
        Assert.Equal(12, AsInt(Eval("=CInt(12.4)")));
        Assert.Equal(13, AsInt(Eval("=CLng(12.6)")));
        Assert.Equal(12.5, AsDouble(Eval("=CDbl('12.5')")));
        Assert.True(AsBool(Eval("=CBool(1)")));
        Assert.Equal(new DateTime(2026, 5, 2), Eval("=CDate('2026-05-02')"));
        Assert.Equal("12.35", Eval("=Format(12.345, '0.00')"));
    }

    [Fact]
    public void AccessBuiltIns_DomainFunctions_InvokeResolver()
    {
        var calls = new List<FormulaDomainFunctionRequest>();
        object? Resolver(FormulaDomainFunctionRequest request)
        {
            calls.Add(request);
            return request.FunctionName switch
            {
                "DLOOKUP" => "Contoso",
                "DCOUNT" => 2,
                "DSUM" => 42.5,
                "DAVG" => 21.25,
                "DMIN" => 10,
                "DMAX" => 50,
                _ => null,
            };
        }

        Assert.Equal("Contoso", Eval("=DLookup('Name', 'Customers', 'Id = 1')", domainResolver: Resolver));
        Assert.Equal(2, AsInt(Eval("=DCount('*', 'Customers')", domainResolver: Resolver)));
        Assert.Equal(42.5, AsDouble(Eval("=DSum('Amount', 'Orders')", domainResolver: Resolver)));
        Assert.Equal(21.25, AsDouble(Eval("=DAvg('Amount', 'Orders')", domainResolver: Resolver)));
        Assert.Equal(10, AsInt(Eval("=DMin('Amount', 'Orders')", domainResolver: Resolver)));
        Assert.Equal(50, AsInt(Eval("=DMax('Amount', 'Orders')", domainResolver: Resolver)));

        Assert.Contains(calls, call =>
            call.FunctionName == "DLOOKUP" &&
            call.Expression == "Name" &&
            call.Domain == "Customers" &&
            call.Criteria == "Id = 1");
    }

    [Fact]
    public void FormulaFunctionCatalog_MatchesEvaluatorBuiltIns()
    {
        Assert.All(FormulaFunctionCatalog.ExpressionFunctions, function =>
            Assert.True(FormulaEvaluator.IsBuiltInFunctionName(function.Name), function.Name));

        string[] duplicates = FormulaFunctionCatalog.AllFunctions
            .GroupBy(function => function.Name, StringComparer.OrdinalIgnoreCase)
            .Where(group => group.Count() > 1)
            .Select(group => group.Key)
            .ToArray();

        Assert.Empty(duplicates);
        Assert.Contains(FormulaFunctionCatalog.AllFunctions, function => function.Name == "Nz" && function.Example.StartsWith('='));
        Assert.Contains(FormulaFunctionCatalog.AllFunctions, function => function.Name == "DLookup" && function.Category == "Domain");
        Assert.Contains(FormulaFunctionCatalog.AllFunctions, function => function.Name == "SUM" && function.Category == "Child Aggregates");
    }

    [Fact]
    public void GetDomainReferences_ReturnsLiteralDomains()
    {
        IReadOnlyList<string> domains = FormulaEvaluator.GetDomainReferences(
            "=DLookup('Name', 'Customers', 'Id = 1') & DSum('Amount', Orders)");

        Assert.Equal(["Customers", "Orders"], domains);
    }

    [Fact]
    public void FormAutomationMetadata_DoesNotExportBuiltInFunctionsAsCallbacks()
    {
        var form = new FormDefinition(
            "form1",
            "Orders",
            "Orders",
            1,
            "schema",
            new LayoutDefinition("absolute", 8, true, []),
            [
                new ControlDefinition(
                    "calc",
                    "computed",
                    new Rect(0, 0, 100, 24),
                    Binding: new BindingDefinition("Total", "OneWay"),
                    Props: new PropertyBag(new Dictionary<string, object?>
                    {
                        ["formula"] = "=Nz(Amount, 0) + Round(Discount, 2) + DSum('Amount', 'Orders')",
                    }),
                    ValidationOverride: null),
            ]);

        DbAutomationMetadata metadata = FormAutomationMetadata.Build(form);

        Assert.Empty(metadata.ScalarFunctions ?? []);
    }

    private static object? Eval(
        string formula,
        IReadOnlyDictionary<string, object?>? record = null,
        FormulaDomainFunctionResolver? domainResolver = null)
        => FormulaEvaluator.EvaluateValue(
            formula,
            field => record is not null && record.TryGetValue(field, out object? value) ? value : null,
            DbFunctionRegistry.Empty,
            callbackPolicy: null,
            domainResolver);

    private static int AsInt(object? value)
        => Convert.ToInt32(value, CultureInfo.InvariantCulture);

    private static double AsDouble(object? value)
        => Convert.ToDouble(value, CultureInfo.InvariantCulture);

    private static bool AsBool(object? value)
        => Assert.IsType<bool>(value);
}
