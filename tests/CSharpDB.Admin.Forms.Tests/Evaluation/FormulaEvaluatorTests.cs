using CSharpDB.Admin.Forms.Evaluation;

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
}
