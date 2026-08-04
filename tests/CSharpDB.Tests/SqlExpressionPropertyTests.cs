using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Tests;

public sealed class SqlExpressionPropertyTests
{
    private const int Seed = 0x5EED_2026;
    private const int GeneratedCaseCount = 96;

    [Fact]
    public async Task DeterministicGeneratedExpressions_ParseAndEvaluateToModel()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using Database database = await Database.OpenInMemoryAsync(ct);
        var random = new Random(Seed);

        for (int caseIndex = 0; caseIndex < GeneratedCaseCount; caseIndex++)
        {
            GeneratedExpression generated = (caseIndex % 3) switch
            {
                0 => GenerateNumeric(random, depth: 4),
                1 => GeneratePredicate(random, depth: 4),
                _ => GeneratePrecedenceExpression(random),
            };

            _ = Parser.ParseExpressionSql(generated.Sql);

            await using QueryResult result = await database.ExecuteAsync(
                $"SELECT {generated.Sql}",
                ct);
            IReadOnlyList<DbValue[]> rows = await result.ToListAsync(ct);
            DbValue actual = rows[0][0];

            Assert.True(
                actual == generated.Expected,
                $"seed={Seed}, case={caseIndex}, sql={generated.Sql}, " +
                $"expected={Describe(generated.Expected)}, actual={Describe(actual)}");
        }
    }

    private static GeneratedExpression GenerateNumeric(Random random, int depth)
    {
        if (depth == 0 || random.Next(4) == 0)
        {
            if (random.Next(5) == 0)
                return new GeneratedExpression("NULL", DbValue.Null);

            long value = random.Next(-9, 10);
            return new GeneratedExpression(
                value.ToString(System.Globalization.CultureInfo.InvariantCulture),
                DbValue.FromInteger(value));
        }

        if (random.Next(5) == 0)
        {
            GeneratedExpression operand = GenerateNumeric(random, depth - 1);
            return new GeneratedExpression(
                $"-({operand.Sql})",
                operand.Expected.IsNull
                    ? DbValue.Null
                    : DbValue.FromInteger(-operand.Expected.AsInteger));
        }

        GeneratedExpression left = GenerateNumeric(random, depth - 1);
        GeneratedExpression right = GenerateNumeric(random, depth - 1);
        char operation = random.Next(3) switch
        {
            0 => '+',
            1 => '-',
            _ => '*',
        };

        DbValue expected;
        if (left.Expected.IsNull || right.Expected.IsNull)
        {
            expected = DbValue.Null;
        }
        else
        {
            long leftValue = left.Expected.AsInteger;
            long rightValue = right.Expected.AsInteger;
            expected = DbValue.FromInteger(operation switch
            {
                '+' => leftValue + rightValue,
                '-' => leftValue - rightValue,
                _ => leftValue * rightValue,
            });
        }

        return new GeneratedExpression(
            $"({left.Sql} {operation} {right.Sql})",
            expected);
    }

    private static GeneratedExpression GeneratePredicate(Random random, int depth)
    {
        if (depth > 0 && random.Next(3) != 0)
        {
            if (random.Next(5) == 0)
            {
                GeneratedExpression operand = GeneratePredicate(random, depth - 1);
                return new GeneratedExpression(
                    $"NOT ({operand.Sql})",
                    SqlNot(operand.Expected));
            }

            GeneratedExpression left = GeneratePredicate(random, depth - 1);
            GeneratedExpression right = GeneratePredicate(random, depth - 1);
            bool useAnd = random.Next(2) == 0;
            return new GeneratedExpression(
                $"({left.Sql} {(useAnd ? "AND" : "OR")} {right.Sql})",
                useAnd
                    ? SqlAnd(left.Expected, right.Expected)
                    : SqlOr(left.Expected, right.Expected));
        }

        GeneratedExpression numericLeft = GenerateNumeric(random, depth: 1);
        GeneratedExpression numericRight = GenerateNumeric(random, depth: 1);
        string comparison = random.Next(6) switch
        {
            0 => "=",
            1 => "<>",
            2 => "<",
            3 => ">",
            4 => "<=",
            _ => ">=",
        };
        DbValue expected = Compare(
            numericLeft.Expected,
            numericRight.Expected,
            comparison);
        return new GeneratedExpression(
            $"{numericLeft.Sql} {comparison} {numericRight.Sql}",
            expected);
    }

    private static GeneratedExpression GeneratePrecedenceExpression(Random random)
    {
        long a = random.Next(-8, 9);
        long b = random.Next(-8, 9);
        long c = random.Next(-8, 9);
        long d = random.Next(-8, 9);

        return random.Next(2) == 0
            ? new GeneratedExpression(
                $"{a} + {b} * {c} - {d}",
                DbValue.FromInteger(a + (b * c) - d))
            : new GeneratedExpression(
                $"{a} = {b} OR {c} < {d} AND {a} <> {d}",
                SqlOr(
                    DbValue.FromInteger(a == b ? 1 : 0),
                    SqlAnd(
                        DbValue.FromInteger(c < d ? 1 : 0),
                        DbValue.FromInteger(a != d ? 1 : 0))));
    }

    private static DbValue Compare(DbValue left, DbValue right, string operation)
    {
        if (left.IsNull || right.IsNull)
            return DbValue.Null;

        int comparison = DbValue.Compare(left, right);
        bool result = operation switch
        {
            "=" => comparison == 0,
            "<>" => comparison != 0,
            "<" => comparison < 0,
            ">" => comparison > 0,
            "<=" => comparison <= 0,
            _ => comparison >= 0,
        };
        return DbValue.FromInteger(result ? 1 : 0);
    }

    private static DbValue SqlNot(DbValue operand) =>
        operand.IsNull
            ? DbValue.Null
            : DbValue.FromInteger(operand.IsTruthy ? 0 : 1);

    private static DbValue SqlAnd(DbValue left, DbValue right)
    {
        if ((!left.IsNull && !left.IsTruthy) ||
            (!right.IsNull && !right.IsTruthy))
        {
            return DbValue.FromInteger(0);
        }

        return left.IsNull || right.IsNull
            ? DbValue.Null
            : DbValue.FromInteger(1);
    }

    private static DbValue SqlOr(DbValue left, DbValue right)
    {
        if ((!left.IsNull && left.IsTruthy) ||
            (!right.IsNull && right.IsTruthy))
        {
            return DbValue.FromInteger(1);
        }

        return left.IsNull || right.IsNull
            ? DbValue.Null
            : DbValue.FromInteger(0);
    }

    private static string Describe(DbValue value) =>
        $"{value.Type}:{value}";

    private readonly record struct GeneratedExpression(
        string Sql,
        DbValue Expected);
}
