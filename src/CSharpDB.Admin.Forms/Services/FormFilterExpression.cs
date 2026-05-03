using System.Globalization;
using System.Text.Json;
using CSharpDB.Admin.Forms.Models;

namespace CSharpDB.Admin.Forms.Services;

internal sealed class FormFilterExpression
{
    private readonly Node _root;

    private FormFilterExpression(Node root, IReadOnlyCollection<string> fields, IReadOnlyCollection<string> parameters)
    {
        _root = root;
        Fields = fields;
        Parameters = parameters;
    }

    public IReadOnlyCollection<string> Fields { get; }

    public IReadOnlyCollection<string> Parameters { get; }

    public bool Evaluate(
        IReadOnlyDictionary<string, object?> record,
        IReadOnlyDictionary<string, object?>? parameters = null)
        => IsTruthy(_root.Evaluate(record, parameters ?? EmptyObjectDictionary.Instance));

    public static bool TryParse(
        string expression,
        FormTableDefinition? table,
        out FormFilterExpression? filter,
        out string? error)
    {
        filter = null;
        error = null;

        if (string.IsNullOrWhiteSpace(expression))
        {
            error = "Filter expression is empty.";
            return false;
        }

        if (!Tokenizer.TryTokenize(expression, out List<Token> tokens, out error))
            return false;

        var parser = new Parser(tokens);
        if (!parser.TryParse(out Node? root, out error))
            return false;

        var fields = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var parameters = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        root!.CollectReferences(fields, parameters);

        if (table is not null)
        {
            var availableFields = new HashSet<string>(
                table.Fields.Select(static field => field.Name),
                StringComparer.OrdinalIgnoreCase);
            string? missingField = fields.FirstOrDefault(field => !availableFields.Contains(field));
            if (missingField is not null)
            {
                error = $"Filter references unknown field '{missingField}'.";
                return false;
            }
        }

        filter = new FormFilterExpression(root, fields, parameters);
        return true;
    }

    private static object? NormalizeValue(object? value)
        => value is JsonElement json ? NormalizeJsonValue(json) : value;

    private static object? NormalizeJsonValue(JsonElement value)
        => value.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.String => value.GetString(),
            JsonValueKind.Number => value.TryGetInt64(out long integer) ? integer : value.GetDouble(),
            _ => value.ToString(),
        };

    private static bool IsTruthy(object? value)
    {
        value = NormalizeValue(value);
        if (value is null)
            return false;

        if (value is bool boolean)
            return boolean;

        if (TryConvertDouble(value, out double number))
            return Math.Abs(number) > double.Epsilon;

        return !string.IsNullOrWhiteSpace(Convert.ToString(value, CultureInfo.InvariantCulture));
    }

    private static bool Compare(object? left, object? right, string op)
    {
        left = NormalizeValue(left);
        right = NormalizeValue(right);

        if (left is null || right is null)
        {
            int nullComparison = left is null && right is null ? 0 : left is null ? -1 : 1;
            return ApplyComparison(nullComparison, op);
        }

        if (TryConvertDouble(left, out double leftNumber) &&
            TryConvertDouble(right, out double rightNumber))
        {
            return ApplyComparison(leftNumber.CompareTo(rightNumber), op);
        }

        if (left is bool leftBool && right is bool rightBool)
            return ApplyComparison(leftBool.CompareTo(rightBool), op);

        int comparison = string.Compare(
            Convert.ToString(left, CultureInfo.InvariantCulture),
            Convert.ToString(right, CultureInfo.InvariantCulture),
            StringComparison.OrdinalIgnoreCase);
        return ApplyComparison(comparison, op);
    }

    private static bool ApplyComparison(int comparison, string op)
        => op switch
        {
            "=" or "==" => comparison == 0,
            "!=" or "<>" => comparison != 0,
            ">" => comparison > 0,
            ">=" => comparison >= 0,
            "<" => comparison < 0,
            "<=" => comparison <= 0,
            _ => false,
        };

    private static bool TryConvertDouble(object? value, out double result)
    {
        value = NormalizeValue(value);
        return value switch
        {
            byte number => Set(number, out result),
            short number => Set(number, out result),
            int number => Set(number, out result),
            long number => Set(number, out result),
            float number => Set(number, out result),
            double number => Set(number, out result),
            decimal number => Set((double)number, out result),
            string text => double.TryParse(text, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out result),
            _ => Set(0, out result, success: false),
        };
    }

    private static bool Set(double value, out double result, bool success = true)
    {
        result = value;
        return success;
    }

    private abstract class Node
    {
        public abstract object? Evaluate(
            IReadOnlyDictionary<string, object?> record,
            IReadOnlyDictionary<string, object?> parameters);

        public virtual void CollectReferences(HashSet<string> fields, HashSet<string> parameters)
        {
        }
    }

    private sealed class LiteralNode(object? value) : Node
    {
        public override object? Evaluate(
            IReadOnlyDictionary<string, object?> record,
            IReadOnlyDictionary<string, object?> parameters)
            => value;
    }

    private sealed class FieldNode(string name) : Node
    {
        public override object? Evaluate(
            IReadOnlyDictionary<string, object?> record,
            IReadOnlyDictionary<string, object?> parameters)
        {
            if (record.TryGetValue(name, out object? value))
                return value;

            string? actualKey = record.Keys.FirstOrDefault(candidate => string.Equals(candidate, name, StringComparison.OrdinalIgnoreCase));
            return actualKey is null ? null : record[actualKey];
        }

        public override void CollectReferences(HashSet<string> fields, HashSet<string> parameters)
            => fields.Add(name);
    }

    private sealed class ParameterNode(string name) : Node
    {
        public override object? Evaluate(
            IReadOnlyDictionary<string, object?> record,
            IReadOnlyDictionary<string, object?> parameters)
        {
            if (parameters.TryGetValue(name, out object? value))
                return value;

            string? actualKey = parameters.Keys.FirstOrDefault(candidate => string.Equals(candidate, name, StringComparison.OrdinalIgnoreCase));
            return actualKey is null ? null : parameters[actualKey];
        }

        public override void CollectReferences(HashSet<string> fields, HashSet<string> parameters)
            => parameters.Add(name);
    }

    private sealed class ComparisonNode(Node left, string op, Node right) : Node
    {
        public override object? Evaluate(
            IReadOnlyDictionary<string, object?> record,
            IReadOnlyDictionary<string, object?> parameters)
            => Compare(left.Evaluate(record, parameters), right.Evaluate(record, parameters), op);

        public override void CollectReferences(HashSet<string> fields, HashSet<string> parameters)
        {
            left.CollectReferences(fields, parameters);
            right.CollectReferences(fields, parameters);
        }
    }

    private sealed class LogicalNode(Node left, TokenType op, Node right) : Node
    {
        public override object? Evaluate(
            IReadOnlyDictionary<string, object?> record,
            IReadOnlyDictionary<string, object?> parameters)
            => op == TokenType.And
                ? IsTruthy(left.Evaluate(record, parameters)) && IsTruthy(right.Evaluate(record, parameters))
                : IsTruthy(left.Evaluate(record, parameters)) || IsTruthy(right.Evaluate(record, parameters));

        public override void CollectReferences(HashSet<string> fields, HashSet<string> parameters)
        {
            left.CollectReferences(fields, parameters);
            right.CollectReferences(fields, parameters);
        }
    }

    private sealed class Parser(List<Token> tokens)
    {
        private int _position;

        public bool TryParse(out Node? node, out string? error)
        {
            node = ParseOr(out error);
            if (node is null)
                return false;

            if (!IsAtEnd)
            {
                error = $"Unexpected token '{Current.Text}' in filter expression.";
                node = null;
                return false;
            }

            return true;
        }

        private Node? ParseOr(out string? error)
        {
            Node? left = ParseAnd(out error);
            if (left is null)
                return null;

            while (Match(TokenType.Or))
            {
                Node? right = ParseAnd(out error);
                if (right is null)
                    return null;

                left = new LogicalNode(left, TokenType.Or, right);
            }

            return left;
        }

        private Node? ParseAnd(out string? error)
        {
            Node? left = ParseComparison(out error);
            if (left is null)
                return null;

            while (Match(TokenType.And))
            {
                Node? right = ParseComparison(out error);
                if (right is null)
                    return null;

                left = new LogicalNode(left, TokenType.And, right);
            }

            return left;
        }

        private Node? ParseComparison(out string? error)
        {
            Node? left = ParsePrimary(out error);
            if (left is null)
                return null;

            if (Current.Type != TokenType.Operator)
                return left;

            string op = Current.Text;
            Advance();
            Node? right = ParsePrimary(out error);
            return right is null ? null : new ComparisonNode(left, op, right);
        }

        private Node? ParsePrimary(out string? error)
        {
            Token token = Current;
            switch (token.Type)
            {
                case TokenType.Field:
                    Advance();
                    error = null;
                    return new FieldNode(token.Text);
                case TokenType.Parameter:
                    Advance();
                    error = null;
                    return new ParameterNode(token.Text);
                case TokenType.String:
                case TokenType.Number:
                case TokenType.Boolean:
                case TokenType.Null:
                    Advance();
                    error = null;
                    return new LiteralNode(token.Value);
                case TokenType.LeftParen:
                    Advance();
                    Node? expression = ParseOr(out error);
                    if (expression is null)
                        return null;
                    if (!Match(TokenType.RightParen))
                    {
                        error = "Filter expression is missing a closing parenthesis.";
                        return null;
                    }

                    error = null;
                    return expression;
                default:
                    error = IsAtEnd
                        ? "Filter expression ended unexpectedly."
                        : $"Unexpected token '{token.Text}' in filter expression.";
                    return null;
            }
        }

        private bool Match(TokenType type)
        {
            if (Current.Type != type)
                return false;

            Advance();
            return true;
        }

        private void Advance()
        {
            if (!IsAtEnd)
                _position++;
        }

        private bool IsAtEnd => Current.Type == TokenType.End;

        private Token Current => tokens[_position];
    }

    private static class Tokenizer
    {
        public static bool TryTokenize(string expression, out List<Token> tokens, out string? error)
        {
            tokens = [];
            error = null;

            for (int i = 0; i < expression.Length;)
            {
                char ch = expression[i];
                if (char.IsWhiteSpace(ch))
                {
                    i++;
                    continue;
                }

                if (ch == '[')
                {
                    int end = expression.IndexOf(']', i + 1);
                    if (end < 0)
                    {
                        error = "Filter expression has an unclosed field reference.";
                        return false;
                    }

                    string fieldName = expression[(i + 1)..end].Trim();
                    if (fieldName.Length == 0)
                    {
                        error = "Filter expression contains an empty field reference.";
                        return false;
                    }

                    tokens.Add(new Token(TokenType.Field, fieldName));
                    i = end + 1;
                    continue;
                }

                if (ch == '@')
                {
                    int start = i + 1;
                    int end = ReadIdentifierEnd(expression, start);
                    if (end == start)
                    {
                        error = "Filter expression contains an empty parameter reference.";
                        return false;
                    }

                    tokens.Add(new Token(TokenType.Parameter, expression[start..end]));
                    i = end;
                    continue;
                }

                if (ch == '\'')
                {
                    if (!TryReadString(expression, i, out string? value, out int nextIndex, out error))
                        return false;

                    tokens.Add(new Token(TokenType.String, value, value));
                    i = nextIndex;
                    continue;
                }

                if (ch == '(')
                {
                    tokens.Add(new Token(TokenType.LeftParen, "("));
                    i++;
                    continue;
                }

                if (ch == ')')
                {
                    tokens.Add(new Token(TokenType.RightParen, ")"));
                    i++;
                    continue;
                }

                string? op = ReadOperator(expression, i);
                if (op is not null)
                {
                    tokens.Add(new Token(TokenType.Operator, op));
                    i += op.Length;
                    continue;
                }

                if (char.IsDigit(ch) || ch == '-')
                {
                    int end = ReadNumberEnd(expression, i);
                    if (end > i && TryReadNumber(expression[i..end], out object number))
                    {
                        tokens.Add(new Token(TokenType.Number, expression[i..end], number));
                        i = end;
                        continue;
                    }
                }

                if (IsIdentifierStart(ch))
                {
                    int end = ReadIdentifierEnd(expression, i);
                    string identifier = expression[i..end];
                    if (string.Equals(identifier, "AND", StringComparison.OrdinalIgnoreCase))
                        tokens.Add(new Token(TokenType.And, identifier));
                    else if (string.Equals(identifier, "OR", StringComparison.OrdinalIgnoreCase))
                        tokens.Add(new Token(TokenType.Or, identifier));
                    else if (string.Equals(identifier, "NULL", StringComparison.OrdinalIgnoreCase))
                        tokens.Add(new Token(TokenType.Null, identifier, null));
                    else if (bool.TryParse(identifier, out bool boolean))
                        tokens.Add(new Token(TokenType.Boolean, identifier, boolean));
                    else
                        tokens.Add(new Token(TokenType.String, identifier, identifier));
                    i = end;
                    continue;
                }

                error = $"Unexpected character '{ch}' in filter expression.";
                return false;
            }

            tokens.Add(new Token(TokenType.End, string.Empty));
            return true;
        }

        private static bool TryReadString(
            string expression,
            int start,
            out string value,
            out int nextIndex,
            out string? error)
        {
            var builder = new System.Text.StringBuilder();
            for (int i = start + 1; i < expression.Length; i++)
            {
                char ch = expression[i];
                if (ch == '\'')
                {
                    if (i + 1 < expression.Length && expression[i + 1] == '\'')
                    {
                        builder.Append('\'');
                        i++;
                        continue;
                    }

                    value = builder.ToString();
                    nextIndex = i + 1;
                    error = null;
                    return true;
                }

                builder.Append(ch);
            }

            value = string.Empty;
            nextIndex = expression.Length;
            error = "Filter expression has an unclosed string literal.";
            return false;
        }

        private static string? ReadOperator(string expression, int index)
        {
            foreach (string op in new[] { ">=", "<=", "==", "!=", "<>", "=", ">", "<" })
            {
                if (expression.AsSpan(index).StartsWith(op, StringComparison.Ordinal))
                    return op;
            }

            return null;
        }

        private static int ReadNumberEnd(string expression, int start)
        {
            int i = start;
            if (i < expression.Length && expression[i] == '-')
                i++;

            bool hasDigit = false;
            while (i < expression.Length && char.IsDigit(expression[i]))
            {
                hasDigit = true;
                i++;
            }

            if (i < expression.Length && expression[i] == '.')
            {
                i++;
                while (i < expression.Length && char.IsDigit(expression[i]))
                {
                    hasDigit = true;
                    i++;
                }
            }

            return hasDigit ? i : start;
        }

        private static bool TryReadNumber(string text, out object value)
        {
            if (long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out long integer))
            {
                value = integer;
                return true;
            }

            if (double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out double real))
            {
                value = real;
                return true;
            }

            value = text;
            return false;
        }

        private static int ReadIdentifierEnd(string expression, int start)
        {
            int i = start;
            while (i < expression.Length && (char.IsLetterOrDigit(expression[i]) || expression[i] == '_'))
                i++;

            return i;
        }

        private static bool IsIdentifierStart(char value)
            => char.IsLetter(value) || value == '_';
    }

    private sealed record Token(TokenType Type, string Text, object? Value = null);

    private enum TokenType
    {
        Field,
        Parameter,
        String,
        Number,
        Boolean,
        Null,
        Operator,
        And,
        Or,
        LeftParen,
        RightParen,
        End,
    }

    private static class EmptyObjectDictionary
    {
        public static readonly IReadOnlyDictionary<string, object?> Instance =
            new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
    }
}
