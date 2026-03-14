using System.Globalization;
using CSharpDB.Primitives;

namespace CSharpDB.Sql;

public readonly record struct SimplePrimaryKeyLookupSql
{
    public string TableName { get; }
    public bool SelectStar { get; }
    public string[] ProjectionColumns { get; }
    public string PredicateColumn { get; }
    public long LookupValue { get; }
    public bool HasResidualPredicate { get; }
    public string ResidualPredicateColumn { get; }
    public DbValue ResidualPredicateLiteral { get; }
    public DbValue PredicateLiteral { get; }

    public string ProjectionColumn => ProjectionColumns.Length == 1 ? ProjectionColumns[0] : string.Empty;

    public SimplePrimaryKeyLookupSql(
        string tableName,
        bool selectStar,
        string[]? projectionColumns,
        string predicateColumn,
        long lookupValue,
        bool hasResidualPredicate = false,
        string residualPredicateColumn = "",
        DbValue residualPredicateLiteral = default,
        DbValue predicateLiteral = default)
    {
        TableName = tableName;
        SelectStar = selectStar;
        ProjectionColumns = projectionColumns ?? Array.Empty<string>();
        PredicateColumn = predicateColumn;
        LookupValue = lookupValue;
        HasResidualPredicate = hasResidualPredicate;
        ResidualPredicateColumn = residualPredicateColumn;
        ResidualPredicateLiteral = residualPredicateLiteral;
        PredicateLiteral = predicateLiteral;
    }
}

public readonly record struct SimpleInsertSql
{
    public string TableName { get; }
    public DbValue[][] ValueRows { get; }
    public DbValue[] Values => ValueRows[0];
    public int RowCount { get; }

    public SimpleInsertSql(string tableName, DbValue[] values)
        : this(tableName, [values])
    {
    }

    public SimpleInsertSql(string tableName, DbValue[][] valueRows)
        : this(tableName, valueRows, valueRows.Length)
    {
    }

    public SimpleInsertSql(string tableName, DbValue[][] valueRows, int rowCount)
    {
        ArgumentNullException.ThrowIfNull(valueRows);
        if ((uint)rowCount > (uint)valueRows.Length)
            throw new ArgumentOutOfRangeException(nameof(rowCount));

        TableName = tableName;
        ValueRows = valueRows;
        RowCount = rowCount;
    }

    public InsertStatement ToStatement()
    {
        var valueRows = new List<List<Expression>>(RowCount);
        for (int rowIndex = 0; rowIndex < RowCount; rowIndex++)
        {
            var valueRow = new List<Expression>(ValueRows[rowIndex].Length);
            for (int i = 0; i < ValueRows[rowIndex].Length; i++)
                valueRow.Add(ToLiteralExpression(ValueRows[rowIndex][i]));

            valueRows.Add(valueRow);
        }

        return new InsertStatement
        {
            TableName = TableName,
            ColumnNames = null,
            ValueRows = valueRows,
        };
    }

    private static LiteralExpression ToLiteralExpression(DbValue value)
    {
        return value.Type switch
        {
            DbType.Null => new LiteralExpression { LiteralType = TokenType.Null, Value = null },
            DbType.Integer => new LiteralExpression { LiteralType = TokenType.IntegerLiteral, Value = value.AsInteger },
            DbType.Real => new LiteralExpression { LiteralType = TokenType.RealLiteral, Value = value.AsReal },
            DbType.Text => new LiteralExpression { LiteralType = TokenType.StringLiteral, Value = value.AsText },
            _ => throw new CSharpDbException(ErrorCode.SyntaxError, $"Unsupported fast INSERT literal type: {value.Type}."),
        };
    }
}

public sealed class Parser
{
    private readonly List<Token> _tokens;
    private int _pos;

    public Parser(List<Token> tokens)
    {
        _tokens = tokens;
        _pos = 0;
    }

    public static Statement Parse(string sql)
    {
        if (TryParseSimpleSelect(sql, out var simpleSelect))
            return simpleSelect;
        if (TryParseSimpleInsert(sql, out var simpleInsert))
            return simpleInsert.ToStatement();

        var tokenizer = new Tokenizer(sql);
        var tokens = tokenizer.Tokenize();
        var parser = new Parser(tokens);
        return parser.ParseStatement();
    }

    /// <summary>
    /// Fast-path parser for simple single-table SELECT queries:
    /// SELECT col-list FROM table [WHERE col = literal [AND col = literal]*] [;]
    /// Falls back to the full parser when unsupported syntax is encountered.
    /// </summary>
    public static bool TryParseSimpleSelect(string sql, out Statement statement)
    {
        var fast = new FastSimpleSelectParser(sql);
        return fast.TryParse(out statement);
    }

    /// <summary>
    /// Fast-path parser for a narrow point-lookup shape:
    /// SELECT *|column FROM table WHERE column = literal [AND column = literal] [;]
    /// Produces lightweight metadata for direct planner execution.
    /// </summary>
    public static bool TryParseSimplePrimaryKeyLookup(string sql, out SimplePrimaryKeyLookupSql lookup)
    {
        var fast = new FastPrimaryKeyLookupParser(sql);
        return fast.TryParse(out lookup);
    }

    /// <summary>
    /// Fast-path parser for a narrow INSERT shape:
    /// INSERT INTO table VALUES (literal [, literal ...]) [, (...)] [;]
    /// Produces resolved values directly, bypassing tokenization and AST expression allocation.
    /// </summary>
    public static bool TryParseSimpleInsert(string sql, out SimpleInsertSql insert)
    {
        var fast = new FastSimpleInsertParser(sql);
        return fast.TryParse(out insert);
    }

    public Statement ParseStatement()
    {
        var token = Peek();
        Statement stmt = token.Type switch
        {
            TokenType.Create => ParseCreate(),
            TokenType.Drop => ParseDrop(),
            TokenType.Insert => ParseInsert(),
            TokenType.Select => ParseQueryExpression(),
            TokenType.Delete => ParseDelete(),
            TokenType.Update => ParseUpdate(),
            TokenType.Alter => ParseAlterTable(),
            TokenType.With => ParseWith(),
            TokenType.Analyze => ParseAnalyze(),
            _ => throw Error($"Unexpected token '{token.Value}', expected a statement."),
        };

        // Optional trailing semicolon
        if (Peek().Type == TokenType.Semicolon)
            Advance();

        return stmt;
    }

    private ref struct FastSimpleSelectParser
    {
        private readonly ReadOnlySpan<char> _text;
        private int _pos;

        public FastSimpleSelectParser(string sql)
        {
            _text = sql.AsSpan();
            _pos = 0;
        }

        public bool TryParse(out Statement statement)
        {
            statement = null!;

            if (!TryReadKeyword("SELECT"))
                return false;

            if (!TryParseSelectColumns(out var columns))
                return false;

            if (!TryReadKeyword("FROM"))
                return false;

            if (!TryReadMultipartIdentifier(out string tableName))
                return false;

            Expression? where = null;
            if (TryReadKeyword("WHERE"))
            {
                if (!TryParseWhereConjuncts(out where))
                    return false;
            }

            if (!TryConsumeOptionalSemicolonAndRequireEnd())
                return false;

            statement = new SelectStatement
            {
                IsDistinct = false,
                Columns = columns,
                From = new SimpleTableRef { TableName = tableName },
                Where = where,
                GroupBy = null,
                Having = null,
                OrderBy = null,
                Limit = null,
                Offset = null,
            };
            return true;
        }

        private bool TryParseSelectColumns(out List<SelectColumn> columns)
        {
            columns = new List<SelectColumn>();

            while (true)
            {
                SkipWhitespace();

                SelectColumn column;
                if (TryConsumeChar('*'))
                {
                    column = new SelectColumn { IsStar = true };
                }
                else
                {
                    if (!TryReadIdentifier(out string columnName))
                        return false;

                    string? alias = null;
                    if (TryReadKeyword("AS"))
                    {
                        if (!TryReadIdentifier(out alias))
                            return false;
                    }

                    column = new SelectColumn
                    {
                        IsStar = false,
                        Expression = new ColumnRefExpression { ColumnName = columnName },
                        Alias = alias,
                    };
                }

                columns.Add(column);

                SkipWhitespace();
                if (!TryConsumeChar(','))
                    break;
            }

            return columns.Count > 0;
        }

        private bool TryParseWhereConjuncts(out Expression? where)
        {
            where = null;
            if (!TryParseSingleEqualityCondition(out var first))
                return false;

            where = first;
            while (TryReadKeyword("AND"))
            {
                if (!TryParseSingleEqualityCondition(out var next))
                    return false;

                where = new BinaryExpression
                {
                    Op = BinaryOp.And,
                    Left = where,
                    Right = next,
                };
            }

            return true;
        }

        private bool TryParseSingleEqualityCondition(out Expression condition)
        {
            condition = null!;

            if (!TryReadIdentifier(out string columnName))
                return false;

            SkipWhitespace();
            if (!TryConsumeChar('='))
                return false;

            if (!TryReadLiteral(out var literal))
                return false;

            condition = new BinaryExpression
            {
                Op = BinaryOp.Equals,
                Left = new ColumnRefExpression { ColumnName = columnName },
                Right = literal,
            };
            return true;
        }

        private bool TryReadLiteral(out LiteralExpression literal)
        {
            literal = null!;
            SkipWhitespace();

            if (_pos >= _text.Length)
                return false;

            if (_text[_pos] == '\'')
                return TryReadStringLiteral(out literal);

            if (TryReadKeyword("NULL"))
            {
                literal = new LiteralExpression
                {
                    LiteralType = TokenType.Null,
                    Value = null,
                };
                return true;
            }

            return TryReadNumericLiteral(out literal);
        }

        private bool TryReadStringLiteral(out LiteralExpression literal)
        {
            literal = null!;
            if (!TryConsumeChar('\''))
                return false;

            var sb = new System.Text.StringBuilder();
            while (_pos < _text.Length)
            {
                char c = _text[_pos++];
                if (c == '\'')
                {
                    if (_pos < _text.Length && _text[_pos] == '\'')
                    {
                        sb.Append('\'');
                        _pos++;
                        continue;
                    }

                    literal = new LiteralExpression
                    {
                        LiteralType = TokenType.StringLiteral,
                        Value = sb.ToString(),
                    };
                    return true;
                }

                sb.Append(c);
            }

            return false;
        }

        private bool TryReadNumericLiteral(out LiteralExpression literal)
        {
            literal = null!;
            SkipWhitespace();
            int start = _pos;

            if (_pos < _text.Length && (_text[_pos] == '-' || _text[_pos] == '+'))
                _pos++;

            int intPartStart = _pos;
            while (_pos < _text.Length && char.IsDigit(_text[_pos]))
                _pos++;
            bool hasIntDigits = _pos > intPartStart;

            bool hasDot = false;
            if (_pos < _text.Length && _text[_pos] == '.')
            {
                hasDot = true;
                _pos++;
                int fracStart = _pos;
                while (_pos < _text.Length && char.IsDigit(_text[_pos]))
                    _pos++;

                if (!hasIntDigits && _pos == fracStart)
                    return false;
            }
            else if (!hasIntDigits)
            {
                _pos = start;
                return false;
            }

            var literalText = _text.Slice(start, _pos - start);
            if (hasDot)
            {
                if (!double.TryParse(literalText, NumberStyles.Float, CultureInfo.InvariantCulture, out double realValue))
                    return false;

                literal = new LiteralExpression
                {
                    LiteralType = TokenType.RealLiteral,
                    Value = realValue,
                };
                return true;
            }

            if (!long.TryParse(literalText, NumberStyles.Integer, CultureInfo.InvariantCulture, out long intValue))
                return false;

            literal = new LiteralExpression
            {
                LiteralType = TokenType.IntegerLiteral,
                Value = intValue,
            };
            return true;
        }

        private bool TryReadKeyword(string keyword)
        {
            SkipWhitespace();
            if (_pos + keyword.Length > _text.Length)
                return false;

            var span = _text.Slice(_pos, keyword.Length);
            if (!span.Equals(keyword.AsSpan(), StringComparison.OrdinalIgnoreCase))
                return false;

            int end = _pos + keyword.Length;
            if (end < _text.Length && IsIdentifierChar(_text[end]))
                return false;

            _pos = end;
            return true;
        }

        private bool TryReadIdentifier(out string identifier)
        {
            identifier = string.Empty;
            SkipWhitespace();
            if (_pos >= _text.Length || !IsIdentifierStart(_text[_pos]))
                return false;

            int start = _pos++;
            while (_pos < _text.Length && IsIdentifierChar(_text[_pos]))
                _pos++;

            identifier = _text.Slice(start, _pos - start).ToString();
            return true;
        }

        private bool TryReadMultipartIdentifier(out string identifier)
        {
            if (!TryReadIdentifier(out identifier))
                return false;

            while (true)
            {
                SkipWhitespace();
                if (_pos >= _text.Length || _text[_pos] != '.')
                    break;

                _pos++; // consume '.'

                if (!TryReadIdentifier(out string part))
                    return false;

                identifier = $"{identifier}.{part}";
            }

            return true;
        }

        private bool TryConsumeChar(char ch)
        {
            SkipWhitespace();
            if (_pos >= _text.Length || _text[_pos] != ch)
                return false;

            _pos++;
            return true;
        }

        private bool TryConsumeOptionalSemicolonAndRequireEnd()
        {
            SkipWhitespace();
            if (_pos < _text.Length && _text[_pos] == ';')
                _pos++;

            SkipWhitespace();
            return _pos == _text.Length;
        }

        private void SkipWhitespace()
        {
            while (_pos < _text.Length && char.IsWhiteSpace(_text[_pos]))
                _pos++;
        }

        private static bool IsIdentifierStart(char c) => char.IsLetter(c) || c == '_';
        private static bool IsIdentifierChar(char c) => char.IsLetterOrDigit(c) || c == '_';
    }

    private ref struct FastPrimaryKeyLookupParser
    {
        private readonly ReadOnlySpan<char> _text;
        private int _pos;

        public FastPrimaryKeyLookupParser(string sql)
        {
            _text = sql.AsSpan();
            _pos = 0;
        }

        public bool TryParse(out SimplePrimaryKeyLookupSql lookup)
        {
            lookup = default;

            if (!TryReadKeyword("SELECT"))
                return false;

            SkipWhitespace();
            bool selectStar;
            string[] projectionColumns;
            if (TryConsumeChar('*'))
            {
                selectStar = true;
                projectionColumns = Array.Empty<string>();
            }
            else
            {
                if (!TryReadProjectionColumns(out projectionColumns))
                    return false;
                selectStar = false;
            }

            if (!TryReadKeyword("FROM"))
                return false;

            if (!TryReadMultipartIdentifier(out string tableName))
                return false;

            if (!TryReadKeyword("WHERE"))
                return false;

            if (!TryReadIdentifier(out string predicateColumn))
                return false;

            SkipWhitespace();
            if (!TryConsumeChar('='))
                return false;

            if (!TryReadSimpleLiteral(out DbValue predicateLiteral))
                return false;

            bool hasResidual = false;
            string residualColumn = string.Empty;
            DbValue residualLiteral = default;
            if (TryReadKeyword("AND"))
            {
                if (!TryReadIdentifier(out residualColumn))
                    return false;

                SkipWhitespace();
                if (!TryConsumeChar('='))
                    return false;

                if (!TryReadSimpleLiteral(out residualLiteral))
                    return false;

                hasResidual = true;
            }

            if (!TryConsumeOptionalSemicolonAndRequireEnd())
                return false;

            long lookupValue = predicateLiteral.Type == DbType.Integer
                ? predicateLiteral.AsInteger
                : 0;

            lookup = new SimplePrimaryKeyLookupSql(
                tableName,
                selectStar,
                projectionColumns,
                predicateColumn,
                lookupValue,
                hasResidual,
                residualColumn,
                residualLiteral,
                predicateLiteral);
            return true;
        }

        private bool TryReadProjectionColumns(out string[] projectionColumns)
        {
            var columns = new List<string>();
            do
            {
                if (!TryReadIdentifier(out string projectionColumn))
                {
                    projectionColumns = Array.Empty<string>();
                    return false;
                }

                columns.Add(projectionColumn);
                SkipWhitespace();
            }
            while (TryConsumeChar(','));

            projectionColumns = columns.ToArray();
            return projectionColumns.Length > 0;
        }

        private bool TryReadSimpleLiteral(out DbValue literal)
        {
            literal = default;
            SkipWhitespace();

            if (_pos >= _text.Length)
                return false;

            if (_text[_pos] == '\'')
            {
                if (!TryReadStringLiteral(out string textValue))
                    return false;

                literal = DbValue.FromText(textValue);
                return true;
            }

            if (TryReadKeyword("NULL"))
            {
                literal = DbValue.Null;
                return true;
            }

            if (!TryReadIntegerLiteral(out long intValue))
                return false;

            literal = DbValue.FromInteger(intValue);
            return true;
        }

        private bool TryReadStringLiteral(out string value)
        {
            value = string.Empty;
            if (!TryConsumeChar('\''))
                return false;

            var sb = new System.Text.StringBuilder();
            while (_pos < _text.Length)
            {
                char c = _text[_pos++];
                if (c == '\'')
                {
                    if (_pos < _text.Length && _text[_pos] == '\'')
                    {
                        sb.Append('\'');
                        _pos++;
                        continue;
                    }

                    value = sb.ToString();
                    return true;
                }

                sb.Append(c);
            }

            return false;
        }

        private bool TryReadIntegerLiteral(out long value)
        {
            value = 0;
            SkipWhitespace();

            int start = _pos;
            if (_pos < _text.Length && (_text[_pos] == '+' || _text[_pos] == '-'))
                _pos++;

            int digitStart = _pos;
            while (_pos < _text.Length && char.IsDigit(_text[_pos]))
                _pos++;

            if (_pos == digitStart)
            {
                _pos = start;
                return false;
            }

            return long.TryParse(
                _text.Slice(start, _pos - start),
                NumberStyles.Integer,
                CultureInfo.InvariantCulture,
                out value);
        }

        private bool TryReadKeyword(string keyword)
        {
            SkipWhitespace();
            if (_pos + keyword.Length > _text.Length)
                return false;

            var span = _text.Slice(_pos, keyword.Length);
            if (!span.Equals(keyword.AsSpan(), StringComparison.OrdinalIgnoreCase))
                return false;

            int end = _pos + keyword.Length;
            if (end < _text.Length && IsIdentifierChar(_text[end]))
                return false;

            _pos = end;
            return true;
        }

        private bool TryReadIdentifier(out string identifier)
        {
            identifier = string.Empty;
            SkipWhitespace();
            if (_pos >= _text.Length || !IsIdentifierStart(_text[_pos]))
                return false;

            int start = _pos++;
            while (_pos < _text.Length && IsIdentifierChar(_text[_pos]))
                _pos++;

            identifier = _text.Slice(start, _pos - start).ToString();
            return true;
        }

        private bool TryReadMultipartIdentifier(out string identifier)
        {
            if (!TryReadIdentifier(out identifier))
                return false;

            while (true)
            {
                SkipWhitespace();
                if (_pos >= _text.Length || _text[_pos] != '.')
                    break;

                _pos++;

                if (!TryReadIdentifier(out string part))
                    return false;

                identifier = $"{identifier}.{part}";
            }

            return true;
        }

        private bool TryConsumeChar(char ch)
        {
            SkipWhitespace();
            if (_pos >= _text.Length || _text[_pos] != ch)
                return false;

            _pos++;
            return true;
        }

        private bool TryConsumeOptionalSemicolonAndRequireEnd()
        {
            SkipWhitespace();
            if (_pos < _text.Length && _text[_pos] == ';')
                _pos++;

            SkipWhitespace();
            return _pos == _text.Length;
        }

        private void SkipWhitespace()
        {
            while (_pos < _text.Length && char.IsWhiteSpace(_text[_pos]))
                _pos++;
        }

        private static bool IsIdentifierStart(char c) => char.IsLetter(c) || c == '_';
        private static bool IsIdentifierChar(char c) => char.IsLetterOrDigit(c) || c == '_';
    }

    private ref struct FastSimpleInsertParser
    {
        private readonly ReadOnlySpan<char> _text;
        private int _pos;

        public FastSimpleInsertParser(string sql)
        {
            _text = sql.AsSpan();
            _pos = 0;
        }

        public bool TryParse(out SimpleInsertSql insert)
        {
            insert = default;

            if (!TryReadKeyword("INSERT"))
                return false;

            if (!TryReadKeyword("INTO"))
                return false;

            if (!TryReadMultipartIdentifier(out string tableName))
                return false;

            if (!TryReadKeyword("VALUES"))
                return false;

            var valueRows = new List<DbValue[]>();
            int expectedValueCount = -1;
            do
            {
                if (!TryConsumeChar('('))
                    return false;

                var values = new List<DbValue>();
                do
                {
                    if (!TryReadLiteral(out var value))
                        return false;

                    values.Add(value);
                } while (TryConsumeChar(','));

                if (!TryConsumeChar(')'))
                    return false;

                if (values.Count == 0)
                    return false;

                if (expectedValueCount >= 0 && values.Count != expectedValueCount)
                    return false;

                expectedValueCount = values.Count;
                valueRows.Add(values.ToArray());
            } while (TryConsumeChar(','));

            if (!TryConsumeOptionalSemicolonAndRequireEnd())
                return false;

            insert = new SimpleInsertSql(tableName, valueRows.ToArray());
            return valueRows.Count > 0;
        }

        private bool TryReadLiteral(out DbValue literal)
        {
            literal = default;
            SkipWhitespace();

            if (_pos >= _text.Length)
                return false;

            if (_text[_pos] == '\'')
            {
                if (!TryReadStringLiteral(out string textValue))
                    return false;

                literal = DbValue.FromText(textValue);
                return true;
            }

            if (TryReadKeyword("NULL"))
            {
                literal = DbValue.Null;
                return true;
            }

            return TryReadNumericLiteral(out literal);
        }

        private bool TryReadStringLiteral(out string value)
        {
            value = string.Empty;
            if (!TryConsumeChar('\''))
                return false;

            int segmentStart = _pos;
            System.Text.StringBuilder? builder = null;

            while (_pos < _text.Length)
            {
                char c = _text[_pos++];
                if (c != '\'')
                    continue;

                if (_pos < _text.Length && _text[_pos] == '\'')
                {
                    builder ??= new System.Text.StringBuilder();
                    builder.Append(_text.Slice(segmentStart, _pos - segmentStart - 1));
                    builder.Append('\'');
                    _pos++;
                    segmentStart = _pos;
                    continue;
                }

                if (builder == null)
                {
                    value = _text.Slice(segmentStart, _pos - segmentStart - 1).ToString();
                    return true;
                }

                builder.Append(_text.Slice(segmentStart, _pos - segmentStart - 1));
                value = builder.ToString();
                return true;
            }

            return false;
        }

        private bool TryReadNumericLiteral(out DbValue literal)
        {
            literal = default;
            SkipWhitespace();
            int start = _pos;

            if (_pos < _text.Length && (_text[_pos] == '-' || _text[_pos] == '+'))
                _pos++;

            int intPartStart = _pos;
            while (_pos < _text.Length && char.IsDigit(_text[_pos]))
                _pos++;
            bool hasIntDigits = _pos > intPartStart;

            bool hasDot = false;
            if (_pos < _text.Length && _text[_pos] == '.')
            {
                hasDot = true;
                _pos++;
                int fracStart = _pos;
                while (_pos < _text.Length && char.IsDigit(_text[_pos]))
                    _pos++;

                if (!hasIntDigits && _pos == fracStart)
                {
                    _pos = start;
                    return false;
                }
            }
            else if (!hasIntDigits)
            {
                _pos = start;
                return false;
            }

            var literalText = _text.Slice(start, _pos - start);
            if (hasDot)
            {
                if (!double.TryParse(literalText, NumberStyles.Float, CultureInfo.InvariantCulture, out double realValue))
                    return false;

                literal = DbValue.FromReal(realValue);
                return true;
            }

            if (!long.TryParse(literalText, NumberStyles.Integer, CultureInfo.InvariantCulture, out long intValue))
                return false;

            literal = DbValue.FromInteger(intValue);
            return true;
        }

        private bool TryReadKeyword(string keyword)
        {
            SkipWhitespace();
            if (_pos + keyword.Length > _text.Length)
                return false;

            var span = _text.Slice(_pos, keyword.Length);
            if (!span.Equals(keyword.AsSpan(), StringComparison.OrdinalIgnoreCase))
                return false;

            int end = _pos + keyword.Length;
            if (end < _text.Length && IsIdentifierChar(_text[end]))
                return false;

            _pos = end;
            return true;
        }

        private bool TryReadIdentifier(out string identifier)
        {
            identifier = string.Empty;
            SkipWhitespace();
            if (_pos >= _text.Length || !IsIdentifierStart(_text[_pos]))
                return false;

            int start = _pos++;
            while (_pos < _text.Length && IsIdentifierChar(_text[_pos]))
                _pos++;

            identifier = _text.Slice(start, _pos - start).ToString();
            return true;
        }

        private bool TryReadMultipartIdentifier(out string identifier)
        {
            if (!TryReadIdentifier(out identifier))
                return false;

            while (true)
            {
                SkipWhitespace();
                if (_pos >= _text.Length || _text[_pos] != '.')
                    break;

                _pos++;
                if (!TryReadIdentifier(out string part))
                    return false;

                identifier = $"{identifier}.{part}";
            }

            return true;
        }

        private bool TryConsumeChar(char ch)
        {
            SkipWhitespace();
            if (_pos >= _text.Length || _text[_pos] != ch)
                return false;

            _pos++;
            return true;
        }

        private bool TryConsumeOptionalSemicolonAndRequireEnd()
        {
            SkipWhitespace();
            if (_pos < _text.Length && _text[_pos] == ';')
                _pos++;

            SkipWhitespace();
            return _pos == _text.Length;
        }

        private void SkipWhitespace()
        {
            while (_pos < _text.Length && char.IsWhiteSpace(_text[_pos]))
                _pos++;
        }

        private static bool IsIdentifierStart(char c) => char.IsLetter(c) || c == '_';
        private static bool IsIdentifierChar(char c) => char.IsLetterOrDigit(c) || c == '_';
    }

    #region DDL

    private Statement ParseCreate()
    {
        Expect(TokenType.Create);

        if (Peek().Type == TokenType.Unique)
        {
            Advance(); // consume UNIQUE
            Expect(TokenType.Index);
            return ParseCreateIndexBody(isUnique: true);
        }

        var t = Peek().Type;
        if (t == TokenType.Index)
        {
            Advance(); // consume INDEX
            return ParseCreateIndexBody(isUnique: false);
        }
        if (t == TokenType.View)
        {
            Advance(); // consume VIEW
            return ParseCreateViewBody();
        }
        if (t == TokenType.Trigger)
        {
            Advance(); // consume TRIGGER
            return ParseCreateTriggerBody();
        }

        // Default: CREATE TABLE
        return ParseCreateTableBody();
    }

    private CreateTableStatement ParseCreateTableBody()
    {
        Expect(TokenType.Table);

        bool ifNotExists = false;
        if (Peek().Type == TokenType.If)
        {
            Advance();
            Expect(TokenType.Not);
            Expect(TokenType.Exists);
            ifNotExists = true;
        }

        string tableName = ExpectIdentifier();
        Expect(TokenType.LeftParen);

        var columns = new List<ColumnDef>();
        do
        {
            columns.Add(ParseColumnDef());
        } while (TryConsume(TokenType.Comma));

        Expect(TokenType.RightParen);

        return new CreateTableStatement
        {
            TableName = tableName,
            Columns = columns,
            IfNotExists = ifNotExists,
        };
    }

    private CreateIndexStatement ParseCreateIndexBody(bool isUnique)
    {
        bool ifNotExists = false;
        if (Peek().Type == TokenType.If)
        {
            Advance();
            Expect(TokenType.Not);
            Expect(TokenType.Exists);
            ifNotExists = true;
        }

        string indexName = ExpectIdentifier();
        Expect(TokenType.On);
        string tableName = ExpectIdentifier();
        Expect(TokenType.LeftParen);

        var columns = new List<string>();
        do
        {
            columns.Add(ExpectIdentifier());
        } while (TryConsume(TokenType.Comma));

        Expect(TokenType.RightParen);

        return new CreateIndexStatement
        {
            IndexName = indexName,
            TableName = tableName,
            Columns = columns,
            IsUnique = isUnique,
            IfNotExists = ifNotExists,
        };
    }

    private CreateViewStatement ParseCreateViewBody()
    {
        bool ifNotExists = false;
        if (Peek().Type == TokenType.If)
        {
            Advance();
            Expect(TokenType.Not);
            Expect(TokenType.Exists);
            ifNotExists = true;
        }

        string viewName = ExpectIdentifier();
        Expect(TokenType.As);
        var query = ParseQueryExpression();

        return new CreateViewStatement
        {
            ViewName = viewName,
            Query = query,
            IfNotExists = ifNotExists,
        };
    }

    private ColumnDef ParseColumnDef()
    {
        string name = ExpectIdentifier();
        var typeToken = Peek().Type;

        if (typeToken is not (TokenType.Integer or TokenType.Real or TokenType.Text or TokenType.Blob))
            throw Error($"Expected type name, got '{Peek().Value}'.");
        Advance();

        bool isPK = false;
        bool isIdentity = false;
        bool isNullable = true;

        // Check for PRIMARY KEY / NOT NULL / IDENTITY / AUTOINCREMENT modifiers.
        while (true)
        {
            if (Peek().Type == TokenType.Primary)
            {
                Advance();
                Expect(TokenType.Key);
                if (isPK)
                    throw Error($"PRIMARY KEY specified multiple times for column '{name}'.");
                isPK = true;
                isNullable = false;
            }
            else if (Peek().Type is TokenType.Identity or TokenType.Autoincrement)
            {
                if (isIdentity)
                    throw Error($"IDENTITY/AUTOINCREMENT specified multiple times for column '{name}'.");
                Advance();
                isIdentity = true;
            }
            else if (Peek().Type == TokenType.Not)
            {
                Advance();
                Expect(TokenType.Null);
                if (!isNullable)
                    throw Error($"NOT NULL specified multiple times for column '{name}'.");
                isNullable = false;
            }
            else break;
        }

        if (isIdentity)
        {
            if (typeToken != TokenType.Integer)
                throw Error($"IDENTITY/AUTOINCREMENT requires INTEGER type for column '{name}'.");

            // Identity always implies PK semantics in CSharpDB.
            isPK = true;
            isNullable = false;
        }

        return new ColumnDef
        {
            Name = name,
            TypeToken = typeToken,
            IsPrimaryKey = isPK,
            IsIdentity = isIdentity,
            IsNullable = isNullable,
        };
    }

    private Statement ParseDrop()
    {
        Expect(TokenType.Drop);

        var t = Peek().Type;
        if (t == TokenType.Index)
        {
            Advance(); // consume INDEX
            return ParseDropIndexBody();
        }
        if (t == TokenType.View)
        {
            Advance(); // consume VIEW
            return ParseDropViewBody();
        }
        if (t == TokenType.Trigger)
        {
            Advance(); // consume TRIGGER
            return ParseDropTriggerBody();
        }

        // Default: DROP TABLE
        return ParseDropTableBody();
    }

    private DropTableStatement ParseDropTableBody()
    {
        Expect(TokenType.Table);

        bool ifExists = false;
        if (Peek().Type == TokenType.If)
        {
            Advance();
            Expect(TokenType.Exists);
            ifExists = true;
        }

        string tableName = ExpectIdentifier();
        return new DropTableStatement { TableName = tableName, IfExists = ifExists };
    }

    private DropIndexStatement ParseDropIndexBody()
    {
        bool ifExists = false;
        if (Peek().Type == TokenType.If)
        {
            Advance();
            Expect(TokenType.Exists);
            ifExists = true;
        }

        string indexName = ExpectIdentifier();
        return new DropIndexStatement { IndexName = indexName, IfExists = ifExists };
    }

    private DropViewStatement ParseDropViewBody()
    {
        bool ifExists = false;
        if (Peek().Type == TokenType.If)
        {
            Advance();
            Expect(TokenType.Exists);
            ifExists = true;
        }

        string viewName = ExpectIdentifier();
        return new DropViewStatement { ViewName = viewName, IfExists = ifExists };
    }

    private CreateTriggerStatement ParseCreateTriggerBody()
    {
        bool ifNotExists = false;
        if (Peek().Type == TokenType.If)
        {
            Advance();
            Expect(TokenType.Not);
            Expect(TokenType.Exists);
            ifNotExists = true;
        }

        string triggerName = ExpectIdentifier();

        // BEFORE | AFTER
        CSharpDB.Primitives.TriggerTiming timing;
        if (Peek().Type == TokenType.Before) { Advance(); timing = CSharpDB.Primitives.TriggerTiming.Before; }
        else if (Peek().Type == TokenType.After) { Advance(); timing = CSharpDB.Primitives.TriggerTiming.After; }
        else throw Error("Expected BEFORE or AFTER.");

        // INSERT | UPDATE | DELETE
        CSharpDB.Primitives.TriggerEvent evt;
        if (Peek().Type == TokenType.Insert) { Advance(); evt = CSharpDB.Primitives.TriggerEvent.Insert; }
        else if (Peek().Type == TokenType.Update) { Advance(); evt = CSharpDB.Primitives.TriggerEvent.Update; }
        else if (Peek().Type == TokenType.Delete) { Advance(); evt = CSharpDB.Primitives.TriggerEvent.Delete; }
        else throw Error("Expected INSERT, UPDATE, or DELETE.");

        Expect(TokenType.On);
        string tableName = ExpectIdentifier();

        // Optional: FOR EACH ROW
        if (Peek().Type == TokenType.For)
        {
            Advance();
            Expect(TokenType.Each);
            Expect(TokenType.Row);
        }

        // Optional: WHEN (condition)
        Expression? whenCondition = null;
        if (Peek().Type == TokenType.Where || (Peek().Type == TokenType.Identifier && Peek().Value.Equals("WHEN", StringComparison.OrdinalIgnoreCase)))
        {
            // WHEN is not a keyword token, handle it as identifier check
            Advance();
            Expect(TokenType.LeftParen);
            whenCondition = ParseExpression();
            Expect(TokenType.RightParen);
        }

        // BEGIN ... END
        Expect(TokenType.Begin);
        var body = new List<Statement>();
        while (Peek().Type != TokenType.End && Peek().Type != TokenType.Eof)
        {
            body.Add(ParseStatement());
            // ParseStatement already consumes trailing semicolons
        }
        Expect(TokenType.End);

        return new CreateTriggerStatement
        {
            TriggerName = triggerName,
            TableName = tableName,
            Timing = timing,
            Event = evt,
            WhenCondition = whenCondition,
            Body = body,
            IfNotExists = ifNotExists,
        };
    }

    private DropTriggerStatement ParseDropTriggerBody()
    {
        bool ifExists = false;
        if (Peek().Type == TokenType.If)
        {
            Advance();
            Expect(TokenType.Exists);
            ifExists = true;
        }

        string triggerName = ExpectIdentifier();
        return new DropTriggerStatement { TriggerName = triggerName, IfExists = ifExists };
    }

    private AlterTableStatement ParseAlterTable()
    {
        Expect(TokenType.Alter);
        Expect(TokenType.Table);
        string tableName = ExpectIdentifier();

        AlterAction action;
        var t = Peek().Type;

        if (t == TokenType.Add)
        {
            Advance();
            // Optional COLUMN keyword
            TryConsume(TokenType.Column);
            var colDef = ParseColumnDef();
            action = new AddColumnAction { Column = colDef };
        }
        else if (t == TokenType.Drop)
        {
            Advance();
            // Optional COLUMN keyword
            TryConsume(TokenType.Column);
            string colName = ExpectIdentifier();
            action = new DropColumnAction { ColumnName = colName };
        }
        else if (t == TokenType.Rename)
        {
            Advance();
            if (Peek().Type == TokenType.To)
            {
                // ALTER TABLE x RENAME TO y
                Advance();
                string newName = ExpectIdentifier();
                action = new RenameTableAction { NewTableName = newName };
            }
            else if (Peek().Type == TokenType.Column)
            {
                // ALTER TABLE x RENAME COLUMN old TO new
                Advance();
                string oldCol = ExpectIdentifier();
                Expect(TokenType.To);
                string newCol = ExpectIdentifier();
                action = new RenameColumnAction { OldColumnName = oldCol, NewColumnName = newCol };
            }
            else
            {
                // ALTER TABLE x RENAME old TO new (implicit COLUMN)
                string oldCol = ExpectIdentifier();
                Expect(TokenType.To);
                string newCol = ExpectIdentifier();
                action = new RenameColumnAction { OldColumnName = oldCol, NewColumnName = newCol };
            }
        }
        else
        {
            throw Error($"Expected ADD, DROP, or RENAME after ALTER TABLE, got '{Peek().Value}'.");
        }

        return new AlterTableStatement { TableName = tableName, Action = action };
    }

    #endregion

    #region DML

    private InsertStatement ParseInsert()
    {
        Expect(TokenType.Insert);
        Expect(TokenType.Into);
        string tableName = ExpectIdentifier();

        List<string>? columnNames = null;
        if (Peek().Type == TokenType.LeftParen)
        {
            // Check if this is a column list or VALUES
            // Peek ahead to see if the first item after '(' is an identifier
            if (_tokens[_pos + 1].Type == TokenType.Identifier)
            {
                Advance(); // consume '('
                columnNames = new List<string>();
                do
                {
                    columnNames.Add(ExpectIdentifier());
                } while (TryConsume(TokenType.Comma));
                Expect(TokenType.RightParen);
            }
        }

        Expect(TokenType.Values);

        var valueRows = new List<List<Expression>>();
        do
        {
            Expect(TokenType.LeftParen);
            var row = new List<Expression>();
            do
            {
                row.Add(ParseExpression());
            } while (TryConsume(TokenType.Comma));
            Expect(TokenType.RightParen);
            valueRows.Add(row);
        } while (TryConsume(TokenType.Comma));

        return new InsertStatement { TableName = tableName, ColumnNames = columnNames, ValueRows = valueRows };
    }

    private QueryStatement ParseQueryExpression()
    {
        QueryStatement query = ParseUnionExceptExpression();
        var orderBy = ParseOptionalOrderBy();
        int? limit = ParseOptionalLimit();
        int? offset = ParseOptionalOffset();

        return query switch
        {
            SelectStatement select => new SelectStatement
            {
                IsDistinct = select.IsDistinct,
                Columns = select.Columns,
                From = select.From,
                Where = select.Where,
                GroupBy = select.GroupBy,
                Having = select.Having,
                OrderBy = orderBy,
                Limit = limit,
                Offset = offset,
            },
            CompoundSelectStatement compound => new CompoundSelectStatement
            {
                Left = compound.Left,
                Right = compound.Right,
                Operation = compound.Operation,
                OrderBy = orderBy,
                Limit = limit,
                Offset = offset,
            },
            _ => throw new InvalidOperationException($"Unknown query statement type: {query.GetType().Name}"),
        };
    }

    private QueryStatement ParseUnionExceptExpression()
    {
        QueryStatement left = ParseIntersectExpression();

        while (Peek().Type is TokenType.Union or TokenType.Except)
        {
            var op = Advance().Type switch
            {
                TokenType.Union => SetOperationKind.Union,
                TokenType.Except => SetOperationKind.Except,
                _ => throw new InvalidOperationException(),
            };

            var right = ParseIntersectExpression();
            left = new CompoundSelectStatement
            {
                Left = left,
                Right = right,
                Operation = op,
            };
        }

        return left;
    }

    private QueryStatement ParseIntersectExpression()
    {
        QueryStatement left = ParseSelectCore();

        while (Peek().Type == TokenType.Intersect)
        {
            Advance();
            var right = ParseSelectCore();
            left = new CompoundSelectStatement
            {
                Left = left,
                Right = right,
                Operation = SetOperationKind.Intersect,
            };
        }

        return left;
    }

    private SelectStatement ParseSelectCore()
    {
        Expect(TokenType.Select);
        bool isDistinct = TryConsume(TokenType.Distinct);

        var columns = new List<SelectColumn>();
        if (Peek().Type == TokenType.Star)
        {
            Advance();
            columns.Add(new SelectColumn { IsStar = true });
        }
        else
        {
            do
            {
                var expr = ParseExpression();
                string? alias = null;
                if (Peek().Type == TokenType.As)
                {
                    Advance();
                    alias = ExpectIdentifier();
                }
                else if (Peek().Type == TokenType.Identifier)
                {
                    // Implicit alias: SELECT expr alias (no AS keyword)
                    alias = Peek().Value;
                    Advance();
                }
                columns.Add(new SelectColumn { Expression = expr, Alias = alias });
            } while (TryConsume(TokenType.Comma));
        }

        Expect(TokenType.From);
        var from = ParseTableRef();

        Expression? where = null;
        if (Peek().Type == TokenType.Where)
        {
            Advance();
            where = ParseExpression();
        }

        List<Expression>? groupBy = null;
        if (Peek().Type == TokenType.Group)
        {
            Advance();
            Expect(TokenType.By);
            groupBy = new List<Expression>();
            do
            {
                groupBy.Add(ParseExpression());
            } while (TryConsume(TokenType.Comma));
        }

        Expression? having = null;
        if (Peek().Type == TokenType.Having)
        {
            Advance();
            having = ParseExpression();
        }

        return new SelectStatement
        {
            IsDistinct = isDistinct,
            Columns = columns,
            From = from,
            Where = where,
            GroupBy = groupBy,
            Having = having,
            OrderBy = null,
            Limit = null,
            Offset = null,
        };
    }

    private List<OrderByClause>? ParseOptionalOrderBy()
    {
        if (Peek().Type != TokenType.Order)
            return null;

        Advance();
        Expect(TokenType.By);

        var orderBy = new List<OrderByClause>();
        do
        {
            var expr = ParseExpression();
            bool desc = false;
            if (Peek().Type == TokenType.Desc)
            {
                Advance();
                desc = true;
            }
            else if (Peek().Type == TokenType.Asc)
            {
                Advance();
            }

            orderBy.Add(new OrderByClause { Expression = expr, Descending = desc });
        } while (TryConsume(TokenType.Comma));

        return orderBy;
    }

    private int? ParseOptionalLimit()
    {
        if (Peek().Type != TokenType.Limit)
            return null;

        Advance();
        var limitToken = Expect(TokenType.IntegerLiteral);
        return int.Parse(limitToken.Value, CultureInfo.InvariantCulture);
    }

    private int? ParseOptionalOffset()
    {
        if (Peek().Type != TokenType.Offset)
            return null;

        Advance();
        var offsetToken = Expect(TokenType.IntegerLiteral);
        return int.Parse(offsetToken.Value, CultureInfo.InvariantCulture);
    }

    private DeleteStatement ParseDelete()
    {
        Expect(TokenType.Delete);
        Expect(TokenType.From);
        string tableName = ExpectIdentifier();

        Expression? where = null;
        if (Peek().Type == TokenType.Where)
        {
            Advance();
            where = ParseExpression();
        }

        return new DeleteStatement { TableName = tableName, Where = where };
    }

    private AnalyzeStatement ParseAnalyze()
    {
        Expect(TokenType.Analyze);

        string? tableName = null;
        if (Peek().Type == TokenType.Identifier)
            tableName = ParseMultipartIdentifier();

        return new AnalyzeStatement { TableName = tableName };
    }

    private UpdateStatement ParseUpdate()
    {
        Expect(TokenType.Update);
        string tableName = ExpectIdentifier();
        Expect(TokenType.Set);

        var setClauses = new List<SetClause>();
        do
        {
            string colName = ExpectIdentifier();
            Expect(TokenType.Equals);
            var value = ParseExpression();
            setClauses.Add(new SetClause { ColumnName = colName, Value = value });
        } while (TryConsume(TokenType.Comma));

        Expression? where = null;
        if (Peek().Type == TokenType.Where)
        {
            Advance();
            where = ParseExpression();
        }

        return new UpdateStatement { TableName = tableName, SetClauses = setClauses, Where = where };
    }

    private WithStatement ParseWith()
    {
        Expect(TokenType.With);

        // Optional RECURSIVE keyword (parsed but not used — recursive CTEs not supported yet)
        TryConsume(TokenType.Recursive);

        var ctes = new List<CteDefinition>();
        do
        {
            string cteName = ExpectIdentifier();

            // Optional column list: WITH name(col1, col2) AS (...)
            List<string>? columnNames = null;
            if (Peek().Type == TokenType.LeftParen)
            {
                // Peek ahead: if next token after '(' is an identifier followed by ',' or ')',
                // then this is a column name list. Otherwise it could be a subquery etc.
                // For CTE column lists, the token after '(' should always be an identifier.
                if (_tokens[_pos + 1].Type == TokenType.Identifier)
                {
                    Advance(); // consume '('
                    columnNames = new List<string>();
                    do
                    {
                        columnNames.Add(ExpectIdentifier());
                    } while (TryConsume(TokenType.Comma));
                    Expect(TokenType.RightParen);
                }
            }

            Expect(TokenType.As);
            Expect(TokenType.LeftParen);
            var query = ParseQueryExpression();
            Expect(TokenType.RightParen);

            ctes.Add(new CteDefinition { Name = cteName, ColumnNames = columnNames, Query = query });
        } while (TryConsume(TokenType.Comma));

        // The main query after the CTEs
        var mainQuery = ParseQueryExpression();

        return new WithStatement { Ctes = ctes, MainQuery = mainQuery };
    }

    #endregion

    #region FROM Clause / JOINs

    private TableRef ParseTableRef()
    {
        TableRef left = ParseSimpleTableRef();

        // Parse chained JOINs
        while (IsJoinKeyword(Peek().Type))
        {
            var joinType = ParseJoinType();
            var right = ParseSimpleTableRef();
            Expression? condition = null;
            if (joinType != JoinType.Cross && Peek().Type == TokenType.On)
            {
                Advance();
                condition = ParseExpression();
            }
            left = new JoinTableRef { Left = left, Right = right, JoinType = joinType, Condition = condition };
        }

        return left;
    }

    private SimpleTableRef ParseSimpleTableRef()
    {
        string tableName = ParseMultipartIdentifier();
        string? alias = null;

        // Check for alias: FROM users AS u  or  FROM users u
        if (Peek().Type == TokenType.As)
        {
            Advance();
            alias = ExpectIdentifier();
        }
        else if (Peek().Type == TokenType.Identifier && !IsClauseKeyword(Peek().Type))
        {
            // Implicit alias (no AS keyword)
            alias = Peek().Value;
            Advance();
        }

        return new SimpleTableRef { TableName = tableName, Alias = alias };
    }

    private string ParseMultipartIdentifier()
    {
        string identifier = ExpectIdentifier();
        while (TryConsume(TokenType.Dot))
            identifier = $"{identifier}.{ExpectIdentifier()}";

        return identifier;
    }

    private JoinType ParseJoinType()
    {
        var t = Peek().Type;

        if (t == TokenType.Cross)
        {
            Advance();
            Expect(TokenType.Join);
            return JoinType.Cross;
        }

        if (t == TokenType.Inner)
        {
            Advance();
            Expect(TokenType.Join);
            return JoinType.Inner;
        }

        if (t == TokenType.Left)
        {
            Advance();
            TryConsume(TokenType.Outer);
            Expect(TokenType.Join);
            return JoinType.LeftOuter;
        }

        if (t == TokenType.Right)
        {
            Advance();
            TryConsume(TokenType.Outer);
            Expect(TokenType.Join);
            return JoinType.RightOuter;
        }

        if (t == TokenType.Join)
        {
            Advance();
            return JoinType.Inner; // Plain JOIN = INNER JOIN
        }

        throw Error($"Expected JOIN keyword, got '{Peek().Value}'.");
    }

    private static bool IsJoinKeyword(TokenType type) =>
        type is TokenType.Join or TokenType.Inner or TokenType.Left or TokenType.Right or TokenType.Cross;

    /// <summary>
    /// Returns true if the token is a SQL clause keyword that cannot be a table alias.
    /// </summary>
    private static bool IsClauseKeyword(TokenType type) =>
        type is TokenType.Where or TokenType.Group or TokenType.Having
            or TokenType.Order or TokenType.Limit or TokenType.Offset
            or TokenType.On or TokenType.Join or TokenType.Inner
            or TokenType.Left or TokenType.Right or TokenType.Cross
            or TokenType.With
            or TokenType.Trigger or TokenType.Before or TokenType.After
            or TokenType.For or TokenType.Each or TokenType.Row
            or TokenType.Begin or TokenType.End
            or TokenType.New or TokenType.Old
            or TokenType.Eof or TokenType.Semicolon or TokenType.RightParen;

    #endregion

    #region Expression Parsing (precedence climbing)

    private Expression ParseExpression() => ParseOr();

    private Expression ParseOr()
    {
        var left = ParseAnd();
        while (Peek().Type == TokenType.Or)
        {
            Advance();
            var right = ParseAnd();
            left = new BinaryExpression { Op = BinaryOp.Or, Left = left, Right = right };
        }
        return left;
    }

    private Expression ParseAnd()
    {
        var left = ParseNot();
        while (Peek().Type == TokenType.And)
        {
            Advance();
            var right = ParseNot();
            left = new BinaryExpression { Op = BinaryOp.And, Left = left, Right = right };
        }
        return left;
    }

    private Expression ParseNot()
    {
        if (Peek().Type == TokenType.Not)
        {
            Advance();
            var operand = ParseNot();
            return new UnaryExpression { Op = TokenType.Not, Operand = operand };
        }
        return ParseComparison();
    }

    private Expression ParseComparison()
    {
        var left = ParseAddSub();
        var t = Peek().Type;

        // Check for [NOT] LIKE / IN / BETWEEN and IS [NOT] NULL
        bool negated = false;
        if (t == TokenType.Not)
        {
            var next = _tokens[_pos + 1].Type;
            if (next is TokenType.Like or TokenType.In or TokenType.Between)
            {
                Advance(); // consume NOT
                negated = true;
                t = Peek().Type;
            }
        }

        if (t == TokenType.Like)
        {
            Advance();
            var pattern = ParseAddSub();
            Expression? escapeChar = null;
            if (Peek().Type == TokenType.Escape)
            {
                Advance();
                escapeChar = ParsePrimary();
            }
            return new LikeExpression { Operand = left, Pattern = pattern, EscapeChar = escapeChar, Negated = negated };
        }

        if (t == TokenType.In)
        {
            Advance();
            Expect(TokenType.LeftParen);
            if (IsSubqueryStart(Peek().Type))
            {
                var query = ParseQueryExpression();
                Expect(TokenType.RightParen);
                return new InSubqueryExpression { Operand = left, Query = query, Negated = negated };
            }

            var values = new List<Expression>();
            do
            {
                values.Add(ParseExpression());
            } while (TryConsume(TokenType.Comma));
            Expect(TokenType.RightParen);
            return new InExpression { Operand = left, Values = values, Negated = negated };
        }

        if (t == TokenType.Between)
        {
            Advance();
            var low = ParseAddSub();
            Expect(TokenType.And);
            var high = ParseAddSub();
            return new BetweenExpression { Operand = left, Low = low, High = high, Negated = negated };
        }

        if (t == TokenType.Is)
        {
            Advance();
            bool isNot = false;
            if (Peek().Type == TokenType.Not)
            {
                Advance();
                isNot = true;
            }
            Expect(TokenType.Null);
            return new IsNullExpression { Operand = left, Negated = isNot };
        }

        BinaryOp? op = t switch
        {
            TokenType.Equals => BinaryOp.Equals,
            TokenType.NotEquals => BinaryOp.NotEquals,
            TokenType.LessThan => BinaryOp.LessThan,
            TokenType.GreaterThan => BinaryOp.GreaterThan,
            TokenType.LessOrEqual => BinaryOp.LessOrEqual,
            TokenType.GreaterOrEqual => BinaryOp.GreaterOrEqual,
            _ => null,
        };

        if (op != null)
        {
            Advance();
            var right = ParseAddSub();
            return new BinaryExpression { Op = op.Value, Left = left, Right = right };
        }

        return left;
    }

    private Expression ParseAddSub()
    {
        var left = ParseMulDiv();
        while (Peek().Type is TokenType.Plus or TokenType.Minus)
        {
            var op = Advance().Type == TokenType.Plus ? BinaryOp.Plus : BinaryOp.Minus;
            var right = ParseMulDiv();
            left = new BinaryExpression { Op = op, Left = left, Right = right };
        }
        return left;
    }

    private Expression ParseMulDiv()
    {
        var left = ParseUnary();
        while (Peek().Type is TokenType.Star or TokenType.Slash)
        {
            var op = Advance().Type == TokenType.Star ? BinaryOp.Multiply : BinaryOp.Divide;
            var right = ParseUnary();
            left = new BinaryExpression { Op = op, Left = left, Right = right };
        }
        return left;
    }

    private Expression ParseUnary()
    {
        if (Peek().Type == TokenType.Minus)
        {
            Advance();
            var operand = ParsePrimary();
            return new UnaryExpression { Op = TokenType.Minus, Operand = operand };
        }
        return ParsePrimary();
    }

    private static bool IsAggregateFunctionToken(TokenType type) =>
        type is TokenType.Count or TokenType.Sum or TokenType.Avg or TokenType.Min or TokenType.Max;

    private static bool IsScalarFunctionToken(TokenType type) =>
        type is TokenType.Text;

    private static bool IsSubqueryStart(TokenType type) =>
        type == TokenType.Select;

    private bool IsFunctionCallStart(Token token) =>
        (IsAggregateFunctionToken(token.Type) || IsScalarFunctionToken(token.Type) || token.Type == TokenType.Identifier)
        && _pos + 1 < _tokens.Count
        && _tokens[_pos + 1].Type == TokenType.LeftParen;

    private Expression ParseFunctionCall(Token token, bool allowAggregateModifiers)
    {
        string funcName = token.Value.ToUpperInvariant();
        Advance();
        Expect(TokenType.LeftParen);

        bool isStar = false;
        bool isDistinct = false;
        var args = new List<Expression>();

        if (allowAggregateModifiers && Peek().Type == TokenType.Star)
        {
            Advance();
            isStar = true;
        }
        else
        {
            if (Peek().Type == TokenType.Distinct)
            {
                if (!allowAggregateModifiers)
                    throw Error($"DISTINCT is only supported for aggregate functions, not {funcName}().");

                Advance();
                isDistinct = true;
            }

            args.Add(ParseExpression());
        }

        Expect(TokenType.RightParen);
        return new FunctionCallExpression
        {
            FunctionName = funcName,
            Arguments = args,
            IsStarArg = isStar,
            IsDistinct = isDistinct,
        };
    }

    private Expression ParsePrimary()
    {
        var token = Peek();

        if (IsFunctionCallStart(token))
            return ParseFunctionCall(token, allowAggregateModifiers: IsAggregateFunctionToken(token.Type));

        switch (token.Type)
        {
            case TokenType.IntegerLiteral:
                Advance();
                return new LiteralExpression
                {
                    Value = long.Parse(token.Value, CultureInfo.InvariantCulture),
                    LiteralType = TokenType.IntegerLiteral,
                };
            case TokenType.RealLiteral:
                Advance();
                return new LiteralExpression
                {
                    Value = double.Parse(token.Value, CultureInfo.InvariantCulture),
                    LiteralType = TokenType.RealLiteral,
                };
            case TokenType.StringLiteral:
                Advance();
                return new LiteralExpression { Value = token.Value, LiteralType = TokenType.StringLiteral };
            case TokenType.Null:
                Advance();
                return new LiteralExpression { Value = null, LiteralType = TokenType.Null };
            case TokenType.Parameter:
                Advance();
                return new ParameterExpression { Name = token.Value };
            case TokenType.Identifier:
                Advance();
                // Check for qualified column ref: table.column
                if (Peek().Type == TokenType.Dot)
                {
                    Advance(); // consume '.'
                    string colName = ExpectIdentifier();
                    return new ColumnRefExpression { TableAlias = token.Value, ColumnName = colName };
                }
                return new ColumnRefExpression { ColumnName = token.Value };
            case TokenType.New:
            case TokenType.Old:
                Advance();
                if (Peek().Type == TokenType.Dot)
                {
                    Advance(); // consume '.'
                    string trigColName = ExpectIdentifier();
                    return new ColumnRefExpression { TableAlias = token.Value.ToUpperInvariant(), ColumnName = trigColName };
                }
                // NEW/OLD without dot — treat as identifier
                return new ColumnRefExpression { ColumnName = token.Value };
            case TokenType.Exists:
                Advance();
                Expect(TokenType.LeftParen);
                if (!IsSubqueryStart(Peek().Type))
                    throw Error("EXISTS requires a subquery.");
                var existsQuery = ParseQueryExpression();
                Expect(TokenType.RightParen);
                return new ExistsExpression { Query = existsQuery };
            case TokenType.LeftParen:
                Advance();
                if (IsSubqueryStart(Peek().Type))
                {
                    var query = ParseQueryExpression();
                    Expect(TokenType.RightParen);
                    return new ScalarSubqueryExpression { Query = query };
                }
                var expr = ParseExpression();
                Expect(TokenType.RightParen);
                return expr;
            default:
                throw Error($"Unexpected token '{token.Value}' in expression.");
        }
    }

    #endregion

    #region Helpers

    private Token Peek() => _tokens[_pos];
    private Token Advance() => _tokens[_pos++];

    private Token Expect(TokenType type)
    {
        var token = Peek();
        if (token.Type != type)
            throw Error($"Expected {type}, got '{token.Value}' ({token.Type}).");
        return Advance();
    }

    private string ExpectIdentifier()
    {
        var token = Peek();
        if (token.Type != TokenType.Identifier)
            throw Error($"Expected identifier, got '{token.Value}' ({token.Type}).");
        Advance();
        return token.Value;
    }

    private bool TryConsume(TokenType type)
    {
        if (Peek().Type == type)
        {
            Advance();
            return true;
        }
        return false;
    }

    private CSharpDbException Error(string message) =>
        new(ErrorCode.SyntaxError, $"Syntax error at position {Peek().Position}: {message}");

    #endregion
}
