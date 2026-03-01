using System.Globalization;
using CSharpDB.Core;

namespace CSharpDB.Sql;

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

    public Statement ParseStatement()
    {
        var token = Peek();
        Statement stmt = token.Type switch
        {
            TokenType.Create => ParseCreate(),
            TokenType.Drop => ParseDrop(),
            TokenType.Insert => ParseInsert(),
            TokenType.Select => ParseSelect(),
            TokenType.Delete => ParseDelete(),
            TokenType.Update => ParseUpdate(),
            TokenType.Alter => ParseAlterTable(),
            TokenType.With => ParseWith(),
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
        var query = ParseSelect();

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
        bool isNullable = true;

        // Check for PRIMARY KEY and NOT NULL modifiers
        while (true)
        {
            if (Peek().Type == TokenType.Primary)
            {
                Advance();
                Expect(TokenType.Key);
                isPK = true;
                isNullable = false;
            }
            else if (Peek().Type == TokenType.Not)
            {
                Advance();
                Expect(TokenType.Null);
                isNullable = false;
            }
            else break;
        }

        return new ColumnDef { Name = name, TypeToken = typeToken, IsPrimaryKey = isPK, IsNullable = isNullable };
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
        CSharpDB.Core.TriggerTiming timing;
        if (Peek().Type == TokenType.Before) { Advance(); timing = CSharpDB.Core.TriggerTiming.Before; }
        else if (Peek().Type == TokenType.After) { Advance(); timing = CSharpDB.Core.TriggerTiming.After; }
        else throw Error("Expected BEFORE or AFTER.");

        // INSERT | UPDATE | DELETE
        CSharpDB.Core.TriggerEvent evt;
        if (Peek().Type == TokenType.Insert) { Advance(); evt = CSharpDB.Core.TriggerEvent.Insert; }
        else if (Peek().Type == TokenType.Update) { Advance(); evt = CSharpDB.Core.TriggerEvent.Update; }
        else if (Peek().Type == TokenType.Delete) { Advance(); evt = CSharpDB.Core.TriggerEvent.Delete; }
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

    private SelectStatement ParseSelect()
    {
        Expect(TokenType.Select);

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

        List<OrderByClause>? orderBy = null;
        if (Peek().Type == TokenType.Order)
        {
            Advance();
            Expect(TokenType.By);
            orderBy = new List<OrderByClause>();
            do
            {
                var expr = ParseExpression();
                bool desc = false;
                if (Peek().Type == TokenType.Desc) { Advance(); desc = true; }
                else if (Peek().Type == TokenType.Asc) { Advance(); }
                orderBy.Add(new OrderByClause { Expression = expr, Descending = desc });
            } while (TryConsume(TokenType.Comma));
        }

        int? limit = null;
        if (Peek().Type == TokenType.Limit)
        {
            Advance();
            var limitToken = Expect(TokenType.IntegerLiteral);
            limit = int.Parse(limitToken.Value, CultureInfo.InvariantCulture);
        }

        int? offset = null;
        if (Peek().Type == TokenType.Offset)
        {
            Advance();
            var offsetToken = Expect(TokenType.IntegerLiteral);
            offset = int.Parse(offsetToken.Value, CultureInfo.InvariantCulture);
        }

        return new SelectStatement
        {
            Columns = columns,
            From = from,
            Where = where,
            GroupBy = groupBy,
            Having = having,
            OrderBy = orderBy,
            Limit = limit,
            Offset = offset,
        };
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
            var query = ParseSelect();
            Expect(TokenType.RightParen);

            ctes.Add(new CteDefinition { Name = cteName, ColumnNames = columnNames, Query = query });
        } while (TryConsume(TokenType.Comma));

        // The main query after the CTEs
        var mainQuery = ParseSelect();

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
        string tableName = ExpectIdentifier();
        while (TryConsume(TokenType.Dot))
        {
            tableName = $"{tableName}.{ExpectIdentifier()}";
        }
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

    private Expression ParsePrimary()
    {
        var token = Peek();

        // Aggregate function call: COUNT/SUM/AVG/MIN/MAX followed by '('
        if (IsAggregateFunctionToken(token.Type) && _pos + 1 < _tokens.Count && _tokens[_pos + 1].Type == TokenType.LeftParen)
        {
            string funcName = token.Value.ToUpperInvariant();
            Advance(); // consume function name
            Expect(TokenType.LeftParen);

            bool isStar = false;
            bool isDistinct = false;
            var args = new List<Expression>();

            if (Peek().Type == TokenType.Star)
            {
                Advance();
                isStar = true;
            }
            else
            {
                if (Peek().Type == TokenType.Distinct)
                {
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
            case TokenType.LeftParen:
                Advance();
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
