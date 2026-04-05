using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Tests;

public class ParserTests
{
    [Fact]
    public void Parse_CreateTable()
    {
        var stmt = Parser.Parse("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)");
        var create = Assert.IsType<CreateTableStatement>(stmt);
        Assert.Equal("users", create.TableName);
        Assert.Equal(3, create.Columns.Count);
        Assert.True(create.Columns[0].IsPrimaryKey);
        Assert.False(create.Columns[1].IsNullable);
        Assert.True(create.Columns[2].IsNullable);
    }

    [Theory]
    [InlineData("CREATE TABLE users (id INTEGER PRIMARY KEY IDENTITY, name TEXT)")]
    [InlineData("CREATE TABLE users (id INTEGER AUTOINCREMENT PRIMARY KEY, name TEXT)")]
    [InlineData("CREATE TABLE users (id INTEGER IDENTITY PRIMARY KEY, name TEXT)")]
    public void Parse_CreateTable_IdentityModifiers(string sql)
    {
        var stmt = Parser.Parse(sql);
        var create = Assert.IsType<CreateTableStatement>(stmt);

        Assert.True(create.Columns[0].IsPrimaryKey);
        Assert.True(create.Columns[0].IsIdentity);
        Assert.False(create.Columns[0].IsNullable);
    }

    [Fact]
    public void Parse_CreateTable_IdentityWithoutPrimaryKey_ImplicitPrimaryKey()
    {
        var stmt = Parser.Parse("CREATE TABLE users (id INTEGER IDENTITY, name TEXT)");
        var create = Assert.IsType<CreateTableStatement>(stmt);

        Assert.True(create.Columns[0].IsPrimaryKey);
        Assert.True(create.Columns[0].IsIdentity);
        Assert.False(create.Columns[0].IsNullable);
    }

    [Fact]
    public void Parse_CreateTable_IdentityOnNonInteger_Throws()
    {
        Assert.Throws<CSharpDB.Primitives.CSharpDbException>(() => Parser.Parse("CREATE TABLE users (id TEXT PRIMARY KEY IDENTITY, name TEXT)"));
    }

    [Fact]
    public void Parse_CreateTable_WithColumnCollation()
    {
        var stmt = Parser.Parse("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT COLLATE nocase NOT NULL)");
        var create = Assert.IsType<CreateTableStatement>(stmt);

        Assert.Null(create.Columns[0].Collation);
        Assert.Equal("NOCASE", create.Columns[1].Collation);
        Assert.False(create.Columns[1].IsNullable);
    }

    [Fact]
    public void Parse_CreateTable_WithIcuColumnCollation()
    {
        var stmt = Parser.Parse("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT COLLATE ICU:tr-TR NOT NULL)");
        var create = Assert.IsType<CreateTableStatement>(stmt);

        Assert.Equal("ICU:tr-TR", create.Columns[1].Collation);
    }

    [Fact]
    public void Parse_CreateTable_WithForeignKeyClause()
    {
        var stmt = Parser.Parse(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER REFERENCES customers(id) ON DELETE CASCADE)");
        var create = Assert.IsType<CreateTableStatement>(stmt);
        Assert.NotNull(create.Columns[1].ForeignKey);
        var foreignKey = create.Columns[1].ForeignKey!;

        Assert.Equal("customers", foreignKey.ReferencedTableName);
        Assert.Equal("id", foreignKey.ReferencedColumnName);
        Assert.Equal(ForeignKeyOnDeleteAction.Cascade, foreignKey.OnDelete);
    }

    [Fact]
    public void Parse_CreateTable_TableLevelForeignKey_Throws()
    {
        var error = Assert.Throws<CSharpDB.Primitives.CSharpDbException>(
            () => Parser.Parse("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, FOREIGN KEY (customer_id) REFERENCES customers(id))"));

        Assert.Equal(ErrorCode.SyntaxError, error.Code);
        Assert.Contains("Table-level FOREIGN KEY constraints are not supported", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Parse_CreateTable_OnlyOnDeleteCascadeIsSupported()
    {
        var error = Assert.Throws<CSharpDB.Primitives.CSharpDbException>(
            () => Parser.Parse("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER REFERENCES customers(id) ON UPDATE CASCADE)"));

        Assert.Equal(ErrorCode.SyntaxError, error.Code);
        Assert.Contains("Only ON DELETE is supported", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Parse_Insert()
    {
        var stmt = Parser.Parse("INSERT INTO users (name, age) VALUES ('Alice', 30)");
        var insert = Assert.IsType<InsertStatement>(stmt);
        Assert.Equal("users", insert.TableName);
        Assert.Equal(2, insert.ColumnNames!.Count);
        Assert.Single(insert.ValueRows);
    }

    [Fact]
    public void Parse_InsertWithoutColumns()
    {
        var stmt = Parser.Parse("INSERT INTO users VALUES (1, 'Bob', 25)");
        var insert = Assert.IsType<InsertStatement>(stmt);
        Assert.Null(insert.ColumnNames);
        Assert.Single(insert.ValueRows);
        Assert.Equal(3, insert.ValueRows[0].Count);
    }

    [Fact]
    public void Parse_Select_Star()
    {
        var stmt = Parser.Parse("SELECT * FROM users WHERE age > 18 LIMIT 10");
        var select = Assert.IsType<SelectStatement>(stmt);
        Assert.True(select.Columns[0].IsStar);
        var from = Assert.IsType<SimpleTableRef>(select.From);
        Assert.Equal("users", from.TableName);
        Assert.NotNull(select.Where);
        Assert.Equal(10, select.Limit);
    }

    [Fact]
    public void Parse_SelectDistinct()
    {
        var stmt = Parser.Parse("SELECT DISTINCT name, age FROM users");
        var select = Assert.IsType<SelectStatement>(stmt);
        Assert.True(select.IsDistinct);
        Assert.Equal(2, select.Columns.Count);
    }

    [Fact]
    public void Parse_Select_WithOrderBy()
    {
        var stmt = Parser.Parse("SELECT name, age FROM users ORDER BY age DESC, name ASC");
        var select = Assert.IsType<SelectStatement>(stmt);
        Assert.Equal(2, select.Columns.Count);
        Assert.Equal(2, select.OrderBy!.Count);
        Assert.True(select.OrderBy[0].Descending);
        Assert.False(select.OrderBy[1].Descending);
    }

    [Fact]
    public void Parse_Select_WithExplicitComparisonCollation()
    {
        var stmt = Parser.Parse("SELECT * FROM users WHERE name COLLATE nocase = 'alice'");
        var select = Assert.IsType<SelectStatement>(stmt);
        var where = Assert.IsType<BinaryExpression>(select.Where);
        var collate = Assert.IsType<CollateExpression>(where.Left);

        Assert.Equal("NOCASE", collate.Collation);
        Assert.IsType<ColumnRefExpression>(collate.Operand);
    }

    [Fact]
    public void Parse_Select_WithExplicitIcuComparisonCollation()
    {
        var stmt = Parser.Parse("SELECT * FROM users WHERE name COLLATE ICU:sv-SE = 'z'");
        var select = Assert.IsType<SelectStatement>(stmt);
        var where = Assert.IsType<BinaryExpression>(select.Where);
        var collate = Assert.IsType<CollateExpression>(where.Left);

        Assert.Equal("ICU:sv-SE", collate.Collation);
    }

    [Fact]
    public void Parse_Select_WithExplicitOrderByCollation()
    {
        var stmt = Parser.Parse("SELECT name FROM users ORDER BY name COLLATE nocase DESC");
        var select = Assert.IsType<SelectStatement>(stmt);
        var collate = Assert.IsType<CollateExpression>(select.OrderBy![0].Expression);

        Assert.Equal("NOCASE", collate.Collation);
        Assert.True(select.OrderBy[0].Descending);
        Assert.IsType<ColumnRefExpression>(collate.Operand);
    }

    [Fact]
    public void TryParseSimpleSelect_ParsesSimpleLookup()
    {
        bool parsed = Parser.TryParseSimpleSelect(
            "SELECT id FROM users WHERE id = 42",
            out var stmt);

        Assert.True(parsed);
        var select = Assert.IsType<SelectStatement>(stmt);
        Assert.Single(select.Columns);
        Assert.Equal("users", Assert.IsType<SimpleTableRef>(select.From).TableName);
        var where = Assert.IsType<BinaryExpression>(select.Where);
        Assert.Equal(BinaryOp.Equals, where.Op);
    }

    [Fact]
    public void TryParseSimpleSelect_ParsesMultipartTableName()
    {
        bool parsed = Parser.TryParseSimpleSelect(
            "SELECT table_name FROM sys.tables WHERE table_name = 'users'",
            out var stmt);

        Assert.True(parsed);
        var select = Assert.IsType<SelectStatement>(stmt);
        Assert.Equal("sys.tables", Assert.IsType<SimpleTableRef>(select.From).TableName);
    }

    [Fact]
    public void TryParseSimpleSelect_ParsesConjunctiveEqualityWithEscapedString()
    {
        bool parsed = Parser.TryParseSimpleSelect(
            "SELECT id FROM users WHERE id = 42 AND name = 'O''Reilly'",
            out var stmt);

        Assert.True(parsed);
        var select = Assert.IsType<SelectStatement>(stmt);
        var andExpr = Assert.IsType<BinaryExpression>(select.Where);
        Assert.Equal(BinaryOp.And, andExpr.Op);
        var rhs = Assert.IsType<BinaryExpression>(andExpr.Right);
        var rhsLiteral = Assert.IsType<LiteralExpression>(rhs.Right);
        Assert.Equal(TokenType.StringLiteral, rhsLiteral.LiteralType);
        Assert.Equal("O'Reilly", rhsLiteral.Value);
    }

    [Fact]
    public void TryParseSimpleSelect_RejectsUnsupportedClauses()
    {
        bool parsed = Parser.TryParseSimpleSelect(
            "SELECT id FROM users WHERE id = 1 ORDER BY id",
            out _);

        Assert.False(parsed);
    }

    [Fact]
    public void TryParseSimplePrimaryKeyLookup_ParsesTextPredicateLiteral()
    {
        bool parsed = Parser.TryParseSimplePrimaryKeyLookup(
            "SELECT name FROM users WHERE name = 'Alice'",
            out var lookup);

        Assert.True(parsed);
        Assert.Equal("users", lookup.TableName);
        Assert.False(lookup.SelectStar);
        Assert.Equal("name", lookup.ProjectionColumn);
        Assert.Equal("name", lookup.PredicateColumn);
        Assert.Equal(DbType.Text, lookup.PredicateLiteral.Type);
        Assert.Equal("Alice", lookup.PredicateLiteral.AsText);
    }

    [Fact]
    public void TryParseSimplePrimaryKeyLookup_ParsesMultipleProjectionColumns()
    {
        bool parsed = Parser.TryParseSimplePrimaryKeyLookup(
            "SELECT id, name FROM users WHERE id = 42",
            out var lookup);

        Assert.True(parsed);
        Assert.False(lookup.SelectStar);
        Assert.Equal(["id", "name"], lookup.ProjectionColumns);
        Assert.Equal("id", lookup.PredicateColumn);
        Assert.Equal(42, lookup.LookupValue);
    }

    [Fact]
    public void TryParseSimpleInsert_ParsesLiteralValues()
    {
        bool parsed = Parser.TryParseSimpleInsert(
            "INSERT INTO sys.tables VALUES (1, 'O''Reilly', NULL, -2.5);",
            out var insert);

        Assert.True(parsed);
        Assert.Equal("sys.tables", insert.TableName);
        Assert.Equal(4, insert.Values.Length);
        Assert.Equal(1, insert.Values[0].AsInteger);
        Assert.Equal("O'Reilly", insert.Values[1].AsText);
        Assert.True(insert.Values[2].IsNull);
        Assert.Equal(-2.5, insert.Values[3].AsReal);
    }

    [Fact]
    public void TryParseSimpleInsert_ParsesMultipleRows()
    {
        bool parsed = Parser.TryParseSimpleInsert(
            "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')",
            out var insert);

        Assert.True(parsed);
        Assert.Equal("users", insert.TableName);
        Assert.Equal(2, insert.RowCount);
        Assert.Equal(2, insert.ValueRows.Length);
        Assert.Equal(1, insert.ValueRows[0][0].AsInteger);
        Assert.Equal("Alice", insert.ValueRows[0][1].AsText);
        Assert.Equal(2, insert.ValueRows[1][0].AsInteger);
        Assert.Equal("Bob", insert.ValueRows[1][1].AsText);
    }

    [Fact]
    public void TryParseSimpleInsert_RejectsColumnList()
    {
        bool parsed = Parser.TryParseSimpleInsert(
            "INSERT INTO users (name, age) VALUES ('Alice', 30)",
            out _);

        Assert.False(parsed);
    }

    [Fact]
    public void Parse_Delete()
    {
        var stmt = Parser.Parse("DELETE FROM users WHERE age < 18");
        var delete = Assert.IsType<DeleteStatement>(stmt);
        Assert.Equal("users", delete.TableName);
        Assert.NotNull(delete.Where);
    }

    [Fact]
    public void Parse_Update()
    {
        var stmt = Parser.Parse("UPDATE users SET name = 'Charlie', age = 35 WHERE id = 1");
        var update = Assert.IsType<UpdateStatement>(stmt);
        Assert.Equal("users", update.TableName);
        Assert.Equal(2, update.SetClauses.Count);
        Assert.NotNull(update.Where);
    }

    [Fact]
    public void Parse_DropTable()
    {
        var stmt = Parser.Parse("DROP TABLE IF EXISTS users");
        var drop = Assert.IsType<DropTableStatement>(stmt);
        Assert.Equal("users", drop.TableName);
        Assert.True(drop.IfExists);
    }

    [Fact]
    public void Parse_AnalyzeAllTables()
    {
        var stmt = Parser.Parse("ANALYZE");
        var analyze = Assert.IsType<AnalyzeStatement>(stmt);
        Assert.Null(analyze.TableName);
    }

    [Fact]
    public void Parse_AnalyzeSingleTable()
    {
        var stmt = Parser.Parse("ANALYZE sys.table_stats");
        var analyze = Assert.IsType<AnalyzeStatement>(stmt);
        Assert.Equal("sys.table_stats", analyze.TableName);
    }

    [Fact]
    public void Parse_ComplexExpression()
    {
        var stmt = Parser.Parse("SELECT * FROM t WHERE (a + b) * c > 10 AND name = 'test'");
        var select = Assert.IsType<SelectStatement>(stmt);
        Assert.IsType<BinaryExpression>(select.Where);
    }

    [Fact]
    public void Parse_ParameterExpression()
    {
        var stmt = Parser.Parse("SELECT * FROM t WHERE id = @id");
        var select = Assert.IsType<SelectStatement>(stmt);
        var where = Assert.IsType<BinaryExpression>(select.Where);
        Assert.IsType<ColumnRefExpression>(where.Left);
        var param = Assert.IsType<ParameterExpression>(where.Right);
        Assert.Equal("id", param.Name);
    }

    [Fact]
    public void Parse_Like()
    {
        var stmt = Parser.Parse("SELECT * FROM t WHERE name LIKE '%foo%'");
        var select = Assert.IsType<SelectStatement>(stmt);
        var like = Assert.IsType<LikeExpression>(select.Where);
        Assert.False(like.Negated);
        Assert.IsType<ColumnRefExpression>(like.Operand);
        Assert.IsType<LiteralExpression>(like.Pattern);
        Assert.Null(like.EscapeChar);
    }

    [Fact]
    public void Parse_NotLike()
    {
        var stmt = Parser.Parse("SELECT * FROM t WHERE name NOT LIKE 'test%'");
        var select = Assert.IsType<SelectStatement>(stmt);
        var like = Assert.IsType<LikeExpression>(select.Where);
        Assert.True(like.Negated);
    }

    [Fact]
    public void Parse_Like_WithEscape()
    {
        var stmt = Parser.Parse("SELECT * FROM t WHERE name LIKE '%!%%' ESCAPE '!'");
        var select = Assert.IsType<SelectStatement>(stmt);
        var like = Assert.IsType<LikeExpression>(select.Where);
        Assert.NotNull(like.EscapeChar);
    }

    [Fact]
    public void Parse_In()
    {
        var stmt = Parser.Parse("SELECT * FROM t WHERE id IN (1, 2, 3)");
        var select = Assert.IsType<SelectStatement>(stmt);
        var inExpr = Assert.IsType<InExpression>(select.Where);
        Assert.False(inExpr.Negated);
        Assert.Equal(3, inExpr.Values.Count);
    }

    [Fact]
    public void Parse_NotIn()
    {
        var stmt = Parser.Parse("SELECT * FROM t WHERE status NOT IN ('active', 'pending')");
        var select = Assert.IsType<SelectStatement>(stmt);
        var inExpr = Assert.IsType<InExpression>(select.Where);
        Assert.True(inExpr.Negated);
        Assert.Equal(2, inExpr.Values.Count);
    }

    [Fact]
    public void Parse_InSubquery()
    {
        var stmt = Parser.Parse("SELECT * FROM t WHERE id IN (SELECT ref_id FROM refs)");
        var select = Assert.IsType<SelectStatement>(stmt);
        var inExpr = Assert.IsType<InSubqueryExpression>(select.Where);
        Assert.False(inExpr.Negated);
        Assert.IsType<SelectStatement>(inExpr.Query);
    }

    [Fact]
    public void Parse_ScalarSubquery()
    {
        var stmt = Parser.Parse("SELECT (SELECT value FROM cfg) AS current_value FROM t");
        var select = Assert.IsType<SelectStatement>(stmt);
        var scalar = Assert.IsType<ScalarSubqueryExpression>(select.Columns[0].Expression);
        Assert.IsType<SelectStatement>(scalar.Query);
        Assert.Equal("current_value", select.Columns[0].Alias);
    }

    [Fact]
    public void Parse_ExistsSubquery()
    {
        var stmt = Parser.Parse("SELECT * FROM t WHERE EXISTS (SELECT id FROM flags)");
        var select = Assert.IsType<SelectStatement>(stmt);
        var exists = Assert.IsType<ExistsExpression>(select.Where);
        Assert.IsType<SelectStatement>(exists.Query);
    }

    [Fact]
    public void Parse_Between()
    {
        var stmt = Parser.Parse("SELECT * FROM t WHERE age BETWEEN 18 AND 65");
        var select = Assert.IsType<SelectStatement>(stmt);
        var bet = Assert.IsType<BetweenExpression>(select.Where);
        Assert.False(bet.Negated);
    }

    [Fact]
    public void Parse_NotBetween()
    {
        var stmt = Parser.Parse("SELECT * FROM t WHERE price NOT BETWEEN 10.0 AND 20.0");
        var select = Assert.IsType<SelectStatement>(stmt);
        var bet = Assert.IsType<BetweenExpression>(select.Where);
        Assert.True(bet.Negated);
    }

    [Fact]
    public void Parse_IsNull()
    {
        var stmt = Parser.Parse("SELECT * FROM t WHERE val IS NULL");
        var select = Assert.IsType<SelectStatement>(stmt);
        var isNull = Assert.IsType<IsNullExpression>(select.Where);
        Assert.False(isNull.Negated);
    }

    [Fact]
    public void Parse_IsNotNull()
    {
        var stmt = Parser.Parse("SELECT * FROM t WHERE val IS NOT NULL");
        var select = Assert.IsType<SelectStatement>(stmt);
        var isNull = Assert.IsType<IsNullExpression>(select.Where);
        Assert.True(isNull.Negated);
    }

    [Fact]
    public void Parse_CountStar()
    {
        var stmt = Parser.Parse("SELECT COUNT(*) FROM t");
        var select = Assert.IsType<SelectStatement>(stmt);
        Assert.Single(select.Columns);
        var func = Assert.IsType<FunctionCallExpression>(select.Columns[0].Expression);
        Assert.Equal("COUNT", func.FunctionName);
        Assert.True(func.IsStarArg);
    }

    [Fact]
    public void Parse_AggregateWithGroupBy()
    {
        var stmt = Parser.Parse("SELECT category, SUM(price) FROM t GROUP BY category");
        var select = Assert.IsType<SelectStatement>(stmt);
        Assert.Equal(2, select.Columns.Count);
        Assert.NotNull(select.GroupBy);
        Assert.Single(select.GroupBy);
        var func = Assert.IsType<FunctionCallExpression>(select.Columns[1].Expression);
        Assert.Equal("SUM", func.FunctionName);
    }

    [Fact]
    public void Parse_GroupByWithHaving()
    {
        var stmt = Parser.Parse("SELECT cat, AVG(val) FROM t GROUP BY cat HAVING AVG(val) > 10");
        var select = Assert.IsType<SelectStatement>(stmt);
        Assert.NotNull(select.GroupBy);
        Assert.NotNull(select.Having);
        var having = Assert.IsType<BinaryExpression>(select.Having);
        Assert.IsType<FunctionCallExpression>(having.Left);
    }

    [Fact]
    public void Parse_CountDistinct()
    {
        var stmt = Parser.Parse("SELECT COUNT(DISTINCT name) FROM t");
        var select = Assert.IsType<SelectStatement>(stmt);
        var func = Assert.IsType<FunctionCallExpression>(select.Columns[0].Expression);
        Assert.Equal("COUNT", func.FunctionName);
        Assert.True(func.IsDistinct);
        Assert.False(func.IsStarArg);
    }

    [Fact]
    public void Parse_ScalarTextFunction()
    {
        var stmt = Parser.Parse("SELECT TEXT(id) AS id_text FROM t WHERE TEXT(id) LIKE '%1%'");
        var select = Assert.IsType<SelectStatement>(stmt);

        var projection = Assert.IsType<FunctionCallExpression>(select.Columns[0].Expression);
        Assert.Equal("TEXT", projection.FunctionName);
        Assert.Single(projection.Arguments);
        Assert.False(projection.IsDistinct);
        Assert.False(projection.IsStarArg);

        var where = Assert.IsType<LikeExpression>(select.Where);
        var predicate = Assert.IsType<FunctionCallExpression>(where.Operand);
        Assert.Equal("TEXT", predicate.FunctionName);
    }

    [Fact]
    public void Parse_ScalarTextAroundAggregate()
    {
        var stmt = Parser.Parse("SELECT TEXT(COUNT(*)) FROM t");
        var select = Assert.IsType<SelectStatement>(stmt);

        var outer = Assert.IsType<FunctionCallExpression>(select.Columns[0].Expression);
        Assert.Equal("TEXT", outer.FunctionName);

        var inner = Assert.IsType<FunctionCallExpression>(Assert.Single(outer.Arguments));
        Assert.Equal("COUNT", inner.FunctionName);
        Assert.True(inner.IsStarArg);
    }

    [Fact]
    public void Parse_AsAlias()
    {
        var stmt = Parser.Parse("SELECT name AS n, COUNT(*) AS cnt FROM t GROUP BY name");
        var select = Assert.IsType<SelectStatement>(stmt);
        Assert.Equal("n", select.Columns[0].Alias);
        Assert.Equal("cnt", select.Columns[1].Alias);
    }

    [Fact]
    public void Parse_MultipleAggregates()
    {
        var stmt = Parser.Parse("SELECT MIN(a), MAX(a), AVG(a), SUM(a), COUNT(a) FROM t");
        var select = Assert.IsType<SelectStatement>(stmt);
        Assert.Equal(5, select.Columns.Count);
        for (int i = 0; i < 5; i++)
            Assert.IsType<FunctionCallExpression>(select.Columns[i].Expression);
    }

    [Fact]
    public void Parse_Union()
    {
        var stmt = Parser.Parse("SELECT id FROM a UNION SELECT id FROM b");
        var compound = Assert.IsType<CompoundSelectStatement>(stmt);
        Assert.Equal(SetOperationKind.Union, compound.Operation);
        Assert.IsType<SelectStatement>(compound.Left);
        Assert.IsType<SelectStatement>(compound.Right);
    }

    [Fact]
    public void Parse_IntersectHasHigherPrecedenceThanUnion()
    {
        var stmt = Parser.Parse("SELECT id FROM a UNION SELECT id FROM b INTERSECT SELECT id FROM c");
        var outer = Assert.IsType<CompoundSelectStatement>(stmt);
        Assert.Equal(SetOperationKind.Union, outer.Operation);
        Assert.IsType<SelectStatement>(outer.Left);

        var right = Assert.IsType<CompoundSelectStatement>(outer.Right);
        Assert.Equal(SetOperationKind.Intersect, right.Operation);
    }

    [Fact]
    public void Parse_CompoundSelect_WithOuterOrderByLimitOffset()
    {
        var stmt = Parser.Parse("SELECT id FROM a UNION SELECT id FROM b ORDER BY id DESC LIMIT 5 OFFSET 2");
        var compound = Assert.IsType<CompoundSelectStatement>(stmt);
        Assert.NotNull(compound.OrderBy);
        Assert.Single(compound.OrderBy);
        Assert.True(compound.OrderBy[0].Descending);
        Assert.Equal(5, compound.Limit);
        Assert.Equal(2, compound.Offset);
    }

    [Fact]
    public void Parse_CreateView_WithUnion()
    {
        var stmt = Parser.Parse("CREATE VIEW v AS SELECT id FROM a UNION SELECT id FROM b");
        var create = Assert.IsType<CreateViewStatement>(stmt);
        var query = Assert.IsType<CompoundSelectStatement>(create.Query);
        Assert.Equal(SetOperationKind.Union, query.Operation);
    }

    #region JOIN Parsing

    [Fact]
    public void Parse_InnerJoin()
    {
        var stmt = Parser.Parse("SELECT * FROM a JOIN b ON a.id = b.aid");
        var select = Assert.IsType<SelectStatement>(stmt);
        var join = Assert.IsType<JoinTableRef>(select.From);
        Assert.Equal(JoinType.Inner, join.JoinType);
        var left = Assert.IsType<SimpleTableRef>(join.Left);
        var right = Assert.IsType<SimpleTableRef>(join.Right);
        Assert.Equal("a", left.TableName);
        Assert.Equal("b", right.TableName);
        Assert.NotNull(join.Condition);
    }

    [Fact]
    public void Parse_ExplicitInnerJoin()
    {
        var stmt = Parser.Parse("SELECT * FROM a INNER JOIN b ON a.id = b.aid");
        var select = Assert.IsType<SelectStatement>(stmt);
        var join = Assert.IsType<JoinTableRef>(select.From);
        Assert.Equal(JoinType.Inner, join.JoinType);
    }

    [Fact]
    public void Parse_LeftJoin()
    {
        var stmt = Parser.Parse("SELECT * FROM a LEFT JOIN b ON a.id = b.aid");
        var select = Assert.IsType<SelectStatement>(stmt);
        var join = Assert.IsType<JoinTableRef>(select.From);
        Assert.Equal(JoinType.LeftOuter, join.JoinType);
    }

    [Fact]
    public void Parse_LeftOuterJoin()
    {
        var stmt = Parser.Parse("SELECT * FROM a LEFT OUTER JOIN b ON a.id = b.aid");
        var select = Assert.IsType<SelectStatement>(stmt);
        var join = Assert.IsType<JoinTableRef>(select.From);
        Assert.Equal(JoinType.LeftOuter, join.JoinType);
    }

    [Fact]
    public void Parse_RightJoin()
    {
        var stmt = Parser.Parse("SELECT * FROM a RIGHT JOIN b ON a.id = b.aid");
        var select = Assert.IsType<SelectStatement>(stmt);
        var join = Assert.IsType<JoinTableRef>(select.From);
        Assert.Equal(JoinType.RightOuter, join.JoinType);
    }

    [Fact]
    public void Parse_CrossJoin()
    {
        var stmt = Parser.Parse("SELECT * FROM a CROSS JOIN b");
        var select = Assert.IsType<SelectStatement>(stmt);
        var join = Assert.IsType<JoinTableRef>(select.From);
        Assert.Equal(JoinType.Cross, join.JoinType);
        Assert.Null(join.Condition);
    }

    [Fact]
    public void Parse_QualifiedColumnRef()
    {
        var stmt = Parser.Parse("SELECT t1.name FROM users t1");
        var select = Assert.IsType<SelectStatement>(stmt);
        var colRef = Assert.IsType<ColumnRefExpression>(select.Columns[0].Expression);
        Assert.Equal("t1", colRef.TableAlias);
        Assert.Equal("name", colRef.ColumnName);
    }

    [Fact]
    public void Parse_TableAlias()
    {
        var stmt = Parser.Parse("SELECT * FROM users u");
        var select = Assert.IsType<SelectStatement>(stmt);
        var simple = Assert.IsType<SimpleTableRef>(select.From);
        Assert.Equal("users", simple.TableName);
        Assert.Equal("u", simple.Alias);
    }

    [Fact]
    public void Parse_TableAliasWithAs()
    {
        var stmt = Parser.Parse("SELECT * FROM users AS u");
        var select = Assert.IsType<SelectStatement>(stmt);
        var simple = Assert.IsType<SimpleTableRef>(select.From);
        Assert.Equal("users", simple.TableName);
        Assert.Equal("u", simple.Alias);
    }

    [Fact]
    public void Parse_MultipartTableName()
    {
        var stmt = Parser.Parse("SELECT * FROM sys.tables");
        var select = Assert.IsType<SelectStatement>(stmt);
        var simple = Assert.IsType<SimpleTableRef>(select.From);
        Assert.Equal("sys.tables", simple.TableName);
    }

    [Fact]
    public void Parse_MultiWayJoin()
    {
        var stmt = Parser.Parse("SELECT * FROM a JOIN b ON a.id = b.aid JOIN c ON b.id = c.bid");
        var select = Assert.IsType<SelectStatement>(stmt);
        // Structure: JoinTableRef(JoinTableRef(a, b), c)
        var outerJoin = Assert.IsType<JoinTableRef>(select.From);
        var innerJoin = Assert.IsType<JoinTableRef>(outerJoin.Left);
        var c = Assert.IsType<SimpleTableRef>(outerJoin.Right);
        Assert.Equal("c", c.TableName);
        var a = Assert.IsType<SimpleTableRef>(innerJoin.Left);
        var b = Assert.IsType<SimpleTableRef>(innerJoin.Right);
        Assert.Equal("a", a.TableName);
        Assert.Equal("b", b.TableName);
    }

    [Fact]
    public void Parse_JoinWithWhere()
    {
        var stmt = Parser.Parse("SELECT * FROM a JOIN b ON a.id = b.aid WHERE a.id > 5");
        var select = Assert.IsType<SelectStatement>(stmt);
        Assert.IsType<JoinTableRef>(select.From);
        Assert.NotNull(select.Where);
    }

    [Fact]
    public void Parse_JoinWithoutOn_Throws()
    {
        var error = Assert.Throws<CSharpDbException>(() => Parser.Parse("SELECT * FROM a JOIN b a.id = b.aid"));
        Assert.Contains("Expected ON clause after JOIN target", error.Message, StringComparison.Ordinal);
    }

    #endregion

    #region ALTER TABLE Parsing

    [Fact]
    public void Parse_AlterTable_AddColumn()
    {
        var stmt = Parser.Parse("ALTER TABLE users ADD COLUMN email TEXT");
        var alter = Assert.IsType<AlterTableStatement>(stmt);
        Assert.Equal("users", alter.TableName);
        var add = Assert.IsType<AddColumnAction>(alter.Action);
        Assert.Equal("email", add.Column.Name);
        Assert.Equal(TokenType.Text, add.Column.TypeToken);
        Assert.True(add.Column.IsNullable);
    }

    [Fact]
    public void Parse_AlterTable_AddColumnWithoutColumnKeyword()
    {
        var stmt = Parser.Parse("ALTER TABLE users ADD email TEXT NOT NULL");
        var alter = Assert.IsType<AlterTableStatement>(stmt);
        var add = Assert.IsType<AddColumnAction>(alter.Action);
        Assert.Equal("email", add.Column.Name);
        Assert.False(add.Column.IsNullable);
    }

    [Fact]
    public void Parse_AlterTable_AddColumn_WithCollation()
    {
        var stmt = Parser.Parse("ALTER TABLE users ADD COLUMN email TEXT COLLATE nocase");
        var alter = Assert.IsType<AlterTableStatement>(stmt);
        var add = Assert.IsType<AddColumnAction>(alter.Action);

        Assert.Equal("email", add.Column.Name);
        Assert.Equal(TokenType.Text, add.Column.TypeToken);
        Assert.Equal("NOCASE", add.Column.Collation);
    }

    [Fact]
    public void Parse_AlterTable_AddColumn_WithForeignKeyClause()
    {
        var stmt = Parser.Parse("ALTER TABLE orders ADD COLUMN customer_id INTEGER REFERENCES customers(id) ON DELETE CASCADE");
        var alter = Assert.IsType<AlterTableStatement>(stmt);
        var add = Assert.IsType<AddColumnAction>(alter.Action);
        Assert.NotNull(add.Column.ForeignKey);
        var foreignKey = add.Column.ForeignKey!;

        Assert.Equal("customers", foreignKey.ReferencedTableName);
        Assert.Equal("id", foreignKey.ReferencedColumnName);
        Assert.Equal(ForeignKeyOnDeleteAction.Cascade, foreignKey.OnDelete);
    }

    [Fact]
    public void Parse_AlterTable_DropColumn()
    {
        var stmt = Parser.Parse("ALTER TABLE users DROP COLUMN age");
        var alter = Assert.IsType<AlterTableStatement>(stmt);
        Assert.Equal("users", alter.TableName);
        var drop = Assert.IsType<DropColumnAction>(alter.Action);
        Assert.Equal("age", drop.ColumnName);
    }

    [Fact]
    public void Parse_AlterTable_DropColumnWithoutColumnKeyword()
    {
        var stmt = Parser.Parse("ALTER TABLE users DROP age");
        var alter = Assert.IsType<AlterTableStatement>(stmt);
        var drop = Assert.IsType<DropColumnAction>(alter.Action);
        Assert.Equal("age", drop.ColumnName);
    }

    [Fact]
    public void Parse_AlterTable_DropConstraint()
    {
        var stmt = Parser.Parse("ALTER TABLE users DROP CONSTRAINT fk_users_parent_id_abcd1234");
        var alter = Assert.IsType<AlterTableStatement>(stmt);
        Assert.Equal("users", alter.TableName);
        var drop = Assert.IsType<DropConstraintAction>(alter.Action);
        Assert.Equal("fk_users_parent_id_abcd1234", drop.ConstraintName);
    }

    [Fact]
    public void Parse_AlterTable_RenameTable()
    {
        var stmt = Parser.Parse("ALTER TABLE users RENAME TO people");
        var alter = Assert.IsType<AlterTableStatement>(stmt);
        Assert.Equal("users", alter.TableName);
        var rename = Assert.IsType<RenameTableAction>(alter.Action);
        Assert.Equal("people", rename.NewTableName);
    }

    [Fact]
    public void Parse_AlterTable_RenameColumn()
    {
        var stmt = Parser.Parse("ALTER TABLE users RENAME COLUMN name TO full_name");
        var alter = Assert.IsType<AlterTableStatement>(stmt);
        Assert.Equal("users", alter.TableName);
        var rename = Assert.IsType<RenameColumnAction>(alter.Action);
        Assert.Equal("name", rename.OldColumnName);
        Assert.Equal("full_name", rename.NewColumnName);
    }

    [Fact]
    public void Parse_AlterTable_RenameColumnWithoutColumnKeyword()
    {
        var stmt = Parser.Parse("ALTER TABLE users RENAME name TO full_name");
        var alter = Assert.IsType<AlterTableStatement>(stmt);
        var rename = Assert.IsType<RenameColumnAction>(alter.Action);
        Assert.Equal("name", rename.OldColumnName);
        Assert.Equal("full_name", rename.NewColumnName);
    }

    #endregion

    #region INDEX Parsing

    [Fact]
    public void Parse_CreateIndex()
    {
        var stmt = Parser.Parse("CREATE INDEX idx_age ON users (age)");
        var create = Assert.IsType<CreateIndexStatement>(stmt);
        Assert.Equal("idx_age", create.IndexName);
        Assert.Equal("users", create.TableName);
        Assert.Single(create.Columns);
        Assert.Equal("age", create.Columns[0]);
        Assert.False(create.IsUnique);
        Assert.False(create.IfNotExists);
    }

    [Fact]
    public void Parse_CreateIndex_MultiColumn()
    {
        var stmt = Parser.Parse("CREATE INDEX idx_ab ON users (a, b)");
        var create = Assert.IsType<CreateIndexStatement>(stmt);
        Assert.Equal("idx_ab", create.IndexName);
        Assert.Equal("users", create.TableName);
        Assert.Equal(["a", "b"], create.Columns);
        Assert.False(create.IsUnique);
    }

    [Fact]
    public void Parse_CreateIndex_WithColumnCollations()
    {
        var stmt = Parser.Parse("CREATE INDEX idx_name_age ON users (name COLLATE nocase, age)");
        var create = Assert.IsType<CreateIndexStatement>(stmt);

        Assert.Equal(["name", "age"], create.Columns);
        Assert.Equal(2, create.ColumnCollations.Count);
        Assert.Equal("NOCASE", create.ColumnCollations[0]);
        Assert.Null(create.ColumnCollations[1]);
    }

    [Fact]
    public void Parse_CreateIndex_WithIcuColumnCollation()
    {
        var stmt = Parser.Parse("CREATE INDEX idx_name ON users (name COLLATE ICU:de-DE)");
        var create = Assert.IsType<CreateIndexStatement>(stmt);

        Assert.Equal(["name"], create.Columns);
        Assert.Equal(["ICU:de-DE"], create.ColumnCollations);
    }

    [Fact]
    public void Parse_CreateUniqueIndex()
    {
        var stmt = Parser.Parse("CREATE UNIQUE INDEX idx_email ON users (email)");
        var create = Assert.IsType<CreateIndexStatement>(stmt);
        Assert.Equal("idx_email", create.IndexName);
        Assert.Equal("users", create.TableName);
        Assert.True(create.IsUnique);
    }

    [Fact]
    public void Parse_CreateIndex_IfNotExists()
    {
        var stmt = Parser.Parse("CREATE INDEX IF NOT EXISTS idx_age ON users (age)");
        var create = Assert.IsType<CreateIndexStatement>(stmt);
        Assert.True(create.IfNotExists);
        Assert.Equal("idx_age", create.IndexName);
    }

    [Fact]
    public void Parse_DropIndex()
    {
        var stmt = Parser.Parse("DROP INDEX idx_age");
        var drop = Assert.IsType<DropIndexStatement>(stmt);
        Assert.Equal("idx_age", drop.IndexName);
        Assert.False(drop.IfExists);
    }

    [Fact]
    public void Parse_DropIndex_IfExists()
    {
        var stmt = Parser.Parse("DROP INDEX IF EXISTS idx_age");
        var drop = Assert.IsType<DropIndexStatement>(stmt);
        Assert.Equal("idx_age", drop.IndexName);
        Assert.True(drop.IfExists);
    }

    #endregion

    #region VIEW Parsing

    [Fact]
    public void Parse_CreateView()
    {
        var stmt = Parser.Parse("CREATE VIEW active_users AS SELECT * FROM users WHERE age > 18");
        var create = Assert.IsType<CreateViewStatement>(stmt);
        Assert.Equal("active_users", create.ViewName);
        Assert.False(create.IfNotExists);
        var query = Assert.IsType<SelectStatement>(create.Query);
        Assert.NotNull(query.Where);
    }

    [Fact]
    public void Parse_CreateView_IfNotExists()
    {
        var stmt = Parser.Parse("CREATE VIEW IF NOT EXISTS v AS SELECT * FROM t");
        var create = Assert.IsType<CreateViewStatement>(stmt);
        Assert.True(create.IfNotExists);
        Assert.Equal("v", create.ViewName);
    }

    [Fact]
    public void Parse_DropView()
    {
        var stmt = Parser.Parse("DROP VIEW active_users");
        var drop = Assert.IsType<DropViewStatement>(stmt);
        Assert.Equal("active_users", drop.ViewName);
        Assert.False(drop.IfExists);
    }

    [Fact]
    public void Parse_DropView_IfExists()
    {
        var stmt = Parser.Parse("DROP VIEW IF EXISTS active_users");
        var drop = Assert.IsType<DropViewStatement>(stmt);
        Assert.True(drop.IfExists);
    }

    #endregion

    #region CTE Parsing

    [Fact]
    public void Parse_SingleCte()
    {
        var stmt = Parser.Parse("WITH cte AS (SELECT id, name FROM users) SELECT * FROM cte");
        var with = Assert.IsType<WithStatement>(stmt);
        Assert.Single(with.Ctes);
        Assert.Equal("cte", with.Ctes[0].Name);
        Assert.Null(with.Ctes[0].ColumnNames);
        Assert.NotNull(with.MainQuery);
    }

    [Fact]
    public void Parse_MultipleCtes()
    {
        var stmt = Parser.Parse(
            "WITH a AS (SELECT id FROM t1), b AS (SELECT id FROM t2) SELECT * FROM a JOIN b ON a.id = b.id");
        var with = Assert.IsType<WithStatement>(stmt);
        Assert.Equal(2, with.Ctes.Count);
        Assert.Equal("a", with.Ctes[0].Name);
        Assert.Equal("b", with.Ctes[1].Name);
    }

    [Fact]
    public void Parse_CteWithExplicitColumns()
    {
        var stmt = Parser.Parse("WITH cte(x, y) AS (SELECT id, name FROM users) SELECT * FROM cte");
        var with = Assert.IsType<WithStatement>(stmt);
        Assert.Single(with.Ctes);
        Assert.NotNull(with.Ctes[0].ColumnNames);
        Assert.Equal(new[] { "x", "y" }, with.Ctes[0].ColumnNames);
    }

    [Fact]
    public void Parse_CteWithWhereInMainQuery()
    {
        var stmt = Parser.Parse("WITH cte AS (SELECT id, val FROM t) SELECT * FROM cte WHERE val > 10");
        var with = Assert.IsType<WithStatement>(stmt);
        var mainQuery = Assert.IsType<SelectStatement>(with.MainQuery);
        Assert.NotNull(mainQuery.Where);
    }

    [Fact]
    public void Parse_CteWithRecursiveKeyword()
    {
        // RECURSIVE is accepted but not functionally implemented — just ensure it parses
        var stmt = Parser.Parse("WITH RECURSIVE cte AS (SELECT id FROM t) SELECT * FROM cte");
        var with = Assert.IsType<WithStatement>(stmt);
        Assert.Single(with.Ctes);
    }

    #endregion

    #region Trigger Parsing

    [Fact]
    public void Parse_CreateTrigger_BeforeInsert()
    {
        var stmt = Parser.Parse(
            "CREATE TRIGGER audit_insert BEFORE INSERT ON users FOR EACH ROW BEGIN INSERT INTO log VALUES (1, 'insert'); END");
        var trig = Assert.IsType<CreateTriggerStatement>(stmt);
        Assert.Equal("audit_insert", trig.TriggerName);
        Assert.Equal("users", trig.TableName);
        Assert.Equal(CSharpDB.Primitives.TriggerTiming.Before, trig.Timing);
        Assert.Equal(CSharpDB.Primitives.TriggerEvent.Insert, trig.Event);
        Assert.Single(trig.Body);
        Assert.False(trig.IfNotExists);
    }

    [Fact]
    public void Parse_CreateTrigger_AfterDelete()
    {
        var stmt = Parser.Parse(
            "CREATE TRIGGER after_del AFTER DELETE ON items BEGIN DELETE FROM log; END");
        var trig = Assert.IsType<CreateTriggerStatement>(stmt);
        Assert.Equal(CSharpDB.Primitives.TriggerTiming.After, trig.Timing);
        Assert.Equal(CSharpDB.Primitives.TriggerEvent.Delete, trig.Event);
    }

    [Fact]
    public void Parse_CreateTrigger_AfterUpdate()
    {
        var stmt = Parser.Parse(
            "CREATE TRIGGER after_upd AFTER UPDATE ON items BEGIN UPDATE log SET val = 1; END");
        var trig = Assert.IsType<CreateTriggerStatement>(stmt);
        Assert.Equal(CSharpDB.Primitives.TriggerTiming.After, trig.Timing);
        Assert.Equal(CSharpDB.Primitives.TriggerEvent.Update, trig.Event);
    }

    [Fact]
    public void Parse_CreateTrigger_IfNotExists()
    {
        var stmt = Parser.Parse(
            "CREATE TRIGGER IF NOT EXISTS my_trig BEFORE INSERT ON t BEGIN INSERT INTO log VALUES (1, 'x'); END");
        var trig = Assert.IsType<CreateTriggerStatement>(stmt);
        Assert.True(trig.IfNotExists);
    }

    [Fact]
    public void Parse_CreateTrigger_MultipleBodyStatements()
    {
        var stmt = Parser.Parse(
            "CREATE TRIGGER multi AFTER INSERT ON t BEGIN INSERT INTO a VALUES (1, 'x'); INSERT INTO b VALUES (2, 'y'); END");
        var trig = Assert.IsType<CreateTriggerStatement>(stmt);
        Assert.Equal(2, trig.Body.Count);
    }

    [Fact]
    public void Parse_DropTrigger()
    {
        var stmt = Parser.Parse("DROP TRIGGER my_trigger");
        var drop = Assert.IsType<DropTriggerStatement>(stmt);
        Assert.Equal("my_trigger", drop.TriggerName);
        Assert.False(drop.IfExists);
    }

    [Fact]
    public void Parse_DropTrigger_IfExists()
    {
        var stmt = Parser.Parse("DROP TRIGGER IF EXISTS my_trigger");
        var drop = Assert.IsType<DropTriggerStatement>(stmt);
        Assert.True(drop.IfExists);
    }

    [Fact]
    public void Parse_CreateTrigger_WithNewOldRefs()
    {
        var stmt = Parser.Parse(
            "CREATE TRIGGER audit AFTER INSERT ON users BEGIN INSERT INTO log (user_id) VALUES (NEW.id); END");
        var trig = Assert.IsType<CreateTriggerStatement>(stmt);
        var insert = Assert.IsType<InsertStatement>(trig.Body[0]);
        // The value expression should be a qualified column ref with TableAlias = "NEW"
        var colRef = Assert.IsType<ColumnRefExpression>(insert.ValueRows[0][0]);
        Assert.Equal("NEW", colRef.TableAlias);
        Assert.Equal("id", colRef.ColumnName);
    }

    #endregion
}
