using CSharpDB.Sql;
using CSharpDB.Admin.Models;

namespace CSharpDB.Admin.Services;

public static class SchemaColumnRules
{
    public static string Identifier(string value)
    {
        if (string.IsNullOrWhiteSpace(value)) throw new InvalidOperationException("An object name is required.");
        return (char.IsLetter(value[0]) || value[0] == '_') && value.All(c => char.IsLetterOrDigit(c) || c == '_')
            ? value : "\"" + value.Replace("\"", "\"\"") + "\"";
    }

    public static void ValidateLiteralDefault(string value)
    {
        static bool Literal(Expression expression) => expression switch
        {
            LiteralExpression => true,
            UnaryExpression { Op: TokenType.Minus, Operand: LiteralExpression { LiteralType: TokenType.IntegerLiteral or TokenType.RealLiteral } } => true,
            CastExpression cast => Literal(cast.Operand),
            _ => false
        };
        if (!Literal(Parser.ParseExpressionSql(value))) throw new InvalidOperationException("Defaults must be typed literals or NULL, not functions or expressions.");
    }

    public static string RenderColumn(DataModelColumn column)
    {
        string type = NormalizeType(column.IsRowVersion ? "ROWVERSION" : column.TypeLabel);
        bool rowversion = type == "ROWVERSION";
        if (rowversion && (column.IsPrimaryKey || column.IsIdentity || !string.IsNullOrWhiteSpace(column.DefaultSql) || !string.IsNullOrWhiteSpace(column.Collation) || column.Checks.Count > 0))
            throw new InvalidOperationException("ROWVERSION is generated and cannot have key, identity, default, collation, or check modifiers.");
        if (column.IsIdentity && (!column.IsPrimaryKey || type is not ("INTEGER" or "BIGINT")))
            throw new InvalidOperationException("IDENTITY requires a single INTEGER or BIGINT primary-key column.");
        if (!string.IsNullOrWhiteSpace(column.DefaultSql)) ValidateLiteralDefault(column.DefaultSql);
        if (!string.IsNullOrWhiteSpace(column.Collation))
        {
            var declared = ((CreateTableStatement)Parser.Parse($"CREATE TABLE probe (value {type});")).Columns[0].DeclaredType;
            CSharpDB.Execution.QueryPlanner.ValidateColumnCollation(column.Name, declared, column.Collation);
        }
        string sql = Identifier(column.Name) + " " + type;
        if (column.IsPrimaryKey) sql += " PRIMARY KEY";
        if (column.IsIdentity) sql += " IDENTITY";
        if (!column.Nullable && !column.IsPrimaryKey && !rowversion) sql += " NOT NULL";
        if (!string.IsNullOrWhiteSpace(column.Collation)) sql += " COLLATE " + Identifier(column.Collation);
        if (!string.IsNullOrWhiteSpace(column.DefaultSql)) sql += " DEFAULT " + column.DefaultSql;
        foreach (var check in column.Checks) sql += $" CONSTRAINT {Identifier(check.ConstraintName ?? "")} CHECK ({check.ExpressionSql})";
        var parsed = Parser.Parse($"CREATE TABLE probe ({sql});");
        if (parsed is not CreateTableStatement { Columns.Count: 1 }) throw new InvalidOperationException("Invalid column definition.");
        return sql;
    }

    public static string RenderCreateTable(string table, IReadOnlyList<DataModelColumn> columns)
    {
        if (columns.Count == 0) throw new InvalidOperationException("Add at least one column.");
        if (columns.Select(column => column.Name).Distinct(StringComparer.OrdinalIgnoreCase).Count() != columns.Count)
            throw new InvalidOperationException("Column names must be unique.");
        if (columns.Count(column => column.IsPrimaryKey) > 1)
            throw new InvalidOperationException("Use one ordered table-level primary key for a composite key, not multiple column primary keys.");
        string sql = $"CREATE TABLE {Identifier(table)} (\n{string.Join(",\n", columns.Select(column => "    " + RenderColumn(column)))}\n);";
        _ = Parser.Parse(sql);
        return sql;
    }

    public static string NormalizeType(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new InvalidOperationException("A column SQL type is required.");

        try
        {
            Statement statement = Parser.Parse(
                $"CREATE TABLE __csharpdb_type_probe (__value {value.Trim()});");
            if (statement is not CreateTableStatement { Columns.Count: 1 } create ||
                create.CheckConstraints.Count != 0 ||
                create.KeyConstraints.Count != 0 ||
                create.ForeignKeys.Count != 0)
            {
                throw new InvalidOperationException($"Invalid column SQL type '{value}'.");
            }

            ColumnDef column = create.Columns[0];
            if (column.IsRowVersion &&
                !column.IsPrimaryKey &&
                !column.IsIdentity &&
                column.Collation is null &&
                column.ForeignKey is null &&
                column.DefaultExpression is null &&
                column.CheckConstraints.Count == 0)
            {
                return "ROWVERSION";
            }

            if (column.IsPrimaryKey || column.IsIdentity || column.IsRowVersion ||
                !column.IsNullable || column.Collation is not null ||
                column.ForeignKey is not null || column.DefaultExpression is not null ||
                column.CheckConstraints.Count != 0)
            {
                throw new InvalidOperationException(
                    $"Column type '{value}' contains unsupported column modifiers.");
            }

            return column.DeclaredType.ToSql();
        }
        catch (CSharpDB.Primitives.CSharpDbException error)
        {
            throw new InvalidOperationException($"Invalid column SQL type '{value}'.", error);
        }
    }
}
