namespace CSharpDB.Sql;

// ============ Statements ============

public abstract class Statement { }

public sealed class CreateTableStatement : Statement
{
    public required string TableName { get; init; }
    public required List<ColumnDef> Columns { get; init; }
    public bool IfNotExists { get; init; }
}

public sealed class ColumnDef
{
    public required string Name { get; init; }
    public required TokenType TypeToken { get; init; } // Integer, Real, Text, Blob
    public bool IsPrimaryKey { get; init; }
    public bool IsNullable { get; init; } = true;
}

public sealed class DropTableStatement : Statement
{
    public required string TableName { get; init; }
    public bool IfExists { get; init; }
}

public sealed class InsertStatement : Statement
{
    public required string TableName { get; init; }
    public List<string>? ColumnNames { get; init; }
    public required List<List<Expression>> ValueRows { get; init; }
}

public sealed class SelectStatement : Statement
{
    public required List<SelectColumn> Columns { get; init; }
    public required TableRef From { get; init; }
    public Expression? Where { get; init; }
    public List<Expression>? GroupBy { get; init; }
    public Expression? Having { get; init; }
    public List<OrderByClause>? OrderBy { get; init; }
    public int? Limit { get; init; }
    public int? Offset { get; init; }
}

// ============ Table References (FROM clause) ============

public abstract class TableRef { }

public sealed class SimpleTableRef : TableRef
{
    public required string TableName { get; init; }
    public string? Alias { get; init; }
}

public sealed class JoinTableRef : TableRef
{
    public required TableRef Left { get; init; }
    public required TableRef Right { get; init; }
    public required JoinType JoinType { get; init; }
    public Expression? Condition { get; init; } // ON condition (null for CROSS JOIN)
}

public enum JoinType
{
    Inner,
    LeftOuter,
    RightOuter,
    Cross,
}

public sealed class SelectColumn
{
    public bool IsStar { get; init; }
    public Expression? Expression { get; init; }
    public string? Alias { get; init; }
}

public sealed class OrderByClause
{
    public required Expression Expression { get; init; }
    public bool Descending { get; init; }
}

public sealed class DeleteStatement : Statement
{
    public required string TableName { get; init; }
    public Expression? Where { get; init; }
}

public sealed class UpdateStatement : Statement
{
    public required string TableName { get; init; }
    public required List<SetClause> SetClauses { get; init; }
    public Expression? Where { get; init; }
}

public sealed class SetClause
{
    public required string ColumnName { get; init; }
    public required Expression Value { get; init; }
}

// ============ ALTER TABLE ============

public sealed class AlterTableStatement : Statement
{
    public required string TableName { get; init; }
    public required AlterAction Action { get; init; }
}

public abstract class AlterAction { }

public sealed class AddColumnAction : AlterAction
{
    public required ColumnDef Column { get; init; }
}

public sealed class DropColumnAction : AlterAction
{
    public required string ColumnName { get; init; }
}

public sealed class RenameTableAction : AlterAction
{
    public required string NewTableName { get; init; }
}

public sealed class RenameColumnAction : AlterAction
{
    public required string OldColumnName { get; init; }
    public required string NewColumnName { get; init; }
}

// ============ CREATE INDEX / DROP INDEX ============

public sealed class CreateIndexStatement : Statement
{
    public required string IndexName { get; init; }
    public required string TableName { get; init; }
    public required List<string> Columns { get; init; }
    public bool IsUnique { get; init; }
    public bool IfNotExists { get; init; }
}

public sealed class DropIndexStatement : Statement
{
    public required string IndexName { get; init; }
    public bool IfExists { get; init; }
}

// ============ CREATE VIEW / DROP VIEW ============

public sealed class CreateViewStatement : Statement
{
    public required string ViewName { get; init; }
    public required SelectStatement Query { get; init; }
    public bool IfNotExists { get; init; }
}

public sealed class DropViewStatement : Statement
{
    public required string ViewName { get; init; }
    public bool IfExists { get; init; }
}

// ============ Triggers ============

public sealed class CreateTriggerStatement : Statement
{
    public required string TriggerName { get; init; }
    public required string TableName { get; init; }
    public required CSharpDB.Core.TriggerTiming Timing { get; init; }
    public required CSharpDB.Core.TriggerEvent Event { get; init; }
    public Expression? WhenCondition { get; init; }
    public required List<Statement> Body { get; init; }
    public bool IfNotExists { get; init; }
}

public sealed class DropTriggerStatement : Statement
{
    public required string TriggerName { get; init; }
    public bool IfExists { get; init; }
}

// ============ Common Table Expressions (CTEs) ============

public sealed class CteDefinition
{
    public required string Name { get; init; }
    public List<string>? ColumnNames { get; init; }
    public required SelectStatement Query { get; init; }
}

public sealed class WithStatement : Statement
{
    public required List<CteDefinition> Ctes { get; init; }
    public required SelectStatement MainQuery { get; init; }
}

// ============ Expressions ============

public abstract class Expression { }

public sealed class LiteralExpression : Expression
{
    public required object? Value { get; init; } // long, double, string, null, or byte[]
    public required TokenType LiteralType { get; init; }
}

public sealed class ParameterExpression : Expression
{
    public required string Name { get; init; }
}

public sealed class ColumnRefExpression : Expression
{
    public string? TableAlias { get; init; }
    public required string ColumnName { get; init; }
}

public enum BinaryOp
{
    Equals, NotEquals, LessThan, GreaterThan, LessOrEqual, GreaterOrEqual,
    And, Or,
    Plus, Minus, Multiply, Divide,
}

public sealed class BinaryExpression : Expression
{
    public required BinaryOp Op { get; init; }
    public required Expression Left { get; init; }
    public required Expression Right { get; init; }
}

public sealed class UnaryExpression : Expression
{
    public required TokenType Op { get; init; } // Not, Minus
    public required Expression Operand { get; init; }
}

public sealed class LikeExpression : Expression
{
    public required Expression Operand { get; init; }
    public required Expression Pattern { get; init; }
    public Expression? EscapeChar { get; init; }
    public bool Negated { get; init; }
}

public sealed class InExpression : Expression
{
    public required Expression Operand { get; init; }
    public required List<Expression> Values { get; init; }
    public bool Negated { get; init; }
}

public sealed class BetweenExpression : Expression
{
    public required Expression Operand { get; init; }
    public required Expression Low { get; init; }
    public required Expression High { get; init; }
    public bool Negated { get; init; }
}

public sealed class IsNullExpression : Expression
{
    public required Expression Operand { get; init; }
    public bool Negated { get; init; } // IS NOT NULL
}

public sealed class FunctionCallExpression : Expression
{
    public required string FunctionName { get; init; } // COUNT, SUM, AVG, MIN, MAX
    public required List<Expression> Arguments { get; init; }
    public bool IsDistinct { get; init; }
    public bool IsStarArg { get; init; } // for COUNT(*)
}
