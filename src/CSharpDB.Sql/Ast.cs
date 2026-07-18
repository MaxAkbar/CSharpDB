namespace CSharpDB.Sql;

// ============ Statements ============

public abstract class Statement { }

public abstract class QueryStatement : Statement { }

public sealed class CreateTableStatement : Statement
{
    public required string TableName { get; init; }
    public required List<ColumnDef> Columns { get; init; }
    public List<CheckConstraintClause> CheckConstraints { get; init; } = [];
    public List<KeyConstraintClause> KeyConstraints { get; init; } = [];
    public List<ForeignKeyConstraintClause> ForeignKeys { get; init; } = [];
    public bool IfNotExists { get; init; }
    public bool IsTemporary { get; init; }
}

public sealed class CreateExternalTableStatement : Statement
{
    public required string TableName { get; init; }
    public required string Path { get; init; }
    public bool IfNotExists { get; init; }
}

public sealed class ForeignKeyClause
{
    public required string ReferencedTableName { get; init; }
    public string? ReferencedColumnName { get; init; }
    public CSharpDB.Primitives.ForeignKeyOnDeleteAction OnDelete { get; init; } =
        CSharpDB.Primitives.ForeignKeyOnDeleteAction.Restrict;
}

public sealed class ForeignKeyConstraintClause
{
    public string? ConstraintName { get; init; }
    public required List<string> Columns { get; init; }
    public required string ReferencedTableName { get; init; }
    public List<string>? ReferencedColumns { get; init; }
    public CSharpDB.Primitives.ForeignKeyOnDeleteAction OnDelete { get; init; } =
        CSharpDB.Primitives.ForeignKeyOnDeleteAction.Restrict;
}

public sealed class ColumnDef
{
    public required string Name { get; init; }
    public required TokenType TypeToken { get; init; } // Integer, Real, Text, Blob
    public bool IsPrimaryKey { get; init; }
    public bool IsIdentity { get; init; }
    public bool IsNullable { get; init; } = true;
    public string? Collation { get; init; }
    public ForeignKeyClause? ForeignKey { get; init; }
    public Expression? DefaultExpression { get; init; }
    public List<CheckConstraintClause> CheckConstraints { get; init; } = [];
}

public sealed class CheckConstraintClause
{
    public string? ConstraintName { get; init; }
    public required Expression Expression { get; init; }
}

public sealed class KeyConstraintClause
{
    public string? ConstraintName { get; init; }
    public CSharpDB.Primitives.KeyConstraintKind Kind { get; init; }
    public required List<string> Columns { get; init; }
}

public sealed class DropTableStatement : Statement
{
    public required string TableName { get; init; }
    public bool IfExists { get; init; }
    public bool IsTemporary { get; init; }
}

public sealed class PersistTempTableStatement : Statement
{
    public required string TempTableName { get; init; }
    public required string TargetTableName { get; init; }
}

public sealed class DropExternalTableStatement : Statement
{
    public required string TableName { get; init; }
    public bool IfExists { get; init; }
}

public sealed class InsertStatement : Statement
{
    public required string TableName { get; init; }
    public List<string>? ColumnNames { get; init; }
    public required List<List<Expression>> ValueRows { get; init; }
    public bool IsDefaultValues { get; init; }
}

public sealed class SelectStatement : QueryStatement
{
    public bool IsDistinct { get; init; }
    public required List<SelectColumn> Columns { get; init; }
    public required TableRef From { get; init; }
    public Expression? Where { get; init; }
    public List<Expression>? GroupBy { get; init; }
    public Expression? Having { get; init; }
    public List<OrderByClause>? OrderBy { get; init; }
    public int? Limit { get; init; }
    public int? Offset { get; init; }
}

public enum SetOperationKind
{
    Union,
    Intersect,
    Except,
}

public enum SetQuantifier
{
    Distinct,
    All,
}

public sealed class CompoundSelectStatement : QueryStatement
{
    public required QueryStatement Left { get; init; }
    public required QueryStatement Right { get; init; }
    public required SetOperationKind Operation { get; init; }
    public SetQuantifier Quantifier { get; init; } = SetQuantifier.Distinct;
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

public sealed class SingleRowTableRef : TableRef
{
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

public sealed class AddCheckConstraintAction : AlterAction
{
    public required string ConstraintName { get; init; }
    public required Expression Expression { get; init; }
}

public sealed class DropColumnAction : AlterAction
{
    public required string ColumnName { get; init; }
}

public sealed class DropConstraintAction : AlterAction
{
    public required string ConstraintName { get; init; }
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

public sealed class AlterColumnSetDefaultAction : AlterAction
{
    public required string ColumnName { get; init; }
    public required Expression DefaultExpression { get; init; }
}

public sealed class AlterColumnDropDefaultAction : AlterAction
{
    public required string ColumnName { get; init; }
}

public sealed class AlterColumnSetNotNullAction : AlterAction
{
    public required string ColumnName { get; init; }
}

public sealed class AlterColumnDropNotNullAction : AlterAction
{
    public required string ColumnName { get; init; }
}

// ============ CREATE INDEX / DROP INDEX ============

public sealed class CreateIndexStatement : Statement
{
    public required string IndexName { get; init; }
    public required string TableName { get; init; }
    public required List<string> Columns { get; init; }
    public List<string?> ColumnCollations { get; init; } = [];
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
    public required QueryStatement Query { get; init; }
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
    public required CSharpDB.Primitives.TriggerTiming Timing { get; init; }
    public required CSharpDB.Primitives.TriggerEvent Event { get; init; }
    public Expression? WhenCondition { get; init; }
    public required List<Statement> Body { get; init; }
    public bool IfNotExists { get; init; }
}

public sealed class DropTriggerStatement : Statement
{
    public required string TriggerName { get; init; }
    public bool IfExists { get; init; }
}

public sealed class AnalyzeStatement : Statement
{
    public string? TableName { get; init; }
}

public sealed class ExplainEstimateStatement : Statement
{
    public required Statement Target { get; init; }
}

// ============ Data Hygiene ============

public enum DuplicateKeepMode
{
    First,
    Last,
}

public sealed class FindDuplicatesStatement : QueryStatement
{
    public required string TableName { get; init; }
    public required List<Expression> KeyExpressions { get; init; }
}

public sealed class DedupStatement : Statement
{
    public required string TableName { get; init; }
    public required List<Expression> KeyExpressions { get; init; }
    public required DuplicateKeepMode KeepMode { get; init; }
}

public sealed class MergeDuplicatesStatement : Statement
{
    public required string TableName { get; init; }
    public required List<Expression> KeyExpressions { get; init; }
}

public sealed class CreateValidationRuleStatement : Statement
{
    public required string RuleName { get; init; }
    public required string TableName { get; init; }
    public string? ColumnName { get; init; }
    public required Expression Expression { get; init; }
    public required string Message { get; init; }
}

public sealed class ValidateTableStatement : QueryStatement
{
    public required string TableName { get; init; }
}

public sealed class FindOrphansStatement : QueryStatement
{
    public required string ChildTableName { get; init; }
    public string? ChildColumnName { get; init; }
    public string? ParentTableName { get; init; }
    public string? ParentColumnName { get; init; }
}

// ============ Common Table Expressions (CTEs) ============

public sealed class CteDefinition
{
    public required string Name { get; init; }
    public List<string>? ColumnNames { get; init; }
    public required QueryStatement Query { get; init; }
}

public sealed class WithStatement : Statement
{
    public required List<CteDefinition> Ctes { get; init; }
    public required QueryStatement MainQuery { get; init; }
}

// ============ Expressions ============

public abstract class Expression { }

/// <summary>
/// Contextual DEFAULT marker used by INSERT value lists. It is materialized
/// against the target column and is not a generally evaluable SQL expression.
/// </summary>
public sealed class DefaultExpression : Expression { }

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

public sealed class CollateExpression : Expression
{
    public required Expression Operand { get; init; }
    public required string Collation { get; init; }
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

public sealed class InSubqueryExpression : Expression
{
    public required Expression Operand { get; init; }
    public required QueryStatement Query { get; init; }
    public bool Negated { get; init; }
}

public sealed class ScalarSubqueryExpression : Expression
{
    public required QueryStatement Query { get; init; }
}

public sealed class ExistsExpression : Expression
{
    public required QueryStatement Query { get; init; }
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
    public required string FunctionName { get; init; } // COUNT, SUM, AVG, MIN, MAX, TEXT
    public required List<Expression> Arguments { get; init; }
    public bool IsDistinct { get; init; }
    public bool IsStarArg { get; init; } // for COUNT(*)
}

/// <summary>
/// A function evaluated over a SQL window. Window calls are represented
/// separately from ordinary function calls so aggregate detection cannot
/// accidentally collapse a row-preserving window query.
/// </summary>
public sealed class WindowFunctionExpression : Expression
{
    public required FunctionCallExpression Function { get; init; }
    public required WindowSpecification Window { get; init; }
}

public sealed class WindowSpecification
{
    public List<Expression> PartitionBy { get; init; } = [];
    public List<OrderByClause> OrderBy { get; init; } = [];
}
