using System.Collections.Concurrent;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.Execution;

/// <summary>
/// Shared materialization and validation for rows entering table storage.
/// The first DEFAULT slice intentionally accepts only typed literals/NULL, and
/// CHECK accepts only deterministic row-local expressions.
/// </summary>
internal static class RowConstraintValidator
{
    private static readonly ConcurrentDictionary<string, Expression> ParsedExpressions =
        new(StringComparer.Ordinal);

    public static void ValidateSchemaDefinitions(TableSchema schema)
    {
        var namedConstraints = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < schema.ForeignKeys.Count; i++)
        {
            string foreignKeyName = schema.ForeignKeys[i].ConstraintName;
            if (!namedConstraints.Add(foreignKeyName))
            {
                throw new CSharpDbException(
                    ErrorCode.SyntaxError,
                    $"Constraint name '{foreignKeyName}' is specified multiple times on table '{schema.TableName}'.");
            }
        }

        int primaryKeyCount = 0;
        for (int i = 0; i < schema.KeyConstraints.Count; i++)
        {
            KeyConstraintDefinition key = schema.KeyConstraints[i];
            if (key.Columns.Count == 0)
            {
                throw new CSharpDbException(
                    ErrorCode.SyntaxError,
                    $"Key constraint on table '{schema.TableName}' must contain at least one column.");
            }

            if (key.Kind == KeyConstraintKind.PrimaryKey)
                primaryKeyCount++;

            if (key.ConstraintName is { Length: > 0 } keyName && !namedConstraints.Add(keyName))
            {
                throw new CSharpDbException(
                    ErrorCode.SyntaxError,
                    $"Constraint name '{keyName}' is specified multiple times on table '{schema.TableName}'.");
            }

            var keyColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            for (int columnIndex = 0; columnIndex < key.Columns.Count; columnIndex++)
            {
                string columnName = key.Columns[columnIndex];
                if (schema.GetColumnIndex(columnName) < 0)
                {
                    throw new CSharpDbException(
                        ErrorCode.ColumnNotFound,
                        $"Column '{columnName}' referenced by key constraint on table '{schema.TableName}' was not found.");
                }

                if (!keyColumns.Add(columnName))
                {
                    throw new CSharpDbException(
                        ErrorCode.SyntaxError,
                        $"Column '{columnName}' is specified more than once in a key constraint on table '{schema.TableName}'.");
                }
            }
        }

        if (primaryKeyCount > 1)
        {
            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                $"Table '{schema.TableName}' defines more than one PRIMARY KEY.");
        }

        for (int i = 0; i < schema.Columns.Count; i++)
        {
            ColumnDefinition column = schema.Columns[i];
            if (column.DefaultSql is null)
                continue;

            if (column.IsIdentity)
            {
                throw new CSharpDbException(
                    ErrorCode.SyntaxError,
                    $"Identity column '{column.Name}' cannot define a DEFAULT expression.");
            }

            Expression expression = ParseMetadataExpression(column.DefaultSql, "DEFAULT");
            ValidateDefaultExpression(expression, column.Name);
            ValidateDefaultValueType(column, EvaluateDefaultExpression(expression, schema));
        }

        for (int i = 0; i < schema.CheckConstraints.Count; i++)
        {
            CheckConstraintDefinition check = schema.CheckConstraints[i];
            if (check.ConstraintName is { Length: > 0 } name && !namedConstraints.Add(name))
            {
                throw new CSharpDbException(
                    ErrorCode.SyntaxError,
                    $"Constraint name '{name}' is specified multiple times on table '{schema.TableName}'.");
            }

            if (check.ColumnName is { Length: > 0 } columnName &&
                schema.GetColumnIndex(columnName) < 0)
            {
                throw new CSharpDbException(
                    ErrorCode.ColumnNotFound,
                    $"Column-scoped CHECK references missing column '{columnName}' on table '{schema.TableName}'.");
            }

            Expression expression = ParseMetadataExpression(check.ExpressionSql, "CHECK");
            ValidateCheckExpression(expression, schema, check.ColumnName);
        }
    }

    public static void ValidateDefaultExpression(Expression expression, string columnName)
    {
        bool supported = expression switch
        {
            LiteralExpression => true,
            UnaryExpression
            {
                Op: TokenType.Minus,
                Operand: LiteralExpression
                {
                    LiteralType: TokenType.IntegerLiteral or TokenType.RealLiteral,
                },
            } => true,
            _ => false,
        };

        if (!supported)
        {
            throw new CSharpDbException(
                ErrorCode.SyntaxError,
                $"DEFAULT for column '{columnName}' must be a typed literal or NULL; column references, parameters, functions, and subqueries are not supported.");
        }
    }

    public static void ApplyDefaults(TableSchema schema, DbValue[] row, ReadOnlySpan<bool> useDefault)
    {
        if (row.Length != schema.Columns.Count || useDefault.Length != schema.Columns.Count)
            throw new ArgumentException("Row/default presence metadata does not match the table schema.");

        for (int i = 0; i < useDefault.Length; i++)
        {
            if (!useDefault[i])
                continue;

            ColumnDefinition column = schema.Columns[i];
            row[i] = column.DefaultSql is null
                ? DbValue.Null
                : EvaluateDefault(column, schema);
        }
    }

    public static DbValue EvaluateDefault(ColumnDefinition column, TableSchema schema)
    {
        if (column.DefaultSql is null)
            return DbValue.Null;

        Expression expression = ParseMetadataExpression(column.DefaultSql, "DEFAULT");
        ValidateDefaultExpression(expression, column.Name);
        DbValue value = EvaluateDefaultExpression(expression, schema);
        ValidateDefaultValueType(column, value);
        return value;
    }

    public static void ValidateRow(TableSchema schema, ReadOnlySpan<DbValue> row)
    {
        if (row.Length != schema.Columns.Count)
        {
            throw new CSharpDbException(
                ErrorCode.ConstraintViolation,
                $"Row for table '{schema.TableName}' has {row.Length} values; expected {schema.Columns.Count}.");
        }

        for (int i = 0; i < schema.Columns.Count; i++)
        {
            ColumnDefinition column = schema.Columns[i];
            if (!column.Nullable && row[i].IsNull)
            {
                throw new CSharpDbException(
                    ErrorCode.ConstraintViolation,
                    $"NOT NULL constraint failed: {schema.TableName}.{column.Name}.");
            }
        }

        for (int i = 0; i < schema.CheckConstraints.Count; i++)
        {
            CheckConstraintDefinition check = schema.CheckConstraints[i];
            Expression expression = ParseMetadataExpression(check.ExpressionSql, "CHECK");
            ValidateCheckExpression(expression, schema, check.ColumnName);
            DbValue result = ExpressionEvaluator.Evaluate(expression, row, schema, DbFunctionRegistry.Empty);

            // SQL CHECK rejects only FALSE. TRUE and UNKNOWN/NULL both pass.
            if (!result.IsNull && !result.IsTruthy)
            {
                string constraintDisplay = check.ConstraintName is { Length: > 0 } name
                    ? $"'{name}'"
                    : $"unnamed CHECK #{i + 1}";
                throw new CSharpDbException(
                    ErrorCode.ConstraintViolation,
                    $"CHECK constraint {constraintDisplay} failed on table '{schema.TableName}'.");
            }
        }
    }

    private static DbValue EvaluateDefaultExpression(Expression expression, TableSchema schema) =>
        ExpressionEvaluator.Evaluate(expression, ReadOnlySpan<DbValue>.Empty, schema, DbFunctionRegistry.Empty);

    private static void ValidateDefaultValueType(ColumnDefinition column, DbValue value)
    {
        if (value.IsNull || value.Type == column.Type)
            return;

        if (column.Type == DbType.Real && value.Type == DbType.Integer)
            return;

        throw new CSharpDbException(
            ErrorCode.TypeMismatch,
            $"DEFAULT for column '{column.Name}' has type {value.Type}, but the column type is {column.Type}.");
    }

    private static void ValidateCheckExpression(
        Expression expression,
        TableSchema schema,
        string? scopedColumnName)
    {
        switch (expression)
        {
            case LiteralExpression:
                return;
            case ColumnRefExpression column:
                if (column.TableAlias is not null)
                    throw UnsupportedCheck("qualified column references");
                if (schema.GetColumnIndex(column.ColumnName) < 0)
                {
                    throw new CSharpDbException(
                        ErrorCode.ColumnNotFound,
                        $"Column '{column.ColumnName}' referenced by CHECK on table '{schema.TableName}' was not found.");
                }
                if (scopedColumnName is not null &&
                    !string.Equals(scopedColumnName, column.ColumnName, StringComparison.OrdinalIgnoreCase))
                {
                    throw new CSharpDbException(
                        ErrorCode.SyntaxError,
                        $"Column CHECK on '{scopedColumnName}' cannot reference column '{column.ColumnName}' in the first CHECK slice.");
                }
                return;
            case BinaryExpression binary:
                ValidateCheckExpression(binary.Left, schema, scopedColumnName);
                ValidateCheckExpression(binary.Right, schema, scopedColumnName);
                return;
            case UnaryExpression { Op: TokenType.Not or TokenType.Minus } unary:
                ValidateCheckExpression(unary.Operand, schema, scopedColumnName);
                return;
            case CollateExpression collate:
                ValidateCheckExpression(collate.Operand, schema, scopedColumnName);
                return;
            case LikeExpression like:
                ValidateCheckExpression(like.Operand, schema, scopedColumnName);
                ValidateCheckExpression(like.Pattern, schema, scopedColumnName);
                if (like.EscapeChar is not null)
                    ValidateCheckExpression(like.EscapeChar, schema, scopedColumnName);
                return;
            case InExpression inExpression:
                ValidateCheckExpression(inExpression.Operand, schema, scopedColumnName);
                foreach (Expression value in inExpression.Values)
                    ValidateCheckExpression(value, schema, scopedColumnName);
                return;
            case BetweenExpression between:
                ValidateCheckExpression(between.Operand, schema, scopedColumnName);
                ValidateCheckExpression(between.Low, schema, scopedColumnName);
                ValidateCheckExpression(between.High, schema, scopedColumnName);
                return;
            case IsNullExpression isNull:
                ValidateCheckExpression(isNull.Operand, schema, scopedColumnName);
                return;
            case ParameterExpression:
                throw UnsupportedCheck("parameters");
            case FunctionCallExpression:
                throw UnsupportedCheck("functions");
            case WindowFunctionExpression:
                throw UnsupportedCheck("window functions");
            case InSubqueryExpression or ScalarSubqueryExpression or ExistsExpression:
                throw UnsupportedCheck("subqueries");
            case DefaultExpression:
                throw UnsupportedCheck("DEFAULT markers");
            default:
                throw UnsupportedCheck(expression.GetType().Name);
        }
    }

    private static CSharpDbException UnsupportedCheck(string construct) =>
        new(
            ErrorCode.SyntaxError,
            $"CHECK expressions must be deterministic and row-local; {construct} are not supported.");

    private static Expression ParseMetadataExpression(string sql, string metadataKind)
    {
        try
        {
            return ParsedExpressions.GetOrAdd(sql, static expressionSql => Parser.ParseExpressionSql(expressionSql));
        }
        catch (CSharpDbException ex)
        {
            throw new CSharpDbException(
                ErrorCode.CorruptDatabase,
                $"Persisted {metadataKind} expression '{sql}' is invalid.",
                ex);
        }
    }
}
