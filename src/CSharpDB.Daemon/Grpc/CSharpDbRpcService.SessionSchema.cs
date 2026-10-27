using CSharpDB.Client.Grpc;
using CSharpDB.Client.Models;

namespace CSharpDB.Daemon.Grpc;

public sealed partial class CSharpDbRpcService
{
    private const string SingleInt64ResultCapability = "single-int64-result-v1";

    private static void SetCommandSessionResult(CommandSessionResponse response, SqlExecutionResult result,
        CommandResultSchemaCache? schemas, bool allowCompactScalarInt64)
    {
        if (schemas is null) response.Result = GrpcModelMapper.ToMessage(result, includeMetadata: true, allowCompactScalarInt64: allowCompactScalarInt64);
        else schemas.SetResult(response, result, allowCompactScalarInt64);
    }

    /// <summary>One privately owned schema snapshot for one negotiated command stream.</summary>
    private sealed class CommandResultSchemaCache
    {
        internal const string Capability = "result-schema-ref-v1";
        private ResultSchemaSnapshot? _schema;
        private ulong _schemaId;

        internal void Clear() => _schema = null;

        internal void SetResult(CommandSessionResponse response, SqlExecutionResult result, bool allowCompactScalarInt64)
        {
            if (!result.IsQuery || result.Error is not null)
            {
                Clear();
                response.Result = GrpcModelMapper.ToMessage(result, includeMetadata: true, allowCompactScalarInt64: allowCompactScalarInt64);
                return;
            }
            if (_schema?.Matches(result) == true)
            {
                // Compare fresh backend metadata first; a hit never builds or
                // serializes metadata messages merely to discard them afterward.
                response.Result = GrpcModelMapper.ToMessage(result, includeMetadata: false, allowCompactScalarInt64: allowCompactScalarInt64);
                response.ResultSchemaId = _schemaId;
                response.ResultSchemaReference = true;
                return;
            }

            SqlExecutionResultMessage message = GrpcModelMapper.ToMessage(result, includeMetadata: true, allowCompactScalarInt64: allowCompactScalarInt64);
            var schema = new ResultSchemaSnapshot(result);
            ulong id = checked(_schemaId + 1);
            response.Result = message;
            response.ResultSchemaId = id;
            _schema = schema;
            _schemaId = id;
        }
    }

    private sealed class ResultSchemaSnapshot
    {
        private readonly string[]? _names;
        private readonly string[]? _types;
        private readonly bool[]? _nullability;
        private readonly ColumnDefinition[]? _columns;

        internal ResultSchemaSnapshot(SqlExecutionResult result)
        {
            _names = result.ColumnNames is null ? null : (string[])result.ColumnNames.Clone();
            _types = result.ColumnTypes is null ? null : (string[])result.ColumnTypes.Clone();
            _nullability = result.ColumnNullability is null ? null : (bool[])result.ColumnNullability.Clone();
            if (result.Columns is { } columns)
            {
                _columns = new ColumnDefinition[columns.Length];
                for (int i = 0; i < columns.Length; i++) _columns[i] = CopyColumn(columns[i]);
            }
        }

        internal bool Matches(SqlExecutionResult result)
        {
            if (!EqualValues(_names, result.ColumnNames) || !EqualValues(_types, result.ColumnTypes)
                || !EqualValues(_nullability, result.ColumnNullability)) return false;
            var columns = result.Columns;
            if (_columns is null || columns is null) return _columns is null && columns is null;
            if (_columns.Length != columns.Length) return false;
            for (int i = 0; i < columns.Length; i++)
                if (!EqualColumn(_columns[i], columns[i])) return false;
            return true;
        }

        private static bool EqualValues<T>(T[]? expected, T[]? actual)
        {
            if (expected is null || actual is null) return expected is null && actual is null;
            if (expected.Length != actual.Length) return false;
            for (int i = 0; i < expected.Length; i++)
                if (!EqualityComparer<T>.Default.Equals(expected[i], actual[i])) return false;
            return true;
        }

        private static bool EqualColumn(ColumnDefinition expected, ColumnDefinition? actual)
            => actual is not null && expected.SchemaId == actual.SchemaId && expected.Name == actual.Name
                && expected.Type == actual.Type && expected.Nullable == actual.Nullable
                && expected.IsPrimaryKey == actual.IsPrimaryKey && expected.IsIdentity == actual.IsIdentity
                && expected.IsRowVersion == actual.IsRowVersion && expected.Collation == actual.Collation
                && expected.DefaultSql == actual.DefaultSql && EqualDeclaredType(expected.DeclaredType, actual.DeclaredType);

        private static bool EqualDeclaredType(SqlTypeDescriptor? expected, SqlTypeDescriptor? actual)
        {
            if (expected is null || actual is null) return expected is null && actual is null;
            return expected.Kind == actual.Kind && expected.Length == actual.Length
                && expected.Precision == actual.Precision && expected.Scale == actual.Scale
                && expected.FractionalSecondsPrecision == actual.FractionalSecondsPrecision;
        }

        private static ColumnDefinition CopyColumn(ColumnDefinition column)
            => new()
            {
                SchemaId = column.SchemaId,
                Name = column.Name,
                Type = column.Type,
                DeclaredType = column.DeclaredType is { } type ? new SqlTypeDescriptor
                {
                    Kind = type.Kind, Length = type.Length, Precision = type.Precision,
                    Scale = type.Scale, FractionalSecondsPrecision = type.FractionalSecondsPrecision,
                } : null,
                Nullable = column.Nullable,
                IsPrimaryKey = column.IsPrimaryKey,
                IsIdentity = column.IsIdentity,
                IsRowVersion = column.IsRowVersion,
                Collation = column.Collation,
                DefaultSql = column.DefaultSql,
            };
    }
}
