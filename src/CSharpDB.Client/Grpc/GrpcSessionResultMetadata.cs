using CSharpDB.Client.Models;
using CSharpDB.Client.Internal;

namespace CSharpDB.Client.Grpc;

/// <summary>Private canonical mapped metadata for one validated session schema.</summary>
internal sealed class GrpcSessionResultMetadata
{
    private readonly string[]? _names;
    private readonly string[]? _types;
    private readonly bool[]? _nullability;
    private readonly ColumnDefinition[]? _columns;
    private readonly bool _hasScalarProjectionMetadata;
    private readonly ScalarTypeSnapshot? _scalarType;

    internal bool HasSingleColumnMetadata { get; }

    internal GrpcSessionResultMetadata(SqlExecutionResultMessage definition)
    {
        HasSingleColumnMetadata = GrpcModelMapper.HasSingleColumnMetadata(definition);
        _names = definition.ColumnNames?.Values.ToArray();
        _types = definition.ColumnTypes?.Values.ToArray();
        _nullability = definition.ColumnNullability?.Values.ToArray();
        if (definition.Columns.Count != 0)
        {
            _columns = new ColumnDefinition[definition.Columns.Count];
            for (int i = 0; i < _columns.Length; i++)
                _columns[i] = GrpcModelMapper.ToModel(definition.Columns[i]);
        }

        _hasScalarProjectionMetadata = HasSingleColumnMetadata
            && _names is { Length: 1 } && _columns is { Length: 1 };
        if (_hasScalarProjectionMetadata && _columns![0].DeclaredType is { } type)
        {
            _scalarType = new ScalarTypeSnapshot(
                type.Kind, type.Length, type.Precision, type.Scale, type.FractionalSecondsPrecision);
        }
    }

    internal bool TryGetScalarType(out ScalarTypeSnapshot? declaredType)
    {
        declaredType = _scalarType;
        return _hasScalarProjectionMetadata;
    }

    internal string[]? CreateColumnNames()
        => _names is null ? null : (string[])_names.Clone();

    internal string[]? CreateColumnTypes()
        => _types is null ? null : (string[])_types.Clone();

    internal bool[]? CreateColumnNullability()
        => _nullability is null ? null : (bool[])_nullability.Clone();

    internal ColumnDefinition[]? CreateColumns()
    {
        if (_columns is null) return null;
        var columns = new ColumnDefinition[_columns.Length];
        for (int i = 0; i < columns.Length; i++)
        {
            ColumnDefinition canonical = _columns[i];
            columns[i] = new ColumnDefinition
            {
                SchemaId = canonical.SchemaId,
                Name = canonical.Name,
                Type = canonical.Type,
                DeclaredType = canonical.DeclaredType is { } type ? new SqlTypeDescriptor
                {
                    Kind = type.Kind,
                    Length = type.Length,
                    Precision = type.Precision,
                    Scale = type.Scale,
                    FractionalSecondsPrecision = type.FractionalSecondsPrecision,
                } : null,
                Nullable = canonical.Nullable,
                IsPrimaryKey = canonical.IsPrimaryKey,
                IsIdentity = canonical.IsIdentity,
                IsRowVersion = canonical.IsRowVersion,
                Collation = canonical.Collation,
                DefaultSql = canonical.DefaultSql,
            };
        }
        return columns;
    }
}
