using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Data;

public sealed class CSharpDbCommand : DbCommand
{
    private const int DefaultPreparedCacheCapacity = 512;
    private static readonly PreparedStatementCache s_preparedCache = new(DefaultPreparedCacheCapacity);

    private CSharpDbParameterCollection _parameters = new();
    private string _commandText = "";
    private PreparedStatementTemplate? _preparedTemplate;

    [AllowNull]
    public override string CommandText
    {
        get => _commandText;
        set
        {
            _commandText = value ?? "";
            _preparedTemplate = null;
        }
    }

    public override int CommandTimeout { get; set; }

    public override CommandType CommandType
    {
        get => CommandType.Text;
        set
        {
            if (value != CommandType.Text)
                throw new NotSupportedException("Only CommandType.Text is supported.");
        }
    }

    public override bool DesignTimeVisible { get; set; }
    public override UpdateRowSource UpdatedRowSource { get; set; }
    protected override DbConnection? DbConnection { get; set; }
    protected override DbTransaction? DbTransaction { get; set; }
    protected override DbParameterCollection DbParameterCollection => _parameters;

    public new CSharpDbParameterCollection Parameters => _parameters;

    // ─── Parameter factory ───────────────────────────────────────────

    public new CSharpDbParameter CreateParameter() => new();
    protected override DbParameter CreateDbParameter() => new CSharpDbParameter();

    // ─── Execution ───────────────────────────────────────────────────

    public override int ExecuteNonQuery()
        => ExecuteNonQueryAsync(CancellationToken.None).GetAwaiter().GetResult();

    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        await using var result = await ExecuteQueryAsync(cancellationToken);
        return result.RowsAffected;
    }

    public override object? ExecuteScalar()
        => ExecuteScalarAsync(CancellationToken.None).GetAwaiter().GetResult();

    public override async Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        await using var result = await ExecuteQueryAsync(cancellationToken);
        if (result.IsQuery &&
            result.Schema.Length > 0 &&
            await result.MoveNextAsync(cancellationToken))
        {
            return TypeMapper.GetClrValue(result.Current[0]);
        }

        return null;
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        => ExecuteDbDataReaderAsync(behavior, CancellationToken.None).GetAwaiter().GetResult();

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(
        CommandBehavior behavior, CancellationToken cancellationToken)
    {
        var connection = (CSharpDbConnection)(DbConnection
            ?? throw new InvalidOperationException("Connection is not set."));
        var result = await ExecuteQueryAsync(cancellationToken);
        return new CSharpDbDataReader(result, behavior, connection);
    }

    internal async ValueTask<CSharpDbCommandExecutionResult> ExecuteCommandAsync(CancellationToken cancellationToken)
    {
        QueryResult result = await ExecuteQueryAsync(cancellationToken);
        long? generatedIntegerKey = result.TryGetGeneratedIntegerKey(out long key) ? key : null;
        return new CSharpDbCommandExecutionResult(result, generatedIntegerKey);
    }

    private async ValueTask<QueryResult> ExecuteQueryAsync(CancellationToken cancellationToken)
    {
        var connection = (CSharpDbConnection)(DbConnection
            ?? throw new InvalidOperationException("Connection is not set."));
        var session = connection.GetSession();

        try
        {
            if (!session.SupportsStructuredExecution)
            {
                string sql = SqlParameterBinder.Bind(CommandText, _parameters);
                return await session.ExecuteAsync(sql, cancellationToken);
            }

            var preparedTemplate = _preparedTemplate;
            if (preparedTemplate == null)
            {
                if (!TryGetAutoPreparedTemplate(out preparedTemplate) || preparedTemplate == null)
                {
                    string sql = SqlParameterBinder.Bind(CommandText, _parameters);
                    return await session.ExecuteAsync(sql, cancellationToken);
                }
            }

            if (preparedTemplate.TryBindSimpleInsert(_parameters, out var simpleInsert))
                return await session.ExecuteAsync(simpleInsert, cancellationToken);

            var preparedStatement = preparedTemplate.Bind(_parameters);
            return await session.ExecuteAsync(preparedStatement, cancellationToken);
        }
        catch (CSharpDbException ex)
        {
            throw new CSharpDbDataException(ex);
        }
    }

    public override void Cancel() { /* no-op */ }
    public override void Prepare()
    {
        try
        {
            _preparedTemplate = s_preparedCache.GetOrAdd(CommandText, static sql => PreparedStatementTemplate.Create(sql));
        }
        catch (CSharpDbException)
        {
            // Keep ADO.NET compatibility: unsupported templates can fall back
            // to the existing SQL-text execution path.
            _preparedTemplate = null;
        }
    }

    private bool TryGetAutoPreparedTemplate(out PreparedStatementTemplate? preparedTemplate)
    {
        preparedTemplate = null;

        if (_parameters.Count == 0 || CommandText.IndexOf('@') < 0)
            return false;

        try
        {
            preparedTemplate = s_preparedCache.GetOrAdd(CommandText, static sql => PreparedStatementTemplate.Create(sql));
            return true;
        }
        catch (CSharpDbException)
        {
            // Fallback to SQL text binding/parsing for patterns not supported by template parsing.
            return false;
        }
    }
}

internal readonly record struct CSharpDbCommandExecutionResult(QueryResult Result, long? GeneratedIntegerKey);
