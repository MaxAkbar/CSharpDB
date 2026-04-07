using CSharpDB.Data;
using CSharpDB.Engine;
using CSharpDB.Execution;
using CSharpDB.Primitives;

namespace CSharpDB.Native;

internal sealed class NativePreparedStatement
{
    private const int DefaultPreparedCacheCapacity = 512;
    private static readonly PreparedStatementCache s_preparedCache = new(DefaultPreparedCacheCapacity);

    private readonly Database _database;
    private readonly string _sql;
    private readonly PreparedStatementTemplate? _preparedTemplate;
    private readonly bool _usesTextBindingFallback;
    private readonly CSharpDbParameterCollection _parameters = new();

    private NativePreparedStatement(
        Database database,
        string sql,
        PreparedStatementTemplate? preparedTemplate,
        bool usesTextBindingFallback)
    {
        _database = database;
        _sql = sql;
        _preparedTemplate = preparedTemplate;
        _usesTextBindingFallback = usesTextBindingFallback;
    }

    internal static NativePreparedStatement Create(Database database, string sql)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);

        try
        {
            var preparedTemplate = s_preparedCache.GetOrAdd(sql, static text => PreparedStatementTemplate.Create(text));
            return new NativePreparedStatement(database, sql, preparedTemplate, usesTextBindingFallback: false);
        }
        catch (CSharpDbException)
        {
            // Match the ADO.NET command path: unsupported prepared templates still execute
            // through SQL text binding rather than failing at prepare time.
            return new NativePreparedStatement(database, sql, preparedTemplate: null, usesTextBindingFallback: true);
        }
    }

    internal void BindInt64(string name, long value) => SetParameter(name, value);

    internal void BindDouble(string name, double value) => SetParameter(name, value);

    internal void BindText(string name, string? value) => SetParameter(name, value);

    internal void BindNull(string name) => SetParameter(name, DBNull.Value);

    internal void ClearBindings() => _parameters.Clear();

    internal async ValueTask<QueryResult> ExecuteAsync(CancellationToken cancellationToken = default)
    {
        var preparedTemplate = _preparedTemplate;
        if (preparedTemplate != null)
        {
            if (preparedTemplate.TryBindSimpleInsert(_parameters, out var simpleInsert))
                return await _database.ExecuteAsync(simpleInsert, cancellationToken);

            var preparedStatement = preparedTemplate.Bind(_parameters);
            return await _database.ExecuteAsync(preparedStatement, cancellationToken);
        }

        string sql = _usesTextBindingFallback || _parameters.Count > 0
            ? SqlParameterBinder.Bind(_sql, _parameters)
            : _sql;

        return await _database.ExecuteAsync(sql, cancellationToken);
    }

    private void SetParameter(string name, object? value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        int existingIndex = _parameters.IndexOf(name);
        if (existingIndex >= 0)
        {
            var existing = _parameters[existingIndex];
            existing.ParameterName = name;
            existing.Value = value;
            _parameters[existingIndex] = existing;
            return;
        }

        _parameters.Add(new CSharpDbParameter(name, value));
    }
}
