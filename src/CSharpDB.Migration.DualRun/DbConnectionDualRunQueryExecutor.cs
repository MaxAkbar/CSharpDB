using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Runtime.CompilerServices;
using CSharpDB.Migration.Canonicalization;

namespace CSharpDB.Migration.DualRun;

/// <summary>
/// Generic ADO.NET source executor. A mandatory statement validator runs before
/// the connection factory, and callers explicitly declare whether the supplied
/// connection is independently constrained to read-only credentials or mode.
/// </summary>
public sealed class DbConnectionDualRunQueryExecutor : IDualRunQueryExecutor
{
    private readonly Func<CancellationToken, ValueTask<DbConnection>> _connectionFactory;
    private readonly IDualRunReadOnlyStatementValidator _statementValidator;
    private readonly Func<DbException, string>? _providerErrorCodeFactory;

    public DbConnectionDualRunQueryExecutor(
        string providerId,
        string snapshotIdentity,
        Func<CancellationToken, ValueTask<DbConnection>> connectionFactory,
        IDualRunReadOnlyStatementValidator statementValidator,
        bool connectionIsReadOnly,
        Func<DbException, string>? providerErrorCodeFactory = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(providerId);
        ArgumentException.ThrowIfNullOrWhiteSpace(snapshotIdentity);
        ArgumentNullException.ThrowIfNull(connectionFactory);
        ArgumentNullException.ThrowIfNull(statementValidator);
        ArgumentException.ThrowIfNullOrWhiteSpace(statementValidator.ValidatorId);
        if (!connectionIsReadOnly)
        {
            throw new ArgumentException(
                "Generic dual-run sources must use an independently enforced read-only connection.",
                nameof(connectionIsReadOnly));
        }

        ProviderId = providerId;
        SnapshotIdentity = snapshotIdentity;
        _connectionFactory = connectionFactory;
        _statementValidator = statementValidator;
        _providerErrorCodeFactory = providerErrorCodeFactory;
        ReadOnlyEnforcement =
            DualRunReadOnlyEnforcement.StatementValidatedAndReadOnlyConnection;
    }

    public string ProviderId { get; }

    public string SnapshotIdentity { get; }

    public DualRunReadOnlyEnforcement ReadOnlyEnforcement { get; }

    public string ReadOnlyValidatorId => _statementValidator.ValidatorId;

    public async ValueTask<IDualRunQueryExecution> ExecuteReadOnlyAsync(
        DualRunExecutionRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ValidateRequestBinding(request);

        DualRunReadOnlyValidation validation;
        try
        {
            validation = _statementValidator.Validate(request.Sql)
                ?? throw new InvalidOperationException("Read-only validation returned no decision.");
        }
        catch (Exception ex)
        {
            throw new DualRunExecutionException(
                DualRunErrorKind.SafetyRejected,
                "DUALRUN_READ_ONLY_UNPROVEN",
                ex);
        }

        if (!validation.IsReadOnly)
        {
            throw new DualRunExecutionException(
                DualRunErrorKind.SafetyRejected,
                string.IsNullOrWhiteSpace(validation.RejectionCode)
                    ? "DUALRUN_READ_ONLY_REQUIRED"
                    : validation.RejectionCode);
        }

        DbConnection? connection = null;
        DbCommand? command = null;
        DbDataReader? reader = null;
        try
        {
            connection = await _connectionFactory(cancellationToken).ConfigureAwait(false)
                ?? throw new DualRunExecutionException(
                    DualRunErrorKind.ProviderError,
                    "DUALRUN_SOURCE_CONNECTION_MISSING");
            if (connection.State != ConnectionState.Open)
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

            command = connection.CreateCommand()
                ?? throw new DualRunExecutionException(
                    DualRunErrorKind.ProviderError,
                    "DUALRUN_SOURCE_COMMAND_MISSING");
            command.CommandType = CommandType.Text;
            command.CommandText = request.Sql;
            command.CommandTimeout = Math.Max(
                1,
                checked((int)Math.Ceiling(request.Limits.TimeoutPerEndpoint.TotalSeconds)));
            foreach (DualRunParameter parameter in request.Parameters)
            {
                DbParameter providerParameter = command.CreateParameter()
                    ?? throw new DualRunExecutionException(
                        DualRunErrorKind.ProviderError,
                        "DUALRUN_SOURCE_PARAMETER_MISSING");
                providerParameter.ParameterName = parameter.Name;
                providerParameter.Direction = ParameterDirection.Input;
                providerParameter.DbType = ToProviderType(parameter.Type);
                providerParameter.Value = CloneProviderValue(parameter.Value);
                command.Parameters.Add(providerParameter);
            }

            reader = await command.ExecuteReaderAsync(
                    CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                    cancellationToken)
                .ConfigureAwait(false);
            return new DbConnectionQueryExecution(
                connection,
                command,
                reader,
                request.Columns,
                GetProviderErrorCode);
        }
        catch (DbException ex)
        {
            if (reader is not null)
                await reader.DisposeAsync().ConfigureAwait(false);
            if (command is not null)
                await command.DisposeAsync().ConfigureAwait(false);
            if (connection is not null)
                await connection.DisposeAsync().ConfigureAwait(false);
            throw ProviderError(ex);
        }
        catch
        {
            if (reader is not null)
                await reader.DisposeAsync().ConfigureAwait(false);
            if (command is not null)
                await command.DisposeAsync().ConfigureAwait(false);
            if (connection is not null)
                await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private string GetProviderErrorCode(DbException exception)
    {
        if (_providerErrorCodeFactory is not null)
        {
            try
            {
                string code = _providerErrorCodeFactory(exception);
                if (!string.IsNullOrWhiteSpace(code))
                    return code;
            }
            catch
            {
                return "DUALRUN_PROVIDER_ERROR_CLASSIFICATION_FAILED";
            }
        }

        return string.Create(
            CultureInfo.InvariantCulture,
            $"{ProviderId}_DB_{exception.ErrorCode}");
    }

    private DualRunExecutionException ProviderError(DbException exception) =>
        new(
            DualRunErrorKind.ProviderError,
            GetProviderErrorCode(exception),
            exception);

    private void ValidateRequestBinding(DualRunExecutionRequest request)
    {
        if (!string.Equals(
                request.SnapshotIdentity,
                SnapshotIdentity,
                StringComparison.Ordinal))
        {
            throw new DualRunExecutionException(
                DualRunErrorKind.SafetyRejected,
                "DUALRUN_SOURCE_SNAPSHOT_IDENTITY_MISMATCH");
        }
        if (!string.Equals(
                request.CanonicalizationId,
                DualRunReportFormats.CanonicalizationId,
                StringComparison.Ordinal) ||
            !string.Equals(
                request.CanonicalizationContractHash,
                DualRunReportFormats.CanonicalizationContractHash,
                StringComparison.Ordinal))
        {
            throw new DualRunExecutionException(
                DualRunErrorKind.SafetyRejected,
                "DUALRUN_CANONICALIZATION_CONTRACT_MISMATCH");
        }
    }

    private static object CloneProviderValue(object? value) => value switch
    {
        null => DBNull.Value,
        byte[] bytes => (byte[])bytes.Clone(),
        Memory<byte> memory => memory.ToArray(),
        ReadOnlyMemory<byte> memory => memory.ToArray(),
        _ => value,
    };

    private static System.Data.DbType ToProviderType(CanonicalType type) => type switch
    {
        CanonicalType.Boolean => System.Data.DbType.Boolean,
        CanonicalType.Int64 => System.Data.DbType.Int64,
        CanonicalType.UInt64 => System.Data.DbType.UInt64,
        CanonicalType.Decimal => System.Data.DbType.Decimal,
        CanonicalType.Binary32 => System.Data.DbType.Single,
        CanonicalType.Binary64 => System.Data.DbType.Double,
        CanonicalType.Text => System.Data.DbType.String,
        CanonicalType.Blob => System.Data.DbType.Binary,
        CanonicalType.Guid => System.Data.DbType.Guid,
        CanonicalType.Date => System.Data.DbType.Date,
        CanonicalType.Time => System.Data.DbType.Time,
        CanonicalType.WallDateTime => System.Data.DbType.DateTime2,
        CanonicalType.UtcInstant => System.Data.DbType.DateTimeOffset,
        CanonicalType.OffsetDateTime => System.Data.DbType.DateTimeOffset,
        _ => throw new DualRunExecutionException(
            DualRunErrorKind.InvalidResult,
            "DUALRUN_PARAMETER_TYPE_UNSUPPORTED"),
    };

    private static bool TryGetCanonicalType(Type type, out CanonicalType canonicalType)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;
        if (type == typeof(bool))
            canonicalType = CanonicalType.Boolean;
        else if (type == typeof(sbyte) || type == typeof(byte) ||
                 type == typeof(short) || type == typeof(ushort) ||
                 type == typeof(int) || type == typeof(uint) ||
                 type == typeof(long))
        {
            canonicalType = CanonicalType.Int64;
        }
        else if (type == typeof(ulong))
            canonicalType = CanonicalType.UInt64;
        else if (type == typeof(decimal))
            canonicalType = CanonicalType.Decimal;
        else if (type == typeof(float))
            canonicalType = CanonicalType.Binary32;
        else if (type == typeof(double))
            canonicalType = CanonicalType.Binary64;
        else if (type == typeof(string) || type == typeof(char))
            canonicalType = CanonicalType.Text;
        else if (type == typeof(byte[]))
            canonicalType = CanonicalType.Blob;
        else if (type == typeof(Guid))
            canonicalType = CanonicalType.Guid;
        else if (type == typeof(DateOnly))
            canonicalType = CanonicalType.Date;
        else if (type == typeof(TimeOnly) || type == typeof(TimeSpan))
            canonicalType = CanonicalType.Time;
        else if (type == typeof(DateTime))
            canonicalType = CanonicalType.WallDateTime;
        else if (type == typeof(DateTimeOffset))
            canonicalType = CanonicalType.OffsetDateTime;
        else
        {
            canonicalType = default;
            return false;
        }
        return true;
    }

    private sealed class DbConnectionQueryExecution : IDualRunQueryExecution
    {
        private readonly Func<DbException, string> _providerErrorCode;
        private DbConnection? _connection;
        private DbCommand? _command;
        private DbDataReader? _reader;
        private int _enumerated;

        internal DbConnectionQueryExecution(
            DbConnection connection,
            DbCommand command,
            DbDataReader reader,
            IReadOnlyList<DualRunColumnContract> declaredColumns,
            Func<DbException, string> providerErrorCode)
        {
            _connection = connection;
            _command = command;
            _reader = reader;
            _providerErrorCode = providerErrorCode;

            var columns = new DualRunResultColumn[reader.FieldCount];
            bool declaredWidthMatches = declaredColumns.Count == reader.FieldCount;
            for (int index = 0; index < columns.Length; index++)
            {
                if (!TryGetCanonicalType(reader.GetFieldType(index), out CanonicalType type))
                {
                    if (!declaredWidthMatches)
                    {
                        throw new DualRunExecutionException(
                            DualRunErrorKind.InvalidResult,
                            "DUALRUN_SOURCE_COLUMN_TYPE_UNSUPPORTED");
                    }
                    type = declaredColumns[index].Type;
                }

                columns[index] = new DualRunResultColumn
                {
                    Name = reader.GetName(index),
                    InferredType = type,
                };
            }
            Columns = columns;
        }

        public IReadOnlyList<DualRunResultColumn> Columns { get; }

        public async IAsyncEnumerable<IReadOnlyList<object?>> ReadRowsAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            if (Interlocked.Exchange(ref _enumerated, 1) != 0)
            {
                throw new DualRunExecutionException(
                    DualRunErrorKind.InvalidResult,
                    "DUALRUN_RESULT_ENUMERATED_TWICE");
            }

            DbDataReader reader = _reader ??
                throw new ObjectDisposedException(nameof(DbConnectionQueryExecution));
            while (true)
            {
                bool hasRow;
                try
                {
                    hasRow = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (DbException ex)
                {
                    throw ProviderError(ex);
                }

                if (!hasRow)
                    break;

                object?[] row;
                try
                {
                    row = new object?[reader.FieldCount];
                    for (int index = 0; index < row.Length; index++)
                    {
                        object value = reader.GetValue(index);
                        row[index] = value is byte[] bytes ? (byte[])bytes.Clone() : value;
                    }
                }
                catch (DbException ex)
                {
                    throw ProviderError(ex);
                }
                yield return row;
            }

            bool hasNextResult;
            try
            {
                hasNextResult = await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (DbException ex)
            {
                throw ProviderError(ex);
            }
            if (hasNextResult)
            {
                throw new DualRunExecutionException(
                    DualRunErrorKind.InvalidResult,
                    "DUALRUN_MULTIPLE_RESULTS_REJECTED");
            }
        }

        public async ValueTask DisposeAsync()
        {
            DbDataReader? reader = Interlocked.Exchange(ref _reader, null);
            DbCommand? command = Interlocked.Exchange(ref _command, null);
            DbConnection? connection = Interlocked.Exchange(ref _connection, null);
            try
            {
                if (reader is not null)
                    await reader.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                try
                {
                    if (command is not null)
                        await command.DisposeAsync().ConfigureAwait(false);
                }
                finally
                {
                    if (connection is not null)
                        await connection.DisposeAsync().ConfigureAwait(false);
                }
            }
        }

        private DualRunExecutionException ProviderError(DbException exception) =>
            new(
                DualRunErrorKind.ProviderError,
                _providerErrorCode(exception),
                exception);
    }
}
