using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;
using CSharpDB.Data;
using CSharpDB.Migration.Canonicalization;
using CSharpDB.Sql;

namespace CSharpDB.Migration.DualRun;

/// <summary>
/// Parameterized CSharpDB target executor. SQL is parsed and classified by the
/// CSharpDB parser before a connection is opened; mutating or unparseable text
/// is never submitted to the target.
/// </summary>
public sealed class CSharpDbDualRunQueryExecutor : IDualRunQueryExecutor
{
    private readonly Func<CancellationToken, ValueTask<CSharpDbConnection>> _connectionFactory;

    public CSharpDbDualRunQueryExecutor(
        string connectionString,
        string snapshotIdentity)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(snapshotIdentity);
        SnapshotIdentity = snapshotIdentity;
        _connectionFactory = async cancellationToken =>
        {
            var connection = new CSharpDbConnection(connectionString);
            try
            {
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                return connection;
            }
            catch
            {
                await connection.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        };
    }

    public CSharpDbDualRunQueryExecutor(
        string snapshotIdentity,
        Func<CancellationToken, ValueTask<CSharpDbConnection>> connectionFactory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(snapshotIdentity);
        ArgumentNullException.ThrowIfNull(connectionFactory);
        SnapshotIdentity = snapshotIdentity;
        _connectionFactory = connectionFactory;
    }

    public string ProviderId => "csharpdb";

    public string SnapshotIdentity { get; }

    public DualRunReadOnlyEnforcement ReadOnlyEnforcement =>
        DualRunReadOnlyEnforcement.StatementValidated;

    public string ReadOnlyValidatorId => "csharpdb-sql-statement-classifier/v1";

    public async ValueTask<IDualRunQueryExecution> ExecuteReadOnlyAsync(
        DualRunExecutionRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ValidateRequestBinding(request);

        try
        {
            SqlStatementClassification classification = SqlStatementClassifier.Classify(request.Sql);
            if (!classification.IsReadOnly)
            {
                throw new DualRunExecutionException(
                    DualRunErrorKind.SafetyRejected,
                    "DUALRUN_CSHARPDB_READ_ONLY_REQUIRED");
            }
        }
        catch (DualRunExecutionException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new DualRunExecutionException(
                DualRunErrorKind.SafetyRejected,
                "DUALRUN_CSHARPDB_READ_ONLY_UNPROVEN",
                ex);
        }

        CSharpDbConnection? connection = null;
        CSharpDbCommand? command = null;
        DbDataReader? reader = null;
        try
        {
            connection = await _connectionFactory(cancellationToken).ConfigureAwait(false)
                ?? throw new DualRunExecutionException(
                    DualRunErrorKind.ProviderError,
                    "DUALRUN_CSHARPDB_CONNECTION_MISSING");
            if (connection.State != ConnectionState.Open)
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

            command = connection.CreateCommand();
            command.CommandText = request.Sql;
            command.CommandTimeout = Math.Max(
                1,
                checked((int)Math.Ceiling(request.Limits.TimeoutPerEndpoint.TotalSeconds)));
            foreach (DualRunParameter parameter in request.Parameters)
            {
                CSharpDbParameter providerParameter = command.CreateParameter();
                providerParameter.ParameterName = parameter.Name;
                providerParameter.DbType = ToProviderType(parameter.Type);
                providerParameter.Value = CloneProviderValue(parameter.Value);
                command.Parameters.Add(providerParameter);
            }

            reader = await command.ExecuteReaderAsync(
                    CommandBehavior.SequentialAccess | CommandBehavior.SingleResult,
                    cancellationToken)
                .ConfigureAwait(false);
            return new CSharpDbQueryExecution(connection, command, reader);
        }
        catch (CSharpDbDataException ex)
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

    private static object CloneProviderValue(object? value) => value switch
    {
        null => DBNull.Value,
        byte[] bytes => (byte[])bytes.Clone(),
        Memory<byte> memory => memory.ToArray(),
        ReadOnlyMemory<byte> memory => memory.ToArray(),
        _ => value,
    };

    private void ValidateRequestBinding(DualRunExecutionRequest request)
    {
        if (!string.Equals(
                request.SnapshotIdentity,
                SnapshotIdentity,
                StringComparison.Ordinal))
        {
            throw new DualRunExecutionException(
                DualRunErrorKind.SafetyRejected,
                "DUALRUN_CSHARPDB_SNAPSHOT_IDENTITY_MISMATCH");
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

    private static CanonicalType ToCanonicalType(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;
        if (type == typeof(bool))
            return CanonicalType.Boolean;
        if (type == typeof(sbyte) || type == typeof(byte) ||
            type == typeof(short) || type == typeof(ushort) ||
            type == typeof(int) || type == typeof(uint) ||
            type == typeof(long))
        {
            return CanonicalType.Int64;
        }
        if (type == typeof(ulong))
            return CanonicalType.UInt64;
        if (type == typeof(decimal))
            return CanonicalType.Decimal;
        if (type == typeof(float))
            return CanonicalType.Binary32;
        if (type == typeof(double))
            return CanonicalType.Binary64;
        if (type == typeof(string) || type == typeof(char))
            return CanonicalType.Text;
        if (type == typeof(byte[]))
            return CanonicalType.Blob;
        if (type == typeof(Guid))
            return CanonicalType.Guid;
        if (type == typeof(DateOnly))
            return CanonicalType.Date;
        if (type == typeof(TimeOnly) || type == typeof(TimeSpan))
            return CanonicalType.Time;
        if (type == typeof(DateTime))
            return CanonicalType.WallDateTime;
        if (type == typeof(DateTimeOffset))
            return CanonicalType.OffsetDateTime;

        throw new DualRunExecutionException(
            DualRunErrorKind.InvalidResult,
            "DUALRUN_CSHARPDB_COLUMN_TYPE_UNSUPPORTED");
    }

    private static DualRunExecutionException ProviderError(CSharpDbDataException exception) =>
        new(
            DualRunErrorKind.ProviderError,
            $"CSHARPDB_{exception.ErrorCode.ToString().ToUpperInvariant()}",
            exception);

    private sealed class CSharpDbQueryExecution : IDualRunQueryExecution
    {
        private CSharpDbConnection? _connection;
        private CSharpDbCommand? _command;
        private DbDataReader? _reader;
        private int _enumerated;

        internal CSharpDbQueryExecution(
            CSharpDbConnection connection,
            CSharpDbCommand command,
            DbDataReader reader)
        {
            _connection = connection;
            _command = command;
            _reader = reader;

            var columns = new DualRunResultColumn[reader.FieldCount];
            for (int index = 0; index < columns.Length; index++)
            {
                columns[index] = new DualRunResultColumn
                {
                    Name = reader.GetName(index),
                    InferredType = ToCanonicalType(reader.GetFieldType(index)),
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
                throw new ObjectDisposedException(nameof(CSharpDbQueryExecution));
            while (true)
            {
                bool hasRow;
                try
                {
                    hasRow = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (CSharpDbDataException ex)
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
                catch (CSharpDbDataException ex)
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
            catch (CSharpDbDataException ex)
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
            CSharpDbCommand? command = Interlocked.Exchange(ref _command, null);
            CSharpDbConnection? connection = Interlocked.Exchange(ref _connection, null);
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
    }
}
