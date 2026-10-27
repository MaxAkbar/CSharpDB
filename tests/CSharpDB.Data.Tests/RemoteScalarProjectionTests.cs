using System.Data;
using CSharpDB.Client;
using CSharpDB.Testing;

namespace CSharpDB.Data.Tests;

// Run unchanged against the existing materialized path and the scalar candidate.
// Real socket transports cover gRPC-Web, native unary, and the persistent stream.
[Collection("ConnectionPoolState")]
public sealed class RemoteScalarProjectionTests
{
    [Theory(Timeout = 45000)]
    [InlineData(CSharpDbGrpcMode.Auto, CSharpDbGrpcCallMode.Unary)]
    [InlineData(CSharpDbGrpcMode.Native, CSharpDbGrpcCallMode.Unary)]
    [InlineData(CSharpDbGrpcMode.Native, CSharpDbGrpcCallMode.Streaming)]
    public async Task Scalar_PreservesDeclaredClrTypes_NullAndFirstCellSemantics(
        CSharpDbGrpcMode mode, CSharpDbGrpcCallMode calls)
    {
        var ct = TestContext.Current.CancellationToken;
        await RunAsync(mode, calls, async connection =>
        {
            using var command = connection.CreateCommand();
            (string Sql, object Expected)[] cases =
            [
                ("SELECT CAST(123 AS INTEGER)", 123),
                ("SELECT CAST(2147483648 AS BIGINT)", 2147483648L),
                ("SELECT CAST(12345678901234.1234 AS DECIMAL(18,4))", 12345678901234.1234m),
                ("SELECT CAST(1 AS BOOLEAN)", true),
                ("SELECT CAST(0 AS BOOLEAN)", false),
                ("SELECT CAST(NULL AS INTEGER)", DBNull.Value),
                ("SELECT CAST(NULL AS BOOLEAN)", DBNull.Value),
                ("SELECT NULL", DBNull.Value),
            ];
            foreach (var testCase in cases)
            {
                command.CommandText = testCase.Sql;
                object? actual = await command.ExecuteScalarAsync(ct);
                Assert.Equal(testCase.Expected.GetType(), actual?.GetType());
                Assert.Equal(testCase.Expected, actual);
            }

            command.CommandText = "CREATE TABLE scalar_cells (id INTEGER PRIMARY KEY, value INTEGER, ignored TEXT)";
            Assert.Null(await command.ExecuteScalarAsync(ct));
            command.CommandText = "INSERT INTO scalar_cells VALUES (2, 222, 'second'); INSERT INTO scalar_cells VALUES (1, 111, 'first')";
            Assert.Null(await command.ExecuteScalarAsync(ct));
            command.CommandText = "SELECT value, ignored FROM scalar_cells ORDER BY id";
            Assert.Equal(111, Assert.IsType<int>(await command.ExecuteScalarAsync(ct)));
            command.CommandText = "SELECT value, ignored FROM scalar_cells WHERE id = 99";
            Assert.Null(await command.ExecuteScalarAsync(ct));
            command.CommandText = "SELECT NULL, ignored FROM scalar_cells ORDER BY id";
            Assert.Same(DBNull.Value, await command.ExecuteScalarAsync(ct));
        }, ct);
    }

    [Theory(Timeout = 45000)]
    [InlineData(CSharpDbGrpcMode.Auto, CSharpDbGrpcCallMode.Unary)]
    [InlineData(CSharpDbGrpcMode.Native, CSharpDbGrpcCallMode.Unary)]
    [InlineData(CSharpDbGrpcMode.Native, CSharpDbGrpcCallMode.Streaming)]
    public async Task Scalar_ReusedCommandAndSql_UseCurrentResponseMetadataAfterSchemaChanges(
        CSharpDbGrpcMode mode, CSharpDbGrpcCallMode calls)
    {
        var ct = TestContext.Current.CancellationToken;
        await RunAsync(mode, calls, async connection =>
        {
            using var ddl = connection.CreateCommand();
            ddl.CommandText = "CREATE TABLE scalar_metadata (id INTEGER PRIMARY KEY, value INTEGER); INSERT INTO scalar_metadata VALUES (1, 7)";
            await ddl.ExecuteNonQueryAsync(ct);
            using var scalar = connection.CreateCommand();
            scalar.CommandText = "SELECT value FROM scalar_metadata WHERE id = 1";
            scalar.Prepare();
            Assert.Equal(7, Assert.IsType<int>(await scalar.ExecuteScalarAsync(ct)));

            // Keep the scalar command and SQL unchanged. Even equal values must
            // change CLR type when their declared SQL type changes.
            ddl.CommandText = "DROP TABLE scalar_metadata; CREATE TABLE scalar_metadata (id INTEGER PRIMARY KEY, value BIGINT); INSERT INTO scalar_metadata VALUES (1, 7)";
            await ddl.ExecuteNonQueryAsync(ct);
            Assert.Equal(7L, Assert.IsType<long>(await scalar.ExecuteScalarAsync(ct)));
            ddl.CommandText = "DROP TABLE scalar_metadata; CREATE TABLE scalar_metadata (id INTEGER PRIMARY KEY, value BOOLEAN); INSERT INTO scalar_metadata VALUES (1, 1)";
            await ddl.ExecuteNonQueryAsync(ct);
            Assert.True(Assert.IsType<bool>(await scalar.ExecuteScalarAsync(ct)));
        }, ct);
    }

    [Theory(Timeout = 45000)]
    [InlineData(CSharpDbGrpcMode.Auto, CSharpDbGrpcCallMode.Unary)]
    [InlineData(CSharpDbGrpcMode.Native, CSharpDbGrpcCallMode.Unary)]
    [InlineData(CSharpDbGrpcMode.Native, CSharpDbGrpcCallMode.Streaming)]
    public async Task Scalar_BindsChangingParameters_AndLeavesQuotedAndCommentedPlaceholdersAlone(
        CSharpDbGrpcMode mode, CSharpDbGrpcCallMode calls)
    {
        var ct = TestContext.Current.CancellationToken;
        await RunAsync(mode, calls, async connection =>
        {
            foreach (bool prepare in new[] { false, true })
            {
                using var command = connection.CreateCommand();
                command.CommandText = "SELECT CAST(@value AS INTEGER) /* @missing */";
                var value = command.Parameters.AddWithValue("@value", 7);
                if (prepare) command.Prepare();
                Assert.Equal(7, Assert.IsType<int>(await command.ExecuteScalarAsync(ct)));
                value.Value = 19;
                Assert.Equal(19, Assert.IsType<int>(await command.ExecuteScalarAsync(ct)));
                value.Value = DBNull.Value;
                Assert.Same(DBNull.Value, await command.ExecuteScalarAsync(ct));

                command.CommandText = "SELECT @value AS \"@missing\" WHERE @value = 'expected' /* @missing */";
                value.Value = "' OR 1 = 1 --";
                if (prepare) command.Prepare();
                Assert.Null(await command.ExecuteScalarAsync(ct));
                command.CommandText = "SELECT @value AS \"@missing\" /* @missing */";
                if (prepare) command.Prepare();
                Assert.Equal("' OR 1 = 1 --", await command.ExecuteScalarAsync(ct));

                command.CommandText = "SELECT @amount";
                command.Parameters.Clear();
                command.Parameters.Add(new CSharpDbParameter("@amount", 12345678901234.1234m)
                {
                    DbType = DbType.Decimal, Precision = 18, Scale = 4,
                });
                if (prepare) command.Prepare();
                Assert.Equal(12345678901234.1234m,
                    Assert.IsType<decimal>(await command.ExecuteScalarAsync(ct)));
            }
        }, ct);
    }

    [Theory(Timeout = 45000)]
    [InlineData(CSharpDbGrpcMode.Auto, CSharpDbGrpcCallMode.Unary)]
    [InlineData(CSharpDbGrpcMode.Native, CSharpDbGrpcCallMode.Unary)]
    [InlineData(CSharpDbGrpcMode.Native, CSharpDbGrpcCallMode.Streaming)]
    public async Task Scalar_DispatchesThroughActiveTransactions_AndTransactionControlCommands(
        CSharpDbGrpcMode mode, CSharpDbGrpcCallMode calls)
    {
        var ct = TestContext.Current.CancellationToken;
        await RunAsync(mode, calls, async connection =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = "CREATE TABLE scalar_tx (id INTEGER PRIMARY KEY, value INTEGER)";
            await command.ExecuteNonQueryAsync(ct);
            await using (var transaction = await connection.BeginTransactionAsync(ct))
            {
                command.Transaction = transaction;
                command.CommandText = "INSERT INTO scalar_tx VALUES (1, 123)";
                Assert.Null(await command.ExecuteScalarAsync(ct));
                command.CommandText = "SELECT value FROM scalar_tx WHERE id = 1";
                Assert.Equal(123, Assert.IsType<int>(await command.ExecuteScalarAsync(ct)));
                await transaction.CommitAsync(ct);
            }
            command.Transaction = null;
            await using (var transaction = await connection.BeginTransactionAsync(ct))
            {
                command.Transaction = transaction;
                command.CommandText = "INSERT INTO scalar_tx VALUES (2, 456)";
                Assert.Null(await command.ExecuteScalarAsync(ct));
                command.CommandText = "SELECT value FROM scalar_tx WHERE id = 2";
                Assert.Equal(456, Assert.IsType<int>(await command.ExecuteScalarAsync(ct)));
                await transaction.RollbackAsync(ct);
            }
            command.Transaction = null;
            command.CommandText = "SELECT COUNT(*) FROM scalar_tx";
            Assert.Equal(1L, Assert.IsType<long>(await command.ExecuteScalarAsync(ct)));

            command.CommandText = "BEGIN TRANSACTION;";
            Assert.Null(await command.ExecuteScalarAsync(ct));
            command.CommandText = "INSERT INTO scalar_tx VALUES (3, 789)";
            Assert.Null(await command.ExecuteScalarAsync(ct));
            command.CommandText = "ROLLBACK;";
            Assert.Null(await command.ExecuteScalarAsync(ct));
            command.CommandText = "SELECT COUNT(*) FROM scalar_tx";
            Assert.Equal(1L, Assert.IsType<long>(await command.ExecuteScalarAsync(ct)));
        }, ct);
    }

    [Theory(Timeout = 45000)]
    [InlineData(CSharpDbGrpcMode.Auto, CSharpDbGrpcCallMode.Unary)]
    [InlineData(CSharpDbGrpcMode.Native, CSharpDbGrpcCallMode.Unary)]
    [InlineData(CSharpDbGrpcMode.Native, CSharpDbGrpcCallMode.Streaming)]
    public async Task Scalar_ExecutesWholeScriptsOnce_AndReturnsTheLastStatementsResult(
        CSharpDbGrpcMode mode, CSharpDbGrpcCallMode calls)
    {
        var ct = TestContext.Current.CancellationToken;
        await RunAsync(mode, calls, async connection =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = "CREATE TABLE scalar_script (id INTEGER PRIMARY KEY, value INTEGER)";
            await command.ExecuteNonQueryAsync(ct);
            command.CommandText = "INSERT INTO scalar_script VALUES (1, 10); SELECT value FROM scalar_script WHERE id = 1; INSERT INTO scalar_script VALUES (2, 20); SELECT COUNT(*) FROM scalar_script";
            Assert.Equal(2L, Assert.IsType<long>(await command.ExecuteScalarAsync(ct)));
            command.CommandText = "SELECT COUNT(*) FROM scalar_script; INSERT INTO scalar_script VALUES (3, 30)";
            Assert.Null(await command.ExecuteScalarAsync(ct));
            command.CommandText = "SELECT COUNT(*) FROM scalar_script";
            Assert.Equal(3L, Assert.IsType<long>(await command.ExecuteScalarAsync(ct)));

            // An earlier SELECT must not hide a later error. Statements before
            // the error retain existing autocommit effects; later ones do not run.
            command.CommandText = "INSERT INTO scalar_script VALUES (4, 40); SELECT value FROM scalar_script WHERE id = 1; SELECT value FROM scalar_missing; INSERT INTO scalar_script VALUES (5, 50)";
            var error = await Assert.ThrowsAsync<CSharpDbDataException>(() => command.ExecuteScalarAsync(ct));
            Assert.Contains("scalar_missing", error.Message, StringComparison.OrdinalIgnoreCase);
            command.CommandText = "SELECT COUNT(*) FROM scalar_script WHERE id = 4";
            Assert.Equal(1L, Assert.IsType<long>(await command.ExecuteScalarAsync(ct)));
            command.CommandText = "SELECT COUNT(*) FROM scalar_script WHERE id = 5";
            Assert.Equal(0L, Assert.IsType<long>(await command.ExecuteScalarAsync(ct)));
        }, ct);
    }

    [Theory(Timeout = 45000)]
    [InlineData(CSharpDbGrpcMode.Auto, CSharpDbGrpcCallMode.Unary)]
    [InlineData(CSharpDbGrpcMode.Native, CSharpDbGrpcCallMode.Unary)]
    [InlineData(CSharpDbGrpcMode.Native, CSharpDbGrpcCallMode.Streaming)]
    public async Task Scalar_PreservesSqlErrorsAndCanceledDispatch_WithoutReplayingWrites(
        CSharpDbGrpcMode mode, CSharpDbGrpcCallMode calls)
    {
        var ct = TestContext.Current.CancellationToken;
        await RunAsync(mode, calls, async connection =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT value FROM scalar_missing";
            var error = await Assert.ThrowsAsync<CSharpDbDataException>(() => command.ExecuteScalarAsync(ct));
            Assert.Contains("scalar_missing", error.Message, StringComparison.OrdinalIgnoreCase);
            command.CommandText = "SELECT CAST(42 AS INTEGER)";
            Assert.Equal(42, Assert.IsType<int>(await command.ExecuteScalarAsync(ct)));

            command.CommandText = "CREATE TABLE scalar_canceled (id INTEGER PRIMARY KEY)";
            await command.ExecuteNonQueryAsync(ct);
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cancellation.Cancel();
            command.CommandText = "INSERT INTO scalar_canceled VALUES (1); SELECT COUNT(*) FROM scalar_canceled";
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => command.ExecuteScalarAsync(cancellation.Token));
            await connection.CloseAsync();
            await connection.OpenAsync(ct);
            command.CommandText = "SELECT COUNT(*) FROM scalar_canceled";
            Assert.Equal(0L, Assert.IsType<long>(await command.ExecuteScalarAsync(ct)));
        }, ct);
    }

    private static async Task RunAsync(
        CSharpDbGrpcMode mode, CSharpDbGrpcCallMode calls,
        Func<CSharpDbConnection, Task> test, CancellationToken cancellationToken)
    {
        await using var host = new GrpcSocketDaemonFixture();
        string endpoint = host.EndpointFor(mode).AbsoluteUri;
        string connectionString = $"Transport=Grpc;Endpoint={endpoint};Grpc Mode={mode};Grpc Call Mode={calls};Pooling=true;Max Pool Size=1";
        try
        {
            await using var connection = new CSharpDbConnection(connectionString);
            await connection.OpenAsync(cancellationToken);
            await test(connection);
        }
        finally
        {
            await CSharpDbClient.ClearPoolAsync(endpoint);
        }
    }
}
