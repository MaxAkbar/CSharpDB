using CSharpDB.Client;
using CSharpDB.Testing;

namespace CSharpDB.Data.Tests;

// Exercise observable command reuse, rather than inspecting the classification cache.
[Collection("ConnectionPoolState")]
public sealed class CommandTextClassificationTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReassignedSql_RebindsCurrentParametersAndInvalidatesPreparedTemplate(bool prepare)
    {
        await using var connection = new CSharpDbConnection("Data Source=:memory:;Pooling=false");
        await connection.OpenAsync(Ct);
        await ExerciseParameterTransitionsAsync(connection, prepare);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task QuotedAndCommentMarkers_KeepTokenizerBindingSemantics(bool prepare)
    {
        await using var connection = new CSharpDbConnection("Data Source=:memory:;Pooling=false");
        await connection.OpenAsync(Ct);
        await ExerciseQuotedMarkersAsync(connection, prepare);
    }

    [Fact]
    public async Task ReusedCommand_RecognizesNormalizedStartBeginCommitAndRollback()
    {
        await using var connection = new CSharpDbConnection("Data Source=:memory:;Pooling=false");
        await connection.OpenAsync(Ct);
        await ExerciseTransactionTransitionsAsync(connection);
    }

    [Theory]
    [InlineData("COMMIT; SELECT 1")]
    [InlineData("COMMIT ; ;")]
    [InlineData("COMMIT /* comment */")]
    [InlineData("ROLLBACK; SELECT 1")]
    [InlineData("BEGIN")]
    [InlineData("BEGIN  TRANSACTION")]
    [InlineData("BEGIN TRANSACTION; SELECT 1")]
    [InlineData("START TRANSACTION; SELECT 1")]
    public async Task TransactionLookalikes_AreDispatchedAsSqlAndDoNotCommitPendingWrites(string sql)
    {
        await using var connection = new CSharpDbConnection("Data Source=:memory:;Pooling=false");
        await connection.OpenAsync(Ct);
        using var command = (CSharpDbCommand)connection.CreateCommand();
        command.CommandText = "CREATE TABLE classification_pending (id INTEGER PRIMARY KEY)";
        await command.ExecuteNonQueryAsync(Ct);
        command.CommandText = "BEGIN TRANSACTION";
        Assert.Null(await command.ExecuteScalarAsync(Ct));
        command.CommandText = "INSERT INTO classification_pending VALUES (1)";
        Assert.Equal(1, await command.ExecuteNonQueryAsync(Ct));

        // These strings do not equal one of the four transaction commands after
        // Trim().TrimEnd(';').Trim(). In particular, spaced semicolons and a
        // transaction prefix in a script must not be broadened into COMMIT.
        command.CommandText = sql;
        await Assert.ThrowsAsync<CSharpDbDataException>(() => command.ExecuteScalarAsync(Ct));

        command.CommandText = " \t ROLLBACK;;; \r\n";
        Assert.Null(await command.ExecuteScalarAsync(Ct));
        command.CommandText = "SELECT COUNT(*) FROM classification_pending";
        Assert.Equal(0L, Convert.ToInt64(await command.ExecuteScalarAsync(Ct)));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" \t\r\n ")]
    [InlineData(";;")]
    public async Task NullAndEmptyAssignments_MatchFreshCommandsAfterPreparedAndTransactionText(string? sql)
    {
        await using var connection = new CSharpDbConnection("Data Source=:memory:;Pooling=false");
        await connection.OpenAsync(Ct);
        using var command = (CSharpDbCommand)connection.CreateCommand();
        command.CommandText = "SELECT @value";
        command.Parameters.AddWithValue("@value", 7);
        command.Prepare();
        Assert.Equal(7L, Convert.ToInt64(await command.ExecuteScalarAsync(Ct)));
        command.CommandText = "BEGIN TRANSACTION";
        Assert.Null(await command.ExecuteScalarAsync(Ct));
        command.CommandText = "ROLLBACK";
        Assert.Null(await command.ExecuteScalarAsync(Ct));

        using var fresh = (CSharpDbCommand)connection.CreateCommand();
        fresh.CommandText = sql;
        command.CommandText = sql;
        Assert.Equal(sql ?? string.Empty, command.CommandText);
        Exception? expected = await Record.ExceptionAsync(() => fresh.ExecuteScalarAsync(Ct));
        Exception? actual = await Record.ExceptionAsync(() => command.ExecuteScalarAsync(Ct));
        Assert.NotNull(expected);
        Assert.NotNull(actual);
        Assert.Equal(expected.GetType(), actual.GetType());
        Assert.Equal(expected.Message, actual.Message);

        command.CommandText = "SELECT 9";
        Assert.Equal(9L, Convert.ToInt64(await command.ExecuteScalarAsync(Ct)));
    }

    [Theory(Timeout = 45000)]
    [InlineData(CSharpDbGrpcMode.Auto, CSharpDbGrpcCallMode.Unary)]
    [InlineData(CSharpDbGrpcMode.Native, CSharpDbGrpcCallMode.Unary)]
    [InlineData(CSharpDbGrpcMode.Native, CSharpDbGrpcCallMode.Streaming)]
    public async Task RemoteScalarAndQueryPaths_ReclassifyAndRebindTheSameCommand(
        CSharpDbGrpcMode mode, CSharpDbGrpcCallMode calls)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var host = new GrpcSocketDaemonFixture();
        string endpoint = host.EndpointFor(mode).AbsoluteUri;
        try
        {
            await using var connection = new CSharpDbConnection(
                $"Transport=Grpc;Endpoint={endpoint};Grpc Mode={mode};Grpc Call Mode={calls};Pooling=true;Max Pool Size=1");
            await connection.OpenAsync(ct);
            await ExerciseParameterTransitionsAsync(connection, prepare: true);
            await ExerciseQuotedMarkersAsync(connection, prepare: true);
            await ExerciseTransactionTransitionsAsync(connection);
        }
        finally
        {
            await CSharpDbClient.ClearPoolAsync(endpoint);
        }
    }

    private static async Task ExerciseParameterTransitionsAsync(CSharpDbConnection connection, bool prepare)
    {
        using var command = (CSharpDbCommand)connection.CreateCommand();
        command.CommandText = "SELECT 11";
        if (prepare) command.Prepare();
        Assert.Equal(11L, Convert.ToInt64(await command.ExecuteScalarAsync(Ct)));

        command.CommandText = "SELECT @value";
        var value = command.Parameters.AddWithValue("@value", 7);
        if (prepare) command.Prepare();
        Assert.Equal(7L, Convert.ToInt64(await command.ExecuteScalarAsync(Ct)));
        value.Value = 19;
        Assert.Equal(19L, Convert.ToInt64(await command.ExecuteScalarAsync(Ct)));

        value.ParameterName = "@renamed";
        await AssertMissingParameterAsync(command, "value");
        // Do not prepare again: reassignment itself must invalidate the template.
        command.CommandText = "SELECT @renamed + 1";
        Assert.Equal(20L, Convert.ToInt64(await command.ExecuteScalarAsync(Ct)));
        command.Parameters.Clear();
        await AssertMissingParameterAsync(command, "renamed");
        value = command.Parameters.AddWithValue("renamed", 23);
        Assert.Equal(24L, Convert.ToInt64(await command.ExecuteScalarAsync(Ct)));

        // A marker-free assignment must stop rebinding the previous template,
        // even while its old parameter has an unusable value.
        value.Value = new ExplodingValue();
        command.CommandText = "SELECT 31";
        Assert.Equal(31L, Convert.ToInt64(await command.ExecuteScalarAsync(Ct)));
        command.CommandText = "SELECT @renamed";
        value.Value = 41;
        Assert.Equal(41L, Convert.ToInt64(await command.ExecuteScalarAsync(Ct)));
        command.Parameters.Clear();
        await AssertMissingParameterAsync(command, "renamed");
    }

    private static async Task ExerciseQuotedMarkersAsync(CSharpDbConnection connection, bool prepare)
    {
        using var command = (CSharpDbCommand)connection.CreateCommand();
        command.CommandText = "SELECT 'COMMIT; @literal' AS \"@alias\" /* @block */ -- @line\n";
        if (prepare) command.Prepare();
        Assert.Equal("COMMIT; @literal", await command.ExecuteScalarAsync(Ct));
        command.Parameters.AddWithValue("@unused", new ExplodingValue());
        Assert.Equal("COMMIT; @literal", await command.ExecuteScalarAsync(Ct));

        command.CommandText = "SELECT @actual AS \"@alias\" /* @missing */ -- @missing\n";
        if (prepare) command.Prepare();
        await AssertMissingParameterAsync(command, "actual");
        var actual = command.Parameters.AddWithValue("@actual", "first");
        Assert.Equal("first", await command.ExecuteScalarAsync(Ct));
        actual.Value = "' OR 1 = 1 -- @missing";
        Assert.Equal("' OR 1 = 1 -- @missing", await command.ExecuteScalarAsync(Ct));
        command.Parameters.Clear();

        // Reassigning identical text must also discard a previously prepared
        // template without treating quoted/comment markers as parameters.
        command.CommandText = "SELECT '@literal' /* @missing */";
        if (prepare) command.Prepare();
        command.CommandText = command.CommandText;
        Assert.Equal("@literal", await command.ExecuteScalarAsync(Ct));
    }

    private static async Task ExerciseTransactionTransitionsAsync(CSharpDbConnection connection)
    {
        using var command = (CSharpDbCommand)connection.CreateCommand();
        command.CommandText = "CREATE TABLE classification_tx (id INTEGER PRIMARY KEY)";
        await command.ExecuteNonQueryAsync(Ct);
        command.CommandText = "SELECT 1";
        Assert.Equal(1L, Convert.ToInt64(await command.ExecuteScalarAsync(Ct)));

        command.CommandText = " \tStArT TrAnSaCtIoN;;; \r\n";
        Assert.Null(await command.ExecuteScalarAsync(Ct));
        command.CommandText = "INSERT INTO classification_tx VALUES (1)";
        Assert.Equal(1, await command.ExecuteNonQueryAsync(Ct));
        command.CommandText = "\r\n cOmMiT;;; \t";
        await using (var reader = await command.ExecuteReaderAsync(Ct))
            Assert.False(await reader.ReadAsync(Ct));
        command.CommandText = "SELECT COUNT(*) FROM classification_tx";
        Assert.Equal(1L, Convert.ToInt64(await command.ExecuteScalarAsync(Ct)));

        command.CommandText = " \t bEgIn TrAnSaCtIoN; \r\n";
        Assert.Equal(0, await command.ExecuteNonQueryAsync(Ct));
        command.CommandText = "INSERT INTO classification_tx VALUES (2)";
        Assert.Equal(1, await command.ExecuteNonQueryAsync(Ct));
        command.CommandText = " rOlLbAcK;;; \t";
        Assert.Null(await command.ExecuteScalarAsync(Ct));
        command.CommandText = "SELECT COUNT(*) FROM classification_tx";
        Assert.Equal(1L, Convert.ToInt64(await command.ExecuteScalarAsync(Ct)));
    }

    private static async Task AssertMissingParameterAsync(CSharpDbCommand command, string name)
    {
        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => command.ExecuteScalarAsync(Ct));
        Assert.Equal($"Parameter '@{name}' was not supplied.", error.Message);
    }

    private sealed class ExplodingValue
    {
        public override string ToString() => throw new InvalidOperationException("Unused parameter was evaluated.");
    }
}
