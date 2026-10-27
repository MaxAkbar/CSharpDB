using System.Reflection;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using ClientColumn = CSharpDB.Client.Models.ColumnDefinition;
using ClientDbType = CSharpDB.Client.Models.DbType;
using ClientSqlType = CSharpDB.Client.Models.SqlTypeDescriptor;
using ClientSqlKind = CSharpDB.Client.Models.SqlTypeKind;

namespace CSharpDB.Data.Tests;

// Compare remote scalar conversion with the materialized result's existing semantics.
public sealed class RemoteScalarProjectionUnitTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(6)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(9)]
    [InlineData(10)]
    [InlineData(11)]
    [InlineData(12)]
    public async Task CompleteSingleCellMetadata_PreservesExistingConversions(int shape)
    {
        var ct = TestContext.Current.CancellationToken;
        (ClientColumn Column, object? Value, object Expected) conversion = shape switch
        {
            0 => (Column(ClientDbType.Integer, ClientSqlKind.Integer), 123L, 123),
            1 => (Column(ClientDbType.Integer, ClientSqlKind.BigInt), 123L, 123L),
            2 => (Column(ClientDbType.Integer, ClientSqlKind.SmallInt), 123L, (short)123),
            3 => (Column(ClientDbType.Integer, ClientSqlKind.TinyInt), 123L, (byte)123),
            4 => (Column(ClientDbType.Integer, ClientSqlKind.Boolean), 1L, true),
            5 => (Column(ClientDbType.Integer, ClientSqlKind.Boolean), false, false),
            6 => (Column(ClientDbType.Decimal, ClientSqlKind.Decimal), 12345678901234.1234m, 12345678901234.1234m),
            7 => (Column(ClientDbType.Text, ClientSqlKind.Text), "text", "text"),
            8 => (Column(ClientDbType.Text, ClientSqlKind.Uuid), "e356efad-5c0f-4fb1-acfe-e7e109a324b4", Guid.Parse("e356efad-5c0f-4fb1-acfe-e7e109a324b4")),
            9 => (Column(ClientDbType.Text, ClientSqlKind.Date), "2026-10-14", new DateOnly(2026, 10, 14)),
            10 => (Column(ClientDbType.Integer, ClientSqlKind.Integer), null, DBNull.Value),
            11 => (Column(ClientDbType.Integer, ClientSqlKind.Boolean), DBNull.Value, DBNull.Value),
            // Legacy physical metadata intentionally uses Int64, unlike INTEGER.
            _ => (new ClientColumn { Name = "value", Type = ClientDbType.Integer }, 123L, 123L),
        };
        var (column, value, expected) = conversion;
        var response = Query([value], column);
        var (client, proxy) = Client(() => Task.FromResult(response));
        await using var session = new RemoteDatabaseSession(client);
        object? actual = await session.ExecuteScalarAsync("SELECT unchanged", ct);
        Assert.Equal(expected.GetType(), actual?.GetType());
        Assert.Equal(expected, actual);
        Assert.Single(proxy.Dispatches);
        Assert.Equal("SELECT unchanged", proxy.Dispatches[0].Sql);
        Assert.Equal(ct, proxy.Dispatches[0].Token);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(6)]
    public async Task OlderServerMetadata_UsesTheSameLogicalAndInferredFallback(int shape)
    {
        var ct = TestContext.Current.CancellationToken;
        (string? Label, object? Input, object Expected) conversion = shape switch
        {
            0 => (null, 42L, 42L),
            1 => ("INTEGER", 42L, 42),
            2 => ("BIGINT", 42L, 42L),
            3 => ("BOOLEAN", 1L, true),
            4 => ("INTEGER", null, DBNull.Value),
            5 => ("provider_specific_label", "value", "value"),
            _ => ("DECIMAL(10,4)", 0.1234m, 0.1234m),
        };
        var (label, input, expected) = conversion;
        var response = new SqlExecutionResult
        {
            IsQuery = true, ColumnNames = ["value"],
            ColumnTypes = label is null ? null : [label],
            ColumnNullability = [true], Rows = [[input]],
        };
        var (client, proxy) = Client(() => Task.FromResult(response));
        await using var session = new RemoteDatabaseSession(client);
        object? actual = await session.ExecuteScalarAsync("SELECT unchanged", ct);
        Assert.Equal(expected.GetType(), actual?.GetType());
        Assert.Equal(expected, actual);
        Assert.Single(proxy.Dispatches);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(6)]
    public async Task MalformedOrUnusedValuesAndMetadata_RetainMaterializedFailures(int shape)
    {
        var ct = TestContext.Current.CancellationToken;
        SqlExecutionResult response = shape switch
        {
            // The second selected cell is unused by ExecuteScalar, but the
            // existing result materializer still converts it and reports failure.
            0 => new()
            {
                IsQuery = true, ColumnNames = ["value", "ignored"],
                Columns = [Column(ClientDbType.Integer, ClientSqlKind.Integer),
                    new ClientColumn { Name = "ignored", Type = ClientDbType.Text }],
                Rows = [[42L, new ExplodingValue()]],
            },
            // The same holds for rows after the selected first row.
            1 => new()
            {
                IsQuery = true, ColumnNames = ["value"],
                Columns = [Column(ClientDbType.Integer, ClientSqlKind.Integer)],
                Rows = [[42L], [new ExplodingValue()]],
            },
            // Unused column descriptors are validated while mapping the schema.
            2 => new()
            {
                IsQuery = true, ColumnNames = ["value", "ignored"],
                Columns = [Column(ClientDbType.Integer, ClientSqlKind.Integer),
                    Column(ClientDbType.Text, (ClientSqlKind)255, "ignored")],
                Rows = [[42L, "ignored"]],
            },
            // A row with no first value is malformed, rather than an empty result.
            3 => Query([], Column(ClientDbType.Integer, ClientSqlKind.Integer)),
            // A surplus physical cell must not bypass its existing conversion.
            4 => Query([42L, new ExplodingValue()], Column(ClientDbType.Integer, ClientSqlKind.Integer)),
            5 => Query([2147483648L], Column(ClientDbType.Integer, ClientSqlKind.Integer)),
            _ => Query([12345678901234567890.1234m], Column(ClientDbType.Decimal, ClientSqlKind.Decimal)),
        };
        var (baselineClient, baselineProxy) = Client(() => Task.FromResult(response));
        await using var baseline = new RemoteDatabaseSession(baselineClient);
        Exception? expected = await Record.ExceptionAsync(() => MaterializedScalarAsync(baseline, "SELECT unchanged", ct));
        Assert.NotNull(expected);

        var (client, proxy) = Client(() => Task.FromResult(response));
        await using var session = new RemoteDatabaseSession(client);
        Exception? actual = await Record.ExceptionAsync(async () => { await session.ExecuteScalarAsync("SELECT unchanged", ct); });
        Assert.NotNull(actual);
        Assert.Equal(expected.GetType(), actual.GetType());
        Assert.Equal(expected.Message, actual.Message);
        Assert.Single(baselineProxy.Dispatches);
        Assert.Single(proxy.Dispatches);
    }

    [Theory]
    [InlineData(0, true)]
    [InlineData(1, true)]
    [InlineData(2, false)]
    [InlineData(3, false)]
    public async Task CancellationAfterCompletedResponse_PreservesQueryBoundaryWithoutReplay(
        int shape, bool throwsCancellation)
    {
        var ct = TestContext.Current.CancellationToken;
        using var canceledAtResponse = CancellationTokenSource.CreateLinkedTokenSource(ct);
        SqlExecutionResult response = shape switch
        {
            0 => Query([42L], Column(ClientDbType.Integer, ClientSqlKind.Integer)),
            1 => new() { IsQuery = true, ColumnNames = ["value"], Columns = [Column(ClientDbType.Integer, ClientSqlKind.Integer)], Rows = [] },
            2 => new() { IsQuery = false, RowsAffected = 1 },
            _ => new() { IsQuery = true, ColumnNames = [], Columns = [], Rows = [] },
        };
        var (client, proxy) = Client(() =>
        {
            // Deterministic: transport reports a completed response, then the
            // ADO conversion boundary observes cancellation. There is no timer.
            canceledAtResponse.Cancel();
            return Task.FromResult(response);
        });
        await using var session = new RemoteDatabaseSession(client);
        const string sql = "INSERT INTO exact_script VALUES (1); SELECT value FROM exact_script";
        if (throwsCancellation)
        {
            var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            {
                await session.ExecuteScalarAsync(sql, canceledAtResponse.Token);
            });
            Assert.Equal(canceledAtResponse.Token, error.CancellationToken);
        }
        else
        {
            // Existing ExecuteScalar does not advance a row for DML or a
            // zero-column response, so it has no post-response cancellation check.
            Assert.Null(await session.ExecuteScalarAsync(sql, canceledAtResponse.Token));
        }
        Assert.Single(proxy.Dispatches);
        Assert.Equal(sql, proxy.Dispatches[0].Sql);
    }

    [Fact]
    public async Task ActiveTransaction_UsesTheSameSingleDispatchAndRetainsExactSql()
    {
        var ct = TestContext.Current.CancellationToken;
        var (client, proxy) = Client(() => Task.FromResult(Query([42L], Column(ClientDbType.Integer, ClientSqlKind.Integer))));
        var session = new RemoteDatabaseSession(client);
        const string sql = "INSERT INTO exact_script VALUES (1); SELECT value FROM exact_script";
        await session.BeginTransactionAsync(ct);
        Assert.Equal(42, Assert.IsType<int>(await session.ExecuteScalarAsync(sql, ct)));
        Assert.Equal(nameof(ICSharpDbClient.ExecuteInTransactionAsync), proxy.Dispatches[0].Method);
        Assert.Equal("scalar-transaction", proxy.Dispatches[0].TransactionId);
        Assert.Equal(sql, proxy.Dispatches[0].Sql);
        Assert.Equal(ct, proxy.Dispatches[0].Token);
        await session.CommitAsync(ct);
        Assert.Equal(42, Assert.IsType<int>(await session.ExecuteScalarAsync(sql, ct)));
        Assert.Equal(2, proxy.Dispatches.Count);
        Assert.Equal(nameof(ICSharpDbClient.ExecuteSqlAsync), proxy.Dispatches[1].Method);
        Assert.Null(proxy.Dispatches[1].TransactionId);
        Assert.Equal(sql, proxy.Dispatches[1].Sql);
        await session.DisposeAsync();
        Assert.Equal(1, proxy.Begins);
        Assert.Equal(1, proxy.Commits);
        Assert.Equal(0, proxy.Rollbacks);
        Assert.Equal(1, proxy.Disposals);
    }

    [Theory]
    [InlineData("result", false)]
    [InlineData("result", true)]
    [InlineData("synchronous", false)]
    [InlineData("synchronous", true)]
    [InlineData("asynchronous", false)]
    [InlineData("asynchronous", true)]
    public async Task Errors_DoNotReplayOrChangeTransactionDispatch(string failure, bool activeTransaction)
    {
        var ct = TestContext.Current.CancellationToken;
        var injected = new HttpRequestException("response lost after dispatch");
        var (client, proxy) = Client(() => failure switch
        {
            "result" => Task.FromResult(new SqlExecutionResult { Error = "injected SQL failure", ErrorCode = ErrorCode.Busy }),
            "synchronous" => throw injected,
            _ => Task.FromException<SqlExecutionResult>(injected),
        });
        var session = new RemoteDatabaseSession(client);
        if (activeTransaction) await session.BeginTransactionAsync(ct);
        const string sql = "INSERT INTO exact_script VALUES (1); SELECT value FROM exact_script";
        Exception? error = await Record.ExceptionAsync(async () => { await session.ExecuteScalarAsync(sql, ct); });
        if (failure == "result")
        {
            var databaseError = Assert.IsType<CSharpDbException>(error);
            Assert.Equal("injected SQL failure", databaseError.Message);
            // The current session maps response.Error to Unknown. Preserve this
            // existing behavior here; ErrorCode transport fidelity is separate.
            Assert.Equal(ErrorCode.Unknown, databaseError.Code);
        }
        else Assert.Same(injected, error);
        Assert.Single(proxy.Dispatches);
        Assert.Equal(sql, proxy.Dispatches[0].Sql);
        Assert.Equal(activeTransaction ? "scalar-transaction" : null, proxy.Dispatches[0].TransactionId);
        Assert.Equal(activeTransaction ? nameof(ICSharpDbClient.ExecuteInTransactionAsync) : nameof(ICSharpDbClient.ExecuteSqlAsync), proxy.Dispatches[0].Method);
        await session.DisposeAsync();
        Assert.Single(proxy.Dispatches);
        Assert.Equal(activeTransaction ? 1 : 0, proxy.Rollbacks);
        Assert.Equal(1, proxy.Disposals);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(6)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(9)]
    [InlineData(10)]
    [InlineData(11)]
    [InlineData(12)]
    [InlineData(13)]
    [InlineData(14)]
    [InlineData(15)]
    public async Task InvalidDeclaredFacets_PreserveValidationForNullAndCanceledResponses(int shape)
    {
        ClientSqlType type = shape switch
        {
            0 => new() { Kind = ClientSqlKind.Integer, Length = 1 },
            1 => new() { Kind = ClientSqlKind.VarChar, Length = 0 },
            2 => new() { Kind = ClientSqlKind.VarBinary, Length = -1 },
            3 => new() { Kind = ClientSqlKind.Integer, Precision = 1 },
            4 => new() { Kind = ClientSqlKind.Integer, Scale = 0 },
            5 => new() { Kind = ClientSqlKind.Decimal, Precision = 0 },
            6 => new() { Kind = ClientSqlKind.Decimal, Precision = 19 },
            7 => new() { Kind = ClientSqlKind.Decimal, Scale = 0 },
            8 => new() { Kind = ClientSqlKind.Decimal, Precision = 3, Scale = -1 },
            9 => new() { Kind = ClientSqlKind.Decimal, Precision = 3, Scale = 4 },
            10 => new() { Kind = ClientSqlKind.Integer, FractionalSecondsPrecision = 0 },
            11 => new() { Kind = ClientSqlKind.Time, FractionalSecondsPrecision = -1 },
            12 => new() { Kind = ClientSqlKind.Timestamp, FractionalSecondsPrecision = 8 },
            13 => new() { Kind = (ClientSqlKind)255 },
            // Multiple invalid facets retain Create's first failing argument.
            14 => new() { Kind = ClientSqlKind.VarChar, Length = 0, Precision = 1 },
            _ => new() { Kind = ClientSqlKind.Decimal, Length = 1, Precision = 0 },
        };
        var column = new ClientColumn { Name = "value", Type = ClientDbType.Integer, DeclaredType = type };
        foreach (bool nullValue in new[] { false, true })
        foreach (bool cancelAtResponse in new[] { false, true })
        {
            // Validation must run even when CLR conversion would return DBNull
            // immediately, and before the query's cancellation boundary.
            var response = Query([nullValue ? null : 42L], column);
            Exception error = await AssertMaterializedFailureAsync(response, cancelAtResponse);
            Assert.IsNotType<OperationCanceledException>(error);
            Assert.True(error is ArgumentException or InvalidOperationException);
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public async Task SingleCellFailures_PreserveValueMetadataCancellationAndClrOrder(int shape)
    {
        ClientColumn invalid = new()
        {
            Name = "value", Type = ClientDbType.Integer,
            DeclaredType = new() { Kind = ClientSqlKind.Integer, Length = 1 },
        };
        SqlExecutionResult response = shape switch
        {
            // Raw cell conversion runs before metadata and cancellation.
            0 => Query([new ExplodingValue()], invalid),
            // Facet validation runs before cancellation and checked Int32 conversion.
            1 => Query([2147483648L], invalid),
            // With valid metadata, cancellation wins over checked CLR conversion.
            2 or 3 => Query([2147483648L], Column(ClientDbType.Integer, ClientSqlKind.Integer)),
            // A SQL error wins before any cell or metadata conversion.
            _ => new()
            {
                IsQuery = true, ColumnNames = ["value"], Columns = [invalid],
                Rows = [[new ExplodingValue()]], Error = "SQL failed before projection",
            },
        };
        Exception error = await AssertMaterializedFailureAsync(response, cancelAtResponse: shape != 3);
        switch (shape)
        {
            case 0:
                Assert.IsType<InvalidOperationException>(error);
                Assert.Equal("unused cell conversion failed", error.Message);
                break;
            case 1:
                Assert.Equal("length", Assert.IsType<ArgumentException>(error).ParamName);
                break;
            case 2:
                Assert.IsAssignableFrom<OperationCanceledException>(error);
                break;
            case 3:
                Assert.IsType<OverflowException>(error);
                break;
            default:
                Assert.Equal("SQL failed before projection", Assert.IsType<CSharpDbException>(error).Message);
                break;
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(6)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(9)]
    [InlineData(10)]
    [InlineData(11)]
    [InlineData(12)]
    [InlineData(13)]
    [InlineData(14)]
    [InlineData(15)]
    [InlineData(16)]
    [InlineData(17)]
    [InlineData(18)]
    [InlineData(19)]
    public async Task RemainingLogicalKindsAndValidFacets_MatchMaterializedProjection(int shape)
    {
        byte[] bytes = [0x80];
        var bits = new SqlBitString(bytes, 1);
        var time = new TimeOnly(12, 34, 56).Add(TimeSpan.FromTicks(1234567));
        var timestamp = new DateTime(2026, 10, 14, 12, 34, 56).AddTicks(1234567);
        var offset = new DateTimeOffset(timestamp, TimeSpan.FromHours(-7));
        (ClientSqlType Type, object Input, object Expected) conversion = shape switch
        {
            0 => (new() { Kind = ClientSqlKind.Real }, 1.25d, 1.25d),
            1 => (new() { Kind = ClientSqlKind.Double }, 1.25d, 1.25d),
            2 => (new() { Kind = ClientSqlKind.Char, Length = 1 }, "x", "x"),
            3 => (new() { Kind = ClientSqlKind.VarChar, Length = int.MaxValue }, "text", "text"),
            4 => (new() { Kind = ClientSqlKind.Binary, Length = 1 }, bytes, bytes),
            5 => (new() { Kind = ClientSqlKind.VarBinary, Length = int.MaxValue }, bytes, bytes),
            6 => (new() { Kind = ClientSqlKind.Blob }, bytes, bytes),
            7 => (new() { Kind = ClientSqlKind.Time, FractionalSecondsPrecision = 7 }, time, time),
            8 => (new() { Kind = ClientSqlKind.Timestamp, FractionalSecondsPrecision = 7 }, timestamp, timestamp),
            9 => (new() { Kind = ClientSqlKind.TimestampWithTimeZone, FractionalSecondsPrecision = 7 }, offset, offset),
            10 => (new() { Kind = ClientSqlKind.IntervalYearToMonth }, "1-2", "1-2"),
            11 => (new() { Kind = ClientSqlKind.IntervalDayToSecond, FractionalSecondsPrecision = 7 }, "1.02:03:04.1234567", TimeSpan.Parse("1.02:03:04.1234567", System.Globalization.CultureInfo.InvariantCulture)),
            12 => (new() { Kind = ClientSqlKind.Json }, "{\"value\":42}", "{\"value\":42}"),
            13 => (new() { Kind = ClientSqlKind.Xml }, "<value>42</value>", "<value>42</value>"),
            14 => (new() { Kind = ClientSqlKind.Bit, Length = 1 }, bits, bits),
            15 => (new() { Kind = ClientSqlKind.VarBit, Length = int.MaxValue }, bits, bits),
            16 => (new() { Kind = ClientSqlKind.Decimal, Precision = 1, Scale = 0 }, 1m, 1m),
            17 => (new() { Kind = ClientSqlKind.Decimal, Precision = 18, Scale = 18 }, 0.123456789012345678m, 0.123456789012345678m),
            18 => (new() { Kind = ClientSqlKind.Time, FractionalSecondsPrecision = 0 }, "12:34:56", new TimeOnly(12, 34, 56)),
            _ => (new() { Kind = ClientSqlKind.IntervalDayToSecond, FractionalSecondsPrecision = 0 }, "02:03:04", new TimeSpan(2, 3, 4)),
        };
        var column = new ClientColumn
        {
            Name = "value", Type = conversion.Type.StorageType, DeclaredType = conversion.Type,
        };
        foreach (bool nullValue in new[] { false, true })
        {
            object expected = nullValue ? DBNull.Value : conversion.Expected;
            object? actual = await AssertMaterializedValueAsync(Query([nullValue ? null : conversion.Input], column));
            Assert.Equal(expected.GetType(), actual?.GetType());
            Assert.Equal(expected, actual);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task UnknownPhysicalType_RetainsLegacyMappingBehavior(bool hasDeclaredType)
    {
        var column = new ClientColumn
        {
            Name = "value", Type = (ClientDbType)255,
            DeclaredType = hasDeclaredType ? new() { Kind = ClientSqlKind.Integer } : null,
        };
        object? actual = await AssertMaterializedValueAsync(Query([42L], column));
        if (hasDeclaredType) Assert.Equal(42, Assert.IsType<int>(actual));
        else Assert.Equal(42L, Assert.IsType<long>(actual));
    }

    private static async Task<object?> AssertMaterializedValueAsync(SqlExecutionResult response)
    {
        const string sql = "SELECT same_sql_and_value";
        var ct = TestContext.Current.CancellationToken;
        var (baselineClient, baselineProxy) = Client(() => Task.FromResult(response));
        await using var baseline = new RemoteDatabaseSession(baselineClient);
        object? expected = await MaterializedScalarAsync(baseline, sql, ct);
        var (client, proxy) = Client(() => Task.FromResult(response));
        await using var session = new RemoteDatabaseSession(client);
        object? actual = await session.ExecuteScalarAsync(sql, ct);
        Assert.Equal(expected?.GetType(), actual?.GetType());
        Assert.Equal(expected, actual);
        AssertSingleSqlDispatch(baselineProxy, sql, ct);
        AssertSingleSqlDispatch(proxy, sql, ct);
        return actual;
    }

    private static async Task<Exception> AssertMaterializedFailureAsync(
        SqlExecutionResult response, bool cancelAtResponse)
    {
        const string sql = "INSERT INTO exact_script VALUES (1); SELECT value FROM exact_script";
        Exception expected = await ExecuteAsync(materialize: true);
        Exception actual = await ExecuteAsync(materialize: false);
        Assert.Equal(expected.GetType(), actual.GetType());
        Assert.Equal(expected.Message, actual.Message);
        if (expected is ArgumentException argument)
            Assert.Equal(argument.ParamName, Assert.IsAssignableFrom<ArgumentException>(actual).ParamName);
        if (expected is ArgumentOutOfRangeException range)
            Assert.Equal(range.ActualValue, Assert.IsType<ArgumentOutOfRangeException>(actual).ActualValue);
        return actual;

        async Task<Exception> ExecuteAsync(bool materialize)
        {
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
            var (client, proxy) = Client(() =>
            {
                if (cancelAtResponse) cancellation.Cancel();
                return Task.FromResult(response);
            });
            await using var session = new RemoteDatabaseSession(client);
            Exception? error = await Record.ExceptionAsync(async () =>
            {
                if (materialize) await MaterializedScalarAsync(session, sql, cancellation.Token);
                else await session.ExecuteScalarAsync(sql, cancellation.Token);
            });
            Assert.NotNull(error);
            if (error is OperationCanceledException canceled)
                Assert.Equal(cancellation.Token, canceled.CancellationToken);
            AssertSingleSqlDispatch(proxy, sql, cancellation.Token);
            return error;
        }
    }

    private static void AssertSingleSqlDispatch(ScalarClientProxy proxy, string sql, CancellationToken token)
    {
        Dispatch dispatch = Assert.Single(proxy.Dispatches);
        Assert.Equal(nameof(ICSharpDbClient.ExecuteSqlAsync), dispatch.Method);
        Assert.Equal(sql, dispatch.Sql);
        Assert.Null(dispatch.TransactionId);
        Assert.Equal(token, dispatch.Token);
    }

    private static ClientColumn Column(ClientDbType physical, ClientSqlKind kind, string name = "value")
        => new() { Name = name, Type = physical, DeclaredType = new ClientSqlType { Kind = kind } };

    private static SqlExecutionResult Query(object?[] row, ClientColumn column)
        => new() { IsQuery = true, ColumnNames = [column.Name], Columns = [column], Rows = [row] };

    private static (ICSharpDbClient Client, ScalarClientProxy Proxy) Client(Func<Task<SqlExecutionResult>> response)
    {
        var client = DispatchProxy.Create<ICSharpDbClient, ScalarClientProxy>();
        var proxy = (ScalarClientProxy)client;
        proxy.Response = response;
        return (client, proxy);
    }

    private static async Task<object?> MaterializedScalarAsync(
        RemoteDatabaseSession session, string sql, CancellationToken cancellationToken)
    {
        await using QueryResult result = await session.ExecuteAsync(sql, cancellationToken);
        if (!result.IsQuery || result.Schema.Length == 0 || !await result.MoveNextAsync(cancellationToken))
            return null;
        var column = result.Schema[0];
        return column.DeclaredType is null
            ? TypeMapper.GetClrValue(result.Current[0])
            : TypeMapper.GetClrValue(result.Current[0], column.EffectiveType);
    }

    private sealed class ExplodingValue
    {
        public override string ToString() => throw new InvalidOperationException("unused cell conversion failed");
    }

    public sealed record Dispatch(string Method, string Sql, string? TransactionId, CancellationToken Token);

    public class ScalarClientProxy : DispatchProxy
    {
        public Func<Task<SqlExecutionResult>> Response { get; set; } = null!;
        public List<Dispatch> Dispatches { get; } = [];
        public int Begins { get; private set; }
        public int Commits { get; private set; }
        public int Rollbacks { get; private set; }
        public int Disposals { get; private set; }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            switch (targetMethod!.Name)
            {
                case nameof(ICSharpDbClient.ExecuteSqlAsync):
                    Dispatches.Add(new(targetMethod.Name, (string)args![0]!, null, (CancellationToken)args[1]!));
                    return Response();
                case nameof(ICSharpDbClient.ExecuteInTransactionAsync):
                    Dispatches.Add(new(targetMethod.Name, (string)args![1]!, (string)args[0]!, (CancellationToken)args[2]!));
                    return Response();
                case nameof(ICSharpDbClient.BeginTransactionAsync):
                    Begins++;
                    return Task.FromResult(new TransactionSessionInfo { TransactionId = "scalar-transaction", ExpiresAtUtc = DateTime.UtcNow.AddMinutes(1) });
                case nameof(ICSharpDbClient.CommitTransactionAsync):
                    Commits++;
                    return Task.CompletedTask;
                case nameof(ICSharpDbClient.RollbackTransactionAsync):
                    Rollbacks++;
                    return Task.CompletedTask;
                case nameof(ICSharpDbClient.DisposeAsync):
                    Disposals++;
                    return ValueTask.CompletedTask;
                default:
                    throw new NotSupportedException(targetMethod.Name);
            }
        }
    }
}
