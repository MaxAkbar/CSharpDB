using System.Reflection;
using CSharpDB.Client;
using CSharpDB.Client.Internal;
using CSharpDB.Client.Models;
using CSharpDB.Execution;
using ClientColumn = CSharpDB.Client.Models.ColumnDefinition;
using ClientDbType = CSharpDB.Client.Models.DbType;
using ClientSqlKind = CSharpDB.Client.Models.SqlTypeKind;
using ClientSqlType = CSharpDB.Client.Models.SqlTypeDescriptor;
using ScalarProxy = CSharpDB.Data.Tests.RemoteScalarProjectionUnitTests.ScalarClientProxy;
using SqlDispatch = CSharpDB.Data.Tests.RemoteScalarProjectionUnitTests.Dispatch;

namespace CSharpDB.Data.Tests;

public sealed class PrivateScalarProjectionTests
{
    private const string Sql = "INSERT INTO exact_script VALUES (1); SELECT value FROM exact_script";
    private static readonly CompactCase[] CompactCases = CreateCompactCases();

    public static IEnumerable<object[]> CompactCaseIndices
        => Enumerable.Range(0, CompactCases.Length).Select(index => new object[] { index });

    public static IEnumerable<object[]> ValidFacetCaseIndices
        => Enumerable.Range(0, 8).Select(index => new object[] { index });

    public static IEnumerable<object[]> InvalidFacetCaseIndices
        => Enumerable.Range(0, 16).Select(index => new object[] { index });

    public static IEnumerable<object[]> FallbackCaseIndices
        => Enumerable.Range(0, 10).Select(index => new object[] { index });

    [Theory]
    [MemberData(nameof(CompactCaseIndices))]
    public async Task CompactInteger_MatchesMaterializedAndLegacyRoutes(int shape)
    {
        CompactCase item = CompactCases[shape];
        var token = TestContext.Current.CancellationToken;
        Outcome expected = await OracleAsync(Query([item.Value], item.Type), token);
        foreach (bool pending in new[] { false, true })
        {
            Outcome actual = await ProjectedAsync(
                ScalarProjectionResult.FromCompactInt64(item.Value, Snapshot(item.Type)), token, pending);
            Equivalent(expected, actual, token);
        }
    }

    [Theory]
    [MemberData(nameof(ValidFacetCaseIndices))]
    public async Task ValidFacetBoundaries_RetainExistingClrConversions(int shape)
    {
        ClientSqlType type = shape switch
        {
            0 => Type(ClientSqlKind.Decimal, precision: 1, scale: 0),
            1 => Type(ClientSqlKind.Decimal, precision: 18, scale: 18),
            2 => Type(ClientSqlKind.Char, length: 1),
            3 => Type(ClientSqlKind.VarBinary, length: int.MaxValue),
            4 => Type(ClientSqlKind.Time, fractionalSecondsPrecision: 0),
            5 => Type(ClientSqlKind.Timestamp, fractionalSecondsPrecision: 7),
            6 => Type(ClientSqlKind.Bit, length: 1),
            _ => Type(ClientSqlKind.IntervalDayToSecond, fractionalSecondsPrecision: 7),
        };
        var token = TestContext.Current.CancellationToken;
        Outcome expected = await OracleAsync(Query([1L], type), token);
        // An incompatible CLR conversion may fail after valid facet metadata;
        // valid metadata must not acquire a new facet validation failure.
        Assert.False(expected.Error is ArgumentException);
        foreach (bool pending in new[] { false, true })
            Equivalent(expected, await ProjectedAsync(
                ScalarProjectionResult.FromCompactInt64(1, Snapshot(type)), token, pending), token);
    }

    [Theory]
    [MemberData(nameof(InvalidFacetCaseIndices))]
    public async Task InvalidFacetsAndKinds_WinBeforeCancellationAndClrOverflow(int shape)
    {
        ClientSqlType type = InvalidType(shape);
        foreach (bool canceled in new[] { false, true })
        {
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
            if (canceled) cancellation.Cancel();
            Outcome expected = await OracleAsync(Query([2147483648L], type), cancellation.Token);
            Assert.NotNull(expected.Error);
            Assert.False(expected.Error is OperationCanceledException or OverflowException);
            foreach (bool pending in new[] { false, true })
            {
                Outcome actual = await ProjectedAsync(
                    ScalarProjectionResult.FromCompactInt64(2147483648L, Snapshot(type)), cancellation.Token, pending);
                Equivalent(expected, actual, cancellation.Token);
            }
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CompletedReply_CancellationBoundaryStillPrecedesCheckedClrConversion(bool cancelAtResponse)
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        ClientSqlType type = Type(ClientSqlKind.Integer);
        Outcome actual = await ProjectedAsync(
            ScalarProjectionResult.FromCompactInt64(2147483648L, Snapshot(type)), cancellation.Token,
            pending: false, onResponse: () => { if (cancelAtResponse) cancellation.Cancel(); });
        Outcome expected = await OracleAsync(Query([2147483648L], type), cancellation.Token);
        Equivalent(expected, actual, cancellation.Token);
        if (cancelAtResponse)
            Assert.IsAssignableFrom<OperationCanceledException>(actual.Error);
        else
            Assert.IsType<OverflowException>(actual.Error);
    }

    [Theory]
    [MemberData(nameof(FallbackCaseIndices))]
    public async Task FullFallback_UsesTheAlreadyReceivedResponseWithoutReplay(int shape)
    {
        ClientSqlType integer = Type(ClientSqlKind.Integer);
        SqlExecutionResult response = shape switch
        {
            0 => Query([42L], integer),
            1 => new() { IsQuery = true, ColumnNames = ["value"], ColumnTypes = ["INTEGER"], Rows = [[42L]] },
            2 => Query(["text scalar"], Type(ClientSqlKind.Text), ClientDbType.Text),
            3 => new()
            {
                IsQuery = true, ColumnNames = ["value"], Columns = [Column(integer)],
                Rows = [[42L], [new ExplodingValue()]],
            },
            4 => new()
            {
                IsQuery = true, ColumnNames = ["value", "unused"],
                Columns = [Column(integer), new ClientColumn { Name = "unused", Type = ClientDbType.Text }],
                Rows = [[42L, new ExplodingValue()]],
            },
            5 => new()
            {
                IsQuery = true, ColumnNames = ["value"], Columns = [Column(InvalidType(0))],
                Rows = [[new ExplodingValue()]], Error = "SQL failed before projection",
            },
            6 => new() { IsQuery = false, RowsAffected = 1 },
            7 => new() { IsQuery = true, ColumnNames = [], Columns = [], Rows = [] },
            8 => new() { IsQuery = true, ColumnNames = ["value"], Columns = [Column(integer)], Rows = [] },
            // Null still requires declared facet validation, even when canceled.
            _ => Query([null], InvalidType(0)),
        };
        foreach (bool canceled in new[] { false, true })
        {
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
            if (canceled) cancellation.Cancel();
            Outcome expected = await OracleAsync(response, cancellation.Token);
            foreach (bool pending in new[] { false, true })
                Equivalent(expected, await ProjectedAsync(
                    ScalarProjectionResult.FromFallback(response), cancellation.Token, pending), cancellation.Token);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CapabilityFalse_SelectsTheExistingRouteBeforeDispatch(bool inTransaction)
    {
        var token = TestContext.Current.CancellationToken;
        var (client, proxy) = CapableClient();
        proxy.Supported = false;
        proxy.Response = () => Task.FromResult(Query([42L], Type(ClientSqlKind.Integer)));
        proxy.ProjectionResponse = () => throw new InvalidOperationException("Unsupported projection was dispatched.");
        await using var session = new RemoteDatabaseSession(client);
        if (inTransaction) await session.BeginTransactionAsync(token);
        Assert.Equal(42, Assert.IsType<int>(await session.ExecuteScalarAsync(Sql, token)));
        Assert.Empty(proxy.ProjectionDispatches);
        SqlDispatch dispatch = Assert.Single(proxy.Dispatches);
        Assert.Equal(inTransaction ? nameof(ICSharpDbClient.ExecuteInTransactionAsync) : nameof(ICSharpDbClient.ExecuteSqlAsync), dispatch.Method);
        Assert.Equal(inTransaction ? "scalar-transaction" : null, dispatch.TransactionId);
        Assert.Equal(Sql, dispatch.Sql);
        Assert.Equal(token, dispatch.Token);
        Assert.Equal(1, proxy.SupportChecks);
        if (inTransaction) await session.CommitAsync(token);
    }

    [Fact]
    public async Task Projection_ForwardsExactScriptTokenAndTransactionAcrossRentalLifetime()
    {
        var token = TestContext.Current.CancellationToken;
        var (client, proxy) = CapableClient();
        proxy.ProjectionResponse = () => Task.FromResult(
            ScalarProjectionResult.FromCompactInt64(42, Snapshot(Type(ClientSqlKind.Integer))));
        var session = new RemoteDatabaseSession(client);
        try
        {
            await session.BeginTransactionAsync(token);
            Assert.Equal(42, Assert.IsType<int>(await session.ExecuteScalarAsync(Sql, token)));
            await session.CommitAsync(token);
            Assert.Equal(42, Assert.IsType<int>(await session.ExecuteScalarAsync(Sql, token)));
            await session.BeginTransactionAsync(token);
            Assert.Equal(42, Assert.IsType<int>(await session.ExecuteScalarAsync(Sql, token)));
        }
        finally { await session.DisposeAsync(); }
        Assert.Empty(proxy.Dispatches);
        Assert.Equal(3, proxy.ProjectionDispatches.Count);
        Assert.Equal(new string?[] { "scalar-transaction", null, "scalar-transaction" },
            proxy.ProjectionDispatches.Select(dispatch => dispatch.TransactionId));
        Assert.All(proxy.ProjectionDispatches, dispatch =>
        {
            Assert.Equal(Sql, dispatch.Sql);
            Assert.Equal(token, dispatch.Token);
        });
        Assert.Equal(2, proxy.Begins);
        Assert.Equal(1, proxy.Commits);
        Assert.Equal(1, proxy.Rollbacks);
        Assert.Equal(1, proxy.Disposals);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task ProjectionFailures_PropagateWithoutPublicRedispatch(int shape)
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        if (shape == 3) cancellation.Cancel();
        var injected = new InvalidOperationException("The first SQL dispatch failed; it must not be replayed.");
        var pending = new TaskCompletionSource<ScalarProjectionResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var (client, proxy) = CapableClient();
        proxy.ProjectionResponse = shape switch
        {
            0 => () => throw injected,
            1 => () => Task.FromException<ScalarProjectionResult>(injected),
            2 => () => pending.Task,
            _ => () => Task.FromCanceled<ScalarProjectionResult>(cancellation.Token),
        };
        await using var session = new RemoteDatabaseSession(client);
        Task<object?> operation = session.ExecuteScalarAsync(Sql, cancellation.Token).AsTask();
        if (shape == 2)
        {
            Assert.False(operation.IsCompleted);
            pending.SetException(injected);
        }
        Outcome actual = await ObserveAsync(() => operation);
        if (shape == 3)
            Assert.Equal(cancellation.Token, Assert.IsAssignableFrom<OperationCanceledException>(actual.Error).CancellationToken);
        else
            Assert.Same(injected, actual.Error);
        AssertProjectionDispatch(proxy, cancellation.Token);
    }

    [Fact]
    public async Task InvalidProjectionEnvelope_FailsWithoutExecutingSqlAgain()
    {
        Outcome actual = await ProjectedAsync(default, TestContext.Current.CancellationToken, pending: false);
        Assert.Equal("The scalar projection did not return a result.", Assert.IsType<InvalidOperationException>(actual.Error).Message);
    }

    [Fact]
    public async Task ClosedSession_RejectsBeforeCapabilityOrSqlDispatch()
    {
        var (client, proxy) = CapableClient();
        var session = new RemoteDatabaseSession(client);
        await session.DisposeAsync();
        var error = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await session.ExecuteScalarAsync(Sql, TestContext.Current.CancellationToken);
        });
        Assert.Equal("Session is closed.", error.Message);
        Assert.Equal(0, proxy.SupportChecks);
        Assert.Empty(proxy.ProjectionDispatches);
        Assert.Empty(proxy.Dispatches);
        Assert.Equal(1, proxy.Disposals);
    }

    [Fact]
    public async Task DataScalar_RepeatedMixedCompletions_DoNotRetainPriorValueFailureOrCancellation()
    {
        var (client, proxy) = CapableClient();
        await using var session = new RemoteDatabaseSession(client);
        var retained = new List<(DataWork Work, Outcome Actual)>();
        try
        {
            for (int index = 0; index < 96; index++)
            {
                DataWork work = await CreateDataWorkAsync(index);
                retained.Add((work, null!));
                bool pending = (index / 12) % 2 != 0;
                proxy.ProjectionResponse = () => pending ? work.Completion.Task : work.CompletedTask();
                ValueTask<object?> operation = session.ExecuteScalarAsync(work.Sql, work.Token);
                if (pending)
                {
                    Assert.False(operation.IsCompleted);
                    work.Complete();
                }
                // The Data ValueTask is consumed exactly once, including when it faults.
                Outcome actual = await ObserveValueTaskAsync(operation);
                retained[^1] = (work, actual);
                AssertDataWork(work, actual);
                AssertDataDispatch(proxy, index, work);
            }
            foreach (var item in retained) AssertDataWork(item.Work, item.Actual);
            Assert.Equal(96, proxy.SupportChecks);
            Assert.Equal(96, proxy.ProjectionDispatches.Count);
            Assert.Empty(proxy.Dispatches);
        }
        finally
        {
            foreach (var item in retained) item.Work.Dispose();
        }
    }

    [Fact]
    public async Task DataScalar_ConcurrentMixedCompletions_KeepIndependentResultsFailuresAndTokens()
    {
        var (client, proxy) = CapableClient();
        await using var session = new RemoteDatabaseSession(client);
        var retained = new List<(DataWork Work, Outcome Actual)>();
        int dispatchIndex = 0;
        for (int batch = 0; batch < 8; batch++)
        {
            var works = new List<DataWork>();
            var operations = new List<Task<Outcome>>();
            try
            {
                for (int lane = 0; lane < 12; lane++)
                {
                    DataWork work = await CreateDataWorkAsync(batch * 12 + lane);
                    works.Add(work);
                    proxy.ProjectionResponse = () => work.Completion.Task;
                    ValueTask<object?> operation = session.ExecuteScalarAsync(work.Sql, work.Token);
                    Assert.False(operation.IsCompleted);
                    operations.Add(ObserveValueTaskAsync(operation));
                    AssertDataDispatch(proxy, dispatchIndex++, work);
                }
                Assert.All(operations, operation => Assert.False(operation.IsCompleted));
                // Alternate completion order so adjacent pooled state cannot stand in for a lane.
                foreach (DataWork work in batch % 2 == 0 ? works.AsEnumerable().Reverse() : works)
                    work.Complete();
                Outcome[] actual = await Task.WhenAll(operations);
                for (int lane = 0; lane < works.Count; lane++)
                {
                    AssertDataWork(works[lane], actual[lane]);
                    retained.Add((works[lane], actual[lane]));
                }
                foreach (var item in retained) AssertDataWork(item.Work, item.Actual);
            }
            finally
            {
                // Even an assertion failure releases every deliberately suspended call.
                foreach (DataWork work in works) work.Complete();
                await Task.WhenAll(operations);
                foreach (DataWork work in works) work.Dispose();
            }
        }
        Assert.Equal(96, proxy.SupportChecks);
        Assert.Equal(96, proxy.ProjectionDispatches.Count);
        Assert.Empty(proxy.Dispatches);
    }

    private static async Task<DataWork> CreateDataWorkAsync(int index)
    {
        var work = new DataWork(index);
        int shape = index % 12;
        ClientSqlType type = Type(ClientSqlKind.Integer);
        long value = index + 17L;
        SqlExecutionResult response;
        switch (shape)
        {
            case 0:
                response = Query([value], type);
                work.Response = ScalarProjectionResult.FromCompactInt64(value, Snapshot(type));
                break;
            case 1:
                value = long.MaxValue - index;
                type = Type(ClientSqlKind.BigInt);
                response = Query([value], type);
                work.Response = ScalarProjectionResult.FromCompactInt64(value, Snapshot(type));
                break;
            case 2:
                response = Query([$"retained text {index}"], Type(ClientSqlKind.Text), ClientDbType.Text);
                work.Response = ScalarProjectionResult.FromFallback(response);
                break;
            case 3:
                work.Cancellation.Cancel();
                response = new() { IsQuery = true, ColumnNames = ["value"], Columns = [Column(InvalidType(0))],
                    Rows = [[new ExplodingValue()]], Error = $"SQL failure {index} precedes conversion and cancellation" };
                work.Response = ScalarProjectionResult.FromFallback(response);
                break;
            case 4:
                work.Cancellation.Cancel();
                response = Query([new ExplodingValue()], InvalidType(0));
                work.Response = ScalarProjectionResult.FromFallback(response);
                break;
            case 5:
                work.Cancellation.Cancel();
                type = InvalidType(index % 16);
                response = Query([2147483648L], type);
                work.Response = ScalarProjectionResult.FromCompactInt64(2147483648L, Snapshot(type));
                break;
            case 6:
                work.Cancellation.Cancel();
                response = Query([2147483648L], type);
                work.Response = ScalarProjectionResult.FromCompactInt64(2147483648L, Snapshot(type));
                break;
            case 7:
                response = Query([2147483648L], type);
                work.Response = ScalarProjectionResult.FromCompactInt64(2147483648L, Snapshot(type));
                break;
            case 8:
                work.Failure = new InvalidOperationException($"Independent transport failure {index}");
                work.Expected = new(null, work.Failure);
                return work;
            case 9:
                work.Cancellation.Cancel();
                response = Query([null], InvalidType(0));
                work.Response = ScalarProjectionResult.FromFallback(response);
                break;
            case 10:
                response = new() { IsQuery = true, ColumnNames = ["value", "unused"],
                    Columns = [Column(type), new ClientColumn { Name = "unused", Type = ClientDbType.Text }],
                    Rows = [[value, new ExplodingValue()]] };
                work.Response = ScalarProjectionResult.FromFallback(response);
                break;
            default:
                value = long.MinValue + index;
                response = Query([value], null);
                work.Response = ScalarProjectionResult.FromCompactInt64(value, null);
                break;
        }
        work.Expected = await OracleAsync(response, work.Token);
        return work;
    }

    private static async Task<Outcome> ObserveValueTaskAsync(ValueTask<object?> operation)
    {
        try { return new(await operation, null); }
        catch (Exception error) { return new(null, error); }
    }

    private static void AssertDataWork(DataWork work, Outcome actual)
    {
        Equivalent(work.Expected, actual, work.Token);
        if (work.Failure is not null) Assert.Same(work.Failure, actual.Error);
    }

    private static void AssertDataDispatch(ProjectionProxy proxy, int index, DataWork work)
    {
        SqlDispatch dispatch = proxy.ProjectionDispatches[index];
        Assert.Equal(nameof(IScalarProjectionClient.ExecuteScalarProjectionAsync), dispatch.Method);
        Assert.Equal(work.Sql, dispatch.Sql);
        Assert.Null(dispatch.TransactionId);
        Assert.Equal(work.Token, dispatch.Token);
        Assert.Empty(proxy.Dispatches);
    }

    private sealed class DataWork : IDisposable
    {
        internal DataWork(int index)
        {
            Sql = $"{PrivateScalarProjectionTests.Sql}; -- reuse-{index}";
            Cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
            Token = Cancellation.Token;
        }
        internal readonly string Sql;
        internal readonly CancellationTokenSource Cancellation;
        internal readonly TaskCompletionSource<ScalarProjectionResult> Completion =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal CancellationToken Token { get; }
        internal ScalarProjectionResult Response;
        internal Exception? Failure;
        internal Outcome Expected = null!;
        internal Task<ScalarProjectionResult> CompletedTask() => Failure is null
            ? Task.FromResult(Response) : Task.FromException<ScalarProjectionResult>(Failure);
        internal void Complete()
        {
            if (Failure is not null) Completion.TrySetException(Failure);
            else Completion.TrySetResult(Response);
        }
        public void Dispose() => Cancellation.Dispose();
    }

    private static CompactCase[] CreateCompactCases()
    {
        var cases = Enum.GetValues<ClientSqlKind>()
            .Select(kind => new CompactCase(42L, Type(kind))).ToList();
        cases.Insert(0, new(42L, null));
        cases.AddRange([
            new(long.MinValue, null), new(long.MaxValue, null),
            new(0, Type(ClientSqlKind.Boolean)),
            new(0, Type(ClientSqlKind.TinyInt)), new(byte.MaxValue, Type(ClientSqlKind.TinyInt)),
            new(-1, Type(ClientSqlKind.TinyInt)), new((long)byte.MaxValue + 1, Type(ClientSqlKind.TinyInt)),
            new(short.MinValue, Type(ClientSqlKind.SmallInt)), new(short.MaxValue, Type(ClientSqlKind.SmallInt)),
            new((long)short.MinValue - 1, Type(ClientSqlKind.SmallInt)), new((long)short.MaxValue + 1, Type(ClientSqlKind.SmallInt)),
            new(int.MinValue, Type(ClientSqlKind.Integer)), new(int.MaxValue, Type(ClientSqlKind.Integer)),
            new((long)int.MinValue - 1, Type(ClientSqlKind.Integer)), new((long)int.MaxValue + 1, Type(ClientSqlKind.Integer)),
            new(long.MinValue, Type(ClientSqlKind.BigInt)), new(long.MaxValue, Type(ClientSqlKind.BigInt)),
        ]);
        return cases.ToArray();
    }

    private static ClientSqlType InvalidType(int shape) => shape switch
    {
        0 => Type(ClientSqlKind.Integer, length: 1),
        1 => Type(ClientSqlKind.VarChar, length: 0),
        2 => Type(ClientSqlKind.VarBinary, length: -1),
        3 => Type(ClientSqlKind.Integer, precision: 1),
        4 => Type(ClientSqlKind.Integer, scale: 0),
        5 => Type(ClientSqlKind.Decimal, precision: 0),
        6 => Type(ClientSqlKind.Decimal, precision: 19),
        7 => Type(ClientSqlKind.Decimal, scale: 0),
        8 => Type(ClientSqlKind.Decimal, precision: 3, scale: -1),
        9 => Type(ClientSqlKind.Decimal, precision: 3, scale: 4),
        10 => Type(ClientSqlKind.Integer, fractionalSecondsPrecision: 0),
        11 => Type(ClientSqlKind.Time, fractionalSecondsPrecision: -1),
        12 => Type(ClientSqlKind.Timestamp, fractionalSecondsPrecision: 8),
        13 => Type((ClientSqlKind)255),
        14 => Type(ClientSqlKind.VarChar, length: 0, precision: 1),
        _ => Type(ClientSqlKind.Decimal, length: 1, precision: 0),
    };

    private static ClientSqlType Type(ClientSqlKind kind, int? length = null, int? precision = null,
        int? scale = null, int? fractionalSecondsPrecision = null)
        => new() { Kind = kind, Length = length, Precision = precision, Scale = scale,
            FractionalSecondsPrecision = fractionalSecondsPrecision };

    private static ScalarTypeSnapshot? Snapshot(ClientSqlType? type)
        => type is null ? null : new(type.Kind, type.Length, type.Precision, type.Scale, type.FractionalSecondsPrecision);

    private static ClientColumn Column(ClientSqlType? type, ClientDbType physical = ClientDbType.Integer)
        => new() { Name = "value", Type = physical, DeclaredType = type };

    private static SqlExecutionResult Query(object?[] row, ClientSqlType? type, ClientDbType physical = ClientDbType.Integer)
        => new() { IsQuery = true, ColumnNames = ["value"], Columns = [Column(type, physical)], Rows = [row] };

    private static async Task<Outcome> OracleAsync(SqlExecutionResult response, CancellationToken token)
    {
        Outcome materialized = await LegacyAsync(response, token, materialize: true);
        Outcome legacy = await LegacyAsync(response, token, materialize: false);
        Equivalent(materialized, legacy, token);
        return materialized;
    }

    private static async Task<Outcome> LegacyAsync(SqlExecutionResult response, CancellationToken token, bool materialize)
    {
        ICSharpDbClient client = DispatchProxy.Create<ICSharpDbClient, ScalarProxy>();
        var proxy = (ScalarProxy)client;
        proxy.Response = () => Task.FromResult(response);
        await using var session = new RemoteDatabaseSession(client);
        Outcome outcome = await ObserveAsync(async () =>
        {
            if (!materialize) return await session.ExecuteScalarAsync(Sql, token);
            await using QueryResult result = await session.ExecuteAsync(Sql, token);
            if (!result.IsQuery || result.Schema.Length == 0 || !await result.MoveNextAsync(token)) return null;
            var column = result.Schema[0];
            return column.DeclaredType is null
                ? TypeMapper.GetClrValue(result.Current[0])
                : TypeMapper.GetClrValue(result.Current[0], column.EffectiveType);
        });
        SqlDispatch dispatch = Assert.Single(proxy.Dispatches);
        Assert.Equal(nameof(ICSharpDbClient.ExecuteSqlAsync), dispatch.Method);
        Assert.Equal(Sql, dispatch.Sql);
        Assert.Null(dispatch.TransactionId);
        Assert.Equal(token, dispatch.Token);
        return outcome;
    }

    private static async Task<Outcome> ProjectedAsync(ScalarProjectionResult response, CancellationToken token,
        bool pending, Action? onResponse = null)
    {
        var (client, proxy) = CapableClient();
        var completion = new TaskCompletionSource<ScalarProjectionResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        proxy.ProjectionResponse = () =>
        {
            Task<ScalarProjectionResult> task = pending ? completion.Task : Task.FromResult(response);
            onResponse?.Invoke(); // A completed reply exists before cancellation is observed by Data.
            return task;
        };
        await using var session = new RemoteDatabaseSession(client);
        Task<object?> operation = session.ExecuteScalarAsync(Sql, token).AsTask();
        if (pending)
        {
            Assert.False(operation.IsCompleted);
            completion.SetResult(response);
        }
        Outcome outcome = await ObserveAsync(() => operation);
        AssertProjectionDispatch(proxy, token);
        return outcome;
    }

    private static void AssertProjectionDispatch(ProjectionProxy proxy, CancellationToken token)
    {
        Assert.Empty(proxy.Dispatches);
        SqlDispatch dispatch = Assert.Single(proxy.ProjectionDispatches);
        Assert.Equal(nameof(IScalarProjectionClient.ExecuteScalarProjectionAsync), dispatch.Method);
        Assert.Equal(Sql, dispatch.Sql);
        Assert.Null(dispatch.TransactionId);
        Assert.Equal(token, dispatch.Token);
        Assert.Equal(1, proxy.SupportChecks);
    }

    private static async Task<Outcome> ObserveAsync(Func<Task<object?>> operation)
    {
        try { return new(await operation(), null); }
        catch (Exception error) { return new(null, error); }
    }

    private static void Equivalent(Outcome expected, Outcome actual, CancellationToken token)
    {
        if (expected.Error is not { } expectedError)
        {
            Assert.Null(actual.Error);
            Assert.Equal(expected.Value?.GetType(), actual.Value?.GetType());
            Assert.Equal(expected.Value, actual.Value);
            return;
        }
        Exception error = Assert.IsAssignableFrom<Exception>(actual.Error);
        Assert.Equal(expectedError.GetType(), error.GetType());
        Assert.Equal(expectedError.Message, error.Message);
        if (expectedError is ArgumentException expectedArgument)
            Assert.Equal(expectedArgument.ParamName, Assert.IsAssignableFrom<ArgumentException>(error).ParamName);
        if (error is OperationCanceledException canceled)
            Assert.Equal(token, canceled.CancellationToken);
    }

    private static (ICSharpDbClient Client, ProjectionProxy Proxy) CapableClient()
    {
        ICSharpDbClient client = DispatchProxy.Create<ICSharpDbClient, ProjectionProxy>();
        var proxy = (ProjectionProxy)client;
        proxy.Response = () => throw new InvalidOperationException("SQL was replayed through the public client.");
        return (client, proxy);
    }

    private sealed record CompactCase(long Value, ClientSqlType? Type);
    private sealed record Outcome(object? Value, Exception? Error);
    private sealed class ExplodingValue
    {
        public override string ToString() => throw new InvalidOperationException("Unused fallback cell was converted.");
    }

    public class ProjectionProxy : ScalarProxy, IScalarProjectionClient
    {
        internal bool Supported { get; set; } = true;
        internal int SupportChecks { get; private set; }
        internal Func<Task<ScalarProjectionResult>> ProjectionResponse { get; set; } = null!;
        internal List<SqlDispatch> ProjectionDispatches { get; } = [];
        bool IScalarProjectionClient.SupportsScalarProjection
        {
            get { SupportChecks++; return Supported; }
        }
        ValueTask<ScalarProjectionResult> IScalarProjectionClient.ExecuteScalarProjectionAsync(
            string sql, string? transactionId, CancellationToken cancellationToken)
        {
            ProjectionDispatches.Add(new(nameof(IScalarProjectionClient.ExecuteScalarProjectionAsync), sql, transactionId, cancellationToken));
            return new ValueTask<ScalarProjectionResult>(ProjectionResponse());
        }
    }
}
