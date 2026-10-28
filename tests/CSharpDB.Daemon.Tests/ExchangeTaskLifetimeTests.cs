using CSharpDB.Client;
using CSharpDB.Client.Grpc;
using CSharpDB.Client.Internal;
using CSharpDB.Client.Models;
using CSharpDB.Testing;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace CSharpDB.Daemon.Tests;

public sealed class ExchangeTaskLifetimeTests
{
    [Fact(Timeout = 30000)]
    public async Task PublicSqlInfoAndTransactionTasks_KeepTheirResultsAcrossExchangesAndRentalReturn()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var host = new GrpcSocketDaemonFixture();
        var options = Options(host);
        try
        {
            Task<SqlExecutionResult> sql;
            Task<DatabaseInfo> info;
            Task<TransactionSessionInfo> transaction;
            SqlExecutionResult sqlResult;
            DatabaseInfo infoResult;
            TransactionSessionInfo transactionResult;
            await using (var first = await CSharpDbClient.RentAsync(options, 1, ct))
            {
                sql = first.ExecuteSqlAsync("SELECT 111", ct);
                sqlResult = await sql;
                info = first.GetInfoAsync(ct);
                infoResult = await info;
                transaction = first.BeginTransactionAsync(ct);
                transactionResult = await transaction;
                await first.RollbackTransactionAsync(transactionResult.TransactionId, ct);
                for (int index = 0; index < 32; index++)
                    Assert.Equal((long)index, (await first.ExecuteSqlAsync($"SELECT {index}", ct)).Rows![0][0]);
            }
            await using (var next = await CSharpDbClient.RentAsync(options, 1, ct))
            {
                for (int index = 32; index < 64; index++)
                    Assert.Equal((long)index, (await next.ExecuteSqlAsync($"SELECT {index}", ct)).Rows![0][0]);
            }
            Assert.Same(sqlResult, await sql);
            Assert.Equal(111L, sqlResult.Rows![0][0]);
            Assert.Same(infoResult, await info);
            Assert.Same(transactionResult, await transaction);
            Assert.False(string.IsNullOrEmpty(transactionResult.TransactionId));
        }
        finally { await CSharpDbClient.ClearPoolAsync(options.Endpoint!); }
    }

    [Theory(Timeout = 30000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PublicSqlFailureTask_KeepsItsErrorOrCancellationAfterLaterRental(bool cancel)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var host = new GrpcSocketDaemonFixture();
        var options = Options(host);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(ct);
        try
        {
            Task<SqlExecutionResult> retained;
            SqlExecutionResult? failedResult = null;
            string? sqlError = null;
            await using (var first = await CSharpDbClient.RentAsync(options, 1, ct))
            {
                if (cancel)
                {
                    cancellation.Cancel();
                    retained = first.ExecuteSqlAsync("SELECT 111", cancellation.Token);
                    var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => retained);
                    Assert.Equal(cancellation.Token, error.CancellationToken);
                }
                else
                {
                    retained = first.ExecuteSqlAsync("SELECT * FROM exchange_lifetime_missing_table", ct);
                    failedResult = await retained;
                    sqlError = failedResult.Error;
                    Assert.False(string.IsNullOrEmpty(sqlError));
                    Assert.Equal(17L, (await first.ExecuteSqlAsync("SELECT 17", ct)).Rows![0][0]);
                }
            }
            await using (var next = await CSharpDbClient.RentAsync(options, 1, ct))
            {
                for (int index = 0; index < 32; index++)
                    Assert.Equal((long)index, (await next.ExecuteSqlAsync($"SELECT {index}", ct)).Rows![0][0]);
            }
            if (cancel)
            {
                var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => retained);
                Assert.Equal(cancellation.Token, error.CancellationToken);
                Assert.True(retained.IsCanceled);
            }
            else
            {
                Assert.Same(failedResult, await retained);
                Assert.Equal(sqlError, (await retained).Error);
            }
        }
        finally { await CSharpDbClient.ClearPoolAsync(options.Endpoint!); }
    }

    [Theory(Timeout = 30000)]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task TaskConsumers_KeepMixedCompletionsAcrossSequentialSessionReuse(int consumer)
    {
        var retained = new List<Observation>();
        for (int index = 0; index < 48; index++)
        {
            TestContext.Current.CancellationToken.ThrowIfCancellationRequested();
            bool hold = index / 4 % 2 == 1;
            await using var work = new Work(consumer, index, (Outcome)(index % 4), hold);
            Assert.Equal(!hold, work.Operation.IsCompleted);
            if (hold) work.Peer.Complete();
            retained.Add(await ObserveAsync(work));
            if (index % 8 == 7) await CheckRetainedAsync(retained);
        }
        await CheckRetainedAsync(retained);
    }

    [Theory(Timeout = 30000)]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task TaskConsumers_KeepIndependentMixedCompletionsAcrossConcurrentSessions(int consumer)
    {
        var retained = new List<Observation>();
        for (int batch = 0; batch < 6; batch++)
        {
            Work[] work = Enumerable.Range(0, 12)
                .Select(lane => new Work(consumer, 1000 + batch * 12 + lane, (Outcome)(lane % 4), hold: true)).ToArray();
            try
            {
                Assert.All(work, item => Assert.False(item.Operation.IsCompleted));
                Task<Observation>[] observations = work.Select(ObserveAsync).ToArray();
                foreach (int lane in batch % 2 == 0 ? Enumerable.Range(0, 12).Reverse() : Enumerable.Range(0, 12))
                    work[lane].Peer.Complete();
                retained.AddRange(await Task.WhenAll(observations)
                    .WaitAsync(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken));
            }
            finally
            {
                foreach (Work item in work) await item.DisposeAsync();
            }
            await CheckRetainedAsync(retained);
        }
    }

    private static CSharpDbClientOptions Options(GrpcSocketDaemonFixture host) => new()
    {
        Transport = CSharpDbTransport.Grpc, GrpcMode = CSharpDbGrpcMode.Native,
        GrpcCallMode = CSharpDbGrpcCallMode.Streaming, Endpoint = host.NativeEndpoint.AbsoluteUri,
    };

    private static async Task<Observation> ObserveAsync(Work work)
    {
        object? result = null;
        RpcException? failure = null;
        CancellationToken? canceledToken = null;
        switch (work.Outcome)
        {
            case Outcome.Success:
                result = await ReadResultAsync(work);
                if (result is SqlExecutionResultMessage sql) Assert.Equal(work.Index, sql.RowsAffected);
                else Assert.Equal(work.Peer.Owner, Assert.IsType<DatabaseInfoMessage>(result).DataSource);
                break;
            case Outcome.WireError:
            case Outcome.TransportError:
                failure = await Assert.ThrowsAsync<RpcException>(() => work.Operation);
                Assert.Equal(work.Outcome == Outcome.WireError ? StatusCode.InvalidArgument : StatusCode.Internal, failure.StatusCode);
                Assert.Equal(work.Peer.Owner, failure.Status.Detail);
                Assert.Equal(work.Peer.Owner, failure.Trailers.GetValue("x-owner"));
                break;
            case Outcome.Canceled:
                var canceled = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => work.Operation);
                canceledToken = canceled.CancellationToken;
                Assert.True(canceledToken.Value.IsCancellationRequested);
                if (work.Consumer != 2) Assert.Equal(work.Token, canceledToken.Value);
                break;
        }
        Assert.Equal(1, work.Peer.CommandWrites);
        Metadata? headers = work.Headers is null ? null : await work.Headers;
        if (headers is not null) Assert.Equal(work.Peer.Owner, headers.GetValue("x-owner"));
        var observed = new Observation(work, result, failure, canceledToken, headers);
        CheckUnaryCompletion(observed);
        return observed;
    }

    private static async Task CheckRetainedAsync(IEnumerable<Observation> retained)
    {
        foreach (Observation item in retained)
        {
            if (item.Failure is not null)
                Assert.Same(item.Failure, await Assert.ThrowsAsync<RpcException>(() => item.Work.Operation));
            else if (item.CanceledToken is not null)
            {
                var error = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => item.Work.Operation);
                Assert.Equal(item.CanceledToken.Value, error.CancellationToken);
                Assert.True(item.Work.Operation.IsCanceled);
            }
            else Assert.Same(item.Result, await ReadResultAsync(item.Work));
            if (item.Work.Headers is not null) Assert.Same(item.Headers, await item.Work.Headers);
            CheckUnaryCompletion(item);
        }
    }

    private static void CheckUnaryCompletion(Observation item)
    {
        if (item.Work.Unary is not { } call) return;
        Assert.Same(item.Work.Operation, call.ResponseAsync);
        StatusCode expected = item.Work.Outcome switch
        {
            Outcome.Success => StatusCode.OK,
            Outcome.WireError => StatusCode.InvalidArgument,
            Outcome.TransportError => StatusCode.Internal,
            _ => StatusCode.Cancelled,
        };
        Assert.Equal(expected, call.GetStatus().StatusCode);
        if (item.Failure is not null) Assert.Equal(item.Work.Peer.Owner, call.GetTrailers().GetValue("x-owner"));
        else Assert.Empty(call.GetTrailers());
    }

    private static async Task<object> ReadResultAsync(Work work)
        => work.Consumer == 0
            ? (await (Task<GrpcCommandSession.SqlReply>)work.Operation).Response
            : await (Task<DatabaseInfoMessage>)work.Operation;

    private enum Outcome { Success, WireError, TransportError, Canceled }
    private sealed record Observation(Work Work, object? Result, RpcException? Failure,
        CancellationToken? CanceledToken, Metadata? Headers);

    private sealed class Work : IAsyncDisposable
    {
        private readonly CancellationTokenSource _cancellation =
            CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        private readonly GrpcCommandSession _session;
        public int Consumer { get; }
        public int Index { get; }
        public Outcome Outcome { get; }
        public CancellationToken Token { get; }
        public Peer Peer { get; }
        public Task Operation { get; }
        public AsyncUnaryCall<DatabaseInfoMessage>? Unary { get; }
        public Task<Metadata>? Headers { get; }
        public Work(int consumer, int index, Outcome outcome, bool hold)
        {
            Consumer = consumer; Index = index; Outcome = outcome; Token = _cancellation.Token;
            Peer = new Peer(index, outcome, hold, _cancellation);
            var invoker = new ScriptedInvoker(Peer);
            _session = new GrpcCommandSession(invoker);
            if (consumer == 0)
                Operation = _session.ExecuteSqlAsync(new() { ExecuteSql = new() { Sql = "SELECT ignored" } }, Token);
            else if (consumer == 1)
                Operation = _session.ExecuteAsync<DatabaseInfoMessage>(new() { GetInfo = new Empty() }, Token);
            else
            {
                var client = new CSharpDbRpc.CSharpDbRpcClient(invoker.Intercept(_session));
                Unary = client.GetInfoAsync(new Empty(), cancellationToken: Token);
                Operation = Unary.ResponseAsync;
                Headers = Unary.ResponseHeadersAsync;
            }
        }
        public async ValueTask DisposeAsync()
        {
            _cancellation.Cancel();
            Peer.Abort();
            await _session.DisposeAsync();
            Unary?.Dispose();
            _cancellation.Dispose();
        }
    }

    private sealed class Peer(int index, Outcome outcome, bool hold, CancellationTokenSource cancellation)
    {
        private readonly TaskCompletionSource<CommandSessionResponse> _reply = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private CommandSessionRequest? _command;
        private bool _helloPending;
        public string Owner { get; } = "exchange-owner-" + index;
        public int CommandWrites { get; private set; }
        public bool Completed { get; private set; }
        public void Write(CommandSessionRequest request)
        {
            if (request.Hello is not null) { _helloPending = true; return; }
            Assert.Null(_command);
            _command = request.Clone(); CommandWrites++;
            if (!hold) Complete();
        }
        public void Complete()
        {
            Assert.NotNull(_command);
            if (outcome == Outcome.Canceled)
            {
                cancellation.Cancel();
                _reply.TrySetCanceled(cancellation.Token);
            }
            else if (outcome == Outcome.TransportError)
                _reply.TrySetException(new RpcException(new Status(StatusCode.Internal, Owner), new Metadata { { "x-owner", Owner } }));
            else
            {
                var response = new CommandSessionResponse { RequestId = _command.RequestId };
                if (outcome == Outcome.WireError)
                {
                    response.Error = new() { StatusCode = (int)StatusCode.InvalidArgument, Detail = Owner };
                    response.Error.Trailers.Add(new CommandSessionMetadata { Key = "x-owner", Text = Owner });
                }
                else if (_command.ExecuteSql is not null)
                    response.Result = new() { IsQuery = false, RowsAffected = index, Elapsed = new Duration() };
                else response.Info = new() { DataSource = Owner, TableCount = index };
                _reply.TrySetResult(response);
            }
        }
        public Task<CommandSessionResponse?> ReadAsync(CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            if (_helloPending)
            {
                _helloPending = false;
                return Task.FromResult<CommandSessionResponse?>(new() { Ready = new()
                    { ProtocolVersion = 1, MaxInFlight = 1, Capabilities = { "sql", "transactions" } } });
            }
            if (Completed) return Task.FromResult<CommandSessionResponse?>(null);
            return WaitReplyAsync(ct);
        }
        private async Task<CommandSessionResponse?> WaitReplyAsync(CancellationToken ct)
            => await _reply.Task.WaitAsync(ct).ConfigureAwait(false);
        public void Finish() => Completed = true;
        public void Abort() => _reply.TrySetCanceled();
    }

    private sealed class ScriptedInvoker(Peer peer) : CallInvoker
    {
        public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options)
            => new((IClientStreamWriter<TRequest>)(object)new Writer(peer),
                (IAsyncStreamReader<TResponse>)(object)new Reader(peer),
                Task.FromResult(new Metadata { { "x-owner", peer.Owner } }),
                () => Status.DefaultSuccess, () => new Metadata(), () => { });
        public override TResponse BlockingUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) => throw new NotSupportedException();
        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) => throw new NotSupportedException();
        public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) => throw new NotSupportedException();
        public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options) => throw new NotSupportedException();
        private sealed class Writer(Peer state) : IClientStreamWriter<CommandSessionRequest>
        {
            public WriteOptions? WriteOptions { get; set; }
            public Task WriteAsync(CommandSessionRequest message) { state.Write(message); return Task.CompletedTask; }
            public Task WriteAsync(CommandSessionRequest message, CancellationToken ct) { ct.ThrowIfCancellationRequested(); return WriteAsync(message); }
            public Task CompleteAsync() { state.Finish(); return Task.CompletedTask; }
        }
        private sealed class Reader(Peer state) : IAsyncStreamReader<CommandSessionResponse>
        {
            public CommandSessionResponse Current { get; private set; } = null!;
            public async Task<bool> MoveNext(CancellationToken ct)
            {
                var response = await state.ReadAsync(ct).ConfigureAwait(false);
                if (response is null) return false;
                Current = response;
                return true;
            }
        }
    }
}
