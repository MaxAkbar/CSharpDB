using CSharpDB.Client.Grpc;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace CSharpDB.Client.Internal;

/// <summary>Guards every RPC on one rental and resets its transaction state before returning the connection.</summary>
internal sealed class GrpcClientLease(GrpcTransportClient owner, Func<GrpcTransportClient, bool, ValueTask> release) : Interceptor
{
    private readonly object _gate = new();
    private HashSet<string>? _transactions;
    private HashSet<string>? _rollbackAttempts;
    private int _activeCalls;
    private bool _healthy = true;
    private TaskCompletionSource? _drained;
    private Task? _disposeTask;

    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        TRequest request, ClientInterceptorContext<TRequest, TResponse> context,
        AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
    {
        string? transactionId = request switch
        {
            TransactionSqlRequest sql => sql.TransactionId,
            TransactionIdRequest transaction => transaction.TransactionId,
            _ => null,
        };
        string method = context.Method.Name;
        Begin(method, transactionId);
        AsyncUnaryCall<TResponse> call;
        try { call = continuation(request, context); }
        catch { Complete(method, transactionId, null, failed: true); throw; }
        return new AsyncUnaryCall<TResponse>(
            ObserveAsync(call.ResponseAsync, method, transactionId),
            static state => ((AsyncUnaryCall<TResponse>)state).ResponseHeadersAsync,
            static state => ((AsyncUnaryCall<TResponse>)state).GetStatus(),
            static state => ((AsyncUnaryCall<TResponse>)state).GetTrailers(),
            static state => ((AsyncUnaryCall<TResponse>)state).Dispose(), call);
    }

    internal Task<TResponse> ExecuteAsync<TResponse>(
        GrpcCommandSession session, CommandSessionRequest command, CancellationToken ct)
        where TResponse : class
    {
        string method = command.CommandCase switch
        {
            CommandSessionRequest.CommandOneofCase.GetInfo => "GetInfo",
            CommandSessionRequest.CommandOneofCase.ExecuteSql => "ExecuteSql",
            CommandSessionRequest.CommandOneofCase.BeginTransaction => "BeginTransaction",
            CommandSessionRequest.CommandOneofCase.ExecuteInTransaction => "ExecuteInTransaction",
            CommandSessionRequest.CommandOneofCase.CommitTransaction => "CommitTransaction",
            CommandSessionRequest.CommandOneofCase.RollbackTransaction => "RollbackTransaction",
            _ => throw new ArgumentException("The command is not supported by a client rental.", nameof(command)),
        };
        string? transactionId = command.CommandCase switch
        {
            CommandSessionRequest.CommandOneofCase.ExecuteInTransaction => command.ExecuteInTransaction.TransactionId,
            CommandSessionRequest.CommandOneofCase.CommitTransaction => command.CommitTransaction.TransactionId,
            CommandSessionRequest.CommandOneofCase.RollbackTransaction => command.RollbackTransaction.TransactionId,
            _ => null,
        };
        Begin(method, transactionId);
        Task<TResponse> response;
        try { response = session.ExecuteAsync<TResponse>(command, ct); }
        catch { Complete(method, transactionId, null, failed: true); throw; }
        return ObserveAsync(response, method, transactionId);
    }

    internal Task<GrpcCommandSession.SqlReply> ExecuteSqlAsync(
        GrpcCommandSession session, CommandSessionRequest command, CancellationToken ct)
    {
        string method = command.CommandCase switch
        {
            CommandSessionRequest.CommandOneofCase.ExecuteSql => "ExecuteSql",
            CommandSessionRequest.CommandOneofCase.ExecuteInTransaction => "ExecuteInTransaction",
            _ => throw new ArgumentException("The command must execute SQL.", nameof(command)),
        };
        string? transactionId = command.ExecuteInTransaction?.TransactionId;
        Begin(method, transactionId);
        Task<GrpcCommandSession.SqlReply> response;
        try { response = session.ExecuteSqlAsync(command, ct); }
        catch { Complete(method, transactionId, null, failed: true); throw; }
        return ObserveSqlAsync(response, method, transactionId);
    }

    private async Task<GrpcCommandSession.SqlReply> ObserveSqlAsync(
        Task<GrpcCommandSession.SqlReply> response, string method, string? transactionId)
    {
        try
        {
            GrpcCommandSession.SqlReply result = await response.ConfigureAwait(false);
            // SQL cannot change transaction ownership. Do not box the reply struct
            // merely to run the common completion/drain accounting.
            Complete(method, transactionId, null, failed: false);
            return result;
        }
        catch { Complete(method, transactionId, null, failed: true); throw; }
    }

    private void Begin(string method, string? transactionId)
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposeTask is not null, this);
            if (transactionId is not null && _transactions?.Contains(transactionId) != true)
                throw new InvalidOperationException("The transaction does not belong to this client rental.");
            if (method == "RollbackTransaction" &&
                !(_rollbackAttempts ??= new(StringComparer.Ordinal)).Add(transactionId!))
                throw new InvalidOperationException("Rollback was already attempted. Dispose this client rental.");
            _activeCalls++;
        }
    }

    private async Task<T> ObserveAsync<T>(Task<T> response, string method, string? transactionId)
    {
        try
        {
            T result = await response.ConfigureAwait(false);
            Complete(method, transactionId, result, failed: false);
            return result;
        }
        catch { Complete(method, transactionId, null, failed: true); throw; }
    }

    private void Complete(string method, string? transactionId, object? response, bool failed)
    {
        lock (_gate)
        {
            if (failed) _healthy = false;
            else if (response is TransactionSessionInfoMessage transaction)
                (_transactions ??= new(StringComparer.Ordinal)).Add(transaction.TransactionId);
            else if (method is "CommitTransaction" or "RollbackTransaction")
            {
                _transactions?.Remove(transactionId!);
                _rollbackAttempts?.Remove(transactionId!);
            }
            _activeCalls--;
            if (_activeCalls == 0) _drained?.TrySetResult();
        }
    }

    public ValueTask DisposeAsync()
    {
        Task drain;
        TaskCompletionSource completion;
        lock (_gate)
        {
            if (_disposeTask is not null) return new ValueTask(_disposeTask);
            if (_activeCalls == 0 && (_transactions is null || _transactions.Count == 0))
            {
                // Completed reads need no cleanup task. Publish disposal before
                // returning the owner so a stale rental cannot issue another RPC.
                _disposeTask = Task.CompletedTask;
                try
                {
                    ValueTask returned = release(owner, _healthy);
                    if (returned.IsCompletedSuccessfully) returned.GetAwaiter().GetResult();
                    else _disposeTask = returned.AsTask();
                }
                catch (Exception error) { _disposeTask = Task.FromException(error); }
                return new ValueTask(_disposeTask);
            }
            completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
            _disposeTask = completion.Task;
            drain = _activeCalls == 0 ? Task.CompletedTask
                : (_drained = new(TaskCreationOptions.RunContinuationsAsynchronously)).Task;
        }
        _ = ResetAndReturnAsync(drain, completion);
        return new ValueTask(completion.Task);
    }

    private async Task ResetAndReturnAsync(Task drain, TaskCompletionSource completion)
    {
        try
        {
            bool needsCleanup;
            lock (_gate) needsCleanup = _activeCalls != 0 || _transactions?.Count > 0;
            if (needsCleanup)
            {
                // Ordinary completed reads need no timer or cleanup RPC.
                // One deadline covers outstanding calls and all rollback cleanup.
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                try { await drain.WaitAsync(timeout.Token).ConfigureAwait(false); }
                catch { lock (_gate) _healthy = false; }
                if (drain.IsCompletedSuccessfully)
                {
                    string[] pending;
                    lock (_gate) pending = _transactions is null ? []
                        : _transactions.Except(_rollbackAttempts ?? []).ToArray();
                    foreach (string transactionId in pending)
                    {
                        try { await owner.RollbackTransactionAsync(transactionId, timeout.Token).WaitAsync(timeout.Token).ConfigureAwait(false); }
                        catch { lock (_gate) _healthy = false; }
                    }
                }
            }
            bool healthy;
            lock (_gate)
            {
                healthy = _healthy;
                _transactions?.Clear();
            }
            await release(owner, healthy).ConfigureAwait(false);
            completion.SetResult();
        }
        catch (Exception error) { completion.SetException(error); }
    }
}
