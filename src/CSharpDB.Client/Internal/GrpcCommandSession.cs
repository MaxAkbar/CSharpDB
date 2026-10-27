using CSharpDB.Client.Grpc;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace CSharpDB.Client.Internal;

/// <summary>Adapts command calls to one ordered, versioned native gRPC stream.</summary>
internal sealed class GrpcCommandSession(CallInvoker invoker) : Interceptor, IAsyncDisposable
{
    private const string ResultSchemaCapability = "result-schema-ref-v1";
    private const string SingleInt64Capability = "single-int64-result-v1";
    private readonly object _gate = new();
    private readonly SemaphoreSlim _commands = new(1, 1);
    private readonly CancellationTokenSource _lifetime = new();
    private readonly CSharpDbRpc.CSharpDbRpcClient _client = new(invoker);
    private AsyncDuplexStreamingCall<CommandSessionRequest, CommandSessionResponse>? _call;
    private Exception? _fault;
    private Task? _disposeTask;
    private bool _negotiated;
    private bool _resultSchemaReferences;
    private bool _singleInt64Results;
    private ulong _requestId;
    private ulong _lastResultSchemaId;
    private ResultSchema? _resultSchema;

    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        TRequest request, ClientInterceptorContext<TRequest, TResponse> context,
        AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
    {
        CommandSessionRequest? command = context.Method.Name switch
        {
            "GetInfo" => new() { GetInfo = (Empty)(object)request },
            "ExecuteSql" => new() { ExecuteSql = (SqlRequest)(object)request },
            "BeginTransaction" => new() { BeginTransaction = (Empty)(object)request },
            "ExecuteInTransaction" => new() { ExecuteInTransaction = (TransactionSqlRequest)(object)request },
            "CommitTransaction" => new() { CommitTransaction = (TransactionIdRequest)(object)request },
            "RollbackTransaction" => new() { RollbackTransaction = (TransactionIdRequest)(object)request },
            _ => null,
        };
        if (command is null) return continuation(request, context);
        var state = new UnaryCall<TResponse>(this, command, context.Options);
        return new AsyncUnaryCall<TResponse>(state.Response,
            static value => ((UnaryCall<TResponse>)value).HeadersAsync(),
            static value => ((UnaryCall<TResponse>)value).Status(),
            static value => ((UnaryCall<TResponse>)value).Trailers(),
            static value => ((UnaryCall<TResponse>)value).Cancel(), state);
    }

    internal async Task<TResponse> ExecuteAsync<TResponse>(CommandSessionRequest command, CancellationToken ct)
        where TResponse : class
        => ReadResponse<TResponse>((await ExchangeAsync(command, ct).ConfigureAwait(false)).Response);

    internal readonly record struct SqlReply(SqlExecutionResultMessage Response, GrpcSessionResultMetadata? Metadata);
    private readonly record struct ExchangeReply(CommandSessionResponse Response, GrpcSessionResultMetadata? Metadata);

    internal async Task<SqlReply> ExecuteSqlAsync(CommandSessionRequest command, CancellationToken ct)
    {
        if (command.CommandCase is not (CommandSessionRequest.CommandOneofCase.ExecuteSql or
            CommandSessionRequest.CommandOneofCase.ExecuteInTransaction))
            throw new ArgumentException("The command must execute SQL.", nameof(command));
        ExchangeReply reply = await ExchangeAsync(command, ct, captureSqlMetadata: true).ConfigureAwait(false);
        return new(ReadResponse<SqlExecutionResultMessage>(reply.Response), reply.Metadata);
    }

    private static TResponse ReadResponse<TResponse>(CommandSessionResponse response) where TResponse : class
    {
        if (response.Error is not null) throw ReadError(response.Error);
        return (TResponse)(object)(response.Info ?? (object?)response.Result ?? response.Transaction ?? (object?)response.Completed)!;
    }

    private async Task<ExchangeReply> ExchangeAsync(CommandSessionRequest command, CancellationToken ct,
        bool captureSqlMetadata = false)
    {
        // A canceled waiter has sent nothing and must not cancel the active command.
        await _commands.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            AsyncDuplexStreamingCall<CommandSessionRequest, CommandSessionResponse> call;
            lock (_gate)
            {
                ObjectDisposedException.ThrowIf(_disposeTask is not null, this);
                if (_fault is not null)
                    throw new RpcException(new Status(StatusCode.Unavailable, "The command session is closed. Commands are never replayed."));
                call = _call ??= _client.CommandSession(cancellationToken: _lifetime.Token);
            }
            try
            {
                ct.ThrowIfCancellationRequested();
                if (!_negotiated)
                {
                    await call.RequestStream.WriteAsync(new CommandSessionRequest
                    {
                        Hello = new CommandSessionHello
                        {
                            ProtocolVersion = 1,
                            Capabilities = { ResultSchemaCapability, SingleInt64Capability },
                        },
                    }, ct).ConfigureAwait(false);
                    if (!await call.ResponseStream.MoveNext(ct).ConfigureAwait(false))
                        throw ProtocolError("The server closed the command session before negotiation.");
                    var hello = call.ResponseStream.Current;
                    if (hello.RequestId != 0 || hello.ResultSchemaId != 0 || hello.ResultSchemaReference ||
                        hello.Ready is not { ProtocolVersion: 1, MaxInFlight: 1 } ||
                        !hello.Ready.Capabilities.Contains("sql") || !hello.Ready.Capabilities.Contains("transactions"))
                        throw ProtocolError("The server did not negotiate the required command session capabilities.");
                    _resultSchemaReferences = hello.Ready.Capabilities.Contains(ResultSchemaCapability);
                    _singleInt64Results = hello.Ready.Capabilities.Contains(SingleInt64Capability);
                    _negotiated = true;
                }
                command.RequestId = checked(++_requestId);
                await call.RequestStream.WriteAsync(command, ct).ConfigureAwait(false);
                if (!await call.ResponseStream.MoveNext(ct).ConfigureAwait(false))
                    throw ProtocolError("The server closed the command session before returning a command result.");
                var response = call.ResponseStream.Current;
                if (response.RequestId != command.RequestId || !Matches(command, response))
                    throw ProtocolError("The command session returned an unexpected request ID or result type.");
                if (response.Error is { } error) _ = ReadError(error); // Validate errors inside the fault boundary, too.
                ResultSchema? schema = ResolveResultSchema(response);
                ValidateSingleInt64Result(response, schema);
                GrpcSessionResultMetadata? metadata = null;
                if (captureSqlMetadata)
                    metadata = schema?.EnsureMetadata();
                else if (response.ResultSchemaReference)
                    schema!.Restore(response.Result!);
                return new(response, metadata);
            }
            catch (Exception error)
            {
                _resultSchema = null;
                lock (_gate) _fault ??= error;
                _lifetime.Cancel();
                call.Dispose();
                throw;
            }
        }
        finally { _commands.Release(); }
    }

    private void ValidateSingleInt64Result(CommandSessionResponse response, ResultSchema? schema)
    {
        if (response.Result is not { HasScalarInt64: true } result) return;
        if (!_singleInt64Results)
            throw ProtocolError("The command session returned a compact integer result without negotiation.");
        // Validate against the captured wire schema without rehydrating reference
        // frames. A present Rows wrapper, even empty, conflicts with this encoding.
        if (!result.IsQuery || result.Error is not null || result.Rows is not null ||
            !(schema?.HasSingleColumnMetadata ?? GrpcModelMapper.HasSingleColumnMetadata(result)))
            throw ProtocolError("A compact integer result requires a successful one-column query and no rows payload.");
    }

    private ResultSchema? ResolveResultSchema(CommandSessionResponse response)
    {
        if (!_resultSchemaReferences)
        {
            if (response.ResultSchemaId != 0 || response.ResultSchemaReference)
                throw ProtocolError("The command session returned result schema fields without negotiation.");
            return null;
        }

        if (response.Result is not { IsQuery: true, Error: null } result)
        {
            if (response.ResultSchemaId != 0 || response.ResultSchemaReference)
                throw ProtocolError("Only successful query results can define or reference a result schema.");
            _resultSchema = null;
            return null;
        }

        if (response.ResultSchemaReference)
        {
            if (_resultSchema is null || response.ResultSchemaId == 0 || response.ResultSchemaId != _resultSchema.Id)
                throw ProtocolError("The command session referenced an unknown or stale result schema.");
            if (result.ColumnNames is not null || result.ColumnTypes is not null ||
                result.ColumnNullability is not null || result.Columns.Count != 0)
                throw ProtocolError("A result schema reference must omit all column metadata.");
            return _resultSchema;
        }

        if (_lastResultSchemaId == ulong.MaxValue || response.ResultSchemaId != _lastResultSchemaId + 1)
            throw ProtocolError("Result schema definitions must have consecutive nonzero IDs.");
        _resultSchema = new ResultSchema(response.ResultSchemaId, result);
        _lastResultSchemaId = response.ResultSchemaId;
        return _resultSchema;
    }

    /// <summary>Owns immutable private metadata, independent of every received result.</summary>
    private sealed class ResultSchema
    {
        private readonly SqlExecutionResultMessage _definition;
        private GrpcSessionResultMetadata? _metadata;
        public ulong Id { get; }
        public bool HasSingleColumnMetadata { get; }

        public ResultSchema(ulong id, SqlExecutionResultMessage result)
        {
            Id = id;
            _definition = new SqlExecutionResultMessage
            {
                ColumnNames = result.ColumnNames?.Clone(),
                ColumnTypes = result.ColumnTypes?.Clone(),
                ColumnNullability = result.ColumnNullability?.Clone(),
            };
            _definition.Columns.Add(result.Columns.Select(column => column.Clone()));
            HasSingleColumnMetadata = GrpcModelMapper.HasSingleColumnMetadata(_definition);
        }

        public GrpcSessionResultMetadata EnsureMetadata()
            => _metadata ??= new GrpcSessionResultMetadata(_definition);

        public void Restore(SqlExecutionResultMessage result)
        {
            // Only the generic wire API restores protobuf metadata. Never expose
            // private cache objects through its mutable returned message, including
            // before the mapped metadata snapshot has first been initialized.
            result.ColumnNames = _definition.ColumnNames?.Clone();
            result.ColumnTypes = _definition.ColumnTypes?.Clone();
            result.ColumnNullability = _definition.ColumnNullability?.Clone();
            result.Columns.Add(_definition.Columns.Select(column => column.Clone()));
        }
    }

    private static bool Matches(CommandSessionRequest command, CommandSessionResponse response)
        => response.Error is not null || command.CommandCase switch
        {
            CommandSessionRequest.CommandOneofCase.GetInfo => response.Info is not null,
            CommandSessionRequest.CommandOneofCase.ExecuteSql or CommandSessionRequest.CommandOneofCase.ExecuteInTransaction => response.Result is not null,
            CommandSessionRequest.CommandOneofCase.BeginTransaction => response.Transaction is not null,
            CommandSessionRequest.CommandOneofCase.CommitTransaction or CommandSessionRequest.CommandOneofCase.RollbackTransaction => response.Completed is not null,
            _ => false,
        };

    private static RpcException ProtocolError(string message)
        => new(new Status(StatusCode.DataLoss, message));

    private static RpcException ReadError(CommandSessionError error)
    {
        var code = (StatusCode)error.StatusCode;
        if (!System.Enum.IsDefined(code) || code == StatusCode.OK) throw ProtocolError("The command session returned an invalid error status.");
        var trailers = new Metadata();
        try
        {
            foreach (var item in error.Trailers)
            {
                if (item.ValueCase == CommandSessionMetadata.ValueOneofCase.Binary) trailers.Add(item.Key, item.Binary.ToByteArray());
                else if (item.ValueCase == CommandSessionMetadata.ValueOneofCase.Text) trailers.Add(item.Key, item.Text);
                else throw ProtocolError("The command session returned an invalid error trailer.");
            }
        }
        catch (ArgumentException) { throw ProtocolError("The command session returned an invalid error trailer."); }
        return new RpcException(new Status(code, error.Detail), trailers);
    }

    public ValueTask DisposeAsync()
    {
        TaskCompletionSource completion;
        lock (_gate)
        {
            if (_disposeTask is not null) return new ValueTask(_disposeTask);
            completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
            _disposeTask = completion.Task;
        }
        _ = CloseAsync(completion);
        return new ValueTask(completion.Task);
    }

    private async Task CloseAsync(TaskCompletionSource completion)
    {
        bool idle = _commands.Wait(0);
        try
        {
            var call = _call;
            if (idle && call is not null && _fault is null)
            {
                using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
                try
                {
                    await call.RequestStream.CompleteAsync().WaitAsync(timeout.Token).ConfigureAwait(false);
                    // EOF acknowledges server-side cleanup before the owner closes.
                    if (await call.ResponseStream.MoveNext(timeout.Token).ConfigureAwait(false))
                        throw ProtocolError("An idle command session returned an unsolicited result.");
                }
                catch { /* Retirement still aborts a broken or unresponsive stream. */ }
            }
        }
        finally
        {
            _lifetime.Cancel();
            _call?.Dispose();
            if (idle) _commands.Release();
            // A concurrent command may still be unwinding its cancellation.
            // The semaphore and token source remain valid until it releases them.
            completion.TrySetResult();
        }
    }

    private sealed class UnaryCall<T> where T : class
    {
        private readonly GrpcCommandSession _session;
        private readonly CancellationTokenSource _cancellation;
        public Task<T> Response { get; }

        public UnaryCall(GrpcCommandSession session, CommandSessionRequest command, CallOptions options)
        {
            _session = session;
            _cancellation = CancellationTokenSource.CreateLinkedTokenSource(options.CancellationToken);
            if (options.Deadline is { } deadline)
            {
                TimeSpan remaining = deadline - DateTime.UtcNow;
                if (remaining <= TimeSpan.Zero) _cancellation.Cancel();
                else _cancellation.CancelAfter(remaining);
            }
            Response = RunAsync(command);
        }

        private async Task<T> RunAsync(CommandSessionRequest command)
        {
            try
            {
                var response = await _session.ExchangeAsync(command, _cancellation.Token).ConfigureAwait(false);
                return ReadResponse<T>(response.Response);
            }
            finally { _cancellation.Dispose(); }
        }

        public void Cancel()
        {
            try { _cancellation.Cancel(); }
            catch (ObjectDisposedException) { }
        }

        public async Task<Metadata> HeadersAsync()
        {
            try { await Response.ConfigureAwait(false); }
            catch { }
            return _session._call is { } call
                ? await call.ResponseHeadersAsync.ConfigureAwait(false) : new Metadata();
        }

        public Status Status()
        {
            if (!Response.IsCompleted) throw new InvalidOperationException("The command has not completed.");
            if (Response.IsCompletedSuccessfully) return global::Grpc.Core.Status.DefaultSuccess;
            return Response.Exception?.GetBaseException() is RpcException rpc
                ? rpc.Status : new Status(StatusCode.Cancelled, "The command was canceled.");
        }

        public Metadata Trailers()
        {
            if (!Response.IsCompleted) throw new InvalidOperationException("The command has not completed.");
            return Response.Exception?.GetBaseException() is RpcException rpc ? rpc.Trailers : new Metadata();
        }
    }
}
