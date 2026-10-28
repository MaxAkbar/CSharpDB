using CSharpDB.Client.Grpc;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace CSharpDB.Daemon.Grpc;

public sealed partial class CSharpDbRpcService
{
    public override async Task CommandSession(
        IAsyncStreamReader<CommandSessionRequest> requestStream,
        IServerStreamWriter<CommandSessionResponse> responseStream,
        ServerCallContext context)
    {
        if (context.GetHttpContext().Request.Protocol != "HTTP/2")
            throw new RpcException(new Status(StatusCode.Unimplemented, "Command sessions require native HTTP/2."));

        var transactions = new HashSet<string>(StringComparer.Ordinal);
        try
        {
            using (var handshake = CancellationTokenSource.CreateLinkedTokenSource(context.CancellationToken))
            {
                handshake.CancelAfter(TimeSpan.FromSeconds(10));
                if (!await requestStream.MoveNext(handshake.Token).ConfigureAwait(false)) return;
            }
            CommandSessionRequest hello = requestStream.Current;
            if (hello.RequestId != 0 || hello.CommandCase != CommandSessionRequest.CommandOneofCase.Hello ||
                hello.Hello.ProtocolVersion != 1)
                throw new RpcException(new Status(StatusCode.Unimplemented, "Command session protocol version 1 is required."));

            var ready = new CommandSessionReady { ProtocolVersion = 1, MaxInFlight = 1 };
            ready.Capabilities.Add(["sql", "transactions"]);
            bool reuseResultSchema = hello.Hello.Capabilities.Contains(CommandResultSchemaCache.Capability);
            CommandResultSchemaCache? resultSchemas = null;
            if (reuseResultSchema)
            {
                ready.Capabilities.Add(CommandResultSchemaCache.Capability);
                resultSchemas = new CommandResultSchemaCache();
            }
            bool compactScalarInt64 = hello.Hello.Capabilities.Contains(SingleInt64ResultCapability);
            if (compactScalarInt64) ready.Capabilities.Add(SingleInt64ResultCapability);
            await responseStream.WriteAsync(new CommandSessionResponse { Ready = ready }).ConfigureAwait(false);
            ulong lastRequest = 0;
            while (await requestStream.MoveNext(context.CancellationToken).ConfigureAwait(false))
            {
                CommandSessionRequest request = requestStream.Current;
                if (request.RequestId != checked(lastRequest + 1) ||
                    request.CommandCase is CommandSessionRequest.CommandOneofCase.None or CommandSessionRequest.CommandOneofCase.Hello)
                    throw new RpcException(new Status(StatusCode.InvalidArgument, "Command IDs must be consecutive and commands must be supported."));
                lastRequest = request.RequestId;
                var response = new CommandSessionResponse { RequestId = request.RequestId };
                try
                {
                    switch (request.CommandCase)
                    {
                        case CommandSessionRequest.CommandOneofCase.GetInfo:
                            response.Info = await GetInfo(EmptyResponse, context).ConfigureAwait(false);
                            break;
                        case CommandSessionRequest.CommandOneofCase.ExecuteSql:
                            var sqlResult = await ExecuteSqlResultAsync(request.ExecuteSql, context, static result => result).ConfigureAwait(false);
                            SetCommandSessionResult(response, sqlResult, resultSchemas, compactScalarInt64);
                            break;
                        case CommandSessionRequest.CommandOneofCase.BeginTransaction:
                            if (transactions.Count >= 64)
                                throw new RpcException(new Status(StatusCode.ResourceExhausted, "A command session supports at most 64 active transactions."));
                            response.Transaction = await BeginTransaction(EmptyResponse, context).ConfigureAwait(false);
                            transactions.Add(response.Transaction.TransactionId);
                            break;
                        case CommandSessionRequest.CommandOneofCase.ExecuteInTransaction:
                            RequireSessionTransaction(transactions, request.ExecuteInTransaction.TransactionId);
                            var transactionResult = await ExecuteAsync(context,
                                ct => client.ExecuteInTransactionAsync(request.ExecuteInTransaction.TransactionId,
                                    request.ExecuteInTransaction.Sql, ct), static result => result).ConfigureAwait(false);
                            SetCommandSessionResult(response, transactionResult, resultSchemas, compactScalarInt64);
                            break;
                        case CommandSessionRequest.CommandOneofCase.CommitTransaction:
                            RequireSessionTransaction(transactions, request.CommitTransaction.TransactionId);
                            response.Completed = await CommitTransaction(request.CommitTransaction, context).ConfigureAwait(false);
                            transactions.Remove(request.CommitTransaction.TransactionId);
                            break;
                        case CommandSessionRequest.CommandOneofCase.RollbackTransaction:
                            RequireSessionTransaction(transactions, request.RollbackTransaction.TransactionId);
                            response.Completed = await RollbackTransaction(request.RollbackTransaction, context).ConfigureAwait(false);
                            transactions.Remove(request.RollbackTransaction.TransactionId);
                            break;
                        default:
                            throw new RpcException(new Status(StatusCode.Unimplemented, "Unsupported session command."));
                    }
                }
                catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested) { throw; }
                catch (Exception error)
                {
                    RpcException rpc = error as RpcException ?? TranslateException(error);
                    response.Error = new CommandSessionError { StatusCode = (int)rpc.StatusCode, Detail = rpc.Status.Detail };
                    foreach (var trailer in rpc.Trailers)
                    {
                        var value = new CommandSessionMetadata { Key = trailer.Key };
                        if (trailer.IsBinary) value.Binary = ByteString.CopyFrom(trailer.ValueBytes);
                        else value.Text = trailer.Value;
                        response.Error.Trailers.Add(value);
                    }
                }
                if (response.Result is not { IsQuery: true, Error: null }) resultSchemas?.Clear();
                await responseStream.WriteAsync(response).ConfigureAwait(false);
            }
        }
        finally
        {
            // Even a Begin response lost to disconnect leaves its transaction
            // owned here. Never close the shared database or replay a command.
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            foreach (string transaction in transactions)
            {
                try { await client.RollbackTransactionAsync(transaction, cleanup.Token).ConfigureAwait(false); }
                catch (Exception error)
                {
                    // Cleanup is best effort; surface failures without masking the original call error.
                    services?.GetService<ILogger<CSharpDbRpcService>>()?
                        .LogWarning(error, "Command session transaction cleanup failed.");
                }
            }
        }
    }

    private static void RequireSessionTransaction(HashSet<string> transactions, string transaction)
    {
        if (!transactions.Contains(transaction))
            throw new RpcException(new Status(StatusCode.PermissionDenied, "The transaction does not belong to this command session."));
    }
}
