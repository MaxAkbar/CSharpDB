using System.Runtime.CompilerServices;
using CSharpDB.Client.Grpc;
using Grpc.Core;

namespace CSharpDB.Client.Internal;

internal sealed partial class GrpcTransportClient : IScalarProjectionClient
{
    bool IScalarProjectionClient.SupportsScalarProjection => _commandSession is not null;

    ValueTask<ScalarProjectionResult> IScalarProjectionClient.ExecuteScalarProjectionAsync(
        string sql, string? transactionId, CancellationToken cancellationToken)
    {
        if (_commandSession is null)
            throw new NotSupportedException("Scalar projection requires a command session.");

        CommandSessionRequest command = transactionId is null
            ? new() { ExecuteSql = new SqlRequest { Sql = sql } }
            : new()
            {
                ExecuteInTransaction = new TransactionSqlRequest { TransactionId = transactionId, Sql = sql },
            };

        // Dispatch through the same lease accounting, transaction checks and session fault boundary.
        return MapScalarProjectionAsync(_lease is not null
                ? _lease.ExecuteSqlAsync(_commandSession, command, cancellationToken)
                : _commandSession.ExecuteSqlAsync(command, cancellationToken),
            cancellationToken);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private static async ValueTask<ScalarProjectionResult> MapScalarProjectionAsync(
        Task<GrpcCommandSession.SqlReply> response, CancellationToken cancellationToken)
    {
        try
        {
            return MapScalarProjectionReply(await response.ConfigureAwait(false));
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled && cancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException(cancellationToken);
        }
        catch (RpcException ex)
        {
            throw TranslateRpcException(ex);
        }
    }

    internal static ScalarProjectionResult MapScalarProjectionReply(GrpcCommandSession.SqlReply reply)
    {
        SqlExecutionResultMessage response = reply.Response;
        if (response.HasScalarInt64 && response.IsQuery && response.Error is null && response.Rows is null
            && reply.Metadata is { } metadata && metadata.TryGetScalarType(out var declaredType))
        {
            // Full SDK mapping validates duration before Data performs type conversion or cancellation.
            _ = response.Elapsed.ToTimeSpan();
            return ScalarProjectionResult.FromCompactInt64(response.ScalarInt64, declaredType);
        }

        // Map the response already received. Unsupported shapes must never execute SQL again.
        return ScalarProjectionResult.FromFallback(GrpcModelMapper.ToSessionModel(response, reply.Metadata));
    }
}
