using CSharpDB.Client;
using CSharpDB.Client.Grpc;
using CSharpDB.Client.Models;
using Grpc.Core;
using CoreDbException = CSharpDB.Core.CSharpDbException;
using CoreErrorCode = CSharpDB.Core.ErrorCode;

namespace CSharpDB.Daemon.Grpc;

public sealed class CSharpDbRpcService(ICSharpDbClient client) : CSharpDbRpc.CSharpDbRpcBase
{
    public override async Task<InvokeResponse> Invoke(InvokeRequest request, ServerCallContext context)
    {
        try
        {
            CancellationToken ct = context.CancellationToken;

            return request.Operation switch
            {
                RpcOperation.GetInfo => Write(await client.GetInfoAsync(ct)),
                RpcOperation.GetTableNames => Write(await client.GetTableNamesAsync(ct)),
                RpcOperation.GetTableSchema => Write(await client.GetTableSchemaAsync(Read<TableNameRequest>(request).TableName, ct)),
                RpcOperation.GetRowCount => Write(await client.GetRowCountAsync(Read<TableNameRequest>(request).TableName, ct)),
                RpcOperation.BrowseTable => await BrowseTableAsync(request, ct),
                RpcOperation.GetRowByPk => await GetRowByPkAsync(request, ct),
                RpcOperation.InsertRow => await InsertRowAsync(request, ct),
                RpcOperation.UpdateRow => await UpdateRowAsync(request, ct),
                RpcOperation.DeleteRow => await DeleteRowAsync(request, ct),
                RpcOperation.DropTable => await DropTableAsync(request, ct),
                RpcOperation.RenameTable => await RenameTableAsync(request, ct),
                RpcOperation.AddColumn => await AddColumnAsync(request, ct),
                RpcOperation.DropColumn => await DropColumnAsync(request, ct),
                RpcOperation.RenameColumn => await RenameColumnAsync(request, ct),
                RpcOperation.GetIndexes => Write(await client.GetIndexesAsync(ct)),
                RpcOperation.CreateIndex => await CreateIndexAsync(request, ct),
                RpcOperation.UpdateIndex => await UpdateIndexAsync(request, ct),
                RpcOperation.DropIndex => await DropIndexAsync(request, ct),
                RpcOperation.GetViewNames => Write(await client.GetViewNamesAsync(ct)),
                RpcOperation.GetViews => Write(await client.GetViewsAsync(ct)),
                RpcOperation.GetView => Write(await client.GetViewAsync(Read<NameRequest>(request).Name, ct)),
                RpcOperation.GetViewSql => Write(await client.GetViewSqlAsync(Read<NameRequest>(request).Name, ct)),
                RpcOperation.BrowseView => await BrowseViewAsync(request, ct),
                RpcOperation.CreateView => await CreateViewAsync(request, ct),
                RpcOperation.UpdateView => await UpdateViewAsync(request, ct),
                RpcOperation.DropView => await DropViewAsync(request, ct),
                RpcOperation.GetTriggers => Write(await client.GetTriggersAsync(ct)),
                RpcOperation.CreateTrigger => await CreateTriggerAsync(request, ct),
                RpcOperation.UpdateTrigger => await UpdateTriggerAsync(request, ct),
                RpcOperation.DropTrigger => await DropTriggerAsync(request, ct),
                RpcOperation.GetSavedQueries => Write(await client.GetSavedQueriesAsync(ct)),
                RpcOperation.GetSavedQuery => Write(await client.GetSavedQueryAsync(Read<NameRequest>(request).Name, ct)),
                RpcOperation.UpsertSavedQuery => await UpsertSavedQueryAsync(request, ct),
                RpcOperation.DeleteSavedQuery => await DeleteSavedQueryAsync(request, ct),
                RpcOperation.GetProcedures => Write(await GetProceduresAsync(request, ct)),
                RpcOperation.GetProcedure => Write(await client.GetProcedureAsync(Read<NameRequest>(request).Name, ct)),
                RpcOperation.CreateProcedure => await CreateProcedureAsync(request, ct),
                RpcOperation.UpdateProcedure => await UpdateProcedureAsync(request, ct),
                RpcOperation.DeleteProcedure => await DeleteProcedureAsync(request, ct),
                RpcOperation.ExecuteProcedure => await ExecuteProcedureAsync(request, ct),
                RpcOperation.ExecuteSql => Write(await client.ExecuteSqlAsync(Read<SqlRequest>(request).Sql, ct)),
                RpcOperation.BeginTransaction => Write(await client.BeginTransactionAsync(ct)),
                RpcOperation.ExecuteInTransaction => await ExecuteInTransactionAsync(request, ct),
                RpcOperation.CommitTransaction => await CommitTransactionAsync(request, ct),
                RpcOperation.RollbackTransaction => await RollbackTransactionAsync(request, ct),
                RpcOperation.GetCollectionNames => Write(await client.GetCollectionNamesAsync(ct)),
                RpcOperation.GetCollectionCount => Write(await client.GetCollectionCountAsync(Read<CollectionNameRequest>(request).CollectionName, ct)),
                RpcOperation.BrowseCollection => await BrowseCollectionAsync(request, ct),
                RpcOperation.GetDocument => await GetDocumentAsync(request, ct),
                RpcOperation.PutDocument => await PutDocumentAsync(request, ct),
                RpcOperation.DeleteDocument => await DeleteDocumentAsync(request, ct),
                RpcOperation.Checkpoint => await CheckpointAsync(ct),
                RpcOperation.InspectStorage => await InspectStorageAsync(request, ct),
                RpcOperation.CheckWal => await CheckWalAsync(request, ct),
                RpcOperation.InspectPage => await InspectPageAsync(request, ct),
                RpcOperation.CheckIndexes => await CheckIndexesAsync(request, ct),
                _ => throw new ArgumentException($"Unsupported gRPC operation '{request.Operation}'."),
            };
        }
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw TranslateException(ex);
        }
    }

    private static T Read<T>(InvokeRequest request)
        => GrpcJson.DeserializeRequired<T>(request.PayloadJson);

    private static InvokeResponse Write<T>(T value)
        => new()
        {
            PayloadJson = GrpcJson.Serialize(value),
        };

    private static InvokeResponse Empty()
        => new()
        {
            PayloadJson = string.Empty,
        };

    private async Task<InvokeResponse> BrowseTableAsync(InvokeRequest request, CancellationToken ct)
    {
        var payload = Read<PagedTableRequest>(request);
        return Write(await client.BrowseTableAsync(payload.TableName, payload.Page, payload.PageSize, ct));
    }

    private async Task<InvokeResponse> GetRowByPkAsync(InvokeRequest request, CancellationToken ct)
    {
        var payload = Read<GetRowByPkRequest>(request);
        return Write(await client.GetRowByPkAsync(payload.TableName, payload.PkColumn, payload.PkValue!, ct));
    }

    private async Task<InvokeResponse> InsertRowAsync(InvokeRequest request, CancellationToken ct)
    {
        var payload = Read<InsertRowRequest>(request);
        return Write(await client.InsertRowAsync(payload.TableName, payload.Values, ct));
    }

    private async Task<InvokeResponse> UpdateRowAsync(InvokeRequest request, CancellationToken ct)
    {
        var payload = Read<UpdateRowRequest>(request);
        return Write(await client.UpdateRowAsync(payload.TableName, payload.PkColumn, payload.PkValue!, payload.Values, ct));
    }

    private async Task<InvokeResponse> DeleteRowAsync(InvokeRequest request, CancellationToken ct)
    {
        var payload = Read<DeleteRowRequest>(request);
        return Write(await client.DeleteRowAsync(payload.TableName, payload.PkColumn, payload.PkValue!, ct));
    }

    private async Task<InvokeResponse> DropTableAsync(InvokeRequest request, CancellationToken ct)
    {
        await client.DropTableAsync(Read<TableNameRequest>(request).TableName, ct);
        return Empty();
    }

    private async Task<InvokeResponse> RenameTableAsync(InvokeRequest request, CancellationToken ct)
    {
        var payload = Read<RenameTableRequest>(request);
        await client.RenameTableAsync(payload.TableName, payload.NewTableName, ct);
        return Empty();
    }

    private async Task<InvokeResponse> AddColumnAsync(InvokeRequest request, CancellationToken ct)
    {
        var payload = Read<AddColumnRequest>(request);
        await client.AddColumnAsync(payload.TableName, payload.ColumnName, payload.Type, payload.NotNull, ct);
        return Empty();
    }

    private async Task<InvokeResponse> DropColumnAsync(InvokeRequest request, CancellationToken ct)
    {
        var payload = Read<DropColumnRequest>(request);
        await client.DropColumnAsync(payload.TableName, payload.ColumnName, ct);
        return Empty();
    }

    private async Task<InvokeResponse> RenameColumnAsync(InvokeRequest request, CancellationToken ct)
    {
        var payload = Read<RenameColumnRequest>(request);
        await client.RenameColumnAsync(payload.TableName, payload.OldColumnName, payload.NewColumnName, ct);
        return Empty();
    }

    private async Task<InvokeResponse> CreateIndexAsync(InvokeRequest request, CancellationToken ct)
    {
        var payload = Read<CreateIndexRequest>(request);
        await client.CreateIndexAsync(payload.IndexName, payload.TableName, payload.ColumnName, payload.IsUnique, ct);
        return Empty();
    }

    private async Task<InvokeResponse> UpdateIndexAsync(InvokeRequest request, CancellationToken ct)
    {
        var payload = Read<UpdateIndexRequest>(request);
        await client.UpdateIndexAsync(payload.ExistingIndexName, payload.NewIndexName, payload.TableName, payload.ColumnName, payload.IsUnique, ct);
        return Empty();
    }

    private async Task<InvokeResponse> DropIndexAsync(InvokeRequest request, CancellationToken ct)
    {
        await client.DropIndexAsync(Read<NameRequest>(request).Name, ct);
        return Empty();
    }

    private async Task<InvokeResponse> BrowseViewAsync(InvokeRequest request, CancellationToken ct)
    {
        var payload = Read<PagedNameRequest>(request);
        return Write(await client.BrowseViewAsync(payload.Name, payload.Page, payload.PageSize, ct));
    }

    private async Task<InvokeResponse> CreateViewAsync(InvokeRequest request, CancellationToken ct)
    {
        var payload = Read<CreateViewRequest>(request);
        await client.CreateViewAsync(payload.ViewName, payload.SelectSql, ct);
        return Empty();
    }

    private async Task<InvokeResponse> UpdateViewAsync(InvokeRequest request, CancellationToken ct)
    {
        var payload = Read<UpdateViewRequest>(request);
        await client.UpdateViewAsync(payload.ExistingViewName, payload.NewViewName, payload.SelectSql, ct);
        return Empty();
    }

    private async Task<InvokeResponse> DropViewAsync(InvokeRequest request, CancellationToken ct)
    {
        await client.DropViewAsync(Read<NameRequest>(request).Name, ct);
        return Empty();
    }

    private async Task<InvokeResponse> CreateTriggerAsync(InvokeRequest request, CancellationToken ct)
    {
        var payload = Read<CreateTriggerRequest>(request);
        await client.CreateTriggerAsync(payload.TriggerName, payload.TableName, payload.Timing, payload.TriggerEvent, payload.BodySql, ct);
        return Empty();
    }

    private async Task<InvokeResponse> UpdateTriggerAsync(InvokeRequest request, CancellationToken ct)
    {
        var payload = Read<UpdateTriggerRequest>(request);
        await client.UpdateTriggerAsync(payload.ExistingTriggerName, payload.NewTriggerName, payload.TableName, payload.Timing, payload.TriggerEvent, payload.BodySql, ct);
        return Empty();
    }

    private async Task<InvokeResponse> DropTriggerAsync(InvokeRequest request, CancellationToken ct)
    {
        await client.DropTriggerAsync(Read<NameRequest>(request).Name, ct);
        return Empty();
    }

    private async Task<InvokeResponse> UpsertSavedQueryAsync(InvokeRequest request, CancellationToken ct)
    {
        var payload = Read<UpsertSavedQueryRequest>(request);
        return Write(await client.UpsertSavedQueryAsync(payload.Name, payload.SqlText, ct));
    }

    private async Task<InvokeResponse> DeleteSavedQueryAsync(InvokeRequest request, CancellationToken ct)
    {
        await client.DeleteSavedQueryAsync(Read<NameRequest>(request).Name, ct);
        return Empty();
    }

    private async Task<IReadOnlyList<ProcedureDefinition>> GetProceduresAsync(InvokeRequest request, CancellationToken ct)
    {
        var payload = Read<GetProceduresRequest>(request);
        return await client.GetProceduresAsync(payload.IncludeDisabled, ct);
    }

    private async Task<InvokeResponse> CreateProcedureAsync(InvokeRequest request, CancellationToken ct)
    {
        await client.CreateProcedureAsync(Read<CreateProcedureRequest>(request).Definition, ct);
        return Empty();
    }

    private async Task<InvokeResponse> UpdateProcedureAsync(InvokeRequest request, CancellationToken ct)
    {
        var payload = Read<UpdateProcedureRequest>(request);
        await client.UpdateProcedureAsync(payload.ExistingName, payload.Definition, ct);
        return Empty();
    }

    private async Task<InvokeResponse> DeleteProcedureAsync(InvokeRequest request, CancellationToken ct)
    {
        await client.DeleteProcedureAsync(Read<NameRequest>(request).Name, ct);
        return Empty();
    }

    private async Task<InvokeResponse> ExecuteProcedureAsync(InvokeRequest request, CancellationToken ct)
    {
        var payload = Read<ExecuteProcedureRequest>(request);
        return Write(await client.ExecuteProcedureAsync(payload.Name, payload.Args, ct));
    }

    private async Task<InvokeResponse> ExecuteInTransactionAsync(InvokeRequest request, CancellationToken ct)
    {
        var payload = Read<TransactionSqlRequest>(request);
        return Write(await client.ExecuteInTransactionAsync(payload.TransactionId, payload.Sql, ct));
    }

    private async Task<InvokeResponse> CommitTransactionAsync(InvokeRequest request, CancellationToken ct)
    {
        await client.CommitTransactionAsync(Read<TransactionIdRequest>(request).TransactionId, ct);
        return Empty();
    }

    private async Task<InvokeResponse> RollbackTransactionAsync(InvokeRequest request, CancellationToken ct)
    {
        await client.RollbackTransactionAsync(Read<TransactionIdRequest>(request).TransactionId, ct);
        return Empty();
    }

    private async Task<InvokeResponse> BrowseCollectionAsync(InvokeRequest request, CancellationToken ct)
    {
        var payload = Read<PagedNameRequest>(request);
        return Write(await client.BrowseCollectionAsync(payload.Name, payload.Page, payload.PageSize, ct));
    }

    private async Task<InvokeResponse> GetDocumentAsync(InvokeRequest request, CancellationToken ct)
    {
        var payload = Read<GetDocumentRequest>(request);
        return Write(await client.GetDocumentAsync(payload.CollectionName, payload.Key, ct));
    }

    private async Task<InvokeResponse> PutDocumentAsync(InvokeRequest request, CancellationToken ct)
    {
        var payload = Read<PutDocumentRequest>(request);
        await client.PutDocumentAsync(payload.CollectionName, payload.Key, payload.Document, ct);
        return Empty();
    }

    private async Task<InvokeResponse> DeleteDocumentAsync(InvokeRequest request, CancellationToken ct)
    {
        var payload = Read<DeleteDocumentRequest>(request);
        return Write(await client.DeleteDocumentAsync(payload.CollectionName, payload.Key, ct));
    }

    private async Task<InvokeResponse> CheckpointAsync(CancellationToken ct)
    {
        await client.CheckpointAsync(ct);
        return Empty();
    }

    private async Task<InvokeResponse> InspectStorageAsync(InvokeRequest request, CancellationToken ct)
    {
        var payload = Read<InspectStorageRequest>(request);
        return Write(await client.InspectStorageAsync(payload.DatabasePath, payload.IncludePages, ct));
    }

    private async Task<InvokeResponse> CheckWalAsync(InvokeRequest request, CancellationToken ct)
    {
        var payload = Read<CheckWalRequest>(request);
        return Write(await client.CheckWalAsync(payload.DatabasePath, ct));
    }

    private async Task<InvokeResponse> InspectPageAsync(InvokeRequest request, CancellationToken ct)
    {
        var payload = Read<InspectPageRequest>(request);
        return Write(await client.InspectPageAsync(payload.PageId, payload.IncludeHex, payload.DatabasePath, ct));
    }

    private async Task<InvokeResponse> CheckIndexesAsync(InvokeRequest request, CancellationToken ct)
    {
        var payload = Read<CheckIndexesRequest>(request);
        return Write(await client.CheckIndexesAsync(payload.DatabasePath, payload.IndexName, payload.SampleSize, ct));
    }

    private static RpcException TranslateException(Exception ex)
    {
        Metadata metadata = [];

        Status status = ex switch
        {
            CoreDbException dbEx => CreateStatus(dbEx, metadata),
            CSharpDbClientConfigurationException configEx => CreateStatus(StatusCode.InvalidArgument, configEx.Message, metadata, GrpcMetadataNames.ErrorTypeConfiguration),
            ArgumentException argumentEx => CreateStatus(StatusCode.InvalidArgument, argumentEx.Message, metadata),
            CSharpDbClientException clientEx => CreateStatus(StatusCode.FailedPrecondition, clientEx.Message, metadata, GrpcMetadataNames.ErrorTypeClient),
            _ => CreateStatus(StatusCode.Internal, ex.Message, metadata),
        };

        return new RpcException(status, metadata);
    }

    private static Status CreateStatus(CoreDbException ex, Metadata metadata)
    {
        metadata.Add(new Metadata.Entry(GrpcMetadataNames.ErrorCode, ex.Code.ToString()));
        return new Status(MapStatusCode(ex.Code), ex.Message);
    }

    private static Status CreateStatus(StatusCode statusCode, string detail, Metadata metadata, string? errorType = null)
    {
        if (!string.IsNullOrWhiteSpace(errorType))
            metadata.Add(new Metadata.Entry(GrpcMetadataNames.ErrorType, errorType));

        return new Status(statusCode, detail);
    }

    private static StatusCode MapStatusCode(CoreErrorCode code)
        => code switch
        {
            CoreErrorCode.TableNotFound or CoreErrorCode.ColumnNotFound or CoreErrorCode.TriggerNotFound => StatusCode.NotFound,
            CoreErrorCode.TableAlreadyExists or CoreErrorCode.TriggerAlreadyExists or CoreErrorCode.DuplicateKey => StatusCode.AlreadyExists,
            CoreErrorCode.SyntaxError or CoreErrorCode.TypeMismatch => StatusCode.InvalidArgument,
            CoreErrorCode.ConstraintViolation => StatusCode.FailedPrecondition,
            CoreErrorCode.Busy => StatusCode.Aborted,
            CoreErrorCode.IoError or CoreErrorCode.JournalError or CoreErrorCode.WalError => StatusCode.Unavailable,
            CoreErrorCode.CorruptDatabase => StatusCode.DataLoss,
            _ => StatusCode.Unknown,
        };
}
