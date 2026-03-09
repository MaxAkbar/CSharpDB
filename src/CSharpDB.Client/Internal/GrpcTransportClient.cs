using System.Text.Json;
using CSharpDB.Client.Grpc;
using CSharpDB.Client.Models;
using CSharpDB.Storage.Diagnostics;
using Grpc.Core;
using Grpc.Net.Client;
using CoreDbException = CSharpDB.Core.CSharpDbException;
using CoreErrorCode = CSharpDB.Core.ErrorCode;

namespace CSharpDB.Client.Internal;

internal sealed class GrpcTransportClient : ICSharpDbClient
{
    private readonly GrpcChannel _channel;
    private readonly CSharpDbRpc.CSharpDbRpcClient _client;
    private readonly string _endpoint;

    public GrpcTransportClient(Uri endpoint, HttpClient? httpClient = null)
    {
        ArgumentNullException.ThrowIfNull(endpoint);

        _endpoint = endpoint.AbsoluteUri;

        var channelOptions = new GrpcChannelOptions();
        if (httpClient is not null)
        {
            channelOptions.HttpClient = httpClient;
            channelOptions.DisposeHttpClient = false;
        }

        _channel = GrpcChannel.ForAddress(endpoint, channelOptions);
        _client = new CSharpDbRpc.CSharpDbRpcClient(_channel);
    }

    public string DataSource => _endpoint;

    public Task<DatabaseInfo> GetInfoAsync(CancellationToken ct = default)
        => InvokeAsync<DatabaseInfo>(RpcOperation.GetInfo, ct: ct);

    public Task<IReadOnlyList<string>> GetTableNamesAsync(CancellationToken ct = default)
        => InvokeAsync<IReadOnlyList<string>>(RpcOperation.GetTableNames, ct: ct);

    public Task<TableSchema?> GetTableSchemaAsync(string tableName, CancellationToken ct = default)
        => InvokeAsync<TableSchema?>(RpcOperation.GetTableSchema, new TableNameRequest(tableName), ct);

    public Task<int> GetRowCountAsync(string tableName, CancellationToken ct = default)
        => InvokeAsync<int>(RpcOperation.GetRowCount, new TableNameRequest(tableName), ct);

    public Task<TableBrowseResult> BrowseTableAsync(string tableName, int page = 1, int pageSize = 50, CancellationToken ct = default)
        => InvokeAsync<TableBrowseResult>(RpcOperation.BrowseTable, new PagedTableRequest(tableName, page, pageSize), ct);

    public Task<Dictionary<string, object?>?> GetRowByPkAsync(string tableName, string pkColumn, object pkValue, CancellationToken ct = default)
        => InvokeAsync<Dictionary<string, object?>?>(RpcOperation.GetRowByPk, new GetRowByPkRequest(tableName, pkColumn, pkValue), ct);

    public Task<int> InsertRowAsync(string tableName, Dictionary<string, object?> values, CancellationToken ct = default)
        => InvokeAsync<int>(RpcOperation.InsertRow, new InsertRowRequest(tableName, values), ct);

    public Task<int> UpdateRowAsync(string tableName, string pkColumn, object pkValue, Dictionary<string, object?> values, CancellationToken ct = default)
        => InvokeAsync<int>(RpcOperation.UpdateRow, new UpdateRowRequest(tableName, pkColumn, pkValue, values), ct);

    public Task<int> DeleteRowAsync(string tableName, string pkColumn, object pkValue, CancellationToken ct = default)
        => InvokeAsync<int>(RpcOperation.DeleteRow, new DeleteRowRequest(tableName, pkColumn, pkValue), ct);

    public Task DropTableAsync(string tableName, CancellationToken ct = default)
        => InvokeVoidAsync(RpcOperation.DropTable, new TableNameRequest(tableName), ct);

    public Task RenameTableAsync(string tableName, string newTableName, CancellationToken ct = default)
        => InvokeVoidAsync(RpcOperation.RenameTable, new RenameTableRequest(tableName, newTableName), ct);

    public Task AddColumnAsync(string tableName, string columnName, DbType type, bool notNull, CancellationToken ct = default)
        => InvokeVoidAsync(RpcOperation.AddColumn, new AddColumnRequest(tableName, columnName, type, notNull), ct);

    public Task DropColumnAsync(string tableName, string columnName, CancellationToken ct = default)
        => InvokeVoidAsync(RpcOperation.DropColumn, new DropColumnRequest(tableName, columnName), ct);

    public Task RenameColumnAsync(string tableName, string oldColumnName, string newColumnName, CancellationToken ct = default)
        => InvokeVoidAsync(RpcOperation.RenameColumn, new RenameColumnRequest(tableName, oldColumnName, newColumnName), ct);

    public Task<IReadOnlyList<IndexSchema>> GetIndexesAsync(CancellationToken ct = default)
        => InvokeAsync<IReadOnlyList<IndexSchema>>(RpcOperation.GetIndexes, ct: ct);

    public Task CreateIndexAsync(string indexName, string tableName, string columnName, bool isUnique, CancellationToken ct = default)
        => InvokeVoidAsync(RpcOperation.CreateIndex, new CreateIndexRequest(indexName, tableName, columnName, isUnique), ct);

    public Task UpdateIndexAsync(string existingIndexName, string newIndexName, string tableName, string columnName, bool isUnique, CancellationToken ct = default)
        => InvokeVoidAsync(RpcOperation.UpdateIndex, new UpdateIndexRequest(existingIndexName, newIndexName, tableName, columnName, isUnique), ct);

    public Task DropIndexAsync(string indexName, CancellationToken ct = default)
        => InvokeVoidAsync(RpcOperation.DropIndex, new NameRequest(indexName), ct);

    public Task<IReadOnlyList<string>> GetViewNamesAsync(CancellationToken ct = default)
        => InvokeAsync<IReadOnlyList<string>>(RpcOperation.GetViewNames, ct: ct);

    public Task<IReadOnlyList<ViewDefinition>> GetViewsAsync(CancellationToken ct = default)
        => InvokeAsync<IReadOnlyList<ViewDefinition>>(RpcOperation.GetViews, ct: ct);

    public Task<ViewDefinition?> GetViewAsync(string viewName, CancellationToken ct = default)
        => InvokeAsync<ViewDefinition?>(RpcOperation.GetView, new NameRequest(viewName), ct);

    public Task<string?> GetViewSqlAsync(string viewName, CancellationToken ct = default)
        => InvokeAsync<string?>(RpcOperation.GetViewSql, new NameRequest(viewName), ct);

    public Task<ViewBrowseResult> BrowseViewAsync(string viewName, int page = 1, int pageSize = 50, CancellationToken ct = default)
        => InvokeAsync<ViewBrowseResult>(RpcOperation.BrowseView, new PagedNameRequest(viewName, page, pageSize), ct);

    public Task CreateViewAsync(string viewName, string selectSql, CancellationToken ct = default)
        => InvokeVoidAsync(RpcOperation.CreateView, new CreateViewRequest(viewName, selectSql), ct);

    public Task UpdateViewAsync(string existingViewName, string newViewName, string selectSql, CancellationToken ct = default)
        => InvokeVoidAsync(RpcOperation.UpdateView, new UpdateViewRequest(existingViewName, newViewName, selectSql), ct);

    public Task DropViewAsync(string viewName, CancellationToken ct = default)
        => InvokeVoidAsync(RpcOperation.DropView, new NameRequest(viewName), ct);

    public Task<IReadOnlyList<TriggerSchema>> GetTriggersAsync(CancellationToken ct = default)
        => InvokeAsync<IReadOnlyList<TriggerSchema>>(RpcOperation.GetTriggers, ct: ct);

    public Task CreateTriggerAsync(string triggerName, string tableName, TriggerTiming timing, TriggerEvent triggerEvent, string bodySql, CancellationToken ct = default)
        => InvokeVoidAsync(RpcOperation.CreateTrigger, new CreateTriggerRequest(triggerName, tableName, timing, triggerEvent, bodySql), ct);

    public Task UpdateTriggerAsync(string existingTriggerName, string newTriggerName, string tableName, TriggerTiming timing, TriggerEvent triggerEvent, string bodySql, CancellationToken ct = default)
        => InvokeVoidAsync(RpcOperation.UpdateTrigger, new UpdateTriggerRequest(existingTriggerName, newTriggerName, tableName, timing, triggerEvent, bodySql), ct);

    public Task DropTriggerAsync(string triggerName, CancellationToken ct = default)
        => InvokeVoidAsync(RpcOperation.DropTrigger, new NameRequest(triggerName), ct);

    public Task<IReadOnlyList<SavedQueryDefinition>> GetSavedQueriesAsync(CancellationToken ct = default)
        => InvokeAsync<IReadOnlyList<SavedQueryDefinition>>(RpcOperation.GetSavedQueries, ct: ct);

    public Task<SavedQueryDefinition?> GetSavedQueryAsync(string name, CancellationToken ct = default)
        => InvokeAsync<SavedQueryDefinition?>(RpcOperation.GetSavedQuery, new NameRequest(name), ct);

    public Task<SavedQueryDefinition> UpsertSavedQueryAsync(string name, string sqlText, CancellationToken ct = default)
        => InvokeAsync<SavedQueryDefinition>(RpcOperation.UpsertSavedQuery, new UpsertSavedQueryRequest(name, sqlText), ct);

    public Task DeleteSavedQueryAsync(string name, CancellationToken ct = default)
        => InvokeVoidAsync(RpcOperation.DeleteSavedQuery, new NameRequest(name), ct);

    public Task<IReadOnlyList<ProcedureDefinition>> GetProceduresAsync(bool includeDisabled = true, CancellationToken ct = default)
        => InvokeAsync<IReadOnlyList<ProcedureDefinition>>(RpcOperation.GetProcedures, new GetProceduresRequest(includeDisabled), ct);

    public Task<ProcedureDefinition?> GetProcedureAsync(string name, CancellationToken ct = default)
        => InvokeAsync<ProcedureDefinition?>(RpcOperation.GetProcedure, new NameRequest(name), ct);

    public Task CreateProcedureAsync(ProcedureDefinition definition, CancellationToken ct = default)
        => InvokeVoidAsync(RpcOperation.CreateProcedure, new CreateProcedureRequest(definition), ct);

    public Task UpdateProcedureAsync(string existingName, ProcedureDefinition definition, CancellationToken ct = default)
        => InvokeVoidAsync(RpcOperation.UpdateProcedure, new UpdateProcedureRequest(existingName, definition), ct);

    public Task DeleteProcedureAsync(string name, CancellationToken ct = default)
        => InvokeVoidAsync(RpcOperation.DeleteProcedure, new NameRequest(name), ct);

    public Task<ProcedureExecutionResult> ExecuteProcedureAsync(string name, IReadOnlyDictionary<string, object?> args, CancellationToken ct = default)
        => InvokeAsync<ProcedureExecutionResult>(RpcOperation.ExecuteProcedure, new ExecuteProcedureRequest(name, args.ToDictionary(kvp => kvp.Key, kvp => kvp.Value)), ct);

    public Task<SqlExecutionResult> ExecuteSqlAsync(string sql, CancellationToken ct = default)
        => InvokeAsync<SqlExecutionResult>(RpcOperation.ExecuteSql, new SqlRequest(sql), ct);

    public Task<TransactionSessionInfo> BeginTransactionAsync(CancellationToken ct = default)
        => InvokeAsync<TransactionSessionInfo>(RpcOperation.BeginTransaction, ct: ct);

    public Task<SqlExecutionResult> ExecuteInTransactionAsync(string transactionId, string sql, CancellationToken ct = default)
        => InvokeAsync<SqlExecutionResult>(RpcOperation.ExecuteInTransaction, new TransactionSqlRequest(transactionId, sql), ct);

    public Task CommitTransactionAsync(string transactionId, CancellationToken ct = default)
        => InvokeVoidAsync(RpcOperation.CommitTransaction, new TransactionIdRequest(transactionId), ct);

    public Task RollbackTransactionAsync(string transactionId, CancellationToken ct = default)
        => InvokeVoidAsync(RpcOperation.RollbackTransaction, new TransactionIdRequest(transactionId), ct);

    public Task<IReadOnlyList<string>> GetCollectionNamesAsync(CancellationToken ct = default)
        => InvokeAsync<IReadOnlyList<string>>(RpcOperation.GetCollectionNames, ct: ct);

    public Task<int> GetCollectionCountAsync(string collectionName, CancellationToken ct = default)
        => InvokeAsync<int>(RpcOperation.GetCollectionCount, new CollectionNameRequest(collectionName), ct);

    public Task<CollectionBrowseResult> BrowseCollectionAsync(string collectionName, int page = 1, int pageSize = 50, CancellationToken ct = default)
        => InvokeAsync<CollectionBrowseResult>(RpcOperation.BrowseCollection, new PagedNameRequest(collectionName, page, pageSize), ct);

    public Task<JsonElement?> GetDocumentAsync(string collectionName, string key, CancellationToken ct = default)
        => InvokeAsync<JsonElement?>(RpcOperation.GetDocument, new GetDocumentRequest(collectionName, key), ct);

    public Task PutDocumentAsync(string collectionName, string key, JsonElement document, CancellationToken ct = default)
        => InvokeVoidAsync(RpcOperation.PutDocument, new PutDocumentRequest(collectionName, key, document), ct);

    public Task<bool> DeleteDocumentAsync(string collectionName, string key, CancellationToken ct = default)
        => InvokeAsync<bool>(RpcOperation.DeleteDocument, new DeleteDocumentRequest(collectionName, key), ct);

    public Task CheckpointAsync(CancellationToken ct = default)
        => InvokeVoidAsync(RpcOperation.Checkpoint, ct: ct);

    public Task<DatabaseInspectReport> InspectStorageAsync(string? databasePath = null, bool includePages = false, CancellationToken ct = default)
        => InvokeAsync<DatabaseInspectReport>(RpcOperation.InspectStorage, new InspectStorageRequest(databasePath, includePages), ct);

    public Task<WalInspectReport> CheckWalAsync(string? databasePath = null, CancellationToken ct = default)
        => InvokeAsync<WalInspectReport>(RpcOperation.CheckWal, new CheckWalRequest(databasePath), ct);

    public Task<PageInspectReport> InspectPageAsync(uint pageId, bool includeHex = false, string? databasePath = null, CancellationToken ct = default)
        => InvokeAsync<PageInspectReport>(RpcOperation.InspectPage, new InspectPageRequest(pageId, includeHex, databasePath), ct);

    public Task<IndexInspectReport> CheckIndexesAsync(string? databasePath = null, string? indexName = null, int? sampleSize = null, CancellationToken ct = default)
        => InvokeAsync<IndexInspectReport>(RpcOperation.CheckIndexes, new CheckIndexesRequest(databasePath, indexName, sampleSize), ct);

    public ValueTask DisposeAsync()
    {
        _channel.Dispose();
        return ValueTask.CompletedTask;
    }

    private Task InvokeVoidAsync(RpcOperation operation, object? request = null, CancellationToken ct = default)
        => InvokeCoreAsync(operation, request, ct);

    private async Task<TResult> InvokeAsync<TResult>(RpcOperation operation, object? request = null, CancellationToken ct = default)
    {
        InvokeResponse response = await InvokeCoreAsync(operation, request, ct);
        return GrpcJson.Deserialize<TResult>(response.PayloadJson)!;
    }

    private async Task<InvokeResponse> InvokeCoreAsync(RpcOperation operation, object? request, CancellationToken ct)
    {
        try
        {
            var call = _client.InvokeAsync(
                new InvokeRequest
                {
                    Operation = operation,
                    PayloadJson = request is null ? string.Empty : GrpcJson.SerializeObject(request),
                },
                cancellationToken: ct);

            return await call.ResponseAsync.ConfigureAwait(false);
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled && ct.IsCancellationRequested)
        {
            throw new OperationCanceledException(ct);
        }
        catch (RpcException ex)
        {
            throw TranslateRpcException(ex);
        }
    }

    private static Exception TranslateRpcException(RpcException ex)
    {
        if (TryGetMetadata(ex.Trailers, GrpcMetadataNames.ErrorCode, out string? rawCode)
            && Enum.TryParse(rawCode, ignoreCase: true, out CoreErrorCode errorCode))
        {
            return new CoreDbException(errorCode, ex.Status.Detail, ex);
        }

        if (TryGetMetadata(ex.Trailers, GrpcMetadataNames.ErrorType, out string? errorType)
            && string.Equals(errorType, GrpcMetadataNames.ErrorTypeConfiguration, StringComparison.Ordinal))
        {
            return new CSharpDbClientConfigurationException(ex.Status.Detail);
        }

        return ex.StatusCode switch
        {
            StatusCode.InvalidArgument => new ArgumentException(ex.Status.Detail, ex),
            StatusCode.Unimplemented => new CSharpDbClientConfigurationException(ex.Status.Detail),
            _ => new CSharpDbClientException($"gRPC transport failed with status '{ex.StatusCode}': {ex.Status.Detail}", ex),
        };
    }

    private static bool TryGetMetadata(Metadata metadata, string key, out string? value)
    {
        var entry = metadata.FirstOrDefault(item => string.Equals(item.Key, key, StringComparison.Ordinal));
        if (entry is null)
        {
            value = null;
            return false;
        }

        value = entry.Value;
        return true;
    }
}
