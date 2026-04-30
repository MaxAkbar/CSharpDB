using System.Text.Json;
using CSharpDB.Client.Grpc;
using CSharpDB.Client.Models;
using CSharpDB.Storage.Diagnostics;
using Grpc.Core;
using Grpc.Net.Client;
using Grpc.Net.Client.Web;
using CoreDbException = CSharpDB.Primitives.CSharpDbException;
using CoreErrorCode = CSharpDB.Primitives.ErrorCode;
using Empty = Google.Protobuf.WellKnownTypes.Empty;

namespace CSharpDB.Client.Internal;

internal sealed class GrpcTransportClient : ICSharpDbClient
{
    private static readonly Empty EmptyRequest = new();

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
        else if (endpoint.Scheme.Equals(Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase))
        {
            channelOptions.HttpHandler = new GrpcWebHandler(
                GrpcWebMode.GrpcWeb,
                new HttpClientHandler());
            channelOptions.HttpVersion = System.Net.HttpVersion.Version11;
            channelOptions.HttpVersionPolicy = HttpVersionPolicy.RequestVersionExact;
        }

        _channel = GrpcChannel.ForAddress(endpoint, channelOptions);
        _client = new CSharpDbRpc.CSharpDbRpcClient(_channel);
    }

    public string DataSource => _endpoint;

    public Task<DatabaseInfo> GetInfoAsync(CancellationToken ct = default)
        => CallAsync(_client.GetInfoAsync(EmptyRequest, cancellationToken: ct), GrpcModelMapper.ToModel, ct);

    public Task<IReadOnlyList<string>> GetTableNamesAsync(CancellationToken ct = default)
        => CallAsync(_client.GetTableNamesAsync(EmptyRequest, cancellationToken: ct), GrpcModelMapper.ToStringList, ct);

    public Task<TableSchema?> GetTableSchemaAsync(string tableName, CancellationToken ct = default)
        => CallAsync(_client.GetTableSchemaAsync(new TableNameRequest { TableName = tableName }, cancellationToken: ct),
            response => response.Value is null ? null : GrpcModelMapper.ToModel(response.Value),
            ct);

    public Task<int> GetRowCountAsync(string tableName, CancellationToken ct = default)
        => CallAsync(_client.GetRowCountAsync(new TableNameRequest { TableName = tableName }, cancellationToken: ct), response => response.Value, ct);

    public Task<TableBrowseResult> BrowseTableAsync(string tableName, int page = 1, int pageSize = 50, CancellationToken ct = default)
        => CallAsync(_client.BrowseTableAsync(new PagedTableRequest
        {
            TableName = tableName,
            Page = page,
            PageSize = pageSize,
        }, cancellationToken: ct), GrpcModelMapper.ToModel, ct);

    public Task<Dictionary<string, object?>?> GetRowByPkAsync(string tableName, string pkColumn, object pkValue, CancellationToken ct = default)
        => CallAsync(_client.GetRowByPkAsync(new GetRowByPkRequest
        {
            TableName = tableName,
            PkColumn = pkColumn,
            PkValue = GrpcValueMapper.ToMessage(pkValue),
        }, cancellationToken: ct), response => response.Value is null ? null : GrpcValueMapper.ToDictionary(response.Value), ct);

    public Task<int> InsertRowAsync(string tableName, Dictionary<string, object?> values, CancellationToken ct = default)
        => CallAsync(_client.InsertRowAsync(new InsertRowRequest
        {
            TableName = tableName,
            Values = GrpcValueMapper.ToObject(values),
        }, cancellationToken: ct), response => response.Value, ct);

    public Task<int> UpdateRowAsync(string tableName, string pkColumn, object pkValue, Dictionary<string, object?> values, CancellationToken ct = default)
        => CallAsync(_client.UpdateRowAsync(new UpdateRowRequest
        {
            TableName = tableName,
            PkColumn = pkColumn,
            PkValue = GrpcValueMapper.ToMessage(pkValue),
            Values = GrpcValueMapper.ToObject(values),
        }, cancellationToken: ct), response => response.Value, ct);

    public Task<int> DeleteRowAsync(string tableName, string pkColumn, object pkValue, CancellationToken ct = default)
        => CallAsync(_client.DeleteRowAsync(new DeleteRowRequest
        {
            TableName = tableName,
            PkColumn = pkColumn,
            PkValue = GrpcValueMapper.ToMessage(pkValue),
        }, cancellationToken: ct), response => response.Value, ct);

    public Task DropTableAsync(string tableName, CancellationToken ct = default)
        => CallEmptyAsync(_client.DropTableAsync(new TableNameRequest { TableName = tableName }, cancellationToken: ct), ct);

    public Task RenameTableAsync(string tableName, string newTableName, CancellationToken ct = default)
        => CallEmptyAsync(_client.RenameTableAsync(new RenameTableRequest
        {
            TableName = tableName,
            NewTableName = newTableName,
        }, cancellationToken: ct), ct);

    public Task AddColumnAsync(string tableName, string columnName, DbType type, bool notNull, CancellationToken ct = default)
        => AddColumnAsync(tableName, columnName, type, notNull, collation: null, ct);

    public Task AddColumnAsync(string tableName, string columnName, DbType type, bool notNull, string? collation, CancellationToken ct = default)
        => CallEmptyAsync(_client.AddColumnAsync(new AddColumnRequest
        {
            TableName = tableName,
            ColumnName = columnName,
            Type = GrpcModelMapper.ToMessage(type),
            NotNull = notNull,
            Collation = collation ?? string.Empty,
        }, cancellationToken: ct), ct);

    public Task DropColumnAsync(string tableName, string columnName, CancellationToken ct = default)
        => CallEmptyAsync(_client.DropColumnAsync(new DropColumnRequest
        {
            TableName = tableName,
            ColumnName = columnName,
        }, cancellationToken: ct), ct);

    public Task RenameColumnAsync(string tableName, string oldColumnName, string newColumnName, CancellationToken ct = default)
        => CallEmptyAsync(_client.RenameColumnAsync(new RenameColumnRequest
        {
            TableName = tableName,
            OldColumnName = oldColumnName,
            NewColumnName = newColumnName,
        }, cancellationToken: ct), ct);

    public Task<IReadOnlyList<IndexSchema>> GetIndexesAsync(CancellationToken ct = default)
        => CallAsync(_client.GetIndexesAsync(EmptyRequest, cancellationToken: ct), response => (IReadOnlyList<IndexSchema>)response.Items.Select(GrpcModelMapper.ToModel).ToList(), ct);

    public Task CreateIndexAsync(string indexName, string tableName, string columnName, bool isUnique, CancellationToken ct = default)
        => CreateIndexAsync(indexName, tableName, columnName, isUnique, collation: null, ct);

    public Task CreateIndexAsync(string indexName, string tableName, string columnName, bool isUnique, string? collation, CancellationToken ct = default)
        => CallEmptyAsync(_client.CreateIndexAsync(new CreateIndexRequest
        {
            IndexName = indexName,
            TableName = tableName,
            ColumnName = columnName,
            IsUnique = isUnique,
            Collation = collation ?? string.Empty,
        }, cancellationToken: ct), ct);

    public Task UpdateIndexAsync(string existingIndexName, string newIndexName, string tableName, string columnName, bool isUnique, CancellationToken ct = default)
        => UpdateIndexAsync(existingIndexName, newIndexName, tableName, columnName, isUnique, collation: null, ct);

    public Task UpdateIndexAsync(string existingIndexName, string newIndexName, string tableName, string columnName, bool isUnique, string? collation, CancellationToken ct = default)
        => CallEmptyAsync(_client.UpdateIndexAsync(new UpdateIndexRequest
        {
            ExistingIndexName = existingIndexName,
            NewIndexName = newIndexName,
            TableName = tableName,
            ColumnName = columnName,
            IsUnique = isUnique,
            Collation = collation ?? string.Empty,
        }, cancellationToken: ct), ct);

    public Task DropIndexAsync(string indexName, CancellationToken ct = default)
        => CallEmptyAsync(_client.DropIndexAsync(new NameRequest { Name = indexName }, cancellationToken: ct), ct);

    public Task<IReadOnlyList<string>> GetViewNamesAsync(CancellationToken ct = default)
        => CallAsync(_client.GetViewNamesAsync(EmptyRequest, cancellationToken: ct), GrpcModelMapper.ToStringList, ct);

    public Task<IReadOnlyList<ViewDefinition>> GetViewsAsync(CancellationToken ct = default)
        => CallAsync(_client.GetViewsAsync(EmptyRequest, cancellationToken: ct), response => (IReadOnlyList<ViewDefinition>)response.Items.Select(GrpcModelMapper.ToModel).ToList(), ct);

    public Task<ViewDefinition?> GetViewAsync(string viewName, CancellationToken ct = default)
        => CallAsync(_client.GetViewAsync(new NameRequest { Name = viewName }, cancellationToken: ct),
            response => response.Value is null ? null : GrpcModelMapper.ToModel(response.Value),
            ct);

    public Task<string?> GetViewSqlAsync(string viewName, CancellationToken ct = default)
        => CallAsync(_client.GetViewSqlAsync(new NameRequest { Name = viewName }, cancellationToken: ct), response => (string?)response.Value, ct);

    public Task<ViewBrowseResult> BrowseViewAsync(string viewName, int page = 1, int pageSize = 50, CancellationToken ct = default)
        => CallAsync(_client.BrowseViewAsync(new PagedNameRequest
        {
            Name = viewName,
            Page = page,
            PageSize = pageSize,
        }, cancellationToken: ct), GrpcModelMapper.ToModel, ct);

    public Task CreateViewAsync(string viewName, string selectSql, CancellationToken ct = default)
        => CallEmptyAsync(_client.CreateViewAsync(new CreateViewRequest
        {
            ViewName = viewName,
            SelectSql = selectSql,
        }, cancellationToken: ct), ct);

    public Task UpdateViewAsync(string existingViewName, string newViewName, string selectSql, CancellationToken ct = default)
        => CallEmptyAsync(_client.UpdateViewAsync(new UpdateViewRequest
        {
            ExistingViewName = existingViewName,
            NewViewName = newViewName,
            SelectSql = selectSql,
        }, cancellationToken: ct), ct);

    public Task DropViewAsync(string viewName, CancellationToken ct = default)
        => CallEmptyAsync(_client.DropViewAsync(new NameRequest { Name = viewName }, cancellationToken: ct), ct);

    public Task<IReadOnlyList<TriggerSchema>> GetTriggersAsync(CancellationToken ct = default)
        => CallAsync(_client.GetTriggersAsync(EmptyRequest, cancellationToken: ct), response => (IReadOnlyList<TriggerSchema>)response.Items.Select(GrpcModelMapper.ToModel).ToList(), ct);

    public Task CreateTriggerAsync(string triggerName, string tableName, TriggerTiming timing, TriggerEvent triggerEvent, string bodySql, CancellationToken ct = default)
        => CallEmptyAsync(_client.CreateTriggerAsync(new CreateTriggerRequest
        {
            TriggerName = triggerName,
            TableName = tableName,
            Timing = GrpcModelMapper.ToMessage(timing),
            TriggerEvent = GrpcModelMapper.ToMessage(triggerEvent),
            BodySql = bodySql,
        }, cancellationToken: ct), ct);

    public Task UpdateTriggerAsync(string existingTriggerName, string newTriggerName, string tableName, TriggerTiming timing, TriggerEvent triggerEvent, string bodySql, CancellationToken ct = default)
        => CallEmptyAsync(_client.UpdateTriggerAsync(new UpdateTriggerRequest
        {
            ExistingTriggerName = existingTriggerName,
            NewTriggerName = newTriggerName,
            TableName = tableName,
            Timing = GrpcModelMapper.ToMessage(timing),
            TriggerEvent = GrpcModelMapper.ToMessage(triggerEvent),
            BodySql = bodySql,
        }, cancellationToken: ct), ct);

    public Task DropTriggerAsync(string triggerName, CancellationToken ct = default)
        => CallEmptyAsync(_client.DropTriggerAsync(new NameRequest { Name = triggerName }, cancellationToken: ct), ct);

    public Task<IReadOnlyList<SavedQueryDefinition>> GetSavedQueriesAsync(CancellationToken ct = default)
        => CallAsync(_client.GetSavedQueriesAsync(EmptyRequest, cancellationToken: ct), response => (IReadOnlyList<SavedQueryDefinition>)response.Items.Select(GrpcModelMapper.ToModel).ToList(), ct);

    public Task<SavedQueryDefinition?> GetSavedQueryAsync(string name, CancellationToken ct = default)
        => CallAsync(_client.GetSavedQueryAsync(new NameRequest { Name = name }, cancellationToken: ct),
            response => response.Value is null ? null : GrpcModelMapper.ToModel(response.Value),
            ct);

    public Task<SavedQueryDefinition> UpsertSavedQueryAsync(string name, string sqlText, CancellationToken ct = default)
        => CallAsync(_client.UpsertSavedQueryAsync(new UpsertSavedQueryRequest
        {
            Name = name,
            SqlText = sqlText,
        }, cancellationToken: ct), GrpcModelMapper.ToModel, ct);

    public Task DeleteSavedQueryAsync(string name, CancellationToken ct = default)
        => CallEmptyAsync(_client.DeleteSavedQueryAsync(new NameRequest { Name = name }, cancellationToken: ct), ct);

    public Task<IReadOnlyList<ProcedureDefinition>> GetProceduresAsync(bool includeDisabled = true, CancellationToken ct = default)
        => CallAsync(_client.GetProceduresAsync(new GetProceduresRequest { IncludeDisabled = includeDisabled }, cancellationToken: ct),
            response => (IReadOnlyList<ProcedureDefinition>)response.Items.Select(GrpcModelMapper.ToModel).ToList(),
            ct);

    public Task<ProcedureDefinition?> GetProcedureAsync(string name, CancellationToken ct = default)
        => CallAsync(_client.GetProcedureAsync(new NameRequest { Name = name }, cancellationToken: ct),
            response => response.Value is null ? null : GrpcModelMapper.ToModel(response.Value),
            ct);

    public Task CreateProcedureAsync(ProcedureDefinition definition, CancellationToken ct = default)
        => CallEmptyAsync(_client.CreateProcedureAsync(new CreateProcedureRequest
        {
            Definition = GrpcModelMapper.ToMessage(definition),
        }, cancellationToken: ct), ct);

    public Task UpdateProcedureAsync(string existingName, ProcedureDefinition definition, CancellationToken ct = default)
        => CallEmptyAsync(_client.UpdateProcedureAsync(new UpdateProcedureRequest
        {
            ExistingName = existingName,
            Definition = GrpcModelMapper.ToMessage(definition),
        }, cancellationToken: ct), ct);

    public Task DeleteProcedureAsync(string name, CancellationToken ct = default)
        => CallEmptyAsync(_client.DeleteProcedureAsync(new NameRequest { Name = name }, cancellationToken: ct), ct);

    public Task<ProcedureExecutionResult> ExecuteProcedureAsync(string name, IReadOnlyDictionary<string, object?> args, CancellationToken ct = default)
        => CallAsync(_client.ExecuteProcedureAsync(new ExecuteProcedureRequest
        {
            Name = name,
            Args = GrpcValueMapper.ToObject(args),
        }, cancellationToken: ct), GrpcModelMapper.ToModel, ct);

    public Task<SqlExecutionResult> ExecuteSqlAsync(string sql, CancellationToken ct = default)
        => CallAsync(_client.ExecuteSqlAsync(new SqlRequest { Sql = sql }, cancellationToken: ct), GrpcModelMapper.ToModel, ct);

    public Task<TransactionSessionInfo> BeginTransactionAsync(CancellationToken ct = default)
        => CallAsync(_client.BeginTransactionAsync(EmptyRequest, cancellationToken: ct), GrpcModelMapper.ToModel, ct);

    public Task<SqlExecutionResult> ExecuteInTransactionAsync(string transactionId, string sql, CancellationToken ct = default)
        => CallAsync(_client.ExecuteInTransactionAsync(new TransactionSqlRequest
        {
            TransactionId = transactionId,
            Sql = sql,
        }, cancellationToken: ct), GrpcModelMapper.ToModel, ct);

    public Task CommitTransactionAsync(string transactionId, CancellationToken ct = default)
        => CallEmptyAsync(_client.CommitTransactionAsync(new TransactionIdRequest { TransactionId = transactionId }, cancellationToken: ct), ct);

    public Task RollbackTransactionAsync(string transactionId, CancellationToken ct = default)
        => CallEmptyAsync(_client.RollbackTransactionAsync(new TransactionIdRequest { TransactionId = transactionId }, cancellationToken: ct), ct);

    public Task<IReadOnlyList<string>> GetCollectionNamesAsync(CancellationToken ct = default)
        => CallAsync(_client.GetCollectionNamesAsync(EmptyRequest, cancellationToken: ct), GrpcModelMapper.ToStringList, ct);

    public Task<int> GetCollectionCountAsync(string collectionName, CancellationToken ct = default)
        => CallAsync(_client.GetCollectionCountAsync(new CollectionNameRequest { CollectionName = collectionName }, cancellationToken: ct), response => response.Value, ct);

    public Task<CollectionBrowseResult> BrowseCollectionAsync(string collectionName, int page = 1, int pageSize = 50, CancellationToken ct = default)
        => CallAsync(_client.BrowseCollectionAsync(new PagedNameRequest
        {
            Name = collectionName,
            Page = page,
            PageSize = pageSize,
        }, cancellationToken: ct), GrpcModelMapper.ToModel, ct);

    public Task<JsonElement?> GetDocumentAsync(string collectionName, string key, CancellationToken ct = default)
        => CallAsync(_client.GetDocumentAsync(new GetDocumentRequest
        {
            CollectionName = collectionName,
            Key = key,
        }, cancellationToken: ct), response => response.Value is null ? (JsonElement?)null : GrpcValueMapper.ToJsonElement(response.Value), ct);

    public Task PutDocumentAsync(string collectionName, string key, JsonElement document, CancellationToken ct = default)
        => CallEmptyAsync(_client.PutDocumentAsync(new PutDocumentRequest
        {
            CollectionName = collectionName,
            Key = key,
            Document = GrpcValueMapper.ToMessage(document),
        }, cancellationToken: ct), ct);

    public Task<bool> DeleteDocumentAsync(string collectionName, string key, CancellationToken ct = default)
        => CallAsync(_client.DeleteDocumentAsync(new DeleteDocumentRequest
        {
            CollectionName = collectionName,
            Key = key,
        }, cancellationToken: ct), response => response.Value, ct);

    public Task DropCollectionAsync(string collectionName, CancellationToken ct = default)
        => CallEmptyAsync(_client.DropCollectionAsync(new CollectionNameRequest { CollectionName = collectionName }, cancellationToken: ct), ct);

    public Task CheckpointAsync(CancellationToken ct = default)
        => CallEmptyAsync(_client.CheckpointAsync(EmptyRequest, cancellationToken: ct), ct);

    public Task<BackupResult> BackupAsync(BackupRequest request, CancellationToken ct = default)
        => CallAsync(_client.BackupAsync(GrpcModelMapper.ToMessage(request), cancellationToken: ct), GrpcModelMapper.ToModel, ct);

    public Task<RestoreResult> RestoreAsync(RestoreRequest request, CancellationToken ct = default)
        => CallAsync(_client.RestoreAsync(GrpcModelMapper.ToMessage(request), cancellationToken: ct), GrpcModelMapper.ToModel, ct);

    public Task<ForeignKeyMigrationResult> MigrateForeignKeysAsync(ForeignKeyMigrationRequest request, CancellationToken ct = default)
        => CallAsync(_client.MigrateForeignKeysAsync(GrpcModelMapper.ToMessage(request), cancellationToken: ct), GrpcModelMapper.ToModel, ct);

    public Task<DatabaseMaintenanceReport> GetMaintenanceReportAsync(CancellationToken ct = default)
        => CallAsync(_client.GetMaintenanceReportAsync(EmptyRequest, cancellationToken: ct), GrpcModelMapper.ToModel, ct);

    public Task<ReindexResult> ReindexAsync(ReindexRequest request, CancellationToken ct = default)
        => CallAsync(_client.ReindexAsync(GrpcModelMapper.ToMessage(request), cancellationToken: ct), GrpcModelMapper.ToModel, ct);

    public Task<VacuumResult> VacuumAsync(CancellationToken ct = default)
        => CallAsync(_client.VacuumAsync(EmptyRequest, cancellationToken: ct), GrpcModelMapper.ToModel, ct);

    public Task<DatabaseInspectReport> InspectStorageAsync(string? databasePath = null, bool includePages = false, CancellationToken ct = default)
        => CallAsync(_client.InspectStorageAsync(new InspectStorageRequest
        {
            DatabasePath = databasePath ?? string.Empty,
            IncludePages = includePages,
        }, cancellationToken: ct), GrpcModelMapper.ToModel, ct);

    public Task<WalInspectReport> CheckWalAsync(string? databasePath = null, CancellationToken ct = default)
        => CallAsync(_client.CheckWalAsync(new CheckWalRequest
        {
            DatabasePath = databasePath ?? string.Empty,
        }, cancellationToken: ct), GrpcModelMapper.ToModel, ct);

    public Task<PageInspectReport> InspectPageAsync(uint pageId, bool includeHex = false, string? databasePath = null, CancellationToken ct = default)
        => CallAsync(_client.InspectPageAsync(new InspectPageRequest
        {
            PageId = pageId,
            IncludeHex = includeHex,
            DatabasePath = databasePath ?? string.Empty,
        }, cancellationToken: ct), GrpcModelMapper.ToModel, ct);

    public Task<IndexInspectReport> CheckIndexesAsync(string? databasePath = null, string? indexName = null, int? sampleSize = null, CancellationToken ct = default)
        => CallAsync(_client.CheckIndexesAsync(new CheckIndexesRequest
        {
            DatabasePath = databasePath ?? string.Empty,
            IndexName = indexName ?? string.Empty,
            SampleSize = sampleSize,
        }, cancellationToken: ct), GrpcModelMapper.ToModel, ct);

    public ValueTask DisposeAsync()
    {
        _channel.Dispose();
        return ValueTask.CompletedTask;
    }

    private async Task CallEmptyAsync(AsyncUnaryCall<Empty> call, CancellationToken ct)
    {
        try
        {
            await call.ResponseAsync.ConfigureAwait(false);
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

    private async Task<TResult> CallAsync<TResponse, TResult>(AsyncUnaryCall<TResponse> call, Func<TResponse, TResult> map, CancellationToken ct)
    {
        try
        {
            TResponse response = await call.ResponseAsync.ConfigureAwait(false);
            return map(response);
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
            && System.Enum.TryParse(rawCode, ignoreCase: true, out CoreErrorCode errorCode))
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
