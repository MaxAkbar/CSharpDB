using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using CSharpDB.Client.Models;
using CSharpDB.Storage.Diagnostics;

namespace CSharpDB.Client.Internal;

internal sealed partial class HttpTransportClient : ICSharpDbClient
{
    private static readonly JsonSerializerOptions s_jsonOptions = CreateJsonOptions();

    private readonly Uri _endpoint;
    private readonly HttpClient _httpClient;
    private readonly bool _disposeHttpClient;

    public HttpTransportClient(Uri endpoint, HttpClient? httpClient = null)
    {
        ArgumentNullException.ThrowIfNull(endpoint);

        _endpoint = EnsureTrailingSlash(endpoint);
        if (httpClient is null)
        {
            _httpClient = new HttpClient();
            _disposeHttpClient = true;
        }
        else
        {
            _httpClient = httpClient;
        }
    }

    public string DataSource => _endpoint.AbsoluteUri;

    public async Task<DatabaseInfo> GetInfoAsync(CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Get, BuildUri("api/info"), payload: null, ct);
        var payload = await ReadRequiredAsync<ApiDatabaseInfoResponse>(response, ct);
        return new DatabaseInfo
        {
            DataSource = payload.DataSource,
            TableCount = payload.TableCount,
            IndexCount = payload.IndexCount,
            ViewCount = payload.ViewCount,
            TriggerCount = payload.TriggerCount,
            ProcedureCount = payload.ProcedureCount,
            CollectionCount = payload.CollectionCount,
            SavedQueryCount = payload.SavedQueryCount,
        };
    }

    public async Task<IReadOnlyList<string>> GetTableNamesAsync(CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Get, BuildUri("api/tables"), payload: null, ct);
        return await ReadRequiredAsync<List<string>>(response, ct);
    }

    public async Task<TableSchema?> GetTableSchemaAsync(string tableName, CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Get, BuildUri($"api/tables/{Escape(tableName)}/schema"), payload: null, ct);
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            var error = await ReadErrorAsync(response, ct);
            if (error.IsEndpointError)
                return null;
        }

        var payload = await ReadRequiredAsync<ApiTableSchemaResponse>(response, ct);
        return MapTableSchema(payload);
    }

    public async Task<int> GetRowCountAsync(string tableName, CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Get, BuildUri($"api/tables/{Escape(tableName)}/count"), payload: null, ct);
        var payload = await ReadRequiredAsync<ApiRowCountResponse>(response, ct);
        return payload.Count;
    }

    public async Task<TableBrowseResult> BrowseTableAsync(string tableName, int page = 1, int pageSize = 50, CancellationToken ct = default)
    {
        TableSchema schema = await GetTableSchemaAsync(tableName, ct)
            ?? throw new CSharpDbClientException($"Table '{tableName}' was not found.");

        using var response = await SendAsync(
            HttpMethod.Get,
            BuildUri(
                $"api/tables/{Escape(tableName)}/rows",
                Q("page", page.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                Q("pageSize", pageSize.ToString(System.Globalization.CultureInfo.InvariantCulture))),
            payload: null,
            ct);

        var payload = await ReadRequiredAsync<ApiBrowseResponse>(response, ct);
        return new TableBrowseResult
        {
            TableName = tableName,
            Schema = schema,
            Rows = MapRows(payload.ColumnNames, payload.Rows),
            TotalRows = payload.TotalRows,
            Page = payload.Page,
            PageSize = payload.PageSize,
        };
    }

    public async Task<Dictionary<string, object?>?> GetRowByPkAsync(string tableName, string pkColumn, object pkValue, CancellationToken ct = default)
    {
        using var response = await SendAsync(
            HttpMethod.Get,
            BuildUri($"api/tables/{Escape(tableName)}/rows/{Escape(ConvertKey(pkValue))}", Q("pkColumn", pkColumn)),
            payload: null,
            ct);

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            var error = await ReadErrorAsync(response, ct);
            if (error.IsEndpointError)
                return null;
        }

        var payload = await ReadRequiredAsync<Dictionary<string, object?>>(response, ct);
        return NormalizeDictionary(payload);
    }

    public async Task<int> InsertRowAsync(string tableName, Dictionary<string, object?> values, CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Post, BuildUri($"api/tables/{Escape(tableName)}/rows"), new { Values = values }, ct);
        var payload = await ReadRequiredAsync<ApiMutationResponse>(response, ct);
        return payload.RowsAffected;
    }

    public async Task<int> UpdateRowAsync(string tableName, string pkColumn, object pkValue, Dictionary<string, object?> values, CancellationToken ct = default)
    {
        using var response = await SendAsync(
            HttpMethod.Put,
            BuildUri($"api/tables/{Escape(tableName)}/rows/{Escape(ConvertKey(pkValue))}", Q("pkColumn", pkColumn)),
            new { Values = values },
            ct);
        var payload = await ReadRequiredAsync<ApiMutationResponse>(response, ct);
        return payload.RowsAffected;
    }

    public async Task<int> DeleteRowAsync(string tableName, string pkColumn, object pkValue, CancellationToken ct = default)
    {
        using var response = await SendAsync(
            HttpMethod.Delete,
            BuildUri($"api/tables/{Escape(tableName)}/rows/{Escape(ConvertKey(pkValue))}", Q("pkColumn", pkColumn)),
            payload: null,
            ct);

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            var error = await ReadErrorAsync(response, ct);
            if (error.IsEndpointError)
                return 0;
        }

        var payload = await ReadRequiredAsync<ApiMutationResponse>(response, ct);
        return payload.RowsAffected;
    }

    public async Task DropTableAsync(string tableName, CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Delete, BuildUri($"api/tables/{Escape(tableName)}"), payload: null, ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task RenameTableAsync(string tableName, string newTableName, CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Patch, BuildUri($"api/tables/{Escape(tableName)}/rename"), new { NewName = newTableName }, ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task AddColumnAsync(string tableName, string columnName, DbType type, bool notNull, CancellationToken ct = default)
    {
        using var response = await SendAsync(
            HttpMethod.Post,
            BuildUri($"api/tables/{Escape(tableName)}/columns"),
            new { ColumnName = columnName, Type = type.ToString(), NotNull = notNull },
            ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task DropColumnAsync(string tableName, string columnName, CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Delete, BuildUri($"api/tables/{Escape(tableName)}/columns/{Escape(columnName)}"), payload: null, ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task RenameColumnAsync(string tableName, string oldColumnName, string newColumnName, CancellationToken ct = default)
    {
        using var response = await SendAsync(
            HttpMethod.Patch,
            BuildUri($"api/tables/{Escape(tableName)}/columns/{Escape(oldColumnName)}/rename"),
            new { NewName = newColumnName },
            ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task<IReadOnlyList<IndexSchema>> GetIndexesAsync(CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Get, BuildUri("api/indexes"), payload: null, ct);
        var payload = await ReadRequiredAsync<List<ApiIndexResponse>>(response, ct);
        return payload.Select(MapIndex).ToList();
    }

    public async Task CreateIndexAsync(string indexName, string tableName, string columnName, bool isUnique, CancellationToken ct = default)
    {
        using var response = await SendAsync(
            HttpMethod.Post,
            BuildUri("api/indexes"),
            new { IndexName = indexName, TableName = tableName, ColumnName = columnName, IsUnique = isUnique },
            ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task UpdateIndexAsync(string existingIndexName, string newIndexName, string tableName, string columnName, bool isUnique, CancellationToken ct = default)
    {
        using var response = await SendAsync(
            HttpMethod.Put,
            BuildUri($"api/indexes/{Escape(existingIndexName)}"),
            new { NewIndexName = newIndexName, TableName = tableName, ColumnName = columnName, IsUnique = isUnique },
            ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task DropIndexAsync(string indexName, CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Delete, BuildUri($"api/indexes/{Escape(indexName)}"), payload: null, ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task<IReadOnlyList<string>> GetViewNamesAsync(CancellationToken ct = default)
        => (await GetViewsAsync(ct)).Select(view => view.Name).ToList();

    public async Task<IReadOnlyList<ViewDefinition>> GetViewsAsync(CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Get, BuildUri("api/views"), payload: null, ct);
        var payload = await ReadRequiredAsync<List<ApiViewResponse>>(response, ct);
        return payload.Select(MapView).ToList();
    }

    public async Task<ViewDefinition?> GetViewAsync(string viewName, CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Get, BuildUri($"api/views/{Escape(viewName)}"), payload: null, ct);
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            var error = await ReadErrorAsync(response, ct);
            if (error.IsEndpointError)
                return null;
        }

        var payload = await ReadRequiredAsync<ApiViewResponse>(response, ct);
        return MapView(payload);
    }

    public async Task<string?> GetViewSqlAsync(string viewName, CancellationToken ct = default)
        => (await GetViewAsync(viewName, ct))?.Sql;

    public async Task<ViewBrowseResult> BrowseViewAsync(string viewName, int page = 1, int pageSize = 50, CancellationToken ct = default)
    {
        using var response = await SendAsync(
            HttpMethod.Get,
            BuildUri(
                $"api/views/{Escape(viewName)}/rows",
                Q("page", page.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                Q("pageSize", pageSize.ToString(System.Globalization.CultureInfo.InvariantCulture))),
            payload: null,
            ct);

        var payload = await ReadRequiredAsync<ApiBrowseResponse>(response, ct);
        return new ViewBrowseResult
        {
            ViewName = viewName,
            ColumnNames = payload.ColumnNames,
            Rows = MapRows(payload.ColumnNames, payload.Rows),
            TotalRows = payload.TotalRows,
            Page = payload.Page,
            PageSize = payload.PageSize,
        };
    }

    public async Task CreateViewAsync(string viewName, string selectSql, CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Post, BuildUri("api/views"), new { ViewName = viewName, SelectSql = selectSql }, ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task UpdateViewAsync(string existingViewName, string newViewName, string selectSql, CancellationToken ct = default)
    {
        using var response = await SendAsync(
            HttpMethod.Put,
            BuildUri($"api/views/{Escape(existingViewName)}"),
            new { NewViewName = newViewName, SelectSql = selectSql },
            ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task DropViewAsync(string viewName, CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Delete, BuildUri($"api/views/{Escape(viewName)}"), payload: null, ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task<IReadOnlyList<TriggerSchema>> GetTriggersAsync(CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Get, BuildUri("api/triggers"), payload: null, ct);
        var payload = await ReadRequiredAsync<List<ApiTriggerResponse>>(response, ct);
        return payload.Select(MapTrigger).ToList();
    }

    public async Task CreateTriggerAsync(string triggerName, string tableName, TriggerTiming timing, TriggerEvent triggerEvent, string bodySql, CancellationToken ct = default)
    {
        using var response = await SendAsync(
            HttpMethod.Post,
            BuildUri("api/triggers"),
            new { TriggerName = triggerName, TableName = tableName, Timing = timing.ToString(), Event = triggerEvent.ToString(), BodySql = bodySql },
            ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task UpdateTriggerAsync(string existingTriggerName, string newTriggerName, string tableName, TriggerTiming timing, TriggerEvent triggerEvent, string bodySql, CancellationToken ct = default)
    {
        using var response = await SendAsync(
            HttpMethod.Put,
            BuildUri($"api/triggers/{Escape(existingTriggerName)}"),
            new { NewTriggerName = newTriggerName, TableName = tableName, Timing = timing.ToString(), Event = triggerEvent.ToString(), BodySql = bodySql },
            ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task DropTriggerAsync(string triggerName, CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Delete, BuildUri($"api/triggers/{Escape(triggerName)}"), payload: null, ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task<IReadOnlyList<SavedQueryDefinition>> GetSavedQueriesAsync(CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Get, BuildUri("api/saved-queries"), payload: null, ct);
        return await ReadRequiredAsync<List<SavedQueryDefinition>>(response, ct);
    }

    public async Task<SavedQueryDefinition?> GetSavedQueryAsync(string name, CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Get, BuildUri($"api/saved-queries/{Escape(name)}"), payload: null, ct);
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            var error = await ReadErrorAsync(response, ct);
            if (error.IsEndpointError)
                return null;
        }

        return await ReadRequiredAsync<SavedQueryDefinition>(response, ct);
    }

    public async Task<SavedQueryDefinition> UpsertSavedQueryAsync(string name, string sqlText, CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Put, BuildUri($"api/saved-queries/{Escape(name)}"), new { SqlText = sqlText }, ct);
        return await ReadRequiredAsync<SavedQueryDefinition>(response, ct);
    }

    public async Task DeleteSavedQueryAsync(string name, CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Delete, BuildUri($"api/saved-queries/{Escape(name)}"), payload: null, ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task<IReadOnlyList<ProcedureDefinition>> GetProceduresAsync(bool includeDisabled = true, CancellationToken ct = default)
    {
        using var response = await SendAsync(
            HttpMethod.Get,
            BuildUri("api/procedures", Q("includeDisabled", includeDisabled ? "true" : "false"), Q("details", "true")),
            payload: null,
            ct);
        var payload = await ReadRequiredAsync<List<ApiProcedureDetailResponse>>(response, ct);
        return payload.Select(MapProcedure).ToList();
    }

    public async Task<ProcedureDefinition?> GetProcedureAsync(string name, CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Get, BuildUri($"api/procedures/{Escape(name)}"), payload: null, ct);
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            var error = await ReadErrorAsync(response, ct);
            if (error.IsEndpointError)
                return null;
        }

        var payload = await ReadRequiredAsync<ApiProcedureDetailResponse>(response, ct);
        return MapProcedure(payload);
    }

    public async Task CreateProcedureAsync(ProcedureDefinition definition, CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Post, BuildUri("api/procedures"), CreateProcedurePayload(definition, newNameOverride: null), ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task UpdateProcedureAsync(string existingName, ProcedureDefinition definition, CancellationToken ct = default)
    {
        using var response = await SendAsync(
            HttpMethod.Put,
            BuildUri($"api/procedures/{Escape(existingName)}"),
            CreateProcedurePayload(definition, definition.Name),
            ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task DeleteProcedureAsync(string name, CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Delete, BuildUri($"api/procedures/{Escape(name)}"), payload: null, ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task<ProcedureExecutionResult> ExecuteProcedureAsync(string name, IReadOnlyDictionary<string, object?> args, CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Post, BuildUri($"api/procedures/{Escape(name)}/execute"), new { Args = args }, ct);
        if (response.IsSuccessStatusCode || response.StatusCode == HttpStatusCode.BadRequest)
        {
            var payload = await ReadRequiredBodyAsync<ApiProcedureExecutionResponse>(response, ct);
            return MapProcedureExecution(payload);
        }

        throw await CreateHttpExceptionAsync(response, ct);
    }

    public async Task<SqlExecutionResult> ExecuteSqlAsync(string sql, CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Post, BuildUri("api/sql/execute"), new { Sql = sql }, ct);
        if (response.IsSuccessStatusCode || response.StatusCode == HttpStatusCode.BadRequest)
        {
            var payload = await ReadRequiredBodyAsync<ApiSqlResultResponse>(response, ct);
            return MapSqlResult(payload);
        }

        throw await CreateHttpExceptionAsync(response, ct);
    }

    public async Task<TransactionSessionInfo> BeginTransactionAsync(CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Post, BuildUri("api/transactions"), payload: null, ct);
        return await ReadRequiredAsync<TransactionSessionInfo>(response, ct);
    }

    public async Task<SqlExecutionResult> ExecuteInTransactionAsync(string transactionId, string sql, CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Post, BuildUri($"api/transactions/{Escape(transactionId)}/execute"), new { Sql = sql }, ct);
        if (response.IsSuccessStatusCode || response.StatusCode == HttpStatusCode.BadRequest)
        {
            var payload = await ReadRequiredBodyAsync<ApiSqlResultResponse>(response, ct);
            return MapSqlResult(payload);
        }

        throw await CreateHttpExceptionAsync(response, ct);
    }

    public async Task CommitTransactionAsync(string transactionId, CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Post, BuildUri($"api/transactions/{Escape(transactionId)}/commit"), payload: null, ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task RollbackTransactionAsync(string transactionId, CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Post, BuildUri($"api/transactions/{Escape(transactionId)}/rollback"), payload: null, ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task<IReadOnlyList<string>> GetCollectionNamesAsync(CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Get, BuildUri("api/collections"), payload: null, ct);
        return await ReadRequiredAsync<List<string>>(response, ct);
    }

    public async Task<int> GetCollectionCountAsync(string collectionName, CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Get, BuildUri($"api/collections/{Escape(collectionName)}/count"), payload: null, ct);
        var payload = await ReadRequiredAsync<ApiCollectionCountResponse>(response, ct);
        return payload.Count;
    }

    public async Task<CollectionBrowseResult> BrowseCollectionAsync(string collectionName, int page = 1, int pageSize = 50, CancellationToken ct = default)
    {
        using var response = await SendAsync(
            HttpMethod.Get,
            BuildUri(
                $"api/collections/{Escape(collectionName)}",
                Q("page", page.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                Q("pageSize", pageSize.ToString(System.Globalization.CultureInfo.InvariantCulture))),
            payload: null,
            ct);
        return await ReadRequiredAsync<CollectionBrowseResult>(response, ct);
    }

    public async Task<JsonElement?> GetDocumentAsync(string collectionName, string key, CancellationToken ct = default)
    {
        using var response = await SendAsync(
            HttpMethod.Get,
            BuildUri($"api/collections/{Escape(collectionName)}/document", Q("key", key)),
            payload: null,
            ct);

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            var error = await ReadErrorAsync(response, ct);
            if (error.IsEndpointError)
                return null;
        }

        var payload = await ReadRequiredAsync<CollectionDocument>(response, ct);
        return payload.Document;
    }

    public async Task PutDocumentAsync(string collectionName, string key, JsonElement document, CancellationToken ct = default)
    {
        using var response = await SendAsync(
            HttpMethod.Put,
            BuildUri($"api/collections/{Escape(collectionName)}/document", Q("key", key)),
            new { Document = document },
            ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task<bool> DeleteDocumentAsync(string collectionName, string key, CancellationToken ct = default)
    {
        using var response = await SendAsync(
            HttpMethod.Delete,
            BuildUri($"api/collections/{Escape(collectionName)}/document", Q("key", key)),
            payload: null,
            ct);

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            var error = await ReadErrorAsync(response, ct);
            if (error.IsEndpointError)
                return false;
        }

        await EnsureSuccessAsync(response, ct);
        return true;
    }

    public async Task CheckpointAsync(CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Post, BuildUri("api/maintenance/checkpoint"), payload: null, ct);
        await EnsureSuccessAsync(response, ct);
    }

    public async Task<DatabaseMaintenanceReport> GetMaintenanceReportAsync(CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Get, BuildUri("api/maintenance/report"), payload: null, ct);
        return await ReadRequiredAsync<DatabaseMaintenanceReport>(response, ct);
    }

    public async Task<ReindexResult> ReindexAsync(ReindexRequest request, CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Post, BuildUri("api/maintenance/reindex"), request, ct);
        return await ReadRequiredAsync<ReindexResult>(response, ct);
    }

    public async Task<VacuumResult> VacuumAsync(CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Post, BuildUri("api/maintenance/vacuum"), payload: null, ct);
        return await ReadRequiredAsync<VacuumResult>(response, ct);
    }

    public async Task<DatabaseInspectReport> InspectStorageAsync(string? databasePath = null, bool includePages = false, CancellationToken ct = default)
    {
        using var response = await SendAsync(
            HttpMethod.Get,
            BuildUri("api/inspect", Q("includePages", includePages ? "true" : "false"), Q("path", databasePath)),
            payload: null,
            ct);
        return await ReadRequiredAsync<DatabaseInspectReport>(response, ct);
    }

    public async Task<WalInspectReport> CheckWalAsync(string? databasePath = null, CancellationToken ct = default)
    {
        using var response = await SendAsync(HttpMethod.Get, BuildUri("api/inspect/wal", Q("path", databasePath)), payload: null, ct);
        return await ReadRequiredAsync<WalInspectReport>(response, ct);
    }

    public async Task<PageInspectReport> InspectPageAsync(uint pageId, bool includeHex = false, string? databasePath = null, CancellationToken ct = default)
    {
        using var response = await SendAsync(
            HttpMethod.Get,
            BuildUri(
                $"api/inspect/page/{pageId.ToString(System.Globalization.CultureInfo.InvariantCulture)}",
                Q("hex", includeHex ? "true" : "false"),
                Q("path", databasePath)),
            payload: null,
            ct);
        return await ReadRequiredAsync<PageInspectReport>(response, ct);
    }

    public async Task<IndexInspectReport> CheckIndexesAsync(string? databasePath = null, string? indexName = null, int? sampleSize = null, CancellationToken ct = default)
    {
        using var response = await SendAsync(
            HttpMethod.Get,
            BuildUri(
                "api/inspect/indexes",
                Q("path", databasePath),
                Q("index", indexName),
                Q("sample", sampleSize?.ToString(System.Globalization.CultureInfo.InvariantCulture))),
            payload: null,
            ct);
        return await ReadRequiredAsync<IndexInspectReport>(response, ct);
    }

    public ValueTask DisposeAsync()
    {
        if (_disposeHttpClient)
            _httpClient.Dispose();

        return ValueTask.CompletedTask;
    }

    private async Task<HttpResponseMessage> SendAsync(HttpMethod method, Uri uri, object? payload, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(method, uri);
        if (payload is not null)
            request.Content = JsonContent.Create(payload, options: s_jsonOptions);

        return await _httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct);
    }

    private async Task EnsureSuccessAsync(HttpResponseMessage response, CancellationToken ct)
    {
        if (response.IsSuccessStatusCode)
            return;

        throw await CreateHttpExceptionAsync(response, ct);
    }

    private async Task<T> ReadRequiredAsync<T>(HttpResponseMessage response, CancellationToken ct)
    {
        if (!response.IsSuccessStatusCode)
            throw await CreateHttpExceptionAsync(response, ct);

        return await ReadRequiredBodyAsync<T>(response, ct);
    }

    private async Task<T> ReadRequiredBodyAsync<T>(HttpResponseMessage response, CancellationToken ct)
    {
        var payload = await response.Content.ReadFromJsonAsync<T>(s_jsonOptions, ct);
        if (payload is null)
            throw new CSharpDbClientException($"HTTP transport returned an empty '{typeof(T).Name}' payload.");

        return payload;
    }

    private async Task<CSharpDbClientException> CreateHttpExceptionAsync(HttpResponseMessage response, CancellationToken ct)
    {
        var error = await ReadErrorAsync(response, ct);
        return new CSharpDbClientException(
            $"HTTP transport returned {(int)response.StatusCode} ({response.ReasonPhrase}): {error.Message}");
    }

    private async Task<HttpError> ReadErrorAsync(HttpResponseMessage response, CancellationToken ct)
    {
        string fallbackMessage = response.ReasonPhrase ?? $"HTTP {(int)response.StatusCode}";
        if (response.Content is null)
            return new HttpError(fallbackMessage, false);

        string payload = await response.Content.ReadAsStringAsync(ct);
        if (string.IsNullOrWhiteSpace(payload))
            return new HttpError(fallbackMessage, false);

        try
        {
            using var json = JsonDocument.Parse(payload);
            if (json.RootElement.ValueKind == JsonValueKind.Object)
            {
                if (json.RootElement.TryGetProperty("error", out var error) && error.ValueKind == JsonValueKind.String)
                    return new HttpError(error.GetString() ?? fallbackMessage, true);

                if (json.RootElement.TryGetProperty("detail", out var detail) && detail.ValueKind == JsonValueKind.String)
                    return new HttpError(detail.GetString() ?? fallbackMessage, false);

                if (json.RootElement.TryGetProperty("title", out var title) && title.ValueKind == JsonValueKind.String)
                    return new HttpError(title.GetString() ?? fallbackMessage, false);
            }
        }
        catch (JsonException)
        {
        }

        return new HttpError(payload.Trim(), false);
    }

    private Uri BuildUri(string relativePath, params KeyValuePair<string, string?>[] query)
    {
        var uri = new Uri(_endpoint, relativePath);
        if (query.Length == 0)
            return uri;

        var nonEmpty = query.Where(pair => !string.IsNullOrWhiteSpace(pair.Value)).ToArray();
        if (nonEmpty.Length == 0)
            return uri;

        var builder = new UriBuilder(uri)
        {
            Query = string.Join("&", nonEmpty.Select(pair => $"{Uri.EscapeDataString(pair.Key)}={Uri.EscapeDataString(pair.Value!)}")),
        };

        return builder.Uri;
    }

    private static Uri EnsureTrailingSlash(Uri endpoint)
    {
        string absolute = endpoint.AbsoluteUri;
        if (absolute.EndsWith("/", StringComparison.Ordinal))
            return endpoint;

        return new Uri(absolute + "/", UriKind.Absolute);
    }

    private static KeyValuePair<string, string?> Q(string key, string? value)
        => new(key, value);

    private static string Escape(string value) => Uri.EscapeDataString(value);

    private static string ConvertKey(object value)
        => value switch
        {
            null => throw new ArgumentNullException(nameof(value)),
            JsonElement element => element.ValueKind switch
            {
                JsonValueKind.String => element.GetString() ?? string.Empty,
                JsonValueKind.Number when element.TryGetInt64(out long integer) => integer.ToString(System.Globalization.CultureInfo.InvariantCulture),
                JsonValueKind.Number => element.GetDouble().ToString(System.Globalization.CultureInfo.InvariantCulture),
                JsonValueKind.True => "1",
                JsonValueKind.False => "0",
                _ => element.GetRawText(),
            },
            IFormattable formattable => formattable.ToString(null, System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty,
            _ => Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty,
        };

    private static JsonSerializerOptions CreateJsonOptions()
    {
        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        };

        options.Converters.Add(new JsonStringEnumConverter(JsonNamingPolicy.CamelCase));
        return options;
    }

    private static TableSchema MapTableSchema(ApiTableSchemaResponse payload)
        => new()
        {
            TableName = payload.TableName,
            Columns = payload.Columns.Select(MapColumn).ToList(),
        };

    private static ColumnDefinition MapColumn(ApiColumnResponse payload)
        => new()
        {
            Name = payload.Name,
            Type = Enum.TryParse<DbType>(payload.Type, ignoreCase: true, out var type)
                ? type
                : throw new CSharpDbClientException($"Unsupported column type '{payload.Type}'."),
            Nullable = payload.Nullable,
            IsPrimaryKey = payload.IsPrimaryKey,
            IsIdentity = payload.IsIdentity,
        };

    private static IndexSchema MapIndex(ApiIndexResponse payload)
        => new()
        {
            IndexName = payload.IndexName,
            TableName = payload.TableName,
            Columns = payload.Columns,
            IsUnique = payload.IsUnique,
        };

    private static ViewDefinition MapView(ApiViewResponse payload)
        => new()
        {
            Name = payload.ViewName,
            Sql = payload.Sql,
        };

    private static TriggerSchema MapTrigger(ApiTriggerResponse payload)
        => new()
        {
            TriggerName = payload.TriggerName,
            TableName = payload.TableName,
            Timing = Enum.TryParse<TriggerTiming>(payload.Timing, ignoreCase: true, out var timing)
                ? timing
                : throw new CSharpDbClientException($"Unsupported trigger timing '{payload.Timing}'."),
            Event = Enum.TryParse<TriggerEvent>(payload.Event, ignoreCase: true, out var triggerEvent)
                ? triggerEvent
                : throw new CSharpDbClientException($"Unsupported trigger event '{payload.Event}'."),
            BodySql = payload.BodySql,
        };

    private static ProcedureDefinition MapProcedure(ApiProcedureDetailResponse payload)
        => new()
        {
            Name = payload.Name,
            BodySql = payload.BodySql,
            Parameters = payload.Parameters.Select(parameter => new ProcedureParameterDefinition
            {
                Name = parameter.Name,
                Type = Enum.TryParse<DbType>(parameter.Type, ignoreCase: true, out var type)
                    ? type
                    : throw new CSharpDbClientException($"Unsupported procedure parameter type '{parameter.Type}'."),
                Required = parameter.Required,
                Default = NormalizeValue(parameter.Default),
                Description = parameter.Description,
            }).ToList(),
            Description = payload.Description,
            IsEnabled = payload.IsEnabled,
            CreatedUtc = payload.CreatedUtc,
            UpdatedUtc = payload.UpdatedUtc,
        };

    private static ProcedureExecutionResult MapProcedureExecution(ApiProcedureExecutionResponse payload)
        => new()
        {
            ProcedureName = payload.ProcedureName,
            Succeeded = payload.Succeeded,
            Statements = payload.Statements.Select(statement => new ProcedureStatementExecutionResult
            {
                StatementIndex = statement.StatementIndex,
                StatementText = statement.StatementText,
                IsQuery = statement.IsQuery,
                ColumnNames = statement.ColumnNames,
                Rows = MapRows(statement.ColumnNames ?? [], statement.Rows),
                RowsAffected = statement.RowsAffected,
                Elapsed = TimeSpan.FromMilliseconds(statement.ElapsedMs),
            }).ToList(),
            Error = payload.Error,
            FailedStatementIndex = payload.FailedStatementIndex,
            Elapsed = TimeSpan.FromMilliseconds(payload.ElapsedMs),
        };

    private static SqlExecutionResult MapSqlResult(ApiSqlResultResponse payload)
        => new()
        {
            IsQuery = payload.IsQuery,
            ColumnNames = payload.ColumnNames,
            Rows = payload.ColumnNames is null ? null : MapRows(payload.ColumnNames, payload.Rows),
            RowsAffected = payload.RowsAffected,
            Error = payload.Error,
            Elapsed = TimeSpan.FromMilliseconds(payload.ElapsedMs),
        };

    private static List<object?[]> MapRows(string[] columnNames, IReadOnlyList<Dictionary<string, object?>>? rows)
    {
        if (rows is null || rows.Count == 0)
            return [];

        var mapped = new List<object?[]>(rows.Count);
        foreach (var row in rows)
        {
            var values = new object?[columnNames.Length];
            for (int i = 0; i < columnNames.Length; i++)
            {
                row.TryGetValue(columnNames[i], out object? rawValue);
                values[i] = NormalizeValue(rawValue);
            }

            mapped.Add(values);
        }

        return mapped;
    }

    private static Dictionary<string, object?> NormalizeDictionary(Dictionary<string, object?> values)
    {
        var normalized = new Dictionary<string, object?>(values.Count, StringComparer.OrdinalIgnoreCase);
        foreach (var (key, value) in values)
            normalized[key] = NormalizeValue(value);

        return normalized;
    }

    private static object? NormalizeValue(object? value) => value switch
    {
        null => null,
        JsonElement element => NormalizeJsonElement(element),
        _ => value,
    };

    private static object? NormalizeJsonElement(JsonElement value) => value.ValueKind switch
    {
        JsonValueKind.Null => null,
        JsonValueKind.String => value.GetString(),
        JsonValueKind.False => false,
        JsonValueKind.True => true,
        JsonValueKind.Number when value.TryGetInt64(out long integer) => integer,
        JsonValueKind.Number => value.GetDouble(),
        _ => value.GetRawText(),
    };

    private static object CreateProcedurePayload(ProcedureDefinition definition, string? newNameOverride)
    {
        var parameters = definition.Parameters.Select(parameter => new
        {
            parameter.Name,
            Type = parameter.Type.ToString(),
            parameter.Required,
            Default = parameter.Default,
            parameter.Description,
        }).ToList();

        return newNameOverride is null
            ? new
            {
                definition.Name,
                definition.BodySql,
                Parameters = parameters,
                definition.Description,
                definition.IsEnabled,
            }
            : new
            {
                NewName = newNameOverride,
                definition.BodySql,
                Parameters = parameters,
                definition.Description,
                definition.IsEnabled,
            };
    }

    private sealed record HttpError(string Message, bool IsEndpointError);

    private sealed record ApiDatabaseInfoResponse(
        string DataSource,
        int TableCount,
        int IndexCount,
        int ViewCount,
        int TriggerCount,
        int ProcedureCount,
        int CollectionCount,
        int SavedQueryCount);

    private sealed record ApiTableSchemaResponse(string TableName, List<ApiColumnResponse> Columns);
    private sealed record ApiColumnResponse(string Name, string Type, bool Nullable, bool IsPrimaryKey, bool IsIdentity);
    private sealed record ApiBrowseResponse(string[] ColumnNames, List<Dictionary<string, object?>> Rows, int TotalRows, int Page, int PageSize, int TotalPages);
    private sealed record ApiRowCountResponse(string TableName, int Count);
    private sealed record ApiMutationResponse(int RowsAffected);
    private sealed record ApiCollectionCountResponse(string CollectionName, int Count);
    private sealed record ApiIndexResponse(string IndexName, string TableName, IReadOnlyList<string> Columns, bool IsUnique);
    private sealed record ApiViewResponse(string ViewName, string Sql);
    private sealed record ApiTriggerResponse(string TriggerName, string TableName, string Timing, string Event, string BodySql);
    private sealed record ApiSqlResultResponse(bool IsQuery, string[]? ColumnNames, IReadOnlyList<Dictionary<string, object?>>? Rows, int RowsAffected, string? Error, double ElapsedMs);
    private sealed record ApiProcedureDetailResponse(string Name, string BodySql, IReadOnlyList<ApiProcedureParameterResponse> Parameters, string? Description, bool IsEnabled, DateTime CreatedUtc, DateTime UpdatedUtc);
    private sealed record ApiProcedureParameterResponse(string Name, string Type, bool Required, object? Default, string? Description);
    private sealed record ApiProcedureExecutionResponse(string ProcedureName, bool Succeeded, IReadOnlyList<ApiProcedureStatementResultResponse> Statements, string? Error, int? FailedStatementIndex, double ElapsedMs);
    private sealed record ApiProcedureStatementResultResponse(int StatementIndex, string StatementText, bool IsQuery, string[]? ColumnNames, IReadOnlyList<Dictionary<string, object?>>? Rows, int RowsAffected, double ElapsedMs);
}
