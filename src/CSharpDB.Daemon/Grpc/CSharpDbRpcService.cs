using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.Client.Grpc;
using CSharpDB.Client.Models;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using CoreDbException = CSharpDB.Primitives.CSharpDbException;
using CoreErrorCode = CSharpDB.Primitives.ErrorCode;

namespace CSharpDB.Daemon.Grpc;

public sealed class CSharpDbRpcService(ICSharpDbClient client) : CSharpDbRpc.CSharpDbRpcBase
{
    private static readonly Empty EmptyResponse = new();

    public override Task<DatabaseInfoMessage> GetInfo(Empty request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.GetInfoAsync(ct), GrpcModelMapper.ToMessage);

    public override Task<StringList> GetTableNames(Empty request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.GetTableNamesAsync(ct), GrpcModelMapper.ToStringList);

    public override Task<OptionalTableSchemaResponse> GetTableSchema(TableNameRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.GetTableSchemaAsync(request.TableName, ct),
            value => new OptionalTableSchemaResponse
            {
                Value = value is null ? null : GrpcModelMapper.ToMessage(value),
            });

    public override Task<Int32Value> GetRowCount(TableNameRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.GetRowCountAsync(request.TableName, ct), value => new Int32Value { Value = value });

    public override Task<TableBrowseResultMessage> BrowseTable(PagedTableRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.BrowseTableAsync(request.TableName, request.Page, request.PageSize, ct), GrpcModelMapper.ToMessage);

    public override Task<OptionalVariantObjectResponse> GetRowByPk(GetRowByPkRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.GetRowByPkAsync(request.TableName, request.PkColumn, ReadRequiredValue(request.PkValue), ct),
            value => new OptionalVariantObjectResponse
            {
                Value = value is null ? null : GrpcValueMapper.ToObject(value),
            });

    public override Task<Int32Value> InsertRow(InsertRowRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.InsertRowAsync(request.TableName, ReadRequiredObject(request.Values), ct), value => new Int32Value { Value = value });

    public override Task<Int32Value> UpdateRow(UpdateRowRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.UpdateRowAsync(
                request.TableName,
                request.PkColumn,
                ReadRequiredValue(request.PkValue),
                ReadRequiredObject(request.Values),
                ct),
            value => new Int32Value { Value = value });

    public override Task<Int32Value> DeleteRow(DeleteRowRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.DeleteRowAsync(request.TableName, request.PkColumn, ReadRequiredValue(request.PkValue), ct), value => new Int32Value { Value = value });

    public override Task<Empty> DropTable(TableNameRequest request, ServerCallContext context)
        => ExecuteEmptyAsync(context, ct => client.DropTableAsync(request.TableName, ct));

    public override Task<Empty> RenameTable(RenameTableRequest request, ServerCallContext context)
        => ExecuteEmptyAsync(context, ct => client.RenameTableAsync(request.TableName, request.NewTableName, ct));

    public override Task<Empty> AddColumn(AddColumnRequest request, ServerCallContext context)
        => ExecuteEmptyAsync(context, ct => client.AddColumnAsync(request.TableName, request.ColumnName, GrpcModelMapper.ToModel(request.Type), request.NotNull, ct));

    public override Task<Empty> DropColumn(DropColumnRequest request, ServerCallContext context)
        => ExecuteEmptyAsync(context, ct => client.DropColumnAsync(request.TableName, request.ColumnName, ct));

    public override Task<Empty> RenameColumn(RenameColumnRequest request, ServerCallContext context)
        => ExecuteEmptyAsync(context, ct => client.RenameColumnAsync(request.TableName, request.OldColumnName, request.NewColumnName, ct));

    public override Task<IndexSchemaListResponse> GetIndexes(Empty request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.GetIndexesAsync(ct), value =>
        {
            var response = new IndexSchemaListResponse();
            response.Items.Add(value.Select(GrpcModelMapper.ToMessage));
            return response;
        });

    public override Task<Empty> CreateIndex(CreateIndexRequest request, ServerCallContext context)
        => ExecuteEmptyAsync(context, ct => client.CreateIndexAsync(request.IndexName, request.TableName, request.ColumnName, request.IsUnique, ct));

    public override Task<Empty> UpdateIndex(UpdateIndexRequest request, ServerCallContext context)
        => ExecuteEmptyAsync(context, ct => client.UpdateIndexAsync(
            request.ExistingIndexName,
            request.NewIndexName,
            request.TableName,
            request.ColumnName,
            request.IsUnique,
            ct));

    public override Task<Empty> DropIndex(NameRequest request, ServerCallContext context)
        => ExecuteEmptyAsync(context, ct => client.DropIndexAsync(request.Name, ct));

    public override Task<StringList> GetViewNames(Empty request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.GetViewNamesAsync(ct), GrpcModelMapper.ToStringList);

    public override Task<ViewDefinitionListResponse> GetViews(Empty request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.GetViewsAsync(ct), value =>
        {
            var response = new ViewDefinitionListResponse();
            response.Items.Add(value.Select(GrpcModelMapper.ToMessage));
            return response;
        });

    public override Task<OptionalViewDefinitionResponse> GetView(NameRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.GetViewAsync(request.Name, ct),
            value => new OptionalViewDefinitionResponse
            {
                Value = value is null ? null : GrpcModelMapper.ToMessage(value),
            });

    public override Task<OptionalStringResponse> GetViewSql(NameRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.GetViewSqlAsync(request.Name, ct),
            value => new OptionalStringResponse
            {
                Value = value,
            });

    public override Task<ViewBrowseResultMessage> BrowseView(PagedNameRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.BrowseViewAsync(request.Name, request.Page, request.PageSize, ct), GrpcModelMapper.ToMessage);

    public override Task<Empty> CreateView(CreateViewRequest request, ServerCallContext context)
        => ExecuteEmptyAsync(context, ct => client.CreateViewAsync(request.ViewName, request.SelectSql, ct));

    public override Task<Empty> UpdateView(UpdateViewRequest request, ServerCallContext context)
        => ExecuteEmptyAsync(context, ct => client.UpdateViewAsync(request.ExistingViewName, request.NewViewName, request.SelectSql, ct));

    public override Task<Empty> DropView(NameRequest request, ServerCallContext context)
        => ExecuteEmptyAsync(context, ct => client.DropViewAsync(request.Name, ct));

    public override Task<TriggerSchemaListResponse> GetTriggers(Empty request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.GetTriggersAsync(ct), value =>
        {
            var response = new TriggerSchemaListResponse();
            response.Items.Add(value.Select(GrpcModelMapper.ToMessage));
            return response;
        });

    public override Task<Empty> CreateTrigger(CreateTriggerRequest request, ServerCallContext context)
        => ExecuteEmptyAsync(context, ct => client.CreateTriggerAsync(
            request.TriggerName,
            request.TableName,
            GrpcModelMapper.ToModel(request.Timing),
            GrpcModelMapper.ToModel(request.TriggerEvent),
            request.BodySql,
            ct));

    public override Task<Empty> UpdateTrigger(UpdateTriggerRequest request, ServerCallContext context)
        => ExecuteEmptyAsync(context, ct => client.UpdateTriggerAsync(
            request.ExistingTriggerName,
            request.NewTriggerName,
            request.TableName,
            GrpcModelMapper.ToModel(request.Timing),
            GrpcModelMapper.ToModel(request.TriggerEvent),
            request.BodySql,
            ct));

    public override Task<Empty> DropTrigger(NameRequest request, ServerCallContext context)
        => ExecuteEmptyAsync(context, ct => client.DropTriggerAsync(request.Name, ct));

    public override Task<SavedQueryDefinitionListResponse> GetSavedQueries(Empty request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.GetSavedQueriesAsync(ct), value =>
        {
            var response = new SavedQueryDefinitionListResponse();
            response.Items.Add(value.Select(GrpcModelMapper.ToMessage));
            return response;
        });

    public override Task<OptionalSavedQueryDefinitionResponse> GetSavedQuery(NameRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.GetSavedQueryAsync(request.Name, ct),
            value => new OptionalSavedQueryDefinitionResponse
            {
                Value = value is null ? null : GrpcModelMapper.ToMessage(value),
            });

    public override Task<SavedQueryDefinitionMessage> UpsertSavedQuery(UpsertSavedQueryRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.UpsertSavedQueryAsync(request.Name, request.SqlText, ct), GrpcModelMapper.ToMessage);

    public override Task<Empty> DeleteSavedQuery(NameRequest request, ServerCallContext context)
        => ExecuteEmptyAsync(context, ct => client.DeleteSavedQueryAsync(request.Name, ct));

    public override Task<ProcedureDefinitionListResponse> GetProcedures(GetProceduresRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.GetProceduresAsync(request.IncludeDisabled, ct), value =>
        {
            var response = new ProcedureDefinitionListResponse();
            response.Items.Add(value.Select(GrpcModelMapper.ToMessage));
            return response;
        });

    public override Task<OptionalProcedureDefinitionResponse> GetProcedure(NameRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.GetProcedureAsync(request.Name, ct),
            value => new OptionalProcedureDefinitionResponse
            {
                Value = value is null ? null : GrpcModelMapper.ToMessage(value),
            });

    public override Task<Empty> CreateProcedure(CreateProcedureRequest request, ServerCallContext context)
        => ExecuteEmptyAsync(context, ct => client.CreateProcedureAsync(ReadRequired(request.Definition, GrpcModelMapper.ToModel, nameof(request.Definition)), ct));

    public override Task<Empty> UpdateProcedure(UpdateProcedureRequest request, ServerCallContext context)
        => ExecuteEmptyAsync(context, ct => client.UpdateProcedureAsync(request.ExistingName, ReadRequired(request.Definition, GrpcModelMapper.ToModel, nameof(request.Definition)), ct));

    public override Task<Empty> DeleteProcedure(NameRequest request, ServerCallContext context)
        => ExecuteEmptyAsync(context, ct => client.DeleteProcedureAsync(request.Name, ct));

    public override Task<ProcedureExecutionResultMessage> ExecuteProcedure(ExecuteProcedureRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.ExecuteProcedureAsync(request.Name, ReadRequiredObject(request.Args), ct), GrpcModelMapper.ToMessage);

    public override Task<SqlExecutionResultMessage> ExecuteSql(SqlRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.ExecuteSqlAsync(request.Sql, ct), GrpcModelMapper.ToMessage);

    public override Task<TransactionSessionInfoMessage> BeginTransaction(Empty request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.BeginTransactionAsync(ct), GrpcModelMapper.ToMessage);

    public override Task<SqlExecutionResultMessage> ExecuteInTransaction(TransactionSqlRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.ExecuteInTransactionAsync(request.TransactionId, request.Sql, ct), GrpcModelMapper.ToMessage);

    public override Task<Empty> CommitTransaction(TransactionIdRequest request, ServerCallContext context)
        => ExecuteEmptyAsync(context, ct => client.CommitTransactionAsync(request.TransactionId, ct));

    public override Task<Empty> RollbackTransaction(TransactionIdRequest request, ServerCallContext context)
        => ExecuteEmptyAsync(context, ct => client.RollbackTransactionAsync(request.TransactionId, ct));

    public override Task<StringList> GetCollectionNames(Empty request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.GetCollectionNamesAsync(ct), GrpcModelMapper.ToStringList);

    public override Task<Int32Value> GetCollectionCount(CollectionNameRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.GetCollectionCountAsync(request.CollectionName, ct), value => new Int32Value { Value = value });

    public override Task<CollectionBrowseResultMessage> BrowseCollection(PagedNameRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.BrowseCollectionAsync(request.Name, request.Page, request.PageSize, ct), GrpcModelMapper.ToMessage);

    public override Task<OptionalVariantValueResponse> GetDocument(GetDocumentRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.GetDocumentAsync(request.CollectionName, request.Key, ct),
            value => new OptionalVariantValueResponse
            {
                Value = value.HasValue ? GrpcValueMapper.ToMessage(value.Value) : null,
            });

    public override Task<Empty> PutDocument(PutDocumentRequest request, ServerCallContext context)
        => ExecuteEmptyAsync(context, ct => client.PutDocumentAsync(request.CollectionName, request.Key, ReadRequired(request.Document, GrpcValueMapper.ToJsonElement, nameof(request.Document)), ct));

    public override Task<BoolValue> DeleteDocument(DeleteDocumentRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.DeleteDocumentAsync(request.CollectionName, request.Key, ct), value => new BoolValue { Value = value });

    public override Task<Empty> Checkpoint(Empty request, ServerCallContext context)
        => ExecuteEmptyAsync(context, ct => client.CheckpointAsync(ct));

    public override Task<BackupResultMessage> Backup(BackupRequestMessage request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.BackupAsync(GrpcModelMapper.ToModel(request), ct), GrpcModelMapper.ToMessage);

    public override Task<RestoreResultMessage> Restore(RestoreRequestMessage request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.RestoreAsync(GrpcModelMapper.ToModel(request), ct), GrpcModelMapper.ToMessage);

    public override Task<DatabaseMaintenanceReportMessage> GetMaintenanceReport(Empty request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.GetMaintenanceReportAsync(ct), GrpcModelMapper.ToMessage);

    public override Task<ReindexResultMessage> Reindex(ReindexRequestMessage request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.ReindexAsync(GrpcModelMapper.ToModel(request), ct), GrpcModelMapper.ToMessage);

    public override Task<VacuumResultMessage> Vacuum(Empty request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.VacuumAsync(ct), GrpcModelMapper.ToMessage);

    public override Task<DatabaseInspectReportMessage> InspectStorage(InspectStorageRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.InspectStorageAsync(NullIfEmpty(request.DatabasePath), request.IncludePages, ct), GrpcModelMapper.ToMessage);

    public override Task<WalInspectReportMessage> CheckWal(CheckWalRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.CheckWalAsync(NullIfEmpty(request.DatabasePath), ct), GrpcModelMapper.ToMessage);

    public override Task<PageInspectReportMessage> InspectPage(InspectPageRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.InspectPageAsync(request.PageId, request.IncludeHex, NullIfEmpty(request.DatabasePath), ct), GrpcModelMapper.ToMessage);

    public override Task<IndexInspectReportMessage> CheckIndexes(CheckIndexesRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.CheckIndexesAsync(NullIfEmpty(request.DatabasePath), NullIfEmpty(request.IndexName), request.SampleSize, ct), GrpcModelMapper.ToMessage);

    private async Task<TResponse> ExecuteAsync<TModel, TResponse>(ServerCallContext context, Func<CancellationToken, Task<TModel>> action, Func<TModel, TResponse> map)
    {
        try
        {
            return map(await action(context.CancellationToken).ConfigureAwait(false));
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

    private async Task<Empty> ExecuteEmptyAsync(ServerCallContext context, Func<CancellationToken, Task> action)
    {
        try
        {
            await action(context.CancellationToken).ConfigureAwait(false);
            return EmptyResponse;
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

    private static object ReadRequiredValue(VariantValue? value)
        => ReadRequired(value, GrpcValueMapper.FromMessage, nameof(value))!;

    private static Dictionary<string, object?> ReadRequiredObject(VariantObject? value)
        => ReadRequired(value, GrpcValueMapper.ToDictionary, nameof(value));

    private static TModel ReadRequired<TMessage, TModel>(TMessage? value, Func<TMessage, TModel> map, string name)
        where TMessage : class
        => value is null
            ? throw new ArgumentException($"The '{name}' payload is required.")
            : map(value);

    private static string? NullIfEmpty(string? value)
        => string.IsNullOrWhiteSpace(value) ? null : value;

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
