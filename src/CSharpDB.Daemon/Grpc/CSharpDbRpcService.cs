using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using CSharpDB.Api;
using CSharpDB.Api.Diagnostics;
using CSharpDB.Api.Security;
using CSharpDB.Client;
using CSharpDB.Client.Grpc;
using CSharpDB.Client.Models;
using CSharpDB.Observability;
using CSharpDB.Sql;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using CoreDbException = CSharpDB.Primitives.CSharpDbException;
using CoreErrorCode = CSharpDB.Primitives.ErrorCode;

namespace CSharpDB.Daemon.Grpc;

public sealed class CSharpDbRpcService : CSharpDbRpc.CSharpDbRpcBase
{
    private static readonly Empty EmptyResponse = new();
    private readonly ICSharpDbClient client;
    private readonly IOptions<CSharpDbApiSecurityOptions> securityOptions;
    private readonly IServiceProvider? services;
    private readonly TimeSpan readinessTimeout;

    public CSharpDbRpcService(ICSharpDbClient client)
        : this(
            client,
            Options.Create(new CSharpDbApiSecurityOptions()),
            serviceProvider: null)
    {
    }

    [ActivatorUtilitiesConstructor]
    public CSharpDbRpcService(
        ICSharpDbClient client,
        IOptions<CSharpDbApiSecurityOptions> securityOptions,
        IServiceProvider? serviceProvider)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(securityOptions);

        this.client = client;
        this.securityOptions = securityOptions;
        services = serviceProvider;
        CSharpDbObservabilityOptions observabilityOptions = serviceProvider?
            .GetService<CSharpDbObservabilityOptions>() ?? new();
        observabilityOptions.Validate();
        readinessTimeout = observabilityOptions.Health.ReadinessTimeout;
    }

    public override Task<DatabaseInfoMessage> GetInfo(Empty request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.GetInfoAsync(ct), GrpcModelMapper.ToMessage);

    public override Task<ShardMapSnapshotMessage> GetShardMap(Empty request, ServerCallContext context)
        => ExecuteAsync(context, ct => GetShardAdminClient().GetShardMapAsync(ct), GrpcModelMapper.ToMessage);

    public override Task<ShardResolutionMessage> ResolveShardRoute(ShardRouteRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => GetShardAdminClient().ResolveRouteAsync(GrpcModelMapper.ToModel(request), ct), GrpcModelMapper.ToMessage);

    public override Task<ShardStatusListResponse> GetShardStatus(Empty request, ServerCallContext context)
        => ExecuteAsync(context, ct => GetShardAdminClient().GetShardStatusAsync(ct), value =>
        {
            var response = new ShardStatusListResponse();
            response.Items.Add(value.Select(GrpcModelMapper.ToMessage));
            return response;
        });

    public override Task<ShardSqlExecutionResultListResponse> ExecuteSqlOnAllShards(SqlRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => GetShardAdminClient().ExecuteSqlOnAllShardsAsync(request.Sql, ct), value =>
        {
            var response = new ShardSqlExecutionResultListResponse();
            response.Items.Add(value.Select(GrpcModelMapper.ToMessage));
            return response;
        });

    public override Task<ShardSqlExecutionResultListResponse> ExecuteReadOnlySqlOnAllShards(SqlRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => GetShardAdminClient().ExecuteReadOnlySqlOnAllShardsAsync(request.Sql, ct), value =>
        {
            var response = new ShardSqlExecutionResultListResponse();
            response.Items.Add(value.Select(GrpcModelMapper.ToMessage));
            return response;
        });

    public override Task<ShardCatalogStateMessage> GetShardCatalog(Empty request, ServerCallContext context)
        => ExecuteAsync(context, ct => GetShardAdminClient().GetShardCatalogAsync(ct), GrpcModelMapper.ToMessage);

    public override Task<ShardCatalogValidationResultMessage> ValidateShardCatalogUpdate(ShardCatalogUpdateRequestMessage request, ServerCallContext context)
        => ExecuteAsync(context, ct => GetShardAdminClient().ValidateShardCatalogUpdateAsync(GrpcModelMapper.ToModel(request), ct), GrpcModelMapper.ToMessage);

    public override Task<ShardCatalogApplyResultMessage> ApplyShardCatalogUpdate(ShardCatalogUpdateRequestMessage request, ServerCallContext context)
        => ExecuteAsync(context, ct => GetShardAdminClient().ApplyShardCatalogUpdateAsync(GrpcModelMapper.ToModel(request), ct), GrpcModelMapper.ToMessage);

    public override Task<ShardMigrationResultMessage> MigrateExactRouteKey(ShardExactKeyMigrationRequestMessage request, ServerCallContext context)
        => ExecuteAsync(context, ct => GetShardAdminClient().MigrateExactRouteKeyAsync(GrpcModelMapper.ToModel(request), ct), GrpcModelMapper.ToMessage);

    public override Task<ShardMigrationResultMessage> MigrateBucketRange(ShardBucketRangeMigrationRequestMessage request, ServerCallContext context)
        => ExecuteAsync(context, ct => GetShardAdminClient().MigrateBucketRangeAsync(GrpcModelMapper.ToModel(request), ct), GrpcModelMapper.ToMessage);

    public override Task<ShardMigrationHistoryListResponse> GetShardMigrationHistory(Empty request, ServerCallContext context)
        => ExecuteAsync(context, ct => GetShardAdminClient().GetShardMigrationHistoryAsync(ct), value =>
        {
            var response = new ShardMigrationHistoryListResponse();
            response.Items.Add(value.Select(GrpcModelMapper.ToMessage));
            return response;
        });

    public override Task<ShardMigrationProgressListResponse> GetShardMigrationProgress(Empty request, ServerCallContext context)
        => ExecuteAsync(context, ct => GetShardAdminClient().GetShardMigrationProgressAsync(ct), value =>
        {
            var response = new ShardMigrationProgressListResponse();
            response.Items.Add(value.Select(GrpcModelMapper.ToMessage));
            return response;
        });

    public override Task<OptionalShardMigrationProgressResponse> GetShardMigrationProgressById(
        ShardMigrationIdRequestMessage request,
        ServerCallContext context)
        => ExecuteAsync(context, ct => GetShardAdminClient().GetShardMigrationProgressAsync(request.MigrationId, ct), value =>
            new OptionalShardMigrationProgressResponse
            {
                Value = value is null ? null : GrpcModelMapper.ToMessage(value),
            });

    public override Task<ShardMigrationResultMessage> ResumeShardMigration(
        ShardMigrationIdRequestMessage request,
        ServerCallContext context)
        => ExecuteAsync(context, ct => GetShardAdminClient().ResumeShardMigrationAsync(request.MigrationId, ct), GrpcModelMapper.ToMessage);

    public override Task<ShardMigrationResultMessage> RetryShardMigration(
        ShardMigrationIdRequestMessage request,
        ServerCallContext context)
        => ExecuteAsync(context, ct => GetShardAdminClient().RetryShardMigrationAsync(request.MigrationId, ct), GrpcModelMapper.ToMessage);

    public override Task<ShardDirectoryResolutionMessage> ResolveShardDirectoryEntry(
        ShardDirectoryResolveRequestMessage request,
        ServerCallContext context)
        => ExecuteAsync(context, ct => GetShardDirectoryClient().ResolveDirectoryEntryAsync(GrpcModelMapper.ToModel(request), ct), GrpcModelMapper.ToMessage);

    public override Task<ShardDirectoryMutationResultMessage> ReserveShardDirectoryEntry(
        ShardDirectoryReserveRequestMessage request,
        ServerCallContext context)
        => ExecuteAsync(context, ct => GetShardDirectoryClient().ReserveDirectoryEntryAsync(GrpcModelMapper.ToModel(request), ct), GrpcModelMapper.ToMessage);

    public override Task<ShardDirectoryMutationResultMessage> ActivateShardDirectoryEntry(
        ShardDirectoryActivateRequestMessage request,
        ServerCallContext context)
        => ExecuteAsync(context, ct => GetShardDirectoryClient().ActivateDirectoryEntryAsync(GrpcModelMapper.ToModel(request), ct), GrpcModelMapper.ToMessage);

    public override Task<ShardDirectoryMutationResultMessage> UpsertShardDirectoryEntry(
        ShardDirectoryUpsertRequestMessage request,
        ServerCallContext context)
        => ExecuteAsync(context, ct => GetShardDirectoryClient().UpsertDirectoryEntryAsync(GrpcModelMapper.ToModel(request), ct), GrpcModelMapper.ToMessage);

    public override Task<ShardDirectoryMutationResultMessage> DisableShardDirectoryEntry(
        ShardDirectoryDisableRequestMessage request,
        ServerCallContext context)
        => ExecuteAsync(context, ct => GetShardDirectoryClient().DisableDirectoryEntryAsync(GrpcModelMapper.ToModel(request), ct), GrpcModelMapper.ToMessage);

    public override Task<ShardDirectoryMutationResultMessage> DeleteShardDirectoryEntry(
        ShardDirectoryDeleteRequestMessage request,
        ServerCallContext context)
        => ExecuteAsync(context, ct => GetShardDirectoryClient().DeleteDirectoryEntryAsync(GrpcModelMapper.ToModel(request), ct), GrpcModelMapper.ToMessage);

    public override Task<ShardDirectoryMutationResultMessage> MarkShardDirectoryEntryStale(
        ShardDirectoryMarkStaleRequestMessage request,
        ServerCallContext context)
        => ExecuteAsync(context, ct => GetShardDirectoryClient().MarkDirectoryEntryStaleAsync(GrpcModelMapper.ToModel(request), ct), GrpcModelMapper.ToMessage);

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
        => ExecuteEmptyAsync(context, ct => client.AddColumnAsync(
            request.TableName,
            request.ColumnName,
            GrpcModelMapper.ToModel(request.Type),
            request.NotNull,
            NullIfEmpty(request.Collation),
            ct));

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
        => ExecuteEmptyAsync(context, ct => client.CreateIndexAsync(
            request.IndexName,
            request.TableName,
            request.ColumnName,
            request.IsUnique,
            NullIfEmpty(request.Collation),
            ct));

    public override Task<Empty> UpdateIndex(UpdateIndexRequest request, ServerCallContext context)
        => ExecuteEmptyAsync(context, ct => client.UpdateIndexAsync(
            request.ExistingIndexName,
            request.NewIndexName,
            request.TableName,
            request.ColumnName,
            request.IsUnique,
            NullIfEmpty(request.Collation),
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
    {
        if (TryCreateStatelessTemporaryTableSqlRejection(request.Sql, out var rejection))
            return Task.FromResult(GrpcModelMapper.ToMessage(rejection));

        return ExecuteAsync(
            context,
            async ct =>
            {
                SqlExecutionResult result = await client.ExecuteSqlAsync(request.Sql, ct).ConfigureAwait(false);
                if (result.ErrorCode == CoreErrorCode.ResourceLimitExceeded)
                {
                    throw new CoreDbException(
                        result.ErrorCode.Value,
                        result.Error ?? "The SQL execution resource limit was exceeded.");
                }

                return result;
            },
            GrpcModelMapper.ToMessage);
    }

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

    public override Task<Empty> DropCollection(CollectionNameRequest request, ServerCallContext context)
        => ExecuteEmptyAsync(context, ct => client.DropCollectionAsync(request.CollectionName, ct));

    public override Task<Empty> Checkpoint(Empty request, ServerCallContext context)
        => ExecuteEmptyAsync(context, ct => client.CheckpointAsync(ct));

    public override Task<BackupResultMessage> Backup(BackupRequestMessage request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.BackupAsync(GrpcModelMapper.ToModel(request), ct), GrpcModelMapper.ToMessage);

    public override Task<RestoreResultMessage> Restore(
        RestoreRequestMessage request,
        ServerCallContext context)
    {
        RestoreRequest model = GrpcModelMapper.ToModel(request);
        return model.ValidateOnly
            ? ExecuteAsync(
                context,
                ct => client.RestoreAsync(model, ct),
                GrpcModelMapper.ToMessage)
            : ExecuteRestoreAsync(
                context,
                ct => client.RestoreAsync(model, ct),
                GrpcModelMapper.ToMessage);
    }

    public override Task<ForeignKeyMigrationResultMessage> MigrateForeignKeys(
        ForeignKeyMigrationRequestMessage request,
        ServerCallContext context)
    {
        ForeignKeyMigrationRequest model = GrpcModelMapper.ToModel(request);
        return model.ValidateOnly
            ? ExecuteAsync(
                context,
                ct => client.MigrateForeignKeysAsync(model, ct),
                GrpcModelMapper.ToMessage)
            : ExecuteNotReadyAsync(
                context,
                CSharpDbReadinessReason.ExclusiveMaintenance,
                ct => client.MigrateForeignKeysAsync(model, ct),
                GrpcModelMapper.ToMessage);
    }

    public override Task<DatabaseMaintenanceReportMessage> GetMaintenanceReport(Empty request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.GetMaintenanceReportAsync(ct), GrpcModelMapper.ToMessage);

    public override Task<ReindexResultMessage> Reindex(
        ReindexRequestMessage request,
        ServerCallContext context)
        => ExecuteNotReadyAsync(
            context,
            CSharpDbReadinessReason.ExclusiveMaintenance,
            ct => client.ReindexAsync(GrpcModelMapper.ToModel(request), ct),
            GrpcModelMapper.ToMessage);

    public override Task<VacuumResultMessage> Vacuum(Empty request, ServerCallContext context)
        => ExecuteNotReadyAsync(
            context,
            CSharpDbReadinessReason.ExclusiveMaintenance,
            ct => client.VacuumAsync(ct),
            GrpcModelMapper.ToMessage);

    public override Task<DatabaseInspectReportMessage> InspectStorage(InspectStorageRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.InspectStorageAsync(NullIfEmpty(request.DatabasePath), request.IncludePages, ct), GrpcModelMapper.ToMessage);

    public override Task<WalInspectReportMessage> CheckWal(CheckWalRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.CheckWalAsync(NullIfEmpty(request.DatabasePath), ct), GrpcModelMapper.ToMessage);

    public override Task<PageInspectReportMessage> InspectPage(InspectPageRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.InspectPageAsync(request.PageId, request.IncludeHex, NullIfEmpty(request.DatabasePath), ct), GrpcModelMapper.ToMessage);

    public override Task<IndexInspectReportMessage> CheckIndexes(CheckIndexesRequest request, ServerCallContext context)
        => ExecuteAsync(context, ct => client.CheckIndexesAsync(NullIfEmpty(request.DatabasePath), NullIfEmpty(request.IndexName), request.SampleSize, ct), GrpcModelMapper.ToMessage);

    public override Task<DiagnosticsJsonResponse> GetRuntimeDiagnostics(
        Empty request,
        ServerCallContext context)
        => ExecuteDiagnosticsAsync(
            context,
            CSharpDbDiagnosticsAccessKind.Runtime,
            static (capability, ct) => capability.GetRuntimeDiagnosticsAsync(ct),
            DiagnosticsJsonTypeInfo<DiagnosticsTopologySnapshot<
                RuntimeDiagnosticsSnapshot>>());

    public override Task<DiagnosticsJsonResponse> GetStorageDiagnostics(
        Empty request,
        ServerCallContext context)
        => ExecuteDiagnosticsAsync(
            context,
            CSharpDbDiagnosticsAccessKind.Runtime,
            static (capability, ct) => capability.GetStorageDiagnosticsAsync(ct),
            DiagnosticsJsonTypeInfo<DiagnosticsTopologySnapshot<
                DiagnosticsValueSnapshot<StorageRuntimeDiagnosticsSnapshot>>>());

    public override Task<DiagnosticsJsonResponse> GetWalDiagnostics(
        Empty request,
        ServerCallContext context)
        => ExecuteDiagnosticsAsync(
            context,
            CSharpDbDiagnosticsAccessKind.Runtime,
            static (capability, ct) => capability.GetWalDiagnosticsAsync(ct),
            DiagnosticsJsonTypeInfo<DiagnosticsTopologySnapshot<
                DiagnosticsValueSnapshot<WalRuntimeDiagnosticsSnapshot>>>());

    public override Task<DiagnosticsJsonResponse> GetActiveQueries(
        DiagnosticsRecordsRequest request,
        ServerCallContext context)
        => ExecuteDiagnosticsAsync(
            context,
            CSharpDbDiagnosticsAccessKind.Runtime,
            (capability, ct) =>
            {
                ValidateDiagnosticsMaximumRecords(request.MaximumRecords);
                return capability.GetActiveQueriesAsync(request.MaximumRecords, ct);
            },
            DiagnosticsJsonTypeInfo<DiagnosticsTopologySnapshot<
                DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>>());

    public override Task<DiagnosticsJsonResponse> GetRecentQueries(
        DiagnosticsRecordsRequest request,
        ServerCallContext context)
        => ExecuteDiagnosticsAsync(
            context,
            CSharpDbDiagnosticsAccessKind.Runtime,
            (capability, ct) =>
            {
                ValidateDiagnosticsMaximumRecords(request.MaximumRecords);
                return capability.GetRecentQueriesAsync(request.MaximumRecords, ct);
            },
            DiagnosticsJsonTypeInfo<DiagnosticsTopologySnapshot<
                DiagnosticsCollectionSnapshot<RecentQuerySnapshot>>>());

    public override Task<DiagnosticsJsonResponse> GetQueryPlanDiagnostics(
        DiagnosticsOperationRequest request,
        ServerCallContext context)
        => ExecuteDiagnosticsAsync(
            context,
            CSharpDbDiagnosticsAccessKind.Runtime,
            (capability, ct) => capability.GetQueryPlanDiagnosticsAsync(
                CreateDiagnosticsOperationId(request.OperationId),
                ct),
            DiagnosticsJsonTypeInfo<DiagnosticsTopologySnapshot<
                DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>>());

    public override Task<DiagnosticsJsonResponse> GetSessions(
        DiagnosticsRecordsRequest request,
        ServerCallContext context)
        => ExecuteDiagnosticsAsync(
            context,
            CSharpDbDiagnosticsAccessKind.Runtime,
            (capability, ct) =>
            {
                ValidateDiagnosticsMaximumRecords(request.MaximumRecords);
                return GetSessionsWithHostRequestsAsync(
                    capability,
                    TryGetHostRequestContributor(services),
                    request.MaximumRecords,
                    ct);
            },
            DiagnosticsJsonTypeInfo<DiagnosticsTopologySnapshot<
                DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>>());

    public override Task<DiagnosticsJsonResponse> GetActiveMaintenanceOperations(
        DiagnosticsRecordsRequest request,
        ServerCallContext context)
        => ExecuteDiagnosticsAsync(
            context,
            CSharpDbDiagnosticsAccessKind.Runtime,
            (capability, ct) =>
            {
                ValidateDiagnosticsMaximumRecords(request.MaximumRecords);
                return capability.GetActiveMaintenanceOperationsAsync(
                    request.MaximumRecords,
                    ct);
            },
            DiagnosticsJsonTypeInfo<DiagnosticsTopologySnapshot<
                DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>>>());

    public override Task<DiagnosticsJsonResponse> GetRecentMaintenanceOperations(
        DiagnosticsRecordsRequest request,
        ServerCallContext context)
        => ExecuteDiagnosticsAsync(
            context,
            CSharpDbDiagnosticsAccessKind.Runtime,
            (capability, ct) =>
            {
                ValidateDiagnosticsMaximumRecords(request.MaximumRecords);
                return capability.GetRecentMaintenanceOperationsAsync(
                    request.MaximumRecords,
                    ct);
            },
            DiagnosticsJsonTypeInfo<DiagnosticsTopologySnapshot<
                DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>>>());

    public override Task<DiagnosticsJsonResponse> GetQueryDetail(
        DiagnosticsOperationRequest request,
        ServerCallContext context)
        => ExecuteDiagnosticsAsync(
            context,
            CSharpDbDiagnosticsAccessKind.QueryDetail,
            (capability, ct) => capability.GetQueryDetailAsync(
                CreateDiagnosticsOperationId(request.OperationId),
                ct),
            DiagnosticsJsonTypeInfo<DiagnosticsTopologySnapshot<
                DiagnosticsValueSnapshot<QueryDetailSnapshot>>>());

    private async Task<DiagnosticsJsonResponse> ExecuteDiagnosticsAsync<T>(
        ServerCallContext context,
        CSharpDbDiagnosticsAccessKind accessKind,
        Func<ICSharpDbObservabilityClient, CancellationToken, Task<T>> action,
        JsonTypeInfo<T> jsonTypeInfo)
    {
        AuthorizeDiagnostics(context, accessKind);
        if (client is not ICSharpDbObservabilityClient capability)
            throw UnsupportedDiagnostics();

        try
        {
            using IDisposable suppression =
                CSharpDbOperationScope.SuppressDiagnostics();
            T result = await action(capability, context.CancellationToken)
                .ConfigureAwait(false);
            return new DiagnosticsJsonResponse
            {
                JsonUtf8 = ByteString.CopyFrom(
                    JsonSerializer.SerializeToUtf8Bytes(result, jsonTypeInfo)),
            };
        }
        catch (OperationCanceledException) when (
            context.CancellationToken.IsCancellationRequested)
        {
            throw new RpcException(new Status(
                StatusCode.Cancelled,
                "The runtime diagnostics request was canceled."));
        }
        catch (CSharpDbObservabilityNotSupportedException)
        {
            throw UnsupportedDiagnostics();
        }
        catch (RpcException)
        {
            throw;
        }
        catch (ArgumentException)
        {
            throw new RpcException(new Status(
                StatusCode.InvalidArgument,
                "The runtime diagnostics request is invalid."));
        }
        catch (Exception)
        {
            throw new RpcException(new Status(
                StatusCode.Internal,
                "The runtime diagnostics request failed."));
        }
    }

    private static async Task<DiagnosticsTopologySnapshot<
        DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>>
        GetSessionsWithHostRequestsAsync(
            ICSharpDbObservabilityClient capability,
            ICSharpDbHostRequestDiagnosticsContributor? contributor,
            int maximumRecords,
            CancellationToken cancellationToken)
    {
        DiagnosticsTopologySnapshot<
            DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>> result =
            await capability.GetSessionsAsync(
                maximumRecords,
                cancellationToken).ConfigureAwait(false);
        return CSharpDbHostRequestDiagnosticsProjection.MergeSessions(
            result,
            contributor,
            maximumRecords);
    }

    private static ICSharpDbHostRequestDiagnosticsContributor?
        TryGetHostRequestContributor(IServiceProvider? serviceProvider)
    {
        if (serviceProvider is null)
            return null;

        try
        {
            if (serviceProvider.GetService<CSharpDbObservabilityOptions>()
                    ?.Enabled != true)
            {
                return null;
            }

            return serviceProvider.GetService<
                ICSharpDbHostRequestDiagnosticsContributor>();
        }
        catch
        {
            return null;
        }
    }

    private void AuthorizeDiagnostics(
        ServerCallContext context,
        CSharpDbDiagnosticsAccessKind accessKind)
    {
        CSharpDbApiSecurityOptions security = securityOptions.Value;
        string headerName = CSharpDbApiKeyValidator.NormalizeHeaderName(
            security.ApiKeyHeaderName,
            forGrpc: true);
        string? suppliedApiKey = context.RequestHeaders.GetValue(headerName);
        CSharpDbDiagnosticsAccessDecision decision =
            CSharpDbDiagnosticsAccessPolicy.Evaluate(
                security,
                context.GetHttpContext().Connection.RemoteIpAddress,
                suppliedApiKey,
                accessKind);
        switch (decision)
        {
            case CSharpDbDiagnosticsAccessDecision.Allowed:
                return;
            case CSharpDbDiagnosticsAccessDecision.Unauthenticated:
                throw new RpcException(new Status(
                    StatusCode.Unauthenticated,
                    "A valid CSharpDB API key is required for runtime diagnostics."));
            default:
                throw new RpcException(new Status(
                    StatusCode.PermissionDenied,
                    "Runtime diagnostics access is not permitted from this endpoint."));
        }
    }

    private static OpaqueDiagnosticsId CreateDiagnosticsOperationId(string value)
    {
        try
        {
            return new OpaqueDiagnosticsId(value);
        }
        catch (ArgumentException)
        {
            throw new RpcException(new Status(
                StatusCode.InvalidArgument,
                "A 32-character lowercase hexadecimal diagnostics operation id is required."));
        }
    }

    private static void ValidateDiagnosticsMaximumRecords(int maximumRecords)
    {
        if (maximumRecords <= 0 ||
            maximumRecords > CSharpDbObservabilityOptions.MaximumHistoryCapacity)
        {
            throw new RpcException(new Status(
                StatusCode.InvalidArgument,
                $"maximum_records must be between 1 and {CSharpDbObservabilityOptions.MaximumHistoryCapacity}."));
        }
    }

    private static RpcException UnsupportedDiagnostics()
        => new(new Status(
            StatusCode.Unimplemented,
            CSharpDbObservabilityNotSupportedException.SafeMessage));

    private static JsonTypeInfo<T> DiagnosticsJsonTypeInfo<T>()
        => (JsonTypeInfo<T>)(CSharpDbObservabilityJsonContext.Default
            .GetTypeInfo(typeof(T)) ??
            throw new InvalidOperationException(
                "The diagnostics response is missing source-generated JSON metadata."));

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

    private async Task<TResponse> ExecuteNotReadyAsync<TModel, TResponse>(
        ServerCallContext context,
        CSharpDbReadinessReason reason,
        Func<CancellationToken, Task<TModel>> action,
        Func<TModel, TResponse> map)
    {
        CSharpDbHostReadinessCoordinator? coordinator = services?
            .GetService<CSharpDbHostReadinessCoordinator>();
        IDisposable? lease = coordinator?.EnterNotReady(reason);
        try
        {
            TResponse response;
            try
            {
                response = await ExecuteAsync(context, action, map)
                    .ConfigureAwait(false);
            }
            catch
            {
                await RequestRecoveryIfUnavailableAsync(
                        context,
                        coordinator,
                        CSharpDbReadinessReason.Unavailable)
                    .ConfigureAwait(false);
                throw;
            }

            await VerifyReadyAsync(
                    context,
                    coordinator,
                    CSharpDbReadinessReason.Unavailable)
                .ConfigureAwait(false);
            return response;
        }
        finally
        {
            lease?.Dispose();
        }
    }

    private async Task<TResponse> ExecuteRestoreAsync<TModel, TResponse>(
        ServerCallContext context,
        Func<CancellationToken, Task<TModel>> action,
        Func<TModel, TResponse> map)
    {
        CSharpDbHostReadinessCoordinator? coordinator = services?
            .GetService<CSharpDbHostReadinessCoordinator>();
        IDisposable? lease = coordinator?.EnterNotReady(
            CSharpDbReadinessReason.RestoreInProgress);
        try
        {
            TResponse response;
            try
            {
                response = await ExecuteAsync(context, action, map)
                    .ConfigureAwait(false);
            }
            catch
            {
                coordinator?.RequestRecovery(
                    CSharpDbReadinessReason.ReopenPending);
                throw;
            }

            await VerifyReadyAsync(
                    context,
                    coordinator,
                    CSharpDbReadinessReason.ReopenPending)
                .ConfigureAwait(false);
            return response;
        }
        finally
        {
            lease?.Dispose();
        }
    }

    private async Task VerifyReadyAsync(
        ServerCallContext context,
        CSharpDbHostReadinessCoordinator? coordinator,
        CSharpDbReadinessReason failureReason)
    {
        if (coordinator is null)
            return;

        try
        {
            _ = await GetInfoForReadinessAsync(context)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (
            context.CancellationToken.IsCancellationRequested)
        {
            coordinator.RequestRecovery(failureReason);
            throw;
        }
        catch (Exception exception)
        {
            coordinator.RequestRecovery(failureReason);
            throw TranslateException(exception);
        }
    }

    private async Task RequestRecoveryIfUnavailableAsync(
        ServerCallContext context,
        CSharpDbHostReadinessCoordinator? coordinator,
        CSharpDbReadinessReason failureReason)
    {
        if (coordinator is null)
            return;

        try
        {
            _ = await GetInfoForReadinessAsync(context)
                .ConfigureAwait(false);
        }
        catch
        {
            coordinator.RequestRecovery(failureReason);
        }
    }

    private async Task<DatabaseInfo> GetInfoForReadinessAsync(
        ServerCallContext context)
    {
        using var verificationCancellation = CancellationTokenSource
            .CreateLinkedTokenSource(context.CancellationToken);
        verificationCancellation.CancelAfter(readinessTimeout);

        Task<DatabaseInfo> attempt = client.GetInfoAsync(
            verificationCancellation.Token);
        try
        {
            return await attempt.WaitAsync(
                    readinessTimeout,
                    context.CancellationToken)
                .ConfigureAwait(false);
        }
        catch (TimeoutException exception)
        {
            verificationCancellation.Cancel();
            ObserveReadinessAttempt(attempt);
            throw new TimeoutException(
                "CSharpDB post-maintenance readiness verification exceeded the configured timeout.",
                exception);
        }
        catch (OperationCanceledException exception) when (
            !context.CancellationToken.IsCancellationRequested &&
            verificationCancellation.IsCancellationRequested)
        {
            ObserveReadinessAttempt(attempt);
            throw new TimeoutException(
                "CSharpDB post-maintenance readiness verification exceeded the configured timeout.",
                exception);
        }
        catch (OperationCanceledException) when (
            context.CancellationToken.IsCancellationRequested)
        {
            ObserveReadinessAttempt(attempt);
            throw;
        }
    }

    private static void ObserveReadinessAttempt(Task attempt)
        => _ = attempt.ContinueWith(
            static completed => _ = completed.Exception,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously |
            TaskContinuationOptions.OnlyOnFaulted,
            TaskScheduler.Default);

    private static bool TryCreateStatelessTemporaryTableSqlRejection(string sql, out SqlExecutionResult result)
    {
        result = null!;

        try
        {
            foreach (string statementSql in SqlScriptSplitter.SplitExecutableStatements(sql))
            {
                Statement statement = Parser.Parse(statementSql);
                if (!SqlStatementClassifier.IsTemporaryTableStatement(statement))
                    continue;

                result = new SqlExecutionResult
                {
                    Error = "Temporary table commands require a transaction session when using stateless gRPC. Use BeginTransaction and ExecuteInTransaction for remote temporary table workflows.",
                };
                return true;
            }
        }
        catch (CoreDbException)
        {
            return false;
        }

        return false;
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

    private ICSharpDbShardAdminClient GetShardAdminClient()
        => client as ICSharpDbShardAdminClient
           ?? throw new CSharpDbClientException(
               "CSharpDB shard-admin APIs are available only when API-level sharding is enabled.");

    private ICSharpDbShardDirectoryClient GetShardDirectoryClient()
        => client as ICSharpDbShardDirectoryClient
           ?? throw new CSharpDbClientException(
               "CSharpDB shard-directory APIs are available only when API-level sharding is enabled.");

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
            CoreErrorCode.ResourceLimitExceeded => StatusCode.ResourceExhausted,
            CoreErrorCode.IoError or CoreErrorCode.JournalError or CoreErrorCode.WalError => StatusCode.Unavailable,
            CoreErrorCode.CorruptDatabase => StatusCode.DataLoss,
            _ => StatusCode.Unknown,
        };
}
