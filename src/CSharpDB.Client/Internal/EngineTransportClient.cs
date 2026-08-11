using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using CSharpDB.Client.Models;
using CSharpDB.Engine;
using CSharpDB.ImportExport.TableArchives;
using CSharpDB.Observability;
using CoreColumnDefinition = CSharpDB.Primitives.ColumnDefinition;
using CoreCheckConstraintDefinition = CSharpDB.Primitives.CheckConstraintDefinition;
using CoreDbType = CSharpDB.Primitives.DbType;
using CoreForeignKeyDefinition = CSharpDB.Primitives.ForeignKeyDefinition;
using CoreForeignKeyOnDeleteAction = CSharpDB.Primitives.ForeignKeyOnDeleteAction;
using CoreIndexSchema = CSharpDB.Primitives.IndexSchema;
using CoreKeyConstraintDefinition = CSharpDB.Primitives.KeyConstraintDefinition;
using CoreKeyConstraintKind = CSharpDB.Primitives.KeyConstraintKind;
using CoreSqlIdentifierRules = CSharpDB.Primitives.SqlIdentifierRules;
using CoreSqlTypeDescriptor = CSharpDB.Primitives.SqlTypeDescriptor;
using CoreSqlTypeKind = CSharpDB.Primitives.SqlTypeKind;
using CoreTextCodec = CSharpDB.Primitives.CSharpDbTextCodec;
using CoreTableSchema = CSharpDB.Primitives.TableSchema;
using CoreTriggerEvent = CSharpDB.Primitives.TriggerEvent;
using CoreTriggerSchema = CSharpDB.Primitives.TriggerSchema;
using CoreTriggerTiming = CSharpDB.Primitives.TriggerTiming;
using ObservabilityTransport = CSharpDB.Observability.CSharpDbTransport;

namespace CSharpDB.Client.Internal;

internal sealed partial class EngineTransportClient :
    ICSharpDbClient,
    ICSharpDbObservabilityClient,
    IEngineBackedClient,
    IClientObservabilitySettingsProvider,
    ICSharpDbTableArchiveProgressExporter,
    ICSharpDbTransactionalSnapshotReader,
    ICSharpDbTransactionalSchemaIdentityWriter
{
    private const string CollectionPrefix = "_col_";
    private const string ProcedureTableName = "__procedures";
    private const string SavedQueryTableName = "__saved_queries";
    private const string ExternalTablesTableName = "__external_tables";
    private const string DataModelDiagramsTableName = "__data_model_diagrams";
    private static readonly Regex s_identifierPattern = new("^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.Compiled);
    private static readonly AsyncLocal<DisposeFlushToken?> s_disposeFlushToken = new();

    private readonly string _databasePath;
    private readonly DatabaseOptions _directDatabaseOptions;
    private readonly HybridDatabaseOptions? _hybridDatabaseOptions;
    private readonly Func<string, DatabaseOptions, CancellationToken, Task<Database>> _openDatabaseAsync;
    private readonly bool _observabilityEnabled;
    private readonly bool _operationalEventsEnabled;
    private readonly bool _queryEventsEnabled;
    private readonly bool _slowQueryEventsEnabled;
    private readonly string _observabilityDatabaseAlias;
    private readonly TimeSpan _scriptSlowQueryThreshold;
    private readonly TimeSpan _procedureSlowQueryThreshold;
    private readonly TimeProvider _observabilityTimeProvider;
    private RuntimeDatabaseFamily _runtimeDatabaseFamily;
    private readonly object _runtimeDiagnosticsLifetimeGate = new();
    private Dictionary<CSharpDbRuntimeDiagnosticsState, int>?
        _runtimeDiagnosticsSessionOwners;
    private HashSet<CSharpDbRuntimeDiagnosticsState>?
        _retiredRuntimeDiagnosticsStates;
    private CSharpDbRuntimeDiagnosticsState? _disabledRuntimeDiagnosticsState;
    private DirectDiagnosticsSession? _diagnosticsSession;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly object _databaseGate = new();
    private readonly object _disposeGate = new();
    private readonly ConcurrentDictionary<string, ClientTransactionSession> _transactions = new(StringComparer.Ordinal);
    private int _activeFinalizations;
    private TaskCompletionSource? _finalizationsDrained;
    private Task<Database>? _databaseTask;
    private List<CSharpDbDeferredDiagnosticBoundary>? _databaseOpenBoundaries;
    private TaskCompletionSource? _databaseReleaseCompletion;
    private Task? _disposeTask;
    private bool _disposeStarted;
    private int _exclusiveMaintenanceActive;
    private long _databaseOwnershipEpoch;
    private bool _catalogsInitialized;
    private List<ClientLockBoundaryState>? _clientLockBoundaryStates;

    public EngineTransportClient(
        string databasePath,
        DatabaseOptions? directDatabaseOptions = null,
        HybridDatabaseOptions? hybridDatabaseOptions = null)
        : this(
            NormalizeDisplayDataSource(databasePath),
            CreateRuntimeDatabaseOptions(directDatabaseOptions, timeProvider: null),
            hybridDatabaseOptions)
    {
    }

    private EngineTransportClient(
        string databasePath,
        RuntimeDatabaseOptions runtimeDatabaseOptions,
        HybridDatabaseOptions? hybridDatabaseOptions)
        : this(
            databasePath,
            CreateOpenDatabaseAsync(hybridDatabaseOptions),
            runtimeDatabaseOptions,
            hybridDatabaseOptions,
            observabilityTimeProvider: null)
    {
    }

    internal EngineTransportClient(
        string databasePath,
        Func<string, CancellationToken, Task<Database>> openDatabaseAsync,
        DatabaseOptions? directDatabaseOptions = null,
        HybridDatabaseOptions? hybridDatabaseOptions = null,
        TimeProvider? observabilityTimeProvider = null)
        : this(
            databasePath,
            AdaptOpenDatabaseAsync(openDatabaseAsync),
            CreateRuntimeDatabaseOptions(directDatabaseOptions, observabilityTimeProvider),
            hybridDatabaseOptions,
            observabilityTimeProvider)
    {
    }

    internal EngineTransportClient(
        string databasePath,
        Func<string, DatabaseOptions, CancellationToken, Task<Database>> openDatabaseAsync,
        DatabaseOptions? directDatabaseOptions = null,
        HybridDatabaseOptions? hybridDatabaseOptions = null,
        TimeProvider? observabilityTimeProvider = null)
        : this(
            databasePath,
            openDatabaseAsync,
            CreateRuntimeDatabaseOptions(directDatabaseOptions, observabilityTimeProvider),
            hybridDatabaseOptions,
            observabilityTimeProvider)
    {
    }

    private EngineTransportClient(
        string databasePath,
        Func<string, DatabaseOptions, CancellationToken, Task<Database>> openDatabaseAsync,
        RuntimeDatabaseOptions runtimeDatabaseOptions,
        HybridDatabaseOptions? hybridDatabaseOptions,
        TimeProvider? observabilityTimeProvider)
    {
        _databasePath = databasePath;
        _directDatabaseOptions = runtimeDatabaseOptions.Value;
        _hybridDatabaseOptions = hybridDatabaseOptions;
        _openDatabaseAsync = openDatabaseAsync ?? throw new ArgumentNullException(nameof(openDatabaseAsync));
        _observabilityTimeProvider = observabilityTimeProvider ?? TimeProvider.System;
        _runtimeDatabaseFamily = new RuntimeDatabaseFamily(
            _directDatabaseOptions,
            runtimeDatabaseOptions.AdvanceCounterEpochOnFirstSuccessfulOpen);
        CSharpDbObservabilityOptions? observability = _directDatabaseOptions.ObservabilityOptions;
        _observabilityEnabled = observability?.Enabled == true;
        _operationalEventsEnabled = _observabilityEnabled &&
            observability!.Logging?.Enabled == true;
        _queryEventsEnabled = _observabilityEnabled &&
            observability!.Logging?.Enabled == true &&
            observability.Logging.Queries;
        _slowQueryEventsEnabled = _observabilityEnabled &&
            observability!.Logging?.Enabled == true &&
            observability.Logging.SlowQueries;
        _observabilityDatabaseAlias =
            CSharpDbObservabilityOptions.IsValidDatabaseAlias(observability?.DatabaseAlias)
                ? observability!.DatabaseAlias
                : "default";
        _scriptSlowQueryThreshold = GetConfiguredSlowQueryThreshold(
            observability?.Logging,
            CSharpDbOperationClass.Script);
        _procedureSlowQueryThreshold = GetConfiguredSlowQueryThreshold(
            observability?.Logging,
            CSharpDbOperationClass.Procedure);
    }

    internal static EngineTransportClient CreatePrivateMemory(
        string displayName,
        string? loadFromPath,
        DatabaseOptions? directDatabaseOptions)
    {
        RuntimeDatabaseOptions runtimeDatabaseOptions =
            CreateRuntimeDatabaseOptions(directDatabaseOptions, timeProvider: null);
        Func<string, DatabaseOptions, CancellationToken, Task<Database>> openDatabaseAsync =
            string.IsNullOrWhiteSpace(loadFromPath)
                ? (_, options, ct) => Database.OpenInMemoryAsync(options, ct).AsTask()
                : (_, options, ct) => Database.LoadIntoMemoryAsync(loadFromPath, options, ct).AsTask();
        return new EngineTransportClient(
            displayName,
            openDatabaseAsync,
            runtimeDatabaseOptions,
            hybridDatabaseOptions: null,
            observabilityTimeProvider: null);
    }

    public string DataSource => _databasePath;
    public bool SupportsTableArchiveExport => true;
    public bool SupportsTransactionalSnapshotReads => true;
    public bool SupportsTransactionalSchemaIdentityWrites => true;
    internal DatabaseOptions DirectDatabaseOptions => _directDatabaseOptions;
    internal DatabaseOptions CurrentDatabaseOptionsForOpen =>
        Volatile.Read(ref _runtimeDatabaseFamily).DatabaseOptions;
    internal CSharpDbRuntimeDiagnosticsState? CurrentRuntimeDiagnosticsState =>
        Volatile.Read(ref _runtimeDatabaseFamily).RuntimeDiagnosticsState;
    internal HybridDatabaseOptions? HybridDatabaseOptions => _hybridDatabaseOptions;
    internal string? RuntimeDiagnosticsServerInstanceId =>
        CurrentRuntimeDiagnosticsState?.ServerInstanceId;
    internal long? RuntimeDiagnosticsCounterEpoch =>
        CurrentRuntimeDiagnosticsState?.CounterEpoch;
    internal bool UsesCurrentRuntimeDiagnosticsState(Database database)
    {
        ArgumentNullException.ThrowIfNull(database);
        return ReferenceEquals(
            CurrentRuntimeDiagnosticsState,
            database.RuntimeDiagnosticsState);
    }
    CSharpDbObservabilityOptions? IClientObservabilitySettingsProvider.ObservabilityOptions
        => _directDatabaseOptions.ObservabilityOptions;
    ObservabilityTransport IClientObservabilitySettingsProvider.ObservabilityTransport
        => ObservabilityTransport.Direct;

    public Task<DatabaseInfo> GetInfoAsync(CancellationToken ct = default)
        => GetInfoCoreAsync(ct);

    public async Task<IReadOnlyList<string>> GetTableNamesAsync(CancellationToken ct = default)
    {
        var db = await GetDatabaseAsync(ct);
        return db.GetTableNames()
            .Where(name => !IsInternalTable(name))
            .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public async Task<TableSchema?> GetTableSchemaAsync(string tableName, CancellationToken ct = default)
    {
        var db = await GetDatabaseAsync(ct);
        if (IsInternalTable(tableName))
            return null;

        var schema = db.GetTableSchema(
            RequireCatalogIdentifier(tableName, nameof(tableName)));
        return schema is null ? null : MapTableSchema(schema);
    }

    public async Task<int> GetRowCountAsync(string tableName, CancellationToken ct = default)
    {
        var db = await GetDatabaseAsync(ct);
        string normalizedTableName =
            RequireCatalogIdentifier(tableName, nameof(tableName));
        var schema = db.GetTableSchema(normalizedTableName);
        if (schema is null || IsInternalTable(normalizedTableName))
            throw new CSharpDbClientException($"Table '{normalizedTableName}' was not found.");

        return await CountRowsViaScalarAsync(db, normalizedTableName, ct);
    }

    public async Task<TableBrowseResult> BrowseTableAsync(string tableName, int page = 1, int pageSize = 50, CancellationToken ct = default)
    {
        var db = await GetDatabaseAsync(ct);
        return await BrowseTablePageAsync(db, tableName, page, pageSize, ct);
    }

    public async Task<TableArchiveExportResult> ExportTableArchiveAsync(
        string tableName,
        string path,
        CancellationToken ct = default)
        => await ExportTableArchiveAsync(tableName, path, progress: null, ct);

    public async Task<TableArchiveExportResult> ExportTableArchiveAsync(
        string tableName,
        string path,
        IProgress<TableArchiveExportProgress>? progress,
        CancellationToken ct = default)
    {
        string normalizedTableName =
            RequireCatalogIdentifier(tableName, nameof(tableName));
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        var db = await GetDatabaseAsync(ct);
        var schema = db.GetTableSchema(normalizedTableName);
        if (schema is null || IsInternalTable(normalizedTableName))
            throw new CSharpDbClientException($"Table '{normalizedTableName}' was not found.");

        int totalRows = await CountRowsViaScalarAsync(db, normalizedTableName, ct);
        ReportArchiveExportProgress(
            progress,
            normalizedTableName,
            "Preparing",
            "Preparing snapshot",
            rowsExported: 0,
            totalRows,
            path);

        using var reader = db.CreateReaderSession();
        await using var result = await reader.ExecuteReadAsync(
            $"SELECT * FROM {CoreSqlIdentifierRules.Quote(normalizedTableName)}",
            ct);
        ReportArchiveExportProgress(
            progress,
            normalizedTableName,
            "Exporting",
            "Writing table archive",
            rowsExported: 0,
            totalRows,
            path);
        var rows = ReportArchiveRowsAsync(result.GetRowsAsync(ct), normalizedTableName, totalRows, path, progress, ct);
        CSharpDB.Primitives.IndexSchema[] secondaryIndexes = db.GetIndexes()
            .Where(index =>
                index.Kind == CSharpDB.Primitives.IndexKind.Sql &&
                index.State == CSharpDB.Primitives.IndexState.Ready &&
                string.Equals(index.TableName, normalizedTableName, StringComparison.OrdinalIgnoreCase))
            .ToArray();
        var manifest = await TableArchiveWriter.WriteAsync(
            path,
            schema,
            secondaryIndexes,
            rows,
            ct);
        ReportArchiveExportProgress(
            progress,
            normalizedTableName,
            "Finalizing",
            "Finalizing archive",
            manifest.RowCount,
            totalRows,
            path);

        return new TableArchiveExportResult
        {
            TableName = normalizedTableName,
            Path = path,
            FileName = Path.GetFileName(path),
            RowCount = manifest.RowCount,
        };
    }

    private static async IAsyncEnumerable<CSharpDB.Primitives.DbValue[]> ReportArchiveRowsAsync(
        IAsyncEnumerable<CSharpDB.Primitives.DbValue[]> rows,
        string tableName,
        long totalRows,
        string path,
        IProgress<TableArchiveExportProgress>? progress,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        long exported = 0;
        var interval = Stopwatch.StartNew();
        await foreach (var row in rows.WithCancellation(ct).ConfigureAwait(false))
        {
            ct.ThrowIfCancellationRequested();
            yield return row;
            exported++;

            if (exported == totalRows || exported % 1_000 == 0 || interval.ElapsedMilliseconds >= 500)
            {
                ReportArchiveExportProgress(
                    progress,
                    tableName,
                    "Exporting",
                    "Writing table archive",
                    exported,
                    totalRows,
                    path);
                interval.Restart();
                await Task.Yield();
            }
        }

        ct.ThrowIfCancellationRequested();
        ReportArchiveExportProgress(
            progress,
            tableName,
            "Exporting",
            "Writing table archive",
            exported,
            totalRows,
            path);
    }

    private static void ReportArchiveExportProgress(
        IProgress<TableArchiveExportProgress>? progress,
        string tableName,
        string stage,
        string message,
        long rowsExported,
        long? totalRows,
        string path)
    {
        progress?.Report(new TableArchiveExportProgress
        {
            TableName = tableName,
            Stage = stage,
            Message = message,
            RowsExported = rowsExported,
            TotalRows = totalRows,
            Path = path,
        });
    }

    public async Task<Dictionary<string, object?>?> GetRowByPkAsync(string tableName, string pkColumn, object pkValue, CancellationToken ct = default)
    {
        using IDisposable? transportScope = EnterDirectTransportScope();
        string normalizedTableName;
        string normalizedPkColumn;
        string sql;
        try
        {
            normalizedTableName = RequireCatalogIdentifier(tableName, nameof(tableName));
            normalizedPkColumn = RequireCatalogIdentifier(pkColumn, nameof(pkColumn));
            sql =
                $"SELECT * FROM {CoreSqlIdentifierRules.Quote(normalizedTableName)} " +
                $"WHERE {CoreSqlIdentifierRules.Quote(normalizedPkColumn)} = {FormatSqlLiteral(pkValue)}";
        }
        catch (Exception exception)
        {
            CompositeQueryOperation? invalidOperation = StartCompositeQueryOperation(
                CSharpDbOperationClass.Query);
            using IDisposable? invalidOperationScope = invalidOperation?.EnterScope();
            invalidOperation?.Fail(exception);
            throw;
        }

        CompositeQueryOperation? operation = StartCompositeQueryOperation(
            CSharpDbOperationClass.Query,
            sql);
        using IDisposable? operationScope = operation?.EnterScope();
        bool queryDispatched = false;
        try
        {
            var db = await GetDatabaseAsync(ct);
            operation?.MarkDequeued();
            using IDisposable? queueDurationScope = operation?.EnterQueueDurationScope();
            var schema = db.GetTableSchema(normalizedTableName);
            if (schema is null || IsInternalTable(normalizedTableName))
                throw new CSharpDbClientException($"Table '{normalizedTableName}' was not found.");

            if (!schema.Columns.Any(column => string.Equals(column.Name, normalizedPkColumn, StringComparison.OrdinalIgnoreCase)))
                throw new CSharpDbClientException($"Column '{normalizedPkColumn}' was not found in table '{normalizedTableName}'.");

            queryDispatched = true;
            await using var result = await db.ExecuteAsync(sql, ct);
            if (!result.IsQuery || !await result.MoveNextAsync(ct))
                return null;

            return ToRowDictionary(result.Schema, result.Current);
        }
        catch (Exception exception)
        {
            if (!queryDispatched)
                operation?.Fail(exception);
            throw;
        }
    }

    public async Task<int> InsertRowAsync(string tableName, Dictionary<string, object?> values, CancellationToken ct = default)
    {
        string normalizedTableName =
            RequireCatalogIdentifier(tableName, nameof(tableName));
        if (values.Count == 0)
        {
            return await ExecuteNonQueryAsync(
                await GetDatabaseAsync(ct),
                $"INSERT INTO {CoreSqlIdentifierRules.Quote(normalizedTableName)} DEFAULT VALUES",
                ct);
        }

        var assignments = values.Select(kvp =>
            new KeyValuePair<string, object?>(
                RequireCatalogIdentifier(kvp.Key, nameof(values)),
                kvp.Value)).ToArray();
        string columns = string.Join(
            ", ",
            assignments.Select(item =>
                CoreSqlIdentifierRules.Quote(item.Key)));
        string literals = string.Join(", ", assignments.Select(item => FormatSqlLiteral(item.Value)));
        return await ExecuteNonQueryAsync(
            await GetDatabaseAsync(ct),
            $"INSERT INTO {CoreSqlIdentifierRules.Quote(normalizedTableName)} ({columns}) VALUES ({literals})",
            ct);
    }

    public async Task<int> UpdateRowAsync(string tableName, string pkColumn, object pkValue, Dictionary<string, object?> values, CancellationToken ct = default)
    {
        if (values.Count == 0)
            return 0;

        string normalizedTableName =
            RequireCatalogIdentifier(tableName, nameof(tableName));
        string normalizedPkColumn =
            RequireCatalogIdentifier(pkColumn, nameof(pkColumn));
        string setClause = string.Join(
            ", ",
            values.Select(kvp =>
                $"{CoreSqlIdentifierRules.Quote(RequireCatalogIdentifier(kvp.Key, nameof(values)))} = {FormatSqlLiteral(kvp.Value)}"));
        string sql =
            $"UPDATE {CoreSqlIdentifierRules.Quote(normalizedTableName)} " +
            $"SET {setClause} WHERE {CoreSqlIdentifierRules.Quote(normalizedPkColumn)} = {FormatSqlLiteral(pkValue)}";
        return await ExecuteNonQueryAsync(await GetDatabaseAsync(ct), sql, ct);
    }

    public async Task<int> DeleteRowAsync(string tableName, string pkColumn, object pkValue, CancellationToken ct = default)
    {
        string normalizedTableName =
            RequireCatalogIdentifier(tableName, nameof(tableName));
        string normalizedPkColumn =
            RequireCatalogIdentifier(pkColumn, nameof(pkColumn));
        string sql =
            $"DELETE FROM {CoreSqlIdentifierRules.Quote(normalizedTableName)} " +
            $"WHERE {CoreSqlIdentifierRules.Quote(normalizedPkColumn)} = {FormatSqlLiteral(pkValue)}";
        return await ExecuteNonQueryAsync(await GetDatabaseAsync(ct), sql, ct);
    }

    public async Task DropTableAsync(string tableName, CancellationToken ct = default)
        => await ExecuteStatementAsync(
            await GetDatabaseAsync(ct),
            $"DROP TABLE {QuoteCatalogIdentifier(tableName, nameof(tableName))}",
            ct);

    public async Task RenameTableAsync(string tableName, string newTableName, CancellationToken ct = default)
        => await ExecuteStatementAsync(
            await GetDatabaseAsync(ct),
            $"ALTER TABLE {QuoteCatalogIdentifier(tableName, nameof(tableName))} " +
            $"RENAME TO {QuoteCatalogIdentifier(newTableName, nameof(newTableName))}",
            ct);

    public Task AddColumnAsync(string tableName, string columnName, Models.DbType type, bool notNull, CancellationToken ct = default)
        => AddColumnAsync(tableName, columnName, type, notNull, collation: null, ct);

    public async Task AddColumnAsync(string tableName, string columnName, Models.DbType type, bool notNull, string? collation, CancellationToken ct = default)
    {
        string sql =
            $"ALTER TABLE {QuoteCatalogIdentifier(tableName, nameof(tableName))} " +
            $"ADD COLUMN {BuildColumnDefinitionSql(columnName, type, notNull, collation)}";
        await ExecuteStatementAsync(await GetDatabaseAsync(ct), sql, ct);
    }

    public async Task DropColumnAsync(string tableName, string columnName, CancellationToken ct = default)
        => await ExecuteStatementAsync(
            await GetDatabaseAsync(ct),
            $"ALTER TABLE {QuoteCatalogIdentifier(tableName, nameof(tableName))} " +
            $"DROP COLUMN {QuoteCatalogIdentifier(columnName, nameof(columnName))}",
            ct);

    public async Task RenameColumnAsync(string tableName, string oldColumnName, string newColumnName, CancellationToken ct = default)
        => await ExecuteStatementAsync(
            await GetDatabaseAsync(ct),
            $"ALTER TABLE {QuoteCatalogIdentifier(tableName, nameof(tableName))} " +
            $"RENAME COLUMN {QuoteCatalogIdentifier(oldColumnName, nameof(oldColumnName))} " +
            $"TO {QuoteCatalogIdentifier(newColumnName, nameof(newColumnName))}",
            ct);

    public async Task<IReadOnlyList<IndexSchema>> GetIndexesAsync(CancellationToken ct = default)
    {
        var db = await GetDatabaseAsync(ct);
        return db.GetIndexes()
            .Where(index => index.Kind is not (
                CSharpDB.Primitives.IndexKind.ForeignKeyInternal or
                CSharpDB.Primitives.IndexKind.ConstraintInternal))
            .Select(MapIndexSchema)
            .OrderBy(index => index.IndexName, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public Task CreateIndexAsync(string indexName, string tableName, string columnName, bool isUnique, CancellationToken ct = default)
        => CreateIndexAsync(indexName, tableName, columnName, isUnique, collation: null, ct);

    public async Task CreateIndexAsync(string indexName, string tableName, string columnName, bool isUnique, string? collation, CancellationToken ct = default)
        => await ExecuteStatementAsync(await GetDatabaseAsync(ct), BuildCreateIndexSql(indexName, tableName, columnName, isUnique, collation), ct);

    public Task UpdateIndexAsync(string existingIndexName, string newIndexName, string tableName, string columnName, bool isUnique, CancellationToken ct = default)
        => UpdateIndexAsync(existingIndexName, newIndexName, tableName, columnName, isUnique, collation: null, ct);

    public async Task UpdateIndexAsync(string existingIndexName, string newIndexName, string tableName, string columnName, bool isUnique, string? collation, CancellationToken ct = default)
    {
        var db = await GetDatabaseAsync(ct);
        await ExecuteInSingleTransactionAsync(db, ct,
            $"DROP INDEX {RequireIdentifier(existingIndexName, nameof(existingIndexName))}",
            BuildCreateIndexSql(newIndexName, tableName, columnName, isUnique, collation));
    }

    public async Task DropIndexAsync(string indexName, CancellationToken ct = default)
        => await ExecuteStatementAsync(await GetDatabaseAsync(ct), $"DROP INDEX {RequireIdentifier(indexName, nameof(indexName))}", ct);

    public async Task<IReadOnlyList<ViewDefinition>> GetViewsAsync(CancellationToken ct = default)
    {
        var db = await GetDatabaseAsync(ct);
        return db.GetViewNames()
            .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
            .Select(name => new ViewDefinition
            {
                Name = name,
                Sql = db.GetViewSql(name) ?? string.Empty,
            })
            .ToArray();
    }

    public async Task<ViewDefinition?> GetViewAsync(string viewName, CancellationToken ct = default)
    {
        var db = await GetDatabaseAsync(ct);
        string normalizedViewName = RequireIdentifier(viewName, nameof(viewName));
        string? sql = db.GetViewSql(normalizedViewName);
        return sql is null ? null : new ViewDefinition { Name = normalizedViewName, Sql = sql };
    }

    public async Task<ViewBrowseResult> BrowseViewAsync(string viewName, int page = 1, int pageSize = 50, CancellationToken ct = default)
    {
        var db = await GetDatabaseAsync(ct);
        string normalizedViewName = RequireIdentifier(viewName, nameof(viewName));
        var result = await ExecuteQueryAsync(db, $"SELECT * FROM {normalizedViewName}", ct);
        return PageViewResult(
            new ViewBrowseResult
            {
                ViewName = normalizedViewName,
                ColumnNames = result.ColumnNames ?? [],
                ColumnTypes = result.ColumnTypes,
                Rows = result.Rows ?? [],
                TotalRows = result.Rows?.Count ?? 0,
                Page = 1,
                PageSize = Math.Max(result.Rows?.Count ?? 0, 1),
            },
            page,
            pageSize);
    }

    public async Task CreateViewAsync(string viewName, string selectSql, CancellationToken ct = default)
        => await ExecuteStatementAsync(await GetDatabaseAsync(ct), $"CREATE VIEW {RequireIdentifier(viewName, nameof(viewName))} AS {NormalizeEmbeddedSql(selectSql)}", ct);

    public async Task UpdateViewAsync(string existingViewName, string newViewName, string selectSql, CancellationToken ct = default)
    {
        var db = await GetDatabaseAsync(ct);
        await ExecuteInSingleTransactionAsync(db, ct,
            $"DROP VIEW {RequireIdentifier(existingViewName, nameof(existingViewName))}",
            $"CREATE VIEW {RequireIdentifier(newViewName, nameof(newViewName))} AS {NormalizeEmbeddedSql(selectSql)}");
    }

    public async Task DropViewAsync(string viewName, CancellationToken ct = default)
        => await ExecuteStatementAsync(await GetDatabaseAsync(ct), $"DROP VIEW {RequireIdentifier(viewName, nameof(viewName))}", ct);

    public async Task<IReadOnlyList<TriggerSchema>> GetTriggersAsync(CancellationToken ct = default)
    {
        var db = await GetDatabaseAsync(ct);
        return db.GetTriggers()
            .Select(MapTriggerSchema)
            .OrderBy(trigger => trigger.TriggerName, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public async Task CreateTriggerAsync(string triggerName, string tableName, TriggerTiming timing, TriggerEvent triggerEvent, string bodySql, CancellationToken ct = default)
        => await ExecuteStatementAsync(await GetDatabaseAsync(ct), BuildCreateTriggerSql(triggerName, tableName, timing, triggerEvent, bodySql), ct);

    public async Task UpdateTriggerAsync(string existingTriggerName, string newTriggerName, string tableName, TriggerTiming timing, TriggerEvent triggerEvent, string bodySql, CancellationToken ct = default)
    {
        var db = await GetDatabaseAsync(ct);
        await ExecuteInSingleTransactionAsync(db, ct,
            $"DROP TRIGGER {RequireIdentifier(existingTriggerName, nameof(existingTriggerName))}",
            BuildCreateTriggerSql(newTriggerName, tableName, timing, triggerEvent, bodySql));
    }

    public async Task DropTriggerAsync(string triggerName, CancellationToken ct = default)
        => await ExecuteStatementAsync(await GetDatabaseAsync(ct), $"DROP TRIGGER {RequireIdentifier(triggerName, nameof(triggerName))}", ct);

    public Task<SqlExecutionResult> ExecuteSqlAsync(string sql, CancellationToken ct = default)
        => ExecuteSqlCoreAsync(sql, ct);

    public async Task<TransactionSessionInfo> BeginTransactionAsync(CancellationToken ct = default)
    {
        using ClientLockLease clientLock = await AcquireClientLockAsync(ct);
        ThrowIfDisposing();
        long reuseEpoch = CaptureDatabaseOwnershipEpoch();
        Database? database = null;
        if (_hybridDatabaseOptions is null)
        {
            database = await DetachCachedDatabaseCoreAsync(
                ct,
                "Cannot start a client-managed transaction while direct snapshot readers are active.");
            if (database is not null)
            {
                try
                {
                    await database.ResetReusableSessionStateAsync();
                }
                catch
                {
                    await database.DisposeAsync();
                    throw;
                }
            }
        }
        else
        {
            // Hybrid persistence may be configured to run only on Dispose.
            // Preserve that physical-close boundary rather than retaining
            // the handle across logical transaction sessions.
            await ReleaseCachedDatabaseCoreAsync(
                ct,
                "Cannot start a client-managed transaction while direct snapshot readers are active.");
        }

        database ??= await OpenOwnedDatabaseAsync(_databasePath, ct);
        try
        {
            await database.BeginTransactionAsync(ct);
        }
        catch
        {
            await database.DisposeAsync();
            throw;
        }

        string transactionId = Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture);
        var session = new ClientTransactionSession(
            database,
            reuseEpoch,
            _observabilityEnabled
                ? database.RuntimeDiagnosticsState?.TimeProvider ??
                  _observabilityTimeProvider
                : null,
            database.RuntimeDiagnosticsState?.SessionAbandonmentThreshold ??
            _directDatabaseOptions.ObservabilityOptions?.SessionAbandonmentThreshold ??
            TimeSpan.FromMinutes(30));
        if (!_transactions.TryAdd(transactionId, session))
        {
            try
            {
                await database.RollbackAsync(CancellationToken.None);
            }
            finally
            {
                await database.DisposeAsync();
            }

            throw new CSharpDbClientException("Failed to register the transaction session.");
        }

        RetainRuntimeDiagnosticsState(session);

        if (_transactions.Count > 1)
        {
            foreach (ClientTransactionSession activeSession in _transactions.Values)
                activeSession.DisableReuse();
        }

        return new TransactionSessionInfo
        {
            TransactionId = transactionId,
            ExpiresAtUtc = DateTime.MaxValue,
        };
    }

    public async Task<SqlExecutionResult> ExecuteInTransactionAsync(string transactionId, string sql, CancellationToken ct = default)
    {
        OpaqueDiagnosticsId? diagnosticsSessionId =
            TryGetTransactionDiagnosticsSessionId(transactionId);
        using IDisposable? transportScope =
            EnterDirectTransportScopeForSession(diagnosticsSessionId);
        CompositeQueryOperation? operation = StartCompositeQueryOperation(
            CSharpDbOperationClass.Query,
            sql,
            diagnosticsSessionId);
        using IDisposable? operationScope = operation?.EnterScope();
        ClientTransactionSession? session = null;
        bool operationGateEntered = false;
        long queueStartingTimestamp = 0;
        bool queueMeasurementStarted = false;
        bool queryDispatched = false;
        try
        {
            session = GetTransactionSession(transactionId);
            operation?.BindRuntimeDiagnosticsState(
                session.Database.RuntimeDiagnosticsState);
            if (operation is not null)
            {
                queueStartingTimestamp = _observabilityTimeProvider.GetTimestamp();
                queueMeasurementStarted = true;
            }

            bool entered = await session.TryEnterOperationAsync(ct);
            if (queueMeasurementStarted)
            {
                operation?.MarkDequeued(
                    _observabilityTimeProvider.GetElapsedTime(queueStartingTimestamp));
            }

            if (!entered)
                throw TransactionNotFound(transactionId);
            operationGateEntered = true;
            session.SetCurrentDiagnosticsOperation(operation?.OperationId);
            using IDisposable? queueDurationScope = operation?.EnterQueueDurationScope();

            queryDispatched = true;
            return await ExecuteQueryAsync(session.Database, sql, ct);
        }
        catch (Exception exception)
        {
            if (!queryDispatched)
            {
                if (!operationGateEntered)
                {
                    operation?.MarkDequeued(queueMeasurementStarted
                        ? _observabilityTimeProvider.GetElapsedTime(queueStartingTimestamp)
                        : TimeSpan.Zero);
                }
                operation?.Fail(exception);
            }
            throw;
        }
        finally
        {
            if (operationGateEntered)
            {
                session!.ClearCurrentDiagnosticsOperation(operation?.OperationId);
                session!.ExitOperation();
            }
        }
    }

    public async ValueTask<TransactionTableSnapshot?> ReadTableSnapshotAsync(
        string transactionId,
        string tableName,
        CancellationToken ct = default)
    {
        string normalizedTableName =
            RequireCatalogIdentifier(tableName, nameof(tableName));
        if (IsInternalTable(normalizedTableName))
            return null;

        ClientTransactionSession session = GetTransactionSession(transactionId);
        if (!await session.TryEnterOperationAsync(ct))
            throw TransactionNotFound(transactionId);

        try
        {
            CoreTableSchema? schema = session.Database.GetTableSchema(normalizedTableName);
            if (schema is null)
                return null;

            IndexSchema[] indexes = session.Database.GetIndexes()
                .Where(index =>
                    index.Kind is not (
                        CSharpDB.Primitives.IndexKind.ForeignKeyInternal or
                        CSharpDB.Primitives.IndexKind.ConstraintInternal) &&
                    string.Equals(index.TableName, normalizedTableName, StringComparison.OrdinalIgnoreCase))
                .Select(MapIndexSchema)
                .OrderBy(index => index.IndexName, StringComparer.OrdinalIgnoreCase)
                .ToArray();

            return new TransactionTableSnapshot
            {
                Schema = MapTableSchema(schema),
                Indexes = indexes,
            };
        }
        finally
        {
            session.ExitOperation();
        }
    }

    public async ValueTask ApplyTableSchemaIdentitiesAsync(
        string transactionId,
        string tableName,
        TableSchema identitySource,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(identitySource);
        string normalizedTableName =
            RequireCatalogIdentifier(tableName, nameof(tableName));
        if (IsInternalTable(normalizedTableName))
        {
            throw new CSharpDbClientException(
                "Schema identities cannot be applied to an internal table.");
        }

        ClientTransactionSession session = GetTransactionSession(transactionId);
        if (!await session.TryEnterOperationAsync(ct))
            throw TransactionNotFound(transactionId);

        try
        {
            await session.Database.ApplyTableSchemaIdentitiesAsync(
                normalizedTableName,
                MapCoreTableSchema(identitySource),
                ct);
        }
        finally
        {
            session.ExitOperation();
        }
    }

    public async ValueTask<ForwardOnlyQueryCursor?> TryOpenForwardOnlyQueryCursorAsync(
        string sql,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);
        CSharpDbDeferredDiagnosticBoundary? deferredBoundary =
            CreateDeferredTransportBoundary();
        IDisposable? transportScope = deferredBoundary?.Enter() ??
            EnterDirectTransportScope();
        CompositeQueryOperation? operation = StartCompositeQueryOperation(
            CSharpDbOperationClass.Query,
            sql);
        IDisposable? operationScope = operation?.EnterScope();
        IDisposable? queueDurationScope = null;
        CSharpDB.Execution.QueryResult? result = null;
        bool boundaryTransferred = false;
        bool queryDispatched = false;
        try
        {
            Database database = await GetDatabaseAsync(ct);
            operation?.MarkDequeued();
            queueDurationScope = operation?.EnterQueueDurationScope();
            queryDispatched = true;
            result = await database.ExecuteAsync(sql, ct);
            if (!result.IsQuery)
                return null;

            var cursor = new ForwardOnlyQueryCursor(
                result,
                deferredBoundary: deferredBoundary);
            boundaryTransferred = true;
            return cursor;
        }
        catch (Exception exception)
        {
            if (!queryDispatched)
                operation?.Fail(exception);
            throw;
        }
        finally
        {
            try
            {
                if (!boundaryTransferred && result is not null)
                    await result.DisposeAsync();
            }
            finally
            {
                queueDurationScope?.Dispose();
                operationScope?.Dispose();
                transportScope?.Dispose();
                if (!boundaryTransferred)
                    deferredBoundary?.Dispose();
            }
        }
    }

    public async ValueTask<ForwardOnlyQueryCursor?> TryOpenForwardOnlyQueryCursorAsync(
        string transactionId,
        string sql,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);
        OpaqueDiagnosticsId? diagnosticsSessionId =
            TryGetTransactionDiagnosticsSessionId(transactionId);
        CSharpDbDeferredDiagnosticBoundary? deferredBoundary =
            CreateDeferredTransportBoundaryForSession(diagnosticsSessionId);
        IDisposable? transportScope = deferredBoundary?.Enter() ??
            EnterDirectTransportScope();
        CompositeQueryOperation? operation = StartCompositeQueryOperation(
            CSharpDbOperationClass.Query,
            sql,
            diagnosticsSessionId);
        IDisposable? operationScope = operation?.EnterScope();
        ClientTransactionSession? session = null;
        CSharpDB.Execution.QueryResult? result = null;
        bool operationGateTransferred = false;
        bool operationGateEntered = false;
        bool boundaryTransferred = false;
        bool diagnosticsReaderRegistered = false;
        long queueStartingTimestamp = 0;
        bool queueMeasurementStarted = false;
        bool queryDispatched = false;
        try
        {
            session = GetTransactionSession(transactionId);
            operation?.BindRuntimeDiagnosticsState(
                session.Database.RuntimeDiagnosticsState);
            if (operation is not null)
            {
                queueStartingTimestamp = _observabilityTimeProvider.GetTimestamp();
                queueMeasurementStarted = true;
            }

            bool entered = await session.TryEnterOperationAsync(ct);
            if (queueMeasurementStarted)
            {
                operation?.MarkDequeued(
                    _observabilityTimeProvider.GetElapsedTime(queueStartingTimestamp));
            }

            if (!entered)
                throw TransactionNotFound(transactionId);
            operationGateEntered = true;
            session.SetCurrentDiagnosticsOperation(operation?.OperationId);
            using IDisposable? queueDurationScope = operation?.EnterQueueDurationScope();

            queryDispatched = true;
            result = await session.Database.ExecuteAsync(sql, ct);
            if (!result.IsQuery)
                return null;

            session.AddDiagnosticsReader();
            diagnosticsReaderRegistered = true;
            var cursor = new ForwardOnlyQueryCursor(
                result,
                () =>
                {
                    session.RemoveDiagnosticsReader();
                    diagnosticsReaderRegistered = false;
                    session.ClearCurrentDiagnosticsOperation(operation?.OperationId);
                    session.ExitOperation();
                    return ValueTask.CompletedTask;
                },
                deferredBoundary);
            operationGateTransferred = true;
            boundaryTransferred = true;
            return cursor;
        }
        catch (Exception exception)
        {
            if (!queryDispatched)
            {
                if (!operationGateEntered)
                {
                    operation?.MarkDequeued(queueMeasurementStarted
                        ? _observabilityTimeProvider.GetElapsedTime(queueStartingTimestamp)
                        : TimeSpan.Zero);
                }
                operation?.Fail(exception);
            }
            throw;
        }
        finally
        {
            try
            {
                if (operationGateEntered && !operationGateTransferred)
                {
                    try
                    {
                        if (result is not null)
                            await result.DisposeAsync();
                    }
                    finally
                    {
                        if (diagnosticsReaderRegistered)
                        {
                            session!.RemoveDiagnosticsReader();
                            diagnosticsReaderRegistered = false;
                        }
                        session!.ClearCurrentDiagnosticsOperation(operation?.OperationId);
                        session!.ExitOperation();
                    }
                }
            }
            finally
            {
                operationScope?.Dispose();
                transportScope?.Dispose();
                if (!boundaryTransferred)
                    deferredBoundary?.Dispose();
            }
        }
    }

    public Task CommitTransactionAsync(string transactionId, CancellationToken ct = default)
        => CompleteTransactionAsync(transactionId, commit: true, ct);

    public Task RollbackTransactionAsync(string transactionId, CancellationToken ct = default)
        => CompleteTransactionAsync(transactionId, commit: false, ct);

    public async Task<IReadOnlyList<string>> GetCollectionNamesAsync(CancellationToken ct = default)
    {
        var db = await GetDatabaseAsync(ct);
        return db.GetCollectionNames()
            .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public async Task<int> GetCollectionCountAsync(string collectionName, CancellationToken ct = default)
    {
        var collection = await (await GetDatabaseAsync(ct)).GetCollectionAsync<JsonElement>(RequireIdentifier(collectionName, nameof(collectionName)), ct);
        return checked((int)await collection.CountAsync(ct));
    }

    public async Task<CollectionBrowseResult> BrowseCollectionAsync(string collectionName, int page = 1, int pageSize = 50, CancellationToken ct = default)
    {
        string normalizedName = RequireIdentifier(collectionName, nameof(collectionName));
        var collection = await (await GetDatabaseAsync(ct)).GetCollectionAsync<JsonElement>(normalizedName, ct);
        var documents = new List<CollectionDocument>();
        await foreach (var item in collection.ScanAsync(ct))
        {
            documents.Add(new CollectionDocument
            {
                Key = item.Key,
                Document = item.Value,
            });
        }

        documents.Sort((left, right) => string.Compare(left.Key, right.Key, StringComparison.OrdinalIgnoreCase));
        int normalizedPage = NormalizePage(page);
        int normalizedPageSize = NormalizePageSize(pageSize);
        int skip = (normalizedPage - 1) * normalizedPageSize;

        return new CollectionBrowseResult
        {
            CollectionName = normalizedName,
            Documents = documents.Skip(skip).Take(normalizedPageSize).ToArray(),
            TotalCount = documents.Count,
            Page = normalizedPage,
            PageSize = normalizedPageSize,
        };
    }

    public async Task<JsonElement?> GetDocumentAsync(string collectionName, string key, CancellationToken ct = default)
    {
        var collection = await (await GetDatabaseAsync(ct)).GetCollectionAsync<JsonElement>(RequireIdentifier(collectionName, nameof(collectionName)), ct);
        var document = await collection.GetAsync(key, ct);
        return document.ValueKind == JsonValueKind.Undefined ? null : document;
    }

    public async Task PutDocumentAsync(string collectionName, string key, JsonElement document, CancellationToken ct = default)
    {
        var collection = await (await GetDatabaseAsync(ct)).GetCollectionAsync<JsonElement>(RequireIdentifier(collectionName, nameof(collectionName)), ct);
        await collection.PutAsync(key, document, ct);
    }

    public async Task<bool> DeleteDocumentAsync(string collectionName, string key, CancellationToken ct = default)
    {
        var collection = await (await GetDatabaseAsync(ct)).GetCollectionAsync<JsonElement>(RequireIdentifier(collectionName, nameof(collectionName)), ct);
        return await collection.DeleteAsync(key, ct);
    }

    public async Task DropCollectionAsync(string collectionName, CancellationToken ct = default)
    {
        string normalizedName = RequireIdentifier(collectionName, nameof(collectionName));
        await (await GetDatabaseAsync(ct)).DropCollectionAsync(normalizedName, ct);
    }

    public async Task CheckpointAsync(CancellationToken ct = default)
    {
        using IDisposable? operationalBoundary =
            EnterDirectOperationalTransportScope();
        await (await GetDatabaseAsync(ct)).CheckpointAsync(ct);
    }

    public ValueTask DisposeAsync()
    {
        TaskCompletionSource? ownedCompletion = null;
        Task disposeTask;
        lock (_disposeGate)
        {
            if (_disposeTask is null)
            {
                ownedCompletion = new TaskCompletionSource(
                    TaskCreationOptions.RunContinuationsAsynchronously);
                _disposeTask = ownedCompletion.Task;
            }

            disposeTask = _disposeTask;
        }

        if (ownedCompletion is not null)
            _ = CompleteDisposeAsync(ownedCompletion);

        if (ownedCompletion is null &&
            TryGetDisposeFlushReentry(out ValueTask reentrantCompletion))
        {
            return reentrantCompletion;
        }

        return new ValueTask(disposeTask);
    }

    private async Task CompleteDisposeAsync(TaskCompletionSource completion)
    {
        try
        {
            await DisposeCoreAsync();
            completion.TrySetResult();
        }
        catch (Exception exception)
        {
            completion.TrySetException(exception);
        }
    }

    private async Task DisposeCoreAsync()
    {
        ClientLockLease lifecycleBoundaryLock =
            await AcquireClientLockAsync(CancellationToken.None);
        List<ClientTransactionSession> sessionsToDispose;
        Task? finalizationsDrained;
        try
        {
            _disposeStarted = true;
            sessionsToDispose = new List<ClientTransactionSession>();
            foreach (var pair in _transactions.ToArray())
            {
                if (!_transactions.TryRemove(pair.Key, out ClientTransactionSession? session))
                    continue;

                if (!session.TryClaimFinalization())
                    continue;

                RegisterFinalization();
                sessionsToDispose.Add(session);
            }

            finalizationsDrained = GetFinalizationsDrainedTask();
        }
        finally
        {
            lifecycleBoundaryLock.ReleaseLock();
        }

        Exception? disposalFailure = null;
        try
        {
            try
            {
                await Task.WhenAll(sessionsToDispose.Select(DisposeClaimedSessionAsync));
            }
            finally
            {
                if (finalizationsDrained is not null)
                    await finalizationsDrained;
            }

            using (ClientLockLease finalLock =
                   await AcquireClientLockAsync(CancellationToken.None))
            {
                if (_databaseTask is not null)
                {
                    try
                    {
                        var db = await _databaseTask;
                        await db.DisposeAsync();
                    }
                    catch
                    {
                        // ignore lazy-init failures during dispose
                    }
                }
            }

        }
        catch (Exception exception)
        {
            disposalFailure = exception;
            throw;
        }
        finally
        {
            DisposeFlushToken? previousToken = s_disposeFlushToken.Value;
            var flushToken = new DisposeFlushToken(
                this,
                previousToken,
                disposalFailure);
            s_disposeFlushToken.Value = flushToken;
            try
            {
                lifecycleBoundaryLock.Dispose();
            }
            finally
            {
                flushToken.Deactivate();
                s_disposeFlushToken.Value = previousToken;
                DisposeRuntimeDiagnosticsStates();
                _lock.Dispose();
            }
        }
    }

    private bool TryGetDisposeFlushReentry(
        out ValueTask reentrantCompletion)
    {
        for (DisposeFlushToken? token = s_disposeFlushToken.Value;
             token is not null;
             token = token.Previous)
        {
            if (token.IsActive && ReferenceEquals(token.Owner, this))
            {
                reentrantCompletion = token.Failure is null
                    ? ValueTask.CompletedTask
                    : ValueTask.FromException(token.Failure);
                return true;
            }
        }

        reentrantCompletion = default;
        return false;
    }

    private static Func<string, DatabaseOptions, CancellationToken, Task<Database>>
        CreateOpenDatabaseAsync(HybridDatabaseOptions? hybridDatabaseOptions)
    {
        return hybridDatabaseOptions is null
            ? (path, options, ct) => Database.OpenAsync(path, options, ct).AsTask()
            : (path, options, ct) => Database.OpenHybridAsync(
                path,
                options,
                hybridDatabaseOptions,
                ct).AsTask();
    }

    private static Func<string, DatabaseOptions, CancellationToken, Task<Database>>
        AdaptOpenDatabaseAsync(
            Func<string, CancellationToken, Task<Database>> openDatabaseAsync)
    {
        ArgumentNullException.ThrowIfNull(openDatabaseAsync);
        return (path, _, ct) => openDatabaseAsync(path, ct);
    }

    private static RuntimeDatabaseOptions CreateRuntimeDatabaseOptions(
        DatabaseOptions? configuredOptions,
        TimeProvider? timeProvider)
    {
        DatabaseOptions source = configuredOptions ?? new DatabaseOptions();
        CSharpDbObservabilityOptions? observability = source.ObservabilityOptions;
        CSharpDbObservabilityOptions? observabilitySnapshot = null;
        if (observability is not null)
        {
            byte[] serialized = JsonSerializer.SerializeToUtf8Bytes(
                observability,
                CSharpDbObservabilityJsonContext.Default.CSharpDbObservabilityOptions);
            observabilitySnapshot = JsonSerializer.Deserialize(
                serialized,
                CSharpDbObservabilityJsonContext.Default.CSharpDbObservabilityOptions)
                ?? throw new InvalidOperationException(
                    "Failed to snapshot the client observability configuration.");
        }

        bool retainsRuntimeIdentity =
            observabilitySnapshot?.Enabled == true &&
            source.RuntimeDiagnosticsState is not null;
        CSharpDbRuntimeDiagnosticsState? runtimeDiagnosticsState =
            observabilitySnapshot?.Enabled == true
                ? source.RuntimeDiagnosticsState?.CreateForOptions(observabilitySnapshot)
                  ?? new CSharpDbRuntimeDiagnosticsState(observabilitySnapshot, timeProvider)
                : null;

        return new RuntimeDatabaseOptions(
            new DatabaseOptions
            {
                AdaptiveQueryReoptimization = source.AdaptiveQueryReoptimization,
                Functions = source.Functions,
                ImplicitInsertExecutionMode = source.ImplicitInsertExecutionMode,
                ObservabilityOptions = observabilitySnapshot,
                RuntimeDiagnosticsState = runtimeDiagnosticsState,
                StorageEngineFactory = source.StorageEngineFactory,
                StorageEngineOptions = source.StorageEngineOptions,
                WindowExecution = source.WindowExecution,
            },
            retainsRuntimeIdentity);
    }

    private async Task<Database> OpenOwnedDatabaseAsync(
        string databasePath,
        CancellationToken ct)
    {
        RuntimeDatabaseFamily runtimeFamily =
            Volatile.Read(ref _runtimeDatabaseFamily);
        DatabaseOptions databaseOptions = runtimeFamily.DatabaseOptions;
        // The injected two-argument factory is an internal fault/reentrancy
        // test seam and ignores databaseOptions. Normal composition consumes
        // this exact per-family state in the Database it constructs.
        Database database = await _openDatabaseAsync(databasePath, databaseOptions, ct);
        runtimeFamily.CompleteOpen();

        return database;
    }

    private void MarkRuntimeDiagnosticsCounterFamilyReset()
    {
        RuntimeDatabaseFamily currentFamily =
            Volatile.Read(ref _runtimeDatabaseFamily);
        CSharpDbRuntimeDiagnosticsState? current =
            currentFamily.RuntimeDiagnosticsState;
        CSharpDbObservabilityOptions? observability =
            _directDatabaseOptions.ObservabilityOptions;
        if (current is null || observability is null)
            return;

        CSharpDbRuntimeDiagnosticsState replacement =
            current.CreateForOptions(observability);
        DatabaseOptions replacementOptions = CreateDatabaseOptionsForOpen(replacement);
        Volatile.Write(
            ref _runtimeDatabaseFamily,
            new RuntimeDatabaseFamily(
                replacementOptions,
                advanceCounterEpochOnFirstSuccessfulOpen: true));
        RetireRuntimeDiagnosticsState(current);
    }

    private void RetainRuntimeDiagnosticsState(ClientTransactionSession session)
    {
        CSharpDbRuntimeDiagnosticsState? state =
            session.Database.RuntimeDiagnosticsState;
        if (state is null)
            return;

        lock (_runtimeDiagnosticsLifetimeGate)
            RetainRuntimeDiagnosticsStateLocked(state);
    }

    private void ReleaseRuntimeDiagnosticsState(ClientTransactionSession session)
    {
        if (!session.TryReleaseRuntimeDiagnosticsStateOwnership())
            return;

        CSharpDbRuntimeDiagnosticsState? state =
            session.Database.RuntimeDiagnosticsState;
        if (state is null)
            return;

        ReleaseRuntimeDiagnosticsStateOwnership(state);
    }

    private void RetainRuntimeDiagnosticsStateLocked(
        CSharpDbRuntimeDiagnosticsState state)
    {
        _runtimeDiagnosticsSessionOwners ??= [];
        _runtimeDiagnosticsSessionOwners.TryGetValue(state, out int ownerCount);
        _runtimeDiagnosticsSessionOwners[state] = ownerCount == int.MaxValue
            ? int.MaxValue
            : ownerCount + 1;
    }

    private void ReleaseRuntimeDiagnosticsStateOwnership(
        CSharpDbRuntimeDiagnosticsState state)
    {
        bool dispose = false;
        lock (_runtimeDiagnosticsLifetimeGate)
        {
            if (_runtimeDiagnosticsSessionOwners is not null &&
                _runtimeDiagnosticsSessionOwners.TryGetValue(state, out int ownerCount))
            {
                if (ownerCount == int.MaxValue)
                {
                    // Saturation is fail-safe: retain the state until process
                    // teardown rather than risking premature disposal.
                }
                else if (ownerCount > 1)
                {
                    _runtimeDiagnosticsSessionOwners[state] = ownerCount - 1;
                }
                else
                {
                    _runtimeDiagnosticsSessionOwners.Remove(state);
                    if (_runtimeDiagnosticsSessionOwners.Count == 0)
                        _runtimeDiagnosticsSessionOwners = null;
                    dispose = _retiredRuntimeDiagnosticsStates?.Remove(state) == true;
                    if (_retiredRuntimeDiagnosticsStates is { Count: 0 })
                        _retiredRuntimeDiagnosticsStates = null;
                }
            }
        }

        if (dispose)
            state.Dispose();
    }

    private void RetireRuntimeDiagnosticsState(
        CSharpDbRuntimeDiagnosticsState state)
    {
        bool dispose;
        lock (_runtimeDiagnosticsLifetimeGate)
        {
            dispose = _runtimeDiagnosticsSessionOwners?.ContainsKey(state) != true;
            if (!dispose)
                (_retiredRuntimeDiagnosticsStates ??= []).Add(state);
        }

        if (dispose)
            state.Dispose();
    }

    private void DisposeRuntimeDiagnosticsStates()
    {
        CSharpDbRuntimeDiagnosticsState? current =
            CurrentRuntimeDiagnosticsState;
        CSharpDbRuntimeDiagnosticsState[] states;
        lock (_runtimeDiagnosticsLifetimeGate)
        {
            var collected = new HashSet<CSharpDbRuntimeDiagnosticsState>(
                _retiredRuntimeDiagnosticsStates ?? []);
            if (current is not null)
                collected.Add(current);
            if (_disabledRuntimeDiagnosticsState is not null)
                collected.Add(_disabledRuntimeDiagnosticsState);
            var disposable = new List<CSharpDbRuntimeDiagnosticsState>(collected.Count);
            foreach (CSharpDbRuntimeDiagnosticsState state in collected)
            {
                if (_runtimeDiagnosticsSessionOwners?.ContainsKey(state) == true)
                    (_retiredRuntimeDiagnosticsStates ??= []).Add(state);
                else
                    disposable.Add(state);
            }
            states = disposable.ToArray();
            _disabledRuntimeDiagnosticsState = null;
        }

        foreach (CSharpDbRuntimeDiagnosticsState state in states)
            state.Dispose();
    }

    private DatabaseOptions CreateDatabaseOptionsForOpen(
        CSharpDbRuntimeDiagnosticsState runtimeDiagnosticsState)
        => new()
        {
            AdaptiveQueryReoptimization = _directDatabaseOptions.AdaptiveQueryReoptimization,
            Functions = _directDatabaseOptions.Functions,
            ImplicitInsertExecutionMode = _directDatabaseOptions.ImplicitInsertExecutionMode,
            ObservabilityOptions = runtimeDiagnosticsState.CreateOptionsSnapshot(),
            RuntimeDiagnosticsState = runtimeDiagnosticsState,
            StorageEngineFactory = _directDatabaseOptions.StorageEngineFactory,
            StorageEngineOptions = _directDatabaseOptions.StorageEngineOptions,
            WindowExecution = _directDatabaseOptions.WindowExecution,
        };

    private static string NormalizeDisplayDataSource(string dataSource)
    {
        if (dataSource.StartsWith(":memory:", StringComparison.OrdinalIgnoreCase))
            return dataSource;

        return Path.GetFullPath(dataSource);
    }

    private readonly record struct RuntimeDatabaseOptions(
        DatabaseOptions Value,
        bool AdvanceCounterEpochOnFirstSuccessfulOpen);

    private sealed class RuntimeDatabaseFamily
    {
        private int _advanceCounterEpochOnNextOpen;

        internal RuntimeDatabaseFamily(
            DatabaseOptions databaseOptions,
            bool advanceCounterEpochOnFirstSuccessfulOpen)
        {
            DatabaseOptions = databaseOptions;
            RuntimeDiagnosticsState = databaseOptions.RuntimeDiagnosticsState;
            _advanceCounterEpochOnNextOpen =
                advanceCounterEpochOnFirstSuccessfulOpen ? 1 : 0;
        }

        internal DatabaseOptions DatabaseOptions { get; }
        internal CSharpDbRuntimeDiagnosticsState? RuntimeDiagnosticsState { get; }

        internal void CompleteOpen()
        {
            CSharpDbRuntimeDiagnosticsState? state = RuntimeDiagnosticsState;
            if (state is null)
                return;

            bool replacesExistingFamily =
                Interlocked.Exchange(ref _advanceCounterEpochOnNextOpen, 0) != 0;
            state.CompleteCounterFamilyOpen(replacesExistingFamily);
        }
    }

    private async Task<Database> GetDatabaseAsync(CancellationToken ct)
    {
        using IDisposable? operationalBoundary =
            EnterDirectOperationalTransportScope();

        while (true)
        {
            Task<Database>? openTask = null;
            Task<Database>? createdOpenTask = null;
            Task? releaseTask = null;
            IDisposable? databaseGateBoundaryLifetime = null;

            try
            {
                lock (_databaseGate)
                {
                    if (_databaseReleaseCompletion is { Task.IsCompleted: false } releaseCompletion)
                    {
                        releaseTask = releaseCompletion.Task;
                    }
                    else
                    {
                        _databaseReleaseCompletion = null;

                        if (_databaseTask is null)
                        {
                            CSharpDbDeferredDiagnosticBoundary? openBoundary =
                                CreateDeferredOperationalTransportBoundary();
                            var openCompletion = new TaskCompletionSource<Database>(
                                TaskCreationOptions.RunContinuationsAsynchronously);
                            createdOpenTask = openCompletion.Task;
                            _databaseOwnershipEpoch++;
                            _databaseTask = createdOpenTask;
                            if (openBoundary is not null)
                            {
                                (_databaseOpenBoundaries ??= []).Add(openBoundary);
                                databaseGateBoundaryLifetime =
                                    openBoundary.TryAcquireLifetime();
                                if (_clientLockBoundaryStates is not null)
                                {
                                    foreach (ClientLockBoundaryState boundaryState in
                                             _clientLockBoundaryStates)
                                    {
                                        boundaryState.Attach(openBoundary);
                                    }
                                }
                            }

                            _ = OpenDatabaseCoreAsync(openCompletion, openBoundary);

                            async Task OpenDatabaseCoreAsync(
                                TaskCompletionSource<Database> completion,
                                CSharpDbDeferredDiagnosticBoundary? boundary)
                            {
                                IDisposable? boundaryEntry = boundary?.Enter();
                                try
                                {
                                    Database database = await OpenOwnedDatabaseAsync(
                                        _databasePath,
                                        CancellationToken.None);
                                    completion.TrySetResult(database);
                                }
                                catch (OperationCanceledException exception)
                                {
                                    completion.TrySetCanceled(exception.CancellationToken);
                                }
                                catch (Exception exception)
                                {
                                    completion.TrySetException(exception);
                                }
                                finally
                                {
                                    if (boundary is not null)
                                    {
                                        lock (_databaseGate)
                                        {
                                            if (_clientLockBoundaryStates is not null)
                                            {
                                                foreach (ClientLockBoundaryState boundaryState in
                                                         _clientLockBoundaryStates)
                                                {
                                                    boundaryState.Attach(boundary);
                                                }
                                            }
                                        }

                                        boundaryEntry?.Dispose();
                                        boundary.Dispose();
                                        _ = RemoveDatabaseOpenBoundaryAfterFlushAsync(
                                            boundary);
                                    }

                                    if (!completion.Task.IsCompletedSuccessfully)
                                    {
                                        lock (_databaseGate)
                                        {
                                            if (ReferenceEquals(
                                                    _databaseTask,
                                                    completion.Task))
                                            {
                                                _databaseTask = null;
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        openTask = createdOpenTask ?? _databaseTask;
                    }
                }
            }
            finally
            {
                databaseGateBoundaryLifetime?.Dispose();
            }

            if (releaseTask is not null)
            {
                await releaseTask.WaitAsync(ct);
                continue;
            }

            Database db = await openTask!.WaitAsync(ct);

            lock (_databaseGate)
            {
                if (ReferenceEquals(_databaseTask, openTask))
                    return db;

                if (_databaseReleaseCompletion is { Task.IsCompleted: false } completion)
                    releaseTask = completion.Task;
            }

            if (releaseTask is not null)
            {
                await releaseTask.WaitAsync(ct);
            }
        }
    }

    private async Task RemoveDatabaseOpenBoundaryAfterFlushAsync(
        CSharpDbDeferredDiagnosticBoundary boundary)
    {
        await boundary.FlushCompletion.ConfigureAwait(false);
        lock (_databaseGate)
        {
            _databaseOpenBoundaries?.Remove(boundary);
            if (_databaseOpenBoundaries is { Count: 0 })
                _databaseOpenBoundaries = null;
        }
    }

    public async ValueTask<Database?> TryGetDatabaseAsync(CancellationToken ct = default)
        => await GetDatabaseAsync(ct);

    public async ValueTask ReleaseCachedDatabaseAsync(CancellationToken ct = default)
    {
        using ClientLockLease clientLock = await AcquireClientLockAsync(ct);
        await ReleaseCachedDatabaseCoreAsync(
            ct,
            "Cannot release the direct database handle while snapshot readers are active.");
    }

    private async Task<ExclusiveDatabaseAccessLease> AcquireExclusiveDatabaseAccessAsync(
        CancellationToken ct,
        string activeReaderMessage)
    {
        ClientLockLease clientLock = await AcquireClientLockAsync(ct);

        Task<Database>? openTask;
        var releaseCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        lock (_databaseGate)
        {
            openTask = _databaseTask;
            _catalogsInitialized = false;
            _databaseTask = null;
            _databaseReleaseCompletion = releaseCompletion;
        }
        Volatile.Write(ref _exclusiveMaintenanceActive, 1);

        try
        {
            if (openTask is not null)
            {
                Database db;
                try
                {
                    db = await openTask.WaitAsync(ct);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    lock (_databaseGate)
                    {
                        if (_databaseTask is null)
                            _databaseTask = openTask;
                    }

                    throw;
                }
                catch
                {
                    return new ExclusiveDatabaseAccessLease(
                        this,
                        releaseCompletion,
                        clientLock);
                }

                if (db.ActiveReaderCount > 0)
                {
                    lock (_databaseGate)
                    {
                        if (_databaseTask is null)
                            _databaseTask = openTask;
                    }

                    throw new CSharpDbClientException(activeReaderMessage);
                }

                await db.DisposeAsync();
                MarkRuntimeDiagnosticsCounterFamilyReset();
            }

            return new ExclusiveDatabaseAccessLease(
                this,
                releaseCompletion,
                clientLock);
        }
        catch
        {
            CompleteExclusiveDatabaseAccess(releaseCompletion, ref clientLock);
            throw;
        }
    }

    private void CompleteExclusiveDatabaseAccess(
        TaskCompletionSource releaseCompletion,
        ref ClientLockLease clientLock)
    {
        Volatile.Write(ref _exclusiveMaintenanceActive, 0);
        lock (_databaseGate)
        {
            if (ReferenceEquals(_databaseReleaseCompletion, releaseCompletion))
                _databaseReleaseCompletion = null;
        }

        releaseCompletion.TrySetResult();
        clientLock.Dispose();
    }

    private struct ClientLockLease : IDisposable
    {
        private EngineTransportClient? _owner;
        private IDisposable? _transportScope;
        private ClientLockBoundaryState? _boundaryState;

        internal ClientLockLease(
            EngineTransportClient owner,
            IDisposable? transportScope,
            ClientLockBoundaryState? boundaryState)
        {
            _owner = owner;
            _transportScope = transportScope;
            _boundaryState = boundaryState;
        }

        internal void ReleaseLock()
        {
            EngineTransportClient? owner = _owner;
            if (owner is null)
                return;

            _owner = null;
            ClientLockBoundaryState? boundaryState = _boundaryState;
            _boundaryState = null;
            owner.ReleaseClientLock(boundaryState);
        }

        public void Dispose()
        {
            ReleaseLock();
            IDisposable? transportScope = _transportScope;
            _transportScope = null;
            transportScope?.Dispose();
        }
    }

    private sealed class DisposeFlushToken(
        EngineTransportClient owner,
        DisposeFlushToken? previous,
        Exception? failure)
    {
        private int _active = 1;

        internal EngineTransportClient Owner { get; } = owner;
        internal DisposeFlushToken? Previous { get; } = previous;
        internal Exception? Failure { get; } = failure;
        internal bool IsActive => Volatile.Read(ref _active) != 0;

        internal void Deactivate() => Volatile.Write(ref _active, 0);
    }

    private sealed class ClientLockBoundaryState
    {
        private List<(
            CSharpDbDeferredDiagnosticBoundary Boundary,
            IDisposable Lifetime)>? _lifetimes;

        internal void Attach(CSharpDbDeferredDiagnosticBoundary boundary)
        {
            if (_lifetimes?.Any(item => ReferenceEquals(item.Boundary, boundary)) == true)
                return;

            IDisposable? lifetime = boundary.TryAcquireLifetime();
            if (lifetime is not null)
                (_lifetimes ??= []).Add((boundary, lifetime));
        }

        internal void Release()
        {
            List<(
                CSharpDbDeferredDiagnosticBoundary Boundary,
                IDisposable Lifetime)>? lifetimes = _lifetimes;
            _lifetimes = null;
            if (lifetimes is null)
                return;

            foreach ((_, IDisposable lifetime) in lifetimes)
                lifetime.Dispose();
        }
    }

    private static long GetUtcNowTicksSafely(TimeProvider timeProvider, long fallbackTicks)
    {
        try
        {
            return timeProvider.GetUtcNow().UtcDateTime.Ticks;
        }
        catch
        {
            return fallbackTicks;
        }
    }

    private static long GetTimestampSafely(TimeProvider timeProvider, long fallbackTimestamp)
    {
        try
        {
            return timeProvider.GetTimestamp();
        }
        catch
        {
            return fallbackTimestamp;
        }
    }

    private static TimeSpan GetElapsedTimeSafely(
        TimeProvider timeProvider,
        long startingTimestamp,
        long endingTimestamp)
    {
        try
        {
            TimeSpan elapsed = timeProvider.GetElapsedTime(
                startingTimestamp,
                endingTimestamp);
            return elapsed < TimeSpan.Zero ? TimeSpan.Zero : elapsed;
        }
        catch
        {
            return TimeSpan.Zero;
        }
    }

    private sealed class DirectDiagnosticsSession
    {
        private readonly TimeProvider _timeProvider;
        private readonly long _createdAtUtcTicks;
        private long _lastActiveAtUtcTicks;

        internal DirectDiagnosticsSession(TimeProvider timeProvider)
        {
            _timeProvider = timeProvider;
            _createdAtUtcTicks = GetUtcNowTicksSafely(
                timeProvider,
                DateTimeOffset.UtcNow.UtcDateTime.Ticks);
            _lastActiveAtUtcTicks = _createdAtUtcTicks;
            SessionId = OpaqueDiagnosticsId.Create();
        }

        internal OpaqueDiagnosticsId SessionId { get; }
        internal DateTimeOffset CreatedAtUtc =>
            new(_createdAtUtcTicks, TimeSpan.Zero);
        internal DateTimeOffset LastActiveAtUtc =>
            new(Interlocked.Read(ref _lastActiveAtUtcTicks), TimeSpan.Zero);

        internal void Touch()
        {
            long current = Interlocked.Read(ref _lastActiveAtUtcTicks);
            long captured = GetUtcNowTicksSafely(_timeProvider, current);
            captured = Math.Max(_createdAtUtcTicks, captured);
            while (captured > current)
            {
                long observed = Interlocked.CompareExchange(
                    ref _lastActiveAtUtcTicks,
                    captured,
                    current);
                if (observed == current)
                    break;
                current = observed;
            }
        }
    }

    private sealed class ClientTransactionSession
    {
        private readonly SemaphoreSlim _operationGate = new(1, 1);
        private readonly TimeProvider? _diagnosticsTimeProvider;
        private readonly TimeSpan _diagnosticsAbandonmentThreshold;
        private readonly long _createdAtUtcTicks;
        private readonly long _createdTimestamp;
        private OpaqueDiagnosticsId? _diagnosticsSessionId;
        private OpaqueDiagnosticsId? _currentDiagnosticsOperationId;
        private long _lastActiveAtUtcTicks;
        private long _lastActiveTimestamp;
        private int _activeGateOperations;
        private int _activeDiagnosticsReaders;
        private int _reuseDisabled;
        private int _runtimeDiagnosticsStateOwnershipReleased;
        private int _state;

        public ClientTransactionSession(
            Database database,
            long reuseEpoch,
            TimeProvider? diagnosticsTimeProvider,
            TimeSpan diagnosticsAbandonmentThreshold)
        {
            Database = database;
            ReuseEpoch = reuseEpoch;
            _diagnosticsTimeProvider = diagnosticsTimeProvider;
            _diagnosticsAbandonmentThreshold = diagnosticsAbandonmentThreshold;
            if (diagnosticsTimeProvider is not null)
            {
                _createdAtUtcTicks = GetUtcNowTicksSafely(
                    diagnosticsTimeProvider,
                    DateTimeOffset.UtcNow.UtcDateTime.Ticks);
                _lastActiveAtUtcTicks = _createdAtUtcTicks;
                _createdTimestamp = GetTimestampSafely(
                    diagnosticsTimeProvider,
                    fallbackTimestamp: 0);
                _lastActiveTimestamp = _createdTimestamp;
            }
        }

        public Database Database { get; }
        public long ReuseEpoch { get; }
        public bool ReuseAllowed => Volatile.Read(ref _reuseDisabled) == 0;
        public bool CanPublishDiagnostics => _diagnosticsTimeProvider is not null;
        public bool HasActiveDiagnosticsReader =>
            Volatile.Read(ref _activeDiagnosticsReaders) > 0;

        public void DisableReuse() => Volatile.Write(ref _reuseDisabled, 1);

        public async ValueTask<bool> TryEnterOperationAsync(CancellationToken ct)
        {
            if (Volatile.Read(ref _state) != 0)
                return false;

            await _operationGate.WaitAsync(ct);
            if (Volatile.Read(ref _state) == 0)
            {
                // Publish ownership before consulting the clock so even a slow
                // or adversarial provider cannot make active work look idle.
                Volatile.Write(ref _activeGateOperations, 1);
                TouchDiagnosticsActivity();
                return true;
            }

            _operationGate.Release();
            return false;
        }

        public void ExitOperation()
        {
            // Refresh activity before publishing the idle transition. Snapshot
            // readers sample this marker before the timestamp.
            TouchDiagnosticsActivity();
            Volatile.Write(ref _activeGateOperations, 0);
            _operationGate.Release();
        }

        public void SetCurrentDiagnosticsOperation(OpaqueDiagnosticsId? operationId)
        {
            if (_diagnosticsTimeProvider is null || operationId is null)
                return;

            TouchDiagnosticsActivity();
            Volatile.Write(ref _currentDiagnosticsOperationId, operationId);
        }

        public void ClearCurrentDiagnosticsOperation(OpaqueDiagnosticsId? operationId)
        {
            if (operationId is null)
                return;

            // Move the activity watermark first. Otherwise a concurrent
            // snapshot can observe a cleared operation with an old idle time.
            TouchDiagnosticsActivity();
            Interlocked.CompareExchange(
                ref _currentDiagnosticsOperationId,
                value: null,
                comparand: operationId);
        }

        public void AddDiagnosticsReader()
        {
            while (true)
            {
                int current = Volatile.Read(ref _activeDiagnosticsReaders);
                if (current == int.MaxValue ||
                    Interlocked.CompareExchange(
                        ref _activeDiagnosticsReaders,
                        current + 1,
                        current) == current)
                {
                    return;
                }
            }
        }

        public void RemoveDiagnosticsReader()
        {
            while (true)
            {
                int current = Volatile.Read(ref _activeDiagnosticsReaders);
                if (current <= 0)
                    return;
                if (Interlocked.CompareExchange(
                        ref _activeDiagnosticsReaders,
                        current - 1,
                        current) == current)
                {
                    return;
                }
            }
        }

        public OpaqueDiagnosticsId? GetOrCreateDiagnosticsSessionId()
        {
            if (_diagnosticsTimeProvider is null)
                return null;

            OpaqueDiagnosticsId? sessionId = Volatile.Read(ref _diagnosticsSessionId);
            if (sessionId is not null)
                return sessionId;

            OpaqueDiagnosticsId created = OpaqueDiagnosticsId.Create();
            return Interlocked.CompareExchange(
                ref _diagnosticsSessionId,
                created,
                comparand: null) ?? created;
        }

        public SessionDiagnosticsSnapshot CreateDiagnosticsSnapshot(
            DiagnosticsSnapshotMetadata metadata,
            DateTimeOffset capturedAtUtc)
        {
            if (_diagnosticsTimeProvider is null)
            {
                throw new InvalidOperationException(
                    "Disabled transaction sessions do not publish diagnostics records.");
            }

            OpaqueDiagnosticsId sessionId = GetOrCreateDiagnosticsSessionId()
                ?? throw new InvalidOperationException(
                    "Enabled transaction diagnostics require a safe session identifier.");

            int lifecycleStateBefore = Volatile.Read(ref _state);
            bool hadActiveGateOperation =
                Volatile.Read(ref _activeGateOperations) != 0;
            OpaqueDiagnosticsId? currentOperationId =
                Volatile.Read(ref _currentDiagnosticsOperationId);
            bool hasActiveReader = Volatile.Read(ref _activeDiagnosticsReaders) > 0;
            long lastActiveAtUtcTicks = Interlocked.Read(ref _lastActiveAtUtcTicks);
            long lastActiveTimestamp = Interlocked.Read(ref _lastActiveTimestamp);
            bool hasActiveGateOperation = hadActiveGateOperation ||
                Volatile.Read(ref _activeGateOperations) != 0;
            currentOperationId ??= Volatile.Read(ref _currentDiagnosticsOperationId);
            hasActiveReader = hasActiveReader ||
                Volatile.Read(ref _activeDiagnosticsReaders) > 0;
            lastActiveAtUtcTicks = Math.Max(
                lastActiveAtUtcTicks,
                Interlocked.Read(ref _lastActiveAtUtcTicks));
            lastActiveTimestamp = Math.Max(
                lastActiveTimestamp,
                Interlocked.Read(ref _lastActiveTimestamp));
            int lifecycleStateAfter = Volatile.Read(ref _state);
            long capturedTimestamp = GetTimestampSafely(
                _diagnosticsTimeProvider,
                lastActiveTimestamp);
            TimeSpan idleDuration = GetElapsedTimeSafely(
                _diagnosticsTimeProvider,
                lastActiveTimestamp,
                capturedTimestamp);
            DateTimeOffset createdAtUtc = new(_createdAtUtcTicks, TimeSpan.Zero);
            DateTimeOffset lastActiveAtUtc = new(lastActiveAtUtcTicks, TimeSpan.Zero);
            bool isAbandoned = lifecycleStateBefore == 0 &&
                lifecycleStateAfter == 0 &&
                !hasActiveGateOperation &&
                currentOperationId is null &&
                !hasActiveReader &&
                idleDuration >= _diagnosticsAbandonmentThreshold;
            return new SessionDiagnosticsSnapshot(
                metadata,
                sessionId,
                createdAtUtc,
                lastActiveAtUtc,
                currentOperationId,
                hasActiveReader,
                HasActiveTransaction: true,
                ObservabilityTransport.Direct)
            {
                State = isAbandoned
                    ? DiagnosticsSessionState.Abandoned
                    : hasActiveReader
                        ? DiagnosticsSessionState.SnapshotReader
                        : DiagnosticsSessionState.Transaction,
            };
        }

        private void TouchDiagnosticsActivity()
        {
            TimeProvider? timeProvider = _diagnosticsTimeProvider;
            if (timeProvider is null)
                return;

            long current = Interlocked.Read(ref _lastActiveAtUtcTicks);
            long captured = GetUtcNowTicksSafely(timeProvider, current);
            captured = Math.Max(_createdAtUtcTicks, captured);
            while (captured > current)
            {
                long observed = Interlocked.CompareExchange(
                    ref _lastActiveAtUtcTicks,
                    captured,
                    current);
                if (observed == current)
                    break;
                current = observed;
            }

            current = Interlocked.Read(ref _lastActiveTimestamp);
            long capturedTimestamp = GetTimestampSafely(timeProvider, current);
            while (capturedTimestamp > current)
            {
                long observed = Interlocked.CompareExchange(
                    ref _lastActiveTimestamp,
                    capturedTimestamp,
                    current);
                if (observed == current)
                    break;
                current = observed;
            }
        }

        public TimeSpan GetDiagnosticsAge()
        {
            TimeProvider? timeProvider = _diagnosticsTimeProvider;
            if (timeProvider is null)
                return TimeSpan.Zero;

            long capturedTimestamp = GetTimestampSafely(
                timeProvider,
                _createdTimestamp);
            return GetElapsedTimeSafely(
                timeProvider,
                _createdTimestamp,
                capturedTimestamp);
        }

        public bool TryClaimFinalization()
            => Interlocked.CompareExchange(ref _state, 1, 0) == 0;

        public ValueTask WaitForOperationsAsync(CancellationToken ct)
            => new(_operationGate.WaitAsync(ct));

        public void CancelFinalizationClaim()
            => Volatile.Write(ref _state, 0);

        public void CompleteFinalization()
        {
            Volatile.Write(ref _state, 2);
            _operationGate.Release();
        }

        public bool TryReleaseRuntimeDiagnosticsStateOwnership()
            => Interlocked.Exchange(
                ref _runtimeDiagnosticsStateOwnershipReleased,
                1) == 0;
    }

    private sealed class ExclusiveDatabaseAccessLease : IAsyncDisposable
    {
        private readonly EngineTransportClient _owner;
        private readonly TaskCompletionSource _releaseCompletion;
        private ClientLockLease _clientLock;
        private int _disposed;

        public ExclusiveDatabaseAccessLease(
            EngineTransportClient owner,
            TaskCompletionSource releaseCompletion,
            ClientLockLease clientLock)
        {
            _owner = owner;
            _releaseCompletion = releaseCompletion;
            _clientLock = clientLock;
        }

        public ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                _owner.CompleteExclusiveDatabaseAccess(
                    _releaseCompletion,
                    ref _clientLock);
            }

            return ValueTask.CompletedTask;
        }
    }

    private static TableSchema MapTableSchema(CoreTableSchema schema)
        => new()
        {
            SchemaId = schema.SchemaId,
            TableName = schema.TableName,
            Columns = schema.Columns.Select(MapColumnDefinition).ToArray(),
            ForeignKeys = schema.ForeignKeys.Select(MapForeignKeyDefinition).ToArray(),
            CheckConstraints = schema.CheckConstraints.Select(MapCheckConstraintDefinition).ToArray(),
            KeyConstraints = schema.KeyConstraints.Select(MapKeyConstraintDefinition).ToArray(),
            NextRowId = schema.NextRowId,
        };

    private static CoreTableSchema MapCoreTableSchema(TableSchema schema)
        => new()
        {
            SchemaId = schema.SchemaId,
            TableName = schema.TableName,
            Columns = schema.Columns.Select(static column => new CoreColumnDefinition
            {
                SchemaId = column.SchemaId,
                Name = column.Name,
                Type = column.Type switch
                {
                    Models.DbType.Integer => CoreDbType.Integer,
                    Models.DbType.Real => CoreDbType.Real,
                    Models.DbType.Text => CoreDbType.Text,
                    Models.DbType.Blob => CoreDbType.Blob,
                    Models.DbType.Decimal => CoreDbType.Decimal,
                    _ => throw new CSharpDbClientException(
                        $"Unsupported column type '{column.Type}'."),
                },
                DeclaredType = column.DeclaredType is null
                    ? null
                    : MapCoreSqlTypeDescriptor(column.DeclaredType),
                Nullable = column.Nullable,
                IsPrimaryKey = column.IsPrimaryKey,
                IsIdentity = column.IsIdentity,
                IsRowVersion = column.IsRowVersion,
                Collation = column.Collation,
                DefaultSql = column.DefaultSql,
            }).ToArray(),
            ForeignKeys = schema.ForeignKeys.Select(static foreignKey =>
                new CoreForeignKeyDefinition
                {
                    SchemaId = foreignKey.SchemaId,
                    ColumnSchemaIds = foreignKey.ColumnSchemaIds.ToArray(),
                    ReferencedTableSchemaId = foreignKey.ReferencedTableSchemaId,
                    ReferencedColumnSchemaIds =
                        foreignKey.ReferencedColumnSchemaIds.ToArray(),
                    ReferencedKeySchemaId = foreignKey.ReferencedKeySchemaId,
                    ConstraintName = foreignKey.ConstraintName,
                    ColumnName = foreignKey.ColumnName,
                    ReferencedTableName = foreignKey.ReferencedTableName,
                    ReferencedColumnName = foreignKey.ReferencedColumnName,
                    ColumnNames = foreignKey.ColumnNames.Count > 0
                        ? foreignKey.ColumnNames.ToArray()
                        : [foreignKey.ColumnName],
                    ReferencedColumnNames = foreignKey.ReferencedColumnNames.Count > 0
                        ? foreignKey.ReferencedColumnNames.ToArray()
                        : [foreignKey.ReferencedColumnName],
                    OnDelete = MapForeignKeyActionToCore(foreignKey.OnDelete),
                    OnUpdate = MapForeignKeyActionToCore(foreignKey.OnUpdate),
                    SupportingIndexName = foreignKey.SupportingIndexName,
                }).ToArray(),
            CheckConstraints = schema.CheckConstraints.Select(static check =>
                new CoreCheckConstraintDefinition
                {
                    SchemaId = check.SchemaId,
                    ConstraintName = check.ConstraintName,
                    ExpressionSql = check.ExpressionSql,
                    ColumnName = check.ColumnName,
                }).ToArray(),
            KeyConstraints = schema.KeyConstraints.Select(static key =>
                new CoreKeyConstraintDefinition
                {
                    SchemaId = key.SchemaId,
                    ConstraintName = key.ConstraintName,
                    Kind = key.Kind switch
                    {
                        KeyConstraintKind.PrimaryKey =>
                            CoreKeyConstraintKind.PrimaryKey,
                        KeyConstraintKind.Unique =>
                            CoreKeyConstraintKind.Unique,
                        _ => throw new CSharpDbClientException(
                            $"Unsupported key constraint kind '{key.Kind}'."),
                    },
                    Columns = key.Columns.ToArray(),
                    BackingIndexName = key.BackingIndexName,
                }).ToArray(),
            NextRowId = schema.NextRowId,
        };

    private static CoreSqlTypeDescriptor MapCoreSqlTypeDescriptor(
        Models.SqlTypeDescriptor type) =>
        CoreSqlTypeDescriptor.Create(
            type.Kind switch
            {
                Models.SqlTypeKind.Boolean => CoreSqlTypeKind.Boolean,
                Models.SqlTypeKind.TinyInt => CoreSqlTypeKind.TinyInt,
                Models.SqlTypeKind.SmallInt => CoreSqlTypeKind.SmallInt,
                Models.SqlTypeKind.Integer => CoreSqlTypeKind.Integer,
                Models.SqlTypeKind.BigInt => CoreSqlTypeKind.BigInt,
                Models.SqlTypeKind.Real => CoreSqlTypeKind.Real,
                Models.SqlTypeKind.Double => CoreSqlTypeKind.Double,
                Models.SqlTypeKind.Decimal => CoreSqlTypeKind.Decimal,
                Models.SqlTypeKind.Char => CoreSqlTypeKind.Char,
                Models.SqlTypeKind.VarChar => CoreSqlTypeKind.VarChar,
                Models.SqlTypeKind.Text => CoreSqlTypeKind.Text,
                Models.SqlTypeKind.Binary => CoreSqlTypeKind.Binary,
                Models.SqlTypeKind.VarBinary => CoreSqlTypeKind.VarBinary,
                Models.SqlTypeKind.Blob => CoreSqlTypeKind.Blob,
                Models.SqlTypeKind.Uuid => CoreSqlTypeKind.Uuid,
                Models.SqlTypeKind.Date => CoreSqlTypeKind.Date,
                Models.SqlTypeKind.Time => CoreSqlTypeKind.Time,
                Models.SqlTypeKind.Timestamp => CoreSqlTypeKind.Timestamp,
                Models.SqlTypeKind.TimestampWithTimeZone => CoreSqlTypeKind.TimestampWithTimeZone,
                Models.SqlTypeKind.IntervalYearToMonth => CoreSqlTypeKind.IntervalYearToMonth,
                Models.SqlTypeKind.IntervalDayToSecond => CoreSqlTypeKind.IntervalDayToSecond,
                Models.SqlTypeKind.Json => CoreSqlTypeKind.Json,
                Models.SqlTypeKind.Xml => CoreSqlTypeKind.Xml,
                Models.SqlTypeKind.Bit => CoreSqlTypeKind.Bit,
                Models.SqlTypeKind.VarBit => CoreSqlTypeKind.VarBit,
                _ => throw new CSharpDbClientException($"Unsupported logical column type '{type.Kind}'."),
            },
            type.Length,
            type.Precision,
            type.Scale,
            type.FractionalSecondsPrecision);

    private static CheckConstraintDefinition MapCheckConstraintDefinition(CoreCheckConstraintDefinition check)
        => new()
        {
            SchemaId = check.SchemaId,
            ConstraintName = check.ConstraintName,
            ExpressionSql = check.ExpressionSql,
            ColumnName = check.ColumnName,
        };

    private static KeyConstraintDefinition MapKeyConstraintDefinition(CoreKeyConstraintDefinition key)
        => new()
        {
            SchemaId = key.SchemaId,
            ConstraintName = key.ConstraintName,
            Kind = key.Kind switch
            {
                CoreKeyConstraintKind.PrimaryKey => KeyConstraintKind.PrimaryKey,
                CoreKeyConstraintKind.Unique => KeyConstraintKind.Unique,
                _ => throw new CSharpDbClientException($"Unsupported key constraint kind '{key.Kind}'."),
            },
            Columns = key.Columns.ToArray(),
            BackingIndexName = key.BackingIndexName,
        };

    private static ForeignKeyDefinition MapForeignKeyDefinition(CoreForeignKeyDefinition foreignKey)
        => new()
        {
            SchemaId = foreignKey.SchemaId,
            ColumnSchemaIds = foreignKey.ColumnSchemaIds.ToArray(),
            ReferencedTableSchemaId = foreignKey.ReferencedTableSchemaId,
            ReferencedColumnSchemaIds = foreignKey.ReferencedColumnSchemaIds.ToArray(),
            ReferencedKeySchemaId = foreignKey.ReferencedKeySchemaId,
            ConstraintName = foreignKey.ConstraintName,
            ColumnName = foreignKey.ColumnName,
            ReferencedTableName = foreignKey.ReferencedTableName,
            ReferencedColumnName = foreignKey.ReferencedColumnName,
            ColumnNames = foreignKey.ColumnNames.Count > 0 ? foreignKey.ColumnNames.ToArray() : [foreignKey.ColumnName],
            ReferencedColumnNames = foreignKey.ReferencedColumnNames.Count > 0 ? foreignKey.ReferencedColumnNames.ToArray() : [foreignKey.ReferencedColumnName],
            OnDelete = MapForeignKeyActionToClient(foreignKey.OnDelete),
            OnUpdate = MapForeignKeyActionToClient(foreignKey.OnUpdate),
            SupportingIndexName = foreignKey.SupportingIndexName,
        };

    private static CoreForeignKeyOnDeleteAction MapForeignKeyActionToCore(
        ForeignKeyOnDeleteAction action) =>
        action switch
        {
            ForeignKeyOnDeleteAction.Restrict => CoreForeignKeyOnDeleteAction.Restrict,
            ForeignKeyOnDeleteAction.Cascade => CoreForeignKeyOnDeleteAction.Cascade,
            ForeignKeyOnDeleteAction.NoAction => CoreForeignKeyOnDeleteAction.NoAction,
            ForeignKeyOnDeleteAction.SetNull => CoreForeignKeyOnDeleteAction.SetNull,
            ForeignKeyOnDeleteAction.SetDefault => CoreForeignKeyOnDeleteAction.SetDefault,
            _ => throw new CSharpDbClientException(
                $"Unsupported foreign key referential action '{action}'."),
        };

    private static ForeignKeyOnDeleteAction MapForeignKeyActionToClient(
        CoreForeignKeyOnDeleteAction action) =>
        action switch
        {
            CoreForeignKeyOnDeleteAction.Restrict => ForeignKeyOnDeleteAction.Restrict,
            CoreForeignKeyOnDeleteAction.Cascade => ForeignKeyOnDeleteAction.Cascade,
            CoreForeignKeyOnDeleteAction.NoAction => ForeignKeyOnDeleteAction.NoAction,
            CoreForeignKeyOnDeleteAction.SetNull => ForeignKeyOnDeleteAction.SetNull,
            CoreForeignKeyOnDeleteAction.SetDefault => ForeignKeyOnDeleteAction.SetDefault,
            _ => throw new CSharpDbClientException(
                $"Unsupported foreign key referential action '{action}'."),
        };

    private static ColumnDefinition MapColumnDefinition(CoreColumnDefinition column)
        => new()
        {
            SchemaId = column.SchemaId,
            Name = column.Name,
            Type = column.Type switch
            {
                CoreDbType.Integer => Models.DbType.Integer,
                CoreDbType.Real => Models.DbType.Real,
                CoreDbType.Text => Models.DbType.Text,
                CoreDbType.Blob => Models.DbType.Blob,
                CoreDbType.Decimal => Models.DbType.Decimal,
                _ => throw new CSharpDbClientException($"Unsupported column type '{column.Type}'."),
            },
            DeclaredType = column.DeclaredType is null
                ? null
                : MapSqlTypeDescriptor(column.DeclaredType),
            Nullable = column.Nullable,
            IsPrimaryKey = column.IsPrimaryKey,
            IsIdentity = column.IsIdentity,
            IsRowVersion = column.IsRowVersion,
            Collation = column.Collation,
            DefaultSql = column.DefaultSql,
        };

    private static Models.SqlTypeDescriptor MapSqlTypeDescriptor(
        CoreSqlTypeDescriptor type) =>
        new()
        {
            Kind = type.Kind switch
            {
                CoreSqlTypeKind.Boolean => Models.SqlTypeKind.Boolean,
                CoreSqlTypeKind.TinyInt => Models.SqlTypeKind.TinyInt,
                CoreSqlTypeKind.SmallInt => Models.SqlTypeKind.SmallInt,
                CoreSqlTypeKind.Integer => Models.SqlTypeKind.Integer,
                CoreSqlTypeKind.BigInt => Models.SqlTypeKind.BigInt,
                CoreSqlTypeKind.Real => Models.SqlTypeKind.Real,
                CoreSqlTypeKind.Double => Models.SqlTypeKind.Double,
                CoreSqlTypeKind.Decimal => Models.SqlTypeKind.Decimal,
                CoreSqlTypeKind.Char => Models.SqlTypeKind.Char,
                CoreSqlTypeKind.VarChar => Models.SqlTypeKind.VarChar,
                CoreSqlTypeKind.Text => Models.SqlTypeKind.Text,
                CoreSqlTypeKind.Binary => Models.SqlTypeKind.Binary,
                CoreSqlTypeKind.VarBinary => Models.SqlTypeKind.VarBinary,
                CoreSqlTypeKind.Blob => Models.SqlTypeKind.Blob,
                CoreSqlTypeKind.Uuid => Models.SqlTypeKind.Uuid,
                CoreSqlTypeKind.Date => Models.SqlTypeKind.Date,
                CoreSqlTypeKind.Time => Models.SqlTypeKind.Time,
                CoreSqlTypeKind.Timestamp => Models.SqlTypeKind.Timestamp,
                CoreSqlTypeKind.TimestampWithTimeZone => Models.SqlTypeKind.TimestampWithTimeZone,
                CoreSqlTypeKind.IntervalYearToMonth => Models.SqlTypeKind.IntervalYearToMonth,
                CoreSqlTypeKind.IntervalDayToSecond => Models.SqlTypeKind.IntervalDayToSecond,
                CoreSqlTypeKind.Json => Models.SqlTypeKind.Json,
                CoreSqlTypeKind.Xml => Models.SqlTypeKind.Xml,
                CoreSqlTypeKind.Bit => Models.SqlTypeKind.Bit,
                CoreSqlTypeKind.VarBit => Models.SqlTypeKind.VarBit,
                _ => throw new CSharpDbClientException($"Unsupported logical column type '{type.Kind}'."),
            },
            Length = type.Length,
            Precision = type.Precision,
            Scale = type.Scale,
            FractionalSecondsPrecision = type.FractionalSecondsPrecision,
        };

    private static IndexSchema MapIndexSchema(CoreIndexSchema index)
        => new()
        {
            IndexName = index.IndexName,
            TableName = index.TableName,
            Columns = index.Columns.ToArray(),
            ColumnCollations = index.ColumnCollations.ToArray(),
            IsUnique = index.IsUnique,
        };

    private static TriggerSchema MapTriggerSchema(CoreTriggerSchema trigger)
        => new()
        {
            TriggerName = trigger.TriggerName,
            TableName = trigger.TableName,
            Timing = trigger.Timing == CoreTriggerTiming.Before ? TriggerTiming.Before : TriggerTiming.After,
            Event = trigger.Event switch
            {
                CoreTriggerEvent.Insert => TriggerEvent.Insert,
                CoreTriggerEvent.Update => TriggerEvent.Update,
                CoreTriggerEvent.Delete => TriggerEvent.Delete,
                _ => throw new CSharpDbClientException($"Unsupported trigger event '{trigger.Event}'."),
            },
            BodySql = trigger.BodySql,
        };

    private static CSharpDB.Client.Models.DatabaseMaintenanceReport MapMaintenanceReport(CSharpDB.Engine.DatabaseMaintenanceReport report)
        => new()
        {
            SchemaVersion = report.SchemaVersion,
            DatabasePath = report.DatabasePath,
            SpaceUsage = new SpaceUsageReport
            {
                DatabaseFileBytes = report.SpaceUsage.DatabaseFileBytes,
                WalFileBytes = report.SpaceUsage.WalFileBytes,
                PageSizeBytes = report.SpaceUsage.PageSizeBytes,
                PhysicalPageCount = report.SpaceUsage.PhysicalPageCount,
                DeclaredPageCount = report.SpaceUsage.DeclaredPageCount,
                FreelistPageCount = report.SpaceUsage.FreelistPageCount,
                FreelistBytes = report.SpaceUsage.FreelistBytes,
            },
            Fragmentation = new FragmentationReport
            {
                BTreeFreeBytes = report.Fragmentation.BTreeFreeBytes,
                PagesWithFreeSpace = report.Fragmentation.PagesWithFreeSpace,
                TailFreelistPageCount = report.Fragmentation.TailFreelistPageCount,
                TailFreelistBytes = report.Fragmentation.TailFreelistBytes,
            },
            PageTypeHistogram = new Dictionary<string, int>(report.PageTypeHistogram, StringComparer.OrdinalIgnoreCase),
        };

    private static DatabaseForeignKeyMigrationRequest MapForeignKeyMigrationRequest(ForeignKeyMigrationRequest request)
        => new()
        {
            ValidateOnly = request.ValidateOnly,
            BackupDestinationPath = request.BackupDestinationPath,
            ViolationSampleLimit = request.ViolationSampleLimit,
            Constraints = request.Constraints.Select(spec => new DatabaseForeignKeyMigrationConstraintSpec
            {
                TableName = spec.TableName,
                ColumnName = spec.ColumnName,
                ReferencedTableName = spec.ReferencedTableName,
                ReferencedColumnName = spec.ReferencedColumnName,
                OnDelete = MapForeignKeyActionToCore(spec.OnDelete),
                OnUpdate = MapForeignKeyActionToCore(spec.OnUpdate),
            }).ToArray(),
        };

    private static ForeignKeyMigrationResult MapForeignKeyMigrationResult(DatabaseForeignKeyMigrationResult result)
        => new()
        {
            ValidateOnly = result.ValidateOnly,
            Succeeded = result.Succeeded,
            BackupDestinationPath = result.BackupDestinationPath,
            AffectedTables = result.AffectedTables,
            AppliedForeignKeys = result.AppliedForeignKeys,
            CopiedRows = result.CopiedRows,
            ViolationCount = result.ViolationCount,
            Violations = result.Violations.Select(static violation => new ForeignKeyMigrationViolation
            {
                TableName = violation.TableName,
                ColumnName = violation.ColumnName,
                ReferencedTableName = violation.ReferencedTableName,
                ReferencedColumnName = violation.ReferencedColumnName,
                ChildKeyColumnName = violation.ChildKeyColumnName,
                ChildKeyValue = ToObject(violation.ChildKeyValue),
                ChildValue = ToObject(violation.ChildValue),
                Reason = violation.Reason,
            }).ToArray(),
            AppliedConstraints = result.AppliedConstraints.Select(static constraint => new ForeignKeyMigrationAppliedConstraint
            {
                TableName = constraint.TableName,
                ColumnName = constraint.ColumnName,
                ReferencedTableName = constraint.ReferencedTableName,
                ReferencedColumnName = constraint.ReferencedColumnName,
                ConstraintName = constraint.ConstraintName,
                SupportingIndexName = constraint.SupportingIndexName,
                OnDelete = MapForeignKeyActionToClient(constraint.OnDelete),
                OnUpdate = MapForeignKeyActionToClient(constraint.OnUpdate),
            }).ToArray(),
        };

    private static DatabaseReindexRequest MapReindexRequest(ReindexRequest request)
        => new()
        {
            Scope = request.Scope switch
            {
                ReindexScope.All => DatabaseReindexScope.All,
                ReindexScope.Table => DatabaseReindexScope.Table,
                ReindexScope.Index => DatabaseReindexScope.Index,
                _ => throw new ArgumentOutOfRangeException(nameof(request.Scope), request.Scope, null),
            },
            Name = request.Name,
            AllowCorruptIndexRecovery = request.AllowCorruptIndexRecovery,
        };

    private static ReindexResult MapReindexResult(DatabaseReindexResult result)
        => new()
        {
            Scope = result.Scope switch
            {
                DatabaseReindexScope.All => ReindexScope.All,
                DatabaseReindexScope.Table => ReindexScope.Table,
                DatabaseReindexScope.Index => ReindexScope.Index,
                _ => throw new ArgumentOutOfRangeException(nameof(result.Scope), result.Scope, null),
            },
            Name = result.Name,
            RebuiltIndexCount = result.RebuiltIndexCount,
            RecoveredCorruptIndexCount = result.RecoveredCorruptIndexCount,
        };

    private static VacuumResult MapVacuumResult(DatabaseVacuumResult result)
        => new()
        {
            DatabaseFileBytesBefore = result.DatabaseFileBytesBefore,
            DatabaseFileBytesAfter = result.DatabaseFileBytesAfter,
            PhysicalPageCountBefore = result.PhysicalPageCountBefore,
            PhysicalPageCountAfter = result.PhysicalPageCountAfter,
        };

    private async Task<SqlExecutionResult> ExecuteQueryAsync(
        Database db,
        string sql,
        CancellationToken ct,
        string? observabilitySql = null)
    {
        using IDisposable? transportScope = EnterDirectTransportScope();
        await using var result = await db.ExecuteAsync(sql, observabilitySql ?? sql, ct);
        var rows = await result.ToListAsync(ct);
        return new SqlExecutionResult
        {
            IsQuery = result.IsQuery,
            ColumnNames = result.IsQuery ? result.Schema.Select(column => column.Name).ToArray() : null,
            Columns = result.IsQuery && result.Schema.All(column => column.Type is not CoreDbType.Null)
                ? result.Schema.Select(MapColumnDefinition).ToArray()
                : null,
            ColumnTypes = result.IsQuery
                ? result.Schema.Select(column =>
                    column.IsRowVersion
                        ? "ROWVERSION"
                        : column.Type is CoreDbType.Null && column.DeclaredType is null
                            ? "NULL"
                            : column.EffectiveType.ToSql()).ToArray()
                : null,
            ColumnNullability = result.IsQuery ? result.Schema.Select(column => column.Nullable).ToArray() : null,
            Rows = result.IsQuery
                ? rows.Select(row => ToObjects(result.Schema, row)).ToList()
                : null,
            RowsAffected = result.IsQuery ? rows.Count : result.RowsAffected,
        };
    }

    private async Task ExecuteStatementAsync(Database db, string sql, CancellationToken ct)
    {
        using IDisposable? transportScope = EnterDirectTransportScope();
        await using var result = await db.ExecuteAsync(sql, ct);
        if (result.IsQuery)
            await result.ToListAsync(ct);
    }

    private async Task<int> ExecuteNonQueryAsync(Database db, string sql, CancellationToken ct)
    {
        using IDisposable? transportScope = EnterDirectTransportScope();
        await using var result = await db.ExecuteAsync(sql, ct);
        if (result.IsQuery)
            await result.ToListAsync(ct);
        return result.RowsAffected;
    }

    private async Task ExecuteInSingleTransactionAsync(Database db, CancellationToken ct, params string[] statements)
    {
        await db.BeginTransactionAsync(ct);
        try
        {
            foreach (string statement in statements)
                await ExecuteStatementAsync(db, statement, ct);

            await db.CommitAsync(ct);
        }
        catch
        {
            await db.RollbackAsync(ct);
            throw;
        }
    }

    private ValueTask<ClientLockLease> AcquireClientLockAsync(CancellationToken ct)
    {
        IDisposable? transportScope = EnterDirectTransportScope();
        ClientLockBoundaryState? boundaryState =
            RegisterPendingClientLockBoundaryState();
        try
        {
            Task waitTask = _lock.WaitAsync(ct);
            if (waitTask.IsCompletedSuccessfully)
            {
                return new ValueTask<ClientLockLease>(
                    new ClientLockLease(this, transportScope, boundaryState));
            }

            return AwaitLockAsync(
                this,
                waitTask,
                transportScope,
                boundaryState);
        }
        catch
        {
            UnregisterClientLockBoundaryState(boundaryState);
            transportScope?.Dispose();
            throw;
        }

        static async ValueTask<ClientLockLease> AwaitLockAsync(
            EngineTransportClient owner,
            Task waitTask,
            IDisposable? transportScope,
            ClientLockBoundaryState? boundaryState)
        {
            try
            {
                await waitTask;
                return new ClientLockLease(owner, transportScope, boundaryState);
            }
            catch
            {
                owner.UnregisterClientLockBoundaryState(boundaryState);
                transportScope?.Dispose();
                throw;
            }
        }
    }

    private ClientLockBoundaryState? RegisterPendingClientLockBoundaryState()
    {
        if (!_operationalEventsEnabled ||
            CSharpDbOperationScope.IsDiagnosticsSuppressed)
        {
            return null;
        }

        var state = new ClientLockBoundaryState();
        lock (_databaseGate)
        {
            if (_databaseOpenBoundaries is not null)
            {
                foreach (CSharpDbDeferredDiagnosticBoundary openBoundary in
                         _databaseOpenBoundaries)
                {
                    state.Attach(openBoundary);
                }
            }

            (_clientLockBoundaryStates ??= []).Add(state);
        }

        return state;
    }

    private void UnregisterClientLockBoundaryState(
        ClientLockBoundaryState? boundaryState)
    {
        if (boundaryState is null)
            return;

        lock (_databaseGate)
        {
            _clientLockBoundaryStates?.Remove(boundaryState);
            if (_clientLockBoundaryStates is { Count: 0 })
                _clientLockBoundaryStates = null;
        }

        boundaryState.Release();
    }

    private void ReleaseClientLock(ClientLockBoundaryState? boundaryState)
    {
        if (boundaryState is not null)
        {
            lock (_databaseGate)
            {
                _clientLockBoundaryStates?.Remove(boundaryState);
                if (_clientLockBoundaryStates is { Count: 0 })
                    _clientLockBoundaryStates = null;
            }
        }

        _lock.Release();
        boundaryState?.Release();
    }

    private IDisposable? EnterDirectTransportScope()
        => EnterDirectTransportScope(includeQueryEvents: true);

    private IDisposable? EnterDirectTransportScopeForSession(
        OpaqueDiagnosticsId? diagnosticsSessionId)
        => EnterDirectTransportScope(
            includeQueryEvents: true,
            diagnosticsSessionId);

    private IDisposable? EnterDirectOperationalTransportScope()
        => EnterDirectTransportScope(includeQueryEvents: false);

    private CSharpDbDeferredDiagnosticBoundary? CreateDeferredTransportBoundary()
        => CreateDeferredTransportBoundary(includeQueryEvents: true);

    private CSharpDbDeferredDiagnosticBoundary? CreateDeferredTransportBoundaryForSession(
        OpaqueDiagnosticsId? diagnosticsSessionId)
        => CreateDeferredTransportBoundary(
            includeQueryEvents: true,
            diagnosticsSessionId);

    private CSharpDbDeferredDiagnosticBoundary?
        CreateDeferredOperationalTransportBoundary()
        => CreateDeferredTransportBoundary(includeQueryEvents: false);

    private CSharpDbDeferredDiagnosticBoundary? CreateDeferredTransportBoundary(
        bool includeQueryEvents,
        OpaqueDiagnosticsId? diagnosticsSessionId = null)
    {
        if (!_observabilityEnabled ||
            CSharpDbOperationScope.IsDiagnosticsSuppressed ||
            (!_operationalEventsEnabled &&
             (!includeQueryEvents ||
              (!_queryEventsEnabled && !_slowQueryEventsEnabled))))
        {
            return null;
        }

        ObservabilityTransport transport = CSharpDbOperationScope.CurrentTransport;
        OpaqueDiagnosticsId? sessionId = CSharpDbOperationScope.CurrentSessionId;
        if (transport == ObservabilityTransport.Embedded)
        {
            transport = ObservabilityTransport.Direct;
            sessionId = diagnosticsSessionId ?? GetOrCreateDiagnosticsSessionId();
        }

        return CSharpDbOperationScope.CreateDeferredBoundary(transport, sessionId);
    }

    private IDisposable? EnterDirectTransportScope(
        bool includeQueryEvents,
        OpaqueDiagnosticsId? diagnosticsSessionId = null)
    {
        if (!_observabilityEnabled ||
            CSharpDbOperationScope.IsDiagnosticsSuppressed)
        {
            return null;
        }

        if (!_operationalEventsEnabled &&
            (!includeQueryEvents ||
             (!_queryEventsEnabled && !_slowQueryEventsEnabled)))
        {
            return null;
        }

        if (CSharpDbOperationScope.CurrentDiagnosticEventBuffer is not null)
            return null;

        ObservabilityTransport transport = CSharpDbOperationScope.CurrentTransport;
        OpaqueDiagnosticsId? sessionId = CSharpDbOperationScope.CurrentSessionId;
        if (transport == ObservabilityTransport.Embedded)
        {
            transport = ObservabilityTransport.Direct;
            sessionId = diagnosticsSessionId ?? GetOrCreateDiagnosticsSessionId();
        }

        return CSharpDbOperationScope.EnterBoundary(
            transport,
            sessionId);
    }

    private OpaqueDiagnosticsId GetOrCreateDiagnosticsSessionId()
    {
        DirectDiagnosticsSession session = GetOrCreateDirectDiagnosticsSession();
        session.Touch();
        return session.SessionId;
    }

    private OpaqueDiagnosticsId? TryGetTransactionDiagnosticsSessionId(
        string transactionId)
        => _observabilityEnabled &&
           _transactions.TryGetValue(transactionId, out ClientTransactionSession? session)
            ? session.GetOrCreateDiagnosticsSessionId()
            : null;

    private DirectDiagnosticsSession GetOrCreateDirectDiagnosticsSession()
    {
        DirectDiagnosticsSession? session = Volatile.Read(ref _diagnosticsSession);
        if (session is not null)
            return session;

        var created = new DirectDiagnosticsSession(_observabilityTimeProvider);
        return Interlocked.CompareExchange(
            ref _diagnosticsSession,
            created,
            comparand: null) ?? created;
    }

    private void CaptureQueryObservationInterest(
        out bool queryEventsObserved,
        out bool slowQueryEventsObserved,
        out bool longRunningQueryEventsObserved)
    {
        CSharpDbDiagnosticEventPublisher publisher = CSharpDbDiagnostics.EventPublisher;
        queryEventsObserved = _queryEventsEnabled &&
            (publisher.IsEnabled(CSharpDbLogEvents.QueryCompleted) ||
             publisher.IsEnabled(CSharpDbLogEvents.QueryFailed) ||
             publisher.IsEnabled(CSharpDbLogEvents.QueryCanceled));
        slowQueryEventsObserved = _slowQueryEventsEnabled &&
            publisher.IsEnabled(CSharpDbLogEvents.SlowQuery);
        longRunningQueryEventsObserved = _slowQueryEventsEnabled &&
            publisher.IsEnabled(CSharpDbLogEvents.LongRunningQuery);
    }

    private static TimeSpan GetConfiguredSlowQueryThreshold(
        CSharpDbLoggingOptions? logging,
        CSharpDbOperationClass operationClass)
    {
        TimeSpan threshold = logging?.SlowQueryThresholdOverrides?.TryGetValue(
            operationClass,
            out TimeSpan configured) == true
                ? configured
                : logging?.SlowQueryThreshold ?? TimeSpan.FromMilliseconds(500);
        return threshold > TimeSpan.Zero &&
               threshold <= CSharpDbObservabilityOptions.MaximumThreshold
            ? threshold
            : TimeSpan.FromMilliseconds(500);
    }

    private static ViewBrowseResult PageViewResult(ViewBrowseResult result, int page, int pageSize)
    {
        int normalizedPage = NormalizePage(page);
        int normalizedPageSize = NormalizePageSize(pageSize);
        int skip = (normalizedPage - 1) * normalizedPageSize;

        return new ViewBrowseResult
        {
            ViewName = result.ViewName,
            ColumnNames = result.ColumnNames,
            ColumnTypes = result.ColumnTypes,
            Rows = result.Rows.Skip(skip).Take(normalizedPageSize).ToList(),
            TotalRows = result.Rows.Count,
            Page = normalizedPage,
            PageSize = normalizedPageSize,
        };
    }

    private async Task<TableBrowseResult> BrowseTablePageAsync(Database db, string tableName, int page, int pageSize, CancellationToken ct)
    {
        string normalizedTableName =
            RequireCatalogIdentifier(tableName, nameof(tableName));
        var schema = db.GetTableSchema(normalizedTableName);
        if (schema is null || IsInternalTable(normalizedTableName))
            throw new CSharpDbClientException($"Table '{normalizedTableName}' was not found.");

        int normalizedPage = NormalizePage(page);
        int normalizedPageSize = NormalizePageSize(pageSize);
        long skip = (long)(normalizedPage - 1) * normalizedPageSize;
        int totalRows = await CountRowsViaScalarAsync(db, normalizedTableName, ct);
        var query = await ExecuteQueryAsync(
            db,
            $"SELECT * FROM {CoreSqlIdentifierRules.Quote(normalizedTableName)} LIMIT {normalizedPageSize} OFFSET {skip}",
            ct);

        return new TableBrowseResult
        {
            TableName = normalizedTableName,
            Schema = MapTableSchema(schema),
            Rows = query.Rows ?? [],
            TotalRows = totalRows,
            Page = normalizedPage,
            PageSize = normalizedPageSize,
        };
    }

    private static Dictionary<string, object?> ToRowDictionary(CoreColumnDefinition[] schema, CSharpDB.Primitives.DbValue[] row)
    {
        var values = new Dictionary<string, object?>(schema.Length, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < schema.Length && i < row.Length; i++)
            values[schema[i].Name] = ToObject(row[i], schema[i]);
        return values;
    }

    private static object?[] ToObjects(
        CoreColumnDefinition[] schema,
        CSharpDB.Primitives.DbValue[] row)
    {
        var values = new object?[row.Length];
        for (int i = 0; i < row.Length; i++)
            values[i] = ToObject(row[i], i < schema.Length ? schema[i] : null);
        return values;
    }

    private static object? ToObject(
        CSharpDB.Primitives.DbValue value,
        CoreColumnDefinition? column = null) => value.Type switch
    {
        CoreDbType.Null => null,
        CoreDbType.Integer when column?.DeclaredType?.Kind == CoreSqlTypeKind.Boolean =>
            value.AsInteger != 0,
        CoreDbType.Integer => value.AsInteger,
        CoreDbType.Real => value.AsReal,
        CoreDbType.Decimal => value.AsDecimal,
        CoreDbType.Text => value.AsText,
        CoreDbType.Blob when value.IsBitString =>
            new SqlBitString(value.AsBlob, value.BitLength),
        CoreDbType.Blob => value.AsBlob,
        _ => throw new CSharpDbClientException($"Unsupported DbValue type '{value.Type}'."),
    };

    private static bool ValuesEqual(CSharpDB.Primitives.DbValue value, object candidate)
    {
        object? normalized = NormalizeValue(candidate);
        if (normalized is null)
            return value.Type == CoreDbType.Null;

        return value.Type switch
        {
            CoreDbType.Integer when normalized is long integer => value.AsInteger == integer,
            CoreDbType.Real when normalized is double real => Math.Abs(value.AsReal - real) < double.Epsilon,
            CoreDbType.Decimal when normalized is decimal number => value.AsDecimal == number,
            CoreDbType.Text when normalized is string text => string.Equals(value.AsText, text, StringComparison.Ordinal),
            CoreDbType.Blob when normalized is SqlBitString bits =>
                value.IsBitString &&
                value.BitLength == bits.BitLength &&
                value.AsBlob.AsSpan().SequenceEqual(bits.PackedBytes.Span),
            CoreDbType.Blob when normalized is byte[] blob => value.AsBlob.AsSpan().SequenceEqual(blob),
            CoreDbType.Integer when normalized is double real => Math.Abs(value.AsReal - real) < double.Epsilon,
            CoreDbType.Real when normalized is long integer => Math.Abs(value.AsReal - integer) < double.Epsilon,
            CoreDbType.Decimal when normalized is long integer => value.AsDecimal == integer,
            CoreDbType.Decimal when normalized is double real => value.AsReal == real,
            _ => false,
        };
    }

    private static object? NormalizeValue(object? value) => value switch
    {
        null => null,
        JsonElement json => NormalizeJsonElement(json),
        bool boolean => boolean ? 1L : 0L,
        byte or sbyte or short or ushort or int or uint or long => Convert.ToInt64(value, CultureInfo.InvariantCulture),
        decimal number => number,
        float or double => Convert.ToDouble(value, CultureInfo.InvariantCulture),
        Guid guid => CoreTextCodec.FormatGuid(guid),
        DateOnly date => CoreTextCodec.FormatDate(date),
        TimeOnly time => CoreTextCodec.FormatTime(time),
        DateTime dateTime => CoreTextCodec.FormatDateTime(dateTime),
        DateTimeOffset dateTimeOffset => CoreTextCodec.FormatDateTimeOffset(dateTimeOffset),
        SqlBitString bits => bits,
        string text => text,
        byte[] blob => blob,
        _ => Convert.ToString(value, CultureInfo.InvariantCulture),
    };

    private static object? NormalizeJsonElement(JsonElement value) => value.ValueKind switch
    {
        JsonValueKind.Null => null,
        JsonValueKind.String => value.GetString(),
        JsonValueKind.False => 0L,
        JsonValueKind.True => 1L,
        JsonValueKind.Number when value.TryGetInt64(out long integer) => integer,
        JsonValueKind.Number => value.GetDouble(),
        _ => value.GetRawText(),
    };

    private static string FormatSqlLiteral(object? value)
    {
        object? normalized = NormalizeValue(value);
        return normalized switch
        {
            null => "NULL",
            long integer => integer.ToString(CultureInfo.InvariantCulture),
            decimal exact => exact.ToString(CultureInfo.InvariantCulture),
            double real => real.ToString(CultureInfo.InvariantCulture),
            string text => $"'{text.Replace("'", "''", StringComparison.Ordinal)}'",
            SqlBitString bits => $"'{bits.ToBitString()}'",
            byte[] blob => $"X'{Convert.ToHexString(blob)}'",
            _ => $"'{Convert.ToString(normalized, CultureInfo.InvariantCulture)?.Replace("'", "''", StringComparison.Ordinal) ?? string.Empty}'",
        };
    }

    private static string RequireIdentifier(string value, string paramName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value, paramName);
        if (!s_identifierPattern.IsMatch(value))
            throw new CSharpDbClientException($"Identifier '{value}' is not supported by the engine-only client.");
        return value;
    }

    private static string RequireCatalogIdentifier(
        string value,
        string paramName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value, paramName);
        if (value.Length > CoreSqlIdentifierRules.MaxLength ||
            value.IndexOf('\0') >= 0)
        {
            throw new CSharpDbClientException(
                $"Identifier '{value}' is not supported by the engine-only client.");
        }

        return value;
    }

    private static string QuoteCatalogIdentifier(
        string value,
        string paramName) =>
        CoreSqlIdentifierRules.Quote(
            RequireCatalogIdentifier(value, paramName));

    private static bool IsInternalTable(string tableName)
        => tableName.StartsWith(CollectionPrefix, StringComparison.Ordinal)
           || string.Equals(tableName, ProcedureTableName, StringComparison.OrdinalIgnoreCase)
           || string.Equals(tableName, SavedQueryTableName, StringComparison.OrdinalIgnoreCase)
           || string.Equals(tableName, ExternalTablesTableName, StringComparison.OrdinalIgnoreCase)
           || string.Equals(tableName, DataModelDiagramsTableName, StringComparison.OrdinalIgnoreCase);

    private static int NormalizePage(int page) => page < 1 ? 1 : page;

    private static int NormalizePageSize(int pageSize)
    {
        if (pageSize < 1)
            return 50;
        return Math.Min(pageSize, 1000);
    }

    private static string BuildColumnDefinitionSql(string columnName, Models.DbType type, bool notNull, string? collation)
    {
        var builder = new StringBuilder()
            .Append(QuoteCatalogIdentifier(columnName, nameof(columnName)))
            .Append(' ')
            .Append(MapDbType(type))
            .Append(BuildCollationClause(collation));
        if (notNull)
            builder.Append(" NOT NULL");

        return builder.ToString();
    }

    private static string BuildCreateIndexSql(string indexName, string tableName, string columnName, bool isUnique, string? collation)
    {
        string unique = isUnique ? "UNIQUE " : string.Empty;
        return $"CREATE {unique}INDEX {RequireIdentifier(indexName, nameof(indexName))} ON {RequireIdentifier(tableName, nameof(tableName))} ({RequireIdentifier(columnName, nameof(columnName))}{BuildCollationClause(collation)})";
    }

    private static string BuildCreateTriggerSql(string triggerName, string tableName, TriggerTiming timing, TriggerEvent triggerEvent, string bodySql)
    {
        string normalizedBody = NormalizeEmbeddedSql(bodySql);
        return $"CREATE TRIGGER {RequireIdentifier(triggerName, nameof(triggerName))} {timing.ToString().ToUpperInvariant()} {triggerEvent.ToString().ToUpperInvariant()} ON {RequireIdentifier(tableName, nameof(tableName))} BEGIN {normalizedBody}; END";
    }

    private static string NormalizeEmbeddedSql(string sql)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);
        return sql.Trim().TrimEnd(';');
    }

    private static string BuildCollationClause(string? collation)
        => string.IsNullOrWhiteSpace(collation)
            ? string.Empty
            : $" COLLATE {RequireIdentifier(collation, nameof(collation))}";

    private static string MapDbType(Models.DbType type) => type switch
    {
        Models.DbType.Integer => "INTEGER",
        Models.DbType.Real => "REAL",
        Models.DbType.Text => "TEXT",
        Models.DbType.Blob => "BLOB",
        Models.DbType.Decimal => "DECIMAL",
        _ => throw new CSharpDbClientException($"Unsupported DbType '{type}'."),
    };

    private ClientTransactionSession GetTransactionSession(string transactionId)
    {
        if (!_transactions.TryGetValue(transactionId, out ClientTransactionSession? session))
            throw TransactionNotFound(transactionId);
        return session;
    }

    private static CSharpDbClientException TransactionNotFound(string transactionId) =>
        new($"Transaction '{transactionId}' was not found.");

    private void ThrowIfDisposing()
    {
        if (_disposeStarted)
            throw new ObjectDisposedException(nameof(EngineTransportClient));
    }

    private void RegisterFinalization()
    {
        if (_activeFinalizations++ == 0)
            _finalizationsDrained = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private void UnregisterFinalization()
    {
        if (--_activeFinalizations == 0)
        {
            _finalizationsDrained?.TrySetResult();
            _finalizationsDrained = null;
        }
    }

    private Task? GetFinalizationsDrainedTask()
        => _activeFinalizations == 0 ? null : _finalizationsDrained!.Task;

    private async Task DisposeClaimedSessionAsync(ClientTransactionSession session)
    {
        try
        {
            await session.WaitForOperationsAsync(CancellationToken.None);
            await session.Database.DisposeAsync();
        }
        finally
        {
            ReleaseRuntimeDiagnosticsState(session);
            session.CompleteFinalization();
            using ClientLockLease clientLock =
                await AcquireClientLockAsync(CancellationToken.None);
            UnregisterFinalization();
        }
    }

    private async Task CompleteTransactionAsync(
        string transactionId,
        bool commit,
        CancellationToken ct)
    {
        ClientLockLease lifecycleBoundaryLock = await AcquireClientLockAsync(ct);
        ClientTransactionSession? session = null;
        bool adopted = false;
        bool operationGateAcquired = false;
        bool sessionResetSucceeded = false;
        try
        {
            ThrowIfDisposing();
            session = GetTransactionSession(transactionId);
            if (!session.TryClaimFinalization())
                throw TransactionNotFound(transactionId);

            if (!_transactions.TryRemove(transactionId, out ClientTransactionSession? removed) ||
                !ReferenceEquals(session, removed))
            {
                session.CancelFinalizationClaim();
                throw TransactionNotFound(transactionId);
            }

            RegisterFinalization();
            lifecycleBoundaryLock.ReleaseLock();

            try
            {
                await session.WaitForOperationsAsync(ct);
                operationGateAcquired = true;
            }
            catch
            {
                bool disposeClaimedSession;
                using (ClientLockLease recoveryLock =
                       await AcquireClientLockAsync(CancellationToken.None))
                {
                    session.CancelFinalizationClaim();
                    disposeClaimedSession = _disposeStarted;
                    if (!disposeClaimedSession)
                        _transactions.TryAdd(transactionId, session);
                }

                if (disposeClaimedSession)
                {
                    try
                    {
                        await session.Database.DisposeAsync();
                    }
                    finally
                    {
                        ReleaseRuntimeDiagnosticsState(session);
                        session.CompleteFinalization();
                        using ClientLockLease unregisterLock =
                            await AcquireClientLockAsync(CancellationToken.None);
                        UnregisterFinalization();
                    }
                }
                else
                {
                    using ClientLockLease unregisterLock =
                        await AcquireClientLockAsync(CancellationToken.None);
                    UnregisterFinalization();
                }

                session = null;
                throw;
            }

            Database database = session.Database;

            if (commit)
                await database.CommitAsync(ct);
            else
                await database.RollbackAsync(ct);

            if (_hybridDatabaseOptions is null)
            {
                await database.ResetReusableSessionStateAsync();
                sessionResetSucceeded = true;
            }
        }
        finally
        {
            if (session is not null && operationGateAcquired)
            {
                using (ClientLockLease adoptionLock =
                       await AcquireClientLockAsync(CancellationToken.None))
                {
                    if (!_disposeStarted && _hybridDatabaseOptions is null && sessionResetSucceeded)
                        adopted = TryAdoptCachedDatabase(session);
                }

                try
                {
                    if (!adopted)
                        await session.Database.DisposeAsync();
                }
                finally
                {
                    ReleaseRuntimeDiagnosticsState(session);
                    session.CompleteFinalization();
                    using ClientLockLease unregisterLock =
                        await AcquireClientLockAsync(CancellationToken.None);
                    UnregisterFinalization();
                }
            }

            lifecycleBoundaryLock.Dispose();
        }
    }

    private long CaptureDatabaseOwnershipEpoch()
    {
        lock (_databaseGate)
            return _databaseOwnershipEpoch;
    }

    private bool TryAdoptCachedDatabase(ClientTransactionSession session)
    {
        lock (_databaseGate)
        {
            if (!session.ReuseAllowed ||
                session.ReuseEpoch != _databaseOwnershipEpoch ||
                !ReferenceEquals(
                    session.Database.RuntimeDiagnosticsState,
                    CurrentRuntimeDiagnosticsState) ||
                !_transactions.IsEmpty ||
                _databaseTask is not null ||
                _databaseReleaseCompletion is { Task.IsCompleted: false })
            {
                return false;
            }

            _catalogsInitialized = false;
            _databaseReleaseCompletion = null;
            _databaseOwnershipEpoch++;
            _databaseTask = Task.FromResult(session.Database);
            return true;
        }
    }

    private async Task<Database?> DetachCachedDatabaseCoreAsync(
        CancellationToken ct,
        string activeReaderMessage)
    {
        Task<Database>? openTask;
        TaskCompletionSource releaseCompletion;
        lock (_databaseGate)
        {
            openTask = _databaseTask;
            _catalogsInitialized = false;
            if (openTask is null)
                return null;

            _databaseTask = null;
            releaseCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _databaseReleaseCompletion = releaseCompletion;
        }

        try
        {
            Database database;
            try
            {
                database = await openTask.WaitAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                lock (_databaseGate)
                {
                    if (_databaseTask is null)
                        _databaseTask = openTask;
                }

                throw;
            }
            catch
            {
                return null;
            }

            if (database.ActiveReaderCount > 0)
            {
                lock (_databaseGate)
                {
                    if (_databaseTask is null)
                        _databaseTask = openTask;
                }

                throw new CSharpDbClientException(activeReaderMessage);
            }

            return database;
        }
        finally
        {
            lock (_databaseGate)
            {
                if (ReferenceEquals(_databaseReleaseCompletion, releaseCompletion))
                    _databaseReleaseCompletion = null;
            }

            releaseCompletion.TrySetResult();
        }
    }

    private async Task ReleaseCachedDatabaseCoreAsync(CancellationToken ct, string activeReaderMessage)
    {
        Task<Database>? openTask;
        TaskCompletionSource releaseCompletion;
        lock (_databaseGate)
        {
            openTask = _databaseTask;
            _catalogsInitialized = false;
            if (openTask is null)
                return;

            _databaseTask = null;
            releaseCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _databaseReleaseCompletion = releaseCompletion;
        }

        Database db;
        try
        {
            try
            {
                db = await openTask.WaitAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                lock (_databaseGate)
                {
                    if (_databaseTask is null)
                        _databaseTask = openTask;
                }

                throw;
            }
            catch
            {
                return;
            }

            if (db.ActiveReaderCount > 0)
            {
                lock (_databaseGate)
                {
                    if (_databaseTask is null)
                        _databaseTask = openTask;
                }

                throw new CSharpDbClientException(activeReaderMessage);
            }

            await db.DisposeAsync();
            MarkRuntimeDiagnosticsCounterFamilyReset();
        }
        finally
        {
            lock (_databaseGate)
            {
                if (ReferenceEquals(_databaseReleaseCompletion, releaseCompletion))
                    _databaseReleaseCompletion = null;
            }

            releaseCompletion.TrySetResult();
        }
    }
}
