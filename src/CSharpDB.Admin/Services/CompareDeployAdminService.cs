using System.Diagnostics;
using CSharpDB.Admin.Models;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.DevOps;

namespace CSharpDB.Admin.Services;

public sealed class CompareDeployAdminService(ICSharpDbClient client)
{
    public Task<IReadOnlyList<string>> GetCurrentTableNamesAsync(CancellationToken ct = default)
        => GetTableNamesAsync(CompareDeployEndpointSpec.CurrentDatabase, ct);

    public Task<TableSchema?> GetCurrentTableSchemaAsync(string tableName, CancellationToken ct = default)
        => GetTableSchemaAsync(CompareDeployEndpointSpec.CurrentDatabase, tableName, ct);

    public async Task<IReadOnlyList<string>> GetTableNamesAsync(
        CompareDeployEndpointSpec endpoint,
        CancellationToken ct = default)
    {
        await using CompareDeployTargetHandle handle = await CreateTargetAsync(endpoint, tableNameOverride: null, ct);
        SchemaSnapshot snapshot = await handle.SchemaTarget.LoadSchemaAsync(ct);
        return snapshot.Tables
            .Select(static table => table.TableName)
            .Where(static name => !IsSystemTableName(name))
            .OrderBy(static name => name, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public async Task<TableSchema?> GetTableSchemaAsync(
        CompareDeployEndpointSpec endpoint,
        string tableName,
        CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(tableName))
            return null;

        string normalizedTableName = tableName.Trim();
        await using CompareDeployTargetHandle handle = await CreateTargetAsync(endpoint, normalizedTableName, ct);
        return await handle.DataTarget.GetTableSchemaAsync(normalizedTableName, ct);
    }

    public async Task<IReadOnlyList<CompareDeploySchemaObjectOption>> GetSchemaObjectsAsync(
        CompareDeployEndpointSpec endpoint,
        CancellationToken ct = default)
    {
        await using CompareDeployTargetHandle handle = await CreateTargetAsync(endpoint, tableNameOverride: null, ct);
        SchemaSnapshot snapshot = await handle.SchemaTarget.LoadSchemaAsync(ct);
        var userTables = snapshot.Tables
            .Where(table => !IsSystemTableName(table.TableName))
            .Select(table => table.TableName)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        return snapshot.Tables
            .Where(table => userTables.Contains(table.TableName))
            .Select(table => new CompareDeploySchemaObjectOption(SchemaObjectKind.Table, table.TableName))
            .Concat(snapshot.Views.Select(view => new CompareDeploySchemaObjectOption(SchemaObjectKind.View, view.Name)))
            .Concat(snapshot.Indexes
                .Where(index => userTables.Contains(index.TableName))
                .Select(index => new CompareDeploySchemaObjectOption(SchemaObjectKind.Index, index.IndexName, index.TableName)))
            .Concat(snapshot.Triggers
                .Where(trigger => userTables.Contains(trigger.TableName))
                .Select(trigger => new CompareDeploySchemaObjectOption(SchemaObjectKind.Trigger, trigger.TriggerName, trigger.TableName)))
            .Concat(snapshot.Procedures.Select(procedure => new CompareDeploySchemaObjectOption(SchemaObjectKind.Procedure, procedure.Name)))
            .OrderBy(option => option.Kind)
            .ThenBy(option => option.ParentName ?? string.Empty, StringComparer.OrdinalIgnoreCase)
            .ThenBy(option => option.Name, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public Task<CompareDeployRunResult<SchemaDiffReport>> CompareSchemaAsync(
        string sourcePath,
        string? tableName = null,
        CancellationToken ct = default)
        => CompareSchemaAsync(
            new CompareDeployEndpointSpec(CompareDeployEndpointKind.DatabaseFile, sourcePath),
            CompareDeployEndpointSpec.CurrentDatabase,
            tableName,
            ct);

    public async Task<CompareDeployRunResult<SchemaDiffReport>> CompareSchemaAsync(
        CompareDeployEndpointSpec source,
        CompareDeployEndpointSpec target,
        string? tableName = null,
        CancellationToken ct = default)
    {
        await using CompareDeployTargetHandle sourceHandle = await CreateTargetAsync(source, tableName, ct);
        await using CompareDeployTargetHandle targetHandle = await CreateTargetAsync(target, tableName, ct);
        ISchemaCompareTarget sourceTarget = sourceHandle.SchemaTarget;
        ISchemaCompareTarget targetTarget = targetHandle.SchemaTarget;

        if (!string.IsNullOrWhiteSpace(tableName))
        {
            sourceTarget = new FilteredSchemaCompareTarget(sourceTarget, tableName);
            targetTarget = new FilteredSchemaCompareTarget(targetTarget, tableName);
        }

        var stopwatch = Stopwatch.StartNew();
        SchemaDiffReport report = await new SchemaComparisonService().CompareAsync(sourceTarget, targetTarget, ct);
        stopwatch.Stop();
        return new CompareDeployRunResult<SchemaDiffReport>(report, stopwatch.Elapsed);
    }

    public Task<CompareDeployRunResult<DataDiffReport>> CompareDataAsync(
        string sourcePath,
        string tableName,
        string keyColumns,
        int maxPreviewRows = 100,
        CancellationToken ct = default)
        => CompareDataAsync(
            new CompareDeployEndpointSpec(CompareDeployEndpointKind.DatabaseFile, sourcePath),
            CompareDeployEndpointSpec.CurrentDatabase,
            tableName,
            keyColumns,
            maxPreviewRows,
            ct);

    public async Task<CompareDeployRunResult<DataDiffReport>> CompareDataAsync(
        CompareDeployEndpointSpec source,
        CompareDeployEndpointSpec target,
        string tableName,
        string keyColumns,
        int maxPreviewRows = 100,
        CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(tableName))
            throw new InvalidOperationException("Data compare requires a table name.");

        string normalizedTableName = tableName.Trim();
        await using CompareDeployTargetHandle sourceHandle = await CreateTargetAsync(source, normalizedTableName, ct);
        await using CompareDeployTargetHandle targetHandle = await CreateTargetAsync(target, normalizedTableName, ct);
        var options = new DataCompareOptions
        {
            TableName = normalizedTableName,
            KeyColumns = ParseKeyColumns(keyColumns),
            MaxPreviewRows = maxPreviewRows,
        };

        var stopwatch = Stopwatch.StartNew();
        DataDiffReport report = await new DataComparisonService().CompareAsync(
            sourceHandle.DataTarget,
            targetHandle.DataTarget,
            options,
            ct);
        stopwatch.Stop();
        return new CompareDeployRunResult<DataDiffReport>(report, stopwatch.Elapsed);
    }

    public Task<CompareDeployRunResult<DriftReport>> CreateDriftReportAsync(
        string baselinePath,
        string? tableName,
        string keyColumns,
        int maxPreviewRows = 100,
        CancellationToken ct = default)
        => CreateDriftReportAsync(
            new CompareDeployEndpointSpec(CompareDeployEndpointKind.DatabaseFile, baselinePath),
            CompareDeployEndpointSpec.CurrentDatabase,
            tableName,
            keyColumns,
            maxPreviewRows,
            ct);

    public async Task<CompareDeployRunResult<DriftReport>> CreateDriftReportAsync(
        CompareDeployEndpointSpec baseline,
        CompareDeployEndpointSpec current,
        string? tableName,
        string keyColumns,
        int maxPreviewRows = 100,
        CancellationToken ct = default)
    {
        string? tableOverride = string.IsNullOrWhiteSpace(tableName) ? null : tableName.Trim();
        await using CompareDeployTargetHandle baselineHandle = await CreateTargetAsync(baseline, tableOverride, ct);
        await using CompareDeployTargetHandle currentHandle = await CreateTargetAsync(current, tableOverride, ct);

        ISchemaCompareTarget baselineSchema = baselineHandle.SchemaTarget;
        ISchemaCompareTarget currentSchema = currentHandle.SchemaTarget;
        IDataCompareTarget? baselineData = null;
        IDataCompareTarget? currentData = null;

        var dataTables = new List<DataCompareOptions>();
        if (!string.IsNullOrWhiteSpace(tableOverride))
        {
            baselineData = baselineHandle.DataTarget;
            currentData = currentHandle.DataTarget;
            dataTables.Add(new DataCompareOptions
            {
                TableName = tableOverride,
                KeyColumns = ParseKeyColumns(keyColumns),
                MaxPreviewRows = maxPreviewRows,
            });
        }

        if (baselineHandle.SchemaTarget.Descriptor.Kind == DevOpsTargetKind.TableArchive
            || currentHandle.SchemaTarget.Descriptor.Kind == DevOpsTargetKind.TableArchive)
        {
            if (string.IsNullOrWhiteSpace(tableOverride))
                throw new InvalidOperationException("Table archive drift requires a table name.");

            baselineSchema = new FilteredSchemaCompareTarget(baselineSchema, tableOverride);
            currentSchema = new FilteredSchemaCompareTarget(currentSchema, tableOverride);
        }

        var options = new DriftReportOptions { DataTables = dataTables };
        var stopwatch = Stopwatch.StartNew();
        DriftReport report = await new DriftReportService().CreateAsync(
            baselineSchema,
            currentSchema,
            baselineData,
            currentData,
            options,
            ct);
        stopwatch.Stop();
        return new CompareDeployRunResult<DriftReport>(report, stopwatch.Elapsed);
    }

    public string RenderSchemaScript(SchemaDiffReport report)
        => SchemaScriptRenderer.RenderDeployScript(report);

    public async Task<CompareDeployRunResult<string>> ScriptSchemaAsync(
        CompareDeployEndpointSpec endpoint,
        SchemaScriptOptions options,
        CancellationToken ct = default)
    {
        await using CompareDeployTargetHandle handle = await CreateTargetAsync(endpoint, options.ObjectName, ct);
        var stopwatch = Stopwatch.StartNew();
        SchemaSnapshot snapshot = await handle.SchemaTarget.LoadSchemaAsync(ct);
        string script = SchemaScriptRenderer.RenderSnapshotScript(snapshot, options);
        stopwatch.Stop();
        return new CompareDeployRunResult<string>(script, stopwatch.Elapsed);
    }

    public string RenderDataScript(DataDiffReport report)
        => new DataComparisonService().RenderSyncScript(report);

    public Task<CompareDeployApplyResult> ApplyScriptAsync(string script, CancellationToken ct = default)
        => ApplyScriptAsync(script, CompareDeployEndpointSpec.CurrentDatabase, ct);

    public async Task<CompareDeployApplyResult> ApplyScriptAsync(
        string script,
        CompareDeployEndpointSpec target,
        CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(script))
            throw new InvalidOperationException("A preview script is required before apply.");
        if (!IsWritableEndpoint(target))
            throw new InvalidOperationException("The selected target is read-only. Choose the current database or a database file to apply a script.");

        SqlExecutionResult result;
        if (target.Kind == CompareDeployEndpointKind.CurrentDatabase)
        {
            result = await client.ExecuteSqlAsync(script, ct);
        }
        else
        {
            string fullPath = NormalizeExistingPath(target.Path, "Target database path");
            await using ICSharpDbClient targetClient = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = fullPath });
            _ = await targetClient.GetInfoAsync(ct);
            result = await targetClient.ExecuteSqlAsync(script, ct);
        }

        if (!string.IsNullOrWhiteSpace(result.Error))
            throw new InvalidOperationException(result.Error);

        return new CompareDeployApplyResult(result.Elapsed, result.RowsAffected);
    }

    public static IReadOnlyList<string> ParseKeyColumns(string? value)
        => string.IsNullOrWhiteSpace(value)
            ? []
            : value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    public static bool IsWritableEndpoint(CompareDeployEndpointSpec endpoint)
        => endpoint.Kind is CompareDeployEndpointKind.CurrentDatabase or CompareDeployEndpointKind.DatabaseFile;

    public static string FormatEndpoint(CompareDeployEndpointSpec endpoint)
        => endpoint.Kind switch
        {
            CompareDeployEndpointKind.CurrentDatabase => "Current database",
            CompareDeployEndpointKind.DatabaseFile => string.IsNullOrWhiteSpace(endpoint.Path)
                ? "Database file"
                : $"Database file: {Path.GetFileName(endpoint.Path.Trim())}",
            CompareDeployEndpointKind.TableArchive => string.IsNullOrWhiteSpace(endpoint.Path)
                ? "Table archive"
                : $"Table archive: {Path.GetFileName(endpoint.Path.Trim())}",
            _ => endpoint.Kind.ToString(),
        };

    private async Task<CompareDeployTargetHandle> CreateTargetAsync(
        CompareDeployEndpointSpec endpoint,
        string? tableNameOverride,
        CancellationToken ct)
    {
        switch (endpoint.Kind)
        {
            case CompareDeployEndpointKind.CurrentDatabase:
                return new CompareDeployTargetHandle(
                    new ClientSchemaCompareTarget(client, "Current database"),
                    new ClientDataCompareTarget(client, "Current database"),
                    client: null);
            case CompareDeployEndpointKind.DatabaseFile:
            {
                string fullPath = NormalizeExistingPath(endpoint.Path, "Database path");
                ICSharpDbClient externalClient = CSharpDbClient.Create(new CSharpDbClientOptions { DataSource = fullPath });
                try
                {
                    _ = await externalClient.GetInfoAsync(ct);
                    string displayName = Path.GetFileName(fullPath);
                    return new CompareDeployTargetHandle(
                        new ClientSchemaCompareTarget(externalClient, displayName),
                        new ClientDataCompareTarget(externalClient, displayName),
                        externalClient);
                }
                catch
                {
                    await externalClient.DisposeAsync();
                    throw;
                }
            }
            case CompareDeployEndpointKind.TableArchive:
            {
                string fullPath = NormalizeExistingPath(endpoint.Path, "Archive path");
                return new CompareDeployTargetHandle(
                    new TableArchiveSchemaCompareTarget(fullPath, tableNameOverride),
                    new TableArchiveDataCompareTarget(fullPath, tableNameOverride),
                    client: null);
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(endpoint), endpoint.Kind, null);
        }
    }

    private static string NormalizeExistingPath(string? path, string label)
    {
        if (string.IsNullOrWhiteSpace(path))
            throw new InvalidOperationException($"{label} is required.");

        string fullPath = Path.GetFullPath(path.Trim());
        if (!File.Exists(fullPath))
            throw new FileNotFoundException($"Compare target not found: {fullPath}", fullPath);

        return fullPath;
    }

    private static bool IsSystemTableName(string name)
        => name.StartsWith("_", StringComparison.Ordinal)
           || name.StartsWith("sys.", StringComparison.OrdinalIgnoreCase);

    private sealed class CompareDeployTargetHandle(
        ISchemaCompareTarget schemaTarget,
        IDataCompareTarget dataTarget,
        ICSharpDbClient? client)
        : IAsyncDisposable
    {
        public ISchemaCompareTarget SchemaTarget { get; } = schemaTarget;
        public IDataCompareTarget DataTarget { get; } = dataTarget;

        public async ValueTask DisposeAsync()
        {
            if (client is not null)
                await client.DisposeAsync();
        }
    }

    private sealed class FilteredSchemaCompareTarget : ISchemaCompareTarget
    {
        private readonly ISchemaCompareTarget _inner;
        private readonly string _tableName;

        public FilteredSchemaCompareTarget(ISchemaCompareTarget inner, string tableName)
        {
            _inner = inner;
            _tableName = tableName.Trim();
            Descriptor = new SchemaTargetDescriptor
            {
                Kind = inner.Descriptor.Kind,
                DisplayName = $"{inner.Descriptor.DisplayName} [{_tableName}]",
                Location = inner.Descriptor.Location,
            };
        }

        public SchemaTargetDescriptor Descriptor { get; }

        public async Task<SchemaSnapshot> LoadSchemaAsync(CancellationToken ct = default)
        {
            SchemaSnapshot snapshot = await _inner.LoadSchemaAsync(ct);
            var tableNames = snapshot.Tables
                .Where(table => table.TableName.Equals(_tableName, StringComparison.OrdinalIgnoreCase))
                .Select(table => table.TableName)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            return new SchemaSnapshot
            {
                Target = Descriptor,
                Tables = snapshot.Tables
                    .Where(table => tableNames.Contains(table.TableName))
                    .ToArray(),
                Indexes = snapshot.Indexes
                    .Where(index => tableNames.Contains(index.TableName))
                    .ToArray(),
                Triggers = snapshot.Triggers
                    .Where(trigger => tableNames.Contains(trigger.TableName))
                    .ToArray(),
            };
        }
    }
}
