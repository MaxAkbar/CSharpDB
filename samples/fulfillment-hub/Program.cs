using System.Globalization;
using System.Text.Json;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Forms.Services;
using CSharpDB.Admin.Reports.Models;
using CSharpDB.Admin.Reports.Services;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.Client.Pipelines;
using CSharpDB.Engine;
using CSharpDB.Pipelines.Models;
using CSharpDB.Pipelines.Serialization;
using CSharpDB.Sql;
using Forms = CSharpDB.Admin.Forms.Models;
using Reports = CSharpDB.Admin.Reports.Models;

var jsonOptions = new JsonSerializerOptions
{
    PropertyNameCaseInsensitive = true,
};

string sampleDirectory = AppContext.BaseDirectory;
string schemaPath = Path.Combine(sampleDirectory, "schema.sql");
string proceduresPath = Path.Combine(sampleDirectory, "procedures.json");
string savedQueriesPath = Path.Combine(sampleDirectory, "saved-queries.json");
string pipelinesDirectory = Path.Combine(sampleDirectory, "pipelines");
string outputDirectory = Path.Combine(sampleDirectory, "generated-output");
string dbPath = Path.Combine(sampleDirectory, "fulfillment-hub-demo.db");

PrepareFreshRun(dbPath, outputDirectory);

FullTextSeedSummary fullTextSummary;
CollectionSeedSummary collectionSummary;

await using (Database db = await Database.OpenAsync(dbPath))
{
    await ExecuteSchemaAsync(db, schemaPath);
    await db.EnsureFullTextIndexAsync("fts_ops_playbooks", "ops_playbooks", ["title", "body"]);
    fullTextSummary = await CaptureFullTextSummaryAsync(db);
    collectionSummary = await SeedCollectionsAsync(db);
}

await using ICSharpDbClient client = CSharpDbClient.Create(new CSharpDbClientOptions
{
    Transport = CSharpDbTransport.Direct,
    DataSource = dbPath,
});

await ImportProceduresAsync(client, proceduresPath, jsonOptions);
await ImportSavedQueriesAsync(client, savedQueriesPath, jsonOptions);

DbFormRepository formRepository = new(client);
DbSchemaProvider schemaProvider = new(client);
await SeedFormsAsync(formRepository, schemaProvider);

DbReportRepository reportRepository = new(client);
DbReportSourceProvider reportSourceProvider = new(client);
await SeedReportsAsync(reportRepository, reportSourceProvider);

CSharpDbPipelineCatalogClient pipelineCatalog = new(client);
PipelineSeedSummary pipelineSummary = await SeedPipelinesAsync(pipelineCatalog, pipelinesDirectory, sampleDirectory, outputDirectory);
ProcedureExecutionResult procedureSmokeTest = await client.ExecuteProcedureAsync("RefreshOperationalStats", new Dictionary<string, object?>());
if (!procedureSmokeTest.Succeeded)
    throw new InvalidOperationException($"Procedure smoke test failed: {procedureSmokeTest.Error}");

IReadOnlyList<ProcedureDefinition> procedures = await client.GetProceduresAsync();
IReadOnlyList<SavedQueryDefinition> savedQueries = await client.GetSavedQueriesAsync();
IReadOnlyList<Forms.FormDefinition> forms = await formRepository.ListAsync();
IReadOnlyList<Reports.ReportDefinition> reports = await reportRepository.ListAsync();
IReadOnlyList<PipelineDefinitionSummary> storedPipelines = await pipelineCatalog.ListPipelinesAsync();
IReadOnlyList<PipelineRunResult> pipelineRuns = await pipelineCatalog.ListRunsAsync();

Console.WriteLine("Fulfillment Hub");
Console.WriteLine();
Console.WriteLine($"Database: {dbPath}");
Console.WriteLine($"Forms: {forms.Count} | Reports: {reports.Count} | Procedures: {procedures.Count} | Saved queries: {savedQueries.Count}");
Console.WriteLine($"Stored pipelines: {storedPipelines.Count} | Pipeline runs: {pipelineRuns.Count}");
Console.WriteLine($"Procedure smoke test: {procedureSmokeTest.ProcedureName} -> {procedureSmokeTest.Statements.Count} statements");
Console.WriteLine($"Collections: scanner_sessions={collectionSummary.ScannerSessionCount}, webhook_archive={collectionSummary.WebhookCount}");
Console.WriteLine();

Console.WriteLine("Top open order queue rows:");
await PrintQueryAsync(client, """
    SELECT order_number, customer_name, warehouse_code, order_status, required_ship_date
    FROM order_fulfillment_board
    WHERE order_status IN ('released', 'allocated', 'picking')
    ORDER BY required_ship_date, priority_code DESC, order_number
    LIMIT 4;
    """);

Console.WriteLine();
Console.WriteLine("Low-stock watch:");
await PrintQueryAsync(client, """
    SELECT warehouse_code, sku, available_qty, inbound_qty, reorder_point, shortage_qty
    FROM low_stock_watch
    WHERE shortage_qty > 0
    ORDER BY shortage_qty DESC, warehouse_code, sku
    LIMIT 6;
    """);

Console.WriteLine();
Console.WriteLine("Full-text hits for 'partial receipt':");
if (fullTextSummary.Hits.Count == 0)
{
    Console.WriteLine("  (no hits)");
}
else
{
    foreach (string line in fullTextSummary.Hits)
        Console.WriteLine($"  {line}");
}

Console.WriteLine();
Console.WriteLine("Collection lookups:");
Console.WriteLine($"  scanner_sessions by CurrentWave.OrderNumber=SO-7005 -> {string.Join(", ", collectionSummary.ScannerSessionMatches)}");
Console.WriteLine($"  webhook_archive by $.tags[]=cold-chain -> {string.Join(", ", collectionSummary.WebhookTagMatches)}");

Console.WriteLine();
Console.WriteLine("Pipeline outputs:");
foreach (string line in pipelineSummary.RunSummaries)
    Console.WriteLine($"  {line}");
Console.WriteLine($"  Export file: {pipelineSummary.ExportPath}");

Console.WriteLine();
Console.WriteLine("Seeded forms:");
foreach (Forms.FormDefinition form in forms.OrderBy(item => item.Name, StringComparer.OrdinalIgnoreCase))
    Console.WriteLine($"  {form.FormId} -> {form.Name} ({form.TableName})");

Console.WriteLine();
Console.WriteLine("Seeded reports:");
foreach (Reports.ReportDefinition report in reports.OrderBy(item => item.Name, StringComparer.OrdinalIgnoreCase))
    Console.WriteLine($"  {report.ReportId} -> {report.Name} ({report.Source.Kind}:{report.Source.Name})");

static void PrepareFreshRun(string dbPath, string outputDirectory)
{
    foreach (string path in new[] { dbPath, $"{dbPath}-wal", $"{dbPath}-shm" })
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    if (Directory.Exists(outputDirectory))
        Directory.Delete(outputDirectory, recursive: true);

    Directory.CreateDirectory(outputDirectory);
}

static async Task ExecuteSchemaAsync(Database db, string schemaPath)
{
    string script = await File.ReadAllTextAsync(schemaPath);
    foreach (string statement in SqlScriptSplitter.SplitExecutableStatements(script))
        await db.ExecuteAsync(statement);
}

static async Task<FullTextSeedSummary> CaptureFullTextSummaryAsync(Database db)
{
    IReadOnlyList<FullTextSearchHit> hits = await db.SearchAsync("fts_ops_playbooks", "partial receipt");
    var lines = new List<string>(hits.Count);

    foreach (FullTextSearchHit hit in hits.Take(3))
    {
        await using var titleResult = await db.ExecuteAsync($"""
            SELECT title
            FROM ops_playbooks
            WHERE id = {hit.RowId};
            """);

        var rows = await titleResult.ToListAsync();
        string title = rows.Count > 0 ? rows[0][0].AsText : $"Playbook {hit.RowId}";
        lines.Add($"row={hit.RowId} | score={hit.Score:F2} | {title}");
    }

    return new FullTextSeedSummary(lines);
}

static async Task<CollectionSeedSummary> SeedCollectionsAsync(Database db)
{
    Collection<ScannerSessionDocument> scannerSessions = await db.GetCollectionAsync<ScannerSessionDocument>("scanner_sessions");
    await scannerSessions.PutAsync("session:sea:wave-a", new ScannerSessionDocument(
        "scanner-01",
        "SEA-FC",
        "Ava Cole",
        "wave-a",
        new ScannerWaveState("SO-7005", "picking", "2026-04-24T10:15:00Z"),
        ["BOT-220", "TRJ-100"],
        ["night-shift", "priority-lane"],
        "2026-04-24T10:17:00Z"));
    await scannerSessions.PutAsync("session:den:receiving", new ScannerSessionDocument(
        "scanner-07",
        "DEN-DC",
        "Marcus Lin",
        "receiving",
        new ScannerWaveState("PO-9002", "receiving", "2026-04-24T09:42:00Z"),
        ["MPR-510", "CCS-810"],
        ["receiving", "cold-chain"],
        "2026-04-24T09:50:00Z"));
    await scannerSessions.EnsureIndexAsync(item => item.WarehouseCode);
    await scannerSessions.EnsureIndexAsync("CurrentWave.OrderNumber");
    await scannerSessions.EnsureIndexAsync("$.tags[]");

    Collection<WebhookArchiveDocument> webhookArchive = await db.GetCollectionAsync<WebhookArchiveDocument>("webhook_archive");
    await webhookArchive.PutAsync("webhook:carrier:1", new WebhookArchiveDocument(
        "carrier-webhook",
        "shipment.scan",
        "2026-04-24T08:14:00Z",
        new WebhookHeaders("SO-7003", "SHP-8001"),
        ["carriers", "tracking"],
        """{"tracking":"1Z999AA10123456784","status":"delivered"}"""));
    await webhookArchive.PutAsync("webhook:marketplace:2", new WebhookArchiveDocument(
        "marketplace-webhook",
        "inventory.adjustment",
        "2026-04-24T08:42:00Z",
        new WebhookHeaders("SO-7005", null),
        ["marketplace", "cold-chain"],
        """{"sku":"CCS-810","reason":"safety-stock-floor"}"""));
    await webhookArchive.EnsureIndexAsync(item => item.Provider);
    await webhookArchive.EnsureIndexAsync("Headers.OrderNumber");
    await webhookArchive.EnsureIndexAsync("$.tags[]");

    var scannerMatches = new List<string>();
    await foreach (var match in scannerSessions.FindByPathAsync("CurrentWave.OrderNumber", "SO-7005"))
        scannerMatches.Add(match.Key);

    var webhookMatches = new List<string>();
    await foreach (var match in webhookArchive.FindByPathAsync("$.tags[]", "cold-chain"))
        webhookMatches.Add(match.Key);

    return new CollectionSeedSummary(
        ScannerSessionCount: (int)await scannerSessions.CountAsync(),
        WebhookCount: (int)await webhookArchive.CountAsync(),
        ScannerSessionMatches: scannerMatches,
        WebhookTagMatches: webhookMatches);
}

static async Task ImportProceduresAsync(ICSharpDbClient client, string proceduresPath, JsonSerializerOptions jsonOptions)
{
    string json = await File.ReadAllTextAsync(proceduresPath);
    List<StoredProcedureFile>? fileDefinitions = JsonSerializer.Deserialize<List<StoredProcedureFile>>(json, jsonOptions);
    if (fileDefinitions is null)
        throw new InvalidOperationException("Procedure file did not deserialize.");

    foreach (StoredProcedureFile fileDefinition in fileDefinitions)
    {
        ProcedureDefinition definition = new()
        {
            Name = fileDefinition.Name,
            BodySql = fileDefinition.BodySql,
            Description = fileDefinition.Description,
            IsEnabled = fileDefinition.IsEnabled,
            Parameters = fileDefinition.Parameters.Select(parameter => new ProcedureParameterDefinition
            {
                Name = parameter.Name,
                Type = Enum.Parse<DbType>(parameter.Type, ignoreCase: true),
                Required = parameter.Required,
                Default = ConvertJsonValue(parameter.Default),
                Description = parameter.Description,
            }).ToArray(),
        };

        ProcedureDefinition? existing = await client.GetProcedureAsync(definition.Name);
        if (existing is null)
            await client.CreateProcedureAsync(definition);
        else
            await client.UpdateProcedureAsync(definition.Name, definition);
    }
}

static async Task ImportSavedQueriesAsync(ICSharpDbClient client, string savedQueriesPath, JsonSerializerOptions jsonOptions)
{
    string json = await File.ReadAllTextAsync(savedQueriesPath);
    List<SavedQueryFile>? queries = JsonSerializer.Deserialize<List<SavedQueryFile>>(json, jsonOptions);
    if (queries is null)
        throw new InvalidOperationException("Saved query file did not deserialize.");

    foreach (SavedQueryFile query in queries)
        await client.UpsertSavedQueryAsync(query.Name, query.SqlText);
}

static async Task SeedFormsAsync(DbFormRepository repository, DbSchemaProvider schemaProvider)
{
    Forms.FormTableDefinition orders = await RequireTableAsync(schemaProvider, "orders");
    Forms.FormTableDefinition purchaseOrders = await RequireTableAsync(schemaProvider, "purchase_orders");
    Forms.FormTableDefinition returns = await RequireTableAsync(schemaProvider, "returns");

    await repository.CreateAsync(CreateOrderWorkbenchForm(orders));
    await repository.CreateAsync(CreatePurchaseOrderReceivingForm(purchaseOrders));
    await repository.CreateAsync(CreateReturnIntakeForm(returns));
}

static async Task SeedReportsAsync(DbReportRepository repository, DbReportSourceProvider sourceProvider)
{
    Reports.ReportSourceDefinition shipmentManifestSource = await RequireReportSourceAsync(sourceProvider, ReportSourceKind.View, "shipment_manifest_report_source");
    Reports.ReportSourceDefinition lowStockSource = await RequireReportSourceAsync(sourceProvider, ReportSourceKind.View, "low_stock_watch");
    Reports.ReportSourceDefinition openOrderSource = await RequireReportSourceAsync(sourceProvider, ReportSourceKind.SavedQuery, "Open Order Queue");

    await repository.CreateAsync(CreateShipmentManifestReport(shipmentManifestSource));
    await repository.CreateAsync(CreateLowStockReport(lowStockSource));
    await repository.CreateAsync(CreateOpenOrderQueueReport(openOrderSource));
}

static async Task<PipelineSeedSummary> SeedPipelinesAsync(
    CSharpDbPipelineCatalogClient pipelineCatalog,
    string pipelinesDirectory,
    string sampleDirectory,
    string outputDirectory)
{
    string[] pipelineFiles =
    [
        Path.Combine(pipelinesDirectory, "supplier-receipts-import.json"),
        Path.Combine(pipelinesDirectory, "marketplace-orders-import.json"),
        Path.Combine(pipelinesDirectory, "low-stock-export.json"),
    ];

    var runSummaries = new List<string>(pipelineFiles.Length);
    foreach (string pipelineFile in pipelineFiles)
    {
        PipelinePackageDefinition filePackage = await PipelinePackageSerializer.LoadFromFileAsync(pipelineFile);
        PipelinePackageDefinition package = ResolvePipelinePaths(filePackage, sampleDirectory, outputDirectory);
        await pipelineCatalog.SavePipelineAsync(package);
        PipelineRunResult run = await pipelineCatalog.RunStoredPipelineAsync(package.Name);
        runSummaries.Add($"{package.Name} -> {run.Status} | rowsRead={run.Metrics.RowsRead} | rowsWritten={run.Metrics.RowsWritten} | rejects={run.Metrics.RowsRejected}");
    }

    return new PipelineSeedSummary(
        RunSummaries: runSummaries,
        ExportPath: Path.Combine(outputDirectory, "low-stock-watch.csv"));
}

static PipelinePackageDefinition ResolvePipelinePaths(PipelinePackageDefinition package, string sampleDirectory, string outputDirectory)
{
    string ResolvePath(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return value ?? string.Empty;

        return value
            .Replace("__SAMPLE_DIR__", sampleDirectory, StringComparison.Ordinal)
            .Replace("__OUTPUT_DIR__", outputDirectory, StringComparison.Ordinal);
    }

    return new PipelinePackageDefinition
    {
        Name = package.Name,
        Version = package.Version,
        Description = package.Description,
        Source = new PipelineSourceDefinition
        {
            Kind = package.Source.Kind,
            Path = ResolvePath(package.Source.Path),
            ConnectionString = package.Source.ConnectionString,
            TableName = package.Source.TableName,
            QueryText = package.Source.QueryText,
            HasHeaderRow = package.Source.HasHeaderRow,
        },
        Transforms = package.Transforms.Select(transform => new PipelineTransformDefinition
        {
            Kind = transform.Kind,
            SelectColumns = transform.SelectColumns?.ToArray(),
            RenameMappings = transform.RenameMappings?.Select(mapping => new PipelineRenameMapping
            {
                Source = mapping.Source,
                Target = mapping.Target,
            }).ToArray(),
            CastMappings = transform.CastMappings?.Select(mapping => new PipelineCastMapping
            {
                Column = mapping.Column,
                TargetType = mapping.TargetType,
            }).ToArray(),
            FilterExpression = transform.FilterExpression,
            DerivedColumns = transform.DerivedColumns?.Select(column => new PipelineDerivedColumn
            {
                Name = column.Name,
                Expression = column.Expression,
            }).ToArray(),
            DeduplicateKeys = transform.DeduplicateKeys?.ToArray(),
        }).ToArray(),
        Destination = new PipelineDestinationDefinition
        {
            Kind = package.Destination.Kind,
            Path = ResolvePath(package.Destination.Path),
            ConnectionString = package.Destination.ConnectionString,
            TableName = package.Destination.TableName,
            Overwrite = package.Destination.Overwrite,
        },
        Options = new PipelineExecutionOptions
        {
            BatchSize = package.Options.BatchSize,
            ErrorMode = package.Options.ErrorMode,
            CheckpointInterval = package.Options.CheckpointInterval,
            MaxRejects = package.Options.MaxRejects,
        },
        Incremental = package.Incremental is null
            ? null
            : new PipelineIncrementalOptions
            {
                WatermarkColumn = package.Incremental.WatermarkColumn,
                LastProcessedValue = package.Incremental.LastProcessedValue,
            },
    };
}

static async Task PrintQueryAsync(ICSharpDbClient client, string sql)
{
    SqlExecutionResult result = await client.ExecuteSqlAsync(sql);
    if (!string.IsNullOrWhiteSpace(result.Error))
        throw new InvalidOperationException(result.Error);

    if (result.Rows is null || result.Rows.Count == 0)
    {
        Console.WriteLine("  (no rows)");
        return;
    }

    foreach (object?[] row in result.Rows)
        Console.WriteLine($"  {string.Join(" | ", row.Select(FormatValue))}");
}

static string FormatValue(object? value)
{
    if (value is null)
        return "NULL";

    return value switch
    {
        double number => number.ToString("F2", CultureInfo.InvariantCulture),
        float number => number.ToString("F2", CultureInfo.InvariantCulture),
        decimal number => number.ToString("F2", CultureInfo.InvariantCulture),
        _ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty,
    };
}

static object? ConvertJsonValue(JsonElement? element)
{
    if (!element.HasValue)
        return null;

    JsonElement value = element.Value;
    return value.ValueKind switch
    {
        JsonValueKind.Null or JsonValueKind.Undefined => null,
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        JsonValueKind.Number when value.TryGetInt64(out long int64) => int64,
        JsonValueKind.Number => value.GetDouble(),
        JsonValueKind.String => value.GetString(),
        _ => value.ToString(),
    };
}

static async Task<Forms.FormTableDefinition> RequireTableAsync(DbSchemaProvider schemaProvider, string tableName)
{
    return await schemaProvider.GetTableDefinitionAsync(tableName)
        ?? throw new InvalidOperationException($"Form table '{tableName}' was not found.");
}

static async Task<Reports.ReportSourceDefinition> RequireReportSourceAsync(DbReportSourceProvider sourceProvider, ReportSourceKind kind, string name)
{
    return await sourceProvider.GetSourceDefinitionAsync(new Reports.ReportSourceReference(kind, name))
        ?? throw new InvalidOperationException($"Report source '{kind}:{name}' was not found.");
}

static Forms.FormDefinition CreateOrderWorkbenchForm(Forms.FormTableDefinition table)
{
    const double labelX = 24;
    const double leftX = 172;
    const double rightLabelX = 420;
    const double rightX = 556;
    const double rowHeight = 34;
    const double labelWidth = 132;
    const double fieldWidth = 220;
    const double top = 24;
    const double step = 48;

    var controls = new List<Forms.ControlDefinition>();
    void AddField(double y, string fieldName, string label, string controlType, IReadOnlyDictionary<string, object?>? props = null, double x = leftX, double labelPos = labelX, double width = fieldWidth)
    {
        var mergedProps = props is null
            ? new Dictionary<string, object?>()
            : new Dictionary<string, object?>(props);

        if (controlType == "checkbox")
            mergedProps["text"] = label;
        else if (controlType is "text" or "number" or "textarea")
            mergedProps.TryAdd("placeholder", label);

        controls.Add(BoundControl(fieldName, controlType, x, y, width, rowHeight, mergedProps, controlType == "computed" ? "OneWay" : "TwoWay"));
    }

    AddField(top + (step * 0), "order_number", "Order Number", "text");
    AddField(top + (step * 0), "customer_id", "Customer", "lookup", LookupProps("customers", "name", "id", "Select customer"), rightX, rightLabelX);
    AddField(top + (step * 1), "warehouse_id", "Warehouse", "lookup", LookupProps("warehouses", "warehouse_code", "id", "Select warehouse"));
    AddField(top + (step * 2), "required_ship_date", "Required Ship", "date");
    AddField(top + (step * 2), "status", "Status", "text", null, rightX, rightLabelX);
    AddField(top + (step * 3), "is_expedited", "Expedited", "checkbox");
    AddField(top + (step * 3), "total_amount", "Order Total", "number", NumberProps(min: 0), rightX, rightLabelX);

    controls.Add(BoundControl("notes", "textarea", leftX, top + (step * 4), 756, 72, new Dictionary<string, object?> { ["placeholder"] = "Planner notes and exceptions." }));

    double summaryY = top + (step * 6);
    controls.Add(LabelControl("Remaining Units", "remaining_units_total", labelX, summaryY, labelWidth, rowHeight));
    AddField(summaryY, "remaining_units_total", "Remaining Units", "computed", ComputedProps("=SUM(order_lines.ordered_qty) - SUM(order_lines.shipped_qty)", "0"), leftX, labelX, 160);

    IReadOnlyList<Forms.ChildTabConfig> tabs =
    [
        new(
            Id: "lines",
            Label: "Lines",
            ChildTable: "order_lines",
            ForeignKeyField: "order_id",
            ParentKeyField: "id",
            VisibleColumns: ["line_number", "product_id", "ordered_qty", "allocated_qty", "line_total"],
            AllowAdd: true,
            AllowEdit: true,
            AllowDelete: true,
            ChildTabs: [])
    ];

    controls.Add(new Forms.ControlDefinition(
        ControlId: "order-tabs",
        ControlType: "childtabs",
        Rect: new Forms.Rect(24, 360, 904, 320),
        Binding: null,
        Props: new Forms.PropertyBag(new Dictionary<string, object?>
        {
            ["tabs"] = Forms.ChildTabConfigMapper.ToPropertyBag(tabs),
        }),
        ValidationOverride: null));

    return new Forms.FormDefinition(
        FormId: "orders-workbench",
        Name: "Order Workbench",
        TableName: table.TableName,
        DefinitionVersion: 1,
        SourceSchemaSignature: table.SourceSchemaSignature,
        Layout: StandardFormLayout(),
        Controls: controls);
}

static Forms.FormDefinition CreatePurchaseOrderReceivingForm(Forms.FormTableDefinition table)
{
    const double labelX = 24;
    const double leftX = 172;
    const double rightLabelX = 420;
    const double rightX = 556;
    const double rowHeight = 34;
    const double labelWidth = 132;
    const double fieldWidth = 220;
    const double top = 24;
    const double step = 48;

    var controls = new List<Forms.ControlDefinition>();
    void AddField(double y, string fieldName, string label, string controlType, IReadOnlyDictionary<string, object?>? props = null, double x = leftX, double labelPos = labelX, double width = fieldWidth)
    {
        var mergedProps = props is null
            ? new Dictionary<string, object?>()
            : new Dictionary<string, object?>(props);

        if (controlType == "checkbox")
            mergedProps["text"] = label;
        else if (controlType is "text" or "number" or "textarea")
            mergedProps.TryAdd("placeholder", label);

        controls.Add(BoundControl(fieldName, controlType, x, y, width, rowHeight, mergedProps, controlType == "computed" ? "OneWay" : "TwoWay"));
    }

    AddField(top + (step * 0), "po_number", "PO Number", "text");
    AddField(top + (step * 0), "supplier_id", "Supplier", "lookup", LookupProps("suppliers", "name", "id", "Select supplier"), rightX, rightLabelX);
    AddField(top + (step * 1), "warehouse_id", "Warehouse", "lookup", LookupProps("warehouses", "warehouse_code", "id", "Select warehouse"));
    AddField(top + (step * 1), "expected_date", "Expected", "date", null, rightX, rightLabelX);
    AddField(top + (step * 2), "status", "Status", "text");
    AddField(top + (step * 2), "priority_receiving", "Priority", "checkbox", null, rightX, rightLabelX);

    controls.Add(BoundControl("notes", "textarea", leftX, top + (step * 3), 756, 72, new Dictionary<string, object?> { ["placeholder"] = "Receiving notes and dock exceptions." }));

    double summaryY = top + (step * 5);
    controls.Add(LabelControl("Outstanding", "outstanding_units_total", labelX, summaryY, labelWidth, rowHeight));
    AddField(summaryY, "outstanding_units_total", "Outstanding", "computed", ComputedProps("=SUM(purchase_order_lines.ordered_qty) - SUM(purchase_order_lines.received_qty)", "0"), leftX, labelX, 160);

    controls.Add(new Forms.ControlDefinition(
        ControlId: "po-lines",
        ControlType: "datagrid",
        Rect: new Forms.Rect(24, 320, 904, 280),
        Binding: null,
        Props: new Forms.PropertyBag(new Dictionary<string, object?>
        {
            ["childTable"] = "purchase_order_lines",
            ["foreignKeyField"] = "purchase_order_id",
            ["parentKeyField"] = "id",
            ["visibleColumns"] = new object?[] { "product_id", "ordered_qty" },
            ["allowAdd"] = true,
            ["allowEdit"] = true,
            ["allowDelete"] = true,
        }),
        ValidationOverride: null));

    return new Forms.FormDefinition(
        FormId: "purchase-orders-receiving",
        Name: "Purchase Order Receiving",
        TableName: table.TableName,
        DefinitionVersion: 1,
        SourceSchemaSignature: table.SourceSchemaSignature,
        Layout: StandardFormLayout(),
        Controls: controls);
}

static Forms.FormDefinition CreateReturnIntakeForm(Forms.FormTableDefinition table)
{
    const double labelX = 24;
    const double leftX = 172;
    const double rightLabelX = 420;
    const double rightX = 556;
    const double rowHeight = 34;
    const double fieldWidth = 220;
    const double top = 24;
    const double step = 48;

    var controls = new List<Forms.ControlDefinition>();
    void AddField(double y, string fieldName, string label, string controlType, IReadOnlyDictionary<string, object?>? props = null, double x = leftX, double labelPos = labelX, double width = fieldWidth)
    {
        var mergedProps = props is null
            ? new Dictionary<string, object?>()
            : new Dictionary<string, object?>(props);

        if (controlType == "checkbox")
            mergedProps["text"] = label;
        else if (controlType is "text" or "number" or "textarea")
            mergedProps.TryAdd("placeholder", label);

        controls.Add(BoundControl(fieldName, controlType, x, y, width, rowHeight, mergedProps));
    }

    AddField(top + (step * 0), "return_number", "Return Number", "text");
    AddField(top + (step * 0), "order_id", "Order", "lookup", LookupProps("orders", "order_number", "id", "Select order"), rightX, rightLabelX);
    AddField(top + (step * 1), "product_id", "Product", "lookup", LookupProps("products", "name", "id", "Select product"));
    AddField(top + (step * 1), "warehouse_id", "Warehouse", "lookup", LookupProps("warehouses", "warehouse_code", "id", "Select warehouse"), rightX, rightLabelX);
    AddField(top + (step * 2), "requested_date", "Requested", "date");
    AddField(top + (step * 2), "received_date", "Received", "date", null, rightX, rightLabelX);
    AddField(top + (step * 3), "quantity", "Quantity", "number", NumberProps(min: 0));
    AddField(top + (step * 3), "status", "Status", "text", null, rightX, rightLabelX);
    AddField(top + (step * 4), "disposition", "Disposition", "text");
    AddField(top + (step * 4), "requires_qc", "Requires QC", "checkbox", null, rightX, rightLabelX);

    controls.Add(BoundControl("reason", "textarea", leftX, top + (step * 5), 756, 60, new Dictionary<string, object?> { ["placeholder"] = "Return reason." }));

    controls.Add(BoundControl("notes", "textarea", leftX, top + (step * 7), 756, 72, new Dictionary<string, object?> { ["placeholder"] = "Inspection notes." }));

    return new Forms.FormDefinition(
        FormId: "returns-intake",
        Name: "Return Intake",
        TableName: table.TableName,
        DefinitionVersion: 1,
        SourceSchemaSignature: table.SourceSchemaSignature,
        Layout: StandardFormLayout(),
        Controls: controls);
}

static Forms.LayoutDefinition StandardFormLayout()
    => new("absolute", 8, true, [new Forms.Breakpoint("md", 0, null)]);

static Forms.ControlDefinition LabelControl(string text, string forField, double x, double y, double width, double height)
    => new(
        ControlId: $"{CompactId(forField)}-lbl-{(int)y}",
        ControlType: "label",
        Rect: new Forms.Rect(x, y, width, height),
        Binding: null,
        Props: new Forms.PropertyBag(new Dictionary<string, object?>
        {
            ["text"] = text,
            ["forField"] = forField,
        }),
        ValidationOverride: null);

static Forms.ControlDefinition BoundControl(
    string fieldName,
    string controlType,
    double x,
    double y,
    double width,
    double height,
    IReadOnlyDictionary<string, object?>? props = null,
    string mode = "TwoWay")
    => new(
        ControlId: $"{CompactId(fieldName)}-{CompactId(controlType)}",
        ControlType: controlType,
        Rect: new Forms.Rect(x, y, width, height),
        Binding: new Forms.BindingDefinition(fieldName, mode),
        Props: new Forms.PropertyBag(props ?? new Dictionary<string, object?>()),
        ValidationOverride: null);

static string CompactId(string value)
    => value.Replace("_", "-", StringComparison.Ordinal).ToLowerInvariant();

static IReadOnlyDictionary<string, object?> LookupProps(string tableName, string displayField, string valueField, string placeholder)
    => new Dictionary<string, object?>
    {
        ["lookupTable"] = tableName,
        ["displayField"] = displayField,
        ["valueField"] = valueField,
        ["placeholder"] = placeholder,
    };

static IReadOnlyDictionary<string, object?> NumberProps(double? min = null, double? max = null)
{
    var props = new Dictionary<string, object?>();
    if (min.HasValue)
        props["min"] = min.Value;
    if (max.HasValue)
        props["max"] = max.Value;
    return props;
}

static IReadOnlyDictionary<string, object?> ComputedProps(string formula, string format)
    => new Dictionary<string, object?>
    {
        ["formula"] = formula,
        ["format"] = format,
        ["readOnly"] = true,
    };

static Reports.ReportDefinition CreateShipmentManifestReport(Reports.ReportSourceDefinition source)
{
    List<Reports.ReportBandDefinition> bands =
    [
        new(
            "report-header",
            ReportBandKind.ReportHeader,
            36,
            GroupId: null,
            Controls:
            [
                ReportLabel("report-title", "report-header", 24, 6, 400, 24, "Shipment Manifest", fontSize: 20, fontWeight: "600")
            ]),
        new(
            "page-header",
            ReportBandKind.PageHeader,
            22,
            GroupId: null,
            Controls:
            [
                ReportLabel("page-sku", "page-header", 24, 2, 90, 18, "SKU", fontWeight: "600"),
                ReportLabel("page-product", "page-header", 126, 2, 220, 18, "Product", fontWeight: "600"),
                ReportLabel("page-qty", "page-header", 362, 2, 80, 18, "Qty", fontWeight: "600"),
                ReportLabel("page-value", "page-header", 456, 2, 90, 18, "Line Total", fontWeight: "600"),
                ReportLabel("page-tracking", "page-header", 566, 2, 180, 18, "Tracking", fontWeight: "600")
            ]),
        new(
            "shipment-group-header",
            ReportBandKind.GroupHeader,
            40,
            GroupId: "shipment-group",
            Controls:
            [
                ReportLabel("gh-shipment-label", "shipment-group-header", 24, 2, 80, 18, "Shipment", fontWeight: "600"),
                ReportBoundText("gh-shipment-number", "shipment-group-header", 108, 2, 120, 18, "shipment_number"),
                ReportLabel("gh-customer-label", "shipment-group-header", 246, 2, 70, 18, "Customer", fontWeight: "600"),
                ReportBoundText("gh-customer", "shipment-group-header", 320, 2, 220, 18, "customer_name"),
                ReportLabel("gh-carrier-label", "shipment-group-header", 24, 20, 70, 18, "Carrier", fontWeight: "600"),
                ReportBoundText("gh-carrier", "shipment-group-header", 108, 20, 120, 18, "carrier_name"),
                ReportLabel("gh-order-label", "shipment-group-header", 246, 20, 60, 18, "Order", fontWeight: "600"),
                ReportBoundText("gh-order", "shipment-group-header", 320, 20, 120, 18, "order_number"),
                ReportLabel("gh-tracking-label", "shipment-group-header", 456, 20, 70, 18, "Tracking", fontWeight: "600"),
                ReportBoundText("gh-tracking", "shipment-group-header", 530, 20, 216, 18, "tracking_number")
            ]),
        new(
            "detail",
            ReportBandKind.Detail,
            22,
            GroupId: null,
            Controls:
            [
                ReportBoundText("detail-sku", "detail", 24, 2, 90, 18, "sku"),
                ReportBoundText("detail-product", "detail", 126, 2, 220, 18, "product_name"),
                ReportBoundText("detail-qty", "detail", 362, 2, 80, 18, "quantity_shipped", formatString: "0"),
                ReportBoundText("detail-total", "detail", 456, 2, 90, 18, "line_total", formatString: "F2"),
                ReportBoundText("detail-tracking", "detail", 566, 2, 180, 18, "tracking_number")
            ]),
        new(
            "shipment-group-footer",
            ReportBandKind.GroupFooter,
            24,
            GroupId: "shipment-group",
            Controls:
            [
                ReportLabel("gf-items-label", "shipment-group-footer", 320, 2, 120, 18, "Shipment Units", fontWeight: "600"),
                ReportCalculated("gf-items", "shipment-group-footer", 442, 2, 80, 18, "=SUM(quantity_shipped)", "0"),
                ReportLabel("gf-value-label", "shipment-group-footer", 540, 2, 90, 18, "Shipment Value", fontWeight: "600"),
                ReportCalculated("gf-value", "shipment-group-footer", 632, 2, 100, 18, "=SUM(line_total)", "F2")
            ]),
        StandardPageFooter()
    ];

    return new Reports.ReportDefinition(
        ReportId: "shipment-manifest",
        Name: "Shipment Manifest",
        Source: new Reports.ReportSourceReference(source.Kind, source.Name),
        DefinitionVersion: 1,
        SourceSchemaSignature: source.SourceSchemaSignature,
        PageSettings: Reports.ReportPageSettings.DefaultLetterPortrait,
        Groups: [new Reports.ReportGroupDefinition("shipment-group", "shipment_number")],
        Sorts: [new Reports.ReportSortDefinition("shipment_number"), new Reports.ReportSortDefinition("sku")],
        Bands: bands);
}

static Reports.ReportDefinition CreateLowStockReport(Reports.ReportSourceDefinition source)
{
    List<Reports.ReportBandDefinition> bands =
    [
        new(
            "report-header",
            ReportBandKind.ReportHeader,
            36,
            GroupId: null,
            Controls:
            [
                ReportLabel("report-title", "report-header", 24, 6, 320, 24, "Low Stock Watch", fontSize: 20, fontWeight: "600")
            ]),
        new(
            "page-header",
            ReportBandKind.PageHeader,
            22,
            GroupId: null,
            Controls:
            [
                ReportLabel("page-sku", "page-header", 24, 2, 80, 18, "SKU", fontWeight: "600"),
                ReportLabel("page-product", "page-header", 112, 2, 190, 18, "Product", fontWeight: "600"),
                ReportLabel("page-supplier", "page-header", 312, 2, 140, 18, "Supplier", fontWeight: "600"),
                ReportLabel("page-available", "page-header", 462, 2, 70, 18, "Avail", fontWeight: "600"),
                ReportLabel("page-inbound", "page-header", 540, 2, 70, 18, "Inbound", fontWeight: "600"),
                ReportLabel("page-reorder", "page-header", 618, 2, 70, 18, "Reorder", fontWeight: "600"),
                ReportLabel("page-shortage", "page-header", 696, 2, 70, 18, "Shortage", fontWeight: "600")
            ]),
        new(
            "warehouse-group-header",
            ReportBandKind.GroupHeader,
            24,
            GroupId: "warehouse-group",
            Controls:
            [
                ReportLabel("warehouse-label", "warehouse-group-header", 24, 2, 90, 18, "Warehouse", fontWeight: "600"),
                ReportBoundText("warehouse-code", "warehouse-group-header", 118, 2, 100, 18, "warehouse_code"),
                ReportBoundText("warehouse-name", "warehouse-group-header", 226, 2, 240, 18, "warehouse_name")
            ]),
        new(
            "detail",
            ReportBandKind.Detail,
            22,
            GroupId: null,
            Controls:
            [
                ReportBoundText("detail-sku", "detail", 24, 2, 80, 18, "sku"),
                ReportBoundText("detail-product", "detail", 112, 2, 190, 18, "product_name"),
                ReportBoundText("detail-supplier", "detail", 312, 2, 140, 18, "supplier_name"),
                ReportBoundText("detail-available", "detail", 462, 2, 70, 18, "available_qty", formatString: "0"),
                ReportBoundText("detail-inbound", "detail", 540, 2, 70, 18, "inbound_qty", formatString: "0"),
                ReportBoundText("detail-reorder", "detail", 618, 2, 70, 18, "reorder_point", formatString: "0"),
                ReportBoundText("detail-shortage", "detail", 696, 2, 70, 18, "shortage_qty", formatString: "0")
            ]),
        new(
            "warehouse-group-footer",
            ReportBandKind.GroupFooter,
            22,
            GroupId: "warehouse-group",
            Controls:
            [
                ReportLabel("footer-shortage-label", "warehouse-group-footer", 520, 2, 120, 18, "Total Shortage", fontWeight: "600"),
                ReportCalculated("footer-shortage", "warehouse-group-footer", 648, 2, 80, 18, "=SUM(shortage_qty)", "0")
            ]),
        StandardPageFooter()
    ];

    return new Reports.ReportDefinition(
        ReportId: "low-stock-watch",
        Name: "Low Stock Watch",
        Source: new Reports.ReportSourceReference(source.Kind, source.Name),
        DefinitionVersion: 1,
        SourceSchemaSignature: source.SourceSchemaSignature,
        PageSettings: Reports.ReportPageSettings.DefaultLetterPortrait,
        Groups: [new Reports.ReportGroupDefinition("warehouse-group", "warehouse_code")],
        Sorts: [new Reports.ReportSortDefinition("warehouse_code"), new Reports.ReportSortDefinition("shortage_qty", Descending: true), new Reports.ReportSortDefinition("sku")],
        Bands: bands);
}

static Reports.ReportDefinition CreateOpenOrderQueueReport(Reports.ReportSourceDefinition source)
{
    List<Reports.ReportBandDefinition> bands =
    [
        new(
            "report-header",
            ReportBandKind.ReportHeader,
            36,
            GroupId: null,
            Controls:
            [
                ReportLabel("report-title", "report-header", 24, 6, 360, 24, "Open Order Queue", fontSize: 20, fontWeight: "600")
            ]),
        new(
            "page-header",
            ReportBandKind.PageHeader,
            22,
            GroupId: null,
            Controls:
            [
                ReportLabel("page-order", "page-header", 24, 2, 90, 18, "Order", fontWeight: "600"),
                ReportLabel("page-customer", "page-header", 126, 2, 180, 18, "Customer", fontWeight: "600"),
                ReportLabel("page-warehouse", "page-header", 318, 2, 90, 18, "Warehouse", fontWeight: "600"),
                ReportLabel("page-status", "page-header", 420, 2, 80, 18, "Status", fontWeight: "600"),
                ReportLabel("page-due", "page-header", 512, 2, 100, 18, "Required", fontWeight: "600"),
                ReportLabel("page-value", "page-header", 624, 2, 90, 18, "Value", fontWeight: "600")
            ]),
        new(
            "detail",
            ReportBandKind.Detail,
            22,
            GroupId: null,
            Controls:
            [
                ReportBoundText("detail-order", "detail", 24, 2, 90, 18, "order_number"),
                ReportBoundText("detail-customer", "detail", 126, 2, 180, 18, "customer_name"),
                ReportBoundText("detail-warehouse", "detail", 318, 2, 90, 18, "warehouse_code"),
                ReportBoundText("detail-status", "detail", 420, 2, 80, 18, "order_status"),
                ReportBoundText("detail-required", "detail", 512, 2, 100, 18, "required_ship_date"),
                ReportBoundText("detail-value", "detail", 624, 2, 90, 18, "total_amount", formatString: "F2")
            ]),
        new(
            "report-footer",
            ReportBandKind.ReportFooter,
            24,
            GroupId: null,
            Controls:
            [
                ReportLabel("footer-value-label", "report-footer", 566, 2, 120, 18, "Open Order Value", fontWeight: "600"),
                ReportCalculated("footer-value", "report-footer", 694, 2, 90, 18, "=SUM(total_amount)", "F2")
            ]),
        StandardPageFooter()
    ];

    return new Reports.ReportDefinition(
        ReportId: "open-order-queue",
        Name: "Open Order Queue",
        Source: new Reports.ReportSourceReference(source.Kind, source.Name),
        DefinitionVersion: 1,
        SourceSchemaSignature: source.SourceSchemaSignature,
        PageSettings: Reports.ReportPageSettings.DefaultLetterPortrait,
        Groups: [],
        Sorts: [new Reports.ReportSortDefinition("required_ship_date"), new Reports.ReportSortDefinition("priority_code", Descending: true)],
        Bands: bands);
}

static Reports.ReportBandDefinition StandardPageFooter()
    => new(
        "page-footer",
        ReportBandKind.PageFooter,
        22,
        GroupId: null,
        Controls:
        [
            ReportCalculated("footer-date", "page-footer", 24, 2, 180, 18, "=PrintDate", "g"),
            ReportCalculated("footer-page", "page-footer", 690, 2, 90, 18, "=PageNumber", null, "Page ", textAlign: "right")
        ]);

static Reports.ReportControlDefinition ReportLabel(
    string id,
    string bandId,
    double x,
    double y,
    double width,
    double height,
    string text,
    long? fontSize = null,
    string? fontWeight = null)
{
    var props = new Dictionary<string, object?> { ["text"] = text };
    if (fontSize.HasValue)
        props["fontSize"] = fontSize.Value;
    if (!string.IsNullOrWhiteSpace(fontWeight))
        props["fontWeight"] = fontWeight;

    return new Reports.ReportControlDefinition(
        id,
        ReportControlType.Label,
        bandId,
        new Reports.Rect(x, y, width, height),
        BoundFieldName: null,
        Expression: null,
        FormatString: null,
        Props: new Reports.PropertyBag(props));
}

static Reports.ReportControlDefinition ReportBoundText(
    string id,
    string bandId,
    double x,
    double y,
    double width,
    double height,
    string fieldName,
    string? formatString = null)
    => new(
        id,
        ReportControlType.BoundText,
        bandId,
        new Reports.Rect(x, y, width, height),
        BoundFieldName: fieldName,
        Expression: null,
        FormatString: formatString,
        Props: Reports.PropertyBag.Empty);

static Reports.ReportControlDefinition ReportCalculated(
    string id,
    string bandId,
    double x,
    double y,
    double width,
    double height,
    string expression,
    string? formatString,
    string? prefix = null,
    string? textAlign = null)
{
    var props = new Dictionary<string, object?>();
    if (!string.IsNullOrWhiteSpace(prefix))
        props["prefix"] = prefix;
    if (!string.IsNullOrWhiteSpace(textAlign))
        props["textAlign"] = textAlign;

    return new Reports.ReportControlDefinition(
        id,
        ReportControlType.CalculatedText,
        bandId,
        new Reports.Rect(x, y, width, height),
        BoundFieldName: null,
        Expression: expression,
        FormatString: formatString,
        Props: new Reports.PropertyBag(props));
}

file sealed record StoredProcedureFile(
    string Name,
    string BodySql,
    IReadOnlyList<StoredProcedureParameterFile> Parameters,
    string? Description,
    bool IsEnabled);

file sealed record StoredProcedureParameterFile(
    string Name,
    string Type,
    bool Required,
    JsonElement? Default,
    string? Description);

file sealed record SavedQueryFile(
    string Name,
    string SqlText);

file sealed record ScannerSessionDocument(
    string DeviceId,
    string WarehouseCode,
    string OperatorName,
    string SessionStatus,
    ScannerWaveState CurrentWave,
    string[] ScannedSkus,
    string[] Tags,
    string LastActivityUtc);

file sealed record ScannerWaveState(
    string OrderNumber,
    string Phase,
    string StartedUtc);

file sealed record WebhookArchiveDocument(
    string Provider,
    string EventType,
    string ReceivedUtc,
    WebhookHeaders Headers,
    string[] Tags,
    string PayloadJson);

file sealed record WebhookHeaders(
    string OrderNumber,
    string? ShipmentNumber);

file sealed record FullTextSeedSummary(
    IReadOnlyList<string> Hits);

file sealed record CollectionSeedSummary(
    int ScannerSessionCount,
    int WebhookCount,
    IReadOnlyList<string> ScannerSessionMatches,
    IReadOnlyList<string> WebhookTagMatches);

file sealed record PipelineSeedSummary(
    IReadOnlyList<string> RunSummaries,
    string ExportPath);
