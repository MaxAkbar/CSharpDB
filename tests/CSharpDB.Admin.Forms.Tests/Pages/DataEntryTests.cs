using System.Reflection;
using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Forms.Pages;
using CSharpDB.Admin.Forms.Services;
using CSharpDB.Primitives;
using Microsoft.JSInterop;

namespace CSharpDB.Admin.Forms.Tests.Pages;

public sealed class DataEntryTests
{
    [Fact]
    public async Task GoToRecordAsync_LoadsContainingBrowsePageAndSelectsTargetRecord()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await db.ExecuteAsync(
            """
            CREATE TABLE Events (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            """);

        for (int i = 1; i <= 100; i++)
            await db.ExecuteAsync($"INSERT INTO Events VALUES ({i}, 'Event {i}')");

        var provider = new DbSchemaProvider(db.Client);
        var recordService = new CountingFormRecordService(new DbFormRecordService(db.Client));
        DataEntry component = await CreateComponentAsync(
            form: CreateForm("events-form", "Events"),
            schemaProvider: provider,
            recordService: recordService);

        SetField(component, "_goToRecordValue", "80");
        await InvokeNonPublicAsync(component, "GoToRecordAsync");

        Assert.False(GetField<bool>(component, "_isFocusedNavigation"));
        Assert.Equal(4, GetField<int>(component, "_page"));
        Assert.Equal(4, GetField<int>(component, "_recordPageIndex"));
        Assert.Equal(80L, ReadCurrentRecord(component)["Id"]);
        Assert.Equal([76L, 77L, 78L, 79L, 80L, 81L, 82L, 83L, 84L, 85L, 86L, 87L, 88L, 89L, 90L, 91L, 92L, 93L, 94L, 95L, 96L, 97L, 98L, 99L, 100L],
            ReadRecords(component).Select(row => (long)row["Id"]!).ToArray());
        Assert.Equal(0, recordService.GetRecordWindowCalls);
        Assert.Equal(1, recordService.GetRecordOrdinalCalls);
        Assert.Equal(2, recordService.ListRecordPageCalls);
    }

    [Fact]
    public async Task InitialRecordId_LoadsRequestedRecord()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await db.ExecuteAsync(
            """
            CREATE TABLE Events (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            INSERT INTO Events VALUES (1, 'Alpha');
            INSERT INTO Events VALUES (2, 'Beta');
            INSERT INTO Events VALUES (3, 'Gamma');
            """);

        var recordService = new CountingFormRecordService(new DbFormRecordService(db.Client));
        DataEntry component = await CreateComponentAsync(
            form: CreateForm("events-form", "Events"),
            schemaProvider: new DbSchemaProvider(db.Client),
            recordService: recordService,
            initialRecordId: 3L);

        Assert.Equal(3L, ReadCurrentRecord(component)["Id"]);
        Assert.Equal("Gamma", ReadCurrentRecord(component)["Name"]);
        Assert.Equal(1, recordService.GetRecordOrdinalCalls);
    }

    [Fact]
    public async Task SaveRecord_UpdatePatchesVisibleRowWithoutReloadingPage()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await db.ExecuteAsync(
            """
            CREATE TABLE Products (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            INSERT INTO Products VALUES (1, 'Widget');
            INSERT INTO Products VALUES (2, 'Gadget');
            """);

        var provider = new DbSchemaProvider(db.Client);
        var recordService = new CountingFormRecordService(new DbFormRecordService(db.Client));
        DataEntry component = await CreateComponentAsync(
            form: CreateForm("products-form", "Products"),
            schemaProvider: provider,
            recordService: recordService);

        Dictionary<string, object?> current = ReadCurrentRecord(component);
        current["Name"] = "Widget Pro";
        SetField(component, "_dirty", true);

        await InvokeNonPublicAsync(component, "SaveRecord");

        Assert.False(GetField<bool>(component, "_isFocusedNavigation"));
        Assert.Equal("Widget Pro", ReadCurrentRecord(component)["Name"]);
        Assert.Equal("Widget Pro", ReadRecords(component)[0]["Name"]);
        Assert.Equal(1, recordService.ListRecordPageCalls);
        Assert.Equal(0, recordService.GetRecordWindowCalls);
    }

    [Fact]
    public async Task SaveRecord_CreateEntersFocusedModeOnInsertedRow()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await db.ExecuteAsync(
            """
            CREATE TABLE Products (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            INSERT INTO Products VALUES (1, 'Widget');
            INSERT INTO Products VALUES (2, 'Gadget');
            """);

        var provider = new DbSchemaProvider(db.Client);
        var recordService = new CountingFormRecordService(new DbFormRecordService(db.Client));
        DataEntry component = await CreateComponentAsync(
            form: CreateForm("products-form", "Products"),
            schemaProvider: provider,
            recordService: recordService);

        InvokeNonPublic(component, "NewRecord");
        Dictionary<string, object?> current = ReadCurrentRecord(component);
        current["Id"] = 1001L;
        current["Name"] = "Created";
        SetField(component, "_dirty", true);

        await InvokeNonPublicAsync(component, "SaveRecord");

        Assert.True(GetField<bool>(component, "_isFocusedNavigation"));
        Assert.Equal(1001L, ReadCurrentRecord(component)["Id"]);
        Assert.Contains(ReadRecords(component), row => Equals(row["Id"], 1001L));
        Assert.Equal(1, recordService.GetRecordWindowCalls);
    }

    [Fact]
    public async Task SaveRecord_CreateDispatchesFormEvents()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await db.ExecuteAsync(
            """
            CREATE TABLE Products (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            """);

        var calls = new List<string>();
        DbCommandRegistry commands = DbCommandRegistry.Create(builder =>
        {
            builder.AddCommand("CaptureRecord", context =>
            {
                calls.Add($"{context.Metadata["event"]}:{context.Arguments["Name"].AsText}");
                Assert.Equal("products-form", context.Metadata["formId"]);
                Assert.Equal("Products", context.Metadata["tableName"]);
                return DbCommandResult.Success();
            });
        });

        FormDefinition form = CreateForm("products-form", "Products") with
        {
            EventBindings =
            [
                new FormEventBinding(FormEventKind.BeforeInsert, "CaptureRecord"),
                new FormEventBinding(FormEventKind.AfterInsert, "CaptureRecord"),
            ],
        };

        DataEntry component = await CreateComponentAsync(
            form,
            new DbSchemaProvider(db.Client),
            new DbFormRecordService(db.Client),
            new DefaultFormEventDispatcher(commands));

        InvokeNonPublic(component, "NewRecord");
        Dictionary<string, object?> current = ReadCurrentRecord(component);
        current["Id"] = 1001L;
        current["Name"] = "Created";
        SetField(component, "_dirty", true);

        await InvokeNonPublicAsync(component, "SaveRecord");

        Assert.Equal(["BeforeInsert:Created", "AfterInsert:Created"], calls);
        Assert.Null(GetField<string?>(component, "_error"));
        Assert.Equal("Created", ReadCurrentRecord(component)["Name"]);
    }

    [Fact]
    public async Task SaveRecord_BeforeInsertFailureCancelsCreate()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await db.ExecuteAsync(
            """
            CREATE TABLE Products (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            """);

        DbCommandRegistry commands = DbCommandRegistry.Create(builder =>
            builder.AddCommand("RejectCreate", static _ => DbCommandResult.Failure("Create blocked by command.")));

        FormDefinition form = CreateForm("products-form", "Products") with
        {
            EventBindings = [new FormEventBinding(FormEventKind.BeforeInsert, "RejectCreate")],
        };

        DataEntry component = await CreateComponentAsync(
            form,
            new DbSchemaProvider(db.Client),
            new DbFormRecordService(db.Client),
            new DefaultFormEventDispatcher(commands));

        InvokeNonPublic(component, "NewRecord");
        Dictionary<string, object?> current = ReadCurrentRecord(component);
        current["Id"] = 1001L;
        current["Name"] = "Created";
        SetField(component, "_dirty", true);

        await InvokeNonPublicAsync(component, "SaveRecord");

        Assert.Equal("Create blocked by command.", GetField<string?>(component, "_error"));
        Assert.Empty(await db.QueryRowsAsync("SELECT * FROM Products"));
        Assert.True(GetField<bool>(component, "_isNew"));
    }

    [Fact]
    public async Task SaveAndDeleteRecord_DispatchUpdateAndDeleteEvents()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await db.ExecuteAsync(
            """
            CREATE TABLE Products (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            INSERT INTO Products VALUES (1, 'Widget');
            """);

        var calls = new List<string>();
        DbCommandRegistry commands = DbCommandRegistry.Create(builder =>
        {
            builder.AddCommand("CaptureRecord", context =>
            {
                calls.Add($"{context.Metadata["event"]}:{context.Arguments["Name"].AsText}");
                return DbCommandResult.Success();
            });
        });

        FormDefinition form = CreateForm("products-form", "Products") with
        {
            EventBindings =
            [
                new FormEventBinding(FormEventKind.BeforeUpdate, "CaptureRecord"),
                new FormEventBinding(FormEventKind.AfterUpdate, "CaptureRecord"),
                new FormEventBinding(FormEventKind.BeforeDelete, "CaptureRecord"),
                new FormEventBinding(FormEventKind.AfterDelete, "CaptureRecord"),
            ],
        };

        DataEntry component = await CreateComponentAsync(
            form,
            new DbSchemaProvider(db.Client),
            new DbFormRecordService(db.Client),
            new DefaultFormEventDispatcher(commands));

        Dictionary<string, object?> current = ReadCurrentRecord(component);
        current["Name"] = "Widget Pro";
        SetField(component, "_dirty", true);

        await InvokeNonPublicAsync(component, "SaveRecord");
        await InvokeNonPublicAsync(component, "DeleteRecord");

        Assert.Equal(
            [
                "BeforeUpdate:Widget Pro",
                "AfterUpdate:Widget Pro",
                "BeforeDelete:Widget Pro",
                "AfterDelete:Widget Pro",
            ],
            calls);
        Assert.Empty(await db.QueryRowsAsync("SELECT * FROM Products"));
    }

    [Fact]
    public async Task FocusedNavigation_NextRecordMovesWithinWindowAndAcrossWindowEdge()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await db.ExecuteAsync(
            """
            CREATE TABLE Events (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            """);

        for (int i = 1; i <= 40; i++)
            await db.ExecuteAsync($"INSERT INTO Events VALUES ({i}, 'Event {i}')");

        var provider = new DbSchemaProvider(db.Client);
        var recordService = new CountingFormRecordService(new DbFormRecordService(db.Client));
        DataEntry component = await CreateComponentAsync(
            form: CreateForm("events-form", "Events"),
            schemaProvider: provider,
            recordService: recordService);

        await InvokeNonPublicAsync(component, "LoadFocusedRecordWindowAsync", 20L);

        await InvokeNonPublicAsync(component, "NextRecord");
        Assert.Equal(21L, ReadCurrentRecord(component)["Id"]);
        Assert.Equal(0, recordService.GetAdjacentRecordCalls);

        for (int i = 0; i < 12; i++)
            await InvokeNonPublicAsync(component, "NextRecord");

        Assert.True(recordService.GetAdjacentRecordCalls > 0);
        Assert.Equal(33L, ReadCurrentRecord(component)["Id"]);
    }

    [Fact]
    public async Task BuiltInFormActions_NavigateRecordsAndGoToRecord()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await db.ExecuteAsync(
            """
            CREATE TABLE Events (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            INSERT INTO Events VALUES (1, 'Event 1');
            INSERT INTO Events VALUES (2, 'Event 2');
            INSERT INTO Events VALUES (3, 'Event 3');
            """);

        DataEntry component = await CreateComponentAsync(
            form: CreateForm("events-form", "Events"),
            schemaProvider: new DbSchemaProvider(db.Client),
            recordService: new DbFormRecordService(db.Client));

        FormEventDispatchResult result = await InvokeNonPublicAsync<FormEventDispatchResult>(
            component,
            "ExecuteBuiltInFormActionAsync",
            new DbActionStep(DbActionKind.NextRecord),
            CancellationToken.None);

        Assert.True(result.Succeeded);
        Assert.Equal(2L, ReadCurrentRecord(component)["Id"]);

        result = await InvokeNonPublicAsync<FormEventDispatchResult>(
            component,
            "ExecuteBuiltInFormActionAsync",
            new DbActionStep(DbActionKind.PreviousRecord),
            CancellationToken.None);

        Assert.True(result.Succeeded);
        Assert.Equal(1L, ReadCurrentRecord(component)["Id"]);

        result = await InvokeNonPublicAsync<FormEventDispatchResult>(
            component,
            "ExecuteBuiltInFormActionAsync",
            new DbActionStep(DbActionKind.GoToRecord, Value: 3L),
            CancellationToken.None);

        Assert.True(result.Succeeded);
        Assert.Equal(3L, ReadCurrentRecord(component)["Id"]);
    }

    [Fact]
    public async Task BuiltInFormActions_CreateSaveRefreshAndDelete()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await db.ExecuteAsync(
            """
            CREATE TABLE Products (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            INSERT INTO Products VALUES (1, 'Widget');
            """);

        DataEntry component = await CreateComponentAsync(
            form: CreateForm("products-form", "Products"),
            schemaProvider: new DbSchemaProvider(db.Client),
            recordService: new DbFormRecordService(db.Client));

        FormEventDispatchResult result = await InvokeNonPublicAsync<FormEventDispatchResult>(
            component,
            "ExecuteBuiltInFormActionAsync",
            new DbActionStep(DbActionKind.NewRecord),
            CancellationToken.None);

        Assert.True(result.Succeeded);
        Assert.True(GetField<bool>(component, "_isNew"));

        Dictionary<string, object?> current = ReadCurrentRecord(component);
        current["Id"] = 2L;
        current["Name"] = "Gadget";
        SetField(component, "_dirty", true);

        result = await InvokeNonPublicAsync<FormEventDispatchResult>(
            component,
            "ExecuteBuiltInFormActionAsync",
            new DbActionStep(DbActionKind.SaveRecord),
            CancellationToken.None);

        Assert.True(result.Succeeded);
        Assert.Equal(2L, ReadCurrentRecord(component)["Id"]);
        Assert.Equal(2, (await db.QueryRowsAsync("SELECT * FROM Products")).Count);

        await db.ExecuteAsync("UPDATE Products SET Name = 'Gadget Pro' WHERE Id = 2");
        result = await InvokeNonPublicAsync<FormEventDispatchResult>(
            component,
            "ExecuteBuiltInFormActionAsync",
            new DbActionStep(DbActionKind.RefreshRecords),
            CancellationToken.None);

        Assert.True(result.Succeeded);
        Assert.Equal("Gadget Pro", ReadCurrentRecord(component)["Name"]);

        result = await InvokeNonPublicAsync<FormEventDispatchResult>(
            component,
            "ExecuteBuiltInFormActionAsync",
            new DbActionStep(DbActionKind.DeleteRecord),
            CancellationToken.None);

        Assert.True(result.Succeeded);
        Assert.Single(await db.QueryRowsAsync("SELECT * FROM Products"));
    }

    [Fact]
    public async Task Phase8Runtime_AppliesAndClearsFormFilter()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await db.ExecuteAsync(
            """
            CREATE TABLE Products (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL,
                Status TEXT NOT NULL
            );
            INSERT INTO Products VALUES (1, 'Widget', 'Open');
            INSERT INTO Products VALUES (2, 'Gadget', 'Closed');
            INSERT INTO Products VALUES (3, 'Sprocket', 'Open');
            """);

        DataEntry component = await CreateComponentAsync(
            form: CreateForm("products-form", "Products"),
            schemaProvider: new DbSchemaProvider(db.Client),
            recordService: new DbFormRecordService(db.Client));

        FormEventDispatchResult result = await ((IFormActionRuntime)component).ApplyFilterAsync(
            CreateRuntimeContext(component),
            "form",
            "[Status] = @status AND [Id] > 1",
            new Dictionary<string, object?>
            {
                ["parameters"] = new Dictionary<string, object?> { ["status"] = "Open" },
            },
            CancellationToken.None);

        Assert.True(result.Succeeded);
        Assert.Equal([3L], ReadRecords(component).Select(row => (long)row["Id"]!).ToArray());

        result = await ((IFormActionRuntime)component).ClearFilterAsync(
            CreateRuntimeContext(component),
            "form",
            CancellationToken.None);

        Assert.True(result.Succeeded);
        Assert.Equal([1L, 2L, 3L], ReadRecords(component).Select(row => (long)row["Id"]!).ToArray());
    }

    [Fact]
    public async Task Phase8Runtime_AppliesAndClearsDataGridFilter()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await db.ExecuteAsync(
            """
            CREATE TABLE Products (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            CREATE TABLE Orders (
                OrderId INTEGER PRIMARY KEY,
                ProductId INTEGER NOT NULL,
                Status TEXT NOT NULL
            );
            INSERT INTO Products VALUES (1, 'Widget');
            INSERT INTO Orders VALUES (101, 1, 'Open');
            """);

        ControlDefinition grid = new(
            "ordersGrid",
            "datagrid",
            new Rect(0, 0, 320, 160),
            Binding: null,
            Props: new PropertyBag(new Dictionary<string, object?>
            {
                ["childTable"] = "Orders",
                ["dataGridMode"] = "related",
                ["foreignKeyField"] = "ProductId",
                ["parentKeyField"] = "Id",
            }),
            ValidationOverride: null);
        DataEntry component = await CreateComponentAsync(
            form: CreateForm("products-form", "Products", [grid]),
            schemaProvider: new DbSchemaProvider(db.Client),
            recordService: new DbFormRecordService(db.Client));

        FormEventDispatchResult result = await ((IFormActionRuntime)component).ApplyFilterAsync(
            CreateRuntimeContext(component),
            "ordersGrid",
            "[Status] = @status",
            new Dictionary<string, object?> { ["status"] = "Open" },
            CancellationToken.None);

        Assert.True(result.Succeeded);
        var filters = GetField<Dictionary<string, ControlFilterState>>(component, "_controlFilters");
        ControlFilterState filter = Assert.Single(filters).Value;
        Assert.Equal("[Status] = @status", filter.FilterExpression);
        Assert.Equal("Open", filter.Parameters["status"]);

        result = await ((IFormActionRuntime)component).ClearFilterAsync(
            CreateRuntimeContext(component),
            "ordersGrid",
            CancellationToken.None);

        Assert.True(result.Succeeded);
        Assert.Empty(filters);
    }

    [Fact]
    public async Task Phase8Runtime_SetsControlPropertyAndBoundValue()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await db.ExecuteAsync(
            """
            CREATE TABLE Products (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            INSERT INTO Products VALUES (1, 'Widget');
            """);

        ControlDefinition control = new(
            "nameBox",
            "text",
            new Rect(0, 0, 120, 32),
            new BindingDefinition("Name", "TwoWay"),
            PropertyBag.Empty,
            ValidationOverride: null);
        DataEntry component = await CreateComponentAsync(
            form: CreateForm("products-form", "Products", [control]),
            schemaProvider: new DbSchemaProvider(db.Client),
            recordService: new DbFormRecordService(db.Client));

        FormEventDispatchResult result = await ((IFormActionRuntime)component).SetControlPropertyAsync(
            CreateRuntimeContext(component),
            "nameBox",
            "visible",
            false,
            CancellationToken.None);

        Assert.True(result.Succeeded);
        var overrides = GetField<Dictionary<string, IReadOnlyDictionary<string, object?>>>(component, "_controlPropertyOverrides");
        Assert.False(Assert.IsType<bool>(overrides["nameBox"]["visible"]));

        result = await ((IFormActionRuntime)component).SetControlPropertyAsync(
            CreateRuntimeContext(component),
            "nameBox",
            "value",
            "Widget Pro",
            CancellationToken.None);

        Assert.True(result.Succeeded);
        Assert.Equal("Widget Pro", ReadCurrentRecord(component)["Name"]);
        Assert.True(GetField<bool>(component, "_dirty"));
    }

    [Fact]
    public async Task Phase8Runtime_RunSqlRequiresHostOptInAndRefreshesRecord()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await db.ExecuteAsync(
            """
            CREATE TABLE Products (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL,
                Status TEXT NOT NULL
            );
            INSERT INTO Products VALUES (1, 'Widget', 'Open');
            """);

        DataEntry component = await CreateComponentAsync(
            form: CreateForm("products-form", "Products"),
            schemaProvider: new DbSchemaProvider(db.Client),
            recordService: new DbFormRecordService(db.Client));
        SetProperty(component, nameof(DataEntry.DbClient), db.Client);

        FormEventDispatchResult result = await ((IFormActionRuntime)component).RunSqlAsync(
            CreateRuntimeContext(component),
            "UPDATE Products SET Status = @status WHERE Id = @id",
            new Dictionary<string, object?> { ["status"] = "Closed", ["id"] = 1L },
            CancellationToken.None);

        Assert.False(result.Succeeded);
        Assert.Contains("disabled", result.Message, StringComparison.OrdinalIgnoreCase);

        SetProperty(component, nameof(DataEntry.EnableSqlActions), true);
        result = await ((IFormActionRuntime)component).RunSqlAsync(
            CreateRuntimeContext(component),
            "UPDATE Products SET Status = @status WHERE Id = @id",
            new Dictionary<string, object?> { ["status"] = "Closed", ["id"] = 1L },
            CancellationToken.None);

        Assert.True(result.Succeeded);
        Assert.Equal("Closed", ReadCurrentRecord(component)["Status"]);
        Assert.Equal("Closed", (await db.QueryRowsAsync("SELECT Status FROM Products WHERE Id = 1"))[0]["Status"]);
    }

    [Fact]
    public async Task ViewBackedForm_LoadsAndSearchesInReadOnlyMode()
    {
        await using var db = await TestDatabaseScope.CreateAsync();
        await db.ExecuteAsync(
            """
            CREATE TABLE Events (
                Id INTEGER PRIMARY KEY,
                Name TEXT NOT NULL
            );
            INSERT INTO Events VALUES (1, 'Alpha');
            INSERT INTO Events VALUES (2, 'Beta');
            INSERT INTO Events VALUES (3, 'Gamma');
            CREATE VIEW EventNames AS
            SELECT Name
            FROM Events;
            """);

        var provider = new DbSchemaProvider(db.Client);
        var recordService = new CountingFormRecordService(new DbFormRecordService(db.Client));
        DataEntry component = await CreateComponentAsync(
            form: CreateForm("event-names-form", "EventNames"),
            schemaProvider: provider,
            recordService: recordService);

        Assert.False(GetField<bool>(component, "_isNew"));
        Assert.Equal(["Alpha", "Beta", "Gamma"], ReadRecords(component).Select(row => Assert.IsType<string>(row["Name"])).ToArray());

        SetField(component, "_searchColumnName", "Name");
        SetField(component, "_searchValue", "mm");
        await InvokeNonPublicAsync(component, "ApplySearchAsync");

        Assert.Equal(["Gamma"], ReadRecords(component).Select(row => Assert.IsType<string>(row["Name"])).ToArray());

        SetField(component, "_goToRecordValue", "1");
        await InvokeNonPublicAsync(component, "GoToRecordAsync");

        Assert.Equal("This form source does not expose a single primary key column.", GetField<string?>(component, "_error"));
        Assert.Equal(0, recordService.GetRecordOrdinalCalls);
    }

    private static async Task<DataEntry> CreateComponentAsync(
        FormDefinition form,
        ISchemaProvider schemaProvider,
        IFormRecordService recordService,
        IFormEventDispatcher? formEvents = null,
        object? initialRecordId = null,
        string? initialMode = null,
        string? initialFilterExpression = null,
        IReadOnlyDictionary<string, object?>? initialFilterParameters = null)
    {
        var component = new DataEntry();
        SetProperty(component, "FormRepository", new StaticFormRepository(form));
        SetProperty(component, "RecordService", recordService);
        SetProperty(component, "SchemaProvider", schemaProvider);
        SetProperty(component, "ValidationService", new PassThroughValidationService());
        SetProperty(component, "FormEvents", formEvents ?? NullFormEventDispatcher.Instance);
        SetProperty(component, "JS", new StubJsRuntime());
        SetProperty(component, nameof(DataEntry.FormId), form.FormId);
        SetProperty(component, nameof(DataEntry.InitialRecordId), initialRecordId);
        SetProperty(component, nameof(DataEntry.InitialMode), initialMode);
        SetProperty(component, nameof(DataEntry.InitialFilterExpression), initialFilterExpression);
        SetProperty(component, nameof(DataEntry.InitialFilterParameters), initialFilterParameters);

        await InvokeNonPublicAsync(component, "OnParametersSetAsync");
        return component;
    }

    private static FormDefinition CreateForm(
        string formId,
        string tableName,
        IReadOnlyList<ControlDefinition>? controls = null)
        => new(
            formId,
            $"{tableName} Form",
            tableName,
            DefinitionVersion: 1,
            SourceSchemaSignature: $"sig:{tableName}",
            Layout: new LayoutDefinition("absolute", 8, SnapToGrid: false, []),
            Controls: controls ?? []);

    private static FormActionRuntimeContext CreateRuntimeContext(DataEntry component)
        => new(
            FormId: component.FormId,
            FormName: null,
            TableName: null,
            EventName: "Test",
            ActionSequenceName: "TestActions",
            StepIndex: 0,
            Record: ReadCurrentRecord(component),
            BindingArguments: null,
            RuntimeArguments: null,
            StepArguments: null,
            Metadata: new Dictionary<string, string>());

    private static Dictionary<string, object?> ReadCurrentRecord(DataEntry component)
        => GetField<Dictionary<string, object?>>(component, "_currentRecord");

    private static List<Dictionary<string, object?>> ReadRecords(DataEntry component)
        => GetField<List<Dictionary<string, object?>>>(component, "_records");

    private static T GetField<T>(object instance, string fieldName)
    {
        FieldInfo field = instance.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Field '{fieldName}' was not found.");
        return (T)field.GetValue(instance)!;
    }

    private static void SetField(object instance, string fieldName, object? value)
    {
        FieldInfo field = instance.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Field '{fieldName}' was not found.");
        field.SetValue(instance, value);
    }

    private static void SetProperty(object instance, string propertyName, object? value)
    {
        PropertyInfo property = instance.GetType().GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Property '{propertyName}' was not found.");
        property.SetValue(instance, value);
    }

    private static void InvokeNonPublic(object instance, string methodName)
    {
        MethodInfo method = instance.GetType().GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Method '{methodName}' was not found.");
        _ = method.Invoke(instance, null);
    }

    private static async Task InvokeNonPublicAsync(object instance, string methodName, params object?[]? args)
    {
        MethodInfo method = instance.GetType().GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Method '{methodName}' was not found.");
        var task = (Task?)method.Invoke(instance, args)
            ?? throw new InvalidOperationException($"Method '{methodName}' did not return a task.");
        await task;
    }

    private static async Task<T> InvokeNonPublicAsync<T>(object instance, string methodName, params object?[]? args)
    {
        MethodInfo method = instance.GetType().GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Method '{methodName}' was not found.");
        var task = (Task<T>?)method.Invoke(instance, args)
            ?? throw new InvalidOperationException($"Method '{methodName}' did not return a task.");
        return await task;
    }

    private sealed class StaticFormRepository(FormDefinition form) : IFormRepository
    {
        public Task<FormDefinition?> GetAsync(string formId)
            => Task.FromResult<FormDefinition?>(string.Equals(formId, form.FormId, StringComparison.OrdinalIgnoreCase) ? form : null);

        public Task<FormDefinition> CreateAsync(FormDefinition value) => throw new NotSupportedException();
        public Task<UpdateResult> TryUpdateAsync(string formId, int expectedVersion, FormDefinition updated) => throw new NotSupportedException();
        public Task<IReadOnlyList<FormDefinition>> ListAsync(string? tableName = null) => throw new NotSupportedException();
        public Task<bool> DeleteAsync(string formId) => throw new NotSupportedException();
    }

    private sealed class PassThroughValidationService : IValidationInferenceService
    {
        public IReadOnlyList<ValidationRule> InferRules(FormFieldDefinition field) => [];
        public IReadOnlyList<ValidationError> Evaluate(FormDefinition form, IDictionary<string, object?> record) => [];
        public Task<IReadOnlyList<ValidationError>> EvaluateAsync(FormDefinition form, IDictionary<string, object?> record, CancellationToken ct = default)
            => Task.FromResult<IReadOnlyList<ValidationError>>([]);
    }

    private sealed class StubJsRuntime : IJSRuntime
    {
        public ValueTask<TValue> InvokeAsync<TValue>(string identifier, object?[]? args)
            => ValueTask.FromResult(default(TValue)!);

        public ValueTask<TValue> InvokeAsync<TValue>(string identifier, CancellationToken cancellationToken, object?[]? args)
            => ValueTask.FromResult(default(TValue)!);
    }

    private sealed class CountingFormRecordService(IFormRecordService inner) : IFormRecordService
    {
        public int ListRecordPageCalls { get; private set; }
        public int GetRecordWindowCalls { get; private set; }
        public int GetAdjacentRecordCalls { get; private set; }
        public int GetRecordOrdinalCalls { get; private set; }

        public string GetPrimaryKeyColumn(FormTableDefinition table) => inner.GetPrimaryKeyColumn(table);

        public Task<Dictionary<string, object?>?> GetRecordAsync(FormTableDefinition table, object pkValue, CancellationToken ct = default)
            => inner.GetRecordAsync(table, pkValue, ct);

        public async Task<FormRecordWindow?> GetRecordWindowAsync(FormTableDefinition table, object pkValue, int pageSize, CancellationToken ct = default)
        {
            GetRecordWindowCalls++;
            return await inner.GetRecordWindowAsync(table, pkValue, pageSize, ct);
        }

        public async Task<Dictionary<string, object?>?> GetAdjacentRecordAsync(FormTableDefinition table, object pkValue, bool previous, CancellationToken ct = default)
        {
            GetAdjacentRecordCalls++;
            return await inner.GetAdjacentRecordAsync(table, pkValue, previous, ct);
        }

        public async Task<FormRecordPage> ListRecordPageAsync(FormTableDefinition table, int pageNumber, int pageSize, CancellationToken ct = default)
        {
            ListRecordPageCalls++;
            return await inner.ListRecordPageAsync(table, pageNumber, pageSize, ct);
        }

        public Task<FormRecordPage> SearchRecordPageAsync(FormTableDefinition table, string searchField, string searchValue, int pageNumber, int pageSize, CancellationToken ct = default)
            => inner.SearchRecordPageAsync(table, searchField, searchValue, pageNumber, pageSize, ct);

        public Task<List<Dictionary<string, object?>>> ListRecordsAsync(FormTableDefinition table, CancellationToken ct = default)
            => inner.ListRecordsAsync(table, ct);

        public async Task<int?> GetRecordOrdinalAsync(FormTableDefinition table, object pkValue, CancellationToken ct = default)
        {
            GetRecordOrdinalCalls++;
            return await inner.GetRecordOrdinalAsync(table, pkValue, ct);
        }

        public async Task<int?> GetRecordOrdinalAsync(FormTableDefinition table, object pkValue, string searchField, string searchValue, CancellationToken ct = default)
        {
            GetRecordOrdinalCalls++;
            return await inner.GetRecordOrdinalAsync(table, pkValue, searchField, searchValue, ct);
        }

        public Task<List<Dictionary<string, object?>>> ListFilteredRecordsAsync(FormTableDefinition table, string filterField, object? filterValue, CancellationToken ct = default)
            => inner.ListFilteredRecordsAsync(table, filterField, filterValue, ct);

        public Task<Dictionary<string, object?>> CreateRecordAsync(FormTableDefinition table, Dictionary<string, object?> values, CancellationToken ct = default)
            => inner.CreateRecordAsync(table, values, ct);

        public Task<Dictionary<string, object?>> UpdateRecordAsync(FormTableDefinition table, object pkValue, Dictionary<string, object?> values, CancellationToken ct = default)
            => inner.UpdateRecordAsync(table, pkValue, values, ct);

        public Task SaveAttachmentAsync(FormAttachmentTableBinding binding, object parentValue, FormAttachmentValue attachment, CancellationToken ct = default)
            => inner.SaveAttachmentAsync(binding, parentValue, attachment, ct);

        public Task DeleteRecordAsync(FormTableDefinition table, object pkValue, CancellationToken ct = default)
            => inner.DeleteRecordAsync(table, pkValue, ct);
    }
}
