using System.Reflection;
using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Forms.Pages;
using CSharpDB.Admin.Forms.Services;
using Microsoft.JSInterop;

namespace CSharpDB.Admin.Forms.Tests.Pages;

public sealed class DataEntryTests
{
    [Fact]
    public async Task GoToRecordAsync_EntersFocusedModeAndLoadsTargetWindow()
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

        Assert.True(GetField<bool>(component, "_isFocusedNavigation"));
        Assert.Equal(80L, ReadCurrentRecord(component)["Id"]);
        Assert.Contains(ReadRecords(component), row => Equals(row["Id"], 80L));
        Assert.Equal(1, recordService.GetRecordWindowCalls);
        Assert.Equal(1, recordService.ListRecordPageCalls);
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

        SetField(component, "_goToRecordValue", "20");
        await InvokeNonPublicAsync(component, "GoToRecordAsync");

        await InvokeNonPublicAsync(component, "NextRecord");
        Assert.Equal(21L, ReadCurrentRecord(component)["Id"]);
        Assert.Equal(0, recordService.GetAdjacentRecordCalls);

        for (int i = 0; i < 12; i++)
            await InvokeNonPublicAsync(component, "NextRecord");

        Assert.True(recordService.GetAdjacentRecordCalls > 0);
        Assert.Equal(33L, ReadCurrentRecord(component)["Id"]);
    }

    private static async Task<DataEntry> CreateComponentAsync(
        FormDefinition form,
        ISchemaProvider schemaProvider,
        IFormRecordService recordService)
    {
        var component = new DataEntry();
        SetProperty(component, "FormRepository", new StaticFormRepository(form));
        SetProperty(component, "RecordService", recordService);
        SetProperty(component, "SchemaProvider", schemaProvider);
        SetProperty(component, "ValidationService", new PassThroughValidationService());
        SetProperty(component, "JS", new StubJsRuntime());
        component.FormId = form.FormId;

        await InvokeNonPublicAsync(component, "OnParametersSetAsync");
        return component;
    }

    private static FormDefinition CreateForm(string formId, string tableName)
        => new(
            formId,
            $"{tableName} Form",
            tableName,
            DefinitionVersion: 1,
            SourceSchemaSignature: $"sig:{tableName}",
            Layout: new LayoutDefinition("absolute", 8, SnapToGrid: false, []),
            Controls: []);

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

    private static async Task InvokeNonPublicAsync(object instance, string methodName)
    {
        MethodInfo method = instance.GetType().GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Method '{methodName}' was not found.");
        var task = (Task?)method.Invoke(instance, null)
            ?? throw new InvalidOperationException($"Method '{methodName}' did not return a task.");
        await task;
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

        public Task<int?> GetRecordOrdinalAsync(FormTableDefinition table, object pkValue, CancellationToken ct = default)
            => inner.GetRecordOrdinalAsync(table, pkValue, ct);

        public Task<int?> GetRecordOrdinalAsync(FormTableDefinition table, object pkValue, string searchField, string searchValue, CancellationToken ct = default)
            => inner.GetRecordOrdinalAsync(table, pkValue, searchField, searchValue, ct);

        public Task<List<Dictionary<string, object?>>> ListFilteredRecordsAsync(FormTableDefinition table, string filterField, object? filterValue, CancellationToken ct = default)
            => inner.ListFilteredRecordsAsync(table, filterField, filterValue, ct);

        public Task<Dictionary<string, object?>> CreateRecordAsync(FormTableDefinition table, Dictionary<string, object?> values, CancellationToken ct = default)
            => inner.CreateRecordAsync(table, values, ct);

        public Task<Dictionary<string, object?>> UpdateRecordAsync(FormTableDefinition table, object pkValue, Dictionary<string, object?> values, CancellationToken ct = default)
            => inner.UpdateRecordAsync(table, pkValue, values, ct);

        public Task DeleteRecordAsync(FormTableDefinition table, object pkValue, CancellationToken ct = default)
            => inner.DeleteRecordAsync(table, pkValue, ct);
    }
}
