using System.Reflection;
using CSharpDB.Admin.Forms.Components.Designer;
using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;

namespace CSharpDB.Admin.Forms.Tests.Components.Designer;

public sealed class ChildDataGridTests
{
    [Fact]
    public async Task OnParametersSetAsync_ReloadsWhenChildTableChangesForSameParentKey()
    {
        var service = new TableSpecificRecordService();
        var component = new ChildDataGrid();
        SetProperty(component, "RecordService", service);

        SetProperty(component, nameof(ChildDataGrid.ChildTableName), "Orders");
        SetProperty(component, nameof(ChildDataGrid.ForeignKeyField), "CustomerId");
        SetProperty(component, nameof(ChildDataGrid.ParentKeyValue), 7L);
        SetProperty(component, nameof(ChildDataGrid.ChildFormTableDefinition), CreateTableDefinition("Orders", "OrderId", "CustomerId"));

        await InvokeNonPublicAsync(component, "OnParametersSetAsync");

        Assert.Equal(["Orders"], service.RequestedTables);
        Assert.Equal([101L], ReadRows(component).Select(row => (long)row["OrderId"]!).ToArray());

        SetProperty(component, nameof(ChildDataGrid.ChildTableName), "Payments");
        SetProperty(component, nameof(ChildDataGrid.ForeignKeyField), "CustomerId");
        SetProperty(component, nameof(ChildDataGrid.ParentKeyValue), 7L);
        SetProperty(component, nameof(ChildDataGrid.ChildFormTableDefinition), CreateTableDefinition("Payments", "PaymentId", "CustomerId"));

        await InvokeNonPublicAsync(component, "OnParametersSetAsync");

        Assert.Equal(["Orders", "Payments"], service.RequestedTables);
        Assert.Equal([201L], ReadRows(component).Select(row => (long)row["PaymentId"]!).ToArray());
    }

    private static FormTableDefinition CreateTableDefinition(string tableName, string primaryKeyField, string foreignKeyField)
        => new(
            tableName,
            $"sig:{tableName}",
            [
                new FormFieldDefinition(primaryKeyField, FieldDataType.Int64, false, false),
                new FormFieldDefinition(foreignKeyField, FieldDataType.Int64, false, false)
            ],
            [primaryKeyField],
            []);

    private static List<Dictionary<string, object?>> ReadRows(ChildDataGrid component)
        => GetField<List<Dictionary<string, object?>>>(component, "_rows");

    private static T GetField<T>(object instance, string fieldName)
    {
        FieldInfo field = instance.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Field '{fieldName}' was not found.");
        return (T)field.GetValue(instance)!;
    }

    private static void SetProperty(object instance, string propertyName, object? value)
    {
        PropertyInfo property = instance.GetType().GetProperty(propertyName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Property '{propertyName}' was not found.");
        property.SetValue(instance, value);
    }

    private static async Task InvokeNonPublicAsync(object instance, string methodName)
    {
        MethodInfo method = instance.GetType().GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException($"Method '{methodName}' was not found.");
        var task = (Task?)method.Invoke(instance, null)
            ?? throw new InvalidOperationException($"Method '{methodName}' did not return a task.");
        await task;
    }

    private sealed class TableSpecificRecordService : IFormRecordService
    {
        public List<string> RequestedTables { get; } = [];

        public string GetPrimaryKeyColumn(FormTableDefinition table) => table.PrimaryKey[0];

        public Task<Dictionary<string, object?>?> GetRecordAsync(FormTableDefinition table, object pkValue, CancellationToken ct = default)
            => Task.FromResult<Dictionary<string, object?>?>(null);

        public Task<FormRecordWindow?> GetRecordWindowAsync(FormTableDefinition table, object pkValue, int pageSize, CancellationToken ct = default)
            => Task.FromResult<FormRecordWindow?>(null);

        public Task<Dictionary<string, object?>?> GetAdjacentRecordAsync(FormTableDefinition table, object pkValue, bool previous, CancellationToken ct = default)
            => Task.FromResult<Dictionary<string, object?>?>(null);

        public Task<FormRecordPage> ListRecordPageAsync(FormTableDefinition table, int pageNumber, int pageSize, CancellationToken ct = default)
            => throw new NotSupportedException();

        public Task<FormRecordPage> SearchRecordPageAsync(FormTableDefinition table, string searchField, string searchValue, int pageNumber, int pageSize, CancellationToken ct = default)
            => throw new NotSupportedException();

        public Task<List<Dictionary<string, object?>>> ListRecordsAsync(FormTableDefinition table, CancellationToken ct = default)
            => throw new NotSupportedException();

        public Task<int?> GetRecordOrdinalAsync(FormTableDefinition table, object pkValue, CancellationToken ct = default)
            => throw new NotSupportedException();

        public Task<int?> GetRecordOrdinalAsync(FormTableDefinition table, object pkValue, string searchField, string searchValue, CancellationToken ct = default)
            => throw new NotSupportedException();

        public Task<List<Dictionary<string, object?>>> ListFilteredRecordsAsync(FormTableDefinition table, string filterField, object? filterValue, CancellationToken ct = default)
        {
            RequestedTables.Add(table.TableName);

            List<Dictionary<string, object?>> rows = table.TableName switch
            {
                "Orders" =>
                [
                    new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["OrderId"] = 101L,
                        [filterField] = filterValue
                    }
                ],
                "Payments" =>
                [
                    new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["PaymentId"] = 201L,
                        [filterField] = filterValue
                    }
                ],
                _ => []
            };

            return Task.FromResult(rows);
        }

        public Task<Dictionary<string, object?>> CreateRecordAsync(FormTableDefinition table, Dictionary<string, object?> values, CancellationToken ct = default)
            => throw new NotSupportedException();

        public Task<Dictionary<string, object?>> UpdateRecordAsync(FormTableDefinition table, object pkValue, Dictionary<string, object?> values, CancellationToken ct = default)
            => throw new NotSupportedException();

        public Task DeleteRecordAsync(FormTableDefinition table, object pkValue, CancellationToken ct = default)
            => throw new NotSupportedException();
    }
}
