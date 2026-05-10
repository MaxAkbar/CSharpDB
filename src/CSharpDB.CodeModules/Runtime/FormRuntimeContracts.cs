using System.Collections.ObjectModel;
using System.Dynamic;
using CSharpDB.Primitives;

namespace CSharpDB.CodeModules.Runtime;

public abstract class FormCodeModule
{
    private FormCodeModuleRuntimeContext? _runtime;

    public dynamic Me => _runtime?.Record
        ?? throw new InvalidOperationException("The form code module has not been initialized.");

    public IFormCommandApi DoCmd => _runtime?.Commands
        ?? throw new InvalidOperationException("The form code module has not been initialized.");

    public FormEventContext CurrentEvent => _runtime?.EventContext
        ?? throw new InvalidOperationException("The form code module has not been initialized.");

    public void Initialize(FormCodeModuleRuntimeContext runtime)
    {
        ArgumentNullException.ThrowIfNull(runtime);
        _runtime = runtime;
    }
}

public sealed record FormCodeModuleRuntimeContext(
    FormModuleRecord Record,
    IFormCommandApi Commands,
    FormEventContext EventContext);

public class FormEventContext
{
    public FormEventContext(
        string? formId,
        string? formName,
        string? tableName,
        string eventName,
        IReadOnlyDictionary<string, object?>? arguments,
        IReadOnlyDictionary<string, string>? metadata)
    {
        FormId = formId;
        FormName = formName;
        TableName = tableName;
        EventName = eventName;
        Arguments = arguments ?? EmptyObjectDictionary.Instance;
        Metadata = metadata ?? EmptyStringDictionary.Instance;
    }

    public string? FormId { get; }
    public string? FormName { get; }
    public string? TableName { get; }
    public string EventName { get; }
    public IReadOnlyDictionary<string, object?> Arguments { get; }
    public IReadOnlyDictionary<string, string> Metadata { get; }
    public bool Canceled { get; private set; }
    public string? Message { get; private set; }

    public void Cancel(string? message = null)
    {
        Canceled = true;
        Message = message;
    }

    public void SetMessage(string message)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(message);
        Message = message;
    }
}

public sealed class FormBeforeEventContext : FormEventContext
{
    public FormBeforeEventContext(
        string? formId,
        string? formName,
        string? tableName,
        string eventName,
        IReadOnlyDictionary<string, object?>? arguments,
        IReadOnlyDictionary<string, string>? metadata)
        : base(formId, formName, tableName, eventName, arguments, metadata)
    {
    }
}

public sealed class FormControlEventContext : FormEventContext
{
    public FormControlEventContext(
        string? formId,
        string? formName,
        string? tableName,
        string eventName,
        string controlId,
        string? controlType,
        IReadOnlyDictionary<string, object?>? arguments,
        IReadOnlyDictionary<string, string>? metadata)
        : base(formId, formName, tableName, eventName, arguments, metadata)
    {
        ControlId = controlId;
        ControlType = controlType;
    }

    public string ControlId { get; }
    public string? ControlType { get; }
}

public interface IFormCommandApi
{
    ValueTask SetFieldAsync(string fieldName, object? value, CancellationToken ct = default);

    ValueTask<FormCommandApiResult> ShowMessageAsync(string message, CancellationToken ct = default);

    ValueTask<FormCommandApiResult> RunActionSequenceAsync(
        string sequenceName,
        IReadOnlyDictionary<string, object?>? arguments = null,
        CancellationToken ct = default);

    ValueTask<DbCommandResult> RunHostCommandAsync(
        string commandName,
        IReadOnlyDictionary<string, object?>? arguments = null,
        CancellationToken ct = default);

    ValueTask<FormCommandApiResult> SaveRecordAsync(CancellationToken ct = default);

    ValueTask<FormCommandApiResult> NewRecordAsync(CancellationToken ct = default);

    ValueTask<FormCommandApiResult> RefreshRecordsAsync(CancellationToken ct = default);

    ValueTask<FormCommandApiResult> OpenFormAsync(
        string formName,
        IReadOnlyDictionary<string, object?>? arguments = null,
        CancellationToken ct = default);

    ValueTask<FormCommandApiResult> CloseFormAsync(string? formName = null, CancellationToken ct = default);

    ValueTask<FormCommandApiResult> ApplyFilterAsync(
        string filter,
        string target = "form",
        IReadOnlyDictionary<string, object?>? arguments = null,
        CancellationToken ct = default);

    ValueTask<FormCommandApiResult> ClearFilterAsync(string target = "form", CancellationToken ct = default);
}

public sealed record FormCommandApiResult(bool Succeeded, string? Message = null)
{
    public static FormCommandApiResult Success(string? message = null) => new(true, message);

    public static FormCommandApiResult Failure(string message)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(message);
        return new(false, message);
    }
}

public sealed class FormModuleRecord : DynamicObject
{
    private readonly IDictionary<string, object?> _record;
    private readonly Dictionary<string, string> _fieldLookup;

    public FormModuleRecord(IDictionary<string, object?> record)
    {
        ArgumentNullException.ThrowIfNull(record);
        _record = record;
        _fieldLookup = record.Keys.ToDictionary(key => key, key => key, StringComparer.OrdinalIgnoreCase);
    }

    public IReadOnlyDictionary<string, object?> Values => new ReadOnlyDictionary<string, object?>(_record);

    public object? this[string fieldName]
    {
        get => TryGetValue(fieldName, out object? value) ? value : throw UnknownField(fieldName);
        set => Set(fieldName, value);
    }

    public bool TryGetValue(string fieldName, out object? value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fieldName);
        if (_fieldLookup.TryGetValue(fieldName, out string? key))
            return _record.TryGetValue(key, out value);

        value = null;
        return false;
    }

    public void Set(string fieldName, object? value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fieldName);
        if (!_fieldLookup.TryGetValue(fieldName, out string? key))
            throw UnknownField(fieldName);

        _record[key] = value;
    }

    public override bool TryGetMember(GetMemberBinder binder, out object? result)
        => TryGetValue(binder.Name, out result);

    public override bool TrySetMember(SetMemberBinder binder, object? value)
    {
        Set(binder.Name, value);
        return true;
    }

    private static InvalidOperationException UnknownField(string fieldName)
        => new($"Unknown form field '{fieldName}'. Code module writes must target fields that exist on the current form record.");
}

internal static class EmptyObjectDictionary
{
    public static readonly IReadOnlyDictionary<string, object?> Instance =
        new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
}

internal static class EmptyStringDictionary
{
    public static readonly IReadOnlyDictionary<string, string> Instance =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
}
