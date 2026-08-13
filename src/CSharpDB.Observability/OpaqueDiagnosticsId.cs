using System.Text.Json;
using System.Text.Json.Serialization;

namespace CSharpDB.Observability;

/// <summary>
/// An independently generated, non-authorizing correlation identifier.
/// Transaction bearer tokens and caller-provided identifiers must never be
/// converted into this type.
/// </summary>
[JsonConverter(typeof(OpaqueDiagnosticsIdJsonConverter))]
public sealed record OpaqueDiagnosticsId
{
    private readonly Guid _id;
    private string? _value;
    private object? _runtimeDiagnosticsOwner;

    public OpaqueDiagnosticsId(string value)
    {
        if (!CSharpDbDiagnostics.IsValidOpaqueIdentifier(value))
            throw new ArgumentException("A 32-character lowercase hexadecimal diagnostics id is required.", nameof(value));

        _id = Guid.ParseExact(value, "N");
        _value = value;
    }

    private OpaqueDiagnosticsId(Guid id)
        => _id = id;

    public string Value
    {
        get
        {
            string? value = Volatile.Read(ref _value);
            if (value is not null)
                return value;

            value = _id.ToString("N");
            Interlocked.CompareExchange(ref _value, value, null);
            return _value;
        }
    }

    public static OpaqueDiagnosticsId Create()
        => new(Guid.NewGuid());

    internal static OpaqueDiagnosticsId Create(Guid id)
        => new(id);

    internal bool Matches(Guid id)
        => _id == id;

    public bool Equals(OpaqueDiagnosticsId? other)
        => other is not null && _id == other._id;

    public override int GetHashCode()
        => _id.GetHashCode();

    internal bool TryClaimRuntimeDiagnostics(object owner)
        => Interlocked.CompareExchange(
            ref _runtimeDiagnosticsOwner,
            owner,
            null) is null;

    internal bool TryTransferRuntimeDiagnostics(
        object previousOwner,
        object newOwner)
    {
        while (true)
        {
            object? current = Volatile.Read(ref _runtimeDiagnosticsOwner);
            if (current is not null && !ReferenceEquals(current, previousOwner))
                return false;

            if (ReferenceEquals(
                    Interlocked.CompareExchange(
                        ref _runtimeDiagnosticsOwner,
                        newOwner,
                        current),
                    current))
            {
                return true;
            }
        }
    }

    internal void ReleaseRuntimeDiagnostics(object owner)
        => Interlocked.CompareExchange(
            ref _runtimeDiagnosticsOwner,
            null,
            owner);

    public override string ToString() => Value;
}

public sealed class OpaqueDiagnosticsIdJsonConverter : JsonConverter<OpaqueDiagnosticsId>
{
    public override OpaqueDiagnosticsId Read(
        ref Utf8JsonReader reader,
        Type typeToConvert,
        JsonSerializerOptions options)
        => new(reader.GetString() ?? throw new JsonException("A diagnostics id is required."));

    public override void Write(
        Utf8JsonWriter writer,
        OpaqueDiagnosticsId value,
        JsonSerializerOptions options)
        => writer.WriteStringValue(value.Value);
}
