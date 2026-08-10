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
    public OpaqueDiagnosticsId(string value)
    {
        if (!CSharpDbDiagnostics.IsValidOpaqueIdentifier(value))
            throw new ArgumentException("A 32-character lowercase hexadecimal diagnostics id is required.", nameof(value));

        Value = value;
    }

    public string Value { get; }

    public static OpaqueDiagnosticsId Create()
        => new(CSharpDbDiagnostics.CreateOpaqueIdentifier());

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
