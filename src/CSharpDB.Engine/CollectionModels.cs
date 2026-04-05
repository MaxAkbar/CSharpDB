using System.Diagnostics.CodeAnalysis;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.Engine;

/// <summary>
/// Comparable key kind used by collection secondary indexes.
/// </summary>
public enum CollectionIndexDataKind
{
    Integer,
    Text,
}

/// <summary>
/// Codec abstraction for collection document payloads.
/// Custom or generated codecs can implement this interface and register through <see cref="CollectionModelRegistry"/>.
/// </summary>
public interface ICollectionDocumentCodec<T>
{
    byte[] Encode(string key, T document);
    (string Key, T Document) Decode(ReadOnlySpan<byte> payload);
    T DecodeDocument(ReadOnlySpan<byte> payload);
    string DecodeKey(ReadOnlySpan<byte> payload);
    bool TryDecodeDocumentForKey(ReadOnlySpan<byte> payload, string expectedKey, out T? document);
    bool PayloadMatchesKey(ReadOnlySpan<byte> payload, string expectedKey);
}

/// <summary>
/// Generated or manually supplied collection model for a document type.
/// </summary>
public interface ICollectionModel<T>
{
    ICollectionDocumentCodec<T> CreateCodec(IRecordSerializer recordSerializer);

    bool TryGetField(string fieldPath, [NotNullWhen(true)] out CollectionField<T>? field);
}

/// <summary>
/// Registration token returned by <see cref="CollectionModelRegistry.Register{T}(ICollectionModel{T})"/>.
/// Dispose to restore the previous registration, if any.
/// </summary>
public sealed class CollectionModelRegistration : IDisposable
{
    private readonly Action? _dispose;

    internal CollectionModelRegistration(Action? dispose)
    {
        _dispose = dispose;
    }

    public void Dispose()
    {
        _dispose?.Invoke();
    }
}

/// <summary>
/// Global registry for generated or manually supplied collection models.
/// Source-generated code can register models during module initialization.
/// </summary>
public static class CollectionModelRegistry
{
    private static readonly object s_gate = new();
    private static readonly Dictionary<Type, object> s_models = new();

    public static CollectionModelRegistration Register<T>(ICollectionModel<T> model)
    {
        ArgumentNullException.ThrowIfNull(model);

        lock (s_gate)
        {
            Type documentType = typeof(T);
            s_models.TryGetValue(documentType, out object? previous);
            s_models[documentType] = model;
            return new CollectionModelRegistration(() =>
            {
                lock (s_gate)
                {
                    if (previous is null)
                    {
                        s_models.Remove(documentType);
                    }
                    else
                    {
                        s_models[documentType] = previous;
                    }
                }
            });
        }
    }

    internal static bool TryGet<T>([NotNullWhen(true)] out ICollectionModel<T>? model)
    {
        lock (s_gate)
        {
            if (s_models.TryGetValue(typeof(T), out object? registered))
            {
                model = (ICollectionModel<T>)registered;
                return true;
            }
        }

        model = null;
        return false;
    }
}

/// <summary>
/// Predeclared collection field descriptor that can be emitted by source generators.
/// </summary>
public abstract class CollectionField<TDocument>
{
    private readonly CollectionFieldAccessor _payloadAccessor;

    protected CollectionField(string fieldPath, CollectionIndexDataKind dataKind)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fieldPath);

        FieldPath = NormalizeFieldPath(fieldPath, out bool targetsArrayElements);
        DataKind = dataKind;
        TargetsArrayElements = targetsArrayElements;
        _payloadAccessor = CollectionFieldAccessor.FromFieldPath(FieldPath);
    }

    public string FieldPath { get; }

    public CollectionIndexDataKind DataKind { get; }

    public bool TargetsArrayElements { get; }

    internal CollectionFieldAccessor PayloadAccessor => _payloadAccessor;

    internal object? ReadValue(TDocument document) => ReadValueCore(document);

    protected abstract object? ReadValueCore(TDocument document);

    private static string NormalizeFieldPath(string fieldPath, out bool targetsArrayElements)
    {
        string trimmed = fieldPath.Trim();
        if (trimmed.StartsWith("$", StringComparison.Ordinal))
        {
            throw new NotSupportedException(
                "Generated collection field descriptors currently require CLR-style member paths like 'Address.City' or 'Tags[]'.");
        }

        string[] segments = trimmed.Split('.');
        targetsArrayElements = false;

        for (int i = 0; i < segments.Length; i++)
        {
            string segment = segments[i].Trim();
            if (segment.Length == 0)
            {
                throw new ArgumentException(
                    $"Collection field path '{fieldPath}' contains an empty path segment.",
                    nameof(fieldPath));
            }

            if (segment.EndsWith("[*]", StringComparison.Ordinal))
            {
                segment = segment[..^3] + "[]";
                targetsArrayElements = true;
            }
            else if (segment.EndsWith("[]", StringComparison.Ordinal))
            {
                targetsArrayElements = true;
            }
            else if (segment.IndexOf('[') >= 0 || segment.IndexOf(']') >= 0)
            {
                throw new NotSupportedException(
                    "Generated collection field descriptors currently support only wildcard array segments like 'Tags[]' or 'Orders[].Sku'.");
            }

            if (segment.Length == 0 || segment == "[]")
            {
                throw new ArgumentException(
                    $"Collection field path '{fieldPath}' contains an empty path segment.",
                    nameof(fieldPath));
            }

            segments[i] = segment;
        }

        return string.Join(".", segments);
    }
}

/// <summary>
/// Strongly typed collection field descriptor for generated collection APIs.
/// </summary>
public sealed class CollectionField<TDocument, TField> : CollectionField<TDocument>
{
    private readonly Func<TDocument, TField> _accessor;

    public CollectionField(
        string fieldPath,
        Func<TDocument, TField> accessor,
        CollectionIndexDataKind dataKind)
        : base(fieldPath, dataKind)
    {
        _accessor = accessor ?? throw new ArgumentNullException(nameof(accessor));
    }

    protected override object? ReadValueCore(TDocument document) => _accessor(document);
}
