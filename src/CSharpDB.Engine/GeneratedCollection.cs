using System.Diagnostics.CodeAnalysis;

namespace CSharpDB.Engine;

/// <summary>
/// Trim-safe typed collection wrapper that exposes only the generated-field collection surface.
/// Use <see cref="Database.GetGeneratedCollectionAsync{T}(string, CancellationToken)"/> with a
/// registered generated collection model.
/// </summary>
[UnconditionalSuppressMessage(
    "TrimAnalysis",
    "IL2026",
    Justification = "GeneratedCollection<T> is created only through Database.GetGeneratedCollectionAsync<T>, which verifies that a generated or manually supplied collection model is registered before delegating to Collection<T>.")]
[UnconditionalSuppressMessage(
    "TrimAnalysis",
    "IL2091",
    Justification = "GeneratedCollection<T> is created only through Database.GetGeneratedCollectionAsync<T>, which verifies that a generated or manually supplied collection model is registered before delegating to Collection<T>.")]
[UnconditionalSuppressMessage(
    "Aot",
    "IL3050",
    Justification = "GeneratedCollection<T> is created only through Database.GetGeneratedCollectionAsync<T>, which verifies that a generated or manually supplied collection model is registered before delegating to Collection<T>.")]
public sealed class GeneratedCollection<T>
{
    private readonly Collection<T> _inner;

    internal GeneratedCollection(Collection<T> inner)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    public ValueTask PutAsync(string key, T document, CancellationToken ct = default)
        => _inner.PutAsync(key, document, ct);

    public ValueTask<T?> GetAsync(string key, CancellationToken ct = default)
        => _inner.GetAsync(key, ct);

    public ValueTask<bool> DeleteAsync(string key, CancellationToken ct = default)
        => _inner.DeleteAsync(key, ct);

    public ValueTask<long> CountAsync(CancellationToken ct = default)
        => _inner.CountAsync(ct);

    public IAsyncEnumerable<KeyValuePair<string, T>> ScanAsync(CancellationToken ct = default)
        => _inner.ScanAsync(ct);

    public IAsyncEnumerable<KeyValuePair<string, T>> FindAsync(
        Func<T, bool> predicate,
        CancellationToken ct = default)
        => _inner.FindAsync(predicate, ct);

    public ValueTask EnsureIndexAsync<TField>(
        CollectionField<T, TField> field,
        CancellationToken ct = default)
        => _inner.EnsureIndexAsync(field, ct);

    public ValueTask EnsureIndexAsync<TField>(
        CollectionField<T, TField> field,
        string? collation,
        CancellationToken ct = default)
        => _inner.EnsureIndexAsync(field, collation, ct);

    public IAsyncEnumerable<KeyValuePair<string, T>> FindByIndexAsync<TField>(
        CollectionField<T, TField> field,
        TField value,
        CancellationToken ct = default)
        => _inner.FindByIndexAsync(field, value, ct);

    public IAsyncEnumerable<KeyValuePair<string, T>> FindByRangeAsync<TField>(
        CollectionField<T, TField> field,
        TField lowerBound,
        TField upperBound,
        bool lowerInclusive = true,
        bool upperInclusive = true,
        CancellationToken ct = default)
        => _inner.FindByRangeAsync(field, lowerBound, upperBound, lowerInclusive, upperInclusive, ct);
}
