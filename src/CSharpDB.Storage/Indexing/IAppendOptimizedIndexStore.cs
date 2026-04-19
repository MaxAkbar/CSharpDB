using CSharpDB.Primitives;

namespace CSharpDB.Storage.Indexing;

public enum AppendRowIdResult
{
    Missing = 0,
    Appended = 1,
    AlreadyExists = 2,
    NotApplicable = 3,
}

/// <summary>
/// Optional index-store capability for appending rowids into large duplicate buckets
/// without rewriting the full logical payload each time.
/// </summary>
public interface IAppendOptimizedIndexStore
{
    ValueTask<AppendRowIdResult> TryAppendHashedRowIdAsync(
        long key,
        DbValue[] keyComponents,
        long rowId,
        CancellationToken ct = default);
}
