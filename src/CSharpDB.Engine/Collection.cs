using System.Runtime.CompilerServices;
using System.Text.Json;
using CSharpDB.Core;

namespace CSharpDB.Engine;

/// <summary>
/// A typed document collection backed by a B+tree.
/// Documents are serialized as JSON and stored with string keys hashed to long B+tree keys.
/// Provides a NoSQL-style API that bypasses the SQL parser/planner entirely.
/// </summary>
public sealed class Collection<T>
{
    private const int MaxProbeDistance = 128;

    private readonly Pager _pager;
    private readonly SchemaCatalog _catalog;
    private readonly IRecordSerializer _recordSerializer;
    private readonly string _catalogTableName;
    private readonly BTree _tree;
    private readonly Func<bool> _isInTransaction;

    private static readonly JsonSerializerOptions s_jsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
    };

    internal Collection(
        Pager pager,
        SchemaCatalog catalog,
        IRecordSerializer recordSerializer,
        string catalogTableName,
        BTree tree,
        Func<bool> isInTransaction)
    {
        _pager = pager;
        _catalog = catalog;
        _recordSerializer = recordSerializer;
        _catalogTableName = catalogTableName;
        _tree = tree;
        _isInTransaction = isInTransaction;
    }

    // ===== Tier 1 API =====

    /// <summary>
    /// Insert or update a document by key.
    /// </summary>
    public async ValueTask PutAsync(string key, T document, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(document);

        await AutoCommitAsync(async () =>
        {
            long startHash = HashDocumentKey(key);
            byte[] newPayload = EncodeDocument(key, document);

            for (int probe = 0; probe < MaxProbeDistance; probe++)
            {
                long probeHash = (startHash + probe) & 0x7FFFFFFFFFFFFFFF;
                byte[]? existing = await _tree.FindAsync(probeHash, ct);

                if (existing == null)
                {
                    // Empty slot: insert here
                    await _tree.InsertAsync(probeHash, newPayload, ct);
                    return;
                }

                string storedKey = DecodeKey(existing);
                if (storedKey == key)
                {
                    // Upsert: delete old entry and insert new
                    await _tree.DeleteAsync(probeHash, ct);
                    await _tree.InsertAsync(probeHash, newPayload, ct);
                    return;
                }
                // Collision with different key: continue probing
            }

            throw new CSharpDbException(ErrorCode.Unknown,
                $"Hash collision probe limit exceeded for key '{key}'.");
        }, ct);
    }

    /// <summary>
    /// Retrieve a document by key. Returns default if not found.
    /// </summary>
    public async ValueTask<T?> GetAsync(string key, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        long startHash = HashDocumentKey(key);

        for (int probe = 0; probe < MaxProbeDistance; probe++)
        {
            long probeHash = (startHash + probe) & 0x7FFFFFFFFFFFFFFF;
            byte[]? payload = await _tree.FindAsync(probeHash, ct);

            if (payload == null)
                return default; // Empty slot means key doesn't exist

            var (storedKey, doc) = DecodeDocument(payload);
            if (storedKey == key)
                return doc;
            // Collision: continue probing
        }

        return default;
    }

    /// <summary>
    /// Delete a document by key. Returns true if the key existed.
    /// </summary>
    public async ValueTask<bool> DeleteAsync(string key, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        bool deleted = false;

        await AutoCommitAsync(async () =>
        {
            long startHash = HashDocumentKey(key);

            for (int probe = 0; probe < MaxProbeDistance; probe++)
            {
                long probeHash = (startHash + probe) & 0x7FFFFFFFFFFFFFFF;
                byte[]? payload = await _tree.FindAsync(probeHash, ct);

                if (payload == null)
                    return; // Not found

                string storedKey = DecodeKey(payload);
                if (storedKey == key)
                {
                    await _tree.DeleteAsync(probeHash, ct);
                    deleted = true;
                    return;
                }
                // Collision: continue probing
            }
        }, ct);

        return deleted;
    }

    /// <summary>
    /// Return the number of documents in the collection.
    /// </summary>
    public async ValueTask<long> CountAsync(CancellationToken ct = default)
    {
        return await _tree.CountEntriesAsync(ct);
    }

    /// <summary>
    /// Iterate all documents in the collection.
    /// </summary>
    public async IAsyncEnumerable<KeyValuePair<string, T>> ScanAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var cursor = _tree.CreateCursor();
        while (await cursor.MoveNextAsync(ct))
        {
            var (key, doc) = DecodeDocument(cursor.CurrentValue.Span);
            yield return new KeyValuePair<string, T>(key, doc);
        }
    }

    // ===== Tier 2 API =====

    /// <summary>
    /// Find all documents matching a predicate (full scan + in-memory filter).
    /// </summary>
    public async IAsyncEnumerable<KeyValuePair<string, T>> FindAsync(
        Func<T, bool> predicate,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        await foreach (var kvp in ScanAsync(ct))
        {
            if (predicate(kvp.Value))
                yield return kvp;
        }
    }

    // ===== Internal helpers =====

    /// <summary>
    /// Hash a string document key to a long B+tree key.
    /// Uses a polynomial hash with multiplier 53 (case-sensitive, distinct from catalog hashes).
    /// </summary>
    internal static long HashDocumentKey(string key)
    {
        long hash = 0;
        foreach (char c in key)
            hash = hash * 53 + c;
        return hash & 0x7FFFFFFFFFFFFFFF; // ensure positive
    }

    private byte[] EncodeDocument(string key, T document)
    {
        string json = JsonSerializer.Serialize(document, s_jsonOptions);
        var values = new DbValue[] { DbValue.FromText(key), DbValue.FromText(json) };
        return _recordSerializer.Encode(values);
    }

    private (string key, T document) DecodeDocument(ReadOnlySpan<byte> payload)
    {
        var values = _recordSerializer.Decode(payload);
        string storedKey = values[0].AsText;
        string json = values[1].AsText;
        T doc = JsonSerializer.Deserialize<T>(json, s_jsonOptions)!;
        return (storedKey, doc);
    }

    /// <summary>
    /// Decode only the key from a payload (avoids deserializing the full document).
    /// </summary>
    private string DecodeKey(ReadOnlySpan<byte> payload)
    {
        var values = _recordSerializer.DecodeUpTo(payload, 0);
        return values[0].AsText;
    }

    /// <summary>
    /// Auto-commit wrapper: begins a transaction, executes the action, and commits.
    /// If an explicit transaction is active, just executes directly.
    /// </summary>
    private async ValueTask AutoCommitAsync(Func<ValueTask> action, CancellationToken ct)
    {
        if (_isInTransaction())
        {
            await action();
            return;
        }

        await _pager.BeginTransactionAsync(ct);
        try
        {
            await action();
            await _catalog.PersistAllRootPageChangesAsync(ct);
            await _pager.CommitAsync(ct);
        }
        catch
        {
            await _pager.RollbackAsync(ct);
            throw;
        }
    }
}
