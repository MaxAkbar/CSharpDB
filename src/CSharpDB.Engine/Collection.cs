using System.Runtime.CompilerServices;
using System.Text;
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
    private readonly string _catalogTableName;
    private readonly BTree _tree;
    private readonly Func<bool> _isInTransaction;
    private readonly IRecordSerializer _recordSerializer;
    private readonly bool _useDirectPayloadFormat;

    private static readonly JsonSerializerOptions s_jsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
    };

    internal Collection(
        Pager pager,
        SchemaCatalog catalog,
        string catalogTableName,
        BTree tree,
        IRecordSerializer recordSerializer,
        Func<bool> isInTransaction)
    {
        _pager = pager;
        _catalog = catalog;
        _catalogTableName = catalogTableName;
        _tree = tree;
        _recordSerializer = recordSerializer ?? throw new ArgumentNullException(nameof(recordSerializer));
        _useDirectPayloadFormat = recordSerializer is DefaultRecordSerializer;
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
            byte[] keyUtf8 = Encoding.UTF8.GetBytes(key);
            byte[] newPayload = EncodeDocument(keyUtf8, document);

            for (int probe = 0; probe < MaxProbeDistance; probe++)
            {
                long probeHash = (startHash + probe) & 0x7FFFFFFFFFFFFFFF;
                var existing = await _tree.FindMemoryAsync(probeHash, ct);

                if (existing is not { } existingPayload)
                {
                    // Empty slot: insert here
                    await _tree.InsertAsync(probeHash, newPayload, ct);
                    return;
                }

                if (PayloadMatchesKey(existingPayload.Span, keyUtf8, key))
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
        byte[] keyUtf8 = Encoding.UTF8.GetBytes(key);

        for (int probe = 0; probe < MaxProbeDistance; probe++)
        {
            long probeHash = (startHash + probe) & 0x7FFFFFFFFFFFFFFF;
            var payload = await _tree.FindMemoryAsync(probeHash, ct);

            if (payload is not { } payloadMemory)
                return default; // Empty slot means key doesn't exist

            if (TryDecodeDocumentForKey(payloadMemory.Span, keyUtf8, key, out var doc))
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
            byte[] keyUtf8 = Encoding.UTF8.GetBytes(key);

            for (int probe = 0; probe < MaxProbeDistance; probe++)
            {
                long probeHash = (startHash + probe) & 0x7FFFFFFFFFFFFFFF;
                var payload = await _tree.FindMemoryAsync(probeHash, ct);

                if (payload is not { } payloadMemory)
                    return; // Not found

                if (PayloadMatchesKey(payloadMemory.Span, keyUtf8, key))
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

    private byte[] EncodeDocument(ReadOnlySpan<byte> keyUtf8, T document)
    {
        if (!_useDirectPayloadFormat)
        {
            string key = Encoding.UTF8.GetString(keyUtf8);
            string json = JsonSerializer.Serialize(document, s_jsonOptions);
            return _recordSerializer.Encode(
                [
                    DbValue.FromText(key),
                    DbValue.FromText(json),
                ]);
        }

        byte[] jsonUtf8 = JsonSerializer.SerializeToUtf8Bytes(document, s_jsonOptions);
        return CollectionPayloadCodec.Encode(keyUtf8, jsonUtf8);
    }

    private (string key, T document) DecodeDocument(ReadOnlySpan<byte> payload)
    {
        if (!_useDirectPayloadFormat || !CollectionPayloadCodec.IsDirectPayload(payload))
            return DecodeLegacyDocument(payload);

        string storedKey = CollectionPayloadCodec.DecodeKey(payload);
        T doc = JsonSerializer.Deserialize<T>(CollectionPayloadCodec.GetJsonUtf8(payload), s_jsonOptions)!;
        return (storedKey, doc);
    }

    private bool TryDecodeDocumentForKey(
        ReadOnlySpan<byte> payload,
        ReadOnlySpan<byte> expectedKeyUtf8,
        string expectedKey,
        out T? document)
    {
        if (!PayloadMatchesKey(payload, expectedKeyUtf8, expectedKey))
        {
            document = default;
            return false;
        }

        if (_useDirectPayloadFormat && CollectionPayloadCodec.IsDirectPayload(payload))
        {
            document = JsonSerializer.Deserialize<T>(CollectionPayloadCodec.GetJsonUtf8(payload), s_jsonOptions);
            return true;
        }

        document = DecodeLegacyDocument(payload).document;
        return true;
    }

    private bool PayloadMatchesKey(
        ReadOnlySpan<byte> payload,
        ReadOnlySpan<byte> expectedKeyUtf8,
        string expectedKey)
    {
        if (_useDirectPayloadFormat && CollectionPayloadCodec.IsDirectPayload(payload))
            return CollectionPayloadCodec.KeyEquals(payload, expectedKeyUtf8);

        if (_recordSerializer.TryColumnTextEquals(payload, 0, expectedKeyUtf8, out bool equals))
            return equals;

        return DecodeLegacyKey(payload) == expectedKey;
    }

    private (string key, T document) DecodeLegacyDocument(ReadOnlySpan<byte> payload)
    {
        var values = _recordSerializer.Decode(payload);
        string storedKey = values[0].AsText;
        string json = values[1].AsText;
        T doc = JsonSerializer.Deserialize<T>(json, s_jsonOptions)!;
        return (storedKey, doc);
    }

    private string DecodeLegacyKey(ReadOnlySpan<byte> payload)
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
            await _catalog.PersistRootPageChangesAsync(_catalogTableName, ct);
            await _pager.CommitAsync(ct);
        }
        catch
        {
            await _pager.RollbackAsync(ct);
            throw;
        }
    }

}
