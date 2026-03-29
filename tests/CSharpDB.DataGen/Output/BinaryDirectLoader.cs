using System.Diagnostics.CodeAnalysis;
using CSharpDB.DataGen.Specs;
using CSharpDB.Engine;
using System.Text.Json;

namespace CSharpDB.DataGen.Output;

public static class BinaryDirectLoader
{
    public static async Task<string> LoadSqlTablesAsync(
        DataGenOptions options,
        IReadOnlyList<SqlTableSpec> tables,
        IReadOnlyDictionary<string, GeneratedSqlTableSource> sources,
        CancellationToken ct = default)
    {
        string dbPath = PrepareDatabasePath(options);

        await using var db = await Database.OpenAsync(dbPath, ct);
        foreach (string statement in SqlSpecBuilder.BuildSchemaScript(tables, includeIndexes: false)
                     .Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries))
        {
            await db.ExecuteAsync(statement, ct);
        }

        foreach (SqlTableSpec table in tables)
        {
            GeneratedSqlTableSource source = GetRequiredSource(sources, table.GeneratorKey);
            await LoadSqlTableAsync(db, table, source.CreateRows(), options.BatchSize, ct);
        }

        if (options.BuildIndexes)
        {
            foreach (string statement in SqlSpecBuilder.BuildSchemaScript(tables, includeIndexes: true)
                         .Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries)
                         .Where(static statement => statement.StartsWith("CREATE INDEX", StringComparison.OrdinalIgnoreCase) ||
                                                    statement.StartsWith("CREATE UNIQUE INDEX", StringComparison.OrdinalIgnoreCase)))
            {
                await db.ExecuteAsync(statement, ct);
            }
        }

        return dbPath;
    }

    [RequiresUnreferencedCode("Direct collection loading uses Collection<T>, which is reflection-based.")]
    [RequiresDynamicCode("Direct collection loading uses Collection<T>, which is reflection-based.")]
    public static async Task<string> LoadCollectionsAsync(
        DataGenOptions options,
        IReadOnlyList<CollectionSpec> collections,
        IReadOnlyDictionary<string, GeneratedCollectionSource> sources,
        CancellationToken ct = default)
    {
        string dbPath = PrepareDatabasePath(options);

        await using var db = await Database.OpenAsync(dbPath, ct);
        foreach (CollectionSpec collectionSpec in collections)
        {
            GeneratedCollectionSource source = GetRequiredCollectionSource(sources, collectionSpec.GeneratorKey);
            var collection = await db.GetCollectionAsync<JsonElement>(collectionSpec.Name, ct);

            await LoadCollectionAsync(
                db,
                collection,
                source.CreateDocuments(),
                static document => document.Key,
                static document => document.Document,
                options.BatchSize,
                ct);

            if (!options.BuildIndexes)
                continue;

            foreach (string indexPath in collectionSpec.IndexPaths)
                await collection.EnsureIndexAsync(indexPath, ct);
        }

        return dbPath;
    }

    private static async Task LoadSqlTableAsync(
        Database db,
        SqlTableSpec table,
        IEnumerable<IReadOnlyDictionary<string, object?>> rows,
        int batchSize,
        CancellationToken ct)
    {
        var batch = db.PrepareInsertBatch(table.Name, batchSize);
        foreach (IReadOnlyDictionary<string, object?> row in rows)
        {
            ct.ThrowIfCancellationRequested();
            batch.AddRow(SqlSpecBuilder.BuildDbValues(table, row));
            if (batch.Count >= batchSize)
                await FlushInsertBatchAsync(db, batch, ct);
        }

        if (batch.Count > 0)
            await FlushInsertBatchAsync(db, batch, ct);
    }

    [RequiresUnreferencedCode("Direct collection loading uses Collection<T>, which is reflection-based.")]
    [RequiresDynamicCode("Direct collection loading uses Collection<T>, which is reflection-based.")]
    private static async Task LoadCollectionAsync<T>(
        Database db,
        Collection<T> collection,
        IEnumerable<GeneratedCollectionDocument> documents,
        Func<GeneratedCollectionDocument, string> keySelector,
        Func<GeneratedCollectionDocument, T> documentSelector,
        int batchSize,
        CancellationToken ct)
    {
        var buffer = new List<GeneratedCollectionDocument>(batchSize);
        foreach (GeneratedCollectionDocument document in documents)
        {
            ct.ThrowIfCancellationRequested();
            buffer.Add(document);

            if (buffer.Count >= batchSize)
                await FlushCollectionBatchAsync(db, collection, buffer, keySelector, documentSelector, ct);
        }

        if (buffer.Count > 0)
            await FlushCollectionBatchAsync(db, collection, buffer, keySelector, documentSelector, ct);
    }

    private static async Task FlushInsertBatchAsync(Database db, InsertBatch batch, CancellationToken ct)
    {
        await db.BeginTransactionAsync(ct);
        try
        {
            await batch.ExecuteAsync(ct);
            await db.CommitAsync(ct);
        }
        catch
        {
            await db.RollbackAsync(ct);
            throw;
        }
    }

    [RequiresUnreferencedCode("Direct collection loading uses Collection<T>, which is reflection-based.")]
    [RequiresDynamicCode("Direct collection loading uses Collection<T>, which is reflection-based.")]
    private static async Task FlushCollectionBatchAsync<T>(
        Database db,
        Collection<T> collection,
        List<GeneratedCollectionDocument> buffer,
        Func<GeneratedCollectionDocument, string> keySelector,
        Func<GeneratedCollectionDocument, T> documentSelector,
        CancellationToken ct)
    {
        await db.BeginTransactionAsync(ct);
        try
        {
            for (int i = 0; i < buffer.Count; i++)
                await collection.PutAsync(keySelector(buffer[i]), documentSelector(buffer[i]), ct);

            await db.CommitAsync(ct);
        }
        catch
        {
            await db.RollbackAsync(ct);
            throw;
        }
        finally
        {
            buffer.Clear();
        }
    }

    private static string PrepareDatabasePath(DataGenOptions options)
    {
        string fullPath = options.ResolvedDatabasePath;
        string? directory = Path.GetDirectoryName(fullPath);
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        if (!File.Exists(fullPath) && !File.Exists(fullPath + ".wal"))
            return fullPath;

        if (!options.OverwriteDatabase)
        {
            throw new InvalidOperationException(
                $"Database '{fullPath}' already exists. Pass --overwrite-db to replace it.");
        }

        try
        {
            if (File.Exists(fullPath))
                File.Delete(fullPath);

            if (File.Exists(fullPath + ".wal"))
                File.Delete(fullPath + ".wal");
        }
        catch (Exception ex)
        {
            throw new IOException($"Failed to overwrite database '{fullPath}'.", ex);
        }

        return fullPath;
    }

    private static GeneratedSqlTableSource GetRequiredSource(
        IReadOnlyDictionary<string, GeneratedSqlTableSource> sources,
        string generatorKey)
    {
        if (sources.TryGetValue(generatorKey, out GeneratedSqlTableSource? source))
            return source;

        throw new InvalidOperationException(
            $"No generated SQL table source was registered for generatorKey '{generatorKey}'.");
    }

    private static GeneratedCollectionSource GetRequiredCollectionSource(
        IReadOnlyDictionary<string, GeneratedCollectionSource> sources,
        string generatorKey)
    {
        if (sources.TryGetValue(generatorKey, out GeneratedCollectionSource? source))
            return source;

        throw new InvalidOperationException(
            $"No generated collection source was registered for generatorKey '{generatorKey}'.");
    }
}
