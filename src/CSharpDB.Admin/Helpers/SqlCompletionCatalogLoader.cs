using CSharpDB.Client;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Helpers;

internal static class SqlCompletionCatalogLoader
{
    internal static async Task<IReadOnlyDictionary<string, IReadOnlyList<SqlCompletionColumn>>> LoadColumnsAsync(
        ICSharpDbClient client, IReadOnlyList<SqlCompletionSource> sources, CancellationToken ct = default)
    {
        var columns = new Dictionary<string, IReadOnlyList<SqlCompletionColumn>>(StringComparer.OrdinalIgnoreCase);
        // System catalog aliases share metadata and need only one request.
        foreach (var source in sources.OrderBy(s => s.Name, StringComparer.OrdinalIgnoreCase))
        {
            ct.ThrowIfCancellationRequested();
            string name = DbSystemCatalogRegistry.TryNormalize(source.Name, out var canonical) ? canonical : source.Name;
            if (columns.TryGetValue(name, out var cached))
            {
                columns[source.Name] = cached.Select(c => c with { SourceName = source.Name }).ToArray();
                continue;
            }
            try
            {
                IReadOnlyList<SqlCompletionColumn> result;
                if (source.Kind == SqlCompletionSourceKind.Table)
                {
                    var schema = await client.GetTableSchemaAsync(name, ct);
                    result = schema?.Columns.Select(c => new SqlCompletionColumn(c.Name, c.EffectiveType.ToSql(), name)).ToArray() ?? [];
                }
                else
                {
                    // The schema API describes physical tables only. Ask for the
                    // result shape of views/catalogs without retrieving result rows.
                    string identifier = source.Kind == SqlCompletionSourceKind.SystemCatalog
                        ? string.Join(".", name.Split('.').Select(SqlIdentifierRules.Quote))
                        : SqlIdentifierRules.Quote(name);
                    var shape = await client.ExecuteSqlAsync($"SELECT * FROM {identifier} LIMIT 0", ct);
                    result = shape.Error is null && shape.ColumnNames is { } names
                        ? names.Select((column, i) => new SqlCompletionColumn(column,
                            shape.ColumnTypes is { } types && i < types.Length ? types[i] : null, name)).ToArray()
                        : [];
                }
                columns[name] = result;
                columns[source.Name] = result.Select(c => c with { SourceName = source.Name }).ToArray();
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
            catch
            {
                // A stale/unsupported object must not erase other sources' columns.
                columns[name] = [];
                columns[source.Name] = [];
            }
        }
        return columns;
    }
}
