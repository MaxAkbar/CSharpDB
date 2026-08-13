using System.Buffers.Binary;
using CSharpDB.Primitives;
using CSharpDB.Storage.Diagnostics.Internal;

namespace CSharpDB.Storage.Diagnostics;

public static class IndexInspector
{
    private sealed class IndexEntry
    {
        public required IndexSchema Schema { get; init; }
        public required uint RootPage { get; init; }
    }

    public static async ValueTask<IndexInspectReport> CheckAsync(
        string dbPath,
        string? indexName = null,
        int? sampleSize = null,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        int effectiveSampleSize = sampleSize.GetValueOrDefault(1000);
        if (effectiveSampleSize <= 0)
            effectiveSampleSize = 1000;

        InspectorEngine.DatabaseSnapshot snapshot = await InspectorEngine.ReadDatabaseSnapshotAsync(
            dbPath,
            captureLeafPayload: true,
            ct);

        List<IntegrityIssue> issues = InspectorEngine.CopyIssues(snapshot.Issues, ct);
        var tableSchemas = new Dictionary<string, TableSchema>(StringComparer.OrdinalIgnoreCase);
        var indexEntries = new List<IndexEntry>();

        uint schemaRoot = snapshot.Header.SchemaRootPage;
        if (schemaRoot == PageConstants.NullPageId)
        {
            issues.Add(new IntegrityIssue
            {
                Code = "SCHEMA_ROOT_MISSING",
                Severity = InspectSeverity.Warning,
                Message = "Schema root page is 0; cannot resolve index catalog.",
                Offset = PageConstants.SchemaRootPageOffset,
            });
        }
        else
        {
            HashSet<uint> schemaTreePages = InspectorEngine.WalkBTree(
                schemaRoot,
                snapshot.Pages,
                snapshot.PhysicalPageCount,
                issues,
                scope: "schema-catalog",
                ct: ct);

            uint indexCatalogRoot = 0;
            foreach (uint pageId in schemaTreePages)
            {
                ct.ThrowIfCancellationRequested();

                if (!snapshot.Pages.TryGetValue(pageId, out var page) || page.PageType != PageConstants.PageTypeLeaf)
                    continue;

                foreach (var cell in page.LeafCells)
                {
                    ct.ThrowIfCancellationRequested();

                    if (!cell.Key.HasValue || cell.Payload is null)
                        continue;

                    if (cell.Payload.Length < 4)
                    {
                        issues.Add(new IntegrityIssue
                        {
                            Code = "CATALOG_ENTRY_PAYLOAD_SHORT",
                            Severity = InspectSeverity.Error,
                            Message = $"Schema catalog entry key {cell.Key.Value} payload is too short.",
                            PageId = pageId,
                            Offset = cell.CellOffset,
                        });
                        continue;
                    }

                    if (cell.Key.Value == InspectorEngine.RowVersionHighWaterCatalogSentinel)
                        continue;

                    uint rootPage = BinaryPrimitives.ReadUInt32LittleEndian(cell.Payload.AsSpan(0, 4));

                    if (cell.Key.Value == InspectorEngine.IndexCatalogSentinel)
                    {
                        indexCatalogRoot = rootPage;
                        continue;
                    }

                    if (cell.Key.Value == InspectorEngine.ViewCatalogSentinel ||
                        cell.Key.Value == InspectorEngine.TriggerCatalogSentinel)
                    {
                        continue;
                    }

                    if (cell.Key.Value == InspectorEngine.TableStatsCatalogSentinel ||
                        cell.Key.Value == InspectorEngine.ColumnStatsCatalogSentinel ||
                        cell.Key.Value == InspectorEngine.ColumnDistributionStatsCatalogSentinel ||
                        cell.Key.Value == InspectorEngine.IndexPrefixStatsCatalogSentinel)
                    {
                        continue;
                    }

                    try
                    {
                        TableSchema tableSchema = SchemaSerializer.Deserialize(cell.Payload.AsSpan(4));
                        tableSchemas[tableSchema.TableName] = tableSchema;
                    }
                    catch
                    {
                        issues.Add(new IntegrityIssue
                        {
                            Code = "CATALOG_TABLE_SCHEMA_DECODE_FAILED",
                            Severity = InspectSeverity.Warning,
                            Message = $"Failed to decode table schema for catalog key {cell.Key.Value}.",
                            PageId = pageId,
                            Offset = cell.CellOffset,
                        });
                    }
                }
            }

            if (indexCatalogRoot != PageConstants.NullPageId)
            {
                HashSet<uint> indexCatalogPages = InspectorEngine.WalkBTree(
                    indexCatalogRoot,
                    snapshot.Pages,
                    snapshot.PhysicalPageCount,
                    issues,
                    scope: "index-catalog",
                    ct: ct);

                foreach (uint pageId in indexCatalogPages)
                {
                    ct.ThrowIfCancellationRequested();

                    if (!snapshot.Pages.TryGetValue(pageId, out var page) || page.PageType != PageConstants.PageTypeLeaf)
                        continue;

                    foreach (var cell in page.LeafCells)
                    {
                        ct.ThrowIfCancellationRequested();

                        if (cell.Payload is null || cell.Payload.Length < 4)
                        {
                            issues.Add(new IntegrityIssue
                            {
                                Code = "CATALOG_INDEX_ENTRY_PAYLOAD_SHORT",
                                Severity = InspectSeverity.Error,
                                Message = "Index catalog entry payload is too short.",
                                PageId = pageId,
                                Offset = cell.CellOffset,
                            });
                            continue;
                        }

                        try
                        {
                            uint rootPage = BinaryPrimitives.ReadUInt32LittleEndian(cell.Payload.AsSpan(0, 4));
                            IndexSchema schema = SchemaSerializer.DeserializeIndex(cell.Payload.AsSpan(4));
                            indexEntries.Add(new IndexEntry
                            {
                                Schema = schema,
                                RootPage = rootPage,
                            });
                        }
                        catch
                        {
                            issues.Add(new IntegrityIssue
                            {
                                Code = "CATALOG_INDEX_SCHEMA_DECODE_FAILED",
                                Severity = InspectSeverity.Warning,
                                Message = "Failed to decode index catalog entry.",
                                PageId = pageId,
                                Offset = cell.CellOffset,
                            });
                        }
                    }
                }
            }
        }

        if (!string.IsNullOrWhiteSpace(indexName))
        {
            var filteredEntries = new List<IndexEntry>();
            foreach (IndexEntry entry in indexEntries)
            {
                ct.ThrowIfCancellationRequested();
                if (entry.Schema.IndexName.Equals(indexName, StringComparison.OrdinalIgnoreCase))
                    filteredEntries.Add(entry);
            }

            indexEntries = filteredEntries;

            if (indexEntries.Count == 0)
            {
                issues.Add(new IntegrityIssue
                {
                    Code = "INDEX_NOT_FOUND",
                    Severity = InspectSeverity.Warning,
                    Message = $"Index '{indexName}' was not found in catalog.",
                });
            }
        }

        var items = new List<IndexCheckItem>(indexEntries.Count);
        List<IndexEntry> orderedEntries = OrderIndexEntries(indexEntries, ct);

        foreach (IndexEntry entry in orderedEntries)
        {
            ct.ThrowIfCancellationRequested();

            bool rootPageValid = false;
            if (entry.RootPage >= snapshot.PhysicalPageCount)
            {
                issues.Add(new IntegrityIssue
                {
                    Code = "INDEX_ROOT_OUT_OF_RANGE",
                    Severity = InspectSeverity.Error,
                    Message = $"Index '{entry.Schema.IndexName}' root page {entry.RootPage} is outside physical page range.",
                    PageId = entry.RootPage,
                });
            }
            else if (!snapshot.Pages.TryGetValue(entry.RootPage, out var rootPage))
            {
                issues.Add(new IntegrityIssue
                {
                    Code = "INDEX_ROOT_MISSING",
                    Severity = InspectSeverity.Error,
                    Message = $"Index '{entry.Schema.IndexName}' root page {entry.RootPage} could not be read.",
                    PageId = entry.RootPage,
                });
            }
            else if (rootPage.PageType is not (PageConstants.PageTypeLeaf or PageConstants.PageTypeInterior))
            {
                issues.Add(new IntegrityIssue
                {
                    Code = "INDEX_ROOT_BAD_PAGE_TYPE",
                    Severity = InspectSeverity.Error,
                    Message = $"Index '{entry.Schema.IndexName}' root page has invalid type {rootPage.PageType}.",
                    PageId = entry.RootPage,
                });
            }
            else
            {
                rootPageValid = true;
            }

            bool tableExists = tableSchemas.TryGetValue(entry.Schema.TableName, out TableSchema? tableSchema);
            if (!tableExists)
            {
                issues.Add(new IntegrityIssue
                {
                    Code = "INDEX_TABLE_MISSING",
                    Severity = InspectSeverity.Warning,
                    Message = $"Index '{entry.Schema.IndexName}' references missing table '{entry.Schema.TableName}'.",
                });
            }

            bool columnsExist = tableExists;
            var columns = new List<string>(entry.Schema.Columns.Count);
            foreach (string column in entry.Schema.Columns)
            {
                ct.ThrowIfCancellationRequested();
                columns.Add(column);
                if (tableExists && tableSchema!.GetColumnIndex(column) < 0)
                    columnsExist = false;
            }

            if (tableExists && !columnsExist)
            {
                issues.Add(new IntegrityIssue
                {
                    Code = "INDEX_COLUMN_MISSING",
                    Severity = InspectSeverity.Warning,
                    Message = $"Index '{entry.Schema.IndexName}' references columns not present in table '{entry.Schema.TableName}'.",
                });
            }

            bool rootReachable = false;
            if (rootPageValid)
            {
                HashSet<uint> visited = InspectorEngine.WalkBTree(
                    entry.RootPage,
                    snapshot.Pages,
                    snapshot.PhysicalPageCount,
                    issues,
                    scope: $"index:{entry.Schema.IndexName}",
                    ct: ct);
                rootReachable = visited.Count > 0 && visited.Contains(entry.RootPage);
            }

            items.Add(new IndexCheckItem
            {
                IndexName = entry.Schema.IndexName,
                TableName = entry.Schema.TableName,
                Columns = columns,
                RootPage = entry.RootPage,
                RootPageValid = rootPageValid,
                TableExists = tableExists,
                ColumnsExistInTable = columnsExist,
                RootTreeReachable = rootReachable,
            });
        }

        return new IndexInspectReport
        {
            DatabasePath = dbPath,
            RequestedIndexName = indexName,
            SampleSize = effectiveSampleSize,
            Indexes = items,
            Issues = issues,
        };
    }

    private static List<IndexEntry> OrderIndexEntries(
        IReadOnlyList<IndexEntry> entries,
        CancellationToken ct)
    {
        var buckets = new SortedDictionary<string, List<IndexEntry>>(StringComparer.OrdinalIgnoreCase);
        foreach (IndexEntry entry in entries)
        {
            ct.ThrowIfCancellationRequested();
            if (!buckets.TryGetValue(entry.Schema.IndexName, out List<IndexEntry>? bucket))
            {
                bucket = [];
                buckets.Add(entry.Schema.IndexName, bucket);
            }

            bucket.Add(entry);
        }

        var ordered = new List<IndexEntry>(entries.Count);
        foreach (List<IndexEntry> bucket in buckets.Values)
        {
            ct.ThrowIfCancellationRequested();
            foreach (IndexEntry entry in bucket)
            {
                ct.ThrowIfCancellationRequested();
                ordered.Add(entry);
            }
        }

        return ordered;
    }
}
