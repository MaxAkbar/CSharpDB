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
        int effectiveSampleSize = sampleSize.GetValueOrDefault(1000);
        if (effectiveSampleSize <= 0)
            effectiveSampleSize = 1000;

        InspectorEngine.DatabaseSnapshot snapshot = await InspectorEngine.ReadDatabaseSnapshotAsync(
            dbPath,
            captureLeafPayload: true,
            ct);

        var issues = new List<IntegrityIssue>(snapshot.Issues);
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
                scope: "schema-catalog");

            uint indexCatalogRoot = 0;
            foreach (uint pageId in schemaTreePages)
            {
                if (!snapshot.Pages.TryGetValue(pageId, out var page) || page.PageType != PageConstants.PageTypeLeaf)
                    continue;

                foreach (var cell in page.LeafCells)
                {
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
                    scope: "index-catalog");

                foreach (uint pageId in indexCatalogPages)
                {
                    if (!snapshot.Pages.TryGetValue(pageId, out var page) || page.PageType != PageConstants.PageTypeLeaf)
                        continue;

                    foreach (var cell in page.LeafCells)
                    {
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
            indexEntries = indexEntries
                .Where(e => e.Schema.IndexName.Equals(indexName, StringComparison.OrdinalIgnoreCase))
                .ToList();

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

        foreach (var entry in indexEntries.OrderBy(e => e.Schema.IndexName, StringComparer.OrdinalIgnoreCase))
        {
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

            bool columnsExist = tableExists && entry.Schema.Columns.All(c => tableSchema!.GetColumnIndex(c) >= 0);
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
                    scope: $"index:{entry.Schema.IndexName}");
                rootReachable = visited.Count > 0 && visited.Contains(entry.RootPage);
            }

            items.Add(new IndexCheckItem
            {
                IndexName = entry.Schema.IndexName,
                TableName = entry.Schema.TableName,
                Columns = entry.Schema.Columns.ToList(),
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
}
