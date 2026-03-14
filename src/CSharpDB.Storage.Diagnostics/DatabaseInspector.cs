using System.Buffers.Binary;
using CSharpDB.Primitives;
using CSharpDB.Storage.Diagnostics.Internal;

namespace CSharpDB.Storage.Diagnostics;

public static class DatabaseInspector
{
    public static async ValueTask<DatabaseInspectReport> InspectAsync(
        string dbPath,
        DatabaseInspectOptions? options = null,
        CancellationToken ct = default)
    {
        options ??= new DatabaseInspectOptions();

        InspectorEngine.DatabaseSnapshot snapshot = await InspectorEngine.ReadDatabaseSnapshotAsync(
            dbPath,
            captureLeafPayload: true,
            ct);

        var issues = new List<IntegrityIssue>(snapshot.Issues);

        // Cross-check schema root and reachable trees.
        uint schemaRoot = snapshot.Header.SchemaRootPage;
        if (schemaRoot == PageConstants.NullPageId)
        {
            issues.Add(new IntegrityIssue
            {
                Code = "SCHEMA_ROOT_MISSING",
                Severity = InspectSeverity.Warning,
                Message = "Schema root page is 0; no catalog tree root is defined.",
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

            CatalogRoots roots = CollectCatalogRoots(schemaTreePages, snapshot.Pages, snapshot.PhysicalPageCount, issues);

            ValidateRootPage(roots.IndexCatalogRootPage, "index-catalog", snapshot, issues);
            ValidateRootPage(roots.ViewCatalogRootPage, "view-catalog", snapshot, issues);
            ValidateRootPage(roots.TriggerCatalogRootPage, "trigger-catalog", snapshot, issues);

            foreach (var tableRoot in roots.TableRoots)
                ValidateRootPage(tableRoot.RootPage, $"table:{tableRoot.Name}", snapshot, issues);

            foreach (var indexRoot in roots.IndexRoots)
                ValidateRootPage(indexRoot.RootPage, $"index:{indexRoot.Name}", snapshot, issues);

            foreach (var viewRoot in roots.ViewRoots)
                ValidateRootPage(viewRoot.RootPage, $"view:{viewRoot.Name}", snapshot, issues);

            foreach (var triggerRoot in roots.TriggerRoots)
                ValidateRootPage(triggerRoot.RootPage, $"trigger:{triggerRoot.Name}", snapshot, issues);
        }

        var histogram = snapshot.Pages.Values
            .GroupBy(p => InspectorEngine.PageTypeName(p.PageType), StringComparer.OrdinalIgnoreCase)
            .ToDictionary(g => g.Key, g => g.Count(), StringComparer.OrdinalIgnoreCase);

        List<PageReport>? pageReports = null;
        if (options.IncludePages)
        {
            pageReports = snapshot.Pages
                .OrderBy(kvp => kvp.Key)
                .Select(kvp => ToReport(kvp.Value))
                .ToList();
        }

        return new DatabaseInspectReport
        {
            DatabasePath = dbPath,
            Header = snapshot.Header,
            PageTypeHistogram = histogram,
            PageCountScanned = snapshot.Pages.Count,
            Pages = pageReports,
            Issues = issues,
        };
    }

    public static async ValueTask<PageInspectReport> InspectPageAsync(
        string dbPath,
        uint pageId,
        bool includeHex = false,
        CancellationToken ct = default)
    {
        var issues = new List<IntegrityIssue>();

        byte[]? pageBytes = await InspectorEngine.ReadPageBytesAsync(dbPath, pageId, ct);
        if (pageBytes is null)
        {
            issues.Add(new IntegrityIssue
            {
                Code = "PAGE_NOT_FOUND",
                Severity = InspectSeverity.Error,
                Message = $"Page {pageId} is outside the file bounds.",
                PageId = pageId,
            });

            return new PageInspectReport
            {
                DatabasePath = dbPath,
                PageId = pageId,
                Exists = false,
                Page = null,
                HexDump = null,
                Issues = issues,
            };
        }

        InspectorEngine.ParsePageResult parsed = InspectorEngine.ParsePage(pageId, pageBytes, captureLeafPayload: true);
        issues.AddRange(parsed.Issues);

        return new PageInspectReport
        {
            DatabasePath = dbPath,
            PageId = pageId,
            Exists = true,
            Page = ToReport(parsed.Page),
            HexDump = includeHex ? InspectorEngine.BuildHexDump(pageBytes) : null,
            Issues = issues,
        };
    }

    private static PageReport ToReport(InspectorEngine.ParsedPage page)
    {
        List<LeafCellReport>? leafCells = null;
        List<InteriorCellReport>? interiorCells = null;

        if (page.PageType == PageConstants.PageTypeLeaf)
        {
            leafCells = page.LeafCells
                .Select(c => new LeafCellReport
                {
                    CellIndex = c.CellIndex,
                    CellOffset = c.CellOffset,
                    HeaderBytes = c.HeaderBytes,
                    CellTotalBytes = c.CellTotalBytes,
                    Key = c.Key,
                    PayloadBytes = c.Payload?.Length ?? 0,
                })
                .ToList();
        }
        else if (page.PageType == PageConstants.PageTypeInterior)
        {
            interiorCells = page.InteriorCells
                .Select(c => new InteriorCellReport
                {
                    CellIndex = c.CellIndex,
                    CellOffset = c.CellOffset,
                    HeaderBytes = c.HeaderBytes,
                    CellTotalBytes = c.CellTotalBytes,
                    LeftChildPage = c.LeftChildPage,
                    Key = c.Key,
                })
                .ToList();
        }

        return new PageReport
        {
            PageId = page.PageId,
            PageTypeCode = page.PageType,
            PageTypeName = InspectorEngine.PageTypeName(page.PageType),
            BaseOffset = page.BaseOffset,
            CellCount = page.CellCount,
            CellContentStart = page.CellContentStart,
            RightChildOrNextLeaf = page.RightChildOrNextLeaf,
            FreeSpaceBytes = page.FreeSpaceBytes,
            CellOffsets = page.CellOffsets.Select(x => (int)x).ToList(),
            LeafCells = leafCells,
            InteriorCells = interiorCells,
        };
    }

    private static void ValidateRootPage(
        uint rootPage,
        string scope,
        InspectorEngine.DatabaseSnapshot snapshot,
        List<IntegrityIssue> issues)
    {
        if (rootPage == PageConstants.NullPageId)
            return;

        if (rootPage >= snapshot.PhysicalPageCount)
        {
            issues.Add(new IntegrityIssue
            {
                Code = "CATALOG_ROOT_OUT_OF_RANGE",
                Severity = InspectSeverity.Error,
                Message = $"{scope} root page {rootPage} is outside physical page range.",
                PageId = rootPage,
            });
            return;
        }

        if (!snapshot.Pages.TryGetValue(rootPage, out var root))
        {
            issues.Add(new IntegrityIssue
            {
                Code = "CATALOG_ROOT_MISSING",
                Severity = InspectSeverity.Error,
                Message = $"{scope} root page {rootPage} could not be read.",
                PageId = rootPage,
            });
            return;
        }

        if (root.PageType is not (PageConstants.PageTypeLeaf or PageConstants.PageTypeInterior))
        {
            issues.Add(new IntegrityIssue
            {
                Code = "CATALOG_ROOT_BAD_PAGE_TYPE",
                Severity = InspectSeverity.Error,
                Message = $"{scope} root page {rootPage} has invalid page type {root.PageType}.",
                PageId = rootPage,
            });
            return;
        }

        _ = InspectorEngine.WalkBTree(rootPage, snapshot.Pages, snapshot.PhysicalPageCount, issues, scope);
    }

    private readonly record struct NamedRoot(string Name, uint RootPage);

    private sealed class CatalogRoots
    {
        public uint IndexCatalogRootPage { get; init; }
        public uint ViewCatalogRootPage { get; init; }
        public uint TriggerCatalogRootPage { get; init; }

        public required List<NamedRoot> TableRoots { get; init; }
        public required List<NamedRoot> IndexRoots { get; init; }
        public required List<NamedRoot> ViewRoots { get; init; }
        public required List<NamedRoot> TriggerRoots { get; init; }
    }

    private static CatalogRoots CollectCatalogRoots(
        HashSet<uint> schemaTreePages,
        IReadOnlyDictionary<uint, InspectorEngine.ParsedPage> pages,
        int physicalPageCount,
        List<IntegrityIssue> issues)
    {
        uint indexCatalogRoot = 0;
        uint viewCatalogRoot = 0;
        uint triggerCatalogRoot = 0;
        var tableRoots = new List<NamedRoot>();

        foreach (uint pageId in schemaTreePages)
        {
            if (!pages.TryGetValue(pageId, out var page) || page.PageType != PageConstants.PageTypeLeaf)
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

                if (cell.Key.Value == InspectorEngine.ViewCatalogSentinel)
                {
                    viewCatalogRoot = rootPage;
                    continue;
                }

                if (cell.Key.Value == InspectorEngine.TriggerCatalogSentinel)
                {
                    triggerCatalogRoot = rootPage;
                    continue;
                }

                try
                {
                    TableSchema tableSchema = SchemaSerializer.Deserialize(cell.Payload.AsSpan(4));
                    tableRoots.Add(new NamedRoot(tableSchema.TableName, rootPage));
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

        var indexRoots = CollectIndexRoots(indexCatalogRoot, pages, physicalPageCount, issues);
        var viewRoots = CollectViewRoots(viewCatalogRoot, pages, physicalPageCount, issues);
        var triggerRoots = CollectTriggerRoots(triggerCatalogRoot, pages, physicalPageCount, issues);

        return new CatalogRoots
        {
            IndexCatalogRootPage = indexCatalogRoot,
            ViewCatalogRootPage = viewCatalogRoot,
            TriggerCatalogRootPage = triggerCatalogRoot,
            TableRoots = tableRoots,
            IndexRoots = indexRoots,
            ViewRoots = viewRoots,
            TriggerRoots = triggerRoots,
        };
    }

    private static List<NamedRoot> CollectIndexRoots(
        uint indexCatalogRoot,
        IReadOnlyDictionary<uint, InspectorEngine.ParsedPage> pages,
        int physicalPageCount,
        List<IntegrityIssue> issues)
    {
        var roots = new List<NamedRoot>();
        if (indexCatalogRoot == 0)
            return roots;

        var treePages = InspectorEngine.WalkBTree(indexCatalogRoot, pages, physicalPageCount, issues, "index-catalog");
        foreach (uint pageId in treePages)
        {
            if (!pages.TryGetValue(pageId, out var page) || page.PageType != PageConstants.PageTypeLeaf)
                continue;

            foreach (var cell in page.LeafCells)
            {
                if (cell.Payload is null || cell.Payload.Length < 4)
                    continue;

                try
                {
                    uint root = BinaryPrimitives.ReadUInt32LittleEndian(cell.Payload.AsSpan(0, 4));
                    IndexSchema schema = SchemaSerializer.DeserializeIndex(cell.Payload.AsSpan(4));
                    roots.Add(new NamedRoot(schema.IndexName, root));
                }
                catch
                {
                    issues.Add(new IntegrityIssue
                    {
                        Code = "CATALOG_INDEX_SCHEMA_DECODE_FAILED",
                        Severity = InspectSeverity.Warning,
                        Message = "Failed to decode index schema entry.",
                        PageId = pageId,
                        Offset = cell.CellOffset,
                    });
                }
            }
        }

        return roots;
    }

    private static List<NamedRoot> CollectViewRoots(
        uint viewCatalogRoot,
        IReadOnlyDictionary<uint, InspectorEngine.ParsedPage> pages,
        int physicalPageCount,
        List<IntegrityIssue> issues)
    {
        var roots = new List<NamedRoot>();
        if (viewCatalogRoot == 0)
            return roots;

        var treePages = InspectorEngine.WalkBTree(viewCatalogRoot, pages, physicalPageCount, issues, "view-catalog");
        foreach (uint pageId in treePages)
        {
            if (!pages.TryGetValue(pageId, out var page) || page.PageType != PageConstants.PageTypeLeaf)
                continue;

            foreach (var cell in page.LeafCells)
            {
                if (cell.Payload is null)
                    continue;

                try
                {
                    string name = ReadLengthPrefixedString(cell.Payload.AsSpan(), 0, out _);
                    roots.Add(new NamedRoot(name, viewCatalogRoot));
                }
                catch
                {
                    issues.Add(new IntegrityIssue
                    {
                        Code = "CATALOG_VIEW_SCHEMA_DECODE_FAILED",
                        Severity = InspectSeverity.Warning,
                        Message = "Failed to decode view catalog entry.",
                        PageId = pageId,
                        Offset = cell.CellOffset,
                    });
                }
            }
        }

        return roots;
    }

    private static List<NamedRoot> CollectTriggerRoots(
        uint triggerCatalogRoot,
        IReadOnlyDictionary<uint, InspectorEngine.ParsedPage> pages,
        int physicalPageCount,
        List<IntegrityIssue> issues)
    {
        var roots = new List<NamedRoot>();
        if (triggerCatalogRoot == 0)
            return roots;

        var treePages = InspectorEngine.WalkBTree(triggerCatalogRoot, pages, physicalPageCount, issues, "trigger-catalog");
        foreach (uint pageId in treePages)
        {
            if (!pages.TryGetValue(pageId, out var page) || page.PageType != PageConstants.PageTypeLeaf)
                continue;

            foreach (var cell in page.LeafCells)
            {
                if (cell.Payload is null)
                    continue;

                try
                {
                    TriggerSchema trigger = SchemaSerializer.DeserializeTrigger(cell.Payload.AsSpan());
                    roots.Add(new NamedRoot(trigger.TriggerName, triggerCatalogRoot));
                }
                catch
                {
                    issues.Add(new IntegrityIssue
                    {
                        Code = "CATALOG_TRIGGER_SCHEMA_DECODE_FAILED",
                        Severity = InspectSeverity.Warning,
                        Message = "Failed to decode trigger catalog entry.",
                        PageId = pageId,
                        Offset = cell.CellOffset,
                    });
                }
            }
        }

        return roots;
    }

    private static string ReadLengthPrefixedString(ReadOnlySpan<byte> data, int pos, out int newPos)
    {
        int len = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(pos, 4));
        string s = System.Text.Encoding.UTF8.GetString(data.Slice(pos + 4, len));
        newPos = pos + 4 + len;
        return s;
    }
}
