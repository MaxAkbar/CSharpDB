using System.Buffers.Binary;

namespace CSharpDB.Storage.Diagnostics.Internal;

internal static class InspectorEngine
{
    internal const long IndexCatalogSentinel = long.MaxValue;
    internal const long ViewCatalogSentinel = long.MaxValue - 1;
    internal const long TriggerCatalogSentinel = long.MaxValue - 2;
    internal const long TableStatsCatalogSentinel = long.MaxValue - 3;
    internal const long ColumnStatsCatalogSentinel = long.MaxValue - 4;

    internal sealed class ParsedLeafCell
    {
        public int CellIndex { get; init; }
        public ushort CellOffset { get; init; }
        public int HeaderBytes { get; init; }
        public int CellTotalBytes { get; init; }
        public long? Key { get; init; }
        public byte[]? Payload { get; init; }
    }

    internal sealed class ParsedInteriorCell
    {
        public int CellIndex { get; init; }
        public ushort CellOffset { get; init; }
        public int HeaderBytes { get; init; }
        public int CellTotalBytes { get; init; }
        public uint? LeftChildPage { get; init; }
        public long? Key { get; init; }
    }

    internal sealed class ParsedPage
    {
        public required uint PageId { get; init; }
        public required int BaseOffset { get; init; }
        public required byte PageType { get; init; }
        public required ushort CellCount { get; init; }
        public required ushort CellContentStart { get; init; }
        public required uint RightChildOrNextLeaf { get; init; }
        public required int FreeSpaceBytes { get; init; }

        public required List<ushort> CellOffsets { get; init; }
        public required List<ParsedLeafCell> LeafCells { get; init; }
        public required List<ParsedInteriorCell> InteriorCells { get; init; }
        public required List<uint> ChildPageReferences { get; init; }
    }

    internal sealed class DatabaseSnapshot
    {
        public required string DatabasePath { get; init; }
        public required FileHeaderReport Header { get; init; }
        public required int PhysicalPageCount { get; init; }
        public required Dictionary<uint, ParsedPage> Pages { get; init; }
        public required List<IntegrityIssue> Issues { get; init; }
    }

    internal static async ValueTask<DatabaseSnapshot> ReadDatabaseSnapshotAsync(
        string databasePath,
        bool captureLeafPayload,
        CancellationToken ct = default)
    {
        if (!File.Exists(databasePath))
            throw new FileNotFoundException($"Database file '{databasePath}' not found.", databasePath);

        var issues = new List<IntegrityIssue>();

        await using var stream = new FileStream(
            databasePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite,
            bufferSize: 4096,
            useAsync: true);

        long fileLength = stream.Length;
        int physicalPageCount = checked((int)(fileLength / PageConstants.PageSize));
        int trailingBytes = checked((int)(fileLength % PageConstants.PageSize));

        if (trailingBytes > 0)
        {
            issues.Add(new IntegrityIssue
            {
                Code = "DB_FILE_TRAILING_BYTES",
                Severity = InspectSeverity.Warning,
                Message = $"Database file has {trailingBytes} trailing byte(s) beyond full pages.",
                Offset = fileLength - trailingBytes,
            });
        }

        byte[] fileHeader = new byte[PageConstants.FileHeaderSize];
        int headerRead = await ReadAtAsync(stream, 0, fileHeader, ct);
        if (headerRead < PageConstants.FileHeaderSize)
        {
            issues.Add(new IntegrityIssue
            {
                Code = "DB_HEADER_SHORT",
                Severity = InspectSeverity.Error,
                Message = $"File header is too short ({headerRead} bytes).",
                Offset = 0,
            });
        }

        string magic = headerRead >= 4 ? System.Text.Encoding.ASCII.GetString(fileHeader.AsSpan(0, 4)) : string.Empty;
        bool magicValid = headerRead >= 4 && fileHeader.AsSpan(0, 4).SequenceEqual(PageConstants.MagicBytes);

        int version = headerRead >= 8 ? BinaryPrimitives.ReadInt32LittleEndian(fileHeader.AsSpan(PageConstants.VersionOffset, 4)) : 0;
        bool versionValid = version == PageConstants.FormatVersion;

        int pageSize = headerRead >= 12 ? BinaryPrimitives.ReadInt32LittleEndian(fileHeader.AsSpan(PageConstants.PageSizeOffset, 4)) : 0;
        bool pageSizeValid = pageSize == PageConstants.PageSize;

        uint declaredPageCount = headerRead >= 16 ? BinaryPrimitives.ReadUInt32LittleEndian(fileHeader.AsSpan(PageConstants.PageCountOffset, 4)) : 0;
        uint schemaRootPage = headerRead >= 20 ? BinaryPrimitives.ReadUInt32LittleEndian(fileHeader.AsSpan(PageConstants.SchemaRootPageOffset, 4)) : 0;
        uint freelistHead = headerRead >= 24 ? BinaryPrimitives.ReadUInt32LittleEndian(fileHeader.AsSpan(PageConstants.FreelistHeadOffset, 4)) : 0;
        uint changeCounter = headerRead >= 28 ? BinaryPrimitives.ReadUInt32LittleEndian(fileHeader.AsSpan(PageConstants.ChangeCounterOffset, 4)) : 0;

        if (!magicValid)
        {
            issues.Add(new IntegrityIssue
            {
                Code = "DB_HEADER_BAD_MAGIC",
                Severity = InspectSeverity.Error,
                Message = $"Unexpected database magic '{magic}'.",
                Offset = 0,
            });
        }

        if (!versionValid)
        {
            issues.Add(new IntegrityIssue
            {
                Code = "DB_HEADER_BAD_VERSION",
                Severity = InspectSeverity.Error,
                Message = $"Unsupported format version {version}.",
                Offset = PageConstants.VersionOffset,
            });
        }

        if (!pageSizeValid)
        {
            issues.Add(new IntegrityIssue
            {
                Code = "DB_HEADER_BAD_PAGE_SIZE",
                Severity = InspectSeverity.Error,
                Message = $"Unexpected page size {pageSize}; expected {PageConstants.PageSize}.",
                Offset = PageConstants.PageSizeOffset,
            });
        }

        if (declaredPageCount != (uint)physicalPageCount)
        {
            issues.Add(new IntegrityIssue
            {
                Code = "DB_PAGE_COUNT_MISMATCH",
                Severity = InspectSeverity.Warning,
                Message = $"Header page count ({declaredPageCount}) does not match physical page count ({physicalPageCount}).",
                Offset = PageConstants.PageCountOffset,
            });
        }

        var header = new FileHeaderReport
        {
            FileLengthBytes = fileLength,
            PhysicalPageCount = physicalPageCount,
            Magic = magic,
            MagicValid = magicValid,
            Version = version,
            VersionValid = versionValid,
            PageSize = pageSize,
            PageSizeValid = pageSizeValid,
            DeclaredPageCount = declaredPageCount,
            DeclaredPageCountMatchesPhysical = declaredPageCount == (uint)physicalPageCount,
            SchemaRootPage = schemaRootPage,
            FreelistHead = freelistHead,
            ChangeCounter = changeCounter,
        };

        var pages = new Dictionary<uint, ParsedPage>();
        byte[] pageBuffer = new byte[PageConstants.PageSize];

        for (uint pageId = 0; pageId < physicalPageCount; pageId++)
        {
            int read = await ReadAtAsync(stream, (long)pageId * PageConstants.PageSize, pageBuffer, ct);
            if (read != PageConstants.PageSize)
            {
                issues.Add(new IntegrityIssue
                {
                    Code = "DB_PAGE_SHORT_READ",
                    Severity = InspectSeverity.Error,
                    Message = $"Failed to read full page {pageId}; got {read} bytes.",
                    PageId = pageId,
                    Offset = (long)pageId * PageConstants.PageSize,
                });
                continue;
            }

            ParsePageResult parsed = ParsePage(pageId, pageBuffer, captureLeafPayload);
            pages[pageId] = parsed.Page;
            issues.AddRange(parsed.Issues);
        }

        return new DatabaseSnapshot
        {
            DatabasePath = databasePath,
            Header = header,
            PhysicalPageCount = physicalPageCount,
            Pages = pages,
            Issues = issues,
        };
    }

    internal static async ValueTask<byte[]?> ReadPageBytesAsync(string databasePath, uint pageId, CancellationToken ct = default)
    {
        if (!File.Exists(databasePath))
            return null;

        await using var stream = new FileStream(
            databasePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite,
            bufferSize: 4096,
            useAsync: true);

        long offset = (long)pageId * PageConstants.PageSize;
        if (offset + PageConstants.PageSize > stream.Length)
            return null;

        var buffer = new byte[PageConstants.PageSize];
        int read = await ReadAtAsync(stream, offset, buffer, ct);
        return read == PageConstants.PageSize ? buffer : null;
    }

    internal readonly record struct ParsePageResult(ParsedPage Page, List<IntegrityIssue> Issues);

    internal static ParsePageResult ParsePage(uint pageId, byte[] pageBytes, bool captureLeafPayload)
    {
        var issues = new List<IntegrityIssue>();
        int baseOffset = PageConstants.ContentOffset(pageId);

        if (baseOffset + PageConstants.SlottedPageHeaderSize > pageBytes.Length)
        {
            var empty = new ParsedPage
            {
                PageId = pageId,
                BaseOffset = baseOffset,
                PageType = 0,
                CellCount = 0,
                CellContentStart = 0,
                RightChildOrNextLeaf = 0,
                FreeSpaceBytes = 0,
                CellOffsets = [],
                LeafCells = [],
                InteriorCells = [],
                ChildPageReferences = [],
            };

            issues.Add(new IntegrityIssue
            {
                Code = "PAGE_HEADER_OUT_OF_RANGE",
                Severity = InspectSeverity.Error,
                Message = "Slotted page header is out of range.",
                PageId = pageId,
                Offset = baseOffset,
            });

            return new ParsePageResult(empty, issues);
        }

        byte pageType = pageBytes[baseOffset + PageConstants.PageTypeOffset];
        ushort cellCount = BinaryPrimitives.ReadUInt16LittleEndian(pageBytes.AsSpan(baseOffset + PageConstants.CellCountOffset, 2));
        ushort cellContentStart = BinaryPrimitives.ReadUInt16LittleEndian(pageBytes.AsSpan(baseOffset + PageConstants.FreeSpaceStartOffset, 2));
        uint rightChildOrNextLeaf = BinaryPrimitives.ReadUInt32LittleEndian(pageBytes.AsSpan(baseOffset + PageConstants.RightChildOffset, 4));

        if (pageType != PageConstants.PageTypeLeaf && pageType != PageConstants.PageTypeInterior)
        {
            if (pageType != PageConstants.PageTypeFreelist)
            {
                issues.Add(new IntegrityIssue
                {
                    Code = "PAGE_TYPE_UNKNOWN",
                    Severity = InspectSeverity.Warning,
                    Message = $"Page {pageId} has unknown page type {pageType}.",
                    PageId = pageId,
                    Offset = baseOffset + PageConstants.PageTypeOffset,
                });
            }

            var nonTreePage = new ParsedPage
            {
                PageId = pageId,
                BaseOffset = baseOffset,
                PageType = pageType,
                CellCount = cellCount,
                CellContentStart = cellContentStart,
                RightChildOrNextLeaf = rightChildOrNextLeaf,
                FreeSpaceBytes = 0,
                CellOffsets = [],
                LeafCells = [],
                InteriorCells = [],
                ChildPageReferences = [],
            };

            return new ParsePageResult(nonTreePage, issues);
        }

        int pointerStart = baseOffset + PageConstants.SlottedPageHeaderSize;
        int maxPointers = Math.Max(0, (PageConstants.PageSize - pointerStart) / PageConstants.CellPointerSize);
        int parseableCellCount = Math.Min(cellCount, maxPointers);

        if (cellCount > maxPointers)
        {
            issues.Add(new IntegrityIssue
            {
                Code = "PAGE_CELL_COUNT_OVERFLOW",
                Severity = InspectSeverity.Error,
                Message = $"Cell count {cellCount} exceeds pointer capacity {maxPointers}.",
                PageId = pageId,
                Offset = pointerStart,
            });
        }

        int pointerEnd = pointerStart + parseableCellCount * PageConstants.CellPointerSize;
        if (cellContentStart > PageConstants.PageSize)
        {
            issues.Add(new IntegrityIssue
            {
                Code = "PAGE_CELL_CONTENT_START_OOB",
                Severity = InspectSeverity.Error,
                Message = $"Cell content start {cellContentStart} is beyond page size.",
                PageId = pageId,
                Offset = baseOffset + PageConstants.FreeSpaceStartOffset,
            });
        }
        else if (cellContentStart < pointerEnd)
        {
            issues.Add(new IntegrityIssue
            {
                Code = "PAGE_CELL_CONTENT_OVERLAP",
                Severity = InspectSeverity.Error,
                Message = $"Cell content start {cellContentStart} overlaps pointer array end {pointerEnd}.",
                PageId = pageId,
                Offset = baseOffset + PageConstants.FreeSpaceStartOffset,
            });
        }

        var cellOffsets = new List<ushort>(parseableCellCount);
        var seenOffsets = new HashSet<ushort>();

        for (int i = 0; i < parseableCellCount; i++)
        {
            int ptrOffset = pointerStart + i * PageConstants.CellPointerSize;
            ushort cellOffset = BinaryPrimitives.ReadUInt16LittleEndian(pageBytes.AsSpan(ptrOffset, 2));
            cellOffsets.Add(cellOffset);

            if (!seenOffsets.Add(cellOffset))
            {
                issues.Add(new IntegrityIssue
                {
                    Code = "PAGE_DUPLICATE_CELL_POINTER",
                    Severity = InspectSeverity.Warning,
                    Message = $"Duplicate cell pointer offset {cellOffset} at index {i}.",
                    PageId = pageId,
                    Offset = ptrOffset,
                });
            }

            if (cellOffset < baseOffset || cellOffset >= PageConstants.PageSize)
            {
                issues.Add(new IntegrityIssue
                {
                    Code = "PAGE_CELL_POINTER_OOB",
                    Severity = InspectSeverity.Error,
                    Message = $"Cell pointer {cellOffset} is out of bounds.",
                    PageId = pageId,
                    Offset = ptrOffset,
                });
            }
        }

        var leafCells = new List<ParsedLeafCell>();
        var interiorCells = new List<ParsedInteriorCell>();
        var childRefs = new List<uint>();

        for (int i = 0; i < parseableCellCount; i++)
        {
            ushort cellOffset = cellOffsets[i];
            if (cellOffset < baseOffset || cellOffset >= PageConstants.PageSize)
                continue;

            ulong payloadSize;
            int headerBytes;
            try
            {
                payloadSize = Varint.Read(pageBytes.AsSpan(cellOffset), out headerBytes);
            }
            catch
            {
                issues.Add(new IntegrityIssue
                {
                    Code = "CELL_VARINT_INVALID",
                    Severity = InspectSeverity.Error,
                    Message = $"Cell {i} has invalid varint header.",
                    PageId = pageId,
                    Offset = cellOffset,
                });
                continue;
            }

            long cellTotal = headerBytes + (long)payloadSize;
            if (headerBytes <= 0 || headerBytes > 10 || cellTotal <= 0)
            {
                issues.Add(new IntegrityIssue
                {
                    Code = "CELL_HEADER_INVALID",
                    Severity = InspectSeverity.Error,
                    Message = $"Cell {i} has invalid header length or payload size.",
                    PageId = pageId,
                    Offset = cellOffset,
                });
                continue;
            }

            if (cellOffset + cellTotal > PageConstants.PageSize)
            {
                issues.Add(new IntegrityIssue
                {
                    Code = "CELL_TOTAL_SIZE_OOB",
                    Severity = InspectSeverity.Error,
                    Message = $"Cell {i} extends beyond page boundary.",
                    PageId = pageId,
                    Offset = cellOffset,
                });
                continue;
            }

            if (pageType == PageConstants.PageTypeLeaf)
            {
                if (payloadSize < 8)
                {
                    issues.Add(new IntegrityIssue
                    {
                        Code = "LEAF_CELL_PAYLOAD_TOO_SMALL",
                        Severity = InspectSeverity.Error,
                        Message = $"Leaf cell {i} payload is smaller than key size.",
                        PageId = pageId,
                        Offset = cellOffset,
                    });
                    continue;
                }

                int keyOffset = cellOffset + headerBytes;
                if (keyOffset + 8 > PageConstants.PageSize)
                {
                    issues.Add(new IntegrityIssue
                    {
                        Code = "LEAF_CELL_KEY_OOB",
                        Severity = InspectSeverity.Error,
                        Message = $"Leaf cell {i} key bytes are out of bounds.",
                        PageId = pageId,
                        Offset = keyOffset,
                    });
                    continue;
                }

                long key = BinaryPrimitives.ReadInt64LittleEndian(pageBytes.AsSpan(keyOffset, 8));
                int payloadLen = (int)payloadSize - 8;
                int payloadStart = keyOffset + 8;
                if (payloadStart + payloadLen > PageConstants.PageSize || payloadLen < 0)
                {
                    issues.Add(new IntegrityIssue
                    {
                        Code = "LEAF_CELL_PAYLOAD_OOB",
                        Severity = InspectSeverity.Error,
                        Message = $"Leaf cell {i} payload bytes are out of bounds.",
                        PageId = pageId,
                        Offset = payloadStart,
                    });
                    continue;
                }

                byte[]? payload = captureLeafPayload
                    ? pageBytes.AsSpan(payloadStart, payloadLen).ToArray()
                    : null;

                leafCells.Add(new ParsedLeafCell
                {
                    CellIndex = i,
                    CellOffset = cellOffset,
                    HeaderBytes = headerBytes,
                    CellTotalBytes = checked((int)cellTotal),
                    Key = key,
                    Payload = payload,
                });
            }
            else if (pageType == PageConstants.PageTypeInterior)
            {
                if (payloadSize < 12)
                {
                    issues.Add(new IntegrityIssue
                    {
                        Code = "INTERIOR_CELL_PAYLOAD_TOO_SMALL",
                        Severity = InspectSeverity.Error,
                        Message = $"Interior cell {i} payload is smaller than expected 12 bytes.",
                        PageId = pageId,
                        Offset = cellOffset,
                    });
                    continue;
                }

                int leftChildOffset = cellOffset + headerBytes;
                if (leftChildOffset + 12 > PageConstants.PageSize)
                {
                    issues.Add(new IntegrityIssue
                    {
                        Code = "INTERIOR_CELL_BYTES_OOB",
                        Severity = InspectSeverity.Error,
                        Message = $"Interior cell {i} bytes are out of bounds.",
                        PageId = pageId,
                        Offset = leftChildOffset,
                    });
                    continue;
                }

                uint leftChild = BinaryPrimitives.ReadUInt32LittleEndian(pageBytes.AsSpan(leftChildOffset, 4));
                long key = BinaryPrimitives.ReadInt64LittleEndian(pageBytes.AsSpan(leftChildOffset + 4, 8));

                interiorCells.Add(new ParsedInteriorCell
                {
                    CellIndex = i,
                    CellOffset = cellOffset,
                    HeaderBytes = headerBytes,
                    CellTotalBytes = checked((int)cellTotal),
                    LeftChildPage = leftChild,
                    Key = key,
                });

                childRefs.Add(leftChild);
            }
        }

        if (pageType == PageConstants.PageTypeLeaf)
        {
            for (int i = 1; i < leafCells.Count; i++)
            {
                long? prev = leafCells[i - 1].Key;
                long? curr = leafCells[i].Key;
                if (prev.HasValue && curr.HasValue && prev.Value > curr.Value)
                {
                    issues.Add(new IntegrityIssue
                    {
                        Code = "BTREE_LEAF_KEY_ORDER",
                        Severity = InspectSeverity.Error,
                        Message = $"Leaf keys are not sorted at cell indices {i - 1} and {i}.",
                        PageId = pageId,
                    });
                    break;
                }
            }
        }
        else if (pageType == PageConstants.PageTypeInterior)
        {
            if (rightChildOrNextLeaf != PageConstants.NullPageId)
                childRefs.Add(rightChildOrNextLeaf);
        }

        int freeSpace = Math.Max(0, cellContentStart - pointerEnd);

        var parsedPage = new ParsedPage
        {
            PageId = pageId,
            BaseOffset = baseOffset,
            PageType = pageType,
            CellCount = cellCount,
            CellContentStart = cellContentStart,
            RightChildOrNextLeaf = rightChildOrNextLeaf,
            FreeSpaceBytes = freeSpace,
            CellOffsets = cellOffsets,
            LeafCells = leafCells,
            InteriorCells = interiorCells,
            ChildPageReferences = childRefs,
        };

        return new ParsePageResult(parsedPage, issues);
    }

    internal static HashSet<uint> WalkBTree(
        uint rootPageId,
        IReadOnlyDictionary<uint, ParsedPage> pages,
        int physicalPageCount,
        List<IntegrityIssue> issues,
        string scope)
    {
        var visited = new HashSet<uint>();
        if (rootPageId == PageConstants.NullPageId)
            return visited;

        var stack = new Stack<uint>();
        stack.Push(rootPageId);

        while (stack.Count > 0)
        {
            uint pageId = stack.Pop();
            if (!visited.Add(pageId))
                continue;

            if (pageId >= physicalPageCount)
            {
                issues.Add(new IntegrityIssue
                {
                    Code = "BTREE_CHILD_OUT_OF_RANGE",
                    Severity = InspectSeverity.Error,
                    Message = $"{scope}: page reference {pageId} is outside physical page range.",
                    PageId = pageId,
                });
                continue;
            }

            if (!pages.TryGetValue(pageId, out ParsedPage? page))
            {
                issues.Add(new IntegrityIssue
                {
                    Code = "BTREE_PAGE_MISSING",
                    Severity = InspectSeverity.Error,
                    Message = $"{scope}: page {pageId} is missing from parsed map.",
                    PageId = pageId,
                });
                continue;
            }

            if (page.PageType != PageConstants.PageTypeLeaf && page.PageType != PageConstants.PageTypeInterior)
            {
                issues.Add(new IntegrityIssue
                {
                    Code = "BTREE_PAGE_TYPE_INVALID",
                    Severity = InspectSeverity.Error,
                    Message = $"{scope}: page {pageId} has invalid B+tree page type {page.PageType}.",
                    PageId = pageId,
                });
                continue;
            }

            if (page.PageType == PageConstants.PageTypeInterior)
            {
                foreach (uint childPage in page.ChildPageReferences)
                {
                    if (childPage == PageConstants.NullPageId)
                    {
                        issues.Add(new IntegrityIssue
                        {
                            Code = "BTREE_NULL_CHILD_REFERENCE",
                            Severity = InspectSeverity.Warning,
                            Message = $"{scope}: interior page {pageId} contains null child reference.",
                            PageId = pageId,
                        });
                        continue;
                    }

                    stack.Push(childPage);
                }
            }
        }

        return visited;
    }

    internal static string PageTypeName(byte pageType) => pageType switch
    {
        PageConstants.PageTypeLeaf => "leaf",
        PageConstants.PageTypeInterior => "interior",
        PageConstants.PageTypeFreelist => "freelist",
        _ => "unknown",
    };

    internal static string BuildHexDump(ReadOnlySpan<byte> bytes)
    {
        var sb = new System.Text.StringBuilder(bytes.Length * 4);
        const int width = 16;
        for (int i = 0; i < bytes.Length; i += width)
        {
            int lineLen = Math.Min(width, bytes.Length - i);
            sb.Append(i.ToString("X4"));
            sb.Append(": ");

            for (int j = 0; j < width; j++)
            {
                if (j < lineLen)
                    sb.Append(bytes[i + j].ToString("X2"));
                else
                    sb.Append("  ");

                if (j != width - 1)
                    sb.Append(' ');
            }

            sb.Append("  |");
            for (int j = 0; j < lineLen; j++)
            {
                byte b = bytes[i + j];
                sb.Append(b >= 32 && b <= 126 ? (char)b : '.');
            }
            sb.Append('|');
            sb.AppendLine();
        }

        return sb.ToString();
    }

    internal static uint Checksum(ReadOnlySpan<byte> data)
    {
        uint sum = 0;
        int i = 0;
        for (; i + 3 < data.Length; i += 4)
            sum += BitConverter.ToUInt32(data[i..]);
        for (; i < data.Length; i++)
            sum += data[i];
        return sum;
    }

    private static async ValueTask<int> ReadAtAsync(FileStream stream, long offset, Memory<byte> buffer, CancellationToken ct)
    {
        stream.Position = offset;
        int total = 0;
        while (total < buffer.Length)
        {
            int read = await stream.ReadAsync(buffer[total..], ct);
            if (read == 0)
                break;
            total += read;
        }

        return total;
    }
}
