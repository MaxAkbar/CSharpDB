using System.Buffers.Binary;
using CSharpDB.Storage.Diagnostics.Internal;

namespace CSharpDB.Storage.Diagnostics;

public static class WalInspector
{
    public static async ValueTask<WalInspectReport> InspectAsync(
        string dbPath,
        DatabaseInspectOptions? options = null,
        CancellationToken ct = default)
    {
        _ = options;

        string walPath = dbPath + ".wal";
        var issues = new List<IntegrityIssue>();

        if (!File.Exists(walPath))
        {
            return new WalInspectReport
            {
                DatabasePath = dbPath,
                WalPath = walPath,
                Exists = false,
                FileLengthBytes = 0,
                FullFrameCount = 0,
                CommitFrameCount = 0,
                TrailingBytes = 0,
                Magic = string.Empty,
                MagicValid = false,
                Version = 0,
                VersionValid = false,
                PageSize = 0,
                PageSizeValid = false,
                Salt1 = 0,
                Salt2 = 0,
                Issues = issues,
            };
        }

        await using var stream = new FileStream(
            walPath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite,
            bufferSize: 4096,
            useAsync: true);

        long fileLength = stream.Length;
        var header = new byte[PageConstants.WalHeaderSize];
        int headerRead = await ReadAtAsync(stream, 0, header, ct);

        string magic = headerRead >= 4 ? System.Text.Encoding.ASCII.GetString(header.AsSpan(0, 4)) : string.Empty;
        bool magicValid = headerRead >= 4 && header.AsSpan(0, 4).SequenceEqual(PageConstants.WalMagic);

        int version = headerRead >= 8 ? BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(4, 4)) : 0;
        bool versionValid = version == 1;

        int pageSize = headerRead >= 12 ? BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(8, 4)) : 0;
        bool pageSizeValid = pageSize == PageConstants.PageSize;

        uint salt1 = headerRead >= 20 ? BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(16, 4)) : 0;
        uint salt2 = headerRead >= 24 ? BinaryPrimitives.ReadUInt32LittleEndian(header.AsSpan(20, 4)) : 0;

        if (headerRead < PageConstants.WalHeaderSize)
        {
            issues.Add(new IntegrityIssue
            {
                Code = "WAL_HEADER_SHORT",
                Severity = InspectSeverity.Error,
                Message = $"WAL header is too short ({headerRead} bytes).",
                Offset = 0,
            });
        }

        if (!magicValid)
        {
            issues.Add(new IntegrityIssue
            {
                Code = "WAL_HEADER_BAD_MAGIC",
                Severity = InspectSeverity.Error,
                Message = $"Unexpected WAL magic '{magic}'.",
                Offset = 0,
            });
        }

        if (!versionValid)
        {
            issues.Add(new IntegrityIssue
            {
                Code = "WAL_HEADER_BAD_VERSION",
                Severity = InspectSeverity.Error,
                Message = $"Unsupported WAL version {version}.",
                Offset = 4,
            });
        }

        if (!pageSizeValid)
        {
            issues.Add(new IntegrityIssue
            {
                Code = "WAL_HEADER_BAD_PAGE_SIZE",
                Severity = InspectSeverity.Error,
                Message = $"Unexpected WAL page size {pageSize}; expected {PageConstants.PageSize}.",
                Offset = 8,
            });
        }

        long frameBytes = Math.Max(0, fileLength - PageConstants.WalHeaderSize);
        int fullFrameCount = checked((int)(frameBytes / PageConstants.WalFrameSize));
        int trailingBytes = checked((int)(frameBytes % PageConstants.WalFrameSize));
        int commitFrameCount = 0;

        if (trailingBytes > 0)
        {
            issues.Add(new IntegrityIssue
            {
                Code = "WAL_TRAILING_PARTIAL_FRAME",
                Severity = InspectSeverity.Warning,
                Message = $"WAL has {trailingBytes} trailing byte(s) that do not form a complete frame.",
                Offset = PageConstants.WalHeaderSize + fullFrameCount * (long)PageConstants.WalFrameSize,
            });
        }

        var frameHeader = new byte[PageConstants.WalFrameHeaderSize];
        var pageData = new byte[PageConstants.PageSize];

        for (int i = 0; i < fullFrameCount; i++)
        {
            long frameOffset = PageConstants.WalHeaderSize + (long)i * PageConstants.WalFrameSize;

            int headerBytes = await ReadAtAsync(stream, frameOffset, frameHeader, ct);
            if (headerBytes != PageConstants.WalFrameHeaderSize)
            {
                issues.Add(new IntegrityIssue
                {
                    Code = "WAL_FRAME_HEADER_SHORT",
                    Severity = InspectSeverity.Error,
                    Message = $"Frame {i} header is short ({headerBytes} bytes).",
                    Offset = frameOffset,
                });
                continue;
            }

            int dataBytes = await ReadAtAsync(stream, frameOffset + PageConstants.WalFrameHeaderSize, pageData, ct);
            if (dataBytes != PageConstants.PageSize)
            {
                issues.Add(new IntegrityIssue
                {
                    Code = "WAL_FRAME_PAGE_SHORT",
                    Severity = InspectSeverity.Error,
                    Message = $"Frame {i} page data is short ({dataBytes} bytes).",
                    Offset = frameOffset + PageConstants.WalFrameHeaderSize,
                });
                continue;
            }

            uint pageId = BinaryPrimitives.ReadUInt32LittleEndian(frameHeader.AsSpan(0, 4));
            uint dbPageCount = BinaryPrimitives.ReadUInt32LittleEndian(frameHeader.AsSpan(4, 4));
            uint frameSalt1 = BinaryPrimitives.ReadUInt32LittleEndian(frameHeader.AsSpan(8, 4));
            uint frameSalt2 = BinaryPrimitives.ReadUInt32LittleEndian(frameHeader.AsSpan(12, 4));
            uint expectedHeaderChecksum = BinaryPrimitives.ReadUInt32LittleEndian(frameHeader.AsSpan(16, 4));
            uint expectedDataChecksum = BinaryPrimitives.ReadUInt32LittleEndian(frameHeader.AsSpan(20, 4));

            if (frameSalt1 != salt1 || frameSalt2 != salt2)
            {
                issues.Add(new IntegrityIssue
                {
                    Code = "WAL_FRAME_SALT_MISMATCH",
                    Severity = InspectSeverity.Error,
                    Message = $"Frame {i} has salt mismatch.",
                    PageId = pageId,
                    Offset = frameOffset + 8,
                });
            }

            uint actualHeaderChecksum = InspectorEngine.Checksum(frameHeader.AsSpan(0, 16));
            if (actualHeaderChecksum != expectedHeaderChecksum)
            {
                issues.Add(new IntegrityIssue
                {
                    Code = "WAL_FRAME_HEADER_CHECKSUM_MISMATCH",
                    Severity = InspectSeverity.Error,
                    Message = $"Frame {i} header checksum mismatch.",
                    PageId = pageId,
                    Offset = frameOffset + 16,
                });
            }

            uint actualDataChecksum = InspectorEngine.Checksum(pageData);
            if (actualDataChecksum != expectedDataChecksum)
            {
                issues.Add(new IntegrityIssue
                {
                    Code = "WAL_FRAME_DATA_CHECKSUM_MISMATCH",
                    Severity = InspectSeverity.Error,
                    Message = $"Frame {i} data checksum mismatch.",
                    PageId = pageId,
                    Offset = frameOffset + 20,
                });
            }

            if (dbPageCount != 0)
                commitFrameCount++;
        }

        if (fullFrameCount > 0 && commitFrameCount == 0)
        {
            issues.Add(new IntegrityIssue
            {
                Code = "WAL_NO_COMMIT_MARKER",
                Severity = InspectSeverity.Warning,
                Message = "WAL contains frames but no commit marker (dbPageCount != 0).",
                Offset = PageConstants.WalHeaderSize,
            });
        }

        return new WalInspectReport
        {
            DatabasePath = dbPath,
            WalPath = walPath,
            Exists = true,
            FileLengthBytes = fileLength,
            FullFrameCount = fullFrameCount,
            CommitFrameCount = commitFrameCount,
            TrailingBytes = trailingBytes,
            Magic = magic,
            MagicValid = magicValid,
            Version = version,
            VersionValid = versionValid,
            PageSize = pageSize,
            PageSizeValid = pageSizeValid,
            Salt1 = salt1,
            Salt2 = salt2,
            Issues = issues,
        };
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
