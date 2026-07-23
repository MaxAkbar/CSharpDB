using System.Buffers;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Cryptography;
using System.Security.Principal;
using System.Text;
using Microsoft.Win32.SafeHandles;

#pragma warning disable CA1416 // Open rejects non-Windows before constructing this Windows-only substrate.

namespace CSharpDB.Migration.Files.Csv;

/// <summary>
/// Windows filesystem substrate for one private prepared CSV data file and its
/// canonical checkpoint siblings. The prepared data handle is the exclusive
/// cross-process lease; disposal deliberately preserves every file.
/// </summary>
internal sealed class CsvExportPreparedOutputFileSystem : IAsyncDisposable
{
    private const int BufferSize = 64 * 1024;
    private const string PublicationPathBindingContract =
        "csharpdb-csv-export-publication-path/v1";
    private const int FileRenameInfo = 3;
    private const int FileDispositionInfo = 4;
    private const int ErrorFileNotFound = 2;
    private const int ErrorPathNotFound = 3;
    private const int ErrorFileExists = 80;
    private const int ErrorAlreadyExists = 183;
    private const uint GenericRead = 0x80000000;
    private const uint GenericWrite = 0x40000000;
    private const uint DeleteAccess = 0x00010000;
    private const uint ReadControl = 0x00020000;
    private const uint FileShareRead = 0x00000001;
    private const uint FileShareWrite = 0x00000002;
    private const uint OpenExisting = 3;
    private const uint FileAttributeNormal = 0x00000080;
    private const uint FileFlagOpenReparsePoint = 0x00200000;
    private const uint FileFlagBackupSemantics = 0x02000000;
    private const uint FileFlagSequentialScan = 0x08000000;
    private const uint FileFlagWriteThrough = 0x80000000;
    private const uint FileFlagOverlapped = 0x40000000;
    private const FileAttributes UnsafeFileAttributes =
        FileAttributes.Directory | FileAttributes.ReparsePoint | FileAttributes.Device;

    private readonly CsvExportPreparedOutputPaths paths;
    private readonly string parentPath;
    private readonly SafeFileHandle parentHandle;
    private bool disposed;

    private CsvExportPreparedOutputFileSystem(
        CsvExportPreparedOutputPaths paths,
        string parentPath,
        SafeFileHandle parentHandle,
        FileStream dataStream)
    {
        this.paths = paths;
        this.parentPath = parentPath;
        this.parentHandle = parentHandle;
        DataStream = dataStream;
    }

    internal FileStream DataStream { get; }

    internal static CsvExportPreparedOutputFileSystem Open(
        CsvExportPreparedOutputPaths paths,
        bool requireExistingData = false)
    {
        if (!OperatingSystem.IsWindows())
        {
            throw new PlatformNotSupportedException(
                "Durable prepared CSV output is currently implemented only on Windows.");
        }

        ArgumentNullException.ThrowIfNull(paths);
        PreparedPathBinding binding = ValidatePaths(paths);
        SafeFileHandle? parent = null;
        FileStream? data = null;
        try
        {
            parent = OpenWindowsParent(binding.ParentPath);
            RejectUnsafeExistingSibling(binding.PreparedDataPath);
            RejectUnsafeExistingSibling(binding.CheckpointPath);
            RejectUnsafeExistingSibling(binding.PendingCheckpointPath);
            data = OpenWindowsPrivateWritable(
                binding.PreparedDataPath,
                requireDeleteAccess: false,
                createIfMissing: !requireExistingData);
            RequireWindowsParentIdentity(binding.ParentPath, parent);
            ValidateOptionalPrivateSibling(binding.CheckpointPath);
            ValidateOptionalPrivateSibling(binding.PendingCheckpointPath);
            RequireWindowsParentIdentity(binding.ParentPath, parent);

            var result = new CsvExportPreparedOutputFileSystem(
                paths,
                binding.ParentPath,
                parent,
                data);
            parent = null;
            data = null;
            return result;
        }
        finally
        {
            data?.Dispose();
            parent?.Dispose();
        }
    }

    internal async ValueTask<byte[]?> ReadActiveCheckpointAsync(
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();
        RequireWindowsParentIdentity(parentPath, parentHandle);

        FileStream? checkpoint = OpenWindowsPrivateRead(
            paths.CheckpointPath,
            allowMissing: true);
        if (checkpoint is null)
        {
            RequireWindowsParentIdentity(parentPath, parentHandle);
            return null;
        }

        await using (checkpoint.ConfigureAwait(false))
        {
            if (checkpoint.Length > CsvExportCheckpointSerializer.MaximumCheckpointBytes)
            {
                throw new InvalidDataException(
                    "The active CSV export checkpoint exceeds its byte ceiling.");
            }

            byte[] buffer = ArrayPool<byte>.Shared.Rent(BufferSize);
            try
            {
                using var bytes = new MemoryStream(
                    capacity: checked((int)checkpoint.Length));
                int maximumRead =
                    checked(CsvExportCheckpointSerializer.MaximumCheckpointBytes + 1);
                int total = 0;
                while (total < maximumRead)
                {
                    int requested = Math.Min(buffer.Length, maximumRead - total);
                    int read = await checkpoint.ReadAsync(
                            buffer.AsMemory(0, requested),
                            cancellationToken)
                        .ConfigureAwait(false);
                    if (read == 0)
                        break;

                    bytes.Write(buffer, 0, read);
                    total = checked(total + read);
                }

                if (total > CsvExportCheckpointSerializer.MaximumCheckpointBytes)
                {
                    throw new InvalidDataException(
                        "The active CSV export checkpoint exceeds its byte ceiling.");
                }

                RequireWindowsParentIdentity(parentPath, parentHandle);
                return bytes.ToArray();
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
            }
        }
    }

    internal async ValueTask FlushDataToDiskAsync(
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        RequireWindowsParentIdentity(parentPath, parentHandle);
        await DataStream.FlushAsync(cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        DataStream.Flush(flushToDisk: true);
        RequireWindowsParentIdentity(parentPath, parentHandle);
    }

    internal void TruncateData(long length)
    {
        ThrowIfDisposed();
        if (length < 0 || length > DataStream.Length)
        {
            throw new ArgumentOutOfRangeException(
                nameof(length),
                "A prepared CSV data file can only be truncated to an existing boundary.");
        }

        RequireWindowsParentIdentity(parentPath, parentHandle);
        DataStream.SetLength(length);
        DataStream.Position = length;
        RequireWindowsParentIdentity(parentPath, parentHandle);
    }

    internal async ValueTask ReplaceCheckpointAsync(
        ReadOnlyMemory<byte> canonicalBytes,
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        if (canonicalBytes.IsEmpty ||
            canonicalBytes.Length > CsvExportCheckpointSerializer.MaximumCheckpointBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(canonicalBytes),
                "Checkpoint bytes must be nonempty and within the canonical byte ceiling.");
        }

        cancellationToken.ThrowIfCancellationRequested();
        RequireWindowsParentIdentity(parentPath, parentHandle);
        FileStream pending = OpenWindowsPrivateWritable(
            paths.PendingCheckpointPath,
            requireDeleteAccess: true);
        bool renamed = false;
        try
        {
            pending.SetLength(0);
            pending.Position = 0;
            await pending.WriteAsync(canonicalBytes, cancellationToken).ConfigureAwait(false);
            await pending.FlushAsync(cancellationToken).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            pending.Flush(flushToDisk: true);

            // Cancellation is intentionally no longer observed after the pending
            // checkpoint becomes durable. The rename either fails or establishes
            // the new active recovery authority.
            ValidateOptionalPrivateSibling(paths.CheckpointPath);
            RequireWindowsParentIdentity(parentPath, parentHandle);
            ReplaceWindowsByHandle(pending, paths.CheckpointPath);
            renamed = true;
        }
        finally
        {
            try
            {
                pending.Dispose();
            }
            catch when (renamed)
            {
                // A successful handle rename is the commit point. Cleanup
                // cannot retroactively turn it into a reported failure.
            }
        }
    }

    internal async ValueTask<CsvExportFilePublicationResult>
        PublishCompletedAsync(
            string destinationPath,
            string manifestPath,
            ReadOnlyMemory<byte> canonicalManifestBytes,
            CsvExportCheckpointProgress progress,
            ICsvExportPublicationFaultInjector? faultInjector,
            CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(progress);
        if (canonicalManifestBytes.IsEmpty ||
            canonicalManifestBytes.Length >
            CsvExportManifestSerializer.MaximumManifestBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(canonicalManifestBytes),
                "Canonical manifest bytes must be nonempty and within the manifest byte ceiling.");
        }

        PublicationPathBinding publication =
            ValidatePublicationPaths(destinationPath, manifestPath);
        cancellationToken.ThrowIfCancellationRequested();
        RequireWindowsParentIdentity(parentPath, parentHandle);

        FileStream? existingData = null;
        FileStream? existingManifest = null;
        FileStream? stableData = null;
        FileStream? stableManifest = null;
        bool committedPair = false;
        try
        {
            existingData = OpenWindowsPrivateRead(
                publication.DestinationPath,
                allowMissing: true);
            existingManifest = OpenWindowsPrivateRead(
                publication.ManifestPath,
                allowMissing: true);

            bool dataMatches =
                existingData is not null &&
                await ExistingDataMatchesAsync(
                        existingData,
                        progress,
                        cancellationToken)
                    .ConfigureAwait(false);
            bool manifestMatches =
                existingManifest is not null &&
                await ExistingBytesMatchAsync(
                        existingManifest,
                        canonicalManifestBytes,
                        cancellationToken)
                    .ConfigureAwait(false);

            if (existingManifest is not null)
            {
                if (!manifestMatches)
                {
                    throw new IOException(
                        "The CSV export manifest destination already contains a different file.");
                }
                if (existingData is null || !dataMatches)
                {
                    throw new InvalidDataException(
                        "A CSV export manifest exists without its exact final data file.");
                }

                committedPair = true;
                return new CsvExportFilePublicationResult(
                    ReusedData: true,
                    ReusedManifest: true);
            }
            if (existingData is not null && !dataMatches)
            {
                throw new IOException(
                    "The CSV export destination already contains a different file.");
            }

            bool dataExistedAtPreflight = existingData is not null;
            bool reusedData = dataExistedAtPreflight;
            if (dataExistedAtPreflight)
            {
                stableData = existingData;
                existingData = null;
            }
            else
            {
                PublishedPrivateFile publishedData =
                    await PublishPreparedDataAsync(
                            publication,
                            progress,
                            faultInjector,
                            cancellationToken)
                        .ConfigureAwait(false);
                stableData = publishedData.Stream;
                reusedData = publishedData.Reused;
                await InjectFaultAsync(
                        faultInjector,
                        CsvExportPublicationFaultPoint
                            .AfterDataNamespaceCommitBeforeManifest,
                        CancellationToken.None)
                    .ConfigureAwait(false);
            }

            // If this invocation established the final CSV name, its commit is
            // the cancellation cut-off. A pre-existing exact CSV may still
            // honor cancellation while staging the missing manifest.
            CancellationToken manifestCancellation =
                dataExistedAtPreflight
                    ? cancellationToken
                    : CancellationToken.None;
            PublishedPrivateFile publishedManifest =
                await PublishManifestAsync(
                        publication,
                        canonicalManifestBytes,
                        faultInjector,
                        manifestCancellation)
                    .ConfigureAwait(false);
            stableManifest = publishedManifest.Stream;

            committedPair = true;
            await InjectFaultAsync(
                    faultInjector,
                    CsvExportPublicationFaultPoint
                        .AfterManifestNamespaceCommitBeforeResult,
                    CancellationToken.None)
                .ConfigureAwait(false);
            return new CsvExportFilePublicationResult(
                ReusedData: reusedData,
                ReusedManifest: publishedManifest.Reused);
        }
        finally
        {
            DisposeAfterPublication(existingManifest, committedPair);
            DisposeAfterPublication(existingData, committedPair);
            DisposeAfterPublication(stableManifest, committedPair);
            DisposeAfterPublication(stableData, committedPair);
        }
    }

    public ValueTask DisposeAsync()
    {
        if (disposed)
            return ValueTask.CompletedTask;

        disposed = true;
        try
        {
            DataStream.Dispose();
        }
        finally
        {
            parentHandle.Dispose();
        }
        return ValueTask.CompletedTask;
    }

    private static PreparedPathBinding ValidatePaths(
        CsvExportPreparedOutputPaths paths)
    {
        string data = ValidateAbsoluteNormalizedPath(
            paths.PreparedDataPath,
            nameof(paths.PreparedDataPath));
        string checkpoint = ValidateAbsoluteNormalizedPath(
            paths.CheckpointPath,
            nameof(paths.CheckpointPath));
        string pending = ValidateAbsoluteNormalizedPath(
            paths.PendingCheckpointPath,
            nameof(paths.PendingCheckpointPath));
        string parent = Path.GetDirectoryName(data)
            ?? throw new ArgumentException("The prepared CSV data path has no parent.");
        string comparisonParent1 = Path.GetDirectoryName(checkpoint)
            ?? throw new ArgumentException("The checkpoint path has no parent.");
        string comparisonParent2 = Path.GetDirectoryName(pending)
            ?? throw new ArgumentException("The pending checkpoint path has no parent.");
        if (!string.Equals(parent, comparisonParent1, StringComparison.OrdinalIgnoreCase) ||
            !string.Equals(parent, comparisonParent2, StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException(
                "Prepared CSV data and checkpoint files must be siblings.");
        }
        if (string.Equals(data, checkpoint, StringComparison.OrdinalIgnoreCase) ||
            string.Equals(data, pending, StringComparison.OrdinalIgnoreCase) ||
            string.Equals(checkpoint, pending, StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException(
                "Prepared CSV data and checkpoint paths must be distinct.");
        }

        ValidateWindowsDirectoryChain(parent);
        return new PreparedPathBinding(data, checkpoint, pending, parent);
    }

    internal static void ValidatePublicationPathsForPreflight(
        string destinationPath,
        string manifestPath)
    {
        _ = BindPublicationPaths(destinationPath, manifestPath);
    }

    private PublicationPathBinding ValidatePublicationPaths(
        string destinationPath,
        string manifestPath)
    {
        PublicationPathBinding publication =
            BindPublicationPaths(destinationPath, manifestPath);
        (
            _,
            CsvExportPreparedOutputPaths reboundPaths,
            _
        ) = CsvExportPreparedOutputLease.BindPaths(
            destinationPath,
            allowExistingDestination: true);
        if (!string.Equals(
                reboundPaths.PreparedDataPath,
                paths.PreparedDataPath,
                StringComparison.OrdinalIgnoreCase) ||
            !string.Equals(
                reboundPaths.CheckpointPath,
                paths.CheckpointPath,
                StringComparison.OrdinalIgnoreCase) ||
            !string.Equals(
                reboundPaths.PendingCheckpointPath,
                paths.PendingCheckpointPath,
                StringComparison.OrdinalIgnoreCase) ||
            !string.Equals(
                Path.GetDirectoryName(publication.DestinationPath),
                parentPath,
                StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidDataException(
                "The CSV publication destination does not match its prepared-output lease.");
        }

        return publication;
    }

    private static PublicationPathBinding BindPublicationPaths(
        string destinationPath,
        string manifestPath)
    {
        (
            string normalizedDestination,
            CsvExportPreparedOutputPaths preparedPaths,
            _
        ) = CsvExportPreparedOutputLease.BindPaths(
            destinationPath,
            allowExistingDestination: true);
        (
            string normalizedManifest,
            _,
            _
        ) = CsvExportPreparedOutputLease.BindPaths(
            manifestPath,
            allowExistingDestination: true);

        string destinationParent = Path.GetDirectoryName(normalizedDestination)
            ?? throw new ArgumentException(
                "The CSV export destination path has no parent.",
                nameof(destinationPath));
        string manifestParent = Path.GetDirectoryName(normalizedManifest)
            ?? throw new ArgumentException(
                "The CSV export manifest path has no parent.",
                nameof(manifestPath));
        if (!string.Equals(
                destinationParent,
                manifestParent,
                StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException(
                "The final CSV and manifest must be siblings in the same directory.",
                nameof(manifestPath));
        }

        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        AddDistinct(names, normalizedDestination);
        AddDistinct(names, normalizedManifest);
        AddDistinct(names, preparedPaths.PreparedDataPath);
        AddDistinct(names, preparedPaths.CheckpointPath);
        AddDistinct(names, preparedPaths.PendingCheckpointPath);

        string pairText = PublicationPathBindingContract + "\0" +
            normalizedDestination.ToUpperInvariant() + "\0" +
            normalizedManifest.ToUpperInvariant();
        byte[] pairBytes = Encoding.UTF8.GetBytes(pairText);
        string digest;
        try
        {
            digest = Convert.ToHexString(SHA256.HashData(pairBytes))
                .ToLowerInvariant();
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pairBytes);
        }

        string stem =
            $".csharpdb-csv-export-{digest[..32]}.publish";
        string dataTemporaryPath =
            Path.Combine(destinationParent, stem + ".data.next");
        string manifestTemporaryPath =
            Path.Combine(destinationParent, stem + ".manifest.next");
        AddDistinct(names, dataTemporaryPath);
        AddDistinct(names, manifestTemporaryPath);

        return new PublicationPathBinding(
            normalizedDestination,
            normalizedManifest,
            dataTemporaryPath,
            manifestTemporaryPath);
    }

    private static void AddDistinct(HashSet<string> paths, string path)
    {
        if (!paths.Add(path))
        {
            throw new ArgumentException(
                "CSV export final and private publication paths must be distinct.");
        }
    }

    private async ValueTask<PublishedPrivateFile> PublishPreparedDataAsync(
        PublicationPathBinding publication,
        CsvExportCheckpointProgress progress,
        ICsvExportPublicationFaultInjector? faultInjector,
        CancellationToken cancellationToken)
    {
        RequireWindowsParentIdentity(parentPath, parentHandle);
        FileStream? temporary = OpenWindowsPrivateWritable(
            publication.DataTemporaryPath,
            requireDeleteAccess: true);
        long preparedPosition = DataStream.Position;
        bool renamed = false;
        try
        {
            temporary.SetLength(0);
            temporary.Position = 0;
            DataStream.Position = 0;

            using IncrementalHash hash =
                IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            byte[] buffer = ArrayPool<byte>.Shared.Rent(BufferSize);
            long remaining = progress.DataPrefixByteLength;
            try
            {
                while (remaining > 0)
                {
                    int requested = (int)Math.Min(remaining, buffer.Length);
                    int read = await DataStream.ReadAsync(
                            buffer.AsMemory(0, requested),
                            cancellationToken)
                        .ConfigureAwait(false);
                    if (read == 0)
                    {
                        throw new InvalidDataException(
                            "The prepared CSV ended before its data-complete boundary.");
                    }

                    hash.AppendData(buffer, 0, read);
                    await temporary.WriteAsync(
                            buffer.AsMemory(0, read),
                            cancellationToken)
                        .ConfigureAwait(false);
                    remaining -= read;
                }
            }
            finally
            {
                CryptographicOperations.ZeroMemory(
                    buffer.AsSpan(0, buffer.Length));
                ArrayPool<byte>.Shared.Return(buffer);
            }

            if (DataStream.Length != progress.DataPrefixByteLength ||
                temporary.Length != progress.DataPrefixByteLength)
            {
                throw new InvalidDataException(
                    "The prepared CSV length changed during publication.");
            }

            Span<byte> actualDigest =
                stackalloc byte[SHA256.HashSizeInBytes];
            if (!hash.TryGetHashAndReset(
                    actualDigest,
                    out int digestBytes) ||
                digestBytes != actualDigest.Length)
            {
                throw new CryptographicException(
                    "The prepared CSV publication digest could not be finalized.");
            }
            VerifyDigest(
                actualDigest,
                progress.DataPrefixDigest,
                "The prepared CSV changed while it was being staged for publication.");

            await FlushPrivateAsync(temporary, cancellationToken)
                .ConfigureAwait(false);
            await InjectFaultAsync(
                    faultInjector,
                    CsvExportPublicationFaultPoint.BeforeDataNamespaceCommit,
                    cancellationToken)
                .ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            using (FileStream? manifestBeforeData =
                   OpenWindowsPrivateRead(
                       publication.ManifestPath,
                       allowMissing: true))
            {
                if (manifestBeforeData is not null)
                {
                    throw new InvalidDataException(
                        "The CSV export manifest appeared before the final data commit.");
                }
            }
            RequireWindowsParentIdentity(parentPath, parentHandle);

            NoReplaceRenameStatus status = RenameWindowsByHandleNoReplace(
                temporary,
                publication.DestinationPath);
            if (status == NoReplaceRenameStatus.Published)
            {
                renamed = true;
                FileStream published = temporary;
                temporary = null;
                return new PublishedPrivateFile(
                    published,
                    Reused: false);
            }

            RemoveWindowsByHandle(temporary);
            temporary.Dispose();
            temporary = null;
            FileStream existing = OpenWindowsPrivateRead(
                    publication.DestinationPath,
                    allowMissing: false)
                ?? throw new IOException(
                    "The CSV destination disappeared during publication.");
            try
            {
                if (!await ExistingDataMatchesAsync(
                        existing,
                        progress,
                        CancellationToken.None)
                    .ConfigureAwait(false))
                {
                    throw new IOException(
                        "The CSV export destination already contains a different file.");
                }

                return new PublishedPrivateFile(existing, Reused: true);
            }
            catch
            {
                existing.Dispose();
                throw;
            }
        }
        finally
        {
            DataStream.Position = preparedPosition;
            if (temporary is not null)
            {
                try
                {
                    if (!renamed)
                        RemoveWindowsByHandle(temporary);
                }
                finally
                {
                    temporary.Dispose();
                }
            }
        }
    }

    private async ValueTask<PublishedPrivateFile> PublishManifestAsync(
        PublicationPathBinding publication,
        ReadOnlyMemory<byte> canonicalManifestBytes,
        ICsvExportPublicationFaultInjector? faultInjector,
        CancellationToken cancellationToken)
    {
        RequireWindowsParentIdentity(parentPath, parentHandle);
        FileStream? temporary = OpenWindowsPrivateWritable(
            publication.ManifestTemporaryPath,
            requireDeleteAccess: true);
        bool renamed = false;
        try
        {
            temporary.SetLength(0);
            temporary.Position = 0;
            await temporary.WriteAsync(
                    canonicalManifestBytes,
                    cancellationToken)
                .ConfigureAwait(false);
            await FlushPrivateAsync(temporary, cancellationToken)
                .ConfigureAwait(false);
            await InjectFaultAsync(
                    faultInjector,
                    CsvExportPublicationFaultPoint.BeforeManifestNamespaceCommit,
                    cancellationToken)
                .ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            RequireWindowsParentIdentity(parentPath, parentHandle);

            NoReplaceRenameStatus status = RenameWindowsByHandleNoReplace(
                temporary,
                publication.ManifestPath);
            if (status == NoReplaceRenameStatus.Published)
            {
                renamed = true;
                FileStream published = temporary;
                temporary = null;
                return new PublishedPrivateFile(
                    published,
                    Reused: false);
            }

            RemoveWindowsByHandle(temporary);
            temporary.Dispose();
            temporary = null;
            FileStream existing = OpenWindowsPrivateRead(
                    publication.ManifestPath,
                    allowMissing: false)
                ?? throw new IOException(
                    "The CSV manifest destination disappeared during publication.");
            try
            {
                if (!await ExistingBytesMatchAsync(
                        existing,
                        canonicalManifestBytes,
                        CancellationToken.None)
                    .ConfigureAwait(false))
                {
                    throw new IOException(
                        "The CSV export manifest destination already contains a different file.");
                }

                return new PublishedPrivateFile(existing, Reused: true);
            }
            catch
            {
                existing.Dispose();
                throw;
            }
        }
        finally
        {
            if (temporary is not null)
            {
                try
                {
                    if (!renamed)
                        RemoveWindowsByHandle(temporary);
                }
                finally
                {
                    temporary.Dispose();
                }
            }
        }
    }

    private async ValueTask<bool> ExistingDataMatchesAsync(
        FileStream existing,
        CsvExportCheckpointProgress progress,
        CancellationToken cancellationToken)
    {
        if (existing.Length != progress.DataPrefixByteLength)
            return false;

        long position = existing.Position;
        byte[] buffer = ArrayPool<byte>.Shared.Rent(BufferSize);
        try
        {
            using IncrementalHash hash =
                IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            existing.Position = 0;
            long remaining = existing.Length;
            while (remaining > 0)
            {
                int requested = (int)Math.Min(remaining, buffer.Length);
                int read = await existing.ReadAsync(
                        buffer.AsMemory(0, requested),
                        cancellationToken)
                    .ConfigureAwait(false);
                if (read == 0)
                    return false;

                hash.AppendData(buffer, 0, read);
                remaining -= read;
            }

            Span<byte> actual = stackalloc byte[SHA256.HashSizeInBytes];
            if (!hash.TryGetHashAndReset(actual, out int written) ||
                written != actual.Length)
            {
                throw new CryptographicException(
                    "The final CSV digest could not be finalized.");
            }

            return DigestEquals(actual, progress.DataPrefixDigest);
        }
        finally
        {
            existing.Position = position;
            CryptographicOperations.ZeroMemory(buffer.AsSpan(0, buffer.Length));
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static async ValueTask<bool> ExistingBytesMatchAsync(
        FileStream existing,
        ReadOnlyMemory<byte> expected,
        CancellationToken cancellationToken)
    {
        if (existing.Length != expected.Length)
            return false;

        long position = existing.Position;
        byte[] buffer = ArrayPool<byte>.Shared.Rent(BufferSize);
        try
        {
            existing.Position = 0;
            int offset = 0;
            while (offset < expected.Length)
            {
                int requested = Math.Min(
                    buffer.Length,
                    expected.Length - offset);
                int read = await existing.ReadAsync(
                        buffer.AsMemory(0, requested),
                        cancellationToken)
                    .ConfigureAwait(false);
                if (read == 0)
                    return false;
                if (!CryptographicOperations.FixedTimeEquals(
                        buffer.AsSpan(0, read),
                        expected.Span.Slice(offset, read)))
                {
                    return false;
                }
                offset += read;
            }

            return true;
        }
        finally
        {
            existing.Position = position;
            CryptographicOperations.ZeroMemory(buffer.AsSpan(0, buffer.Length));
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static async ValueTask FlushPrivateAsync(
        FileStream stream,
        CancellationToken cancellationToken)
    {
        await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        stream.Flush(flushToDisk: true);
    }

    private static ValueTask InjectFaultAsync(
        ICsvExportPublicationFaultInjector? faultInjector,
        CsvExportPublicationFaultPoint point,
        CancellationToken cancellationToken) =>
        faultInjector?.InjectAsync(point, cancellationToken) ??
        ValueTask.CompletedTask;

    private static bool DigestEquals(
        ReadOnlySpan<byte> actual,
        CsvExportHashManifest expected)
    {
        if (!string.Equals(
                expected.Algorithm,
                CsvExportHashManifest.Sha256Algorithm,
                StringComparison.Ordinal))
        {
            return false;
        }

        byte[] expectedBytes = Convert.FromHexString(expected.Value);
        try
        {
            return CryptographicOperations.FixedTimeEquals(
                actual,
                expectedBytes);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(expectedBytes);
        }
    }

    private static void VerifyDigest(
        ReadOnlySpan<byte> actual,
        CsvExportHashManifest expected,
        string message)
    {
        if (!DigestEquals(actual, expected))
            throw new InvalidDataException(message);
    }

    private static void DisposeAfterPublication(
        IDisposable? value,
        bool committedPair)
    {
        if (value is null)
            return;
        try
        {
            value.Dispose();
        }
        catch when (committedPair)
        {
            // A fully published and qualified pair remains success even if a
            // read or staging handle reports a late cleanup failure.
        }
    }

    private static string ValidateAbsoluteNormalizedPath(
        string path,
        string parameterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path, parameterName);
        if (path.Contains('\0'))
            throw new ArgumentException("Prepared CSV paths cannot contain NUL.", parameterName);
        if (!Path.IsPathFullyQualified(path))
            throw new ArgumentException("Prepared CSV paths must be fully qualified.", parameterName);
        if (path.StartsWith(@"\\?\", StringComparison.Ordinal) ||
            path.StartsWith(@"\\.\", StringComparison.Ordinal) ||
            path.StartsWith(@"\\", StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "Windows device, extended, and network paths are not supported.",
                parameterName);
        }

        string full = Path.GetFullPath(path);
        if (!string.Equals(full, path, StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException("Prepared CSV paths must be normalized.", parameterName);
        string root = Path.GetPathRoot(full) ?? string.Empty;
        if (full.AsSpan(root.Length).Contains(':'))
            throw new ArgumentException("Alternate data streams are not supported.", parameterName);
        string leaf = Path.GetFileName(full);
        if (string.IsNullOrWhiteSpace(leaf) || leaf is "." or "..")
            throw new ArgumentException("Prepared CSV file names are invalid.", parameterName);
        return full;
    }

    [SupportedOSPlatform("windows")]
    private static void ValidateWindowsDirectoryChain(string parentPath)
    {
        if (!Directory.Exists(parentPath))
            throw new DirectoryNotFoundException("The prepared CSV parent does not exist.");

        string root = Path.GetPathRoot(parentPath)
            ?? throw new InvalidDataException("The prepared CSV parent root is invalid.");
        string relative = Path.GetRelativePath(root, parentPath);
        string current = root;
        if (relative == ".")
            return;

        foreach (string segment in relative.Split(
                     [Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar],
                     StringSplitOptions.RemoveEmptyEntries))
        {
            current = Path.Combine(current, segment);
            FileAttributes attributes = File.GetAttributes(current);
            if ((attributes & FileAttributes.Directory) == 0 ||
                (attributes & (FileAttributes.ReparsePoint | FileAttributes.Device)) != 0)
            {
                throw new InvalidDataException(
                    "The prepared CSV parent cannot traverse a link, device, or non-directory.");
            }
        }
    }

    [SupportedOSPlatform("windows")]
    private static SafeFileHandle OpenWindowsParent(string path)
    {
        SafeFileHandle handle = CreateFileW(
            path,
            GenericRead | ReadControl,
            FileShareRead | FileShareWrite,
            IntPtr.Zero,
            OpenExisting,
            FileFlagOpenReparsePoint | FileFlagBackupSemantics,
            IntPtr.Zero);
        if (handle.IsInvalid)
        {
            int error = Marshal.GetLastPInvokeError();
            handle.Dispose();
            throw new IOException(
                "The prepared CSV parent cannot be opened safely.",
                new Win32Exception(error));
        }

        try
        {
            FileAttributes attributes = File.GetAttributes(handle);
            if ((attributes & FileAttributes.Directory) == 0 ||
                (attributes & (FileAttributes.ReparsePoint | FileAttributes.Device)) != 0)
            {
                throw new InvalidDataException(
                    "The prepared CSV parent must be a real directory.");
            }
            ValidateLocalWindowsFilesystem(path, handle);
            return handle;
        }
        catch
        {
            handle.Dispose();
            throw;
        }
    }

    [SupportedOSPlatform("windows")]
    private static void ValidateLocalWindowsFilesystem(
        string parentPath,
        SafeFileHandle parent)
    {
        string root = Path.GetPathRoot(parentPath)
            ?? throw new InvalidDataException(
                "The prepared CSV parent volume is invalid.");
        var drive = new DriveInfo(root);
        if (drive.DriveType == DriveType.Network)
        {
            throw new InvalidDataException(
                "Prepared CSV output requires a local Windows filesystem; mapped network drives are unsupported.");
        }

        var finalPath = new StringBuilder(512);
        uint length = GetFinalPathNameByHandleW(
            parent,
            finalPath,
            checked((uint)finalPath.Capacity),
            0);
        if (length >= finalPath.Capacity)
        {
            finalPath.EnsureCapacity(checked((int)length + 1));
            length = GetFinalPathNameByHandleW(
                parent,
                finalPath,
                checked((uint)finalPath.Capacity),
                0);
        }
        if (length == 0 || length >= finalPath.Capacity)
        {
            throw new IOException(
                "The prepared CSV parent volume identity could not be resolved.",
                new Win32Exception(Marshal.GetLastPInvokeError()));
        }
        if (finalPath.ToString().StartsWith(
                @"\\?\UNC\",
                StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidDataException(
                "Prepared CSV output requires a local Windows filesystem; network paths are unsupported.");
        }
    }

    [SupportedOSPlatform("windows")]
    private static void RequireWindowsParentIdentity(
        string path,
        SafeFileHandle expected)
    {
        using SafeFileHandle actual = OpenWindowsParent(path);
        if (!GetFileInformationByHandle(expected, out WindowsFileInformation left) ||
            !GetFileInformationByHandle(actual, out WindowsFileInformation right) ||
            left.VolumeSerialNumber != right.VolumeSerialNumber ||
            left.FileIndexHigh != right.FileIndexHigh ||
            left.FileIndexLow != right.FileIndexLow)
        {
            throw new IOException(
                "The prepared CSV parent identity changed during the operation.");
        }
    }

    [SupportedOSPlatform("windows")]
    private static FileStream OpenWindowsPrivateWritable(
        string path,
        bool requireDeleteAccess,
        bool createIfMissing = true)
    {
        if (createIfMissing)
        {
            try
            {
                FileStream created = FileSystemAclExtensions.Create(
                    new FileInfo(path),
                    FileMode.CreateNew,
                    FileSystemRights.FullControl,
                    FileShare.None,
                    BufferSize,
                    FileOptions.Asynchronous |
                        FileOptions.SequentialScan |
                        FileOptions.WriteThrough,
                    CreatePrivateWindowsSecurity());
                try
                {
                    ValidateWindowsPrivateFile(created);
                    return created;
                }
                catch
                {
                    created.Dispose();
                    throw;
                }
            }
            catch (IOException) when (PathEntryExists(path))
            {
                // The existing private file is opened below.
            }
            catch (UnauthorizedAccessException) when (IsUnsafeExistingSibling(path))
            {
                throw new InvalidDataException(
                    "Prepared CSV sibling paths must be private regular files.");
            }
        }

        uint access = GenericRead | GenericWrite | ReadControl;
        if (requireDeleteAccess)
            access |= DeleteAccess;
        SafeFileHandle handle = CreateFileW(
            path,
            access,
            0,
            IntPtr.Zero,
            OpenExisting,
            FileAttributeNormal |
                FileFlagOpenReparsePoint |
                FileFlagOverlapped |
                FileFlagSequentialScan |
                FileFlagWriteThrough,
            IntPtr.Zero);
        if (handle.IsInvalid)
        {
            int error = Marshal.GetLastPInvokeError();
            handle.Dispose();
            if (error == ErrorFileNotFound)
            {
                throw new FileNotFoundException(
                    "The private prepared CSV file does not exist.",
                    path);
            }
            if (IsUnsafeExistingSibling(path))
            {
                throw new InvalidDataException(
                    "Prepared CSV sibling paths must be private regular files.");
            }
            throw new IOException(
                "The private prepared CSV file is unavailable or already leased.",
                new Win32Exception(error));
        }

        try
        {
            var stream = new FileStream(
                handle,
                FileAccess.ReadWrite,
                BufferSize,
                isAsync: true);
            handle = null!;
            try
            {
                ValidateWindowsPrivateFile(stream);
                return stream;
            }
            catch
            {
                stream.Dispose();
                throw;
            }
        }
        finally
        {
            handle?.Dispose();
        }
    }

    [SupportedOSPlatform("windows")]
    private static FileStream? OpenWindowsPrivateRead(
        string path,
        bool allowMissing)
    {
        SafeFileHandle handle = CreateFileW(
            path,
            GenericRead | ReadControl,
            FileShareRead,
            IntPtr.Zero,
            OpenExisting,
            FileAttributeNormal |
                FileFlagOpenReparsePoint |
                FileFlagOverlapped |
                FileFlagSequentialScan,
            IntPtr.Zero);
        if (handle.IsInvalid)
        {
            int error = Marshal.GetLastPInvokeError();
            handle.Dispose();
            if (allowMissing && error == ErrorFileNotFound)
                return null;
            if (IsUnsafeExistingSibling(path))
            {
                throw new InvalidDataException(
                    "Prepared CSV sibling paths must be private regular files.");
            }
            if (error == ErrorPathNotFound)
                throw new DirectoryNotFoundException("The prepared CSV parent disappeared.");
            throw new IOException(
                "The private CSV export checkpoint cannot be opened safely.",
                new Win32Exception(error));
        }

        try
        {
            var stream = new FileStream(handle, FileAccess.Read, BufferSize, isAsync: true);
            handle = null!;
            try
            {
                ValidateWindowsPrivateFile(stream);
                return stream;
            }
            catch
            {
                stream.Dispose();
                throw;
            }
        }
        finally
        {
            handle?.Dispose();
        }
    }

    [SupportedOSPlatform("windows")]
    private static void ValidateOptionalPrivateSibling(string path)
    {
        using FileStream? stream = OpenWindowsPrivateRead(path, allowMissing: true);
    }

    [SupportedOSPlatform("windows")]
    private static FileSecurity CreatePrivateWindowsSecurity()
    {
        using WindowsIdentity identity = WindowsIdentity.GetCurrent(TokenAccessLevels.Query);
        SecurityIdentifier owner = identity.User
            ?? throw new IOException("The current Windows identity has no SID.");
        var security = new FileSecurity();
        security.SetOwner(owner);
        security.SetAccessRuleProtection(isProtected: true, preserveInheritance: false);
        security.AddAccessRule(new FileSystemAccessRule(
            owner,
            FileSystemRights.FullControl,
            AccessControlType.Allow));
        return security;
    }

    [SupportedOSPlatform("windows")]
    private static void ValidateWindowsPrivateFile(FileStream stream)
    {
        FileAttributes attributes = File.GetAttributes(stream.SafeFileHandle);
        if ((attributes & UnsafeFileAttributes) != 0 ||
            !GetFileInformationByHandle(
                stream.SafeFileHandle,
                out WindowsFileInformation information) ||
            information.NumberOfLinks != 1)
        {
            throw new InvalidDataException(
                "Prepared CSV files must be regular files with exactly one link.");
        }

        FileSecurity security = FileSystemAclExtensions.GetAccessControl(stream);
        using WindowsIdentity identity = WindowsIdentity.GetCurrent(TokenAccessLevels.Query);
        SecurityIdentifier owner = identity.User
            ?? throw new IOException("The current Windows identity has no SID.");
        if (!security.AreAccessRulesProtected ||
            security.GetOwner(typeof(SecurityIdentifier)) is not SecurityIdentifier actual ||
            !owner.Equals(actual))
        {
            throw new InvalidDataException(
                "Prepared CSV files must be private to the current Windows identity.");
        }

        bool ownerHasFullControl = false;
        AuthorizationRuleCollection rules = security.GetAccessRules(
            includeExplicit: true,
            includeInherited: true,
            targetType: typeof(SecurityIdentifier));
        foreach (FileSystemAccessRule rule in rules)
        {
            if (rule.AccessControlType != AccessControlType.Allow)
                continue;
            if (rule.IdentityReference is not SecurityIdentifier sid ||
                !owner.Equals(sid))
            {
                throw new InvalidDataException(
                    "Prepared CSV files grant access beyond the current Windows identity.");
            }
            ownerHasFullControl |=
                (rule.FileSystemRights & FileSystemRights.FullControl) ==
                FileSystemRights.FullControl;
        }
        if (!ownerHasFullControl)
        {
            throw new InvalidDataException(
                "The current Windows identity lacks full control of the prepared CSV file.");
        }
    }

    [SupportedOSPlatform("windows")]
    private static void ReplaceWindowsByHandle(
        FileStream pending,
        string checkpointPath)
    {
        byte[] nameBytes = Encoding.Unicode.GetBytes(checkpointPath);
        int nameOffset = IntPtr.Size == 8 ? 20 : 12;
        int informationLength = checked(nameOffset + nameBytes.Length);
        int allocationLength = checked(informationLength + sizeof(char));
        IntPtr buffer = Marshal.AllocHGlobal(allocationLength);
        try
        {
            Marshal.Copy(new byte[allocationLength], 0, buffer, allocationLength);
            Marshal.WriteByte(buffer, 0, 1);
            Marshal.WriteIntPtr(buffer, IntPtr.Size == 8 ? 8 : 4, IntPtr.Zero);
            Marshal.WriteInt32(buffer, IntPtr.Size == 8 ? 16 : 8, nameBytes.Length);
            Marshal.Copy(nameBytes, 0, IntPtr.Add(buffer, nameOffset), nameBytes.Length);
            if (!SetFileInformationByHandle(
                    pending.SafeFileHandle,
                    FileRenameInfo,
                    buffer,
                    checked((uint)informationLength)))
            {
                throw new IOException(
                    "The active CSV export checkpoint could not be atomically replaced.",
                    new Win32Exception(Marshal.GetLastPInvokeError()));
            }
        }
        finally
        {
            Marshal.FreeHGlobal(buffer);
        }
    }

    [SupportedOSPlatform("windows")]
    private static NoReplaceRenameStatus RenameWindowsByHandleNoReplace(
        FileStream temporary,
        string destinationPath)
    {
        byte[] nameBytes = Encoding.Unicode.GetBytes(destinationPath);
        int nameOffset = IntPtr.Size == 8 ? 20 : 12;
        int informationLength = checked(nameOffset + nameBytes.Length);
        int allocationLength = checked(informationLength + sizeof(char));
        IntPtr buffer = Marshal.AllocHGlobal(allocationLength);
        try
        {
            Marshal.Copy(
                new byte[allocationLength],
                0,
                buffer,
                allocationLength);
            Marshal.WriteIntPtr(
                buffer,
                IntPtr.Size == 8 ? 8 : 4,
                IntPtr.Zero);
            Marshal.WriteInt32(
                buffer,
                IntPtr.Size == 8 ? 16 : 8,
                nameBytes.Length);
            Marshal.Copy(
                nameBytes,
                0,
                IntPtr.Add(buffer, nameOffset),
                nameBytes.Length);
            if (SetFileInformationByHandle(
                    temporary.SafeFileHandle,
                    FileRenameInfo,
                    buffer,
                    checked((uint)informationLength)))
            {
                return NoReplaceRenameStatus.Published;
            }

            int error = Marshal.GetLastPInvokeError();
            if (error is ErrorAlreadyExists or ErrorFileExists)
                return NoReplaceRenameStatus.DestinationExists;
            throw new IOException(
                "The CSV export file could not be atomically published.",
                new Win32Exception(error));
        }
        finally
        {
            Marshal.FreeHGlobal(buffer);
        }
    }

    [SupportedOSPlatform("windows")]
    private static void RemoveWindowsByHandle(FileStream temporary)
    {
        IntPtr disposition = Marshal.AllocHGlobal(1);
        try
        {
            Marshal.WriteByte(disposition, 1);
            if (!SetFileInformationByHandle(
                    temporary.SafeFileHandle,
                    FileDispositionInfo,
                    disposition,
                    1))
            {
                throw new IOException(
                    "The private CSV publication staging file could not be removed.",
                    new Win32Exception(Marshal.GetLastPInvokeError()));
            }
        }
        finally
        {
            Marshal.FreeHGlobal(disposition);
        }
    }

    private static bool PathEntryExists(string path)
    {
        try
        {
            _ = File.GetAttributes(path);
            return true;
        }
        catch (Exception exception) when (
            exception is FileNotFoundException or DirectoryNotFoundException)
        {
            return false;
        }
    }

    private static void RejectUnsafeExistingSibling(string path)
    {
        if (IsUnsafeExistingSibling(path))
        {
            throw new InvalidDataException(
                "Prepared CSV sibling paths must be private regular files.");
        }
    }

    private static bool IsUnsafeExistingSibling(string path)
    {
        try
        {
            return (File.GetAttributes(path) & UnsafeFileAttributes) != 0;
        }
        catch (Exception exception) when (
            exception is FileNotFoundException or DirectoryNotFoundException)
        {
            return false;
        }
    }

    private void ThrowIfDisposed() =>
        ObjectDisposedException.ThrowIf(disposed, this);

    private sealed record PreparedPathBinding(
        string PreparedDataPath,
        string CheckpointPath,
        string PendingCheckpointPath,
        string ParentPath);

    private sealed record PublicationPathBinding(
        string DestinationPath,
        string ManifestPath,
        string DataTemporaryPath,
        string ManifestTemporaryPath);

    private sealed record PublishedPrivateFile(
        FileStream Stream,
        bool Reused);

    private enum NoReplaceRenameStatus
    {
        Published,
        DestinationExists,
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct WindowsFileInformation
    {
        internal uint FileAttributes;
        internal System.Runtime.InteropServices.ComTypes.FILETIME CreationTime;
        internal System.Runtime.InteropServices.ComTypes.FILETIME LastAccessTime;
        internal System.Runtime.InteropServices.ComTypes.FILETIME LastWriteTime;
        internal uint VolumeSerialNumber;
        internal uint FileSizeHigh;
        internal uint FileSizeLow;
        internal uint NumberOfLinks;
        internal uint FileIndexHigh;
        internal uint FileIndexLow;
    }

    [DllImport(
        "kernel32.dll",
        EntryPoint = "CreateFileW",
        CharSet = CharSet.Unicode,
        ExactSpelling = true,
        SetLastError = true)]
    private static extern SafeFileHandle CreateFileW(
        string fileName,
        uint desiredAccess,
        uint shareMode,
        IntPtr securityAttributes,
        uint creationDisposition,
        uint flagsAndAttributes,
        IntPtr templateFile);

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GetFileInformationByHandle(
        SafeFileHandle file,
        out WindowsFileInformation fileInformation);

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool SetFileInformationByHandle(
        SafeFileHandle file,
        int fileInformationClass,
        IntPtr fileInformation,
        uint bufferSize);

    [DllImport(
        "kernel32.dll",
        EntryPoint = "GetFinalPathNameByHandleW",
        CharSet = CharSet.Unicode,
        ExactSpelling = true,
        SetLastError = true)]
    private static extern uint GetFinalPathNameByHandleW(
        SafeFileHandle file,
        StringBuilder filePath,
        uint filePathLength,
        uint flags);
}

#pragma warning restore CA1416
