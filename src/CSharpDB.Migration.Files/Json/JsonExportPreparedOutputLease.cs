using System.Buffers;
using System.Security.Cryptography;
using System.Text;

namespace CSharpDB.Migration.Files.Json;

/// <summary>
/// Public opening state of one private prepared JSON or NDJSON output.
/// </summary>
public enum JsonExportPreparedOutputLeaseState
{
    New,
    Recovered,
    UncheckpointedData,
}

/// <summary>
/// Owns the exclusive Windows prepared-output lease and its durable checkpoint
/// journal. Final data and manifest destinations are never published here.
/// The journal identity uses the destination's exact normalized spelling;
/// cooperating callers must use one spelling consistently because case
/// aliases are not mutually excluded on a case-insensitive directory.
/// The security boundary excludes another same-SID actor that already has
/// independent parent-directory namespace mutation authority.
/// </summary>
public sealed class JsonExportPreparedOutputLease :
    IJsonExportPreparedOutputSession
{
    private const int HashBufferSize =
        64 * 1024;

    private const string PathBindingContract =
        "csharpdb-json-export-prepared-output-path/v1";

    private readonly JsonExportPreparedOutputFileSystem
        fileSystem;

    private readonly JsonExportHashManifest
        expectedBindingDigest;

    private readonly IJsonExportCheckpointFaultInjector?
        checkpointFaultInjector;

    private readonly SemaphoreSlim operationGate =
        new(1, 1);

    private byte[]? currentCheckpointBytes;
    private bool disposed;

    private JsonExportPreparedOutputLease(
        string destinationPath,
        JsonExportPreparedOutputPaths paths,
        JsonExportPreparedOutputFileSystem fileSystem,
        JsonExportHashManifest expectedBindingDigest,
        JsonExportPreparedOutputLeaseState state,
        JsonExportCheckpoint? currentCheckpoint,
        byte[]? currentCheckpointBytes,
        IJsonExportCheckpointFaultInjector?
            checkpointFaultInjector)
    {
        DestinationPath = destinationPath;
        Paths = paths;
        this.fileSystem = fileSystem;
        this.expectedBindingDigest =
            expectedBindingDigest;
        State = state;
        CurrentCheckpoint =
            currentCheckpoint;
        this.currentCheckpointBytes =
            currentCheckpointBytes;
        this.checkpointFaultInjector =
            checkpointFaultInjector;
    }

    /// <summary>The normalized future destination.</summary>
    public string DestinationPath { get; }

    /// <summary>The deterministic private files owned by this lease.</summary>
    public JsonExportPreparedOutputPaths Paths { get; }

    /// <summary>The state observed and qualified while opening.</summary>
    public JsonExportPreparedOutputLeaseState State
    {
        get;
        private set;
    }

    /// <summary>The active durable checkpoint, if one exists.</summary>
    public JsonExportCheckpoint? CurrentCheckpoint
    {
        get;
        private set;
    }

    /// <summary>
    /// The exclusively leased prepared stream. Bytes without an active
    /// checkpoint must be explicitly reset before they can be reused.
    /// </summary>
    public Stream DataStream
    {
        get
        {
            ThrowIfDisposed();
            if (State ==
                JsonExportPreparedOutputLeaseState
                    .UncheckpointedData)
            {
                throw new InvalidOperationException(
                    "The prepared JSON output contains uncheckpointed bytes and must be explicitly reset.");
            }

            return fileSystem.DataStream;
        }
    }

    JsonExportPreparedOutputState
        IJsonExportPreparedOutputSession.State =>
        State switch
        {
            JsonExportPreparedOutputLeaseState.New =>
                JsonExportPreparedOutputState.New,
            JsonExportPreparedOutputLeaseState.Recovered =>
                JsonExportPreparedOutputState.Recovered,
            JsonExportPreparedOutputLeaseState
                .UncheckpointedData =>
                JsonExportPreparedOutputState
                    .UncheckpointedData,
            _ => throw new InvalidOperationException(
                "The prepared JSON lease state is unsupported."),
        };

    Stream IJsonExportPreparedOutputSession.DataStream =>
        DataStream;

    /// <summary>
    /// Opens and exclusively qualifies the private prepared output. Only the
    /// active checkpoint is authoritative; a pending sibling is never used as
    /// recovery authority. The destination must use one consistent exact
    /// spelling across cooperating calls.
    /// </summary>
    public static ValueTask<JsonExportPreparedOutputLease>
        OpenAsync(
        string destinationPath,
        JsonExportCheckpointBinding expectedBinding,
        CancellationToken cancellationToken =
            default) =>
        OpenAsyncCore(
            destinationPath,
            expectedBinding,
            allowCompletedDestination:
                false,
            checkpointFaultInjector: null,
            cancellationToken);

    internal static ValueTask<
        JsonExportPreparedOutputLease>
        OpenAllowingCompletedDestinationAsync(
        string destinationPath,
        JsonExportCheckpointBinding expectedBinding,
        CancellationToken cancellationToken) =>
        OpenAsyncCore(
            destinationPath,
            expectedBinding,
            allowCompletedDestination:
                true,
            checkpointFaultInjector: null,
            cancellationToken);

    internal static ValueTask<
        JsonExportPreparedOutputLease>
        OpenWithCheckpointFaultInjectorAsync(
        string destinationPath,
        JsonExportCheckpointBinding expectedBinding,
        IJsonExportCheckpointFaultInjector
            checkpointFaultInjector,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(
            checkpointFaultInjector);
        return OpenAsyncCore(
            destinationPath,
            expectedBinding,
            allowCompletedDestination:
                false,
            checkpointFaultInjector,
            cancellationToken);
    }

    internal static async ValueTask<
        JsonExportPreparedOutputLease>
        OpenForPublicationAsync(
        string destinationPath,
        string expectedManifestDigest,
        CancellationToken cancellationToken)
    {
        cancellationToken
            .ThrowIfCancellationRequested();
        ValidateManifestDigest(
            expectedManifestDigest);
        (
            string normalizedDestination,
            JsonExportPreparedOutputPaths paths,
            _
        ) = BindPathsCore(
            destinationPath,
            allowExistingDestination: true);

        JsonExportPreparedOutputFileSystem
            fileSystem =
                JsonExportPreparedOutputFileSystem
                    .Open(
                        paths,
                        requireExistingData:
                            true);
        try
        {
            byte[]? checkpointBytes =
                await fileSystem
                    .ReadActiveCheckpointAsync(
                        cancellationToken)
                    .ConfigureAwait(false);
            if (checkpointBytes is null)
            {
                throw new InvalidDataException(
                    "JSON export publication requires an active data-complete checkpoint.");
            }

            JsonExportCheckpoint checkpoint =
                JsonExportCheckpointSerializer
                    .Deserialize(
                        checkpointBytes);
            RequireDataCompleteCheckpoint(
                checkpoint);
            RequireManifestDigestEquals(
                checkpoint.Completion!
                    .ManifestDigest,
                expectedManifestDigest,
                "The completed JSON export does not match the expected manifest digest.");
            await QualifyAndRecoverPrefixAsync(
                    fileSystem,
                    checkpoint,
                    cancellationToken)
                .ConfigureAwait(false);

            return new
                JsonExportPreparedOutputLease(
                    normalizedDestination,
                    paths,
                    fileSystem,
                    checkpoint.BindingDigest,
                    JsonExportPreparedOutputLeaseState
                        .Recovered,
                    checkpoint,
                    checkpointBytes,
                    checkpointFaultInjector:
                        null);
        }
        catch
        {
            await fileSystem
                .DisposeAsync()
                .ConfigureAwait(false);
            throw;
        }
    }

    private static async ValueTask<
        JsonExportPreparedOutputLease>
        OpenAsyncCore(
        string destinationPath,
        JsonExportCheckpointBinding expectedBinding,
        bool allowCompletedDestination,
        IJsonExportCheckpointFaultInjector?
            checkpointFaultInjector,
        CancellationToken cancellationToken)
    {
        cancellationToken
            .ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(
            expectedBinding);

        JsonExportHashManifest bindingDigest =
            JsonExportCheckpointSerializer
                .ComputeBindingDigest(
                    expectedBinding);
        (
            string normalizedDestination,
            JsonExportPreparedOutputPaths paths,
            _
        ) = BindPathsCore(
            destinationPath,
            allowExistingDestination:
                allowCompletedDestination);

        JsonExportPreparedOutputFileSystem
            fileSystem =
                JsonExportPreparedOutputFileSystem
                    .Open(
                        paths,
                        requireExistingData:
                            false);
        try
        {
            byte[]? checkpointBytes =
                await fileSystem
                    .ReadActiveCheckpointAsync(
                        cancellationToken)
                    .ConfigureAwait(false);
            if (checkpointBytes is null)
            {
                JsonExportPreparedOutputLeaseState
                    state =
                        fileSystem.DataStream.Length ==
                        0
                            ? JsonExportPreparedOutputLeaseState
                                .New
                            : JsonExportPreparedOutputLeaseState
                                .UncheckpointedData;
                fileSystem.DataStream.Position = 0;
                return new
                    JsonExportPreparedOutputLease(
                        normalizedDestination,
                        paths,
                        fileSystem,
                        bindingDigest,
                        state,
                        currentCheckpoint: null,
                        currentCheckpointBytes:
                            null,
                        checkpointFaultInjector);
            }

            JsonExportCheckpoint checkpoint =
                JsonExportCheckpointSerializer
                    .Deserialize(
                        checkpointBytes);
            RequireHashEquals(
                checkpoint.BindingDigest,
                bindingDigest,
                "The active JSON export checkpoint belongs to a different export binding.");
            await QualifyAndRecoverPrefixAsync(
                    fileSystem,
                    checkpoint,
                    cancellationToken)
                .ConfigureAwait(false);

            return new
                JsonExportPreparedOutputLease(
                    normalizedDestination,
                    paths,
                    fileSystem,
                    bindingDigest,
                    JsonExportPreparedOutputLeaseState
                        .Recovered,
                    checkpoint,
                    checkpointBytes,
                    checkpointFaultInjector);
        }
        catch
        {
            await fileSystem
                .DisposeAsync()
                .ConfigureAwait(false);
            throw;
        }
    }

    internal async ValueTask<
        JsonExportPreparedOutputPublicationQualification>
        QualifyForPublicationAsync(
        string expectedManifestDigest,
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        ValidateManifestDigest(
            expectedManifestDigest);
        await operationGate
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
        bool releaseGate = true;
        try
        {
            ThrowIfDisposed();
            byte[] checkpointBytes =
                currentCheckpointBytes ??
                throw new InvalidOperationException(
                    "JSON export publication requires an active checkpoint.");
            JsonExportCheckpoint checkpoint =
                JsonExportCheckpointSerializer
                    .Deserialize(
                        checkpointBytes);
            RequireDataCompleteCheckpoint(
                checkpoint);
            RequireManifestDigestEquals(
                checkpoint.Completion!
                    .ManifestDigest,
                expectedManifestDigest,
                "The completed JSON export does not match the expected manifest digest.");
            await QualifyAndRecoverPrefixAsync(
                    fileSystem,
                    checkpoint,
                    cancellationToken)
                .ConfigureAwait(false);

            CurrentCheckpoint = checkpoint;
            var qualification =
                new JsonExportPreparedOutputPublicationQualification(
                    DestinationPath,
                    Paths,
                    fileSystem.DataStream,
                    checkpoint,
                    () => operationGate.Release());
            releaseGate = false;
            return qualification;
        }
        finally
        {
            if (releaseGate)
                operationGate.Release();
        }
    }

    /// <summary>
    /// Explicitly discards private bytes that have no active checkpoint.
    /// </summary>
    public async ValueTask
        ResetUncheckpointedAsync(
        CancellationToken cancellationToken =
            default)
    {
        ThrowIfDisposed();
        await operationGate
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();
            if (State !=
                    JsonExportPreparedOutputLeaseState
                        .UncheckpointedData ||
                CurrentCheckpoint is not null)
            {
                throw new InvalidOperationException(
                    "Only an uncheckpointed prepared JSON output can be reset.");
            }

            cancellationToken
                .ThrowIfCancellationRequested();
            fileSystem.TruncateData(0);
            await fileSystem
                .FlushDataToDiskAsync(
                    cancellationToken)
                .ConfigureAwait(false);
            fileSystem.DataStream.Position = 0;
            State =
                JsonExportPreparedOutputLeaseState
                    .New;
        }
        finally
        {
            operationGate.Release();
        }
    }

    /// <summary>
    /// Makes one complete-object checkpoint durable in data-first order:
    /// durable data, durable pending checkpoint, then pinned-parent-relative
    /// active replacement. Cancellation after pending durability is governed
    /// by the filesystem's non-cancelable activation cutoff.
    /// </summary>
    public async ValueTask PersistCheckpointAsync(
        JsonExportCheckpoint checkpoint,
        CancellationToken cancellationToken =
            default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(
            checkpoint);

        byte[] canonicalBytes =
            JsonExportCheckpointSerializer
                .Serialize(checkpoint);
        JsonExportCheckpoint canonicalCheckpoint =
            JsonExportCheckpointSerializer
                .Deserialize(canonicalBytes);
        RequireHashEquals(
            canonicalCheckpoint.BindingDigest,
            expectedBindingDigest,
            "The JSON export checkpoint does not match this prepared-output lease.");

        await operationGate
            .WaitAsync(cancellationToken)
            .ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();
            if (State ==
                JsonExportPreparedOutputLeaseState
                    .UncheckpointedData)
            {
                throw new InvalidOperationException(
                    "Uncheckpointed prepared JSON bytes must be explicitly reset before checkpointing.");
            }

            bool idempotent =
                ValidateTransition(
                    CurrentCheckpoint,
                    currentCheckpointBytes,
                    canonicalCheckpoint,
                    canonicalBytes);
            FileStream data =
                fileSystem.DataStream;
            if (data.Length !=
                    canonicalCheckpoint.Progress
                        .DataPrefixByteLength ||
                data.Position !=
                    canonicalCheckpoint.Progress
                        .DataPrefixByteLength)
            {
                throw new InvalidOperationException(
                    "The prepared JSON stream must end exactly at the checkpoint byte boundary.");
            }

            await fileSystem
                .FlushDataToDiskAsync(
                    cancellationToken)
                .ConfigureAwait(false);
            await VerifyPrefixAsync(
                    data,
                    canonicalCheckpoint,
                    cancellationToken)
                .ConfigureAwait(false);
            if (idempotent)
                return;

            await InjectCheckpointFaultAsync(
                    checkpointFaultInjector,
                    JsonExportCheckpointFaultPoint
                        .AfterDataDurablyFlushedBeforePendingCheckpoint,
                    cancellationToken)
                .ConfigureAwait(false);
            try
            {
                await fileSystem
                    .ReplaceCheckpointAsync(
                        canonicalBytes,
                        checkpointFaultInjector,
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception replaceException)
            {
                // Replacement may already have crossed the active-rename
                // commit point. Poison this in-memory authority so it cannot
                // attempt another same-generation commit without reopening
                // and re-reading the durable active checkpoint.
                disposed = true;
                try
                {
                    await fileSystem
                        .DisposeAsync()
                        .ConfigureAwait(false);
                }
                catch (Exception disposeException)
                {
                    throw new AggregateException(
                        "JSON checkpoint replacement failed and the prepared-output lease could not be closed.",
                        replaceException,
                        disposeException);
                }

                throw;
            }

            CurrentCheckpoint =
                canonicalCheckpoint;
            currentCheckpointBytes =
                canonicalBytes;
        }
        finally
        {
            operationGate.Release();
        }
    }

    /// <summary>
    /// Releases handles without deleting or rolling back prepared files.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        await operationGate
            .WaitAsync()
            .ConfigureAwait(false);
        try
        {
            if (disposed)
                return;
            disposed = true;
            await fileSystem
                .DisposeAsync()
                .ConfigureAwait(false);
        }
        finally
        {
            operationGate.Release();
        }
    }

    private static async ValueTask
        QualifyAndRecoverPrefixAsync(
        JsonExportPreparedOutputFileSystem
            fileSystem,
        JsonExportCheckpoint checkpoint,
        CancellationToken cancellationToken)
    {
        FileStream data =
            fileSystem.DataStream;
        long prefixLength =
            checkpoint.Progress
                .DataPrefixByteLength;
        long originalLength =
            data.Length;
        if (originalLength < prefixLength)
        {
            throw new InvalidDataException(
                "The prepared JSON data is shorter than its active checkpoint.");
        }

        await VerifyPrefixAsync(
                data,
                checkpoint,
                cancellationToken)
            .ConfigureAwait(false);

        if (originalLength > prefixLength)
        {
            cancellationToken
                .ThrowIfCancellationRequested();
            fileSystem.TruncateData(
                prefixLength);
            await fileSystem
                .FlushDataToDiskAsync(
                    cancellationToken)
                .ConfigureAwait(false);
        }

        data.Position = prefixLength;
    }

    private static async ValueTask VerifyPrefixAsync(
        FileStream data,
        JsonExportCheckpoint checkpoint,
        CancellationToken cancellationToken)
    {
        JsonExportCheckpointProgress progress =
            checkpoint.Progress;
        long position =
            data.Position;
        byte[] buffer =
            ArrayPool<byte>.Shared.Rent(
                HashBufferSize);
        try
        {
            using IncrementalHash hash =
                IncrementalHash.CreateHash(
                    HashAlgorithmName.SHA256);
            data.Position = 0;
            long remaining =
                progress.DataPrefixByteLength;
            while (remaining > 0)
            {
                int requested =
                    (int)Math.Min(
                        remaining,
                        buffer.Length);
                int read =
                    await data.ReadAsync(
                            buffer.AsMemory(
                                0,
                                requested),
                            cancellationToken)
                        .ConfigureAwait(false);
                if (read == 0)
                {
                    throw new InvalidDataException(
                        "The prepared JSON data ended before its checkpoint boundary.");
                }

                hash.AppendData(
                    buffer,
                    0,
                    read);
                remaining -= read;
            }

            byte[] actual =
                hash.GetHashAndReset();
            try
            {
                RequireHashEquals(
                    CreateHashManifest(actual),
                    progress.DataPrefixDigest,
                    "The prepared JSON data prefix does not match its checkpoint.");
            }
            finally
            {
                CryptographicOperations
                    .ZeroMemory(actual);
            }

            await ValidateObservedBoundaryAsync(
                    data,
                    checkpoint,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            data.Position = position;
            CryptographicOperations.ZeroMemory(
                buffer.AsSpan(
                    0,
                    buffer.Length));
            ArrayPool<byte>.Shared.Return(
                buffer);
        }
    }

    private static async ValueTask
        ValidateObservedBoundaryAsync(
        FileStream data,
        JsonExportCheckpoint checkpoint,
        CancellationToken cancellationToken)
    {
        JsonExportCheckpointProgress progress =
            checkpoint.Progress;
        long length =
            progress.DataPrefixByteLength;
        int trailingLength =
            (
                checkpoint.Binding.Json.Framing,
                checkpoint.Phase,
                progress.CompletedRowCount > 0
            ) switch
            {
                (
                    JsonExportFraming.RootArray,
                    JsonExportCheckpointPhase.Writing,
                    _) => 1,
                (
                    JsonExportFraming.RootArray,
                    JsonExportCheckpointPhase
                        .DataComplete,
                    _) => 3,
                (
                    JsonExportFraming.Ndjson,
                    _,
                    false) => 0,
                (
                    JsonExportFraming.Ndjson,
                    _,
                    true) => 2,
                _ => throw new InvalidDataException(
                    "The checkpoint JSON framing is unsupported."),
            };

        byte? firstByte = null;
        if (length > 0)
        {
            data.Position = 0;
            int value =
                await ReadOneByteAsync(
                        data,
                        cancellationToken)
                    .ConfigureAwait(false);
            firstByte =
                checked((byte)value);
        }

        byte[] trailing =
            new byte[trailingLength];
        if (trailing.Length != 0)
        {
            data.Position =
                checked(
                    length -
                    trailing.Length);
            await ReadExactlyAsync(
                    data,
                    trailing,
                    cancellationToken)
                .ConfigureAwait(false);
        }

        _ = JsonExportCheckpointFraming
            .ValidateObservedBoundary(
                checkpoint.Binding,
                checkpoint.Phase,
                progress,
                firstByte,
                trailing);
    }

    private static async ValueTask<int>
        ReadOneByteAsync(
        Stream stream,
        CancellationToken cancellationToken)
    {
        byte[] value = new byte[1];
        await ReadExactlyAsync(
                stream,
                value,
                cancellationToken)
            .ConfigureAwait(false);
        return value[0];
    }

    private static async ValueTask ReadExactlyAsync(
        Stream stream,
        Memory<byte> destination,
        CancellationToken cancellationToken)
    {
        int offset = 0;
        while (offset < destination.Length)
        {
            int read =
                await stream.ReadAsync(
                        destination[offset..],
                        cancellationToken)
                    .ConfigureAwait(false);
            if (read == 0)
            {
                throw new InvalidDataException(
                    "The prepared JSON data ended before its checkpoint boundary.");
            }
            offset += read;
        }
    }

    private static bool ValidateTransition(
        JsonExportCheckpoint? current,
        byte[]? currentBytes,
        JsonExportCheckpoint next,
        byte[] nextBytes)
    {
        if (current is null)
        {
            if (next.Generation != 0 ||
                next.Phase !=
                    JsonExportCheckpointPhase
                        .Writing)
            {
                throw new InvalidOperationException(
                    "The first durable JSON export checkpoint must be writing generation zero.");
            }

            return false;
        }

        JsonExportCheckpointFraming
            .ValidateTransition(
                current,
                next);
        if (next.Generation !=
            current.Generation)
        {
            return false;
        }
        if (currentBytes is null ||
            !currentBytes.AsSpan()
                .SequenceEqual(nextBytes))
        {
            throw new InvalidOperationException(
                "An equal-generation JSON export checkpoint must have identical canonical bytes.");
        }

        return true;
    }

    private static ValueTask
        InjectCheckpointFaultAsync(
        IJsonExportCheckpointFaultInjector?
            faultInjector,
        JsonExportCheckpointFaultPoint point,
        CancellationToken cancellationToken) =>
        faultInjector?.InjectAsync(
            point,
        cancellationToken) ??
        ValueTask.CompletedTask;

    private static void RequireDataCompleteCheckpoint(
        JsonExportCheckpoint checkpoint)
    {
        if (checkpoint.Phase !=
                JsonExportCheckpointPhase
                    .DataComplete ||
            checkpoint.Completion is null)
        {
            throw new InvalidDataException(
                "Only a data-complete JSON export can be published.");
        }
    }

    private static void ValidateManifestDigest(
        string expectedManifestDigest)
    {
        ArgumentException
            .ThrowIfNullOrWhiteSpace(
                expectedManifestDigest);
        if (expectedManifestDigest.Length !=
                SHA256.HashSizeInBytes * 2 ||
            expectedManifestDigest.Any(
                static value =>
                    value is not
                        (>= '0' and <= '9') and
                        not
                        (>= 'a' and <= 'f')))
        {
            throw new ArgumentException(
                "The expected JSON export manifest digest must be lowercase SHA-256 text.",
                nameof(expectedManifestDigest));
        }
    }

    private static void RequireManifestDigestEquals(
        string supplied,
        string expected,
        string message)
    {
        byte[] suppliedBytes;
        byte[] expectedBytes;
        try
        {
            suppliedBytes =
                Convert.FromHexString(
                    supplied);
            expectedBytes =
                Convert.FromHexString(
                    expected);
        }
        catch (Exception exception) when (
            exception is
                FormatException or
                ArgumentNullException)
        {
            throw new InvalidDataException(
                message);
        }

        try
        {
            if (!CryptographicOperations
                    .FixedTimeEquals(
                        suppliedBytes,
                        expectedBytes))
            {
                throw new InvalidDataException(
                    message);
            }
        }
        finally
        {
            CryptographicOperations
                .ZeroMemory(
                    suppliedBytes);
            CryptographicOperations
                .ZeroMemory(
                    expectedBytes);
        }
    }

    private static void RequireHashEquals(
        JsonExportHashManifest actual,
        JsonExportHashManifest expected,
        string message)
    {
        if (actual is null ||
            expected is null ||
            !string.Equals(
                actual.Algorithm,
                expected.Algorithm,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                message);
        }

        byte[] actualBytes;
        byte[] expectedBytes;
        try
        {
            actualBytes =
                Convert.FromHexString(
                    actual.Value);
            expectedBytes =
                Convert.FromHexString(
                    expected.Value);
        }
        catch (Exception exception) when (
            exception is
                FormatException or
                ArgumentNullException)
        {
            throw new InvalidDataException(
                message);
        }

        try
        {
            if (!CryptographicOperations
                    .FixedTimeEquals(
                        actualBytes,
                        expectedBytes))
            {
                throw new InvalidDataException(
                    message);
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(
                actualBytes);
            CryptographicOperations.ZeroMemory(
                expectedBytes);
        }
    }

    private static JsonExportHashManifest
        CreateHashManifest(
        ReadOnlySpan<byte> value) =>
        new()
        {
            Algorithm =
                JsonExportHashManifest
                    .Sha256Algorithm,
            Value =
                Convert.ToHexString(value)
                    .ToLowerInvariant(),
        };

    internal static (
        string Destination,
        JsonExportPreparedOutputPaths Paths)
        BindPaths(
        string destinationPath)
    {
        (
            string destination,
            JsonExportPreparedOutputPaths paths,
            _
        ) = BindPathsCore(
            destinationPath,
            allowExistingDestination:
                false);
        return (
            destination,
            paths);
    }

    internal static (
        string Destination,
        JsonExportPreparedOutputPaths Paths)
        BindPathsAllowingCompletedDestination(
        string destinationPath)
    {
        (
            string destination,
            JsonExportPreparedOutputPaths paths,
            _
        ) = BindPathsCore(
            destinationPath,
            allowExistingDestination:
                true);
        return (
            destination,
            paths);
    }

    private static (
        string Destination,
        JsonExportPreparedOutputPaths Paths,
        bool DestinationExists)
        BindPathsCore(
        string destinationPath,
        bool allowExistingDestination)
    {
        ArgumentException
            .ThrowIfNullOrWhiteSpace(
                destinationPath);
        if (destinationPath.Contains('\0'))
        {
            throw new ArgumentException(
                "The JSON export destination cannot contain a null character.",
                nameof(destinationPath));
        }
        RejectInvalidUnicode(
            destinationPath);
        if (!Path.IsPathFullyQualified(
                destinationPath))
        {
            throw new ArgumentException(
                "The JSON export destination must be fully qualified.",
                nameof(destinationPath));
        }
        RejectDotSegments(
            destinationPath);
        RejectWindowsSpecialPath(
            destinationPath);

        string normalized =
            Path.GetFullPath(
                destinationPath);
        if (!string.Equals(
                normalized,
                destinationPath,
                StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "The JSON export destination must be normalized and cannot contain traversal.",
                nameof(destinationPath));
        }

        string parent =
            Path.GetDirectoryName(
                normalized) ??
            throw new ArgumentException(
                "The JSON export destination must have a parent directory.",
                nameof(destinationPath));
        string leaf =
            Path.GetFileName(
                normalized);
        if (string.IsNullOrWhiteSpace(
                leaf) ||
            leaf is "." or "..")
        {
            throw new ArgumentException(
                "The JSON export destination file name is invalid.",
                nameof(destinationPath));
        }
        JsonExportPathPreflight
            .RejectReservedPrivateLeaf(
                normalized,
                nameof(destinationPath));
        if (!Directory.Exists(parent))
        {
            throw new DirectoryNotFoundException(
                "The JSON export destination parent directory does not exist.");
        }
        bool destinationExists =
            TryGetAttributes(
                normalized,
                out FileAttributes attributes);
        if (destinationExists &&
            !allowExistingDestination)
        {
            throw new InvalidDataException(
                "The JSON export destination already exists; prepared export never overwrites it.");
        }
        if (destinationExists &&
            (attributes &
             (
                 FileAttributes.Directory |
                 FileAttributes.ReparsePoint |
                 FileAttributes.Device
             )) != 0)
        {
            throw new InvalidDataException(
                "The existing JSON export destination must be a regular file.");
        }

        byte[] bindingBytes =
            Encoding.UTF8.GetBytes(
                PathBindingContract +
                "\0" +
                normalized);
        string digest;
        try
        {
            digest =
                Convert.ToHexString(
                        SHA256.HashData(
                            bindingBytes))
                    .ToLowerInvariant();
        }
        finally
        {
            CryptographicOperations
                .ZeroMemory(
                    bindingBytes);
        }

        string stem =
            $".csharpdb-json-export-{digest[..32]}";
        return (
            normalized,
            new JsonExportPreparedOutputPaths
            {
                PreparedDataPath =
                    Path.Combine(
                        parent,
                        stem + ".prepared"),
                CheckpointPath =
                    Path.Combine(
                        parent,
                        stem + ".checkpoint"),
                PendingCheckpointPath =
                    Path.Combine(
                        parent,
                        stem +
                        ".checkpoint.next"),
            },
            destinationExists);
    }

    private static void RejectDotSegments(
        string path)
    {
        string root =
            Path.GetPathRoot(path) ??
            string.Empty;
        foreach (string segment in
                 path[root.Length..]
                     .Split(
                         [
                             Path
                                 .DirectorySeparatorChar,
                             Path
                                 .AltDirectorySeparatorChar,
                         ],
                         StringSplitOptions.None))
        {
            if (segment is "." or "..")
            {
                throw new ArgumentException(
                    "The JSON export destination cannot contain traversal segments.",
                    nameof(path));
            }
        }
    }

    private static void RejectInvalidUnicode(
        string path)
    {
        for (int index = 0;
             index < path.Length;
             index++)
        {
            char value = path[index];
            if (!char.IsSurrogate(value))
                continue;
            if (char.IsHighSurrogate(
                    value) &&
                index + 1 < path.Length &&
                char.IsLowSurrogate(
                    path[index + 1]))
            {
                index++;
                continue;
            }

            throw new ArgumentException(
                "The JSON export destination must contain valid Unicode scalar data.",
                nameof(path));
        }
    }

    private static void RejectWindowsSpecialPath(
        string path)
    {
        if (!OperatingSystem.IsWindows())
            return;
        if (path.StartsWith(
                "\\\\?\\",
                StringComparison.Ordinal) ||
            path.StartsWith(
                "\\\\.\\",
                StringComparison.Ordinal) ||
            path.StartsWith(
                "\\\\",
                StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "Windows device, extended, and network paths cannot be used for prepared JSON export.",
                nameof(path));
        }

        string root =
            Path.GetPathRoot(path) ??
            string.Empty;
        if (path.AsSpan(
                root.Length)
            .Contains(':'))
        {
            throw new ArgumentException(
                "Windows alternate data streams cannot be used for prepared JSON export.",
                nameof(path));
        }

        foreach (string segment in
                 path[root.Length..]
                     .Split(
                         [
                             Path
                                 .DirectorySeparatorChar,
                             Path
                                 .AltDirectorySeparatorChar,
                         ],
                         StringSplitOptions
                             .RemoveEmptyEntries))
        {
            if (segment.Contains('~'))
            {
                throw new ArgumentException(
                    "Windows DOS short-name aliases cannot be used for prepared JSON export paths.",
                    nameof(path));
            }
            if (segment.EndsWith(' ') ||
                segment.EndsWith('.'))
            {
                throw new ArgumentException(
                    "Windows JSON export path segments cannot end in spaces or dots.",
                    nameof(path));
            }
            if (segment.IndexOfAny(
                    Path.GetInvalidFileNameChars()) >=
                0)
            {
                throw new ArgumentException(
                    "The JSON export path contains invalid Windows characters.",
                    nameof(path));
            }
        }

        string destinationLeaf =
            Path.GetFileName(path);
        int firstDot =
            destinationLeaf.IndexOf('.');
        string nameStem =
            (firstDot < 0
                ? destinationLeaf
                : destinationLeaf[..firstDot])
            .TrimEnd(' ', '.');
        if (nameStem.Equals(
                "CON",
                StringComparison.OrdinalIgnoreCase) ||
            nameStem.Equals(
                "PRN",
                StringComparison.OrdinalIgnoreCase) ||
            nameStem.Equals(
                "AUX",
                StringComparison.OrdinalIgnoreCase) ||
            nameStem.Equals(
                "NUL",
                StringComparison.OrdinalIgnoreCase) ||
            (nameStem.Length == 4 &&
             (nameStem.StartsWith(
                  "COM",
                  StringComparison
                      .OrdinalIgnoreCase) ||
              nameStem.StartsWith(
                  "LPT",
                  StringComparison
                      .OrdinalIgnoreCase)) &&
             nameStem[3] is
                 >= '1' and <= '9' or
                 '\u00b9' or
                 '\u00b2' or
                 '\u00b3'))
        {
            throw new ArgumentException(
                "Windows reserved device names cannot be used for prepared JSON export.",
                nameof(path));
        }
    }

    private static bool TryGetAttributes(
        string path,
        out FileAttributes attributes)
    {
        try
        {
            attributes =
                File.GetAttributes(path);
            return true;
        }
        catch (FileNotFoundException)
        {
            attributes = default;
            return false;
        }
        catch (DirectoryNotFoundException)
        {
            attributes = default;
            return false;
        }
    }

    private void ThrowIfDisposed() =>
        ObjectDisposedException.ThrowIf(
            disposed,
            this);
}

/// <summary>
/// Borrowed, gate-held view of a fully requalified data-complete prepared
/// output. The parent lease owns the stream and must outlive this view.
/// Disposing this view releases only the parent's operation gate.
/// </summary>
internal sealed class
    JsonExportPreparedOutputPublicationQualification :
    IAsyncDisposable
{
    private Action? releaseGate;

    internal
        JsonExportPreparedOutputPublicationQualification(
        string destinationPath,
        JsonExportPreparedOutputPaths paths,
        FileStream dataStream,
        JsonExportCheckpoint checkpoint,
        Action releaseGate)
    {
        DestinationPath =
            destinationPath;
        Paths =
            paths;
        DataStream =
            dataStream;
        Checkpoint =
            checkpoint;
        this.releaseGate =
            releaseGate;
    }

    internal string DestinationPath { get; }

    internal JsonExportPreparedOutputPaths Paths { get; }

    /// <summary>
    /// Borrowed exclusive prepared-data handle. The qualification consumer
    /// must not dispose it.
    /// </summary>
    internal FileStream DataStream { get; }

    internal JsonExportCheckpoint Checkpoint { get; }

    public ValueTask DisposeAsync()
    {
        Action? release =
            Interlocked.Exchange(
                ref releaseGate,
                null);
        release?.Invoke();
        return ValueTask.CompletedTask;
    }
}
