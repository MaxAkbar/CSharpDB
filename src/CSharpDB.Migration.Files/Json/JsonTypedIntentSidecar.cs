using System.Security.Cryptography;
using System.Text;

namespace CSharpDB.Migration.Files.Json;

/// <summary>
/// Publishes and verifies a value-free, source-bound typed interpretation
/// sidecar for an immutable JSON source binding.
/// </summary>
public static class JsonTypedIntentSidecar
{
    public const string Format =
        JsonTypedIntentManifestSerializer.Format;
    public const string FileExtension =
        ".csdbjson-intent.json";

    private const int FileBufferBytes = 64 * 1024;
    private static readonly UTF8Encoding s_strictUtf8 = new(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);

    /// <summary>
    /// Atomically publishes one canonical sidecar. The destination must not
    /// already exist. The returned digest pins the exact outer file bytes.
    /// </summary>
    public static async ValueTask<JsonTypedIntentManifest> WriteAsync(
        string path,
        JsonSourceBinding binding,
        JsonTypedIntentOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        ArgumentNullException.ThrowIfNull(binding);
        ArgumentNullException.ThrowIfNull(options);

        JsonTypedColumnIntent[] columns =
            ValidateAndCloneOptions(binding, options);
        JsonTypedIntentManifestPayload payload =
            CreatePayload(binding, options, columns);

        byte[] canonicalBytes;
        try
        {
            canonicalBytes =
                JsonTypedIntentManifestSerializer.Serialize(payload);
        }
        catch (Exception exception) when (
            exception is InvalidDataException or
            JsonTypedIntentManifestValidationException)
        {
            throw new ArgumentException(
                "The typed JSON intent options cannot be serialized as a safe canonical sidecar.",
                nameof(options),
                exception);
        }

        byte[] manifestHash = SHA256.HashData(canonicalBytes);
        string manifestDigest = FormatDigest(manifestHash);
        var manifest = new JsonTypedIntentManifest(
            manifestDigest,
            ClonePayload(payload),
            canonicalBytes);

        string fullPath = Path.GetFullPath(path);
        string parentPath = ValidateDestination(fullPath);
        string tempPath = Path.Combine(
            parentPath,
            $".csdbjson-intent-{Guid.NewGuid():N}.tmp");
        bool tempOwned = false;
        bool published = false;

        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            FileStream destination = CreateSidecarFile(tempPath);
            tempOwned = true;
            await using (destination)
            {
                await destination
                    .WriteAsync(canonicalBytes, cancellationToken)
                    .ConfigureAwait(false);
                await destination
                    .FlushAsync(cancellationToken)
                    .ConfigureAwait(false);
                destination.Flush(flushToDisk: true);
            }

            cancellationToken.ThrowIfCancellationRequested();
            File.Move(tempPath, fullPath, overwrite: false);
            published = true;
            return manifest;
        }
        finally
        {
            CryptographicOperations.ZeroMemory(manifestHash);
            CryptographicOperations.ZeroMemory(canonicalBytes);
            if (tempOwned && !published)
                DeleteOwnedTempFile(tempPath);
        }
    }

    /// <summary>
    /// Opens a bounded regular file without following the final path
    /// component, verifies its optional independent digest pin, then verifies
    /// its canonical manifest and exact source binding.
    /// </summary>
    public static async ValueTask<JsonTypedIntentManifest> OpenAsync(
        string path,
        JsonSourceBinding binding,
        JsonTypedIntentOpenOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        ArgumentNullException.ThrowIfNull(binding);
        JsonTypedIntentOpenOptions settings =
            options ?? new JsonTypedIntentOpenOptions();
        ValidateExpectedManifestDigest(
            settings.ExpectedManifestDigest,
            nameof(options));

        string fullPath = Path.GetFullPath(path);
        RejectAlternateDataStream(fullPath);
        cancellationToken.ThrowIfCancellationRequested();

        byte[] canonicalBytes;
        try
        {
            await using FileStream source =
                OpenSidecarFile(fullPath);
            long length = source.Length;
            if (length > JsonTypedIntentManifestSerializer
                    .MaximumManifestBytes)
            {
                throw SidecarError(
                    JsonTypedIntentRules.SizeLimitExceeded,
                    $"The typed JSON intent sidecar exceeds the {JsonTypedIntentManifestSerializer.MaximumManifestBytes}-byte safety limit.");
            }
            if (length <= 0)
            {
                throw SidecarError(
                    JsonTypedIntentRules.InvalidFormat,
                    "The typed JSON intent sidecar is empty.");
            }

            canonicalBytes = new byte[checked((int)length)];
            try
            {
                await source
                    .ReadExactlyAsync(
                        canonicalBytes,
                        cancellationToken)
                    .ConfigureAwait(false);
                byte[] trailingProbe = new byte[1];
                int trailingLength = await source
                    .ReadAsync(
                        trailingProbe,
                        cancellationToken)
                    .ConfigureAwait(false);
                if (trailingLength != 0)
                {
                    throw SidecarError(
                        JsonTypedIntentRules.InvalidFormat,
                        "The typed JSON intent sidecar changed while it was being read.");
                }
            }
            catch (EndOfStreamException exception)
            {
                throw SidecarError(
                    JsonTypedIntentRules.InvalidFormat,
                    "The typed JSON intent sidecar changed while it was being read.",
                    exception);
            }
        }
        catch (JsonSnapshotPackageException exception) when (
            exception.RuleId ==
                JsonSnapshotPackageRules.UnsafePath)
        {
            throw SidecarError(
                JsonTypedIntentRules.UnsafePath,
                "The typed JSON intent path must identify a regular file and cannot be a link or special file.",
                exception);
        }
        catch (PlatformNotSupportedException exception)
        {
            throw SidecarError(
                JsonTypedIntentRules.UnsafePath,
                "Secure typed JSON intent file opening is not supported on this platform.",
                exception);
        }

        try
        {
            return Parse(
                canonicalBytes,
                binding,
                settings.ExpectedManifestDigest);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(canonicalBytes);
        }
    }

    /// <summary>
    /// Verifies exact canonical sidecar bytes against an immutable source
    /// binding without performing file I/O.
    /// </summary>
    public static JsonTypedIntentManifest Parse(
        ReadOnlyMemory<byte> canonicalUtf8,
        JsonSourceBinding binding,
        string? expectedManifestDigest = null)
    {
        ArgumentNullException.ThrowIfNull(binding);
        ValidateExpectedManifestDigest(
            expectedManifestDigest,
            nameof(expectedManifestDigest));

        if (canonicalUtf8.Length >
            JsonTypedIntentManifestSerializer.MaximumManifestBytes)
        {
            throw SidecarError(
                JsonTypedIntentRules.SizeLimitExceeded,
                $"The typed JSON intent sidecar exceeds the {JsonTypedIntentManifestSerializer.MaximumManifestBytes}-byte safety limit.");
        }
        if (canonicalUtf8.IsEmpty)
        {
            throw SidecarError(
                JsonTypedIntentRules.InvalidFormat,
                "The typed JSON intent sidecar is empty.");
        }

        byte[] workingBytes = canonicalUtf8.ToArray();
        byte[] manifestHash =
            SHA256.HashData(workingBytes);
        try
        {
            ValidateExpectedManifestDigest(
                expectedManifestDigest,
                manifestHash);

            JsonTypedIntentManifestPayload payload;
            try
            {
                payload =
                    JsonTypedIntentManifestSerializer.Deserialize(
                        workingBytes);
            }
            catch (
                JsonTypedIntentManifestValidationException
                    exception)
            {
                throw exception.FailureKind switch
                {
                    JsonTypedIntentManifestFailureKind
                        .Integrity =>
                        SidecarError(
                            JsonTypedIntentRules
                                .IntegrityMismatch,
                            "The typed JSON intent sidecar payload digest does not match its payload."),
                    JsonTypedIntentManifestFailureKind
                        .Policy =>
                        SidecarError(
                            JsonTypedIntentRules
                                .PolicyMismatch,
                            "The typed JSON intent sidecar contains an unsupported contract or invalid declaration."),
                    JsonTypedIntentManifestFailureKind
                        .Limit =>
                        SidecarError(
                            JsonTypedIntentRules
                                .SizeLimitExceeded,
                            "The typed JSON intent sidecar exceeds a supported resource limit."),
                    _ => SidecarError(
                        JsonTypedIntentRules.InvalidFormat,
                        "The typed JSON intent sidecar is invalid."),
                };
            }
            catch (InvalidDataException)
            {
                throw SidecarError(
                    JsonTypedIntentRules.InvalidFormat,
                    "The typed JSON intent sidecar is not a valid canonical manifest.");
            }

            ValidateSourceBinding(payload, binding);
            return new JsonTypedIntentManifest(
                FormatDigest(manifestHash),
                ClonePayload(payload),
                workingBytes);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(manifestHash);
            CryptographicOperations.ZeroMemory(workingBytes);
        }
    }

    internal static JsonTypedColumnIntent CloneIntent(
        JsonTypedColumnIntent intent)
    {
        ArgumentNullException.ThrowIfNull(intent);
        return new JsonTypedColumnIntent
        {
            ColumnIndex = intent.ColumnIndex,
            ExpectedPropertyName = intent.ExpectedPropertyName,
            Codec = intent.Codec,
            Nullable = intent.Nullable,
            MissingPolicy = intent.MissingPolicy,
            Precision = intent.Precision,
            Scale = intent.Scale,
        };
    }

    private static JsonTypedColumnIntent[]
        ValidateAndCloneOptions(
            JsonSourceBinding binding,
            JsonTypedIntentOptions options)
    {
        if (options.Columns is null)
        {
            throw new ArgumentException(
                "Typed JSON intent columns are required.",
                nameof(options));
        }
        if (options.Columns.Count is
            < 1 or >
            JsonTypedIntentManifestSerializer.MaximumColumns)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "The typed JSON intent column count must be between 1 and 16,384.");
        }
        if (options.MaxDecodedBinaryBytes is
            < 1 or >
            JsonTypedIntentManifestSerializer
                .MaximumDecodedBinaryBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                $"The decoded binary limit must be between 1 and {JsonTypedIntentManifestSerializer.MaximumDecodedBinaryBytes} bytes.");
        }
        if (options.MaxDecimalDigits is
            < 1 or >
            JsonTypedIntentManifestSerializer
                .MaximumDecimalDigits)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                $"The decimal digit limit must be between 1 and {JsonTypedIntentManifestSerializer.MaximumDecimalDigits}.");
        }

        var propertyNames =
            new HashSet<string>(StringComparer.Ordinal);
        var result =
            new JsonTypedColumnIntent[options.Columns.Count];
        int previousColumnIndex = -1;
        JsonStreamingReaderOptions readerOptions =
            binding.ReaderOptions;

        for (int index = 0;
             index < options.Columns.Count;
             index++)
        {
            JsonTypedColumnIntent? intent =
                options.Columns[index];
            if (intent is null)
            {
                throw new ArgumentException(
                    $"Typed JSON intent column {index} is null.",
                    nameof(options));
            }
            if (intent.ColumnIndex <= previousColumnIndex ||
                intent.ColumnIndex >=
                    JsonTypedIntentManifestSerializer
                        .MaximumColumns)
            {
                throw new ArgumentException(
                    "Typed JSON intent column indexes must be unique, non-negative, strictly ascending, and below 16,384.",
                    nameof(options));
            }
            previousColumnIndex = intent.ColumnIndex;

            if (intent.ExpectedPropertyName is null)
            {
                throw new ArgumentException(
                    $"Typed JSON intent column {intent.ColumnIndex} must declare a decoded property name.",
                    nameof(options));
            }
            int propertyNameBytes;
            try
            {
                propertyNameBytes = s_strictUtf8.GetByteCount(
                    intent.ExpectedPropertyName);
            }
            catch (EncoderFallbackException exception)
            {
                throw new ArgumentException(
                    $"Typed JSON intent column {intent.ColumnIndex} contains invalid Unicode in its property name.",
                    nameof(options),
                    exception);
            }
            if (propertyNameBytes >
                readerOptions.MaxPropertyNameBytes)
            {
                throw new ArgumentException(
                    $"Typed JSON intent column {intent.ColumnIndex} exceeds the bound reader's decoded property-name limit.",
                    nameof(options));
            }
            if (!propertyNames.Add(
                    intent.ExpectedPropertyName))
            {
                throw new ArgumentException(
                    "Typed JSON intent decoded property names must be unique using ordinal comparison.",
                    nameof(options));
            }

            if (!Enum.IsDefined(intent.Codec) ||
                !Enum.IsDefined(intent.MissingPolicy))
            {
                throw new ArgumentException(
                    $"Typed JSON intent column {intent.ColumnIndex} contains an unsupported policy.",
                    nameof(options));
            }
            if (intent.MissingPolicy ==
                    JsonMissingPropertyPolicy.AsNull &&
                intent.Nullable == false)
            {
                throw new ArgumentException(
                    $"Typed JSON intent column {intent.ColumnIndex} cannot combine missing-as-null with non-nullability.",
                    nameof(options));
            }

            bool decimalCodec = intent.Codec is
                JsonTypedValueCodec.DecimalString or
                JsonTypedValueCodec.DecimalNumber;
            if (decimalCodec)
            {
                if (intent.Precision is not int precision ||
                    intent.Scale is not int scale ||
                    precision < 1 ||
                    precision > options.MaxDecimalDigits ||
                    scale < 0 ||
                    scale > precision)
                {
                    throw new ArgumentException(
                        $"Typed JSON intent decimal column {intent.ColumnIndex} must declare precision and scale within the retained digit limit.",
                        nameof(options));
                }
            }
            else if (intent.Precision is not null ||
                     intent.Scale is not null)
            {
                throw new ArgumentException(
                    $"Typed JSON intent non-decimal column {intent.ColumnIndex} cannot declare decimal facets.",
                    nameof(options));
            }

            result[index] = CloneIntent(intent);
        }

        return result;
    }

    private static JsonTypedIntentManifestPayload CreatePayload(
        JsonSourceBinding binding,
        JsonTypedIntentOptions options,
        IReadOnlyList<JsonTypedColumnIntent> columns) =>
        new()
        {
            Contracts =
                new JsonTypedIntentContractsManifest
                {
                    SourceBinding =
                        JsonSourceBinding
                            .SourceFingerprintAlgorithm,
                    ReaderOptions =
                        JsonSourceBinding.OptionsAlgorithm,
                    PropertyNameComparison =
                        JsonInputContracts
                            .DecodedPropertyNameComparison,
                    TypedValue =
                        JsonTypedIntentManifestSerializer
                            .TypedValueContract,
                    TextCodec =
                        JsonTypedIntentManifestSerializer
                            .TextCodecContract,
                },
            Source = new JsonTypedIntentSourceManifest
            {
                SnapshotIdentity = binding.SnapshotIdentity,
                ContentDigest = binding.ContentDigest,
                ContentLength = binding.ContentLength,
                Identity = binding.Source.Identity,
                Fingerprint = binding.Source.Fingerprint,
                OptionsDigest = binding.OptionsDigest,
            },
            Limits = new JsonTypedIntentLimitsManifest
            {
                MaxDecodedBinaryBytes =
                    options.MaxDecodedBinaryBytes,
                MaxDecimalDigits = options.MaxDecimalDigits,
            },
            Columns = columns
                .Select(CloneIntent)
                .ToArray(),
        };

    private static void ValidateSourceBinding(
        JsonTypedIntentManifestPayload payload,
        JsonSourceBinding binding)
    {
        if (!FixedTimeDigestEquals(
                payload.Source.ContentDigest,
                binding.ContentDigest) ||
            payload.Source.ContentLength !=
                binding.ContentLength ||
            !FixedTimeSafeIdentityEquals(
                payload.Source.Identity,
                binding.Source.Identity) ||
            !FixedTimeDigestEquals(
                payload.Source.Fingerprint,
                binding.Source.Fingerprint) ||
            !FixedTimeDigestEquals(
                payload.Source.OptionsDigest,
                binding.OptionsDigest))
        {
            throw SidecarError(
                JsonTypedIntentRules.SourceMismatch,
                "The typed JSON intent sidecar does not match the exact JSON source identity, bytes, and reader policy.");
        }

        JsonStreamingReaderOptions readerOptions =
            binding.ReaderOptions;
        foreach (JsonTypedColumnIntent column in
                 payload.Columns)
        {
            int propertyNameBytes;
            try
            {
                propertyNameBytes = s_strictUtf8.GetByteCount(
                    column.ExpectedPropertyName);
            }
            catch (EncoderFallbackException exception)
            {
                throw SidecarError(
                    JsonTypedIntentRules.PolicyMismatch,
                    "The typed JSON intent sidecar contains invalid Unicode.",
                    exception);
            }
            if (propertyNameBytes >
                readerOptions.MaxPropertyNameBytes)
            {
                throw SidecarError(
                    JsonTypedIntentRules.PolicyMismatch,
                    "A typed JSON intent property name exceeds the bound reader's decoded property-name limit.");
            }
        }
    }

    private static JsonTypedIntentManifestPayload ClonePayload(
        JsonTypedIntentManifestPayload payload) =>
        new()
        {
            Contracts =
                new JsonTypedIntentContractsManifest
                {
                    SourceBinding =
                        payload.Contracts.SourceBinding,
                    ReaderOptions =
                        payload.Contracts.ReaderOptions,
                    PropertyNameComparison =
                        payload.Contracts
                            .PropertyNameComparison,
                    TypedValue =
                        payload.Contracts.TypedValue,
                    TextCodec =
                        payload.Contracts.TextCodec,
                },
            Source = new JsonTypedIntentSourceManifest
            {
                SnapshotIdentity =
                    payload.Source.SnapshotIdentity,
                ContentDigest =
                    payload.Source.ContentDigest,
                ContentLength =
                    payload.Source.ContentLength,
                Identity = payload.Source.Identity,
                Fingerprint = payload.Source.Fingerprint,
                OptionsDigest =
                    payload.Source.OptionsDigest,
            },
            Limits = new JsonTypedIntentLimitsManifest
            {
                MaxDecodedBinaryBytes =
                    payload.Limits.MaxDecodedBinaryBytes,
                MaxDecimalDigits =
                    payload.Limits.MaxDecimalDigits,
            },
            Columns = payload.Columns
                .Select(CloneIntent)
                .ToArray(),
        };

    private static void ValidateExpectedManifestDigest(
        string? expectedManifestDigest,
        string parameterName)
    {
        if (expectedManifestDigest is not null &&
            !IsCanonicalPrefixedDigest(
                expectedManifestDigest))
        {
            throw new ArgumentException(
                "The expected manifest digest must be canonical lowercase SHA-256 text.",
                parameterName);
        }
    }

    private static void ValidateExpectedManifestDigest(
        string? expectedManifestDigest,
        ReadOnlySpan<byte> actualManifestHash)
    {
        if (expectedManifestDigest is null)
            return;

        byte[] expectedHash = Convert.FromHexString(
            expectedManifestDigest.AsSpan(7));
        try
        {
            if (!CryptographicOperations.FixedTimeEquals(
                    expectedHash,
                    actualManifestHash))
            {
                throw SidecarError(
                    JsonTypedIntentRules.IntegrityMismatch,
                    "The typed JSON intent sidecar does not match the independently retained manifest digest.");
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(expectedHash);
        }
    }

    private static string ValidateDestination(
        string fullPath)
    {
        RejectAlternateDataStream(fullPath);
        string parentPath =
            Path.GetDirectoryName(fullPath) ??
            throw new ArgumentException(
                "The typed JSON intent sidecar path must have a parent directory.");
        if (!Directory.Exists(parentPath))
        {
            throw new DirectoryNotFoundException(
                "The typed JSON intent sidecar parent directory does not exist.");
        }
        FileAttributes parentAttributes =
            File.GetAttributes(parentPath);
        if ((parentAttributes &
             FileAttributes.ReparsePoint) != 0)
        {
            throw SidecarError(
                JsonTypedIntentRules.UnsafePath,
                "The typed JSON intent sidecar parent directory cannot be a reparse point.");
        }
        if (PathExists(fullPath))
        {
            throw new IOException(
                "The typed JSON intent sidecar destination already exists.");
        }

        return parentPath;
    }

    private static void RejectAlternateDataStream(
        string fullPath)
    {
        if (!OperatingSystem.IsWindows())
            return;

        string root =
            Path.GetPathRoot(fullPath) ?? string.Empty;
        if (fullPath.AsSpan(root.Length).Contains(':'))
        {
            throw SidecarError(
                JsonTypedIntentRules.UnsafePath,
                "Windows alternate data streams cannot be used as typed JSON intent sidecars.");
        }
    }

    private static bool PathExists(string path)
    {
        try
        {
            _ = File.GetAttributes(path);
            return true;
        }
        catch (FileNotFoundException)
        {
            return false;
        }
        catch (DirectoryNotFoundException)
        {
            return false;
        }
    }

    private static FileStream CreateSidecarFile(
        string path)
    {
        var options = new FileStreamOptions
        {
            Mode = FileMode.CreateNew,
            Access = FileAccess.Write,
            Share = FileShare.None,
            BufferSize = FileBufferBytes,
            Options =
                FileOptions.Asynchronous |
                FileOptions.SequentialScan |
                FileOptions.WriteThrough,
        };
        if (!OperatingSystem.IsWindows())
        {
            options.UnixCreateMode =
                UnixFileMode.UserRead |
                UnixFileMode.UserWrite;
        }

        return new FileStream(path, options);
    }

    private static FileStream OpenSidecarFile(
        string path) =>
        JsonSnapshotPackageFile.OpenReadNoFollow(
            path,
            FileBufferBytes);

    private static void DeleteOwnedTempFile(
        string tempPath)
    {
        try
        {
            File.Delete(tempPath);
        }
        catch (FileNotFoundException)
        {
        }
        catch (DirectoryNotFoundException)
        {
        }
    }

    private static string FormatDigest(
        ReadOnlySpan<byte> digest) =>
        "sha256:" +
        Convert.ToHexString(digest).ToLowerInvariant();

    private static bool IsCanonicalPrefixedDigest(
        string? digest) =>
        digest is not null &&
        digest.Length == 71 &&
        digest.StartsWith(
            "sha256:",
            StringComparison.Ordinal) &&
        IsLowerHex(digest.AsSpan(7));

    private static bool FixedTimeDigestEquals(
        string left,
        string right)
    {
        if (!IsCanonicalPrefixedDigest(left) ||
            !IsCanonicalPrefixedDigest(right))
        {
            return false;
        }

        byte[] leftHash =
            Convert.FromHexString(left.AsSpan(7));
        byte[] rightHash =
            Convert.FromHexString(right.AsSpan(7));
        try
        {
            return CryptographicOperations.FixedTimeEquals(
                leftHash,
                rightHash);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(leftHash);
            CryptographicOperations.ZeroMemory(rightHash);
        }
    }

    private static bool FixedTimeSafeIdentityEquals(
        string left,
        string right)
    {
        const string contentPrefix = "json-content:";
        const string logicalPrefix = "json-logical:";

        bool leftIsContent = left.StartsWith(
            contentPrefix,
            StringComparison.Ordinal);
        bool rightIsContent = right.StartsWith(
            contentPrefix,
            StringComparison.Ordinal);
        string? leftDigest = leftIsContent
            ? left[contentPrefix.Length..]
            : left.StartsWith(
                logicalPrefix,
                StringComparison.Ordinal)
                ? left[logicalPrefix.Length..]
                : null;
        string? rightDigest = rightIsContent
            ? right[contentPrefix.Length..]
            : right.StartsWith(
                logicalPrefix,
                StringComparison.Ordinal)
                ? right[logicalPrefix.Length..]
                : null;

        return leftIsContent == rightIsContent &&
            leftDigest is not null &&
            rightDigest is not null &&
            FixedTimeDigestEquals(leftDigest, rightDigest);
    }

    private static bool IsLowerHex(
        ReadOnlySpan<char> value)
    {
        foreach (char character in value)
        {
            if (character is not (>= '0' and <= '9') and
                not (>= 'a' and <= 'f'))
            {
                return false;
            }
        }

        return true;
    }

    private static JsonTypedIntentException SidecarError(
        string ruleId,
        string message) =>
        new(ruleId, message);

    private static JsonTypedIntentException SidecarError(
        string ruleId,
        string message,
        Exception innerException) =>
        new(ruleId, message, innerException);
}
