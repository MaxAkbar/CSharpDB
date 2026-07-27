using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Files.Json;

/// <summary>
/// Publishes and reopens one immutable JSON snapshot, reader policy, inference
/// recipe, and catalog binding as an atomic single-file package.
/// </summary>
public static class JsonSnapshotPackage
{
    public const string Format =
        JsonSnapshotPackageManifestSerializer.Format;
    public const string FileExtension = ".csdbjson";

    private const int HeaderSize = 64;
    private const uint HeaderVersion = 1;
    private const uint HeaderFlags = 0;
    private const int ManifestLengthOffset = 16;
    private const int FlagsOffset = 20;
    private const int SnapshotLengthOffset = 24;
    private const int ManifestHashOffset = 32;
    private const int DigestBytes = 32;

    private static ReadOnlySpan<byte> HeaderMagic => "CSDBJSN1"u8;

    /// <summary>
    /// Atomically publishes a retained package. The destination must not
    /// already exist, and ownership of the snapshot and schema stays with the
    /// caller. Unix files are created with user-only mode; on Windows the
    /// caller must choose a parent directory with a trusted ACL.
    /// </summary>
    public static async ValueTask<JsonSnapshotPackageManifest> WriteAsync(
        string path,
        JsonSourceSnapshot snapshot,
        JsonTableSchemaInferenceResult schema,
        string targetCSharpDbVersion,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(schema);
        if (string.IsNullOrWhiteSpace(targetCSharpDbVersion))
        {
            throw new ArgumentException(
                "The target CSharpDB version must be nonblank.",
                nameof(targetCSharpDbVersion));
        }

        string fullPath = Path.GetFullPath(path);
        string parentPath = ValidateDestination(fullPath);
        cancellationToken.ThrowIfCancellationRequested();

        MigrationCatalog catalog =
            schema.CreateCatalog(targetCSharpDbVersion);
        _ = JsonMigrationDataSource.ValidateCatalogBinding(
            schema,
            snapshot,
            catalog);

        JsonSnapshotPackageManifestPayload payload =
            CreatePayload(schema, catalog);
        byte[] manifestBytes =
            JsonSnapshotPackageManifestSerializer.Serialize(payload);
        byte[] manifestHash = SHA256.HashData(manifestBytes);
        byte[] header = CreateHeader(
            manifestBytes.Length,
            snapshot.ContentLength,
            manifestHash);
        var manifest =
            CreatePublicManifest(payload, manifestHash, schema.Source);
        string tempPath = Path.Combine(
            parentPath,
            $".csdbjson-{Guid.NewGuid():N}.tmp");
        bool tempOwned = false;
        bool published = false;

        try
        {
            FileStream destination = CreatePackageFile(tempPath);
            tempOwned = true;
            await using (destination)
            {
                await destination
                    .WriteAsync(header, cancellationToken)
                    .ConfigureAwait(false);
                await destination
                    .WriteAsync(manifestBytes, cancellationToken)
                    .ConfigureAwait(false);
                await CopySnapshotAsync(
                        snapshot,
                        destination,
                        expectedLength: snapshot.ContentLength,
                        expectedDigest: snapshot.ContentDigest,
                        cancellationToken)
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
            CryptographicOperations.ZeroMemory(manifestBytes);
            CryptographicOperations.ZeroMemory(header);
            if (tempOwned && !published)
                DeleteOwnedTempFile(tempPath);
        }
    }

    /// <summary>
    /// Verifies a retained package, copies its raw section into a new private
    /// snapshot, and reconstructs the exact reader, inference, and catalog
    /// policy from that copy.
    /// </summary>
    public static async ValueTask<JsonSnapshotPackageSession> OpenAsync(
        string path,
        JsonSnapshotPackageOpenOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        JsonSnapshotPackageOpenOptions settings = ValidateOpenOptions(
            options ?? new JsonSnapshotPackageOpenOptions());
        string fullPath = Path.GetFullPath(path);
        ValidateInputPath(fullPath);
        cancellationToken.ThrowIfCancellationRequested();

        JsonSourceSnapshot? snapshot = null;
        JsonMigrationDataSource? dataSource = null;
        try
        {
            JsonSnapshotPackageManifestPayload payload;
            byte[] manifestHash;
            await using (FileStream package =
                         OpenPackageFile(
                             fullPath,
                             settings.CopyBufferBytes))
            {
                ValidateOpenedFile(package);
                PackageHeader header = await ReadHeaderAsync(
                        package,
                        settings,
                        cancellationToken)
                    .ConfigureAwait(false);
                ValidateExpectedManifestDigest(
                    settings.ExpectedManifestDigest,
                    header.ManifestHash);

                byte[] manifestBytes =
                    new byte[header.ManifestLength];
                try
                {
                    await package
                        .ReadExactlyAsync(
                            manifestBytes,
                            cancellationToken)
                        .ConfigureAwait(false);
                    byte[] actualManifestHash =
                        SHA256.HashData(manifestBytes);
                    try
                    {
                        if (!CryptographicOperations.FixedTimeEquals(
                                header.ManifestHash,
                                actualManifestHash))
                        {
                            throw PackageError(
                                JsonSnapshotPackageRules
                                    .IntegrityMismatch,
                                "The JSON package manifest hash does not match its bytes.");
                        }
                    }
                    finally
                    {
                        CryptographicOperations.ZeroMemory(
                            actualManifestHash);
                    }

                    try
                    {
                        payload =
                            JsonSnapshotPackageManifestSerializer
                                .Deserialize(manifestBytes);
                    }
                    catch (InvalidDataException)
                    {
                        throw PackageError(
                            JsonSnapshotPackageRules.InvalidFormat,
                            "The JSON package manifest is invalid.");
                    }
                }
                finally
                {
                    CryptographicOperations.ZeroMemory(
                        manifestBytes);
                }

                ValidatePayload(payload, header.SnapshotLength);
                manifestHash = header.ManifestHash;

                await using var rawSection =
                    new ExactLengthReadStream(
                        package,
                        header.SnapshotLength);
                snapshot = await JsonSourceSnapshot.CreateAsync(
                        rawSection,
                        new JsonSourceSnapshotOptions
                        {
                            WorkspacePath = settings.WorkspacePath,
                            MaxSourceBytes = settings.MaxSourceBytes,
                            CopyBufferBytes = settings.CopyBufferBytes,
                            LeaveOpen = true,
                        },
                        cancellationToken)
                    .ConfigureAwait(false);
            }

            ValidateSnapshot(payload.Snapshot, snapshot);
            JsonStreamingReaderOptions readerOptions =
                RestoreReaderOptions(payload.Reader);
            MigrationSourceIdentity retainedSource =
                RestoreSource(payload.Source);

            JsonSourceBinding binding;
            try
            {
                binding =
                    await JsonSourceBinding
                        .RestoreFromVerifiedSnapshotAsync(
                            snapshot,
                            retainedSource,
                            payload.Source.OptionsDigest,
                            readerOptions,
                            cancellationToken)
                        .ConfigureAwait(false);
            }
            catch (InvalidDataException exception)
            {
                throw PackageError(
                    JsonSnapshotPackageRules.PolicyMismatch,
                    "The JSON package reader binding does not match its retained snapshot.",
                    exception);
            }

            JsonTableSchemaInferenceRecipe recipe =
                RestoreRecipe(payload.Inference);
            JsonTableSchemaInferenceResult schema;
            try
            {
                schema = await JsonTableSchemaInferer.ReplayAsync(
                        binding,
                        snapshot,
                        recipe,
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception exception) when (
                exception is InvalidDataException or
                ArgumentException or
                JsonReadException or
                JsonTableSchemaInferenceException)
            {
                throw PackageError(
                    JsonSnapshotPackageRules.PolicyMismatch,
                    "The JSON package inference recipe is invalid.",
                    exception);
            }

            MigrationCatalog catalog = schema.CreateCatalog(
                payload.Catalog.TargetCSharpDbVersion);
            string catalogDigest =
                MigrationArtifactSerializer.ComputeCatalogDigest(
                    catalog);
            if (!string.Equals(
                    catalogDigest,
                    payload.Catalog.Digest,
                    StringComparison.Ordinal))
            {
                throw PackageError(
                    JsonSnapshotPackageRules.PolicyMismatch,
                    "The JSON package catalog digest does not match replayed schema inference.");
            }

            dataSource =
                JsonMigrationDataSource.CreateFromVerifiedSnapshot(
                    schema,
                    snapshot,
                    catalog);
            JsonSnapshotPackageManifest manifest =
                CreatePublicManifest(
                    payload,
                    manifestHash,
                    binding.Source);
            var session = new JsonSnapshotPackageSession(
                manifest,
                snapshot,
                schema,
                catalog,
                dataSource);
            snapshot = null;
            dataSource = null;
            return session;
        }
        catch (Exception operationFailure)
        {
            List<Exception>? cleanupFailures = null;
            if (dataSource is not null)
            {
                try
                {
                    await dataSource
                        .DisposeAsync()
                        .ConfigureAwait(false);
                }
                catch (Exception cleanupFailure)
                {
                    (cleanupFailures ??= []).Add(cleanupFailure);
                }
            }

            if (snapshot is not null)
            {
                try
                {
                    await snapshot
                        .DisposeAsync()
                        .ConfigureAwait(false);
                }
                catch (Exception cleanupFailure)
                {
                    (cleanupFailures ??= []).Add(cleanupFailure);
                }
            }

            if (cleanupFailures is null)
            {
                ExceptionDispatchInfo.Capture(operationFailure).Throw();
                throw;
            }

            cleanupFailures.Insert(0, operationFailure);
            throw new AggregateException(
                "JSON package open failed and private snapshot cleanup also failed.",
                cleanupFailures);
        }
    }

    private static JsonSnapshotPackageManifestPayload CreatePayload(
        JsonTableSchemaInferenceResult schema,
        MigrationCatalog catalog)
    {
        JsonSourceBinding binding = schema.Binding;
        JsonStreamingReaderOptions reader = binding.ReaderOptions;
        JsonTableSchemaInferenceRecipe recipe = schema.Recipe;
        return new JsonSnapshotPackageManifestPayload
        {
            Contracts =
                new JsonSnapshotPackageContractIdsManifest
                {
                    Snapshot = JsonSourceSnapshot.IdentityAlgorithm,
                    Binding =
                        JsonSourceBinding.SourceFingerprintAlgorithm,
                    Options = JsonSourceBinding.OptionsAlgorithm,
                    Schema =
                        JsonTableSchemaInferenceResult.AlgorithmId,
                    Scalar =
                        JsonTableSchemaInferenceResult.ScalarPolicyId,
                    CanonicalValue =
                        JsonInputContracts.CanonicalNestedJsonVersion,
                    CatalogFormat =
                        MigrationArtifactFormats.CatalogV1,
                },
            Snapshot = new JsonSnapshotPackageSnapshotManifest
            {
                ContentLength = binding.ContentLength,
                ContentDigest = binding.ContentDigest,
                SnapshotIdentity = binding.SnapshotIdentity,
            },
            Source = new JsonSnapshotPackageSourceManifest
            {
                Identity = binding.Source.Identity,
                Fingerprint = binding.Source.Fingerprint,
                OptionsDigest = binding.OptionsDigest,
            },
            Reader = new JsonSnapshotPackageReaderManifest
            {
                Framing = reader.Framing,
                MaxValueBytes = reader.MaxValueBytes,
                MaxDepth = reader.MaxDepth,
                MaxPropertiesPerObject =
                    reader.MaxPropertiesPerObject,
                MaxArrayElements = reader.MaxArrayElements,
                MaxTotalNodes = reader.MaxTotalNodes,
                MaxPropertyNameBytes =
                    reader.MaxPropertyNameBytes,
                MaxStringBytes = reader.MaxStringBytes,
                MaxNumberBytes = reader.MaxNumberBytes,
            },
            Inference = new JsonSnapshotPackageInferenceManifest
            {
                CollectProfile = recipe.CollectProfile,
                MaxProfileRecords = recipe.MaxProfileRecords,
                TableName = recipe.TableName,
                MaxColumns = recipe.MaxColumns,
                MaxTotalColumnNameBytes =
                    recipe.MaxTotalColumnNameBytes,
                MaxProfileBytes = recipe.MaxProfileBytes,
                ColumnOverrides = recipe.ColumnOverrides
                    .Select(
                        item =>
                            new JsonSnapshotPackageColumnOverrideManifest
                            {
                                ColumnIndex = item.ColumnIndex,
                                ExpectedPropertyName =
                                    item.ExpectedPropertyName,
                                LogicalType = item.LogicalType,
                                Nullable = item.Nullable,
                                MissingPolicy = item.MissingPolicy,
                            })
                    .ToArray(),
            },
            Catalog = new JsonSnapshotPackageCatalogManifest
            {
                TargetCSharpDbVersion =
                    catalog.TargetCSharpDbVersion,
                Digest =
                    MigrationArtifactSerializer
                        .ComputeCatalogDigest(catalog),
            },
        };
    }

    private static void ValidatePayload(
        JsonSnapshotPackageManifestPayload payload,
        long headerSnapshotLength)
    {
        JsonSnapshotPackageContractIdsManifest contracts =
            payload.Contracts;
        if (!string.Equals(
                contracts.Snapshot,
                JsonSourceSnapshot.IdentityAlgorithm,
                StringComparison.Ordinal) ||
            !string.Equals(
                contracts.Binding,
                JsonSourceBinding.SourceFingerprintAlgorithm,
                StringComparison.Ordinal) ||
            !string.Equals(
                contracts.Options,
                JsonSourceBinding.OptionsAlgorithm,
                StringComparison.Ordinal) ||
            !string.Equals(
                contracts.Schema,
                JsonTableSchemaInferenceResult.AlgorithmId,
                StringComparison.Ordinal) ||
            !string.Equals(
                contracts.Scalar,
                JsonTableSchemaInferenceResult.ScalarPolicyId,
                StringComparison.Ordinal) ||
            !string.Equals(
                contracts.CanonicalValue,
                JsonInputContracts.CanonicalNestedJsonVersion,
                StringComparison.Ordinal) ||
            !string.Equals(
                contracts.CatalogFormat,
                MigrationArtifactFormats.CatalogV1,
                StringComparison.Ordinal))
        {
            throw PackageError(
                JsonSnapshotPackageRules.PolicyMismatch,
                "The JSON package uses an unsupported policy contract version.");
        }

        JsonSnapshotPackageSnapshotManifest snapshot =
            payload.Snapshot;
        if (snapshot.ContentLength != headerSnapshotLength ||
            snapshot.ContentLength < 0 ||
            !IsCanonicalPrefixedDigest(snapshot.ContentDigest) ||
            !string.Equals(
                snapshot.SnapshotIdentity,
                $"{JsonSourceSnapshot.IdentityAlgorithm}:{snapshot.ContentDigest}:bytes:{snapshot.ContentLength}",
                StringComparison.Ordinal))
        {
            throw PackageError(
                JsonSnapshotPackageRules.IntegrityMismatch,
                "The JSON package snapshot identity is invalid or inconsistent with its header.");
        }

        if (!IsCanonicalPrefixedDigest(
                payload.Source.Fingerprint) ||
            !IsCanonicalPrefixedDigest(
                payload.Source.OptionsDigest))
        {
            throw PackageError(
                JsonSnapshotPackageRules.PolicyMismatch,
                "The JSON package source digests are not canonical SHA-256 text.");
        }

        ValidateRetainedResourcePolicy(
            payload.Reader,
            payload.Inference);

        if (string.IsNullOrWhiteSpace(
                payload.Catalog.TargetCSharpDbVersion) ||
            !IsCanonicalHexDigest(payload.Catalog.Digest))
        {
            throw PackageError(
                JsonSnapshotPackageRules.PolicyMismatch,
                "The JSON package catalog binding is invalid.");
        }
    }

    private static void ValidateRetainedResourcePolicy(
        JsonSnapshotPackageReaderManifest reader,
        JsonSnapshotPackageInferenceManifest inference)
    {
        JsonStreamingReaderOptions options =
            RestoreReaderOptions(reader);
        try
        {
            _ = JsonStreamingReaderSettings.Create(options);
        }
        catch (ArgumentException exception)
        {
            throw PackageError(
                JsonSnapshotPackageRules.PolicyMismatch,
                "The JSON package reader resource policy exceeds supported safety ceilings.",
                exception);
        }

        if (inference.MaxProfileRecords <= 0 ||
            inference.MaxProfileRecords >
                JsonTableSchemaInferenceOptions
                    .MaximumSupportedProfileRecords ||
            string.IsNullOrWhiteSpace(inference.TableName) ||
            inference.TableName.Length > 1024 ||
            inference.MaxColumns <= 0 ||
            inference.MaxColumns >
                JsonTableSchemaInferenceOptions
                    .MaximumSupportedColumns ||
            inference.MaxTotalColumnNameBytes <= 0 ||
            inference.MaxTotalColumnNameBytes >
                JsonTableSchemaInferenceOptions
                    .MaximumSupportedTotalColumnNameBytes ||
            inference.MaxProfileBytes <= 0 ||
            inference.MaxProfileBytes >
                JsonTableSchemaInferenceOptions
                    .MaximumSupportedProfileBytes ||
            inference.ColumnOverrides.Count >
                inference.MaxColumns ||
            inference.ColumnOverrides.Any(
                item =>
                    item.ColumnIndex < 0 ||
                    item.ColumnIndex >= inference.MaxColumns ||
                    item.ExpectedPropertyName is null ||
                    StrictUtf8ByteCount(
                        item.ExpectedPropertyName) >
                        reader.MaxPropertyNameBytes ||
                    !Enum.IsDefined(item.LogicalType) ||
                    !Enum.IsDefined(item.MissingPolicy) ||
                    (item.MissingPolicy ==
                         JsonMissingPropertyPolicy.AsNull &&
                     item.Nullable == false)))
        {
            throw PackageError(
                JsonSnapshotPackageRules.PolicyMismatch,
                "The JSON package inference resource policy exceeds supported safety ceilings.");
        }
    }

    private static int StrictUtf8ByteCount(string value)
    {
        try
        {
            return new UTF8Encoding(false, true)
                .GetByteCount(value);
        }
        catch (EncoderFallbackException exception)
        {
            throw PackageError(
                JsonSnapshotPackageRules.PolicyMismatch,
                "The JSON package inference policy contains invalid Unicode.",
                exception);
        }
    }

    private static void ValidateSnapshot(
        JsonSnapshotPackageSnapshotManifest expected,
        JsonSourceSnapshot actual)
    {
        if (expected.ContentLength != actual.ContentLength ||
            !string.Equals(
                expected.ContentDigest,
                actual.ContentDigest,
                StringComparison.Ordinal) ||
            !string.Equals(
                expected.SnapshotIdentity,
                actual.SnapshotIdentity,
                StringComparison.Ordinal))
        {
            throw PackageError(
                JsonSnapshotPackageRules.IntegrityMismatch,
                "The retained JSON bytes do not match the package snapshot identity.");
        }
    }

    private static JsonStreamingReaderOptions RestoreReaderOptions(
        JsonSnapshotPackageReaderManifest manifest) =>
        new()
        {
            Framing = manifest.Framing,
            MaxValueBytes = manifest.MaxValueBytes,
            MaxDepth = manifest.MaxDepth,
            MaxPropertiesPerObject =
                manifest.MaxPropertiesPerObject,
            MaxArrayElements = manifest.MaxArrayElements,
            MaxTotalNodes = manifest.MaxTotalNodes,
            MaxPropertyNameBytes =
                manifest.MaxPropertyNameBytes,
            MaxStringBytes = manifest.MaxStringBytes,
            MaxNumberBytes = manifest.MaxNumberBytes,
            LeaveOpen = false,
        };

    private static MigrationSourceIdentity RestoreSource(
        JsonSnapshotPackageSourceManifest manifest) =>
        new()
        {
            Kind = MigrationSourceKind.Json,
            Identity = manifest.Identity,
            Fingerprint = manifest.Fingerprint,
            ProviderVersion =
                JsonSourceBinding.AdapterProviderVersion,
            SourceVersion = null,
            Consistency = new MigrationConsistencyStrategy
            {
                Kind = MigrationConsistencyKind.Snapshot,
                Description =
                    JsonSourceBinding
                        .SnapshotConsistencyDescription,
                Watermark = null,
            },
        };

    private static JsonTableSchemaInferenceRecipe RestoreRecipe(
        JsonSnapshotPackageInferenceManifest manifest)
    {
        var options = new JsonTableSchemaInferenceOptions
        {
            TableName = manifest.TableName,
            MaxColumns = manifest.MaxColumns,
            MaxTotalColumnNameBytes =
                manifest.MaxTotalColumnNameBytes,
            MaxProfileBytes = manifest.MaxProfileBytes,
            ColumnOverrides = manifest.ColumnOverrides
                .Select(
                    item => new JsonTableColumnSchemaOverride
                    {
                        ColumnIndex = item.ColumnIndex,
                        ExpectedPropertyName =
                            item.ExpectedPropertyName,
                        LogicalType = item.LogicalType,
                        Nullable = item.Nullable,
                        MissingPolicy = item.MissingPolicy,
                    })
                .ToArray(),
        };
        return new JsonTableSchemaInferenceRecipe(
            manifest.CollectProfile,
            manifest.MaxProfileRecords,
            options,
            options.ColumnOverrides);
    }

    private static async ValueTask CopySnapshotAsync(
        JsonSourceSnapshot snapshot,
        Stream destination,
        long expectedLength,
        string expectedDigest,
        CancellationToken cancellationToken)
    {
        byte[] buffer =
            ArrayPool<byte>.Shared.Rent(128 * 1024);
        try
        {
            await using Stream source = snapshot.OpenRead();
            using var hasher =
                IncrementalHash.CreateHash(
                    HashAlgorithmName.SHA256);
            long total = 0;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                long remaining = expectedLength - total;
                int requested = remaining == 0
                    ? 1
                    : (int)Math.Min(
                        buffer.Length,
                        remaining + 1);
                int read = await source
                    .ReadAsync(
                        buffer.AsMemory(0, requested),
                        cancellationToken)
                    .ConfigureAwait(false);
                if (read == 0)
                    break;
                if (read > remaining)
                {
                    throw PackageError(
                        JsonSnapshotPackageRules
                            .IntegrityMismatch,
                        "The JSON snapshot grew while its package was being written.");
                }

                await destination
                    .WriteAsync(
                        buffer.AsMemory(0, read),
                        cancellationToken)
                    .ConfigureAwait(false);
                hasher.AppendData(buffer, 0, read);
                total += read;
            }

            string actualDigest =
                "sha256:" +
                Convert.ToHexString(hasher.GetHashAndReset())
                    .ToLowerInvariant();
            if (total != expectedLength ||
                !string.Equals(
                    actualDigest,
                    expectedDigest,
                    StringComparison.Ordinal))
            {
                throw PackageError(
                    JsonSnapshotPackageRules.IntegrityMismatch,
                    "The JSON snapshot changed while its package was being written.");
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(
                buffer,
                clearArray: true);
        }
    }

    private static byte[] CreateHeader(
        int manifestLength,
        long snapshotLength,
        ReadOnlySpan<byte> manifestHash)
    {
        if (manifestLength <= 0 ||
            manifestLength >
                JsonSnapshotPackageManifestSerializer
                    .MaximumManifestBytes)
        {
            throw new InvalidDataException(
                "The JSON package manifest length is invalid.");
        }
        if (snapshotLength < 0)
        {
            throw new InvalidDataException(
                "The JSON package snapshot length is invalid.");
        }
        if (manifestHash.Length != DigestBytes)
        {
            throw new ArgumentException(
                "The manifest hash must be SHA-256.",
                nameof(manifestHash));
        }

        byte[] header = new byte[HeaderSize];
        HeaderMagic.CopyTo(header);
        BinaryPrimitives.WriteUInt32BigEndian(
            header.AsSpan(8, 4),
            HeaderVersion);
        BinaryPrimitives.WriteUInt32BigEndian(
            header.AsSpan(12, 4),
            HeaderSize);
        BinaryPrimitives.WriteUInt32BigEndian(
            header.AsSpan(ManifestLengthOffset, 4),
            checked((uint)manifestLength));
        BinaryPrimitives.WriteUInt32BigEndian(
            header.AsSpan(FlagsOffset, 4),
            HeaderFlags);
        BinaryPrimitives.WriteUInt64BigEndian(
            header.AsSpan(SnapshotLengthOffset, 8),
            checked((ulong)snapshotLength));
        manifestHash.CopyTo(
            header.AsSpan(ManifestHashOffset, DigestBytes));
        return header;
    }

    private static async ValueTask<PackageHeader> ReadHeaderAsync(
        FileStream package,
        JsonSnapshotPackageOpenOptions settings,
        CancellationToken cancellationToken)
    {
        if (package.Length < HeaderSize)
        {
            throw PackageError(
                JsonSnapshotPackageRules.InvalidFormat,
                "The JSON package header is truncated.");
        }

        byte[] header = new byte[HeaderSize];
        try
        {
            await package
                .ReadExactlyAsync(header, cancellationToken)
                .ConfigureAwait(false);
            if (!header
                    .AsSpan(0, HeaderMagic.Length)
                    .SequenceEqual(HeaderMagic) ||
                BinaryPrimitives.ReadUInt32BigEndian(
                    header.AsSpan(8, 4)) != HeaderVersion ||
                BinaryPrimitives.ReadUInt32BigEndian(
                    header.AsSpan(12, 4)) != HeaderSize ||
                BinaryPrimitives.ReadUInt32BigEndian(
                    header.AsSpan(FlagsOffset, 4)) !=
                    HeaderFlags)
            {
                throw PackageError(
                    JsonSnapshotPackageRules.InvalidFormat,
                    "The JSON package header version, size, or flags are unsupported.");
            }

            uint manifestLength =
                BinaryPrimitives.ReadUInt32BigEndian(
                    header.AsSpan(
                        ManifestLengthOffset,
                        4));
            ulong unsignedSnapshotLength =
                BinaryPrimitives.ReadUInt64BigEndian(
                    header.AsSpan(
                        SnapshotLengthOffset,
                        8));
            if (manifestLength == 0 ||
                manifestLength >
                    JsonSnapshotPackageManifestSerializer
                        .MaximumManifestBytes)
            {
                throw PackageError(
                    JsonSnapshotPackageRules
                        .SizeLimitExceeded,
                    "The JSON package manifest exceeds its safety bound.");
            }
            if (unsignedSnapshotLength >
                    (ulong)settings.MaxSourceBytes ||
                unsignedSnapshotLength > long.MaxValue)
            {
                throw PackageError(
                    JsonSnapshotPackageRules
                        .SizeLimitExceeded,
                    "The JSON package snapshot exceeds the configured byte limit.");
            }

            long snapshotLength =
                (long)unsignedSnapshotLength;
            long exactLength;
            try
            {
                exactLength = checked(
                    HeaderSize +
                    (long)manifestLength +
                    snapshotLength);
            }
            catch (OverflowException exception)
            {
                throw PackageError(
                    JsonSnapshotPackageRules
                        .SizeLimitExceeded,
                    "The JSON package section lengths overflow the supported range.",
                    exception);
            }

            if (package.Length != exactLength)
            {
                throw PackageError(
                    JsonSnapshotPackageRules.InvalidFormat,
                    "The JSON package has truncated, overlapping, or trailing section bytes.");
            }

            return new PackageHeader(
                checked((int)manifestLength),
                snapshotLength,
                header
                    .AsSpan(
                        ManifestHashOffset,
                        DigestBytes)
                    .ToArray());
        }
        finally
        {
            CryptographicOperations.ZeroMemory(header);
        }
    }

    private static JsonSnapshotPackageManifest CreatePublicManifest(
        JsonSnapshotPackageManifestPayload payload,
        ReadOnlySpan<byte> manifestHash,
        MigrationSourceIdentity source) =>
        new(
            "sha256:" +
                Convert.ToHexString(manifestHash)
                    .ToLowerInvariant(),
            payload.Snapshot.SnapshotIdentity,
            payload.Snapshot.ContentDigest,
            payload.Snapshot.ContentLength,
            source,
            payload.Source.OptionsDigest,
            payload.Catalog.TargetCSharpDbVersion,
            payload.Catalog.Digest);

    private static JsonSnapshotPackageOpenOptions
        ValidateOpenOptions(JsonSnapshotPackageOpenOptions options)
    {
        if (options.MaxSourceBytes < 0 ||
            options.MaxSourceBytes == long.MaxValue)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "The source byte limit must be non-negative and leave room for limit detection.");
        }
        if (options.CopyBufferBytes is
            < 4096 or > 16 * 1024 * 1024)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "The copy buffer must be between 4 KiB and 16 MiB.");
        }
        if (options.WorkspacePath is not null &&
            string.IsNullOrWhiteSpace(options.WorkspacePath))
        {
            throw new ArgumentException(
                "The snapshot workspace path cannot be blank.",
                nameof(options));
        }
        if (options.ExpectedManifestDigest is not null &&
            !IsCanonicalPrefixedDigest(
                options.ExpectedManifestDigest))
        {
            throw new ArgumentException(
                "The expected manifest digest must be canonical lowercase SHA-256 text.",
                nameof(options));
        }

        return options;
    }

    private static void ValidateExpectedManifestDigest(
        string? expectedManifestDigest,
        ReadOnlySpan<byte> actualManifestHash)
    {
        if (expectedManifestDigest is null)
            return;

        byte[] expectedBytes = Convert.FromHexString(
            expectedManifestDigest.AsSpan(7));
        try
        {
            if (!CryptographicOperations.FixedTimeEquals(
                    expectedBytes,
                    actualManifestHash))
            {
                throw PackageError(
                    JsonSnapshotPackageRules
                        .IntegrityMismatch,
                    "The JSON package does not match the trusted manifest digest.");
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(expectedBytes);
        }
    }

    private static string ValidateDestination(string fullPath)
    {
        RejectAlternateDataStream(fullPath);
        string parentPath = Path.GetDirectoryName(fullPath)
            ?? throw new ArgumentException(
                "The JSON package path must have a parent directory.");
        if (!Directory.Exists(parentPath))
        {
            throw new DirectoryNotFoundException(
                "The JSON package parent directory does not exist.");
        }
        FileAttributes parentAttributes =
            File.GetAttributes(parentPath);
        if ((parentAttributes &
             FileAttributes.ReparsePoint) != 0)
        {
            throw PackageError(
                JsonSnapshotPackageRules.UnsafePath,
                "The JSON package parent directory cannot be a reparse point.");
        }

        if (PathExists(fullPath))
        {
            throw new IOException(
                "The JSON package destination already exists.");
        }
        return parentPath;
    }

    private static void ValidateInputPath(string fullPath) =>
        RejectAlternateDataStream(fullPath);

    private static void ValidateOpenedFile(FileStream package)
    {
        FileAttributes attributes =
            File.GetAttributes(package.SafeFileHandle);
        if ((attributes &
             (FileAttributes.Directory |
              FileAttributes.ReparsePoint |
              FileAttributes.Device)) != 0 ||
            !package.CanRead ||
            !package.CanSeek)
        {
            throw PackageError(
                JsonSnapshotPackageRules.UnsafePath,
                "The opened JSON package handle is not a regular seekable file.");
        }
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
            throw PackageError(
                JsonSnapshotPackageRules.UnsafePath,
                "Windows alternate data streams cannot be used as JSON packages.");
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

    private static FileStream CreatePackageFile(string path)
    {
        var options = new FileStreamOptions
        {
            Mode = FileMode.CreateNew,
            Access = FileAccess.Write,
            Share = FileShare.None,
            BufferSize = 128 * 1024,
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

    private static FileStream OpenPackageFile(
        string path,
        int bufferSize) =>
        JsonSnapshotPackageFile.OpenReadNoFollow(
            path,
            bufferSize);

    private static void DeleteOwnedTempFile(string tempPath)
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

    private static bool IsCanonicalPrefixedDigest(
        string? digest) =>
        digest is not null &&
        digest.Length == 71 &&
        digest.StartsWith(
            "sha256:",
            StringComparison.Ordinal) &&
        IsLowerHex(digest.AsSpan(7));

    private static bool IsCanonicalHexDigest(string? digest) =>
        digest is not null &&
        digest.Length == 64 &&
        IsLowerHex(digest);

    private static bool IsLowerHex(ReadOnlySpan<char> value)
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

    private static JsonSnapshotPackageException PackageError(
        string ruleId,
        string message) =>
        new(ruleId, message);

    private static JsonSnapshotPackageException PackageError(
        string ruleId,
        string message,
        Exception innerException) =>
        new(ruleId, message, innerException);

    private sealed record PackageHeader(
        int ManifestLength,
        long SnapshotLength,
        byte[] ManifestHash);

    private sealed class ExactLengthReadStream : Stream
    {
        private readonly Stream inner;
        private readonly long length;
        private long position;

        internal ExactLengthReadStream(
            Stream inner,
            long length)
        {
            this.inner = inner;
            this.length = length;
        }

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => length;

        public override long Position
        {
            get => position;
            set => throw new NotSupportedException();
        }

        public override int Read(
            byte[] buffer,
            int offset,
            int count) =>
            Read(buffer.AsSpan(offset, count));

        public override int Read(Span<byte> buffer)
        {
            int requested = (int)Math.Min(
                buffer.Length,
                length - position);
            if (requested == 0)
                return 0;
            int read = inner.Read(buffer[..requested]);
            position += read;
            return read;
        }

        public override Task<int> ReadAsync(
            byte[] buffer,
            int offset,
            int count,
            CancellationToken cancellationToken) =>
            ReadAsync(
                    buffer.AsMemory(offset, count),
                    cancellationToken)
                .AsTask();

        public override async ValueTask<int> ReadAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            int requested = (int)Math.Min(
                buffer.Length,
                length - position);
            if (requested == 0)
                return 0;
            int read = await inner
                .ReadAsync(
                    buffer[..requested],
                    cancellationToken)
                .ConfigureAwait(false);
            position += read;
            return read;
        }

        public override void Flush()
        {
        }

        public override long Seek(
            long offset,
            SeekOrigin origin) =>
            throw new NotSupportedException();

        public override void SetLength(long value) =>
            throw new NotSupportedException();

        public override void Write(
            byte[] buffer,
            int offset,
            int count) =>
            throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            // OpenAsync owns the package stream and keeps it open until all
            // raw-section verification has completed.
            base.Dispose(disposing);
        }
    }
}
