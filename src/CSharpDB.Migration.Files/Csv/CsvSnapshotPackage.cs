using System.Buffers;
using System.Buffers.Binary;
using System.Globalization;
using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Files.Csv;

/// <summary>
/// Publishes and reopens one immutable CSV snapshot, reader policy, inference
/// recipe, and catalog binding as an atomic single-file package.
/// </summary>
public static class CsvSnapshotPackage
{
    public const string Format = CsvSnapshotPackageManifestSerializer.Format;
    public const string FileExtension = ".csdbcsv";

    private const int HeaderSize = 64;
    private const uint HeaderVersion = 1;
    private const uint HeaderFlags = 0;
    private const int ManifestLengthOffset = 16;
    private const int FlagsOffset = 20;
    private const int SnapshotLengthOffset = 24;
    private const int ManifestHashOffset = 32;
    private const int DigestBytes = 32;
    private const string NewlinePolicy = "common-auto";

    private static ReadOnlySpan<byte> HeaderMagic => "CSDBCSV1"u8;

    /// <summary>
    /// Atomically publishes a retained package. The destination must not
    /// already exist, and ownership of the snapshot and schema stays with the
    /// caller. Unix files are created with user-only mode; on Windows the
    /// caller must choose a parent directory with a trusted ACL.
    /// </summary>
    public static async ValueTask<CsvSnapshotPackageManifest> WriteAsync(
        string path,
        CsvSourceSnapshot snapshot,
        CsvSchemaInferenceResult schema,
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

        MigrationCatalog catalog = schema.CreateCatalog(targetCSharpDbVersion);
        _ = CsvMigrationDataSource.ValidateCatalogBinding(schema, snapshot, catalog);

        CsvSnapshotPackageManifestPayload payload = CreatePayload(schema, catalog);
        byte[] manifestBytes = CsvSnapshotPackageManifestSerializer.Serialize(payload);
        byte[] manifestHash = SHA256.HashData(manifestBytes);
        byte[] header = CreateHeader(manifestBytes.Length, snapshot.ContentLength, manifestHash);
        var manifest = CreatePublicManifest(payload, manifestHash, schema.Source);
        string tempPath = Path.Combine(
            parentPath,
            $".csdbcsv-{Guid.NewGuid():N}.tmp");
        bool tempOwned = false;
        bool published = false;

        try
        {
            FileStream destination = CreatePackageFile(tempPath);
            tempOwned = true;
            await using (destination)
            {
                await destination.WriteAsync(header, cancellationToken).ConfigureAwait(false);
                await destination.WriteAsync(manifestBytes, cancellationToken).ConfigureAwait(false);
                await CopySnapshotAsync(
                        snapshot,
                        destination,
                        expectedLength: snapshot.ContentLength,
                        expectedDigest: snapshot.ContentDigest,
                        cancellationToken)
                    .ConfigureAwait(false);
                await destination.FlushAsync(cancellationToken).ConfigureAwait(false);
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
    public static async ValueTask<CsvSnapshotPackageSession> OpenAsync(
        string path,
        CsvSnapshotPackageOpenOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        CsvSnapshotPackageOpenOptions settings = ValidateOpenOptions(
            options ?? new CsvSnapshotPackageOpenOptions());
        string fullPath = Path.GetFullPath(path);
        ValidateInputPath(fullPath);
        cancellationToken.ThrowIfCancellationRequested();

        CsvSourceSnapshot? snapshot = null;
        CsvMigrationDataSource? dataSource = null;
        try
        {
            CsvSnapshotPackageManifestPayload payload;
            byte[] manifestHash;
            await using (FileStream package = OpenPackageFile(fullPath, settings.CopyBufferBytes))
            {
                ValidateOpenedFile(package);
                PackageHeader header = await ReadHeaderAsync(package, settings, cancellationToken)
                    .ConfigureAwait(false);
                ValidateExpectedManifestDigest(
                    settings.ExpectedManifestDigest,
                    header.ManifestHash);
                byte[] manifestBytes = new byte[header.ManifestLength];
                try
                {
                    await package.ReadExactlyAsync(manifestBytes, cancellationToken)
                        .ConfigureAwait(false);
                    byte[] actualManifestHash = SHA256.HashData(manifestBytes);
                    try
                    {
                        if (!CryptographicOperations.FixedTimeEquals(
                                header.ManifestHash,
                                actualManifestHash))
                        {
                            throw PackageError(
                                CsvSnapshotPackageRules.IntegrityMismatch,
                                "The CSV package manifest hash does not match its bytes.");
                        }
                    }
                    finally
                    {
                        CryptographicOperations.ZeroMemory(actualManifestHash);
                    }

                    try
                    {
                        payload = CsvSnapshotPackageManifestSerializer.Deserialize(manifestBytes);
                    }
                    catch (InvalidDataException)
                    {
                        throw PackageError(
                            CsvSnapshotPackageRules.InvalidFormat,
                            "The CSV package manifest is invalid.");
                    }
                }
                finally
                {
                    CryptographicOperations.ZeroMemory(manifestBytes);
                }

                ValidatePayload(payload, header.SnapshotLength);
                manifestHash = header.ManifestHash;

                await using var rawSection = new ExactLengthReadStream(
                    package,
                    header.SnapshotLength);
                snapshot = await CsvSourceSnapshot.CreateAsync(
                        rawSection,
                        new CsvSourceSnapshotOptions
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
            CsvReaderOptions readerOptions = RestoreReaderOptions(payload.Reader, settings);
            CsvResolvedFormat resolvedFormat = RestoreFormat(payload.Reader);
            MigrationSourceIdentity retainedSource = RestoreSource(payload.Source);

            CsvSourceBinding binding;
            try
            {
                binding = await CsvSourceBinding.RestoreFromVerifiedSnapshotAsync(
                        snapshot,
                        retainedSource,
                        resolvedFormat,
                        payload.Source.OptionsDigest,
                        readerOptions,
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (InvalidDataException exception)
            {
                throw PackageError(
                    CsvSnapshotPackageRules.PolicyMismatch,
                    "The CSV package reader binding does not match its retained snapshot.",
                    exception);
            }

            CsvSchemaInferenceRecipe recipe = RestoreRecipe(payload.Inference);
            CsvSchemaInferenceResult schema;
            try
            {
                schema = await CsvSchemaInferer.ReplayAsync(
                        binding,
                        snapshot,
                        recipe,
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception exception) when (
                exception is InvalidDataException or ArgumentException)
            {
                throw PackageError(
                    CsvSnapshotPackageRules.PolicyMismatch,
                    "The CSV package inference recipe is invalid.",
                    exception);
            }

            MigrationCatalog catalog = schema.CreateCatalog(
                payload.Catalog.TargetCSharpDbVersion);
            string catalogDigest = MigrationArtifactSerializer.ComputeCatalogDigest(catalog);
            if (!string.Equals(catalogDigest, payload.Catalog.Digest, StringComparison.Ordinal))
            {
                throw PackageError(
                    CsvSnapshotPackageRules.PolicyMismatch,
                    "The CSV package catalog digest does not match replayed schema inference.");
            }

            dataSource = CsvMigrationDataSource.CreateFromVerifiedSnapshot(
                schema,
                snapshot,
                catalog);
            CsvSnapshotPackageManifest manifest = CreatePublicManifest(
                payload,
                manifestHash,
                binding.Source);
            var session = new CsvSnapshotPackageSession(
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
                    await dataSource.DisposeAsync().ConfigureAwait(false);
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
                    await snapshot.DisposeAsync().ConfigureAwait(false);
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
                "CSV package open failed and private snapshot cleanup also failed.",
                cleanupFailures);
        }
    }

    private static CsvSnapshotPackageManifestPayload CreatePayload(
        CsvSchemaInferenceResult schema,
        MigrationCatalog catalog)
    {
        CsvSourceBinding binding = schema.Binding;
        CsvReaderOptions reader = binding.ReaderOptions;
        ValidateReproducibleCulture(reader.Culture, binding.Format.CulturePolicyDigest);
        CsvSchemaInferenceRecipe recipe = schema.Recipe;
        return new CsvSnapshotPackageManifestPayload
        {
            Contracts = new CsvSnapshotPackageContractIdsManifest
            {
                Snapshot = CsvSourceSnapshot.IdentityAlgorithm,
                Binding = CsvSourceBinding.SourceFingerprintAlgorithm,
                Format = CsvSourceBinding.FormatAlgorithm,
                Inspection = CsvFormatInspection.AlgorithmId,
                Schema = CsvSchemaInferenceResult.AlgorithmId,
                Scalar = CsvSchemaInferenceResult.ScalarPolicyId,
                CatalogFormat = MigrationArtifactFormats.CatalogV1,
            },
            Snapshot = new CsvSnapshotPackageSnapshotManifest
            {
                ContentLength = binding.ContentLength,
                ContentDigest = binding.ContentDigest,
                SnapshotIdentity = binding.SnapshotIdentity,
            },
            Source = new CsvSnapshotPackageSourceManifest
            {
                Identity = binding.Source.Identity,
                Fingerprint = binding.Source.Fingerprint,
                OptionsDigest = binding.OptionsDigest,
            },
            Reader = new CsvSnapshotPackageReaderManifest
            {
                HasHeaderRecord = reader.HasHeaderRecord,
                Delimiter = reader.Delimiter,
                Quote = reader.Quote,
                ConfiguredEncodingName = reader.Encoding.WebName,
                ConfiguredEncodingCodePage = reader.Encoding.CodePage,
                DetectEncodingFromByteOrderMarks = reader.DetectEncodingFromByteOrderMarks,
                ResolvedEncodingName = binding.Format.EncodingName,
                ResolvedEncodingCodePage = binding.Format.EncodingCodePage,
                HasByteOrderMark = binding.Format.HasByteOrderMark,
                CultureName = reader.Culture.Name,
                CultureUseUserOverride = reader.Culture.UseUserOverride,
                CulturePolicyDigest = binding.Format.CulturePolicyDigest,
                NullToken = reader.NullToken,
                NullTokenMatchesQuotedFields = reader.NullTokenMatchesQuotedFields,
                ExpectedFieldCount = reader.ExpectedFieldCount,
                NewlinePolicy = binding.Format.NewlinePolicy,
                MaxFieldCharacters = reader.MaxFieldCharacters,
                MaxRecordCharacters = reader.MaxRecordCharacters,
                MaxFieldsPerRecord = reader.MaxFieldsPerRecord,
            },
            Inference = new CsvSnapshotPackageInferenceManifest
            {
                CollectProfile = recipe.CollectProfile,
                MaxDataRecords = recipe.MaxDataRecords,
                MaxProfileCharacters = recipe.MaxProfileCharacters,
                TableName = recipe.TableName,
                ColumnOverrides = recipe.ColumnOverrides
                    .Select(item => new CsvSnapshotPackageColumnOverrideManifest
                    {
                        Index = item.ColumnIndex,
                        ExpectedHeader = item.ExpectedHeader,
                        LogicalType = item.LogicalType,
                        Nullable = item.Nullable,
                    })
                    .ToArray(),
            },
            Catalog = new CsvSnapshotPackageCatalogManifest
            {
                TargetCSharpDbVersion = catalog.TargetCSharpDbVersion,
                Digest = MigrationArtifactSerializer.ComputeCatalogDigest(catalog),
            },
        };
    }

    private static void ValidatePayload(
        CsvSnapshotPackageManifestPayload payload,
        long headerSnapshotLength)
    {
        CsvSnapshotPackageContractIdsManifest contracts = payload.Contracts;
        if (!string.Equals(contracts.Snapshot, CsvSourceSnapshot.IdentityAlgorithm, StringComparison.Ordinal) ||
            !string.Equals(contracts.Binding, CsvSourceBinding.SourceFingerprintAlgorithm, StringComparison.Ordinal) ||
            !string.Equals(contracts.Format, CsvSourceBinding.FormatAlgorithm, StringComparison.Ordinal) ||
            !string.Equals(contracts.Inspection, CsvFormatInspection.AlgorithmId, StringComparison.Ordinal) ||
            !string.Equals(contracts.Schema, CsvSchemaInferenceResult.AlgorithmId, StringComparison.Ordinal) ||
            !string.Equals(contracts.Scalar, CsvSchemaInferenceResult.ScalarPolicyId, StringComparison.Ordinal) ||
            !string.Equals(contracts.CatalogFormat, MigrationArtifactFormats.CatalogV1, StringComparison.Ordinal))
        {
            throw PackageError(
                CsvSnapshotPackageRules.PolicyMismatch,
                "The CSV package uses an unsupported policy contract version.");
        }

        CsvSnapshotPackageSnapshotManifest snapshot = payload.Snapshot;
        if (snapshot.ContentLength != headerSnapshotLength ||
            snapshot.ContentLength < 0 ||
            !IsCanonicalPrefixedDigest(snapshot.ContentDigest) ||
            !string.Equals(
                snapshot.SnapshotIdentity,
                $"{CsvSourceSnapshot.IdentityAlgorithm}:{snapshot.ContentDigest}:bytes:{snapshot.ContentLength}",
                StringComparison.Ordinal))
        {
            throw PackageError(
                CsvSnapshotPackageRules.IntegrityMismatch,
                "The CSV package snapshot identity is invalid or inconsistent with its header.");
        }

        if (!IsCanonicalPrefixedDigest(payload.Source.Fingerprint) ||
            !IsCanonicalPrefixedDigest(payload.Source.OptionsDigest))
        {
            throw PackageError(
                CsvSnapshotPackageRules.PolicyMismatch,
                "The CSV package source digests are not canonical SHA-256 text.");
        }

        CsvSnapshotPackageReaderManifest reader = payload.Reader;
        if (reader.CultureUseUserOverride ||
            !string.Equals(reader.NewlinePolicy, NewlinePolicy, StringComparison.Ordinal) ||
            !IsCanonicalPrefixedDigest(reader.CulturePolicyDigest) ||
            !IsSupportedEncodingCodePage(reader.ConfiguredEncodingCodePage) ||
            !IsSupportedEncodingCodePage(reader.ResolvedEncodingCodePage))
        {
            throw PackageError(
                CsvSnapshotPackageRules.PolicyMismatch,
                "The CSV package reader policy is unsupported or noncanonical.");
        }

        if (string.IsNullOrWhiteSpace(payload.Catalog.TargetCSharpDbVersion) ||
            !IsCanonicalHexDigest(payload.Catalog.Digest))
        {
            throw PackageError(
                CsvSnapshotPackageRules.PolicyMismatch,
                "The CSV package catalog binding is invalid.");
        }
    }

    private static void ValidateSnapshot(
        CsvSnapshotPackageSnapshotManifest expected,
        CsvSourceSnapshot actual)
    {
        if (expected.ContentLength != actual.ContentLength ||
            !string.Equals(expected.ContentDigest, actual.ContentDigest, StringComparison.Ordinal) ||
            !string.Equals(expected.SnapshotIdentity, actual.SnapshotIdentity, StringComparison.Ordinal))
        {
            throw PackageError(
                CsvSnapshotPackageRules.IntegrityMismatch,
                "The retained CSV bytes do not match the package snapshot identity.");
        }
    }

    private static CsvReaderOptions RestoreReaderOptions(
        CsvSnapshotPackageReaderManifest manifest,
        CsvSnapshotPackageOpenOptions options)
    {
        CultureInfo culture = RestoreCulture(manifest, options.CultureOverride);
        Encoding encoding = CreateEncoding(manifest.ConfiguredEncodingCodePage);
        if (!string.Equals(
                encoding.WebName,
                manifest.ConfiguredEncodingName,
                StringComparison.Ordinal))
        {
            throw PackageError(
                CsvSnapshotPackageRules.PolicyMismatch,
                "The CSV package configured encoding name and code page disagree.");
        }

        return new CsvReaderOptions
        {
            HasHeaderRecord = manifest.HasHeaderRecord,
            Delimiter = manifest.Delimiter,
            Quote = manifest.Quote,
            Culture = culture,
            Encoding = encoding,
            DetectEncodingFromByteOrderMarks = manifest.DetectEncodingFromByteOrderMarks,
            NullToken = manifest.NullToken,
            NullTokenMatchesQuotedFields = manifest.NullTokenMatchesQuotedFields,
            ExpectedFieldCount = manifest.ExpectedFieldCount,
            MaxFieldCharacters = manifest.MaxFieldCharacters,
            MaxRecordCharacters = manifest.MaxRecordCharacters,
            MaxFieldsPerRecord = manifest.MaxFieldsPerRecord,
            LeaveOpen = false,
        };
    }

    private static CsvResolvedFormat RestoreFormat(
        CsvSnapshotPackageReaderManifest manifest) => new(
            manifest.Delimiter,
            manifest.Quote,
            manifest.HasHeaderRecord,
            manifest.ResolvedEncodingName,
            manifest.ResolvedEncodingCodePage,
            manifest.HasByteOrderMark,
            manifest.CultureName,
            manifest.CulturePolicyDigest,
            manifest.NullToken,
            manifest.NullTokenMatchesQuotedFields,
            manifest.ExpectedFieldCount);

    private static MigrationSourceIdentity RestoreSource(
        CsvSnapshotPackageSourceManifest manifest) => new()
        {
            Kind = MigrationSourceKind.Csv,
            Identity = manifest.Identity,
            Fingerprint = manifest.Fingerprint,
            ProviderVersion = CsvSourceBinding.AdapterProviderVersion,
            SourceVersion = null,
            Consistency = new MigrationConsistencyStrategy
            {
                Kind = MigrationConsistencyKind.Snapshot,
                Description = CsvSourceBinding.SnapshotConsistencyDescription,
                Watermark = null,
            },
        };

    private static CsvSchemaInferenceRecipe RestoreRecipe(
        CsvSnapshotPackageInferenceManifest manifest) => new(
            manifest.CollectProfile,
            manifest.MaxDataRecords,
            manifest.TableName,
            manifest.MaxProfileCharacters,
            manifest.ColumnOverrides.Select(item => new CsvColumnSchemaOverride
            {
                ColumnIndex = item.Index,
                ExpectedHeader = item.ExpectedHeader,
                LogicalType = item.LogicalType,
                Nullable = item.Nullable,
            }));

    private static CultureInfo RestoreCulture(
        CsvSnapshotPackageReaderManifest manifest,
        CultureInfo? cultureOverride)
    {
        CultureInfo culture;
        try
        {
            culture = cultureOverride is null
                ? new CultureInfo(manifest.CultureName, useUserOverride: false)
                : (CultureInfo)cultureOverride.Clone();
        }
        catch (CultureNotFoundException exception)
        {
            throw PackageError(
                CsvSnapshotPackageRules.PolicyMismatch,
                "The CSV package culture is unavailable on this platform.",
                exception);
        }

        if (culture.UseUserOverride ||
            !string.Equals(culture.Name, manifest.CultureName, StringComparison.Ordinal) ||
            !string.Equals(
                CsvCulturePolicy.ComputeDigest(culture),
                manifest.CulturePolicyDigest,
                StringComparison.Ordinal))
        {
            throw PackageError(
                CsvSnapshotPackageRules.PolicyMismatch,
                "The available CSV culture policy does not match the retained package.");
        }

        return CultureInfo.ReadOnly(culture);
    }

    private static void ValidateReproducibleCulture(
        CultureInfo culture,
        string expectedDigest)
    {
        CultureInfo reproduced;
        try
        {
            reproduced = new CultureInfo(culture.Name, useUserOverride: false);
        }
        catch (CultureNotFoundException exception)
        {
            throw new NotSupportedException(
                "The CSV culture cannot be retained by package format v1.",
                exception);
        }

        if (culture.UseUserOverride ||
            !string.Equals(
                CsvCulturePolicy.ComputeDigest(reproduced),
                expectedDigest,
                StringComparison.Ordinal))
        {
            throw new NotSupportedException(
                "CSV package format v1 requires a reproducible culture with user overrides disabled.");
        }
    }

    private static Encoding CreateEncoding(int codePage) => codePage switch
    {
        65001 => new UTF8Encoding(false, true),
        1200 => new UnicodeEncoding(false, false, true),
        1201 => new UnicodeEncoding(true, false, true),
        12000 => new UTF32Encoding(false, false, true),
        12001 => new UTF32Encoding(true, false, true),
        _ => throw PackageError(
            CsvSnapshotPackageRules.PolicyMismatch,
            "The CSV package configured encoding is unsupported."),
    };

    private static bool IsSupportedEncodingCodePage(int codePage) =>
        codePage is 65001 or 1200 or 1201 or 12000 or 12001;

    private static async ValueTask CopySnapshotAsync(
        CsvSourceSnapshot snapshot,
        Stream destination,
        long expectedLength,
        string expectedDigest,
        CancellationToken cancellationToken)
    {
        byte[] buffer = ArrayPool<byte>.Shared.Rent(128 * 1024);
        try
        {
            await using Stream source = snapshot.OpenRead();
            using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            long total = 0;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                long remaining = expectedLength - total;
                int requested = remaining == 0
                    ? 1
                    : (int)Math.Min(buffer.Length, remaining + 1);
                int read = await source.ReadAsync(
                        buffer.AsMemory(0, requested),
                        cancellationToken)
                    .ConfigureAwait(false);
                if (read == 0)
                    break;
                if (read > remaining)
                {
                    throw PackageError(
                        CsvSnapshotPackageRules.IntegrityMismatch,
                        "The CSV snapshot grew while its package was being written.");
                }

                await destination.WriteAsync(
                        buffer.AsMemory(0, read),
                        cancellationToken)
                    .ConfigureAwait(false);
                hasher.AppendData(buffer, 0, read);
                total += read;
            }

            string actualDigest =
                "sha256:" + Convert.ToHexString(hasher.GetHashAndReset()).ToLowerInvariant();
            if (total != expectedLength ||
                !string.Equals(actualDigest, expectedDigest, StringComparison.Ordinal))
            {
                throw PackageError(
                    CsvSnapshotPackageRules.IntegrityMismatch,
                    "The CSV snapshot changed while its package was being written.");
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
        }
    }

    private static byte[] CreateHeader(
        int manifestLength,
        long snapshotLength,
        ReadOnlySpan<byte> manifestHash)
    {
        if (manifestLength <= 0 ||
            manifestLength > CsvSnapshotPackageManifestSerializer.MaximumManifestBytes)
        {
            throw new InvalidDataException("The CSV package manifest length is invalid.");
        }
        if (snapshotLength < 0)
            throw new InvalidDataException("The CSV package snapshot length is invalid.");
        if (manifestHash.Length != DigestBytes)
            throw new ArgumentException("The manifest hash must be SHA-256.", nameof(manifestHash));

        byte[] header = new byte[HeaderSize];
        HeaderMagic.CopyTo(header);
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(8, 4), HeaderVersion);
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(12, 4), HeaderSize);
        BinaryPrimitives.WriteUInt32BigEndian(
            header.AsSpan(ManifestLengthOffset, 4),
            checked((uint)manifestLength));
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(FlagsOffset, 4), HeaderFlags);
        BinaryPrimitives.WriteUInt64BigEndian(
            header.AsSpan(SnapshotLengthOffset, 8),
            checked((ulong)snapshotLength));
        manifestHash.CopyTo(header.AsSpan(ManifestHashOffset, DigestBytes));
        return header;
    }

    private static async ValueTask<PackageHeader> ReadHeaderAsync(
        FileStream package,
        CsvSnapshotPackageOpenOptions settings,
        CancellationToken cancellationToken)
    {
        if (package.Length < HeaderSize)
        {
            throw PackageError(
                CsvSnapshotPackageRules.InvalidFormat,
                "The CSV package header is truncated.");
        }

        byte[] header = new byte[HeaderSize];
        try
        {
            await package.ReadExactlyAsync(header, cancellationToken).ConfigureAwait(false);
            if (!header.AsSpan(0, HeaderMagic.Length).SequenceEqual(HeaderMagic) ||
                BinaryPrimitives.ReadUInt32BigEndian(header.AsSpan(8, 4)) != HeaderVersion ||
                BinaryPrimitives.ReadUInt32BigEndian(header.AsSpan(12, 4)) != HeaderSize ||
                BinaryPrimitives.ReadUInt32BigEndian(header.AsSpan(FlagsOffset, 4)) != HeaderFlags)
            {
                throw PackageError(
                    CsvSnapshotPackageRules.InvalidFormat,
                    "The CSV package header version, size, or flags are unsupported.");
            }

            uint manifestLength = BinaryPrimitives.ReadUInt32BigEndian(
                header.AsSpan(ManifestLengthOffset, 4));
            ulong unsignedSnapshotLength = BinaryPrimitives.ReadUInt64BigEndian(
                header.AsSpan(SnapshotLengthOffset, 8));
            if (manifestLength == 0 ||
                manifestLength > CsvSnapshotPackageManifestSerializer.MaximumManifestBytes)
            {
                throw PackageError(
                    CsvSnapshotPackageRules.SizeLimitExceeded,
                    "The CSV package manifest exceeds its safety bound.");
            }
            if (unsignedSnapshotLength > (ulong)settings.MaxSourceBytes ||
                unsignedSnapshotLength > long.MaxValue)
            {
                throw PackageError(
                    CsvSnapshotPackageRules.SizeLimitExceeded,
                    "The CSV package snapshot exceeds the configured byte limit.");
            }

            long snapshotLength = (long)unsignedSnapshotLength;
            long exactLength;
            try
            {
                exactLength = checked(HeaderSize + (long)manifestLength + snapshotLength);
            }
            catch (OverflowException exception)
            {
                throw PackageError(
                    CsvSnapshotPackageRules.SizeLimitExceeded,
                    "The CSV package section lengths overflow the supported range.",
                    exception);
            }

            if (package.Length != exactLength)
            {
                throw PackageError(
                    CsvSnapshotPackageRules.InvalidFormat,
                    "The CSV package has truncated, overlapping, or trailing section bytes.");
            }

            return new PackageHeader(
                checked((int)manifestLength),
                snapshotLength,
                header.AsSpan(ManifestHashOffset, DigestBytes).ToArray());
        }
        finally
        {
            CryptographicOperations.ZeroMemory(header);
        }
    }

    private static CsvSnapshotPackageManifest CreatePublicManifest(
        CsvSnapshotPackageManifestPayload payload,
        ReadOnlySpan<byte> manifestHash,
        MigrationSourceIdentity source) => new(
            "sha256:" + Convert.ToHexString(manifestHash).ToLowerInvariant(),
            payload.Snapshot.SnapshotIdentity,
            payload.Snapshot.ContentDigest,
            payload.Snapshot.ContentLength,
            source,
            payload.Source.OptionsDigest,
            payload.Catalog.TargetCSharpDbVersion,
            payload.Catalog.Digest);

    private static CsvSnapshotPackageOpenOptions ValidateOpenOptions(
        CsvSnapshotPackageOpenOptions options)
    {
        if (options.MaxSourceBytes < 0 || options.MaxSourceBytes == long.MaxValue)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "The source byte limit must be non-negative and leave room for limit detection.");
        }
        if (options.CopyBufferBytes is < 4096 or > 16 * 1024 * 1024)
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
            !IsCanonicalPrefixedDigest(options.ExpectedManifestDigest))
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

        byte[] expectedBytes = Convert.FromHexString(expectedManifestDigest.AsSpan(7));
        try
        {
            if (!CryptographicOperations.FixedTimeEquals(expectedBytes, actualManifestHash))
            {
                throw PackageError(
                    CsvSnapshotPackageRules.IntegrityMismatch,
                    "The CSV package does not match the trusted manifest digest.");
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
            ?? throw new ArgumentException("The CSV package path must have a parent directory.");
        if (!Directory.Exists(parentPath))
            throw new DirectoryNotFoundException("The CSV package parent directory does not exist.");
        FileAttributes parentAttributes = File.GetAttributes(parentPath);
        if ((parentAttributes & FileAttributes.ReparsePoint) != 0)
        {
            throw PackageError(
                CsvSnapshotPackageRules.UnsafePath,
                "The CSV package parent directory cannot be a reparse point.");
        }

        if (PathExists(fullPath))
            throw new IOException("The CSV package destination already exists.");
        return parentPath;
    }

    private static void ValidateInputPath(string fullPath)
    {
        RejectAlternateDataStream(fullPath);
    }

    private static void ValidateOpenedFile(FileStream package)
    {
        FileAttributes attributes = File.GetAttributes(package.SafeFileHandle);
        if ((attributes & (FileAttributes.Directory | FileAttributes.ReparsePoint | FileAttributes.Device)) != 0 ||
            !package.CanRead ||
            !package.CanSeek)
        {
            throw PackageError(
                CsvSnapshotPackageRules.UnsafePath,
                "The opened CSV package handle is not a regular seekable file.");
        }
    }

    private static void RejectAlternateDataStream(string fullPath)
    {
        if (!OperatingSystem.IsWindows())
            return;
        string root = Path.GetPathRoot(fullPath) ?? string.Empty;
        if (fullPath.AsSpan(root.Length).Contains(':'))
        {
            throw PackageError(
                CsvSnapshotPackageRules.UnsafePath,
                "Windows alternate data streams cannot be used as CSV packages.");
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
            Options = FileOptions.Asynchronous |
                FileOptions.SequentialScan |
                FileOptions.WriteThrough,
        };
        if (!OperatingSystem.IsWindows())
            options.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
        return new FileStream(path, options);
    }

    private static FileStream OpenPackageFile(string path, int bufferSize) =>
        CsvSnapshotPackageFile.OpenReadNoFollow(path, bufferSize);

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

    private static bool IsCanonicalPrefixedDigest(string? digest) =>
        digest is not null &&
        digest.Length == 71 &&
        digest.StartsWith("sha256:", StringComparison.Ordinal) &&
        IsLowerHex(digest.AsSpan(7));

    private static bool IsCanonicalHexDigest(string? digest) =>
        digest is not null && digest.Length == 64 && IsLowerHex(digest);

    private static bool IsLowerHex(ReadOnlySpan<char> value)
    {
        foreach (char character in value)
        {
            if (character is not (>= '0' and <= '9') and not (>= 'a' and <= 'f'))
                return false;
        }

        return true;
    }

    private static CsvSnapshotPackageException PackageError(
        string ruleId,
        string message) => new(ruleId, message);

    private static CsvSnapshotPackageException PackageError(
        string ruleId,
        string message,
        Exception innerException) => new(ruleId, message, innerException);

    private sealed record PackageHeader(
        int ManifestLength,
        long SnapshotLength,
        byte[] ManifestHash);

    private sealed class ExactLengthReadStream : Stream
    {
        private readonly Stream inner;
        private readonly long length;
        private long position;

        public ExactLengthReadStream(Stream inner, long length)
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

        public override int Read(byte[] buffer, int offset, int count) =>
            Read(buffer.AsSpan(offset, count));

        public override int Read(Span<byte> buffer)
        {
            int requested = (int)Math.Min(buffer.Length, length - position);
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
            ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override async ValueTask<int> ReadAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            int requested = (int)Math.Min(buffer.Length, length - position);
            if (requested == 0)
                return 0;
            int read = await inner.ReadAsync(buffer[..requested], cancellationToken)
                .ConfigureAwait(false);
            position += read;
            return read;
        }

        public override void Flush()
        {
        }

        public override long Seek(long offset, SeekOrigin origin) =>
            throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            // The package stream is owned by OpenAsync and deliberately stays
            // open until manifest and raw-section verification complete.
            base.Dispose(disposing);
        }
    }
}
