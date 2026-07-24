using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Files.Json;

/// <summary>
/// Publishes and reopens one immutable typed JSON snapshot package. Version 2
/// embeds the exact canonical typed-intent bytes and can only reopen through
/// a typed session.
/// </summary>
public static class JsonTypedSnapshotPackage
{
    public const string Format =
        JsonTypedSnapshotPackageManifestSerializer.Format;
    public const string FileExtension = ".csdbjson";

    private const int HeaderSize = 112;
    private const uint HeaderVersion = 2;
    private const uint HeaderFlags = 0;
    private const int ManifestLengthOffset = 16;
    private const int IntentLengthOffset = 20;
    private const int FlagsOffset = 24;
    private const int ReservedOffset = 28;
    private const int SnapshotLengthOffset = 32;
    private const int ManifestHashOffset = 40;
    private const int IntentHashOffset = 72;
    private const int ReservedTailOffset = 104;
    private const int DigestBytes = 32;

    private static ReadOnlySpan<byte> HeaderMagic =>
        "CSDBJSN2"u8;

    /// <summary>
    /// Atomically publishes a retained typed package. The caller keeps
    /// ownership of the snapshot and schema, and the destination must not
    /// already exist. The destination parent must remain under the caller's
    /// exclusive control for the duration of publication and cleanup.
    /// </summary>
    public static async ValueTask<
        JsonTypedSnapshotPackageManifest> WriteAsync(
        string path,
        JsonSourceSnapshot snapshot,
        JsonTypedTableSchemaInferenceResult schema,
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
        _ = JsonMigrationDataSource.ValidateTypedCatalogBinding(
            schema,
            snapshot,
            catalog);

        byte[] intentBytes =
            schema.IntentManifest.ToCanonicalUtf8Bytes();
        byte[]? manifestBytes = null;
        byte[]? intentHash = null;
        byte[]? manifestHash = null;
        byte[]? header = null;
        string? tempPath = null;
        bool tempOwned = false;
        bool published = false;
        try
        {
            ValidateIntentLength(intentBytes.Length);
            _ = JsonTypedIntentSidecar.Parse(
                intentBytes,
                schema.RepresentationSchema.Binding,
                schema.IntentManifest.ManifestDigest);

            intentHash = SHA256.HashData(intentBytes);
            JsonTypedSnapshotPackageManifestPayload payload =
                CreatePayload(
                    schema,
                    catalog,
                    intentBytes.Length,
                    FormatDigest(intentHash));
            manifestBytes =
                JsonTypedSnapshotPackageManifestSerializer
                    .Serialize(payload);
            manifestHash = SHA256.HashData(manifestBytes);
            header = CreateHeader(
                manifestBytes.Length,
                intentBytes.Length,
                snapshot.ContentLength,
                manifestHash,
                intentHash);
            var manifest = CreatePublicManifest(
                payload,
                manifestHash,
                schema.Source);

            tempPath = Path.Combine(
                parentPath,
                $".csdbjson-v2-{Guid.NewGuid():N}.tmp");
            FileStream destination =
                CreatePackageFile(tempPath);
            tempOwned = true;
            await using (destination)
            {
                await destination
                    .WriteAsync(header, cancellationToken)
                    .ConfigureAwait(false);
                await destination
                    .WriteAsync(
                        manifestBytes,
                        cancellationToken)
                    .ConfigureAwait(false);
                await destination
                    .WriteAsync(
                        intentBytes,
                        cancellationToken)
                    .ConfigureAwait(false);
                await CopySnapshotAsync(
                        snapshot,
                        destination,
                        snapshot.ContentLength,
                        snapshot.ContentDigest,
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
            CryptographicOperations.ZeroMemory(intentBytes);
            Zero(manifestBytes);
            Zero(intentHash);
            Zero(manifestHash);
            Zero(header);
            if (tempOwned && !published && tempPath is not null)
                DeleteOwnedTempFile(tempPath);
        }
    }

    /// <summary>
    /// Verifies a v2 package, copies its raw source into a private snapshot,
    /// reparses the exact embedded typed intent against that copy, and
    /// reconstructs an intent-bound schema, catalog, and data source.
    /// </summary>
    public static async ValueTask<
        JsonTypedSnapshotPackageSession> OpenAsync(
        string path,
        JsonSnapshotPackageOpenOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        JsonSnapshotPackageOpenOptions settings =
            ValidateOpenOptions(
                options ??
                new JsonSnapshotPackageOpenOptions());
        string fullPath = Path.GetFullPath(path);
        ValidateInputPath(fullPath);
        cancellationToken.ThrowIfCancellationRequested();

        JsonSourceSnapshot? snapshot = null;
        JsonMigrationDataSource? dataSource = null;
        PackageHeader? retainedHeader = null;
        byte[]? intentBytes = null;
        try
        {
            JsonTypedSnapshotPackageManifestPayload payload;
            await using (
                FileStream package =
                    OpenPackageFile(
                        fullPath,
                        settings.CopyBufferBytes))
            {
                ValidateOpenedFile(package);
                retainedHeader = await ReadHeaderAsync(
                        package,
                        settings,
                        cancellationToken)
                    .ConfigureAwait(false);
                ValidateExpectedManifestDigest(
                    settings.ExpectedManifestDigest,
                    retainedHeader.ManifestHash);

                byte[] manifestBytes =
                    new byte[retainedHeader.ManifestLength];
                try
                {
                    await package
                        .ReadExactlyAsync(
                            manifestBytes,
                            cancellationToken)
                        .ConfigureAwait(false);
                    VerifySectionHash(
                        manifestBytes,
                        retainedHeader.ManifestHash,
                        "The typed JSON package manifest hash does not match its bytes.");
                    try
                    {
                        payload =
                            JsonTypedSnapshotPackageManifestSerializer
                                .Deserialize(manifestBytes);
                    }
                    catch (InvalidDataException exception)
                    {
                        throw PackageError(
                            JsonSnapshotPackageRules
                                .InvalidFormat,
                            "The typed JSON package manifest is invalid.",
                            exception);
                    }
                }
                finally
                {
                    CryptographicOperations.ZeroMemory(
                        manifestBytes);
                }

                ValidatePayload(payload, retainedHeader);

                intentBytes =
                    new byte[retainedHeader.IntentLength];
                await package
                    .ReadExactlyAsync(
                        intentBytes,
                        cancellationToken)
                    .ConfigureAwait(false);
                VerifySectionHash(
                    intentBytes,
                    retainedHeader.IntentHash,
                    "The embedded typed JSON intent hash does not match its bytes.");
                ValidateIntentDigest(
                    payload.TypedIntent.ManifestDigest,
                    retainedHeader.IntentHash);
                JsonTypedIntentManifestPayload
                    preparsedIntent =
                        PrevalidateIntent(intentBytes);
                ValidatePreparsedIntent(
                    payload,
                    preparsedIntent);

                await using var rawSection =
                    new ExactLengthReadStream(
                        package,
                        retainedHeader.SnapshotLength);
                snapshot = await JsonSourceSnapshot.CreateAsync(
                        rawSection,
                        new JsonSourceSnapshotOptions
                        {
                            WorkspacePath =
                                settings.WorkspacePath,
                            MaxSourceBytes =
                                settings.MaxSourceBytes,
                            CopyBufferBytes =
                                settings.CopyBufferBytes,
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
                    "The typed JSON package reader binding does not match its retained snapshot.",
                    exception);
            }

            JsonTypedIntentManifest intentManifest;
            try
            {
                intentManifest =
                    JsonTypedIntentSidecar.Parse(
                        intentBytes!,
                        binding,
                        payload.TypedIntent.ManifestDigest);
            }
            catch (JsonTypedIntentException exception)
            {
                throw MapIntentError(exception);
            }
            finally
            {
                Zero(intentBytes);
                intentBytes = null;
            }

            ValidateParsedIntent(
                payload.TypedIntent,
                intentManifest);
            JsonTableSchemaInferenceRecipe recipe =
                RestoreRecipe(payload.Inference);

            JsonTypedTableSchemaInferenceResult schema;
            try
            {
                schema = recipe.CollectProfile
                    ? await JsonTypedTableSchemaInferer
                        .InferAsync(
                            binding,
                            snapshot,
                            intentManifest,
                            recipe.MaxProfileRecords,
                            recipe.ToOptions(),
                            cancellationToken)
                        .ConfigureAwait(false)
                    : await JsonTypedTableSchemaInferer
                        .DiscoverAsync(
                            binding,
                            snapshot,
                            intentManifest,
                            recipe.ToOptions(),
                            cancellationToken)
                        .ConfigureAwait(false);
            }
            catch (Exception exception) when (
                exception is InvalidDataException or
                ArgumentException or
                JsonReadException or
                JsonTableSchemaInferenceException or
                JsonTypedTableSchemaException or
                JsonTypedIntentException)
            {
                throw PackageError(
                    JsonSnapshotPackageRules.PolicyMismatch,
                    "The typed JSON package inference recipe is invalid.",
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
                    "The typed JSON package catalog digest does not match replayed schema inference.");
            }

            dataSource =
                JsonMigrationDataSource
                    .CreateFromVerifiedSnapshot(
                        schema,
                        snapshot,
                        catalog);
            JsonTypedSnapshotPackageManifest manifest =
                CreatePublicManifest(
                    payload,
                    retainedHeader!.ManifestHash,
                    schema.Source);
            var session =
                new JsonTypedSnapshotPackageSession(
                    manifest,
                    intentManifest,
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
            var cleanupFailures = new List<Exception>();
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
                    cleanupFailures.Add(cleanupFailure);
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
                    cleanupFailures.Add(cleanupFailure);
                }
            }

            if (cleanupFailures.Count == 0)
            {
                ExceptionDispatchInfo
                    .Capture(operationFailure)
                    .Throw();
                throw;
            }

            cleanupFailures.Insert(0, operationFailure);
            throw new AggregateException(
                "Typed JSON package open failed and private snapshot cleanup also failed.",
                cleanupFailures);
        }
        finally
        {
            Zero(intentBytes);
            retainedHeader?.Zero();
        }
    }

    private static JsonTypedSnapshotPackageManifestPayload
        CreatePayload(
            JsonTypedTableSchemaInferenceResult schema,
            MigrationCatalog catalog,
            int intentByteLength,
            string intentManifestDigest)
    {
        JsonTableSchemaInferenceResult representation =
            schema.RepresentationSchema;
        JsonSourceBinding binding = representation.Binding;
        JsonStreamingReaderOptions reader =
            binding.ReaderOptions;
        JsonTableSchemaInferenceRecipe recipe =
            representation.Recipe;
        IReadOnlyList<
            JsonSnapshotPackageColumnOverrideManifest>
            ordinaryOverrides =
                GetVerifiedOrdinaryOverrides(
                    recipe,
                    schema.IntentManifest);

        return new JsonTypedSnapshotPackageManifestPayload
        {
            Contracts =
                new JsonTypedSnapshotPackageContractIdsManifest
                {
                    Snapshot =
                        JsonSourceSnapshot.IdentityAlgorithm,
                    Binding =
                        JsonSourceBinding
                            .SourceFingerprintAlgorithm,
                    Options =
                        JsonSourceBinding.OptionsAlgorithm,
                    RepresentationSchema =
                        JsonTableSchemaInferenceResult.AlgorithmId,
                    RepresentationScalar =
                        JsonTableSchemaInferenceResult
                            .ScalarPolicyId,
                    TypedSchema =
                        JsonTypedTableSchemaInferenceResult
                            .AlgorithmId,
                    TypedScalar =
                        JsonTypedTableSchemaInferenceResult
                            .ScalarPolicyId,
                    CanonicalValue =
                        JsonInputContracts
                            .CanonicalNestedJsonVersion,
                    CatalogFormat =
                        MigrationArtifactFormats.CatalogV1,
                    IntentFormat =
                        JsonTypedIntentManifestSerializer
                            .Format,
                    TypedValue =
                        JsonTypedIntentManifestSerializer
                            .TypedValueContract,
                    TextCodec =
                        JsonTypedIntentManifestSerializer
                            .TextCodecContract,
                },
            Snapshot =
                new JsonSnapshotPackageSnapshotManifest
                {
                    ContentLength = binding.ContentLength,
                    ContentDigest = binding.ContentDigest,
                    SnapshotIdentity =
                        binding.SnapshotIdentity,
                },
            Source =
                new JsonSnapshotPackageSourceManifest
                {
                    Identity = binding.Source.Identity,
                    Fingerprint =
                        binding.Source.Fingerprint,
                    OptionsDigest = binding.OptionsDigest,
                },
            Reader =
                new JsonSnapshotPackageReaderManifest
                {
                    Framing = reader.Framing,
                    MaxValueBytes = reader.MaxValueBytes,
                    MaxDepth = reader.MaxDepth,
                    MaxPropertiesPerObject =
                        reader.MaxPropertiesPerObject,
                    MaxArrayElements =
                        reader.MaxArrayElements,
                    MaxTotalNodes = reader.MaxTotalNodes,
                    MaxPropertyNameBytes =
                        reader.MaxPropertyNameBytes,
                    MaxStringBytes =
                        reader.MaxStringBytes,
                    MaxNumberBytes =
                        reader.MaxNumberBytes,
                },
            Inference =
                new JsonSnapshotPackageInferenceManifest
                {
                    CollectProfile = recipe.CollectProfile,
                    MaxProfileRecords =
                        recipe.MaxProfileRecords,
                    TableName = recipe.TableName,
                    MaxColumns = recipe.MaxColumns,
                    MaxTotalColumnNameBytes =
                        recipe.MaxTotalColumnNameBytes,
                    MaxProfileBytes =
                        recipe.MaxProfileBytes,
                    ColumnOverrides = ordinaryOverrides,
                },
            TypedIntent =
                new JsonTypedSnapshotPackageIntentManifest
                {
                    ByteLength = intentByteLength,
                    ManifestDigest =
                        intentManifestDigest,
                    MaxDecodedBinaryBytes =
                        schema.IntentManifest
                            .MaxDecodedBinaryBytes,
                    MaxDecimalDigits =
                        schema.IntentManifest
                            .MaxDecimalDigits,
                    ColumnCount =
                        schema.IntentManifest.Columns.Count,
                },
            Catalog =
                new JsonSnapshotPackageCatalogManifest
                {
                    TargetCSharpDbVersion =
                        catalog.TargetCSharpDbVersion,
                    Digest =
                        MigrationArtifactSerializer
                            .ComputeCatalogDigest(catalog),
                },
        };
    }

    private static IReadOnlyList<
        JsonSnapshotPackageColumnOverrideManifest>
        GetVerifiedOrdinaryOverrides(
            JsonTableSchemaInferenceRecipe recipe,
            JsonTypedIntentManifest intentManifest)
    {
        Dictionary<int, JsonTypedColumnIntent> intents =
            intentManifest.Columns.ToDictionary(
                item => item.ColumnIndex);
        var foundTypedOrdinals = new HashSet<int>();
        var ordinary =
            new List<
                JsonSnapshotPackageColumnOverrideManifest>();

        foreach (
            JsonTableColumnSchemaOverride item in
            recipe.ColumnOverrides)
        {
            if (intents.TryGetValue(
                    item.ColumnIndex,
                    out JsonTypedColumnIntent? intent))
            {
                if (!foundTypedOrdinals.Add(
                        item.ColumnIndex) ||
                    !SyntheticOverrideMatches(
                        item,
                        intent))
                {
                    throw new ArgumentException(
                        "The typed JSON schema inference recipe does not contain the exact synthetic override required by its intent.",
                        nameof(recipe));
                }
                continue;
            }

            ordinary.Add(
                new JsonSnapshotPackageColumnOverrideManifest
                {
                    ColumnIndex = item.ColumnIndex,
                    ExpectedPropertyName =
                        item.ExpectedPropertyName,
                    LogicalType = item.LogicalType,
                    Nullable = item.Nullable,
                    MissingPolicy = item.MissingPolicy,
                });
        }

        if (foundTypedOrdinals.Count != intents.Count)
        {
            throw new ArgumentException(
                "The typed JSON schema inference recipe is missing a synthetic typed-intent override.",
                nameof(recipe));
        }

        return ordinary
            .OrderBy(item => item.ColumnIndex)
            .ToArray();
    }

    private static bool SyntheticOverrideMatches(
        JsonTableColumnSchemaOverride item,
        JsonTypedColumnIntent intent) =>
        item.ColumnIndex == intent.ColumnIndex &&
        string.Equals(
            item.ExpectedPropertyName,
            intent.ExpectedPropertyName,
            StringComparison.Ordinal) &&
        item.LogicalType ==
            RepresentationType(intent.Codec) &&
        item.Nullable == intent.Nullable &&
        item.MissingPolicy == intent.MissingPolicy;

    private static JsonTableColumnLogicalType
        RepresentationType(JsonTypedValueCodec codec) =>
        codec switch
        {
            JsonTypedValueCodec.DecimalNumber =>
                JsonTableColumnLogicalType.Decimal,
            JsonTypedValueCodec.BinaryBase64 or
            JsonTypedValueCodec.DecimalString or
            JsonTypedValueCodec.GuidD or
            JsonTypedValueCodec.DateCSharpDbText or
            JsonTypedValueCodec.TimeCSharpDbText or
            JsonTypedValueCodec.DateTimeCSharpDbText or
            JsonTypedValueCodec
                .DateTimeOffsetCSharpDbText or
            JsonTypedValueCodec.Int64String or
            JsonTypedValueCodec.UInt64String =>
                JsonTableColumnLogicalType.Text,
            _ => throw new ArgumentOutOfRangeException(
                nameof(codec)),
        };

    private static void ValidatePayload(
        JsonTypedSnapshotPackageManifestPayload payload,
        PackageHeader header)
    {
        JsonTypedSnapshotPackageContractIdsManifest contracts =
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
                contracts.RepresentationSchema,
                JsonTableSchemaInferenceResult.AlgorithmId,
                StringComparison.Ordinal) ||
            !string.Equals(
                contracts.RepresentationScalar,
                JsonTableSchemaInferenceResult.ScalarPolicyId,
                StringComparison.Ordinal) ||
            !string.Equals(
                contracts.TypedSchema,
                JsonTypedTableSchemaInferenceResult.AlgorithmId,
                StringComparison.Ordinal) ||
            !string.Equals(
                contracts.TypedScalar,
                JsonTypedTableSchemaInferenceResult
                    .ScalarPolicyId,
                StringComparison.Ordinal) ||
            !string.Equals(
                contracts.CanonicalValue,
                JsonInputContracts.CanonicalNestedJsonVersion,
                StringComparison.Ordinal) ||
            !string.Equals(
                contracts.CatalogFormat,
                MigrationArtifactFormats.CatalogV1,
                StringComparison.Ordinal) ||
            !string.Equals(
                contracts.IntentFormat,
                JsonTypedIntentManifestSerializer.Format,
                StringComparison.Ordinal) ||
            !string.Equals(
                contracts.TypedValue,
                JsonTypedIntentManifestSerializer
                    .TypedValueContract,
                StringComparison.Ordinal) ||
            !string.Equals(
                contracts.TextCodec,
                JsonTypedIntentManifestSerializer
                    .TextCodecContract,
                StringComparison.Ordinal))
        {
            throw PackageError(
                JsonSnapshotPackageRules.PolicyMismatch,
                "The typed JSON package uses an unsupported policy contract version.");
        }

        JsonSnapshotPackageSnapshotManifest snapshot =
            payload.Snapshot;
        if (snapshot.ContentLength !=
                header.SnapshotLength ||
            snapshot.ContentLength < 0 ||
            !IsCanonicalPrefixedDigest(
                snapshot.ContentDigest) ||
            !string.Equals(
                snapshot.SnapshotIdentity,
                $"{JsonSourceSnapshot.IdentityAlgorithm}:{snapshot.ContentDigest}:bytes:{snapshot.ContentLength}",
                StringComparison.Ordinal))
        {
            throw PackageError(
                JsonSnapshotPackageRules.IntegrityMismatch,
                "The typed JSON package snapshot identity is invalid or inconsistent with its header.");
        }

        if (!IsCanonicalPrefixedDigest(
                payload.Source.Fingerprint) ||
            !IsCanonicalPrefixedDigest(
                payload.Source.OptionsDigest) ||
            !IsCanonicalSafeSourceIdentity(
                payload.Source.Identity,
                snapshot.ContentDigest))
        {
            throw PackageError(
                JsonSnapshotPackageRules.PolicyMismatch,
                "The typed JSON package source identity or digests are invalid.");
        }

        JsonTypedSnapshotPackageIntentManifest typed =
            payload.TypedIntent;
        if (typed.ByteLength != header.IntentLength)
        {
            throw PackageError(
                JsonSnapshotPackageRules.IntegrityMismatch,
                "The typed JSON package intent length does not match its header.");
        }
        if (typed.ByteLength <= 0 ||
            typed.ByteLength >
                JsonTypedIntentManifestSerializer
                    .MaximumManifestBytes ||
            !IsCanonicalPrefixedDigest(
                typed.ManifestDigest) ||
            typed.MaxDecodedBinaryBytes is
                < 1 or >
                JsonTypedIntentManifestSerializer
                    .MaximumDecodedBinaryBytes ||
            typed.MaxDecimalDigits is
                < 1 or >
                JsonTypedIntentManifestSerializer
                    .MaximumDecimalDigits ||
            typed.ColumnCount is
                < 1 or >
                JsonTypedIntentManifestSerializer
                    .MaximumColumns)
        {
            throw PackageError(
                JsonSnapshotPackageRules.PolicyMismatch,
                "The typed JSON package intent binding is invalid.");
        }

        ValidateRetainedResourcePolicy(
            payload.Reader,
            payload.Inference);
        if (string.IsNullOrWhiteSpace(
                payload.Catalog.TargetCSharpDbVersion) ||
            !IsCanonicalHexDigest(
                payload.Catalog.Digest))
        {
            throw PackageError(
                JsonSnapshotPackageRules.PolicyMismatch,
                "The typed JSON package catalog binding is invalid.");
        }
    }

    private static void ValidateRetainedResourcePolicy(
        JsonSnapshotPackageReaderManifest reader,
        JsonSnapshotPackageInferenceManifest inference)
    {
        try
        {
            _ = JsonStreamingReaderSettings.Create(
                RestoreReaderOptions(reader));
        }
        catch (ArgumentException exception)
        {
            throw PackageError(
                JsonSnapshotPackageRules.PolicyMismatch,
                "The typed JSON package reader resource policy exceeds supported safety ceilings.",
                exception);
        }

        if (inference.MaxProfileRecords <= 0 ||
            inference.MaxProfileRecords >
                JsonTableSchemaInferenceOptions
                    .MaximumSupportedProfileRecords ||
            (!inference.CollectProfile &&
             inference.MaxProfileRecords != 1) ||
            string.IsNullOrWhiteSpace(
                inference.TableName) ||
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
                inference.MaxColumns)
        {
            throw PackageError(
                JsonSnapshotPackageRules.PolicyMismatch,
                "The typed JSON package inference resource policy exceeds supported safety ceilings.");
        }

        int previousIndex = -1;
        var names =
            new HashSet<string>(StringComparer.Ordinal);
        for (int index = 0;
             index < inference.ColumnOverrides.Count;
             index++)
        {
            JsonSnapshotPackageColumnOverrideManifest? item =
                inference.ColumnOverrides[index];
            if (item is null)
            {
                throw PackageError(
                    JsonSnapshotPackageRules.PolicyMismatch,
                    "The typed JSON package ordinary inference overrides contain a null member.");
            }
            if (item.ColumnIndex <= previousIndex ||
                item.ColumnIndex < 0 ||
                item.ColumnIndex >= inference.MaxColumns ||
                item.ExpectedPropertyName is null ||
                StrictUtf8ByteCount(
                    item.ExpectedPropertyName) >
                    reader.MaxPropertyNameBytes ||
                !names.Add(item.ExpectedPropertyName) ||
                !Enum.IsDefined(item.LogicalType) ||
                !Enum.IsDefined(item.MissingPolicy) ||
                (item.MissingPolicy ==
                     JsonMissingPropertyPolicy.AsNull &&
                 item.Nullable == false))
            {
                throw PackageError(
                    JsonSnapshotPackageRules.PolicyMismatch,
                    "The typed JSON package ordinary inference overrides are invalid.");
            }
            previousIndex = item.ColumnIndex;
        }
    }

    private static void ValidateParsedIntent(
        JsonTypedSnapshotPackageIntentManifest expected,
        JsonTypedIntentManifest actual)
    {
        if (!string.Equals(
                expected.ManifestDigest,
                actual.ManifestDigest,
                StringComparison.Ordinal) ||
            expected.MaxDecodedBinaryBytes !=
                actual.MaxDecodedBinaryBytes ||
            expected.MaxDecimalDigits !=
                actual.MaxDecimalDigits ||
            expected.ColumnCount != actual.Columns.Count)
        {
            throw PackageError(
                JsonSnapshotPackageRules.PolicyMismatch,
                "The embedded typed JSON intent does not match the package manifest binding.");
        }
    }

    private static JsonStreamingReaderOptions
        RestoreReaderOptions(
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
            Consistency =
                new MigrationConsistencyStrategy
                {
                    Kind =
                        MigrationConsistencyKind.Snapshot,
                    Description =
                        JsonSourceBinding
                            .SnapshotConsistencyDescription,
                    Watermark = null,
                },
        };

    private static JsonTableSchemaInferenceRecipe
        RestoreRecipe(
            JsonSnapshotPackageInferenceManifest manifest)
    {
        var options =
            new JsonTableSchemaInferenceOptions
            {
                TableName = manifest.TableName,
                MaxColumns = manifest.MaxColumns,
                MaxTotalColumnNameBytes =
                    manifest.MaxTotalColumnNameBytes,
                MaxProfileBytes =
                    manifest.MaxProfileBytes,
                ColumnOverrides =
                    manifest.ColumnOverrides
                        .Select(
                            item =>
                                new JsonTableColumnSchemaOverride
                                {
                                    ColumnIndex =
                                        item.ColumnIndex,
                                    ExpectedPropertyName =
                                        item
                                            .ExpectedPropertyName,
                                    LogicalType =
                                        item.LogicalType,
                                    Nullable = item.Nullable,
                                    MissingPolicy =
                                        item.MissingPolicy,
                                })
                        .ToArray(),
            };
        return new JsonTableSchemaInferenceRecipe(
            manifest.CollectProfile,
            manifest.MaxProfileRecords,
            options,
            options.ColumnOverrides);
    }

    private static void ValidateSnapshot(
        JsonSnapshotPackageSnapshotManifest expected,
        JsonSourceSnapshot actual)
    {
        if (expected.ContentLength !=
                actual.ContentLength ||
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
                "The retained typed JSON bytes do not match the package snapshot identity.");
        }
    }

    private static JsonTypedSnapshotPackageManifest
        CreatePublicManifest(
            JsonTypedSnapshotPackageManifestPayload payload,
            ReadOnlySpan<byte> manifestHash,
            MigrationSourceIdentity source) =>
        new(
            FormatDigest(manifestHash),
            payload.TypedIntent.ManifestDigest,
            payload.Snapshot.SnapshotIdentity,
            payload.Snapshot.ContentDigest,
            payload.Snapshot.ContentLength,
            source,
            payload.Source.OptionsDigest,
            payload.Catalog.TargetCSharpDbVersion,
            payload.Catalog.Digest);

    private static byte[] CreateHeader(
        int manifestLength,
        int intentLength,
        long snapshotLength,
        ReadOnlySpan<byte> manifestHash,
        ReadOnlySpan<byte> intentHash)
    {
        if (manifestLength <= 0 ||
            manifestLength >
                JsonTypedSnapshotPackageManifestSerializer
                    .MaximumManifestBytes)
        {
            throw new InvalidDataException(
                "The typed JSON package manifest length is invalid.");
        }
        ValidateIntentLength(intentLength);
        if (snapshotLength < 0)
        {
            throw new InvalidDataException(
                "The typed JSON package snapshot length is invalid.");
        }
        if (manifestHash.Length != DigestBytes ||
            intentHash.Length != DigestBytes)
        {
            throw new ArgumentException(
                "Typed JSON package section hashes must be SHA-256.");
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
            header.AsSpan(IntentLengthOffset, 4),
            checked((uint)intentLength));
        BinaryPrimitives.WriteUInt32BigEndian(
            header.AsSpan(FlagsOffset, 4),
            HeaderFlags);
        BinaryPrimitives.WriteUInt64BigEndian(
            header.AsSpan(SnapshotLengthOffset, 8),
            checked((ulong)snapshotLength));
        manifestHash.CopyTo(
            header.AsSpan(
                ManifestHashOffset,
                DigestBytes));
        intentHash.CopyTo(
            header.AsSpan(IntentHashOffset, DigestBytes));
        return header;
    }

    private static async ValueTask<PackageHeader>
        ReadHeaderAsync(
            FileStream package,
            JsonSnapshotPackageOpenOptions settings,
            CancellationToken cancellationToken)
    {
        if (package.Length < HeaderSize)
        {
            throw PackageError(
                JsonSnapshotPackageRules.InvalidFormat,
                "The typed JSON package header is truncated.");
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
                    header.AsSpan(8, 4)) !=
                    HeaderVersion ||
                BinaryPrimitives.ReadUInt32BigEndian(
                    header.AsSpan(12, 4)) !=
                    HeaderSize ||
                BinaryPrimitives.ReadUInt32BigEndian(
                    header.AsSpan(FlagsOffset, 4)) !=
                    HeaderFlags ||
                BinaryPrimitives.ReadUInt32BigEndian(
                    header.AsSpan(ReservedOffset, 4)) != 0 ||
                !IsAllZero(
                    header.AsSpan(
                        ReservedTailOffset,
                        HeaderSize -
                        ReservedTailOffset)))
            {
                throw PackageError(
                    JsonSnapshotPackageRules.InvalidFormat,
                    "The typed JSON package header version, size, flags, or reserved bytes are unsupported.");
            }

            uint manifestLength =
                BinaryPrimitives.ReadUInt32BigEndian(
                    header.AsSpan(
                        ManifestLengthOffset,
                        4));
            uint intentLength =
                BinaryPrimitives.ReadUInt32BigEndian(
                    header.AsSpan(
                        IntentLengthOffset,
                        4));
            ulong unsignedSnapshotLength =
                BinaryPrimitives.ReadUInt64BigEndian(
                    header.AsSpan(
                        SnapshotLengthOffset,
                        8));

            if (manifestLength == 0 ||
                manifestLength >
                    JsonTypedSnapshotPackageManifestSerializer
                        .MaximumManifestBytes ||
                intentLength == 0 ||
                intentLength >
                    JsonTypedIntentManifestSerializer
                        .MaximumManifestBytes)
            {
                throw PackageError(
                    JsonSnapshotPackageRules
                        .SizeLimitExceeded,
                    "The typed JSON package manifest or intent section exceeds its safety bound.");
            }
            if (unsignedSnapshotLength >
                    (ulong)settings.MaxSourceBytes ||
                unsignedSnapshotLength > long.MaxValue)
            {
                throw PackageError(
                    JsonSnapshotPackageRules
                        .SizeLimitExceeded,
                    "The typed JSON package snapshot exceeds the configured byte limit.");
            }

            long snapshotLength =
                (long)unsignedSnapshotLength;
            long exactLength;
            try
            {
                exactLength = checked(
                    HeaderSize +
                    (long)manifestLength +
                    intentLength +
                    snapshotLength);
            }
            catch (OverflowException exception)
            {
                throw PackageError(
                    JsonSnapshotPackageRules
                        .SizeLimitExceeded,
                    "The typed JSON package section lengths overflow the supported range.",
                    exception);
            }

            if (package.Length != exactLength)
            {
                throw PackageError(
                    JsonSnapshotPackageRules.InvalidFormat,
                    "The typed JSON package has truncated, overlapping, or trailing section bytes.");
            }

            return new PackageHeader(
                checked((int)manifestLength),
                checked((int)intentLength),
                snapshotLength,
                header
                    .AsSpan(
                        ManifestHashOffset,
                        DigestBytes)
                    .ToArray(),
                header
                    .AsSpan(
                        IntentHashOffset,
                        DigestBytes)
                    .ToArray());
        }
        finally
        {
            CryptographicOperations.ZeroMemory(header);
        }
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
            await using Stream source =
                snapshot.OpenRead();
            using var hasher =
                IncrementalHash.CreateHash(
                    HashAlgorithmName.SHA256);
            long total = 0;
            while (true)
            {
                cancellationToken
                    .ThrowIfCancellationRequested();
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
                        "The typed JSON snapshot grew while its package was being written.");
                }

                await destination
                    .WriteAsync(
                        buffer.AsMemory(0, read),
                        cancellationToken)
                    .ConfigureAwait(false);
                hasher.AppendData(buffer, 0, read);
                total += read;
            }

            byte[] actualHash = hasher.GetHashAndReset();
            try
            {
                string actualDigest =
                    FormatDigest(actualHash);
                if (total != expectedLength ||
                    !string.Equals(
                        actualDigest,
                        expectedDigest,
                        StringComparison.Ordinal))
                {
                    throw PackageError(
                        JsonSnapshotPackageRules
                            .IntegrityMismatch,
                        "The typed JSON snapshot changed while its package was being written.");
                }
            }
            finally
            {
                CryptographicOperations.ZeroMemory(
                    actualHash);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(
                buffer,
                clearArray: true);
        }
    }

    private static JsonTypedIntentManifestPayload
        PrevalidateIntent(ReadOnlyMemory<byte> intentBytes)
    {
        try
        {
            return JsonTypedIntentManifestSerializer
                .Deserialize(intentBytes);
        }
        catch (
            JsonTypedIntentManifestValidationException
                exception)
        {
            string ruleId = exception.FailureKind switch
            {
                JsonTypedIntentManifestFailureKind
                    .Integrity =>
                    JsonSnapshotPackageRules
                        .IntegrityMismatch,
                JsonTypedIntentManifestFailureKind.Policy =>
                    JsonSnapshotPackageRules.PolicyMismatch,
                JsonTypedIntentManifestFailureKind.Limit =>
                    JsonSnapshotPackageRules
                        .SizeLimitExceeded,
                _ => JsonSnapshotPackageRules.InvalidFormat,
            };
            throw PackageError(
                ruleId,
                "The embedded typed JSON intent is invalid.",
                exception);
        }
        catch (InvalidDataException exception)
        {
            throw PackageError(
                JsonSnapshotPackageRules.InvalidFormat,
                "The embedded typed JSON intent is not a valid canonical manifest.",
                exception);
        }
    }

    private static void ValidatePreparsedIntent(
        JsonTypedSnapshotPackageManifestPayload package,
        JsonTypedIntentManifestPayload intent)
    {
        JsonSnapshotPackageSnapshotManifest snapshot =
            package.Snapshot;
        JsonSnapshotPackageSourceManifest source =
            package.Source;
        JsonTypedSnapshotPackageIntentManifest expected =
            package.TypedIntent;

        if (!string.Equals(
                intent.Source.SnapshotIdentity,
                snapshot.SnapshotIdentity,
                StringComparison.Ordinal) ||
            !string.Equals(
                intent.Source.ContentDigest,
                snapshot.ContentDigest,
                StringComparison.Ordinal) ||
            intent.Source.ContentLength !=
                snapshot.ContentLength ||
            !string.Equals(
                intent.Source.Identity,
                source.Identity,
                StringComparison.Ordinal) ||
            !string.Equals(
                intent.Source.Fingerprint,
                source.Fingerprint,
                StringComparison.Ordinal) ||
            !string.Equals(
                intent.Source.OptionsDigest,
                source.OptionsDigest,
                StringComparison.Ordinal))
        {
            throw PackageError(
                JsonSnapshotPackageRules.IntegrityMismatch,
                "The embedded typed JSON intent belongs to a different retained source.");
        }

        if (intent.Limits.MaxDecodedBinaryBytes !=
                expected.MaxDecodedBinaryBytes ||
            intent.Limits.MaxDecimalDigits !=
                expected.MaxDecimalDigits ||
            intent.Columns.Count != expected.ColumnCount)
        {
            throw PackageError(
                JsonSnapshotPackageRules.PolicyMismatch,
                "The embedded typed JSON intent policy does not match the package manifest.");
        }

        var typedOrdinals =
            intent.Columns
                .Select(item => item.ColumnIndex)
                .ToHashSet();
        if (package.Inference.ColumnOverrides.Any(
                item =>
                    item is not null &&
                    typedOrdinals.Contains(
                        item.ColumnIndex)))
        {
            throw PackageError(
                JsonSnapshotPackageRules.PolicyMismatch,
                "The retained ordinary schema overrides overlap embedded typed-intent ordinals.");
        }
    }

    private static JsonSnapshotPackageException
        MapIntentError(JsonTypedIntentException exception)
    {
        string ruleId = exception.RuleId switch
        {
            JsonTypedIntentRules.IntegrityMismatch or
            JsonTypedIntentRules.SourceMismatch =>
                JsonSnapshotPackageRules.IntegrityMismatch,
            JsonTypedIntentRules.PolicyMismatch =>
                JsonSnapshotPackageRules.PolicyMismatch,
            JsonTypedIntentRules.SizeLimitExceeded =>
                JsonSnapshotPackageRules.SizeLimitExceeded,
            _ => JsonSnapshotPackageRules.InvalidFormat,
        };
        return PackageError(
            ruleId,
            "The embedded typed JSON intent failed package verification.",
            exception);
    }

    private static void VerifySectionHash(
        ReadOnlySpan<byte> bytes,
        ReadOnlySpan<byte> expectedHash,
        string message)
    {
        byte[] actualHash = SHA256.HashData(bytes);
        try
        {
            if (!CryptographicOperations.FixedTimeEquals(
                    expectedHash,
                    actualHash))
            {
                throw PackageError(
                    JsonSnapshotPackageRules
                        .IntegrityMismatch,
                    message);
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(
                actualHash);
        }
    }

    private static void ValidateIntentDigest(
        string expectedDigest,
        ReadOnlySpan<byte> actualHash)
    {
        if (!IsCanonicalPrefixedDigest(expectedDigest))
        {
            throw PackageError(
                JsonSnapshotPackageRules.PolicyMismatch,
                "The typed JSON package intent digest is not canonical SHA-256 text.");
        }

        byte[] expectedHash =
            Convert.FromHexString(
                expectedDigest.AsSpan(7));
        try
        {
            if (!CryptographicOperations.FixedTimeEquals(
                    expectedHash,
                    actualHash))
            {
                throw PackageError(
                    JsonSnapshotPackageRules
                        .IntegrityMismatch,
                    "The typed JSON package manifest binds a different embedded intent.");
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(
                expectedHash);
        }
    }

    private static JsonSnapshotPackageOpenOptions
        ValidateOpenOptions(
            JsonSnapshotPackageOpenOptions options)
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
            string.IsNullOrWhiteSpace(
                options.WorkspacePath))
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

        byte[] expectedBytes =
            Convert.FromHexString(
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
                    "The typed JSON package does not match the trusted manifest digest.");
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(
                expectedBytes);
        }
    }

    private static void ValidateIntentLength(int length)
    {
        if (length is < 1 or >
            JsonTypedIntentManifestSerializer
                .MaximumManifestBytes)
        {
            throw new InvalidDataException(
                "The embedded typed JSON intent length is invalid.");
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
                "The typed JSON package inference policy contains invalid Unicode.",
                exception);
        }
    }

    private static string ValidateDestination(
        string fullPath)
    {
        RejectAlternateDataStream(fullPath);
        string parentPath =
            Path.GetDirectoryName(fullPath)
            ?? throw new ArgumentException(
                "The typed JSON package path must have a parent directory.");
        if (!Directory.Exists(parentPath))
        {
            throw new DirectoryNotFoundException(
                "The typed JSON package parent directory does not exist.");
        }
        FileAttributes parentAttributes =
            File.GetAttributes(parentPath);
        if ((parentAttributes &
             FileAttributes.ReparsePoint) != 0)
        {
            throw PackageError(
                JsonSnapshotPackageRules.UnsafePath,
                "The typed JSON package parent directory cannot be a reparse point.");
        }
        if (PathExists(fullPath))
        {
            throw new IOException(
                "The typed JSON package destination already exists.");
        }
        return parentPath;
    }

    private static void ValidateInputPath(string fullPath) =>
        RejectAlternateDataStream(fullPath);

    private static void ValidateOpenedFile(
        FileStream package)
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
                "The opened typed JSON package handle is not a regular seekable file.");
        }
    }

    private static void RejectAlternateDataStream(
        string fullPath)
    {
        if (!OperatingSystem.IsWindows())
            return;
        string root =
            Path.GetPathRoot(fullPath) ?? string.Empty;
        if (fullPath
            .AsSpan(root.Length)
            .Contains(':'))
        {
            throw PackageError(
                JsonSnapshotPackageRules.UnsafePath,
                "Windows alternate data streams cannot be used as typed JSON packages.");
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

    private static FileStream CreatePackageFile(
        string path)
    {
        var options =
            new FileStreamOptions
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

    private static bool IsCanonicalSafeSourceIdentity(
        string? identity,
        string contentDigest)
    {
        if (string.Equals(
                identity,
                "json-content:" + contentDigest,
                StringComparison.Ordinal))
        {
            return true;
        }

        const string logicalPrefix = "json-logical:";
        return identity is not null &&
            identity.StartsWith(
                logicalPrefix,
                StringComparison.Ordinal) &&
            IsCanonicalPrefixedDigest(
                identity[logicalPrefix.Length..]);
    }

    private static bool IsCanonicalPrefixedDigest(
        string? digest) =>
        digest is not null &&
        digest.Length == 71 &&
        digest.StartsWith(
            "sha256:",
            StringComparison.Ordinal) &&
        IsLowerHex(digest.AsSpan(7));

    private static bool IsCanonicalHexDigest(
        string? digest) =>
        digest is not null &&
        digest.Length == 64 &&
        IsLowerHex(digest);

    private static bool IsLowerHex(
        ReadOnlySpan<char> value)
    {
        foreach (char character in value)
        {
            if (character is not
                (>= '0' and <= '9') and
                not (>= 'a' and <= 'f'))
            {
                return false;
            }
        }
        return true;
    }

    private static bool IsAllZero(
        ReadOnlySpan<byte> value)
    {
        foreach (byte item in value)
        {
            if (item != 0)
                return false;
        }
        return true;
    }

    private static void Zero(byte[]? value)
    {
        if (value is not null)
            CryptographicOperations.ZeroMemory(value);
    }

    private static JsonSnapshotPackageException
        PackageError(
            string ruleId,
            string message) =>
        new(ruleId, message);

    private static JsonSnapshotPackageException
        PackageError(
            string ruleId,
            string message,
            Exception innerException) =>
        new(ruleId, message, innerException);

    private sealed record PackageHeader(
        int ManifestLength,
        int IntentLength,
        long SnapshotLength,
        byte[] ManifestHash,
        byte[] IntentHash)
    {
        internal void Zero()
        {
            CryptographicOperations.ZeroMemory(
                ManifestHash);
            CryptographicOperations.ZeroMemory(
                IntentHash);
        }
    }

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
            CancellationToken cancellationToken =
                default)
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
            // OpenAsync owns the enclosing package stream.
            base.Dispose(disposing);
        }
    }
}
