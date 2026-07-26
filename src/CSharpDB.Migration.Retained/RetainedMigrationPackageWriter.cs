using System.Collections.ObjectModel;
using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Retained;

public static class RetainedMigrationPackageWriter
{
    public static ValueTask<RetainedMigrationPackageWriteResult>
        WriteAsync(
        RetainedMigrationPackageWriteRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(request.Catalog);
        return WriteCoreAsync(
            request.OutputPath,
            request.Tables,
            request.Options,
            (_, _) => ValueTask.FromResult(
                new RetainedMigrationCatalogBinding
                {
                    Catalog = request.Catalog,
                    SnapshotIdentity =
                        request.SnapshotIdentity,
                }),
            cancellationToken);
    }

    public static ValueTask<RetainedMigrationPackageWriteResult>
        WriteAsync(
        RetainedMigrationPackageCaptureRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(
            request.CatalogFactory);
        return WriteCoreAsync(
            request.OutputPath,
            request.Tables,
            request.Options,
            request.CatalogFactory,
            cancellationToken);
    }

    private static async ValueTask<
        RetainedMigrationPackageWriteResult>
        WriteCoreAsync(
        string outputPath,
        IReadOnlyList<RetainedMigrationTableWrite>
            tableWrites,
        RetainedMigrationPackageWriteOptions options,
        Func<
            RetainedMigrationContentSummary,
            CancellationToken,
            ValueTask<RetainedMigrationCatalogBinding>>
            catalogFactory,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(tableWrites);
        ArgumentNullException.ThrowIfNull(options);
        ValidateOptions(options);
        string fullOutputPath =
            ResolveOutputPath(outputPath);
        IReadOnlyList<FrozenTableWrite> tables =
            FreezeAndOrderTables(
                tableWrites,
                options);

        string directory =
            Path.GetDirectoryName(fullOutputPath) ??
            throw new ArgumentException(
                "The retained package output path has no parent directory.",
                nameof(outputPath));
        string fileName =
            Path.GetFileName(fullOutputPath);
        string nonce =
            Guid.NewGuid().ToString("N");
        string bodyPath =
            Path.Combine(
                directory,
                $".{fileName}.{nonce}.rows.tmp");
        string assemblyPath =
            Path.Combine(
                directory,
                $".{fileName}.{nonce}.package.tmp");

        ExceptionDispatchInfo? operationFailure =
            null;
        try
        {
            IReadOnlyList<
                RetainedPackageTableBinding>
                bindings =
                await WriteBodyAsync(
                        bodyPath,
                        tables,
                        options,
                        cancellationToken)
                    .ConfigureAwait(false);
            RetainedMigrationContentSummary summary =
                CreateContentSummary(bindings);
            RetainedMigrationCatalogBinding
                catalogBinding =
                await catalogFactory(
                        summary,
                        cancellationToken)
                    .ConfigureAwait(false) ??
                throw new InvalidOperationException(
                    "The retained package catalog factory returned no binding.");
            ValidateCatalogBinding(
                catalogBinding,
                bindings,
                summary);

            string catalogJson =
                MigrationArtifactSerializer
                    .SerializeCatalog(
                        catalogBinding.Catalog,
                        writeIndented: false);
            int catalogBytes =
                RetainedMigrationBinaryCodec
                    .GetUtf8ByteCount(
                        catalogJson,
                        "catalog");
            if (catalogBytes >
                options.MaxCatalogBytes)
            {
                throw new RetainedMigrationPackageLimitException(
                    "The retained migration catalog exceeds its configured byte bound.");
            }
            string catalogDigest =
                MigrationArtifactSerializer
                    .ComputeCatalogDigest(
                        catalogBinding.Catalog);
            byte[] manifestBytes =
                RetainedMigrationBinaryCodec
                    .BuildManifest(
                        catalogBinding.Catalog,
                        catalogJson,
                        catalogDigest,
                        catalogBinding
                            .SnapshotIdentity,
                        summary.ContentDigest,
                        bindings,
                        options.MaxManifestBytes);
            if (manifestBytes.Length >
                options.MaxManifestBytes)
            {
                throw new RetainedMigrationPackageLimitException(
                    "The retained package manifest exceeds its configured byte bound.");
            }

            long bodyLength =
                new FileInfo(bodyPath).Length;
            long finalLength = checked(
                RetainedMigrationBinaryCodec
                    .HeaderBytes +
                (long)manifestBytes.Length +
                bodyLength);
            if (finalLength >
                options.MaxPackageBytes)
            {
                throw new RetainedMigrationPackageLimitException(
                    "The retained package exceeds its configured byte bound.");
            }

            await AssembleAsync(
                    bodyPath,
                    assemblyPath,
                    manifestBytes,
                    options,
                    cancellationToken)
                .ConfigureAwait(false);
            string packageDigest =
                await ComputeFileDigestAsync(
                        assemblyPath,
                        options.CopyBufferBytes,
                        cancellationToken)
                    .ConfigureAwait(false);

            RetainedMigrationPackageManifest
                manifest =
                CreateManifest(
                    catalogBinding,
                    catalogDigest,
                    summary.ContentDigest,
                    bindings);
            var rowCounts =
                new ReadOnlyDictionary<string, long>(
                    bindings.ToDictionary(
                        static item =>
                            item.Descriptor
                                .SourceObjectId,
                        static item =>
                            item.RowCount,
                        StringComparer.Ordinal));
            var completedResult =
                new RetainedMigrationPackageWriteResult
                {
                    Manifest = manifest,
                    PackageDigest =
                    packageDigest,
                    ContentSummary = summary,
                    RowCounts = rowCounts,
                };

            DeleteTemporaryFileBeforePublish(
                bodyPath);
            File.Move(
                assemblyPath,
                fullOutputPath,
                overwrite: false);
            return completedResult;
        }
        catch (OperationCanceledException exception)
        {
            operationFailure =
                ExceptionDispatchInfo.Capture(
                    exception);
        }
        catch (RetainedMigrationPackageException
               exception)
        {
            operationFailure =
                ExceptionDispatchInfo.Capture(
                    exception);
        }
        catch (Exception exception) when (
            exception is IOException or
                UnauthorizedAccessException)
        {
            operationFailure =
                ExceptionDispatchInfo.Capture(
                    new RetainedMigrationPackageException(
                        "The retained migration package could not be created.",
                        exception));
        }
        catch (Exception exception)
        {
            operationFailure =
                ExceptionDispatchInfo.Capture(
                    exception);
        }

        Exception? cleanupFailure =
            CleanupTemporaryFiles(
                bodyPath,
                assemblyPath);
        if (operationFailure is not null)
        {
            if (cleanupFailure is not null)
            {
                throw new AggregateException(
                    operationFailure.SourceException,
                    cleanupFailure);
            }
            operationFailure.Throw();
        }
        if (cleanupFailure is not null)
            throw cleanupFailure;
        throw new InvalidOperationException(
            "The retained package writer failed without an exception.");
    }

    private static async ValueTask<IReadOnlyList<
        RetainedPackageTableBinding>>
        WriteBodyAsync(
        string bodyPath,
        IReadOnlyList<FrozenTableWrite> tables,
        RetainedMigrationPackageWriteOptions options,
        CancellationToken cancellationToken)
    {
        var bindings =
            new List<RetainedPackageTableBinding>(
                tables.Count);
        await using FileStream body =
            CreatePrivateFile(
                bodyPath,
                options.CopyBufferBytes);
        foreach (FrozenTableWrite table in tables)
        {
            cancellationToken
                .ThrowIfCancellationRequested();
            long sectionStart =
                body.Position;
            long rowCount = 0;
            using IncrementalHash sectionHash =
                RetainedMigrationBinaryCodec
                    .CreateSectionHash();

            await foreach (
                MigrationDataRow row in
                table.Rows.WithCancellation(
                    cancellationToken)
                    .ConfigureAwait(false))
            {
                if (rowCount >=
                    options.MaxRowsPerTable)
                {
                    throw new RetainedMigrationPackageLimitException(
                        $"Retained table '{table.Descriptor.SourceObjectId}' exceeds its configured row-count bound.");
                }
                long payloadLength =
                    RetainedMigrationBinaryCodec
                        .MeasureRowPayload(
                            row,
                            table.Descriptor
                                .ColumnObjectIds
                                .Count,
                            options.MaxValueBytes,
                            options
                                .MaxStableKeyBytes,
                            options.MaxRowBytes);
                if (payloadLength >
                    int.MaxValue)
                {
                    throw new ArgumentException(
                        "A retained row cannot be represented by the package format.",
                        nameof(table));
                }
                long encodedLength = checked(
                    RetainedMigrationBinaryCodec
                        .RowHeaderBytes +
                    payloadLength);
                if (body.Position >
                        options.MaxPackageBytes -
                        encodedLength)
                {
                    throw new RetainedMigrationPackageLimitException(
                        "The retained package exceeds its configured byte bound.");
                }
                RetainedMigrationBinaryCodec
                    .WriteRowRecord(
                        body,
                        sectionHash,
                        rowCount,
                        row,
                        checked((int)payloadLength));
                rowCount++;
            }

            long sectionLength =
                body.Position - sectionStart;
            string sectionDigest =
                RetainedMigrationBinaryCodec
                    .FinishDigest(sectionHash);
            bindings.Add(
                new RetainedPackageTableBinding(
                    table.Descriptor,
                    rowCount,
                    sectionStart,
                    sectionLength,
                    sectionDigest));
        }

        await body.FlushAsync(
                cancellationToken)
            .ConfigureAwait(false);
        body.Flush(flushToDisk: true);
        return bindings.AsReadOnly();
    }

    private static async ValueTask AssembleAsync(
        string bodyPath,
        string assemblyPath,
        byte[] manifestBytes,
        RetainedMigrationPackageWriteOptions options,
        CancellationToken cancellationToken)
    {
        await using FileStream destination =
            CreatePrivateFile(
                assemblyPath,
                options.CopyBufferBytes);
        RetainedMigrationBinaryCodec
            .WriteHeader(
                destination,
                manifestBytes.Length);
        await destination.WriteAsync(
                manifestBytes,
                cancellationToken)
            .ConfigureAwait(false);
        await using var body =
            new FileStream(
                bodyPath,
                new FileStreamOptions
                {
                    Mode = FileMode.Open,
                    Access = FileAccess.Read,
                    Share = FileShare.Read,
                    BufferSize =
                        options.CopyBufferBytes,
                    Options =
                        FileOptions.Asynchronous |
                        FileOptions.SequentialScan,
                });
        await body.CopyToAsync(
                destination,
                options.CopyBufferBytes,
                cancellationToken)
            .ConfigureAwait(false);
        await destination.FlushAsync(
                cancellationToken)
            .ConfigureAwait(false);
        destination.Flush(flushToDisk: true);
    }

    private static async ValueTask<string>
        ComputeFileDigestAsync(
        string path,
        int bufferSize,
        CancellationToken cancellationToken)
    {
        await using var stream =
            new FileStream(
                path,
                new FileStreamOptions
                {
                    Mode = FileMode.Open,
                    Access = FileAccess.Read,
                    Share = FileShare.Read,
                    BufferSize = bufferSize,
                    Options =
                        FileOptions.Asynchronous |
                        FileOptions.SequentialScan,
                });
        using IncrementalHash hash =
            IncrementalHash.CreateHash(
                HashAlgorithmName.SHA256);
        byte[] buffer =
            new byte[bufferSize];
        while (true)
        {
            int read =
                await stream.ReadAsync(
                        buffer,
                        cancellationToken)
                    .ConfigureAwait(false);
            if (read == 0)
                break;
            hash.AppendData(buffer, 0, read);
        }
        CryptographicOperations
            .ZeroMemory(buffer);
        return RetainedMigrationBinaryCodec
            .FinishDigest(hash);
    }

    private static RetainedMigrationContentSummary
        CreateContentSummary(
        IReadOnlyList<RetainedPackageTableBinding>
            bindings)
    {
        RetainedMigrationContentTableSummary[] tables =
            bindings.Select(
                    static item =>
                        new RetainedMigrationContentTableSummary
                        {
                            Descriptor =
                                item.Descriptor,
                            RowCount =
                                item.RowCount,
                            SectionDigest =
                                item.SectionDigest,
                        })
                .ToArray();
        return new RetainedMigrationContentSummary
        {
            DigestAlgorithm =
                RetainedMigrationPackageContract
                    .ContentDigestAlgorithm,
            ContentDigest =
                RetainedMigrationBinaryCodec
                    .ComputeContentDigest(
                        bindings),
            Tables = Array.AsReadOnly(tables),
        };
    }

    private static RetainedMigrationPackageManifest
        CreateManifest(
        RetainedMigrationCatalogBinding binding,
        string catalogDigest,
        string contentDigest,
        IReadOnlyList<RetainedPackageTableBinding>
            tables)
    {
        RetainedMigrationPackageTableManifest[]
            manifests =
            tables.Select(
                    static table =>
                        new RetainedMigrationPackageTableManifest
                        {
                            Descriptor =
                                table.Descriptor,
                            RowCount =
                                table.RowCount,
                            SectionLength =
                                table.SectionLength,
                            SectionDigest =
                                table.SectionDigest,
                        })
                .ToArray();
        return new RetainedMigrationPackageManifest
        {
            Format =
                RetainedMigrationPackageContract.Format,
            CatalogDigest = catalogDigest,
            SourceKind =
                binding.Catalog.Source.Kind,
            SourceIdentity =
                binding.Catalog.Source.Identity,
            SourceFingerprint =
                binding.Catalog.Source.Fingerprint,
            SnapshotIdentity =
                binding.SnapshotIdentity,
            ContentDigest = contentDigest,
            Tables =
                Array.AsReadOnly(manifests),
        };
    }

    private static IReadOnlyList<FrozenTableWrite>
        FreezeAndOrderTables(
        IReadOnlyList<RetainedMigrationTableWrite>
            tables,
        RetainedMigrationPackageWriteOptions options)
    {
        if (tables.Count >
            options.MaxTables)
        {
            throw new RetainedMigrationPackageLimitException(
                "The retained package table count exceeds its configured bound.");
        }

        var frozen =
            new List<FrozenTableWrite>(
                tables.Count);
        var objectIds =
            new HashSet<string>(
                StringComparer.Ordinal);
        foreach (RetainedMigrationTableWrite table in
                 tables)
        {
            if (table is null ||
                table.Descriptor is null ||
                table.Rows is null)
            {
                throw new ArgumentException(
                    "Retained package table writes cannot contain null members.",
                    nameof(tables));
            }
            RetainedMigrationTableDescriptor descriptor =
                FreezeDescriptor(
                    table.Descriptor,
                    options);
            if (!objectIds.Add(
                    descriptor.SourceObjectId))
            {
                throw new ArgumentException(
                    "The retained package repeats a table source object id.",
                    nameof(tables));
            }
            frozen.Add(
                new FrozenTableWrite(
                    descriptor,
                    table.Rows));
        }
        frozen.Sort(
            static (left, right) =>
                StringComparer.Ordinal.Compare(
                    left.Descriptor
                        .SourceObjectId,
                    right.Descriptor
                        .SourceObjectId));
        return frozen.AsReadOnly();
    }

    private static RetainedMigrationTableDescriptor
        FreezeDescriptor(
        RetainedMigrationTableDescriptor descriptor,
        RetainedMigrationPackageWriteOptions options)
    {
        ValidateIdentifier(
            descriptor.SourceObjectId,
            "source object id");
        string[] columns =
            FreezeIdentifierList(
                descriptor.ColumnObjectIds,
                options.MaxColumnsPerTable,
                "column object ids");
        string[] orderingKeys =
            FreezeIdentifierList(
                descriptor
                    .OrderingKeyColumnObjectIds,
                options.MaxColumnsPerTable,
                "ordering key column object ids");
        if (columns.Length == 0 ||
            orderingKeys.Length == 0)
        {
            throw new ArgumentException(
                "A retained table requires at least one column and one deterministic ordering-key column.",
                nameof(descriptor));
        }
        var columnSet =
            columns.ToHashSet(
                StringComparer.Ordinal);
        if (orderingKeys.Any(
                key => !columnSet.Contains(key)))
        {
            throw new ArgumentException(
                "Every retained ordering-key column must be part of the stored projection.",
                nameof(descriptor));
        }

        return new RetainedMigrationTableDescriptor
        {
            SourceObjectId =
                descriptor.SourceObjectId,
            ColumnObjectIds =
                Array.AsReadOnly(columns),
            OrderingKeyColumnObjectIds =
                Array.AsReadOnly(orderingKeys),
        };
    }

    private static string[] FreezeIdentifierList(
        IReadOnlyList<string> values,
        int maximumCount,
        string fieldName)
    {
        if (values is null ||
            values.Count > maximumCount)
        {
            throw new RetainedMigrationPackageLimitException(
                $"The retained {fieldName} count exceeds its configured bound.");
        }
        var seen =
            new HashSet<string>(
                StringComparer.Ordinal);
        var frozen =
            new string[values.Count];
        for (int index = 0;
             index < values.Count;
             index++)
        {
            string value = values[index];
            ValidateIdentifier(
                value,
                fieldName);
            if (!seen.Add(value))
            {
                throw new ArgumentException(
                    $"The retained {fieldName} contain a duplicate.");
            }
            frozen[index] = value;
        }
        return frozen;
    }

    private static void ValidateCatalogBinding(
        RetainedMigrationCatalogBinding binding,
        IReadOnlyList<RetainedPackageTableBinding>
            tables,
        RetainedMigrationContentSummary summary)
    {
        ArgumentNullException.ThrowIfNull(binding);
        ArgumentNullException.ThrowIfNull(
            binding.Catalog);
        MigrationContractValidator
            .ValidateCatalog(
                binding.Catalog);
        RetainedMigrationBinaryCodec
            .ValidateSafeManifestText(
                binding.SnapshotIdentity,
                "snapshot identity");
        RetainedMigrationBinaryCodec
            .ValidateSafeManifestText(
                binding.Catalog.Source.Identity,
                "source identity");
        RetainedMigrationBinaryCodec
            .ValidateSafeManifestText(
                binding.Catalog.Source.Fingerprint,
                "source fingerprint");
        RejectCredentialLikeText(
            binding.Catalog.Source.Identity,
            "source identity");
        RejectCredentialLikeText(
            binding.SnapshotIdentity,
            "snapshot identity");

        IReadOnlyDictionary<
            string,
            MigrationCatalogObject> objects =
            binding.Catalog.Objects
                .ToDictionary(
                    static item =>
                        item.ObjectId,
                    StringComparer.Ordinal);
        foreach (RetainedPackageTableBinding table in
                 tables)
        {
            if (!objects.TryGetValue(
                    table.Descriptor
                        .SourceObjectId,
                    out MigrationCatalogObject?
                        sourceObject) ||
                sourceObject.Kind is not (
                    MigrationObjectKind.Table or
                    MigrationObjectKind.Collection))
            {
                throw new ArgumentException(
                    "A retained table descriptor does not identify a catalog table or collection.",
                    nameof(binding));
            }
            foreach (string columnId in
                     table.Descriptor
                         .ColumnObjectIds)
            {
                if (!objects.TryGetValue(
                        columnId,
                        out MigrationCatalogObject?
                            column) ||
                    column.Kind !=
                        MigrationObjectKind.Column ||
                    !string.Equals(
                        column.ParentObjectId,
                        sourceObject.ObjectId,
                        StringComparison.Ordinal))
                {
                    throw new ArgumentException(
                        "A retained column descriptor does not identify a column owned by its catalog table.",
                        nameof(binding));
                }
            }
        }
        if (!RetainedMigrationBinaryCodec
                .FixedTimeDigestEquals(
                    summary.ContentDigest,
                    RetainedMigrationBinaryCodec
                        .ComputeContentDigest(
                            tables)))
        {
            throw new InvalidOperationException(
                "The retained content summary changed before catalog binding.");
        }
    }

    private static void RejectCredentialLikeText(
        string value,
        string fieldName)
    {
        string[] markers =
        [
            "password=",
            "pwd=",
            "user id=",
            "uid=",
            "accountkey=",
            "access token",
            "token=",
        ];
        if (markers.Any(marker =>
                value.Contains(
                    marker,
                    StringComparison.OrdinalIgnoreCase)))
        {
            throw new ArgumentException(
                $"The retained {fieldName} appears to contain connection credentials.");
        }
    }

    private static void ValidateIdentifier(
        string value,
        string fieldName) =>
        RetainedMigrationBinaryCodec
            .ValidateSafeManifestText(
                value,
                fieldName);

    private static void ValidateOptions(
        RetainedMigrationPackageWriteOptions options)
    {
        if (options.MaxPackageBytes <=
                RetainedMigrationBinaryCodec
                    .HeaderBytes ||
            options.MaxCatalogBytes <= 0 ||
            options.MaxManifestBytes <= 0 ||
            options.MaxCatalogBytes >
                options.MaxManifestBytes ||
            options.MaxTables < 0 ||
            options.MaxColumnsPerTable <= 0 ||
            options.MaxRowsPerTable < 0 ||
            options.MaxValueBytes < 0 ||
            options.MaxRowBytes <
                1 + sizeof(int) ||
            options.MaxStableKeyBytes < 0 ||
            options.MaxValueBytes >
                options.MaxRowBytes ||
            options.CopyBufferBytes is
                < 4096 or
                > 16 * 1024 * 1024)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "The retained package write bounds are invalid.");
        }
    }

    private static string ResolveOutputPath(
        string path)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            throw new ArgumentException(
                "A retained package output path is required.",
                nameof(path));
        }
        string fullPath =
            Path.GetFullPath(path);
        RejectAlternateDataStream(fullPath);
        string? directory =
            Path.GetDirectoryName(fullPath);
        if (directory is null ||
            !Directory.Exists(directory))
        {
            throw new DirectoryNotFoundException(
                "The retained package output directory does not exist.");
        }
        FileAttributes attributes =
            File.GetAttributes(directory);
        if ((attributes &
                (FileAttributes.ReparsePoint |
                 FileAttributes.Device)) != 0)
        {
            throw new ArgumentException(
                "The retained package output directory cannot be a link, reparse point, or device.",
                nameof(path));
        }
        if (File.Exists(fullPath) ||
            Directory.Exists(fullPath))
        {
            throw new IOException(
                "The retained package output already exists.");
        }
        return fullPath;
    }

    private static FileStream CreatePrivateFile(
        string path,
        int bufferSize)
    {
        var options =
            new FileStreamOptions
            {
                Mode = FileMode.CreateNew,
                Access = FileAccess.Write,
                Share = FileShare.None,
                BufferSize = bufferSize,
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

    private static void
        DeleteTemporaryFileBeforePublish(
        string path)
    {
        try
        {
            File.Delete(path);
        }
        catch (Exception exception) when (
            exception is IOException or
                UnauthorizedAccessException)
        {
            throw new RetainedMigrationPackageException(
                "The retained migration package plaintext staging file could not be removed before publication.",
                exception);
        }
    }

    private static Exception? CleanupTemporaryFiles(
        params string[] paths)
    {
        var failures =
            new List<Exception>();
        foreach (string path in paths)
        {
            try
            {
                File.Delete(path);
            }
            catch (Exception exception) when (
                exception is IOException or
                    UnauthorizedAccessException)
            {
                failures.Add(
                    new RetainedMigrationPackageException(
                        "A retained migration package plaintext temporary file could not be removed.",
                        exception));
            }
        }
        return failures.Count switch
        {
            0 => null,
            1 => failures[0],
            _ => new AggregateException(failures),
        };
    }

    private static void RejectAlternateDataStream(
        string fullPath)
    {
        if (!OperatingSystem.IsWindows())
            return;
        string root =
            Path.GetPathRoot(fullPath) ??
            string.Empty;
        if (fullPath.AsSpan(root.Length)
            .Contains(':'))
        {
            throw new ArgumentException(
                "Windows alternate data streams cannot be used for retained packages.");
        }
    }

    private sealed record FrozenTableWrite(
        RetainedMigrationTableDescriptor Descriptor,
        IAsyncEnumerable<MigrationDataRow> Rows);
}
