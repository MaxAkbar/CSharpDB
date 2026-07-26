using System.Collections.ObjectModel;
using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using CSharpDB.Migration;

namespace CSharpDB.Migration.Retained;

/// <summary>
/// A fully verified, privately copied retained package and its bound data
/// source. Disposing the session drains active readers before removing the
/// private copy.
/// </summary>
public sealed class RetainedMigrationPackageSession :
    IAsyncDisposable
{
    private const string PrivatePackageFileName =
        "package.csdbretained";

    private readonly string workspaceRoot;
    private readonly string workspacePath;
    private readonly FileStream packageGuard;
    private readonly object gate = new();
    private Task? disposeTask;

    private RetainedMigrationPackageSession(
        string workspaceRoot,
        string workspacePath,
        FileStream packageGuard,
        MigrationCatalog catalog,
        RetainedMigrationPackageManifest manifest,
        string packageDigest,
        RetainedMigrationDataSource dataSource)
    {
        this.workspaceRoot = workspaceRoot;
        this.workspacePath = workspacePath;
        this.packageGuard = packageGuard;
        Catalog = catalog;
        Manifest = manifest;
        PackageDigest = packageDigest;
        DataSource = dataSource;
    }

    public MigrationCatalog Catalog { get; }

    public RetainedMigrationPackageManifest Manifest { get; }

    public string PackageDigest { get; }

    public RetainedMigrationDataSource DataSource { get; }

    public static async ValueTask<
        RetainedMigrationPackageSession>
        OpenAsync(
        string packagePath,
        RetainedMigrationPackageOpenOptions options,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        ValidateOptions(options);
        string sourcePath =
            ResolvePackagePath(packagePath);
        string workspaceRoot =
            ResolveWorkspace(
                options.WorkspacePath);
        string? privateWorkspace = null;
        FileStream? guard = null;
        RetainedMigrationDataSource? dataSource =
            null;
        try
        {
            privateWorkspace =
                CreatePrivateWorkspace(
                    workspaceRoot);
            string privatePackagePath =
                Path.Combine(
                    privateWorkspace,
                    PrivatePackageFileName);
            CopiedRetainedPackage copied =
                await CopyAndVerifyDigestAsync(
                        sourcePath,
                        privatePackagePath,
                        options,
                        cancellationToken)
                    .ConfigureAwait(false);
            string packageDigest =
                copied.PackageDigest;
            guard = copied.Guard;
            guard.Position = 0;

            ParsedRetainedPackage parsed =
                RetainedMigrationBinaryCodec
                    .ReadAndValidateManifest(
                        guard,
                        guard.Length,
                        options);
            MigrationCatalog catalog =
                DeserializeCatalog(
                    parsed.CatalogJson);
            ValidateBindings(
                parsed,
                catalog);
            VerifySections(
                guard,
                parsed,
                options,
                cancellationToken);

            RetainedMigrationPackageManifest
                manifest = CreateManifest(parsed);
            dataSource =
                new RetainedMigrationDataSource(
                    guard.SafeFileHandle,
                    guard.Length,
                    parsed.BodyOffset,
                    catalog,
                    parsed.SnapshotIdentity,
                    parsed.CatalogDigest,
                    packageDigest,
                    parsed.Tables,
                    options);
            var session =
                new RetainedMigrationPackageSession(
                    workspaceRoot,
                    privateWorkspace,
                    guard,
                    catalog,
                    manifest,
                    packageDigest,
                    dataSource);
            privateWorkspace = null;
            guard = null;
            dataSource = null;
            return session;
        }
        catch (Exception operationFailure)
        {
            Exception? cleanupFailure =
                await CleanupAfterFailureAsync(
                        dataSource,
                        guard,
                        workspaceRoot,
                        privateWorkspace)
                    .ConfigureAwait(false);
            if (cleanupFailure is not null)
            {
                throw new AggregateException(
                    operationFailure,
                    cleanupFailure);
            }
            ExceptionDispatchInfo
                .Capture(operationFailure)
                .Throw();
            throw;
        }
    }

    public ValueTask DisposeAsync()
    {
        lock (gate)
        {
            disposeTask ??=
                DisposeCoreAsync();
            return new ValueTask(disposeTask);
        }
    }

    private async Task DisposeCoreAsync()
    {
        var failures =
            new List<Exception>();
        try
        {
            await DataSource.DisposeAsync()
                .ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            failures.Add(exception);
        }
        try
        {
            await packageGuard.DisposeAsync()
                .ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            failures.Add(exception);
        }
        try
        {
            DeleteWorkspace(
                workspaceRoot,
                workspacePath);
        }
        catch (Exception exception)
        {
            failures.Add(
                new RetainedMigrationPackageException(
                    "The private retained-package workspace could not be removed.",
                    exception));
        }

        if (failures.Count == 0)
        {
            GC.SuppressFinalize(this);
            return;
        }
        if (failures.Count == 1)
        {
            ExceptionDispatchInfo
                .Capture(failures[0])
                .Throw();
        }
        throw new AggregateException(failures);
    }

    private static MigrationCatalog
        DeserializeCatalog(string catalogJson)
    {
        try
        {
            return MigrationArtifactSerializer
                .DeserializeCatalog(
                    catalogJson);
        }
        catch (Exception exception) when (
            exception is ArgumentException or
                InvalidDataException or
                System.Text.Json.JsonException)
        {
            throw new RetainedMigrationPackageException(
                "The retained package catalog is invalid.",
                exception);
        }
    }

    private static void ValidateBindings(
        ParsedRetainedPackage parsed,
        MigrationCatalog catalog)
    {
        string computedCatalogDigest =
            MigrationArtifactSerializer
                .ComputeCatalogDigest(catalog);
        if (!RetainedMigrationBinaryCodec
                .FixedTimeBareDigestEquals(
                    parsed.CatalogDigest,
                    computedCatalogDigest))
        {
            throw new RetainedMigrationPackageException(
                "The retained package catalog digest does not match its embedded catalog.");
        }
        if (catalog.Source.Kind !=
                parsed.SourceKind ||
            !string.Equals(
                catalog.Source.Identity,
                parsed.SourceIdentity,
                StringComparison.Ordinal) ||
            !string.Equals(
                catalog.Source.Fingerprint,
                parsed.SourceFingerprint,
                StringComparison.Ordinal))
        {
            throw new RetainedMigrationPackageException(
                "The retained package source binding does not match its embedded catalog.");
        }
        ValidateNonSecretBindingText(
            parsed.SourceIdentity,
            "source identity");
        ValidateNonSecretBindingText(
            parsed.SourceFingerprint,
            "source fingerprint");
        ValidateNonSecretBindingText(
            parsed.SnapshotIdentity,
            "snapshot identity");

        IReadOnlyDictionary<
            string,
            MigrationCatalogObject> objects =
            catalog.Objects.ToDictionary(
                static item =>
                    item.ObjectId,
                StringComparer.Ordinal);
        foreach (RetainedPackageTableBinding table in
                 parsed.Tables)
        {
            RetainedMigrationTableDescriptor
                descriptor = table.Descriptor;
            if (!objects.TryGetValue(
                    descriptor.SourceObjectId,
                    out MigrationCatalogObject?
                        sourceObject) ||
                sourceObject.Kind is not (
                    MigrationObjectKind.Table or
                    MigrationObjectKind.Collection))
            {
                throw new RetainedMigrationPackageException(
                    "A retained table does not bind to a catalog table or collection.");
            }
            if (descriptor.ColumnObjectIds.Count ==
                    0 ||
                descriptor
                    .OrderingKeyColumnObjectIds
                    .Count == 0)
            {
                throw new RetainedMigrationPackageException(
                    "A retained table has no projection or deterministic ordering key.");
            }
            var columnSet =
                descriptor.ColumnObjectIds
                    .ToHashSet(
                        StringComparer.Ordinal);
            if (descriptor
                .OrderingKeyColumnObjectIds
                .Any(key =>
                    !columnSet.Contains(key)))
            {
                throw new RetainedMigrationPackageException(
                    "A retained ordering key is outside its stored projection.");
            }
            foreach (string columnId in
                     descriptor.ColumnObjectIds)
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
                    throw new RetainedMigrationPackageException(
                        "A retained column does not bind to its catalog table.");
                }
            }
        }
        string computedContentDigest =
            RetainedMigrationBinaryCodec
                .ComputeContentDigest(
                    parsed.Tables);
        if (!RetainedMigrationBinaryCodec
                .FixedTimeDigestEquals(
                    parsed.ContentDigest,
                    computedContentDigest))
        {
            throw new RetainedMigrationPackageException(
                "The retained package content summary is invalid.");
        }
    }

    private static void VerifySections(
        FileStream package,
        ParsedRetainedPackage parsed,
        RetainedMigrationPackageOpenOptions
            options,
        CancellationToken cancellationToken)
    {
        foreach (RetainedPackageTableBinding table in
                 parsed.Tables)
        {
            package.Position = checked(
                parsed.BodyOffset +
                table.RelativeOffset);
            long sectionStart =
                package.Position;
            using IncrementalHash hash =
                RetainedMigrationBinaryCodec
                    .CreateSectionHash();
            for (long rowOrdinal = 0;
                 rowOrdinal < table.RowCount;
                 rowOrdinal++)
            {
                cancellationToken
                    .ThrowIfCancellationRequested();
                _ = RetainedMigrationBinaryCodec
                    .ReadRow(
                        package,
                        rowOrdinal,
                        table.Descriptor
                            .ColumnObjectIds
                            .Count,
                        options.MaxValueBytes,
                        options
                            .MaxStableKeyBytes,
                        options.MaxRowBytes,
                        hash);
            }
            long actualLength =
                package.Position -
                sectionStart;
            if (actualLength !=
                table.SectionLength)
            {
                throw new RetainedMigrationPackageException(
                    "A retained table section length or row count is inconsistent.");
            }
            string actualDigest =
                RetainedMigrationBinaryCodec
                    .FinishDigest(hash);
            if (!RetainedMigrationBinaryCodec
                    .FixedTimeDigestEquals(
                        table.SectionDigest,
                        actualDigest))
            {
                throw new RetainedMigrationPackageException(
                    "A retained table section digest does not match its row content.");
            }
        }
    }

    private static void ValidateNonSecretBindingText(
        string value,
        string fieldName)
    {
        try
        {
            RetainedMigrationBinaryCodec
                .ValidateSafeManifestText(
                    value,
                    fieldName);
        }
        catch (ArgumentException exception)
        {
            throw new RetainedMigrationPackageException(
                $"The retained package {fieldName} is invalid.",
                exception);
        }
        string[] credentialMarkers =
        [
            "password=",
            "pwd=",
            "user id=",
            "uid=",
            "accountkey=",
            "access token",
            "token=",
        ];
        if (credentialMarkers.Any(marker =>
                value.Contains(
                    marker,
                    StringComparison.OrdinalIgnoreCase)))
        {
            throw new RetainedMigrationPackageException(
                $"The retained package {fieldName} appears to contain connection credentials.");
        }
    }

    private static RetainedMigrationPackageManifest
        CreateManifest(
        ParsedRetainedPackage parsed)
    {
        RetainedMigrationPackageTableManifest[]
            tables =
            parsed.Tables.Select(
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
                RetainedMigrationPackageContract
                    .Format,
            CatalogDigest =
                parsed.CatalogDigest,
            SourceKind =
                parsed.SourceKind,
            SourceIdentity =
                parsed.SourceIdentity,
            SourceFingerprint =
                parsed.SourceFingerprint,
            SnapshotIdentity =
                parsed.SnapshotIdentity,
            ContentDigest =
                parsed.ContentDigest,
            Tables =
                Array.AsReadOnly(tables),
        };
    }

    private static async ValueTask<
        CopiedRetainedPackage>
        CopyAndVerifyDigestAsync(
        string sourcePath,
        string destinationPath,
        RetainedMigrationPackageOpenOptions options,
        CancellationToken cancellationToken)
    {
        byte[] expectedDigest =
            RetainedMigrationBinaryCodec
                .ParseDigest(
                    options
                        .ExpectedPackageDigest);
        byte[] buffer =
            new byte[
                options.CopyBufferBytes];
        using IncrementalHash hash =
            IncrementalHash.CreateHash(
                HashAlgorithmName.SHA256);
        FileStream? destination = null;
        bool transferred = false;
        try
        {
            await using FileStream source =
                OpenSource(
                    sourcePath,
                    options.CopyBufferBytes);
            if (source.Length >
                options.MaxPackageBytes)
            {
                throw new RetainedMigrationPackageLimitException(
                    "The retained package exceeds its configured byte bound.");
            }
            destination =
                CreatePrivatePackageFile(
                    destinationPath,
                    options.CopyBufferBytes);
            long copied = 0;
            while (true)
            {
                int read =
                    await source.ReadAsync(
                            buffer,
                            cancellationToken)
                        .ConfigureAwait(false);
                if (read == 0)
                    break;
                copied = checked(copied + read);
                if (copied >
                    options.MaxPackageBytes)
                {
                    throw new RetainedMigrationPackageLimitException(
                        "The retained package exceeds its configured byte bound.");
                }
                hash.AppendData(
                    buffer,
                    0,
                    read);
                await destination.WriteAsync(
                        buffer.AsMemory(0, read),
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            await destination.FlushAsync(
                    cancellationToken)
                .ConfigureAwait(false);
            destination.Flush(flushToDisk: true);
            byte[] actualDigest =
                hash.GetHashAndReset();
            try
            {
                if (!CryptographicOperations
                        .FixedTimeEquals(
                            expectedDigest,
                            actualDigest))
                {
                    throw new RetainedMigrationPackageException(
                        "The retained package does not match its trusted whole-package SHA-256 digest.");
                }
                destination.Position = 0;
                ValidateOpenedFile(destination);
                var copiedPackage =
                    new CopiedRetainedPackage(
                        RetainedMigrationBinaryCodec
                            .FormatDigest(
                                actualDigest),
                        destination);
                transferred = true;
                return copiedPackage;
            }
            finally
            {
                CryptographicOperations
                    .ZeroMemory(actualDigest);
            }
        }
        finally
        {
            try
            {
                if (!transferred &&
                    destination is not null)
                {
                    await destination
                        .DisposeAsync()
                        .ConfigureAwait(false);
                }
            }
            finally
            {
                CryptographicOperations
                    .ZeroMemory(expectedDigest);
                CryptographicOperations
                    .ZeroMemory(buffer);
            }
        }
    }

    private static async ValueTask<Exception?>
        CleanupAfterFailureAsync(
        RetainedMigrationDataSource? dataSource,
        FileStream? guard,
        string workspaceRoot,
        string? workspace)
    {
        var failures =
            new List<Exception>();
        if (dataSource is not null)
        {
            try
            {
                await dataSource.DisposeAsync()
                    .ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                failures.Add(exception);
            }
        }
        if (guard is not null)
        {
            try
            {
                await guard.DisposeAsync()
                    .ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                failures.Add(exception);
            }
        }
        if (workspace is not null)
        {
            try
            {
                DeleteWorkspace(
                    workspaceRoot,
                    workspace);
            }
            catch (Exception exception)
            {
                failures.Add(exception);
            }
        }
        return failures.Count switch
        {
            0 => null,
            1 => failures[0],
            _ => new AggregateException(failures),
        };
    }

    private static void ValidateOptions(
        RetainedMigrationPackageOpenOptions options)
    {
        if (!RetainedMigrationBinaryCodec
                .IsCanonicalDigest(
                    options
                        .ExpectedPackageDigest))
        {
            throw new ArgumentException(
                "The expected retained package digest must be canonical lowercase SHA-256.",
                nameof(options));
        }
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
                "The retained package open bounds are invalid.");
        }
        if (options.WorkspacePath is not null &&
            string.IsNullOrWhiteSpace(
                options.WorkspacePath))
        {
            throw new ArgumentException(
                "The retained package workspace path cannot be blank.",
                nameof(options));
        }
    }

    private static string ResolvePackagePath(
        string path)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            throw new ArgumentException(
                "A retained package path is required.",
                nameof(path));
        }
        string fullPath =
            Path.GetFullPath(path);
        RejectAlternateDataStream(fullPath);
        if (!File.Exists(fullPath))
        {
            throw new FileNotFoundException(
                "The retained package does not exist.");
        }
        FileAttributes attributes =
            File.GetAttributes(fullPath);
        if ((attributes &
                (FileAttributes.Directory |
                 FileAttributes.ReparsePoint |
                 FileAttributes.Device)) != 0)
        {
            throw new RetainedMigrationPackageException(
                "The retained package must be a regular file.");
        }
        return fullPath;
    }

    private static string ResolveWorkspace(
        string? path)
    {
        string fullPath =
            Path.GetFullPath(
                path ?? Path.GetTempPath());
        RejectAlternateDataStream(fullPath);
        if (!Directory.Exists(fullPath))
        {
            throw new DirectoryNotFoundException(
                "The retained package workspace root does not exist.");
        }
        FileAttributes attributes =
            File.GetAttributes(fullPath);
        if ((attributes &
                (FileAttributes.ReparsePoint |
                 FileAttributes.Device)) != 0)
        {
            throw new RetainedMigrationPackageException(
                "The retained package workspace root cannot be a link, reparse point, or device.");
        }
        return fullPath;
    }

    private static string CreatePrivateWorkspace(
        string root)
    {
        string path =
            Path.Combine(
                root,
                "csharpdb-retained-" +
                Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        FileAttributes attributes =
            File.GetAttributes(path);
        if ((attributes &
                (FileAttributes.ReparsePoint |
                 FileAttributes.Device)) != 0)
        {
            throw new RetainedMigrationPackageException(
                "The private retained-package workspace cannot be a link, reparse point, or device.");
        }
        if (!OperatingSystem.IsWindows())
        {
            File.SetUnixFileMode(
                path,
                UnixFileMode.UserRead |
                UnixFileMode.UserWrite |
                UnixFileMode.UserExecute);
        }
        return path;
    }

    private static FileStream OpenSource(
        string path,
        int bufferSize)
    {
        var stream = new FileStream(
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
        ValidateOpenedFile(stream);
        return stream;
    }

    private static void ValidateOpenedFile(
        FileStream stream)
    {
        FileAttributes attributes =
            File.GetAttributes(
                stream.SafeFileHandle);
        if ((attributes &
                (FileAttributes.Directory |
                 FileAttributes.ReparsePoint |
                 FileAttributes.Device)) != 0 ||
            !stream.CanRead ||
            !stream.CanSeek)
        {
            throw new RetainedMigrationPackageException(
                "The retained package handle is not a regular seekable file.");
        }
    }

    private static FileStream
        CreatePrivatePackageFile(
        string path,
        int bufferSize)
    {
        var options =
            new FileStreamOptions
            {
                Mode = FileMode.CreateNew,
                Access = FileAccess.ReadWrite,
                Share = FileShare.None,
                BufferSize = bufferSize,
                Options =
                    FileOptions.RandomAccess |
                    FileOptions.DeleteOnClose |
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

    private static void DeleteWorkspace(
        string workspaceRoot,
        string workspace)
    {
        string fullRoot =
            Path.TrimEndingDirectorySeparator(
                Path.GetFullPath(workspaceRoot));
        string fullPath =
            Path.TrimEndingDirectorySeparator(
                Path.GetFullPath(workspace));
        string name =
            Path.GetFileName(fullPath);
        StringComparison pathComparison =
            OperatingSystem.IsWindows()
                ? StringComparison.OrdinalIgnoreCase
                : StringComparison.Ordinal;
        if (!name.StartsWith(
                "csharpdb-retained-",
                StringComparison.Ordinal) ||
            !string.Equals(
                Path.GetDirectoryName(fullPath),
                fullRoot,
                pathComparison) ||
            !Directory.Exists(fullPath))
        {
            throw new InvalidOperationException(
                "The retained package workspace cleanup target is invalid.");
        }
        FileAttributes workspaceAttributes =
            File.GetAttributes(fullPath);
        if ((workspaceAttributes &
                (FileAttributes.ReparsePoint |
                 FileAttributes.Device)) != 0)
        {
            throw new InvalidOperationException(
                "The retained package workspace cleanup target is a link, reparse point, or device.");
        }

        Directory.Delete(
            fullPath,
            recursive: false);
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

    private sealed record CopiedRetainedPackage(
        string PackageDigest,
        FileStream Guard);
}
