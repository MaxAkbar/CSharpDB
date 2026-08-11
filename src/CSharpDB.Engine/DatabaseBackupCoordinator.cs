using System.Security.Cryptography;
using CSharpDB.Observability;
using CSharpDB.Storage.Diagnostics;

namespace CSharpDB.Engine;

public static class DatabaseBackupCoordinator
{
    public static ValueTask<DatabaseBackupResult> BackupAsync(
        Database database,
        string sourcePath,
        string destinationPath,
        bool withManifest,
        CancellationToken ct = default)
        => BackupCoreAsync(
            database,
            sourcePath,
            destinationPath,
            withManifest,
            observation: null,
            allowStateFallback: true,
            ct);

    internal static ValueTask<DatabaseBackupResult> BackupFromClientAsync(
        Database database,
        string sourcePath,
        string destinationPath,
        bool withManifest,
        MaintenanceObservation? observation,
        CancellationToken ct = default)
        => BackupCoreAsync(
            database,
            sourcePath,
            destinationPath,
            withManifest,
            observation,
            allowStateFallback: false,
            ct);

    private static async ValueTask<DatabaseBackupResult> BackupCoreAsync(
        Database database,
        string sourcePath,
        string destinationPath,
        bool withManifest,
        MaintenanceObservation? observation,
        bool allowStateFallback,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(sourcePath);
        ArgumentException.ThrowIfNullOrWhiteSpace(destinationPath);

        MaintenanceObservation? operation = observation;
        if (operation is null && allowStateFallback)
            operation = StartDirectBackupObservation(database);
        using IDisposable? operationScope = operation?.EnterScope();
        var progressObserver = operation is null
            ? null
            : new BackupProgressObserver(operation);

        try
        {
            string normalizedSourcePath = Path.GetFullPath(sourcePath);
            string normalizedDestinationPath = Path.GetFullPath(destinationPath);
            EnsurePathsDiffer(
                normalizedSourcePath,
                normalizedDestinationPath,
                "Backup destination must differ from the current database path.");
            await database.SaveToFileAsync(
                normalizedDestinationPath,
                writeScopeHeld: false,
                progressObserver,
                ct);

            operation?.SetPhase(MaintenanceOperationPhase.Validating);
            var report = await DatabaseInspector.InspectAsync(
                normalizedDestinationPath,
                new DatabaseInspectOptions(),
                ct);
            operation?.SetPhase(MaintenanceOperationPhase.Hashing);
            string sha256 = await ComputeSha256Async(normalizedDestinationPath, ct);
            string? manifestPath = null;

            if (withManifest)
            {
                operation?.SetPhase(MaintenanceOperationPhase.Staging);
                manifestPath = normalizedDestinationPath + ".manifest.json";
                var manifest = new DatabaseBackupManifest
                {
                    SourceDatabasePath = normalizedSourcePath,
                    BackupDatabasePath = normalizedDestinationPath,
                    CreatedUtc = DateTimeOffset.UtcNow,
                    DatabaseFileBytes = report.Header.FileLengthBytes,
                    PhysicalPageCount = report.Header.PhysicalPageCount,
                    DeclaredPageCount = report.Header.DeclaredPageCount,
                    PageSizeBytes = report.Header.PageSize,
                    ChangeCounter = report.Header.ChangeCounter,
                    WarningCount = CountIssues(report.Issues, InspectSeverity.Warning),
                    ErrorCount = CountIssues(report.Issues, InspectSeverity.Error),
                    Sha256 = sha256,
                };

                string json = System.Text.Json.JsonSerializer.Serialize(
                    manifest,
                    EngineJsonContext.Default.DatabaseBackupManifest);
                await File.WriteAllTextAsync(manifestPath, json, ct);
            }

            var result = new DatabaseBackupResult
            {
                SourcePath = normalizedSourcePath,
                DestinationPath = normalizedDestinationPath,
                ManifestPath = manifestPath,
                DatabaseFileBytes = report.Header.FileLengthBytes,
                PhysicalPageCount = report.Header.PhysicalPageCount,
                DeclaredPageCount = report.Header.DeclaredPageCount,
                ChangeCounter = report.Header.ChangeCounter,
                WarningCount = CountIssues(report.Issues, InspectSeverity.Warning),
                ErrorCount = CountIssues(report.Issues, InspectSeverity.Error),
                Sha256 = sha256,
            };
            operation?.Succeed(
                result.DatabaseFileBytes,
                result.DatabaseFileBytes,
                result.WarningCount,
                result.ErrorCount);
            return result;
        }
        catch (Exception exception)
        {
            operation?.Fail(exception);
            throw;
        }
    }

    public static ValueTask<DatabaseRestoreResult> ValidateRestoreSourceAsync(
        string sourcePath,
        CancellationToken ct = default)
        => ValidateRestoreSourceCoreAsync(
            sourcePath,
            observation: null,
            ct);

    internal static ValueTask<DatabaseRestoreResult>
        ValidateRestoreSourceFromClientAsync(
            string sourcePath,
            MaintenanceObservation? observation,
            CancellationToken ct = default)
        => ValidateRestoreSourceCoreAsync(sourcePath, observation, ct);

    private static async ValueTask<DatabaseRestoreResult>
        ValidateRestoreSourceCoreAsync(
            string sourcePath,
            MaintenanceObservation? observation,
            CancellationToken ct)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(sourcePath);
        using IDisposable? operationScope = observation?.EnterScope();

        try
        {
            string normalizedSourcePath = Path.GetFullPath(sourcePath);
            observation?.SetPhase(MaintenanceOperationPhase.Validating);
            if (!File.Exists(normalizedSourcePath))
            {
                throw new FileNotFoundException(
                    "Restore source database file not found.",
                    normalizedSourcePath);
            }

            DatabaseRestoreResult result;
            await using (Database sourceDb = await Database.LoadIntoMemoryAsync(
                             normalizedSourcePath,
                             ct))
            {
                var dbReport = await DatabaseInspector.InspectAsync(
                    normalizedSourcePath,
                    new DatabaseInspectOptions(),
                    ct);
                var walReport = await WalInspector.InspectAsync(
                    normalizedSourcePath,
                    options: null,
                    ct);

                result = new DatabaseRestoreResult
                {
                    SourcePath = normalizedSourcePath,
                    DestinationPath = null,
                    ValidateOnly = true,
                    DatabaseFileBytes = dbReport.Header.FileLengthBytes,
                    PhysicalPageCount = dbReport.Header.PhysicalPageCount,
                    DeclaredPageCount = dbReport.Header.DeclaredPageCount,
                    ChangeCounter = dbReport.Header.ChangeCounter,
                    SourceWalExists = walReport.Exists,
                    WarningCount = CountIssues(dbReport.Issues, InspectSeverity.Warning) + CountIssues(walReport.Issues, InspectSeverity.Warning),
                    ErrorCount = CountIssues(dbReport.Issues, InspectSeverity.Error) + CountIssues(walReport.Issues, InspectSeverity.Error),
                };
            }
            observation?.Succeed(
                result.DatabaseFileBytes,
                result.DatabaseFileBytes,
                result.WarningCount,
                result.ErrorCount);
            return result;
        }
        catch (Exception exception)
        {
            observation?.Fail(exception);
            throw;
        }
    }

    private static MaintenanceObservation? StartDirectBackupObservation(
        Database database)
    {
        CSharpDbRuntimeDiagnosticsState? runtimeState =
            database.RuntimeDiagnosticsState;
        if (runtimeState?.IsEnabled != true ||
            CSharpDbOperationScope.IsDiagnosticsSuppressed)
        {
            return null;
        }

        try
        {
            CSharpDbOperationContext? parent = CSharpDbOperationScope.Current;
            CSharpDbOperationContext context = parent is null
                ? CSharpDbOperationContext.CreateRoot(
                    CSharpDbOperationClass.Backup,
                    CSharpDbOperationScope.CurrentTransport,
                    runtimeState.DatabaseAlias,
                    CSharpDbOperationScope.CurrentSessionId,
                    timeProvider: runtimeState.TimeProvider)
                : CSharpDbOperationContext.CreateRequest(
                    parent,
                    CSharpDbOperationClass.Backup,
                    runtimeState.TimeProvider);
            MaintenanceRuntimeDiagnostics.MaintenanceRuntimeOperation?
                runtimeOperation = MaintenanceRuntimeDiagnostics
                    .GetOrCreate(runtimeState)
                    ?.TryStart(
                        context,
                        MaintenanceOperationKind.Backup,
                        MaintenanceOperationPhase.AcquiringAccess);
            LifecycleOperation? lifecycleOperation =
                database.StartLifecycleObservabilityExact(
                    CSharpDbLogEvents.BackupCompleted,
                    CSharpDbOperationClass.Backup,
                    context);
            if (runtimeOperation is null && lifecycleOperation is null)
                return null;

            return new MaintenanceObservation(
                context,
                runtimeOperation,
                lifecycleOperation);
        }
        catch
        {
            return null;
        }
    }

    private sealed class BackupProgressObserver(
        MaintenanceObservation observation) : IPagerSaveToFileProgressObserver
    {
        public void OnPhase(PagerSaveToFilePhase phase)
            => observation.SetPhase(phase switch
            {
                PagerSaveToFilePhase.Checkpointing =>
                    MaintenanceOperationPhase.Checkpointing,
                PagerSaveToFilePhase.Copying =>
                    MaintenanceOperationPhase.Copying,
                PagerSaveToFilePhase.Staging =>
                    MaintenanceOperationPhase.Staging,
                _ => throw new ArgumentOutOfRangeException(nameof(phase)),
            });
    }

    public static ValueTask<DatabaseRestoreResult> RestoreAsync(
        string sourcePath,
        string destinationPath,
        Func<CancellationToken, ValueTask> releaseDestinationAsync,
        CancellationToken ct = default)
        => RestoreCoreAsync(
            sourcePath,
            destinationPath,
            releaseDestinationAsync,
            reopenDestinationAsync: null,
            reopenUnchangedDestinationOnPrePonr: false,
            observation: null,
            destinationAlreadyReleased: false,
            ct: ct);

    internal static ValueTask<DatabaseRestoreResult> RestoreFromClientAsync(
        string sourcePath,
        string destinationPath,
        Func<CancellationToken, ValueTask> reopenDestinationAsync,
        bool reopenUnchangedDestinationOnPrePonr,
        MaintenanceObservation? observation,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(reopenDestinationAsync);
        return RestoreCoreAsync(
            sourcePath,
            destinationPath,
            releaseDestinationAsync: null,
            reopenDestinationAsync: reopenDestinationAsync,
            reopenUnchangedDestinationOnPrePonr:
                reopenUnchangedDestinationOnPrePonr,
            observation: observation,
            destinationAlreadyReleased: true,
            ct: ct);
    }

    private static async ValueTask<DatabaseRestoreResult> RestoreCoreAsync(
        string sourcePath,
        string destinationPath,
        Func<CancellationToken, ValueTask>? releaseDestinationAsync,
        Func<CancellationToken, ValueTask>? reopenDestinationAsync,
        bool reopenUnchangedDestinationOnPrePonr,
        MaintenanceObservation? observation,
        bool destinationAlreadyReleased,
        CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sourcePath);
        ArgumentException.ThrowIfNullOrWhiteSpace(destinationPath);
        if (!destinationAlreadyReleased)
            ArgumentNullException.ThrowIfNull(releaseDestinationAsync);

        string normalizedDestinationPath = Path.GetFullPath(destinationPath);
        bool unchangedDestinationExists = File.Exists(normalizedDestinationPath);
        string normalizedSourcePath = string.Empty;
        string destinationDirectory =
            Path.GetDirectoryName(normalizedDestinationPath) ??
            Environment.CurrentDirectory;
        string stagedSnapshotPath = normalizedDestinationPath + $".restore.{Guid.NewGuid():N}.tmp";
        string backupPath = normalizedDestinationPath + $".restorebak.{Guid.NewGuid():N}.tmp";
        string backupWalPath = backupPath + ".wal";
        bool destinationReleased = destinationAlreadyReleased;
        bool crossedPointOfNoReturn = false;
        bool deleteBackupFiles = false;
        RestoreFileReplacementState? replacement = null;
        using IDisposable? operationScope = observation?.EnterScope();

        try
        {
            normalizedSourcePath = Path.GetFullPath(sourcePath);
            EnsurePathsDiffer(
                normalizedSourcePath,
                normalizedDestinationPath,
                "Restore source must differ from the current database path.");

            if (!File.Exists(normalizedSourcePath))
            {
                throw new FileNotFoundException(
                    "Restore source database file not found.",
                    normalizedSourcePath);
            }

            Directory.CreateDirectory(destinationDirectory);
            observation?.SetPhase(MaintenanceOperationPhase.Copying);
            await using (var sourceDb = await Database.LoadIntoMemoryAsync(normalizedSourcePath, ct))
            {
                observation?.SetPhase(MaintenanceOperationPhase.Staging);
                await sourceDb.SaveToFileAsync(stagedSnapshotPath, ct);
            }

            observation?.SetPhase(MaintenanceOperationPhase.Validating);
            var stageReport = await DatabaseInspector.InspectAsync(
                stagedSnapshotPath,
                new DatabaseInspectOptions(),
                ct);
            var walReport = await WalInspector.InspectAsync(normalizedSourcePath, options: null, ct);

            // Complete every potentially fallible result calculation before
            // replacement adoption. Once the reopen callback returns, the
            // destination can be cached and no rollback-triggering work may run.
            var result = new DatabaseRestoreResult
            {
                SourcePath = normalizedSourcePath,
                DestinationPath = normalizedDestinationPath,
                ValidateOnly = false,
                DatabaseFileBytes = stageReport.Header.FileLengthBytes,
                PhysicalPageCount = stageReport.Header.PhysicalPageCount,
                DeclaredPageCount = stageReport.Header.DeclaredPageCount,
                ChangeCounter = stageReport.Header.ChangeCounter,
                SourceWalExists = walReport.Exists,
                WarningCount = CountIssues(stageReport.Issues, InspectSeverity.Warning) + CountIssues(walReport.Issues, InspectSeverity.Warning),
                ErrorCount = CountIssues(stageReport.Issues, InspectSeverity.Error) + CountIssues(walReport.Issues, InspectSeverity.Error),
            };

            if (releaseDestinationAsync is not null)
            {
                await releaseDestinationAsync(ct);
                destinationReleased = true;
                reopenUnchangedDestinationOnPrePonr =
                    unchangedDestinationExists;
            }

            // This is the sole caller-cancellation gate. Moving either live
            // destination file starts the non-cancellable replacement region.
            ct.ThrowIfCancellationRequested();
            replacement = new RestoreFileReplacementState(
                normalizedDestinationPath,
                stagedSnapshotPath,
                backupPath,
                backupWalPath);
            crossedPointOfNoReturn = true;
            observation?.SetPhase(MaintenanceOperationPhase.Replacing);
            ReplaceDatabaseFiles(replacement);

            observation?.SetPhase(MaintenanceOperationPhase.Reopening);
            await ReopenDestinationAsync(
                normalizedDestinationPath,
                reopenDestinationAsync);

            deleteBackupFiles = true;
            observation?.Succeed(
                result.DatabaseFileBytes,
                result.DatabaseFileBytes,
                result.WarningCount,
                result.ErrorCount);
            return result;
        }
        catch (Exception restoreFailure)
        {
            Exception terminalFailure = restoreFailure;
            try
            {
                if (crossedPointOfNoReturn && replacement is not null)
                {
                    if (replacement.HasMutatedDestination)
                        observation?.SetPhase(MaintenanceOperationPhase.RollingBack);

                    RestoreOriginalDatabaseFiles(replacement);
                    if (replacement.HadOriginalDatabase)
                    {
                        observation?.SetPhase(MaintenanceOperationPhase.Reopening);
                        await ReopenDestinationAsync(
                            normalizedDestinationPath,
                            reopenDestinationAsync);
                    }

                    deleteBackupFiles = true;
                }
                else if (destinationReleased &&
                         reopenUnchangedDestinationOnPrePonr &&
                         unchangedDestinationExists)
                {
                    observation?.SetPhase(MaintenanceOperationPhase.Reopening);
                    await ReopenDestinationAsync(
                        normalizedDestinationPath,
                        reopenDestinationAsync);
                }
            }
            catch (Exception recoveryFailure)
            {
                terminalFailure = CreateRestoreRecoveryFailure(
                    restoreFailure,
                    recoveryFailure);
            }

            observation?.Fail(terminalFailure);
            if (ReferenceEquals(terminalFailure, restoreFailure))
                throw;

            throw terminalFailure;
        }
        finally
        {
            TryDeleteFile(stagedSnapshotPath);
            TryDeleteFile(stagedSnapshotPath + ".wal");

            if (deleteBackupFiles)
            {
                TryDeleteFile(backupPath);
                TryDeleteFile(backupWalPath);
            }
        }
    }

    private static async ValueTask<string> ComputeSha256Async(string filePath, CancellationToken ct)
    {
        await using var stream = new FileStream(
            filePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 81920,
            useAsync: true);
        byte[] hash = await SHA256.HashDataAsync(stream, ct);
        return Convert.ToHexString(hash);
    }

    private static void EnsurePathsDiffer(string left, string right, string message)
    {
        StringComparison comparison = OperatingSystem.IsWindows()
            ? StringComparison.OrdinalIgnoreCase
            : StringComparison.Ordinal;
        if (string.Equals(Path.GetFullPath(left), Path.GetFullPath(right), comparison))
            throw new InvalidOperationException(message);
    }

    private static int CountIssues(IReadOnlyCollection<IntegrityIssue> issues, InspectSeverity severity)
        => issues.Count(issue => issue.Severity == severity);

    private static void ReplaceDatabaseFiles(
        RestoreFileReplacementState replacement)
    {
        if (replacement.HadOriginalDatabase)
        {
            File.Move(
                replacement.DestinationPath,
                replacement.BackupPath,
                overwrite: false);
            replacement.DatabaseBackedUp = true;
        }

        if (replacement.HadOriginalWal)
        {
            File.Move(
                replacement.DestinationWalPath,
                replacement.BackupWalPath,
                overwrite: false);
            replacement.WalBackedUp = true;
        }

        File.Move(
            replacement.StagedSnapshotPath,
            replacement.DestinationPath,
            overwrite: false);
        replacement.ReplacementPublished = true;
    }

    private static void RestoreOriginalDatabaseFiles(
        RestoreFileReplacementState replacement)
    {
        if (replacement.ReplacementPublished)
        {
            DeleteFileIfPresent(replacement.DestinationWalPath);
            DeleteFileIfPresent(replacement.DestinationPath);
        }

        if (replacement.DatabaseBackedUp)
        {
            File.Copy(
                replacement.BackupPath,
                replacement.DestinationPath,
                overwrite: false);
        }

        if (replacement.WalBackedUp)
        {
            File.Copy(
                replacement.BackupWalPath,
                replacement.DestinationWalPath,
                overwrite: false);
        }
    }

    private static async ValueTask ReopenDestinationAsync(
        string destinationPath,
        Func<CancellationToken, ValueTask>? reopenDestinationAsync)
    {
        if (reopenDestinationAsync is not null)
        {
            await reopenDestinationAsync(CancellationToken.None);
            return;
        }

        await using Database reopened = await Database.OpenAsync(
            destinationPath,
            CancellationToken.None);
    }

    private static Exception CreateRestoreRecoveryFailure(
        Exception restoreFailure,
        Exception recoveryFailure)
        => new InvalidOperationException(
            "Restore failed and the original destination could not be restored to a usable state.",
            new AggregateException(restoreFailure, recoveryFailure));

    private static void DeleteFileIfPresent(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    private sealed class RestoreFileReplacementState
    {
        internal RestoreFileReplacementState(
            string destinationPath,
            string stagedSnapshotPath,
            string backupPath,
            string backupWalPath)
        {
            DestinationPath = destinationPath;
            DestinationWalPath = destinationPath + ".wal";
            StagedSnapshotPath = stagedSnapshotPath;
            BackupPath = backupPath;
            BackupWalPath = backupWalPath;
            HadOriginalDatabase = File.Exists(destinationPath);
            HadOriginalWal = File.Exists(DestinationWalPath);
        }

        internal string DestinationPath { get; }
        internal string DestinationWalPath { get; }
        internal string StagedSnapshotPath { get; }
        internal string BackupPath { get; }
        internal string BackupWalPath { get; }
        internal bool HadOriginalDatabase { get; }
        internal bool HadOriginalWal { get; }
        internal bool DatabaseBackedUp { get; set; }
        internal bool WalBackedUp { get; set; }
        internal bool ReplacementPublished { get; set; }
        internal bool HasMutatedDestination =>
            DatabaseBackedUp || WalBackedUp || ReplacementPublished;
    }

    private static void TryDeleteFile(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch
        {
            // Best-effort cleanup for temporary backup/restore files.
        }
    }
}

internal sealed class DatabaseBackupManifest
{
    public string SchemaVersion { get; init; } = "1.0";
    public required string SourceDatabasePath { get; init; }
    public required string BackupDatabasePath { get; init; }
    public required DateTimeOffset CreatedUtc { get; init; }
    public long DatabaseFileBytes { get; init; }
    public int PhysicalPageCount { get; init; }
    public uint DeclaredPageCount { get; init; }
    public int PageSizeBytes { get; init; }
    public uint ChangeCounter { get; init; }
    public int WarningCount { get; init; }
    public int ErrorCount { get; init; }
    public required string Sha256 { get; init; }
}

public sealed class DatabaseBackupResult
{
    public required string SourcePath { get; init; }
    public required string DestinationPath { get; init; }
    public string? ManifestPath { get; init; }
    public long DatabaseFileBytes { get; init; }
    public int PhysicalPageCount { get; init; }
    public uint DeclaredPageCount { get; init; }
    public uint ChangeCounter { get; init; }
    public int WarningCount { get; init; }
    public int ErrorCount { get; init; }
    public required string Sha256 { get; init; }
}

public sealed class DatabaseRestoreResult
{
    public required string SourcePath { get; init; }
    public string? DestinationPath { get; init; }
    public bool ValidateOnly { get; init; }
    public long DatabaseFileBytes { get; init; }
    public int PhysicalPageCount { get; init; }
    public uint DeclaredPageCount { get; init; }
    public uint ChangeCounter { get; init; }
    public bool SourceWalExists { get; init; }
    public int WarningCount { get; init; }
    public int ErrorCount { get; init; }
}
