using System.Security.Cryptography;
using CSharpDB.Storage.Diagnostics;

namespace CSharpDB.Engine;

public static class DatabaseBackupCoordinator
{
    public static async ValueTask<DatabaseBackupResult> BackupAsync(
        Database database,
        string sourcePath,
        string destinationPath,
        bool withManifest,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(sourcePath);
        ArgumentException.ThrowIfNullOrWhiteSpace(destinationPath);

        string normalizedSourcePath = Path.GetFullPath(sourcePath);
        string normalizedDestinationPath = Path.GetFullPath(destinationPath);
        EnsurePathsDiffer(normalizedSourcePath, normalizedDestinationPath, "Backup destination must differ from the current database path.");

        await database.SaveToFileAsync(normalizedDestinationPath, ct);

        var report = await DatabaseInspector.InspectAsync(
            normalizedDestinationPath,
            new DatabaseInspectOptions(),
            ct);
        string sha256 = await ComputeSha256Async(normalizedDestinationPath, ct);
        string? manifestPath = null;

        if (withManifest)
        {
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

        return new DatabaseBackupResult
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
    }

    public static async ValueTask<DatabaseRestoreResult> ValidateRestoreSourceAsync(
        string sourcePath,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sourcePath);
        string normalizedSourcePath = Path.GetFullPath(sourcePath);

        if (!File.Exists(normalizedSourcePath))
            throw new FileNotFoundException("Restore source database file not found.", normalizedSourcePath);

        await using var sourceDb = await Database.LoadIntoMemoryAsync(normalizedSourcePath, ct);
        var dbReport = await DatabaseInspector.InspectAsync(
            normalizedSourcePath,
            new DatabaseInspectOptions(),
            ct);
        var walReport = await WalInspector.InspectAsync(normalizedSourcePath, options: null, ct);

        return new DatabaseRestoreResult
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

    public static async ValueTask<DatabaseRestoreResult> RestoreAsync(
        string sourcePath,
        string destinationPath,
        Func<CancellationToken, ValueTask> releaseDestinationAsync,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sourcePath);
        ArgumentException.ThrowIfNullOrWhiteSpace(destinationPath);
        ArgumentNullException.ThrowIfNull(releaseDestinationAsync);

        string normalizedSourcePath = Path.GetFullPath(sourcePath);
        string normalizedDestinationPath = Path.GetFullPath(destinationPath);
        EnsurePathsDiffer(normalizedSourcePath, normalizedDestinationPath, "Restore source must differ from the current database path.");

        if (!File.Exists(normalizedSourcePath))
            throw new FileNotFoundException("Restore source database file not found.", normalizedSourcePath);

        string destinationDirectory = Path.GetDirectoryName(normalizedDestinationPath) ?? Environment.CurrentDirectory;
        Directory.CreateDirectory(destinationDirectory);

        string stagedSnapshotPath = normalizedDestinationPath + $".restore.{Guid.NewGuid():N}.tmp";
        string backupPath = normalizedDestinationPath + $".restorebak.{Guid.NewGuid():N}.tmp";
        bool deleteBackupOnSuccess = false;

        try
        {
            await using (var sourceDb = await Database.LoadIntoMemoryAsync(normalizedSourcePath, ct))
            {
                await sourceDb.SaveToFileAsync(stagedSnapshotPath, ct);
            }

            var stageReport = await DatabaseInspector.InspectAsync(
                stagedSnapshotPath,
                new DatabaseInspectOptions(),
                ct);
            var walReport = await WalInspector.InspectAsync(normalizedSourcePath, options: null, ct);

            await releaseDestinationAsync(ct);
            deleteBackupOnSuccess = await ReplaceDatabaseFileAsync(
                normalizedDestinationPath,
                stagedSnapshotPath,
                backupPath,
                ct);

            return new DatabaseRestoreResult
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
        }
        finally
        {
            TryDeleteFile(stagedSnapshotPath);
            TryDeleteFile(stagedSnapshotPath + ".wal");

            if (deleteBackupOnSuccess)
            {
                TryDeleteFile(backupPath);
                TryDeleteFile(backupPath + ".wal");
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

    private static ValueTask<bool> ReplaceDatabaseFileAsync(
        string destinationPath,
        string stagedSnapshotPath,
        string backupPath,
        CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        string destinationWalPath = destinationPath + ".wal";
        if (File.Exists(destinationWalPath))
            File.Delete(destinationWalPath);

        bool hadOriginalFile = File.Exists(destinationPath);
        if (hadOriginalFile)
            File.Move(destinationPath, backupPath, overwrite: true);

        try
        {
            File.Move(stagedSnapshotPath, destinationPath, overwrite: true);
            return ValueTask.FromResult(hadOriginalFile);
        }
        catch
        {
            if (hadOriginalFile && File.Exists(backupPath) && !File.Exists(destinationPath))
            {
                try
                {
                    File.Move(backupPath, destinationPath, overwrite: true);
                }
                catch
                {
                    // Keep the backup in place if rollback fails.
                }
            }

            throw;
        }
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
