using System.Data;
using System.Data.OleDb;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;

namespace CSharpDB.Migration.Access;

public enum AccessOleDbProvider
{
    Ace16,
    Ace12,
}

public sealed record AccessProviderAvailability
{
    public required AccessOleDbProvider Provider { get; init; }

    public required string ProviderId { get; init; }

    public required Architecture ProcessArchitecture { get; init; }

    public bool IsAvailable { get; init; }

    public required string Reason { get; init; }
}

/// <summary>
/// Probes the process-local ACE OLE DB registration without opening a source
/// file. Provider discovery is intentionally Windows-only and never falls
/// back to Jet 4.0.
/// </summary>
public static class AccessProviderProbe
{
    public static AccessProviderAvailability Check(
        AccessOleDbProvider provider)
    {
        string providerId = AccessProviderIds.Resolve(provider);
        return CheckProviderId(
            provider,
            providerId);
    }

    internal static AccessProviderAvailability
        CheckProviderId(
        AccessOleDbProvider provider,
        string providerId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            providerId);
        if (!OperatingSystem.IsWindows())
        {
            return Unavailable(
                provider,
                providerId,
                "Microsoft Access capture requires Windows and a process-matched ACE OLE DB provider.");
        }

        try
        {
            using DataTable providers =
                new OleDbEnumerator().GetElements();
            bool found = providers.Rows
                .Cast<DataRow>()
                .Select(static row =>
                    Convert.ToString(
                        row["SOURCES_NAME"],
                        System.Globalization.CultureInfo.InvariantCulture))
                .Any(name => string.Equals(
                    name,
                    providerId,
                    StringComparison.OrdinalIgnoreCase));
            return found
                ? new AccessProviderAvailability
                {
                    Provider = provider,
                    ProviderId = providerId,
                    ProcessArchitecture =
                        RuntimeInformation.ProcessArchitecture,
                    IsAvailable = true,
                    Reason =
                        "The ACE provider is registered for this process architecture.",
                }
                : Unavailable(
                    provider,
                    providerId,
                    "The ACE provider is not registered for this process architecture.");
        }
        catch (Exception exception) when (
            exception is OleDbException or
                InvalidOperationException or
                PlatformNotSupportedException)
        {
            return Unavailable(
                provider,
                providerId,
                "ACE provider enumeration failed closed for this process.");
        }
    }

    internal static bool ContainsProvider(
        DataTable providers,
        string providerId)
    {
        ArgumentNullException.ThrowIfNull(providers);
        ArgumentException.ThrowIfNullOrWhiteSpace(providerId);
        if (!providers.Columns.Contains("SOURCES_NAME"))
            return false;
        return providers.Rows
            .Cast<DataRow>()
            .Select(static row =>
                Convert.ToString(
                    row["SOURCES_NAME"],
                    System.Globalization.CultureInfo.InvariantCulture))
            .Any(name => string.Equals(
                name,
                providerId,
                StringComparison.OrdinalIgnoreCase));
    }

    private static AccessProviderAvailability Unavailable(
        AccessOleDbProvider provider,
        string providerId,
        string reason) =>
        new()
        {
            Provider = provider,
            ProviderId = providerId,
            ProcessArchitecture =
                RuntimeInformation.ProcessArchitecture,
            IsAvailable = false,
            Reason = reason,
        };
}

public sealed record AccessSourceOptions
{
    public const int MaximumCommandTimeoutSeconds =
        60 * 60;

    public const long MaximumSourceBytes =
        64L * 1024 * 1024 * 1024;

    public AccessOleDbProvider Provider { get; init; } =
        AccessOleDbProvider.Ace16;

    /// <summary>
    /// Allows a deterministic ACE 12 retry only after ACE 16 is proven absent.
    /// Jet 4.0 is never selected.
    /// </summary>
    public bool AllowAce12Fallback { get; init; }

    public int CommandTimeoutSeconds { get; init; } = 30;

    public long MaxSourceBytes { get; init; } =
        MaximumSourceBytes;

    internal void Validate()
    {
        if (!Enum.IsDefined(Provider))
            throw new ArgumentOutOfRangeException(nameof(Provider));
        if (CommandTimeoutSeconds is < 1 or
            > MaximumCommandTimeoutSeconds)
        {
            throw new ArgumentOutOfRangeException(
                nameof(CommandTimeoutSeconds));
        }
        if (MaxSourceBytes <= 0 ||
            MaxSourceBytes > MaximumSourceBytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(MaxSourceBytes));
        }
    }
}

internal static class AccessProviderIds
{
    internal const string Ace16 =
        "Microsoft.ACE.OLEDB.16.0";
    internal const string Ace12 =
        "Microsoft.ACE.OLEDB.12.0";

    internal static string Resolve(
        AccessOleDbProvider provider) =>
        provider switch
        {
            AccessOleDbProvider.Ace16 => Ace16,
            AccessOleDbProvider.Ace12 => Ace12,
            _ => throw new ArgumentOutOfRangeException(
                nameof(provider)),
        };
}

internal sealed class AccessSourceSession : IAsyncDisposable
{
    private const string AdapterVersion =
        "csharpdb-access-adapter/v1";

    private readonly FileStream sourceGuard;
    private int disposed;

    private AccessSourceSession(
        string sourcePath,
        string sourceContentDigest,
        string providerId,
        string providerVersion,
        FileStream sourceGuard,
        OleDbConnection connection,
        int commandTimeoutSeconds)
    {
        SourcePath = sourcePath;
        SourceContentDigest = sourceContentDigest;
        ProviderId = providerId;
        ProviderVersion = providerVersion;
        this.sourceGuard = sourceGuard;
        Connection = connection;
        CommandTimeoutSeconds = commandTimeoutSeconds;
    }

    internal string SourcePath { get; }

    internal string SourceContentDigest { get; }

    internal string ProviderId { get; }

    internal string ProviderVersion { get; }

    internal OleDbConnection Connection { get; }

    internal int CommandTimeoutSeconds { get; }

    internal static async ValueTask<AccessSourceSession>
        OpenAsync(
        string sourceFilePath,
        AccessSourceOptions options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        if (!OperatingSystem.IsWindows())
        {
            throw new AccessMigrationException(
                AccessMigrationErrorCode.UnsupportedPlatform,
                "Microsoft Access capture requires Windows.");
        }

        string sourcePath =
            AccessSourcePath.ResolveExisting(sourceFilePath);
        long sourceLength = new FileInfo(sourcePath).Length;
        if (sourceLength > options.MaxSourceBytes)
        {
            throw new AccessRetainedCaptureLimitException(
                "The Microsoft Access source exceeds the configured source byte bound.");
        }

        AccessOleDbProvider selected =
            AccessProviderSelector.Select(options);
        string providerId =
            AccessProviderIds.Resolve(selected);
        FileStream? guard = null;
        OleDbConnection? connection = null;
        try
        {
            guard = new FileStream(
                sourcePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read,
                1024 * 1024,
                FileOptions.Asynchronous |
                    FileOptions.SequentialScan);
            string sourceDigest =
                await AccessStableDigest.FileAsync(
                        guard,
                        cancellationToken)
                    .ConfigureAwait(false);
            guard.Position = 0;

            connection = new OleDbConnection(
                BuildConnectionString(
                    sourcePath,
                    providerId));
            await connection.OpenAsync(cancellationToken)
                .ConfigureAwait(false);

            string providerVersion =
                typeof(OleDbConnection).Assembly
                    .GetCustomAttribute<
                        AssemblyInformationalVersionAttribute>()?
                    .InformationalVersion ??
                typeof(OleDbConnection).Assembly
                    .GetName().Version?.ToString() ??
                "unknown";
            var session = new AccessSourceSession(
                sourcePath,
                sourceDigest,
                providerId,
                string.Concat(
                    AdapterVersion,
                    "/System.Data.OleDb/",
                    providerVersion,
                    "/",
                    providerId),
                guard,
                connection,
                options.CommandTimeoutSeconds);
            guard = null;
            connection = null;
            return session;
        }
        catch (AccessMigrationException)
        {
            throw;
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (IOException)
        {
            throw new AccessMigrationException(
                AccessMigrationErrorCode.SourceLeaseUnavailable,
                "The Microsoft Access source could not be held under an exclusive write/delete-denying lease.");
        }
        catch (Exception exception) when (
            exception is OleDbException or
                InvalidOperationException or
                UnauthorizedAccessException)
        {
            throw new AccessMigrationException(
                AccessMigrationErrorCode.InvalidSource,
                "The Microsoft Access source could not be opened read-only with the selected ACE provider.");
        }
        finally
        {
            if (connection is not null)
                await connection.DisposeAsync().ConfigureAwait(false);
            if (guard is not null)
                await guard.DisposeAsync().ConfigureAwait(false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
            return;
        await Connection.DisposeAsync().ConfigureAwait(false);
        await sourceGuard.DisposeAsync().ConfigureAwait(false);
    }

    [SupportedOSPlatform("windows")]
    private static string BuildConnectionString(
        string sourcePath,
        string providerId)
    {
        var builder = new OleDbConnectionStringBuilder
        {
            Provider = providerId,
            DataSource = sourcePath,
            PersistSecurityInfo = false,
        };
        builder["Mode"] = "Share Deny Write";
        return builder.ConnectionString;
    }
}

internal static class AccessProviderSelector
{
    internal static AccessOleDbProvider Select(
        AccessSourceOptions options) =>
        Select(
            options,
            AccessProviderProbe.Check);

    internal static AccessOleDbProvider Select(
        AccessSourceOptions options,
        Func<AccessOleDbProvider,
            AccessProviderAvailability> probe)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(probe);
        options.Validate();
        AccessProviderAvailability preferred =
            probe(options.Provider);
        if (preferred.IsAvailable)
            return options.Provider;
        if (options.Provider == AccessOleDbProvider.Ace16 &&
            options.AllowAce12Fallback)
        {
            AccessProviderAvailability fallback =
                probe(AccessOleDbProvider.Ace12);
            if (fallback.IsAvailable)
                return AccessOleDbProvider.Ace12;
        }

        throw new AccessMigrationException(
            AccessMigrationErrorCode.ProviderUnavailable,
            string.Concat(
                "The selected process-matched ACE OLE DB provider is unavailable. Provider=",
                preferred.ProviderId,
                "; Architecture=",
                preferred.ProcessArchitecture,
                "."));
    }
}

internal static class AccessSourcePath
{
    internal static string ResolveExisting(
        string sourceFilePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(
            sourceFilePath);
        string fullPath = Path.GetFullPath(
            sourceFilePath);
        if (!File.Exists(fullPath))
        {
            throw new FileNotFoundException(
                "The Microsoft Access source file was not found.",
                fullPath);
        }
        string extension = Path.GetExtension(fullPath);
        if (!string.Equals(
                extension,
                ".mdb",
                StringComparison.OrdinalIgnoreCase) &&
            !string.Equals(
                extension,
                ".accdb",
                StringComparison.OrdinalIgnoreCase))
        {
            throw new AccessMigrationException(
                AccessMigrationErrorCode.InvalidSource,
                "Microsoft Access capture accepts only .mdb and .accdb source files.");
        }
        FileAttributes attributes =
            File.GetAttributes(fullPath);
        if ((attributes &
                (FileAttributes.Directory |
                 FileAttributes.ReparsePoint |
                 FileAttributes.Device)) != 0)
        {
            throw new AccessMigrationException(
                AccessMigrationErrorCode.InvalidSource,
                "The Microsoft Access source must be a regular file.");
        }
        return fullPath;
    }
}
