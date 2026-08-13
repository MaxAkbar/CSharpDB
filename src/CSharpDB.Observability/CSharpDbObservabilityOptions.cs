using System.Collections.ObjectModel;

namespace CSharpDB.Observability;

public sealed class CSharpDbObservabilityOptions
{
    public const string ConfigurationSectionName = "CSharpDB:Observability";
    public const int MaximumHistoryCapacity = 10_000;
    public const int MaximumActiveOperationCapacity = 10_000;
    public static readonly TimeSpan MaximumRetention = TimeSpan.FromDays(7);
    public static readonly TimeSpan MaximumThreshold = TimeSpan.FromDays(1);

    public bool Enabled { get; set; }
    public string DatabaseAlias { get; set; } = "default";
    public CSharpDbLoggingOptions Logging { get; set; } = new();
    public CSharpDbHistoryOptions History { get; set; } = new();
    public TimeSpan LongRunningQueryThreshold { get; set; } = TimeSpan.FromSeconds(5);
    /// <summary>
    /// Idle duration after which an otherwise-open, non-expiring transaction
    /// session is classified as abandoned in runtime diagnostics.
    /// </summary>
    public TimeSpan SessionAbandonmentThreshold { get; set; } = TimeSpan.FromMinutes(30);
    public CSharpDbOpenTelemetryOptions OpenTelemetry { get; set; } = new();
    public CSharpDbPrometheusOptions Prometheus { get; set; } = new();
    public CSharpDbHealthOptions Health { get; set; } = new();

    public IReadOnlyList<string> GetValidationErrors()
    {
        var errors = new List<string>();

        ValidateAlias(DatabaseAlias, nameof(DatabaseAlias), errors);
        ValidatePositiveDuration(
            LongRunningQueryThreshold,
            nameof(LongRunningQueryThreshold),
            MaximumThreshold,
            errors);
        ValidatePositiveDuration(
            SessionAbandonmentThreshold,
            nameof(SessionAbandonmentThreshold),
            MaximumRetention,
            errors);

        if (Logging is null)
        {
            errors.Add("Logging options are required.");
        }
        else
        {
            if (!Enum.IsDefined(Logging.SqlText))
                errors.Add("Logging.SqlText must be None, Normalized, or Raw.");

            ValidatePositiveDuration(
                Logging.SlowQueryThreshold,
                "Logging.SlowQueryThreshold",
                MaximumThreshold,
                errors);

            if (Logging.SlowQueryThresholdOverrides is null)
            {
                errors.Add("Logging.SlowQueryThresholdOverrides are required.");
            }
            else
            {
                foreach ((CSharpDbOperationClass operationClass, TimeSpan threshold) in
                         Logging.SlowQueryThresholdOverrides.OrderBy(static item => (int)item.Key))
                {
                    if (operationClass == CSharpDbOperationClass.Unknown || !Enum.IsDefined(operationClass))
                    {
                        errors.Add(
                            "Logging.SlowQueryThresholdOverrides keys must be defined operation classes other than Unknown.");
                        continue;
                    }

                    ValidatePositiveDuration(
                        threshold,
                        $"Logging.SlowQueryThresholdOverrides[{operationClass}]",
                        MaximumThreshold,
                        errors);
                }
            }
        }

        if (History is null)
        {
            errors.Add("History options are required.");
        }
        else
        {
            ValidateCapacity(
                History.ActiveQueryCapacity,
                "History.ActiveQueryCapacity",
                MaximumActiveOperationCapacity,
                errors);
            ValidateCapacity(
                History.RecentQueryCapacity,
                "History.RecentQueryCapacity",
                MaximumHistoryCapacity,
                errors);
            ValidateCapacity(
                History.RecentOperationCapacity,
                "History.RecentOperationCapacity",
                MaximumHistoryCapacity,
                errors);
            ValidatePositiveDuration(
                History.Retention,
                "History.Retention",
                MaximumRetention,
                errors);
        }

        if (OpenTelemetry is null)
        {
            errors.Add("OpenTelemetry options are required.");
        }
        else
        {
            if (OpenTelemetry.Enabled && !Enabled)
            {
                errors.Add(
                    "OpenTelemetry cannot be enabled when CSharpDB observability is disabled.");
            }

            if (OpenTelemetry.SamplingRatio is < 0 or > 1 ||
                double.IsNaN(OpenTelemetry.SamplingRatio) ||
                double.IsInfinity(OpenTelemetry.SamplingRatio))
            {
                errors.Add("OpenTelemetry.SamplingRatio must be between 0 and 1.");
            }

            if (OpenTelemetry.Resource is null)
            {
                errors.Add("OpenTelemetry.Resource options are required.");
            }
            else
            {
                ValidateOptionalResourceValue(
                    OpenTelemetry.Resource.ServiceName,
                    "OpenTelemetry.Resource.ServiceName",
                    errors);
                ValidateOptionalResourceValue(
                    OpenTelemetry.Resource.ServiceNamespace,
                    "OpenTelemetry.Resource.ServiceNamespace",
                    errors);
                ValidateOptionalResourceValue(
                    OpenTelemetry.Resource.ServiceVersion,
                    "OpenTelemetry.Resource.ServiceVersion",
                    errors);
                ValidateOptionalResourceValue(
                    OpenTelemetry.Resource.ServiceInstanceId,
                    "OpenTelemetry.Resource.ServiceInstanceId",
                    errors);
                ValidateOptionalResourceValue(
                    OpenTelemetry.Resource.DeploymentEnvironment,
                    "OpenTelemetry.Resource.DeploymentEnvironment",
                    errors);
            }

            if (OpenTelemetry.Otlp is null)
            {
                errors.Add("OpenTelemetry.Otlp options are required.");
            }
            else if (OpenTelemetry.Otlp.Enabled && !OpenTelemetry.Enabled)
            {
                errors.Add("OpenTelemetry.Otlp cannot be enabled when OpenTelemetry is disabled.");
            }

            if (OpenTelemetry.Console is null)
            {
                errors.Add("OpenTelemetry.Console options are required.");
            }
            else if (OpenTelemetry.Console.Enabled && !OpenTelemetry.Enabled)
            {
                errors.Add("OpenTelemetry.Console cannot be enabled when OpenTelemetry is disabled.");
            }
        }

        if (Prometheus is null)
        {
            errors.Add("Prometheus options are required.");
        }
        else
        {
            ValidateEndpointPath(Prometheus.Path, "Prometheus.Path", errors);

            if (Prometheus.Enabled && !Enabled)
            {
                errors.Add(
                    "Prometheus cannot be enabled when CSharpDB observability is disabled.");
            }

            if (Prometheus.AllowInsecureRemoteAccess && !Prometheus.Enabled)
            {
                errors.Add(
                    "Prometheus.AllowInsecureRemoteAccess requires Prometheus to be enabled.");
            }
        }

        if (Health is null)
        {
            errors.Add("Health options are required.");
        }
        else
        {
            ValidateEndpointPath(Health.LivenessPath, "Health.LivenessPath", errors);
            ValidateEndpointPath(Health.ReadinessPath, "Health.ReadinessPath", errors);
            ValidatePositiveDuration(
                Health.ReadinessTimeout,
                "Health.ReadinessTimeout",
                TimeSpan.FromMinutes(1),
                errors);

            if (string.Equals(
                    Health.LivenessPath,
                    Health.ReadinessPath,
                    StringComparison.OrdinalIgnoreCase))
            {
                errors.Add("Health liveness and readiness paths must be different.");
            }

            if (Prometheus is not null && Prometheus.Enabled)
            {
                if (string.Equals(
                        Prometheus.Path,
                        Health.LivenessPath,
                        StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(
                        Prometheus.Path,
                        Health.ReadinessPath,
                        StringComparison.OrdinalIgnoreCase))
                {
                    errors.Add("Prometheus and health endpoint paths must be different.");
                }
            }
        }

        return new ReadOnlyCollection<string>(errors);
    }

    public void Validate()
    {
        IReadOnlyList<string> errors = GetValidationErrors();
        if (errors.Count > 0)
            throw new CSharpDbObservabilityOptionsValidationException(errors);
    }

    public static bool IsValidDatabaseAlias(string? alias)
    {
        if (string.IsNullOrWhiteSpace(alias) ||
            alias.Length > CSharpDbDiagnostics.MaximumDatabaseAliasLength)
        {
            return false;
        }

        foreach (char character in alias)
        {
            if (!char.IsAsciiLetterOrDigit(character) &&
                character is not '-' and not '_' and not '.')
            {
                return false;
            }
        }

        return true;
    }

    public static void ValidateDatabaseAliases(IEnumerable<string> aliases)
    {
        ArgumentNullException.ThrowIfNull(aliases);

        string[] values = aliases.ToArray();
        if (values.Length > CSharpDbDiagnostics.MaximumConfiguredDatabaseAliases)
        {
            throw new ArgumentException(
                $"At most {CSharpDbDiagnostics.MaximumConfiguredDatabaseAliases} database or shard aliases may be configured.",
                nameof(aliases));
        }

        var unique = new HashSet<string>(StringComparer.Ordinal);
        foreach (string alias in values)
        {
            if (!IsValidDatabaseAlias(alias))
                throw new ArgumentException("Every database or shard alias must be a safe bounded label.", nameof(aliases));
            if (!unique.Add(alias))
                throw new ArgumentException("Database or shard aliases must be unique.", nameof(aliases));
        }
    }

    private static void ValidateAlias(string? alias, string name, List<string> errors)
    {
        if (!IsValidDatabaseAlias(alias))
        {
            errors.Add(
                $"{name} must be 1-{CSharpDbDiagnostics.MaximumDatabaseAliasLength} ASCII letters, digits, '.', '-', or '_' and cannot contain a path.");
        }
    }

    private static void ValidateCapacity(
        int value,
        string name,
        int maximum,
        List<string> errors)
    {
        if (value <= 0 || value > maximum)
            errors.Add($"{name} must be between 1 and {maximum}.");
    }

    private static void ValidatePositiveDuration(
        TimeSpan value,
        string name,
        TimeSpan maximum,
        List<string> errors)
    {
        if (value <= TimeSpan.Zero || value > maximum)
            errors.Add($"{name} must be greater than zero and no greater than {maximum}.");
    }

    private static void ValidateEndpointPath(
        string? path,
        string name,
        List<string> errors)
    {
        if (string.IsNullOrWhiteSpace(path) ||
            path[0] != '/' ||
            path.Length == 1 ||
            path.Contains('\\') ||
            path.Contains('?') ||
            path.Contains('#') ||
            path.Contains('%') ||
            path.Contains("//", StringComparison.Ordinal) ||
            path.EndsWith('/') ||
            path.Contains('{') ||
            path.Contains('}') ||
            path.Any(static character =>
                char.IsControl(character) || char.IsWhiteSpace(character)) ||
            path.Split('/', StringSplitOptions.None).Any(
                static segment => segment is "." or ".."))
        {
            errors.Add(
                $"{name} must be a canonical absolute application path without whitespace, percent escapes, a trailing slash, route parameters, dot segments, query, fragment, backslash, or empty segment.");
        }
    }

    private static void ValidateOptionalResourceValue(
        string? value,
        string name,
        List<string> errors)
    {
        if (value is null)
            return;

        if (string.IsNullOrWhiteSpace(value) ||
            value.Length > 128 ||
            !string.Equals(value, value.Trim(), StringComparison.Ordinal) ||
            value.Any(static character =>
                char.IsControl(character) || character is '/' or '\\'))
        {
            errors.Add(
                $"{name} must be 1-128 non-control characters and cannot contain a path.");
        }
    }
}

public sealed class CSharpDbLoggingOptions
{
    public bool Enabled { get; set; } = true;
    public bool Queries { get; set; }
    public bool SlowQueries { get; set; } = true;
    public TimeSpan SlowQueryThreshold { get; set; } = TimeSpan.FromMilliseconds(500);
    public Dictionary<CSharpDbOperationClass, TimeSpan> SlowQueryThresholdOverrides { get; set; } = new();
    public SqlTextCaptureMode SqlText { get; set; } = SqlTextCaptureMode.None;

    public TimeSpan GetSlowQueryThreshold(CSharpDbOperationClass operationClass)
    {
        if (operationClass == CSharpDbOperationClass.Unknown || !Enum.IsDefined(operationClass))
            throw new ArgumentOutOfRangeException(nameof(operationClass));
        if (SlowQueryThresholdOverrides is null)
            throw new InvalidOperationException("Slow-query threshold overrides have not been configured.");

        return SlowQueryThresholdOverrides.TryGetValue(operationClass, out TimeSpan threshold)
            ? threshold
            : SlowQueryThreshold;
    }
}

public sealed class CSharpDbHistoryOptions
{
    /// <summary>
    /// Enables bounded active/recent runtime history. Metrics, tracing, and
    /// structured diagnostic events remain independently controlled by their
    /// respective options.
    /// </summary>
    public bool Enabled { get; set; } = true;

    public int ActiveQueryCapacity { get; set; } = 1_000;
    public int RecentQueryCapacity { get; set; } = 500;
    public int RecentOperationCapacity { get; set; } = 100;
    public TimeSpan Retention { get; set; } = TimeSpan.FromMinutes(15);
}

public sealed class CSharpDbOpenTelemetryOptions
{
    public bool Enabled { get; set; }
    public double SamplingRatio { get; set; } = 1;
    public CSharpDbOpenTelemetryResourceOptions Resource { get; set; } = new();
    public CSharpDbOtlpOptions Otlp { get; set; } = new();
    public CSharpDbConsoleExporterOptions Console { get; set; } = new();
}

public sealed class CSharpDbOpenTelemetryResourceOptions
{
    /// <summary>
    /// OpenTelemetry service name. Hosted products supply their application
    /// name when this value is not configured.
    /// </summary>
    public string? ServiceName { get; set; }

    public string? ServiceNamespace { get; set; } = "CSharpDB";

    /// <summary>
    /// Service version. Hosted products use their informational assembly
    /// version when this value is not configured.
    /// </summary>
    public string? ServiceVersion { get; set; }

    /// <summary>
    /// Optional stable deployment-provided instance id. Hosted products
    /// generate an opaque process-lifetime id when this value is not set.
    /// </summary>
    public string? ServiceInstanceId { get; set; }

    /// <summary>
    /// Deployment environment. Hosted products use the ASP.NET Core host
    /// environment when this value is not configured.
    /// </summary>
    public string? DeploymentEnvironment { get; set; }
}

public sealed class CSharpDbOtlpOptions
{
    public bool Enabled { get; set; }
}

public sealed class CSharpDbConsoleExporterOptions
{
    public bool Enabled { get; set; }
}

public sealed class CSharpDbPrometheusOptions
{
    public bool Enabled { get; set; }
    public string Path { get; set; } = "/metrics";
    public bool AllowInsecureRemoteAccess { get; set; }
}

public sealed class CSharpDbHealthOptions
{
    public bool Enabled { get; set; } = true;
    public string LivenessPath { get; set; } = "/health/live";
    public string ReadinessPath { get; set; } = "/health/ready";
    public TimeSpan ReadinessTimeout { get; set; } = TimeSpan.FromSeconds(2);
}

public sealed class CSharpDbObservabilityOptionsValidationException : Exception
{
    public CSharpDbObservabilityOptionsValidationException(IEnumerable<string> errors)
        : this(new ReadOnlyCollection<string>(errors.ToArray()))
    {
    }

    private CSharpDbObservabilityOptionsValidationException(
        ReadOnlyCollection<string> errors)
        : base($"CSharpDB observability configuration is invalid: {string.Join(" ", errors)}")
    {
        Errors = errors;
    }

    public IReadOnlyList<string> Errors { get; }
}
