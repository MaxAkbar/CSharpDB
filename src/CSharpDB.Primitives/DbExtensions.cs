namespace CSharpDB.Primitives;

public enum DbExtensionRuntimeKind
{
    HostCallback = 0,
    OutOfProcess = 1,
}

public enum DbExtensionExportKind
{
    ScalarFunction = 0,
    Command = 1,
    PipelineTransform = 2,
    ValidationRule = 3,
}

public enum DbExtensionCapability
{
    ScalarFunctions = 0,
    Commands = 1,
    PipelineTransforms = 2,
    ValidationRules = 3,
    ReadDatabase = 4,
    WriteDatabase = 5,
    RunSql = 6,
    Network = 7,
    FileRead = 8,
    FileWrite = 9,
    Clock = 10,
    Random = 11,
    EnvironmentVariables = 12,
}

public enum DbExtensionCapabilityGrantStatus
{
    Denied = 0,
    Granted = 1,
}

[Flags]
public enum DbExtensionHostMode
{
    None = 0,
    Embedded = 1,
    Daemon = 2,
    All = Embedded | Daemon,
}

public sealed record DbExtensionManifest(
    string Id,
    string Name,
    string Version,
    DbExtensionRuntimeKind Runtime,
    string Entrypoint,
    IReadOnlyList<DbExtensionExport> Exports,
    IReadOnlyList<DbExtensionCapabilityRequest> Capabilities,
    string? RequiredCSharpDbVersion = null,
    string? ArtifactSha256 = null,
    string? Signature = null,
    string? Publisher = null,
    IReadOnlyDictionary<string, string>? Metadata = null);

public sealed record DbExtensionExport(
    DbExtensionExportKind Kind,
    string Name,
    int? Arity = null,
    DbType? ReturnType = null,
    IReadOnlyDictionary<string, string>? Metadata = null);

public sealed record DbExtensionCapabilityRequest(
    DbExtensionCapability Name,
    string? Reason = null,
    IReadOnlyList<string>? Exports = null,
    IReadOnlyList<string>? Tables = null,
    IReadOnlyDictionary<string, string>? Scope = null);

public sealed record DbExtensionCapabilityGrant(
    DbExtensionCapability Name,
    DbExtensionCapabilityGrantStatus Status,
    string? Reason = null,
    IReadOnlyList<string>? Exports = null,
    IReadOnlyList<string>? Tables = null,
    IReadOnlyDictionary<string, string>? Scope = null,
    string? PolicySource = null,
    DateTimeOffset? GrantedAt = null);

public sealed record DbExtensionPolicy(
    bool AllowExtensions,
    IReadOnlyList<DbExtensionCapabilityGrant>? Grants = null,
    TimeSpan? DefaultTimeout = null,
    long? MaxMemoryBytes = null,
    bool RequireSignature = true,
    DbExtensionHostMode AllowedHostModes = DbExtensionHostMode.All);

public sealed record DbExtensionPolicyDecision(
    bool Allowed,
    string? DenialReason,
    IReadOnlyList<DbExtensionCapabilityDecision> Capabilities,
    TimeSpan Timeout,
    long? MaxMemoryBytes);

public sealed record DbExtensionCapabilityDecision(
    DbExtensionCapability Name,
    DbExtensionCapabilityGrantStatus Status,
    string? Reason,
    string? PolicySource);

public sealed record DbExtensionInvocationRequest(
    string ExtensionId,
    DbExtensionExportKind Kind,
    string Name,
    IReadOnlyDictionary<string, DbValue> Arguments,
    IReadOnlyDictionary<string, string>? Metadata = null,
    string? CorrelationId = null,
    DateTimeOffset? Deadline = null,
    string? UserIdentity = null);

public sealed record DbExtensionInvocationResult(
    bool Succeeded,
    string? Message = null,
    DbValue Value = default,
    IReadOnlyList<string>? Diagnostics = null,
    IReadOnlyList<string>? Logs = null,
    string? ErrorCode = null);

public static class DbExtensionPolicyEvaluator
{
    private static readonly TimeSpan DefaultExecutionTimeout = TimeSpan.FromSeconds(5);

    public static DbExtensionPolicyDecision Evaluate(
        DbExtensionManifest manifest,
        DbExtensionPolicy policy,
        DbExtensionHostMode hostMode)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        ArgumentNullException.ThrowIfNull(policy);

        IReadOnlyList<DbExtensionCapabilityDecision> capabilityDecisions =
            EvaluateCapabilities(manifest.Capabilities, policy.Grants);

        string? denialReason = null;
        if (!policy.AllowExtensions)
        {
            denialReason = "Extension execution is disabled by host policy.";
        }
        else if ((policy.AllowedHostModes & hostMode) == 0)
        {
            denialReason = $"Extension execution is not allowed in {hostMode} mode.";
        }
        else if (RequiresArtifactSignature(manifest) &&
            policy.RequireSignature &&
            string.IsNullOrWhiteSpace(manifest.Signature))
        {
            denialReason = "Extension policy requires a signature.";
        }
        else
        {
            DbExtensionCapabilityDecision? deniedCapability = capabilityDecisions
                .FirstOrDefault(static decision => decision.Status != DbExtensionCapabilityGrantStatus.Granted);
            if (deniedCapability is not null)
                denialReason = $"Capability '{deniedCapability.Name}' is not granted.";
        }

        return new DbExtensionPolicyDecision(
            Allowed: denialReason is null,
            denialReason,
            capabilityDecisions,
            policy.DefaultTimeout ?? DefaultExecutionTimeout,
            policy.MaxMemoryBytes);
    }

    public static DbExtensionPolicyDecision Evaluate(
        DbHostCallbackDescriptor callback,
        DbExtensionPolicy policy,
        DbExtensionHostMode hostMode)
    {
        ArgumentNullException.ThrowIfNull(callback);
        ArgumentNullException.ThrowIfNull(policy);

        IReadOnlyList<DbExtensionCapabilityDecision> capabilityDecisions =
            EvaluateCapabilities(callback.Capabilities, policy.Grants);

        string? denialReason = null;
        if (!policy.AllowExtensions)
        {
            denialReason = "Extension execution is disabled by host policy.";
        }
        else if ((policy.AllowedHostModes & hostMode) == 0)
        {
            denialReason = $"Extension execution is not allowed in {hostMode} mode.";
        }
        else
        {
            DbExtensionCapabilityDecision? deniedCapability = capabilityDecisions
                .FirstOrDefault(static decision => decision.Status != DbExtensionCapabilityGrantStatus.Granted);
            if (deniedCapability is not null)
                denialReason = $"Capability '{deniedCapability.Name}' is not granted.";
        }

        TimeSpan timeout = callback.Timeout ?? policy.DefaultTimeout ?? DefaultExecutionTimeout;
        return new DbExtensionPolicyDecision(
            Allowed: denialReason is null,
            denialReason,
            capabilityDecisions,
            timeout,
            policy.MaxMemoryBytes);
    }

    private static bool RequiresArtifactSignature(DbExtensionManifest manifest)
        => manifest.Runtime == DbExtensionRuntimeKind.OutOfProcess;

    private static IReadOnlyList<DbExtensionCapabilityDecision> EvaluateCapabilities(
        IReadOnlyList<DbExtensionCapabilityRequest>? requests,
        IReadOnlyList<DbExtensionCapabilityGrant>? grants)
    {
        if (requests is null || requests.Count == 0)
            return [];

        var grantByCapability = (grants ?? [])
            .GroupBy(static grant => grant.Name)
            .ToDictionary(
                static group => group.Key,
                static group => group.Last(),
                EqualityComparer<DbExtensionCapability>.Default);

        var decisions = new DbExtensionCapabilityDecision[requests.Count];
        for (int i = 0; i < requests.Count; i++)
        {
            DbExtensionCapabilityRequest request = requests[i];
            if (!grantByCapability.TryGetValue(request.Name, out DbExtensionCapabilityGrant? grant))
            {
                decisions[i] = new DbExtensionCapabilityDecision(
                    request.Name,
                    DbExtensionCapabilityGrantStatus.Denied,
                    "No matching capability grant.",
                    PolicySource: null);
                continue;
            }

            decisions[i] = new DbExtensionCapabilityDecision(
                request.Name,
                grant.Status,
                grant.Reason,
                grant.PolicySource);
        }

        return decisions;
    }
}
