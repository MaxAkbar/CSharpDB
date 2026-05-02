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

public static class DbExtensionPolicies
{
    public const string DefaultHostCallbackPolicySource = "CSharpDB default host callback policy";

    public static DbExtensionPolicy DefaultHostCallbackPolicy { get; } = new(
        AllowExtensions: true,
        Grants:
        [
            new DbExtensionCapabilityGrant(
                DbExtensionCapability.ScalarFunctions,
                DbExtensionCapabilityGrantStatus.Granted,
                Reason: "Host-registered scalar functions are allowed by default.",
                PolicySource: DefaultHostCallbackPolicySource),
            new DbExtensionCapabilityGrant(
                DbExtensionCapability.Commands,
                DbExtensionCapabilityGrantStatus.Granted,
                Reason: "Host-registered commands are allowed by default.",
                PolicySource: DefaultHostCallbackPolicySource),
            new DbExtensionCapabilityGrant(
                DbExtensionCapability.ValidationRules,
                DbExtensionCapabilityGrantStatus.Granted,
                Reason: "Host-registered validation rules are allowed by default.",
                PolicySource: DefaultHostCallbackPolicySource),
        ],
        DefaultTimeout: TimeSpan.FromSeconds(5),
        RequireSignature: true,
        AllowedHostModes: DbExtensionHostMode.Embedded);
}

public sealed class DbCallbackPolicyException : InvalidOperationException
{
    public DbCallbackPolicyException(
        DbHostCallbackDescriptor callback,
        DbExtensionPolicyDecision decision)
        : base(CreateMessage(callback, decision))
    {
        Callback = callback;
        Decision = decision;
    }

    public DbHostCallbackDescriptor Callback { get; }

    public DbExtensionPolicyDecision Decision { get; }

    private static string CreateMessage(
        DbHostCallbackDescriptor callback,
        DbExtensionPolicyDecision decision)
        => $"Host callback '{callback.Name}' was denied by policy: {decision.DenialReason ?? "No denial reason was provided."}";
}

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
                denialReason = deniedCapability.Reason ?? $"Capability '{deniedCapability.Name}' is not granted.";
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
                denialReason = deniedCapability.Reason ?? $"Capability '{deniedCapability.Name}' is not granted.";
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

        var decisions = new DbExtensionCapabilityDecision[requests.Count];
        for (int i = 0; i < requests.Count; i++)
        {
            DbExtensionCapabilityRequest request = requests[i];
            DbExtensionCapabilityGrant[] candidateGrants = (grants ?? [])
                .Where(grant => grant.Name == request.Name)
                .ToArray();
            if (candidateGrants.Length == 0)
            {
                decisions[i] = new DbExtensionCapabilityDecision(
                    request.Name,
                    DbExtensionCapabilityGrantStatus.Denied,
                    $"No grant exists for capability '{request.Name}'.",
                    PolicySource: null);
                continue;
            }

            DbExtensionCapabilityGrant? deniedGrant = candidateGrants
                .Where(grant => grant.Status == DbExtensionCapabilityGrantStatus.Denied)
                .FirstOrDefault(grant => GrantMatchesRequest(grant, request));
            if (deniedGrant is not null)
            {
                decisions[i] = new DbExtensionCapabilityDecision(
                    request.Name,
                    DbExtensionCapabilityGrantStatus.Denied,
                    deniedGrant.Reason ?? $"Capability '{request.Name}' is denied for {FormatRequestScope(request)}.",
                    deniedGrant.PolicySource);
                continue;
            }

            DbExtensionCapabilityGrant? grantedGrant = candidateGrants
                .Where(grant => grant.Status == DbExtensionCapabilityGrantStatus.Granted)
                .FirstOrDefault(grant => GrantMatchesRequest(grant, request));
            if (grantedGrant is not null)
            {
                decisions[i] = new DbExtensionCapabilityDecision(
                    request.Name,
                    DbExtensionCapabilityGrantStatus.Granted,
                    grantedGrant.Reason,
                    grantedGrant.PolicySource);
                continue;
            }

            decisions[i] = new DbExtensionCapabilityDecision(
                request.Name,
                DbExtensionCapabilityGrantStatus.Denied,
                $"No grant for capability '{request.Name}' matches requested {FormatRequestScope(request)}.",
                PolicySource: null);
        }

        return decisions;
    }

    private static bool GrantMatchesRequest(
        DbExtensionCapabilityGrant grant,
        DbExtensionCapabilityRequest request)
        => MatchesStringScope(grant.Exports, request.Exports)
            && MatchesStringScope(grant.Tables, request.Tables)
            && MatchesDictionaryScope(grant.Scope, request.Scope);

    private static bool MatchesStringScope(
        IReadOnlyList<string>? grantValues,
        IReadOnlyList<string>? requestValues)
    {
        if (grantValues is null || grantValues.Count == 0)
            return true;

        if (requestValues is null || requestValues.Count == 0)
            return false;

        var allowed = new HashSet<string>(
            grantValues.Where(static value => !string.IsNullOrWhiteSpace(value)),
            StringComparer.OrdinalIgnoreCase);
        if (allowed.Contains("*"))
            return true;

        return requestValues
            .Where(static value => !string.IsNullOrWhiteSpace(value))
            .All(allowed.Contains);
    }

    private static bool MatchesDictionaryScope(
        IReadOnlyDictionary<string, string>? grantScope,
        IReadOnlyDictionary<string, string>? requestScope)
    {
        if (grantScope is null || grantScope.Count == 0)
            return true;

        if (requestScope is null || requestScope.Count == 0)
            return false;

        foreach ((string key, string expectedValue) in grantScope)
        {
            if (string.IsNullOrWhiteSpace(key))
                continue;

            if (!requestScope.TryGetValue(key, out string? actualValue))
                return false;

            if (!string.Equals(expectedValue, "*", StringComparison.Ordinal) &&
                !string.Equals(expectedValue, actualValue, StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }
        }

        return true;
    }

    private static string FormatRequestScope(DbExtensionCapabilityRequest request)
    {
        string exports = FormatScopeList(request.Exports);
        string tables = FormatScopeList(request.Tables);
        string scope = request.Scope is { Count: > 0 }
            ? string.Join(", ", request.Scope
                .OrderBy(static item => item.Key, StringComparer.OrdinalIgnoreCase)
                .Select(static item => $"{item.Key}={item.Value}"))
            : "*";

        return $"exports [{exports}], tables [{tables}], scope [{scope}]";
    }

    private static string FormatScopeList(IReadOnlyList<string>? values)
        => values is { Count: > 0 }
            ? string.Join(", ", values)
            : "*";
}
