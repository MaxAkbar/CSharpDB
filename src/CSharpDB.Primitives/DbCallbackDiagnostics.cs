using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace CSharpDB.Primitives;

public sealed record DbCallbackInvocationDiagnostic(
    AutomationCallbackKind CallbackKind,
    string Name,
    int? Arity,
    string? Surface,
    string? Location,
    string? EventName,
    DateTimeOffset? StartedAtUtc,
    string? CorrelationId,
    string? OwnerKind,
    string? OwnerId,
    string? OwnerName,
    TimeSpan Elapsed,
    bool Succeeded,
    bool TimedOut,
    bool Canceled,
    bool? PolicyAllowed,
    string? PolicyDenialReason,
    string? ErrorCode,
    string? ExceptionType,
    string? ResultMessage,
    string? ExceptionMessage,
    IReadOnlyDictionary<string, string> Metadata);

public static class DbCallbackDiagnostics
{
    public const string ListenerName = "CSharpDB.TrustedCallbacks";
    public const string InvocationEventName = "CSharpDB.TrustedCallbacks.Invocation";

    public static DiagnosticListener Listener { get; } = new(ListenerName);

    public static bool IsInvocationEnabled
        => Listener.IsEnabled(InvocationEventName);

    internal static long GetTimestamp()
        => Stopwatch.GetTimestamp();

    internal static TimeSpan GetElapsedTime(long startingTimestamp)
        => Stopwatch.GetElapsedTime(startingTimestamp);

    public static void WriteMissingScalarInvocation(
        string name,
        int arity,
        IReadOnlyDictionary<string, string>? metadata,
        string message)
        => WriteScalarInvocation(
            name,
            arity,
            metadata,
            elapsed: TimeSpan.Zero,
            succeeded: false,
            canceled: false,
            exceptionMessage: message,
            policyDecision: null,
            errorCode: "MissingCallback",
            exceptionType: null);

    public static void WriteMissingCommandInvocation(
        string name,
        IReadOnlyDictionary<string, string>? metadata,
        string message)
        => WriteCommandInvocation(
            name,
            metadata,
            elapsed: TimeSpan.Zero,
            succeeded: false,
            timedOut: false,
            canceled: false,
            resultMessage: null,
            exceptionMessage: message,
            policyDecision: null,
            errorCode: "MissingCallback",
            exceptionType: null);

    public static void WriteMissingValidationInvocation(
        string name,
        IReadOnlyDictionary<string, string>? metadata,
        string message)
        => WriteValidationInvocation(
            name,
            metadata,
            elapsed: TimeSpan.Zero,
            succeeded: false,
            timedOut: false,
            canceled: false,
            resultMessage: null,
            exceptionMessage: message,
            policyDecision: null,
            errorCode: "MissingCallback",
            exceptionType: null);

    public static void WritePolicyDeniedInvocation(
        DbHostCallbackDescriptor callback,
        IReadOnlyDictionary<string, string>? metadata,
        DbExtensionPolicyDecision decision)
    {
        ArgumentNullException.ThrowIfNull(callback);
        ArgumentNullException.ThrowIfNull(decision);

        if (callback.Kind == AutomationCallbackKind.Command)
        {
            WriteCommandInvocation(
                callback.Name,
                metadata,
                elapsed: TimeSpan.Zero,
                succeeded: false,
                timedOut: false,
                canceled: false,
                resultMessage: null,
                exceptionMessage: decision.DenialReason,
                decision,
                errorCode: "PolicyDenied",
                exceptionType: typeof(DbCallbackPolicyException).FullName);
            return;
        }

        if (callback.Kind == AutomationCallbackKind.ValidationRule)
        {
            WriteValidationInvocation(
                callback.Name,
                metadata,
                elapsed: TimeSpan.Zero,
                succeeded: false,
                timedOut: false,
                canceled: false,
                resultMessage: null,
                exceptionMessage: decision.DenialReason,
                decision,
                errorCode: "PolicyDenied",
                exceptionType: typeof(DbCallbackPolicyException).FullName);
            return;
        }

        WriteScalarInvocation(
            callback.Name,
            callback.Arity ?? 0,
            metadata,
            elapsed: TimeSpan.Zero,
            succeeded: false,
            canceled: false,
            exceptionMessage: decision.DenialReason,
            decision,
            errorCode: "PolicyDenied",
            exceptionType: typeof(DbCallbackPolicyException).FullName);
    }

    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2026",
        Justification = "Callback diagnostics are emitted only for subscribed hosts; the strongly typed event payload is part of the public diagnostics contract.")]
    internal static void WriteScalarInvocation(
        string name,
        int arity,
        IReadOnlyDictionary<string, string>? metadata,
        TimeSpan elapsed,
        bool succeeded,
        bool canceled,
        string? exceptionMessage,
        DbExtensionPolicyDecision? policyDecision = null,
        string? errorCode = null,
        string? exceptionType = null)
    {
        if (!IsInvocationEnabled)
            return;

        IReadOnlyDictionary<string, string> metadataSnapshot = CopyMetadata(metadata);
        Listener.Write(
            InvocationEventName,
            new DbCallbackInvocationDiagnostic(
                AutomationCallbackKind.ScalarFunction,
                name,
                arity,
                ReadMetadata(metadataSnapshot, "surface"),
                BuildLocation(metadataSnapshot),
                ReadMetadata(metadataSnapshot, "event"),
                ReadStartedAt(metadataSnapshot, elapsed),
                ReadMetadata(metadataSnapshot, "correlationId") ?? ReadMetadata(metadataSnapshot, "correlation"),
                ReadMetadata(metadataSnapshot, "ownerKind"),
                ReadMetadata(metadataSnapshot, "ownerId"),
                ReadMetadata(metadataSnapshot, "ownerName"),
                elapsed,
                succeeded,
                TimedOut: false,
                canceled,
                policyDecision?.Allowed,
                policyDecision?.DenialReason,
                errorCode,
                exceptionType,
                ResultMessage: null,
                exceptionMessage,
                metadataSnapshot));
    }

    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2026",
        Justification = "Callback diagnostics are emitted only for subscribed hosts; the strongly typed event payload is part of the public diagnostics contract.")]
    internal static void WriteCommandInvocation(
        string name,
        IReadOnlyDictionary<string, string>? metadata,
        TimeSpan elapsed,
        bool succeeded,
        bool timedOut,
        bool canceled,
        string? resultMessage,
        string? exceptionMessage,
        DbExtensionPolicyDecision? policyDecision = null,
        string? errorCode = null,
        string? exceptionType = null)
    {
        if (!IsInvocationEnabled)
            return;

        IReadOnlyDictionary<string, string> metadataSnapshot = CopyMetadata(metadata);
        Listener.Write(
            InvocationEventName,
            new DbCallbackInvocationDiagnostic(
                AutomationCallbackKind.Command,
                name,
                Arity: null,
                ReadMetadata(metadataSnapshot, "surface"),
                BuildLocation(metadataSnapshot),
                ReadMetadata(metadataSnapshot, "event"),
                ReadStartedAt(metadataSnapshot, elapsed),
                ReadMetadata(metadataSnapshot, "correlationId") ?? ReadMetadata(metadataSnapshot, "correlation"),
                ReadMetadata(metadataSnapshot, "ownerKind"),
                ReadMetadata(metadataSnapshot, "ownerId"),
                ReadMetadata(metadataSnapshot, "ownerName"),
                elapsed,
                succeeded,
                timedOut,
                canceled,
                policyDecision?.Allowed,
                policyDecision?.DenialReason,
                errorCode,
                exceptionType,
                resultMessage,
                exceptionMessage,
                metadataSnapshot));
    }

    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2026",
        Justification = "Callback diagnostics are emitted only for subscribed hosts; the strongly typed event payload is part of the public diagnostics contract.")]
    internal static void WriteValidationInvocation(
        string name,
        IReadOnlyDictionary<string, string>? metadata,
        TimeSpan elapsed,
        bool succeeded,
        bool timedOut,
        bool canceled,
        string? resultMessage,
        string? exceptionMessage,
        DbExtensionPolicyDecision? policyDecision = null,
        string? errorCode = null,
        string? exceptionType = null)
    {
        if (!IsInvocationEnabled)
            return;

        IReadOnlyDictionary<string, string> metadataSnapshot = CopyMetadata(metadata);
        Listener.Write(
            InvocationEventName,
            new DbCallbackInvocationDiagnostic(
                AutomationCallbackKind.ValidationRule,
                name,
                Arity: null,
                ReadMetadata(metadataSnapshot, "surface"),
                BuildLocation(metadataSnapshot),
                ReadMetadata(metadataSnapshot, "event"),
                ReadStartedAt(metadataSnapshot, elapsed),
                ReadMetadata(metadataSnapshot, "correlationId") ?? ReadMetadata(metadataSnapshot, "correlation"),
                ReadMetadata(metadataSnapshot, "ownerKind"),
                ReadMetadata(metadataSnapshot, "ownerId"),
                ReadMetadata(metadataSnapshot, "ownerName"),
                elapsed,
                succeeded,
                timedOut,
                canceled,
                policyDecision?.Allowed,
                policyDecision?.DenialReason,
                errorCode,
                exceptionType,
                resultMessage,
                exceptionMessage,
                metadataSnapshot));
    }

    private static IReadOnlyDictionary<string, string> CopyMetadata(
        IReadOnlyDictionary<string, string>? metadata)
    {
        if (metadata is null || metadata.Count == 0)
            return EmptyStringDictionary.Instance;

        return new Dictionary<string, string>(metadata, StringComparer.OrdinalIgnoreCase);
    }

    private static string? BuildLocation(IReadOnlyDictionary<string, string> metadata)
    {
        if (TryReadMetadata(metadata, "location", out string? location))
            return location;

        string? eventName = ReadMetadata(metadata, "event");
        string? actionSequence = ReadMetadata(metadata, "actionSequence");
        string? actionStep = ReadMetadata(metadata, "actionStep");
        if (!string.IsNullOrWhiteSpace(actionSequence) && !string.IsNullOrWhiteSpace(actionStep))
            return $"actionSequences.{actionSequence}.steps[{actionStep}]";

        string? controlId = ReadMetadata(metadata, "controlId");
        if (!string.IsNullOrWhiteSpace(controlId) && !string.IsNullOrWhiteSpace(eventName))
            return $"controls.{controlId}.events.{eventName}";

        if (!string.IsNullOrWhiteSpace(eventName))
            return $"events.{eventName}";

        if (!string.IsNullOrWhiteSpace(actionStep))
            return $"action.steps[{actionStep}]";

        return null;
    }

    private static DateTimeOffset ReadStartedAt(
        IReadOnlyDictionary<string, string> metadata,
        TimeSpan elapsed)
    {
        if (TryReadMetadata(metadata, "startedAtUtc", out string? rawStartedAt) &&
            DateTimeOffset.TryParse(
                rawStartedAt,
                System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal,
                out DateTimeOffset parsed))
        {
            return parsed;
        }

        return DateTimeOffset.UtcNow - elapsed;
    }

    private static string? ReadMetadata(IReadOnlyDictionary<string, string> metadata, string key)
        => TryReadMetadata(metadata, key, out string? value) ? value : null;

    private static bool TryReadMetadata(
        IReadOnlyDictionary<string, string> metadata,
        string key,
        out string? value)
    {
        if (metadata.TryGetValue(key, out string? raw) && !string.IsNullOrWhiteSpace(raw))
        {
            value = raw;
            return true;
        }

        value = null;
        return false;
    }

    private static class EmptyStringDictionary
    {
        public static readonly IReadOnlyDictionary<string, string> Instance =
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    }
}
