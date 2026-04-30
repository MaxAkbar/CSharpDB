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
    TimeSpan Elapsed,
    bool Succeeded,
    bool TimedOut,
    bool Canceled,
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
        string? exceptionMessage)
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
                elapsed,
                succeeded,
                TimedOut: false,
                canceled,
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
        string? exceptionMessage)
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
                elapsed,
                succeeded,
                timedOut,
                canceled,
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
