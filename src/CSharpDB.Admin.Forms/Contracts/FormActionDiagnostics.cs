using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Contracts;

public sealed record FormActionInvocationDiagnostic(
    DbActionKind ActionKind,
    string? Target,
    string? FormId,
    string? FormName,
    string? TableName,
    string? EventName,
    string? ActionSequenceName,
    int StepIndex,
    string? Location,
    TimeSpan Elapsed,
    bool Succeeded,
    bool Canceled,
    string? ResultMessage,
    string? ExceptionMessage,
    IReadOnlyDictionary<string, string> Metadata);

public static class FormActionDiagnostics
{
    public const string ListenerName = "CSharpDB.Admin.Forms.Actions";
    public const string InvocationEventName = "CSharpDB.Admin.Forms.Actions.Invocation";

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
        Justification = "Form action diagnostics are emitted only for subscribed hosts; the strongly typed event payload is part of the public diagnostics contract.")]
    internal static void WriteInvocation(
        DbActionKind actionKind,
        string? target,
        IReadOnlyDictionary<string, string>? metadata,
        TimeSpan elapsed,
        bool succeeded,
        bool canceled,
        string? resultMessage,
        string? exceptionMessage)
    {
        if (!IsInvocationEnabled)
            return;

        IReadOnlyDictionary<string, string> metadataSnapshot = CopyMetadata(metadata);
        Listener.Write(
            InvocationEventName,
            new FormActionInvocationDiagnostic(
                actionKind,
                string.IsNullOrWhiteSpace(target) ? null : target,
                ReadMetadata(metadataSnapshot, "formId"),
                ReadMetadata(metadataSnapshot, "formName"),
                ReadMetadata(metadataSnapshot, "tableName"),
                ReadMetadata(metadataSnapshot, "event"),
                ReadMetadata(metadataSnapshot, "actionSequence"),
                ReadStepIndex(metadataSnapshot),
                BuildLocation(metadataSnapshot),
                elapsed,
                succeeded,
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

        if (!string.IsNullOrWhiteSpace(eventName) && !string.IsNullOrWhiteSpace(actionStep))
            return $"events.{eventName}.actionSequence.steps[{actionStep}]";

        if (!string.IsNullOrWhiteSpace(actionStep))
            return $"action.steps[{actionStep}]";

        return null;
    }

    private static int ReadStepIndex(IReadOnlyDictionary<string, string> metadata)
        => int.TryParse(ReadMetadata(metadata, "actionStep"), out int stepIndex)
            ? stepIndex
            : -1;

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
