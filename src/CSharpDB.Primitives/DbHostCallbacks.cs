namespace CSharpDB.Primitives;

public sealed record DbHostCallbackDescriptor(
    AutomationCallbackKind Kind,
    string Name,
    DbExtensionRuntimeKind Runtime,
    IReadOnlyList<DbExtensionCapabilityRequest> Capabilities,
    int? Arity = null,
    DbType? ReturnType = null,
    string? Description = null,
    bool IsDeterministic = false,
    bool NullPropagating = false,
    TimeSpan? Timeout = null,
    bool IsLongRunning = false,
    IReadOnlyDictionary<string, string>? Metadata = null);

internal static class DbHostCallbackDescriptorFactory
{
    public static DbHostCallbackDescriptor CreateScalar(
        string name,
        int arity,
        DbScalarFunctionOptions options)
        => new(
            AutomationCallbackKind.ScalarFunction,
            name,
            DbExtensionRuntimeKind.HostCallback,
            CreateCapabilities(DbExtensionCapability.ScalarFunctions, name, options.AdditionalCapabilities),
            Arity: arity,
            ReturnType: options.ReturnType,
            Description: options.Description,
            IsDeterministic: options.IsDeterministic,
            NullPropagating: options.NullPropagating,
            Metadata: options.Metadata);

    public static DbHostCallbackDescriptor CreateCommand(
        string name,
        DbCommandOptions options)
        => new(
            AutomationCallbackKind.Command,
            name,
            DbExtensionRuntimeKind.HostCallback,
            CreateCapabilities(DbExtensionCapability.Commands, name, options.AdditionalCapabilities),
            Description: options.Description,
            Timeout: options.Timeout,
            IsLongRunning: options.IsLongRunning,
            Metadata: options.Metadata);

    private static IReadOnlyList<DbExtensionCapabilityRequest> CreateCapabilities(
        DbExtensionCapability baseCapability,
        string exportName,
        IReadOnlyList<DbExtensionCapabilityRequest>? additionalCapabilities)
    {
        var capabilities = new List<DbExtensionCapabilityRequest>
        {
            new(baseCapability, Exports: [exportName]),
        };

        if (additionalCapabilities is { Count: > 0 })
            capabilities.AddRange(additionalCapabilities);

        return capabilities;
    }
}
