using System.Text;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Services;

public static class AdminHostCallbacks
{
    private const string PolicySource = "CSharpDB.Admin host callback policy";

    public static DbFunctionRegistry CreateFunctionRegistry()
        => DbFunctionRegistry.Create(functions =>
        {
            functions.AddScalar(
                "Slugify",
                arity: 1,
                options: new DbScalarFunctionOptions(
                    ReturnType: DbType.Text,
                    IsDeterministic: true,
                    NullPropagating: true,
                    Description: "Formats text as a lowercase URL slug.",
                    Metadata: CreateMetadata("CSharpDB.Admin")),
                invoke: static (_, args) => DbValue.FromText(Slugify(args[0].AsText)));
        });

    public static DbCommandRegistry CreateCommandRegistry()
        => DbCommandRegistry.Create(commands =>
        {
            commands.AddCommand(
                "EchoAutomationEvent",
                new DbCommandOptions(
                    Description: "Returns a small host-owned acknowledgement for automation command wiring.",
                    Metadata: CreateMetadata("CSharpDB.Admin")),
                static context =>
                {
                    string eventName = context.Metadata.TryGetValue("event", out string? value)
                        ? value
                        : "manual";
                    string surface = context.Metadata.TryGetValue("surface", out string? surfaceValue)
                        ? surfaceValue
                        : "unknown";
                    string message = $"Received {surface}.{eventName}.";

                    return DbCommandResult.Success(message, DbValue.FromText(message));
                });
        });

    public static DbExtensionPolicy CreatePolicy()
        => new(
            AllowExtensions: true,
            Grants:
            [
                new DbExtensionCapabilityGrant(
                    DbExtensionCapability.ScalarFunctions,
                    DbExtensionCapabilityGrantStatus.Granted,
                    Reason: "Admin host registered scalar functions.",
                    PolicySource: PolicySource),
                new DbExtensionCapabilityGrant(
                    DbExtensionCapability.Commands,
                    DbExtensionCapabilityGrantStatus.Granted,
                    Reason: "Admin host registered trusted commands.",
                    PolicySource: PolicySource),
            ],
            DefaultTimeout: TimeSpan.FromSeconds(5),
            RequireSignature: true,
            AllowedHostModes: DbExtensionHostMode.Embedded);

    private static Dictionary<string, string> CreateMetadata(string value)
        => new(StringComparer.OrdinalIgnoreCase)
        {
            ["host"] = value,
        };

    private static string Slugify(string text)
    {
        var builder = new StringBuilder(text.Length);
        bool wroteSeparator = false;

        foreach (char ch in text.Trim().ToLowerInvariant())
        {
            if (char.IsLetterOrDigit(ch))
            {
                builder.Append(ch);
                wroteSeparator = false;
                continue;
            }

            if (builder.Length == 0 || wroteSeparator)
                continue;

            builder.Append('-');
            wroteSeparator = true;
        }

        if (builder.Length > 0 && builder[^1] == '-')
            builder.Length--;

        return builder.ToString();
    }
}
