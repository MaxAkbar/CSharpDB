using CSharpDB.Client;

namespace CSharpDB.Cli;

internal sealed class CliShellOptions
{
    public required CSharpDbClientOptions ClientOptions { get; init; }
    public required string DisplayTarget { get; init; }
    public bool EnableLocalDirectFeatures { get; init; }

    public static bool TryParse(string[] args, out CliShellOptions? options, out string? error)
    {
        ArgumentNullException.ThrowIfNull(args);

        string? endpoint = null;
        string? positionalTarget = null;
        CSharpDbTransport? transport = null;

        for (int i = 0; i < args.Length; i++)
        {
            string arg = args[i];
            switch (arg)
            {
                case "--endpoint":
                case "--server":
                    if (!TryReadValue(args, ref i, arg, out var endpointValue, out error))
                    {
                        options = null;
                        return false;
                    }

                    endpoint = endpointValue;
                    break;

                case "--transport":
                    if (!TryReadValue(args, ref i, arg, out var transportValue, out error))
                    {
                        options = null;
                        return false;
                    }

                    if (!TryParseTransport(transportValue, out var parsedTransport))
                    {
                        error = $"Unknown transport '{transportValue}'.";
                        options = null;
                        return false;
                    }

                    transport = parsedTransport;
                    break;

                default:
                    if (arg.StartsWith("-", StringComparison.Ordinal))
                    {
                        error = $"Unknown option '{arg}'.";
                        options = null;
                        return false;
                    }

                    if (positionalTarget is not null)
                    {
                        error = "Only one database path or endpoint may be provided.";
                        options = null;
                        return false;
                    }

                    positionalTarget = arg;
                    break;
            }
        }

        if (endpoint is not null && positionalTarget is not null)
        {
            error = "Specify either a positional database path or --endpoint/--server, not both.";
            options = null;
            return false;
        }

        if (transport is not null && transport != CSharpDbTransport.Direct && endpoint is null)
        {
            error = $"Transport '{transport}' requires --endpoint.";
            options = null;
            return false;
        }

        string displayTarget = endpoint ?? positionalTarget ?? "csharpdb.db";
        options = new CliShellOptions
        {
            DisplayTarget = displayTarget,
            EnableLocalDirectFeatures = IsDirectTarget(transport, displayTarget),
            ClientOptions = new CSharpDbClientOptions
            {
                Transport = transport,
                Endpoint = endpoint ?? positionalTarget,
                DataSource = endpoint is null && positionalTarget is null ? "csharpdb.db" : null,
            },
        };

        error = null;
        return true;
    }

    public static string Usage =>
        "Usage: csharpdb [database-path] [--endpoint <uri>] [--transport <direct|http|grpc|namedpipes>]";

    private static bool TryReadValue(
        string[] args,
        ref int index,
        string optionName,
        out string value,
        out string? error)
    {
        if (index + 1 >= args.Length)
        {
            value = string.Empty;
            error = $"Missing value for {optionName}.";
            return false;
        }

        value = args[++index];
        error = null;
        return true;
    }

    private static bool TryParseTransport(string value, out CSharpDbTransport transport)
    {
        switch (value.Trim().ToLowerInvariant())
        {
            case "direct":
                transport = CSharpDbTransport.Direct;
                return true;
            case "http":
                transport = CSharpDbTransport.Http;
                return true;
            case "grpc":
                transport = CSharpDbTransport.Grpc;
                return true;
            case "namedpipes":
            case "named-pipes":
            case "pipe":
            case "npipe":
                transport = CSharpDbTransport.NamedPipes;
                return true;
            default:
                transport = default;
                return false;
        }
    }

    private static bool IsDirectTarget(CSharpDbTransport? transport, string target)
    {
        if (transport is not null)
            return transport == CSharpDbTransport.Direct;

        if (Path.IsPathRooted(target))
            return true;

        if (Uri.TryCreate(target, UriKind.Absolute, out var uri))
            return string.Equals(uri.Scheme, Uri.UriSchemeFile, StringComparison.OrdinalIgnoreCase);

        return !target.Contains("://", StringComparison.Ordinal);
    }
}
