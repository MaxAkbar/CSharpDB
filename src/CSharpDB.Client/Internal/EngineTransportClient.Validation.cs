namespace CSharpDB.Client.Internal;

internal sealed partial class EngineTransportClient
{
    private static void ValidateIdentifier(string value, string label)
    {
        _ = RequireIdentifier(value, label);
    }
}
