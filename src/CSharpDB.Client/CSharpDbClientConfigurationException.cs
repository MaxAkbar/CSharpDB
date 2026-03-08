namespace CSharpDB.Client;

public sealed class CSharpDbClientConfigurationException : CSharpDbClientException
{
    public CSharpDbClientConfigurationException(string message)
        : base(message)
    {
    }
}
