namespace CSharpDB.Client;

public class CSharpDbClientException : Exception
{
    public CSharpDbClientException(string message)
        : base(message)
    {
    }

    public CSharpDbClientException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
