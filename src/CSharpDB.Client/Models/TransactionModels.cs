namespace CSharpDB.Client.Models;

public sealed class TransactionSessionInfo
{
    public required string TransactionId { get; init; }
    public required DateTime ExpiresAtUtc { get; init; }
}
