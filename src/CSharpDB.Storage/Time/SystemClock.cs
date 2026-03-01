namespace CSharpDB.Storage.Time;

/// <summary>
/// Production clock implementation.
/// </summary>
public sealed class SystemClock : IClock
{
    public DateTimeOffset UtcNow => DateTimeOffset.UtcNow;
}
