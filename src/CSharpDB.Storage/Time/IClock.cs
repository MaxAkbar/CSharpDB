namespace CSharpDB.Storage.Time;

/// <summary>
/// Time provider abstraction used by time-based policies.
/// </summary>
public interface IClock
{
    DateTimeOffset UtcNow { get; }
}
