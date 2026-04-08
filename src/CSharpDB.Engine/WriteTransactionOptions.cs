namespace CSharpDB.Engine;

/// <summary>
/// Retry settings for <see cref="Database.RunWriteTransactionAsync(Func{WriteTransaction, CancellationToken, ValueTask}, WriteTransactionOptions?, CancellationToken)"/>.
/// </summary>
public sealed class WriteTransactionOptions
{
    /// <summary>
    /// Maximum number of retry attempts after the initial transaction execution.
    /// </summary>
    public int MaxRetries { get; set; } = 5;

    /// <summary>
    /// Initial retry backoff. Defaults to 250 microseconds.
    /// </summary>
    public TimeSpan InitialBackoff { get; set; } = TimeSpan.FromMilliseconds(0.25);

    /// <summary>
    /// Maximum retry backoff. Defaults to 20 milliseconds.
    /// </summary>
    public TimeSpan MaxBackoff { get; set; } = TimeSpan.FromMilliseconds(20);

    internal async ValueTask DelayBeforeRetryAsync(int attempt, CancellationToken ct)
    {
        if (MaxRetries <= 0)
            return;

        double multiplier = Math.Pow(2, Math.Max(0, attempt));
        double delayMs = Math.Min(MaxBackoff.TotalMilliseconds, InitialBackoff.TotalMilliseconds * multiplier);
        double jitterMs = delayMs <= 0 ? 0 : Random.Shared.NextDouble() * delayMs;
        TimeSpan delay = TimeSpan.FromMilliseconds(jitterMs);
        if (delay > TimeSpan.Zero)
            await Task.Delay(delay, ct);
    }
}
