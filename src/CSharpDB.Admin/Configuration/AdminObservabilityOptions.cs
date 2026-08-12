namespace CSharpDB.Admin.Configuration;

public sealed class AdminObservabilityOptions
{
    public const string ConfigurationSectionName = "CSharpDB:Admin:Observability";
    public static readonly TimeSpan MinimumRefreshInterval = TimeSpan.FromMilliseconds(250);
    public static readonly TimeSpan MaximumRefreshInterval = TimeSpan.FromMinutes(1);

    public TimeSpan RefreshInterval { get; set; } = TimeSpan.FromSeconds(2);
    public int MaximumRecords { get; set; } = 100;
    public int SampleCapacity { get; set; } = 60;
    public TimeSpan StaleAfter { get; set; } = TimeSpan.FromSeconds(10);

    public bool IsValid()
        => RefreshInterval >= MinimumRefreshInterval &&
           RefreshInterval <= MaximumRefreshInterval &&
           MaximumRecords is >= 1 and <= CSharpDB.Observability.CSharpDbObservabilityOptions.MaximumHistoryCapacity &&
           SampleCapacity is >= 2 and <= 600 &&
           StaleAfter >= RefreshInterval &&
           StaleAfter <= TimeSpan.FromMinutes(10);

    public void Validate()
    {
        if (!IsValid())
        {
            throw new InvalidOperationException(
                $"{ConfigurationSectionName} must use a refresh interval from " +
                $"{MinimumRefreshInterval} through {MaximumRefreshInterval}, 1-{CSharpDB.Observability.CSharpDbObservabilityOptions.MaximumHistoryCapacity} records, " +
                "2-600 samples, and a stale threshold at least as long as the refresh interval " +
                "and no longer than 10 minutes.");
        }
    }
}
