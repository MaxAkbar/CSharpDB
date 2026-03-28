namespace CSharpDB.Storage.StorageEngine;

/// <summary>
/// Controls when advisory planner statistics are persisted for file-backed databases.
/// </summary>
public enum AdvisoryStatisticsPersistenceMode
{
    /// <summary>
    /// Persist advisory statistics as part of ordinary commit flows.
    /// </summary>
    Immediate = 0,

    /// <summary>
    /// Keep advisory statistics current in memory and persist them only on
    /// explicit maintenance boundaries such as ANALYZE, clean close, and export.
    /// </summary>
    Deferred = 1,
}
