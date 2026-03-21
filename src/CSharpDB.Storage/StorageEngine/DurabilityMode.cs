namespace CSharpDB.Storage.StorageEngine;

/// <summary>
/// Controls how file-backed WAL commits are flushed.
/// </summary>
public enum DurabilityMode
{
    /// <summary>
    /// Flush managed stream buffers so writes are issued to the OS, but do not
    /// force an OS-buffer flush on each commit.
    /// </summary>
    Buffered = 0,

    /// <summary>
    /// Force the WAL commit record through the OS-buffer flush primitive before
    /// reporting commit success.
    /// </summary>
    Durable = 1,
}
