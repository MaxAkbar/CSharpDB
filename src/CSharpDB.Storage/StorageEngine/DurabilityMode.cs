namespace CSharpDB.Storage.StorageEngine;

/// <summary>
/// Controls how file-backed WAL commits are flushed.
/// Buffered is analogous to SQLite WAL synchronous NORMAL.
/// Durable is analogous to SQLite WAL synchronous FULL.
/// </summary>
public enum DurabilityMode
{
    /// <summary>
    /// Flush managed stream buffers so writes are issued to the OS, but do not
    /// force an OS-buffer flush on each commit.
    /// Analogous to SQLite WAL synchronous NORMAL.
    /// </summary>
    Buffered = 0,

    /// <summary>
    /// Force the WAL commit record through the OS-buffer flush primitive before
    /// reporting commit success.
    /// Analogous to SQLite WAL synchronous FULL.
    /// </summary>
    Durable = 1,
}
