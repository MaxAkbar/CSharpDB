namespace CSharpDB.VirtualFS;

// ──────────────────────────────────────────────────────────────
//  Data model
// ──────────────────────────────────────────────────────────────
public enum EntryKind : byte
{
    Directory = 1,
    File = 2,
    Shortcut = 3,
}