using CSharpDB.DevOps;

namespace CSharpDB.Admin.Models;

public sealed record DefinitionExplorerSeed(string Table, string? Column, ColumnChangeKind Change);
public sealed record DefinitionObjectSeed(string? ObjectId, string View = "Definition", string? Column = null, string? Table = null);
