using CSharpDB.Primitives;

namespace CSharpDB.ImportExport.Models;

public readonly record struct TableArchiveRowLookupResult(bool IsIndexed, DbValue[]? Row);
