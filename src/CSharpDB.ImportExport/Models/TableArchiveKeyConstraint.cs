using CSharpDB.Primitives;

namespace CSharpDB.ImportExport.Models;

public sealed class TableArchiveKeyConstraint
{
    public string? ConstraintName { get; init; }
    public KeyConstraintKind Kind { get; init; }
    public required IReadOnlyList<string> Columns { get; init; }
    public string? BackingIndexName { get; init; }

    public static TableArchiveKeyConstraint FromKeyConstraint(KeyConstraintDefinition key) => new()
    {
        ConstraintName = key.ConstraintName,
        Kind = key.Kind,
        Columns = key.Columns.ToArray(),
        BackingIndexName = key.BackingIndexName,
    };

    public KeyConstraintDefinition ToKeyConstraint() => new()
    {
        ConstraintName = ConstraintName,
        Kind = Kind switch
        {
            KeyConstraintKind.PrimaryKey => KeyConstraintKind.PrimaryKey,
            KeyConstraintKind.Unique => KeyConstraintKind.Unique,
            _ => throw new InvalidDataException($"Unsupported archived key constraint kind '{Kind}'."),
        },
        Columns = Columns.ToArray(),
        BackingIndexName = BackingIndexName,
    };
}
