namespace CSharpDB.DevOps;

public sealed record DatabaseDocument(string DisplayName, DateTimeOffset CapturedUtc, string CatalogVersion,
    int DocumentationVersion, IReadOnlyList<DatabaseDocumentEntry> Entries, IReadOnlyList<string> Diagnostics)
{
    public bool IsComplete => Diagnostics.Count == 0;
}

public sealed record DatabaseDocumentLink(string Label, string? ObjectId);
public sealed record DatabaseDocumentFact(string Name, string Value);
public sealed record DatabaseDocumentColumn(DatabaseDocumentLink Link, string Type, bool Nullable,
    string Attributes, string? DefaultSql, string? Description);
public sealed record DatabaseDocumentColumnPair(DatabaseDocumentLink Child, DatabaseDocumentLink Parent);
public sealed record DatabaseDocumentRelationship(string Id, string Name, DatabaseDocumentLink ChildTable,
    DatabaseDocumentLink ParentTable, IReadOnlyList<DatabaseDocumentColumnPair> Columns, string OnDelete, string OnUpdate);
public sealed record DatabaseDocumentParameter(string Name, string Type, bool Required, string? Default, string? Description);

public sealed record DatabaseDocumentEntry(string Id, string Kind, string Name, string? OwnerName,
    string Definition, string DefinitionLabel, string? Description, string? OverrideDescription,
    long Revision, string Fingerprint, bool NeedsReview, bool CanEdit,
    IReadOnlyList<DatabaseDocumentFact> Facts, IReadOnlyList<DatabaseDocumentColumn> Columns,
    IReadOnlyList<DatabaseDocumentParameter> Parameters, IReadOnlyList<DatabaseDocumentLink> RelatedObjects,
    IReadOnlyList<DatabaseDocumentRelationship> Relationships);
