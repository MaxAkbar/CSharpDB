using CSharpDB.DevOps;

namespace CSharpDB.Admin.Models;

public static class DefinitionRelationshipLabels
{
    public static string Label(DependencyRelationshipKind kind) => kind switch
    {
        DependencyRelationshipKind.ModelMembership => "Model membership",
        DependencyRelationshipKind.ProposedChange => "Proposed changes",
        DependencyRelationshipKind.ExternalArchive => "Archive relationships",
        _ => "Database usage",
    };

    public static string Description(DependencyRelationshipKind kind) => kind switch
    {
        DependencyRelationshipKind.ModelMembership => "Saved diagrams containing this object. Membership does not enforce a database constraint.",
        DependencyRelationshipKind.ProposedChange => "Saved design changes that have not been applied to the database.",
        DependencyRelationshipKind.ExternalArchive => "Relationships described by archive metadata. They are not enforced as local database constraints.",
        _ => "References in the current database's saved definitions.",
    };

    public static string CssClass(DependencyRelationshipKind kind) => kind switch
    {
        DependencyRelationshipKind.ModelMembership => "membership",
        DependencyRelationshipKind.ProposedChange => "proposed",
        DependencyRelationshipKind.ExternalArchive => "archive",
        _ => "usage",
    };
}
