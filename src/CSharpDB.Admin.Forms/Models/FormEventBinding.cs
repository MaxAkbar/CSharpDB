using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Models;

public enum FormEventKind
{
    OnOpen,
    OnLoad,
    BeforeInsert,
    AfterInsert,
    BeforeUpdate,
    AfterUpdate,
    BeforeDelete,
    AfterDelete,
}

public sealed record FormEventBinding(
    FormEventKind Event,
    string CommandName,
    IReadOnlyDictionary<string, object?>? Arguments = null,
    bool StopOnFailure = true,
    DbActionSequence? ActionSequence = null);
