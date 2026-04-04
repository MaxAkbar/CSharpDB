using CSharpDB.Admin.Forms.Models;

namespace CSharpDB.Admin.Forms.Contracts;

public abstract record UpdateResult
{
    private UpdateResult() { }

    public sealed record Ok(FormDefinition Doc) : UpdateResult;
    public sealed record Conflict : UpdateResult;
    public sealed record NotFound : UpdateResult;
}
