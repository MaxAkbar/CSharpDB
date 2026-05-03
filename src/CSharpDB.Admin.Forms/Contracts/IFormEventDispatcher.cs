using CSharpDB.Admin.Forms.Models;

namespace CSharpDB.Admin.Forms.Contracts;

public interface IFormEventDispatcher
{
    Task<FormEventDispatchResult> DispatchAsync(
        FormDefinition form,
        FormEventKind eventKind,
        IReadOnlyDictionary<string, object?>? record = null,
        CancellationToken ct = default);
}
