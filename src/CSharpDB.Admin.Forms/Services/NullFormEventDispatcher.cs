using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;

namespace CSharpDB.Admin.Forms.Services;

public sealed class NullFormEventDispatcher : IFormEventDispatcher
{
    public static NullFormEventDispatcher Instance { get; } = new();

    private NullFormEventDispatcher()
    {
    }

    public Task<FormEventDispatchResult> DispatchAsync(
        FormDefinition form,
        FormEventKind eventKind,
        IReadOnlyDictionary<string, object?>? record = null,
        CancellationToken ct = default)
        => Task.FromResult(FormEventDispatchResult.Success());
}
