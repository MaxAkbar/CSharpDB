namespace CSharpDB.Admin.Forms.Models;

public sealed record PropertyBag(IReadOnlyDictionary<string, object?> Values)
{
    public static PropertyBag Empty { get; } = new(new Dictionary<string, object?>());
}
