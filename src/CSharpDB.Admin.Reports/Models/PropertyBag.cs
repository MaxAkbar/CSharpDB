namespace CSharpDB.Admin.Reports.Models;

public sealed record PropertyBag(IReadOnlyDictionary<string, object?> Values)
{
    public static PropertyBag Empty { get; } = new(new Dictionary<string, object?>());
}
