using System.Text;

namespace CSharpDB.Primitives;

public sealed class FullTextIndexOptions
{
    public NormalizationForm Normalization { get; init; } = NormalizationForm.FormKC;
    public bool LowercaseInvariant { get; init; } = true;
    public bool StorePositions { get; init; } = true;
}
