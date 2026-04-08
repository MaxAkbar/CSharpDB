using System;

namespace CSharpDB.Engine;

/// <summary>
/// Opts a document type into source-generated collection codec and field-descriptor generation.
/// The annotated type must be a top-level partial class, record, or struct.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, AllowMultiple = false, Inherited = false)]
public sealed class CollectionModelAttribute : Attribute
{
    public CollectionModelAttribute(Type jsonSerializerContextType)
    {
        JsonSerializerContextType = jsonSerializerContextType ?? throw new ArgumentNullException(nameof(jsonSerializerContextType));
    }

    public Type JsonSerializerContextType { get; }
}
