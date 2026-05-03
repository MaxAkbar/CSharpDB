using CSharpDB.Admin.Forms.Contracts;

namespace CSharpDB.Admin.Forms.Services;

internal static class DefaultFormControlRegistry
{
    public static IFormControlRegistry Instance { get; } = Create();

    private static IFormControlRegistry Create()
    {
        var builder = new FormControlRegistryBuilder();
        BuiltInFormControlDescriptors.AddTo(builder);
        return builder.Build();
    }
}
