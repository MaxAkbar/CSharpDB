namespace CSharpDB.Admin.Forms.Services;

internal interface IFormControlRegistryConfiguration
{
    void Configure(FormControlRegistryBuilder builder);
}

internal sealed class DelegateFormControlRegistryConfiguration(Action<FormControlRegistryBuilder> configure)
    : IFormControlRegistryConfiguration
{
    public void Configure(FormControlRegistryBuilder builder) => configure(builder);
}
