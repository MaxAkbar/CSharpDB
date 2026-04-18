using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;

namespace CSharpDB.EntityFrameworkCore.Infrastructure.Internal;

public sealed class CSharpDbConventionSetBuilder : RelationalConventionSetBuilder
{
    public CSharpDbConventionSetBuilder(
        ProviderConventionSetBuilderDependencies dependencies,
        RelationalConventionSetBuilderDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
    }
}
