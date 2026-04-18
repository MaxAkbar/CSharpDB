using Microsoft.EntityFrameworkCore.Update;

namespace CSharpDB.EntityFrameworkCore.Update.Internal;

public sealed class CSharpDbUpdateSqlGenerator : UpdateSqlGenerator
{
    public CSharpDbUpdateSqlGenerator(UpdateSqlGeneratorDependencies dependencies)
        : base(dependencies)
    {
    }
}
