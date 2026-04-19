using System.Data.Common;
using CSharpDB.Data;
using CSharpDB.EntityFrameworkCore.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;

namespace CSharpDB.EntityFrameworkCore.Storage.Internal;

public sealed class CSharpDbRelationalConnection : RelationalConnection
{
    public CSharpDbRelationalConnection(RelationalConnectionDependencies dependencies)
        : base(dependencies)
    {
    }

    protected override DbConnection CreateDbConnection()
    {
        CSharpDbProviderValidation.ValidateConnectionConfiguration(GetValidatedConnectionString(), null);
        return new CSharpDbConnection(GetValidatedConnectionString());
    }
}
