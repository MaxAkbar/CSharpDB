using Microsoft.EntityFrameworkCore.Query;

namespace CSharpDB.EntityFrameworkCore.Query.Internal;

public sealed class CSharpDbQuerySqlGeneratorFactory : IQuerySqlGeneratorFactory
{
    private readonly QuerySqlGeneratorDependencies _dependencies;

    public CSharpDbQuerySqlGeneratorFactory(QuerySqlGeneratorDependencies dependencies)
        => _dependencies = dependencies;

    public QuerySqlGenerator Create()
        => new CSharpDbQuerySqlGenerator(_dependencies);
}
