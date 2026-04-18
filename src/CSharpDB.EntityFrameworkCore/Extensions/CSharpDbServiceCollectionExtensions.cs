using CSharpDB.EntityFrameworkCore;
using CSharpDB.EntityFrameworkCore.Infrastructure.Internal;
using CSharpDB.EntityFrameworkCore.Migrations.Internal;
using CSharpDB.EntityFrameworkCore.Query.Internal;
using CSharpDB.EntityFrameworkCore.Storage.Internal;
using CSharpDB.EntityFrameworkCore.Update.Internal;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;

namespace Microsoft.Extensions.DependencyInjection;

public static class CSharpDbServiceCollectionExtensions
{
    public static IServiceCollection AddEntityFrameworkCSharpDb(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        var builder = new EntityFrameworkRelationalServicesBuilder(services)
            .TryAdd<LoggingDefinitions, CSharpDbLoggingDefinitions>()
            .TryAdd<Microsoft.EntityFrameworkCore.Storage.IDatabaseProvider, DatabaseProvider<CSharpDbOptionsExtension>>()
            .TryAdd<IProviderConventionSetBuilder, CSharpDbConventionSetBuilder>()
            .TryAdd<IRelationalConnection, CSharpDbRelationalConnection>()
            .TryAdd<IRelationalDatabaseCreator, CSharpDbRelationalDatabaseCreator>()
            .TryAdd<IRelationalTypeMappingSource, CSharpDbTypeMappingSource>()
            .TryAdd<IQuerySqlGeneratorFactory, CSharpDbQuerySqlGeneratorFactory>()
            .TryAdd<ISqlGenerationHelper, CSharpDbSqlGenerationHelper>()
            .TryAdd<Microsoft.EntityFrameworkCore.Infrastructure.IModelValidator, CSharpDbModelValidator>()
            .TryAdd<IHistoryRepository, CSharpDbHistoryRepository>()
            .TryAdd<IMigrationsSqlGenerator, CSharpDbMigrationsSqlGenerator>()
            .TryAdd<IUpdateSqlGenerator, CSharpDbUpdateSqlGenerator>()
            .TryAdd<IModificationCommandBatchFactory, CSharpDbModificationCommandBatchFactory>();

        builder.TryAddCoreServices();
        return services;
    }
}
