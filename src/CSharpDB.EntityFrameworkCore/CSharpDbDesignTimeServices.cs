using Microsoft.EntityFrameworkCore.Design;
using Microsoft.Extensions.DependencyInjection;

namespace CSharpDB.EntityFrameworkCore;

public sealed class CSharpDbDesignTimeServices : IDesignTimeServices
{
    public void ConfigureDesignTimeServices(IServiceCollection services)
    {
        services.AddEntityFrameworkCSharpDb();

        var builder = new EntityFrameworkRelationalDesignServicesBuilder(services)
            .TryAdd<IAnnotationCodeGenerator, AnnotationCodeGenerator>();

        builder.TryAddCoreServices();
    }
}
