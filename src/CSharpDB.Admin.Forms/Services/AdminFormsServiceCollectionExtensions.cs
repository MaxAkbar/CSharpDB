using CSharpDB.Admin.Forms.Contracts;
using Microsoft.Extensions.DependencyInjection;

namespace CSharpDB.Admin.Forms.Services;

public static class AdminFormsServiceCollectionExtensions
{
    public static IServiceCollection AddCSharpDbAdminForms(this IServiceCollection services)
    {
        services.AddScoped<IFormRepository, DbFormRepository>();
        services.AddScoped<ISchemaProvider, DbSchemaProvider>();
        services.AddScoped<IFormRecordService, DbFormRecordService>();
        services.AddScoped<IFormGenerator, DefaultFormGenerator>();
        services.AddScoped<IValidationInferenceService, DefaultValidationInferenceService>();
        return services;
    }
}
