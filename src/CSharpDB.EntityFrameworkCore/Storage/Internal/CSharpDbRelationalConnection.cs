using System.Data.Common;
using CSharpDB.Data;
using CSharpDB.EntityFrameworkCore.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;

namespace CSharpDB.EntityFrameworkCore.Storage.Internal;

public sealed class CSharpDbRelationalConnection : RelationalConnection
{
    private readonly CSharpDbOptionsExtension _optionsExtension;

    public CSharpDbRelationalConnection(RelationalConnectionDependencies dependencies)
        : base(dependencies)
    {
        _optionsExtension = dependencies.ContextOptions.FindExtension<CSharpDbOptionsExtension>()
            ?? new CSharpDbOptionsExtension();

        CSharpDbProviderValidation.ValidateConnectionConfiguration(
            _optionsExtension.ConnectionString,
            _optionsExtension.Connection,
            _optionsExtension);
    }

    protected override DbConnection CreateDbConnection()
    {
        string validatedConnectionString = GetValidatedConnectionString();
        var builder = new CSharpDbConnectionStringBuilder(validatedConnectionString);

        if (_optionsExtension.StoragePreset is not null)
            builder.StoragePreset = _optionsExtension.StoragePreset;

        if (_optionsExtension.EmbeddedOpenMode is not null)
            builder.EmbeddedOpenMode = _optionsExtension.EmbeddedOpenMode;

        string effectiveConnectionString = builder.ConnectionString;
        CSharpDbProviderValidation.ValidateConnectionConfiguration(
            effectiveConnectionString,
            connection: null,
            _optionsExtension);

        return new CSharpDbConnection(
            effectiveConnectionString,
            _optionsExtension.DirectDatabaseOptions,
            _optionsExtension.HybridDatabaseOptions);
    }
}
