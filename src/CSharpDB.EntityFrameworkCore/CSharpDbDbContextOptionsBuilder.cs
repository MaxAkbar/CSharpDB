using CSharpDB.Data;
using CSharpDB.Engine;
using CSharpDB.EntityFrameworkCore.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace CSharpDB.EntityFrameworkCore;

public sealed class CSharpDbDbContextOptionsBuilder
{
    public CSharpDbDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder)
        => OptionsBuilder = optionsBuilder ?? throw new ArgumentNullException(nameof(optionsBuilder));

    public DbContextOptionsBuilder OptionsBuilder { get; }

    public CSharpDbDbContextOptionsBuilder UseDirectDatabaseOptions(DatabaseOptions directDatabaseOptions)
    {
        ArgumentNullException.ThrowIfNull(directDatabaseOptions);
        return UpdateExtension(extension => extension.WithDirectDatabaseOptions(directDatabaseOptions));
    }

    public CSharpDbDbContextOptionsBuilder UseHybridDatabaseOptions(HybridDatabaseOptions hybridDatabaseOptions)
    {
        ArgumentNullException.ThrowIfNull(hybridDatabaseOptions);
        return UpdateExtension(extension => extension.WithHybridDatabaseOptions(hybridDatabaseOptions));
    }

    public CSharpDbDbContextOptionsBuilder UseStoragePreset(CSharpDbStoragePreset storagePreset)
        => UpdateExtension(extension => extension.WithStoragePreset(storagePreset));

    public CSharpDbDbContextOptionsBuilder UseEmbeddedOpenMode(CSharpDbEmbeddedOpenMode embeddedOpenMode)
        => UpdateExtension(extension => extension.WithEmbeddedOpenMode(embeddedOpenMode));

    private CSharpDbDbContextOptionsBuilder UpdateExtension(
        Func<CSharpDbOptionsExtension, CSharpDbOptionsExtension> updateExtension)
    {
        ArgumentNullException.ThrowIfNull(updateExtension);

        CSharpDbOptionsExtension extension = OptionsBuilder.Options.FindExtension<CSharpDbOptionsExtension>()
            ?? new CSharpDbOptionsExtension();

        ((IDbContextOptionsBuilderInfrastructure)OptionsBuilder).AddOrUpdateExtension(
            updateExtension(extension));

        return this;
    }
}
