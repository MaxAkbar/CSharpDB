using System.Data.Common;
using CSharpDB.Data;
using CSharpDB.Engine;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace CSharpDB.EntityFrameworkCore.Infrastructure.Internal;

public sealed class CSharpDbOptionsExtension : RelationalOptionsExtension
{
    private DbContextOptionsExtensionInfo? _info;

    public DatabaseOptions? DirectDatabaseOptions { get; private set; }

    public HybridDatabaseOptions? HybridDatabaseOptions { get; private set; }

    public CSharpDbStoragePreset? StoragePreset { get; private set; }

    public CSharpDbEmbeddedOpenMode? EmbeddedOpenMode { get; private set; }

    public CSharpDbOptionsExtension()
    {
    }

    private CSharpDbOptionsExtension(CSharpDbOptionsExtension copyFrom)
        : base(copyFrom)
    {
        DirectDatabaseOptions = copyFrom.DirectDatabaseOptions;
        HybridDatabaseOptions = copyFrom.HybridDatabaseOptions;
        StoragePreset = copyFrom.StoragePreset;
        EmbeddedOpenMode = copyFrom.EmbeddedOpenMode;
    }

    public override DbContextOptionsExtensionInfo Info
        => _info ??= new ExtensionInfo(this);

    public override void ApplyServices(IServiceCollection services)
        => services.AddEntityFrameworkCSharpDb();

    public override void Validate(IDbContextOptions options)
    {
        base.Validate(options);
        CSharpDbProviderValidation.ValidateConnectionConfiguration(ConnectionString, Connection, this);
    }

    public new CSharpDbOptionsExtension WithConnection(DbConnection connection)
        => (CSharpDbOptionsExtension)base.WithConnection(connection);

    public new CSharpDbOptionsExtension WithConnectionString(string connectionString)
        => (CSharpDbOptionsExtension)base.WithConnectionString(connectionString);

    public CSharpDbOptionsExtension WithDirectDatabaseOptions(DatabaseOptions directDatabaseOptions)
    {
        ArgumentNullException.ThrowIfNull(directDatabaseOptions);

        var clone = (CSharpDbOptionsExtension)Clone();
        clone.DirectDatabaseOptions = directDatabaseOptions;
        return clone;
    }

    public CSharpDbOptionsExtension WithHybridDatabaseOptions(HybridDatabaseOptions hybridDatabaseOptions)
    {
        ArgumentNullException.ThrowIfNull(hybridDatabaseOptions);

        var clone = (CSharpDbOptionsExtension)Clone();
        clone.HybridDatabaseOptions = hybridDatabaseOptions;
        return clone;
    }

    public CSharpDbOptionsExtension WithStoragePreset(CSharpDbStoragePreset storagePreset)
    {
        var clone = (CSharpDbOptionsExtension)Clone();
        clone.StoragePreset = storagePreset;
        return clone;
    }

    public CSharpDbOptionsExtension WithEmbeddedOpenMode(CSharpDbEmbeddedOpenMode embeddedOpenMode)
    {
        var clone = (CSharpDbOptionsExtension)Clone();
        clone.EmbeddedOpenMode = embeddedOpenMode;
        return clone;
    }

    protected override RelationalOptionsExtension Clone()
        => new CSharpDbOptionsExtension(this);

    private sealed class ExtensionInfo(IDbContextOptionsExtension extension)
        : RelationalExtensionInfo(extension)
    {
        private new CSharpDbOptionsExtension Extension
            => (CSharpDbOptionsExtension)base.Extension;

        public override bool IsDatabaseProvider => true;

        public override string LogFragment => "using CSharpDB ";

        public override int GetServiceProviderHashCode() => 0;

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
            => debugInfo["CSharpDB:Provider"] = "1";

        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other)
            => other is ExtensionInfo;
    }
}
