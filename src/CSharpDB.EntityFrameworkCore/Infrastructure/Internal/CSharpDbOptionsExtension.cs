using System.Data.Common;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace CSharpDB.EntityFrameworkCore.Infrastructure.Internal;

public sealed class CSharpDbOptionsExtension : RelationalOptionsExtension
{
    private DbContextOptionsExtensionInfo? _info;

    public CSharpDbOptionsExtension()
    {
    }

    private CSharpDbOptionsExtension(CSharpDbOptionsExtension copyFrom)
        : base(copyFrom)
    {
    }

    public override DbContextOptionsExtensionInfo Info
        => _info ??= new ExtensionInfo(this);

    public override void ApplyServices(IServiceCollection services)
        => services.AddEntityFrameworkCSharpDb();

    public override void Validate(IDbContextOptions options)
    {
        base.Validate(options);
        CSharpDbProviderValidation.ValidateConnectionConfiguration(ConnectionString, Connection);
    }

    public new CSharpDbOptionsExtension WithConnection(DbConnection connection)
        => (CSharpDbOptionsExtension)base.WithConnection(connection);

    public new CSharpDbOptionsExtension WithConnectionString(string connectionString)
        => (CSharpDbOptionsExtension)base.WithConnectionString(connectionString);

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
