using System.Text;
using CSharpDB.EntityFrameworkCore.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore.Storage;

namespace CSharpDB.EntityFrameworkCore.Storage.Internal;

public sealed class CSharpDbSqlGenerationHelper : RelationalSqlGenerationHelper
{
    public CSharpDbSqlGenerationHelper(RelationalSqlGenerationHelperDependencies dependencies)
        : base(dependencies)
    {
    }

    public override string DelimitIdentifier(string identifier)
    {
        CSharpDbProviderValidation.ValidateSimpleIdentifier(identifier, "Identifier");
        return identifier;
    }

    public override void DelimitIdentifier(StringBuilder builder, string identifier)
        => builder.Append(DelimitIdentifier(identifier));

    public override string DelimitIdentifier(string name, string? schema)
    {
        if (!string.IsNullOrWhiteSpace(schema))
            throw new NotSupportedException("Schemas are not supported by the CSharpDB EF Core provider.");

        return DelimitIdentifier(name);
    }

    public override void DelimitIdentifier(StringBuilder builder, string name, string? schema)
        => builder.Append(DelimitIdentifier(name, schema));

    public override string EscapeIdentifier(string identifier)
    {
        CSharpDbProviderValidation.ValidateSimpleIdentifier(identifier, "Identifier");
        return identifier;
    }

    public override void EscapeIdentifier(StringBuilder builder, string identifier)
        => builder.Append(EscapeIdentifier(identifier));
}
