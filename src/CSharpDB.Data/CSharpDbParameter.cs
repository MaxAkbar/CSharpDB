using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using SysDbType = System.Data.DbType;

namespace CSharpDB.Data;

public sealed class CSharpDbParameter : DbParameter
{
    public override SysDbType DbType { get; set; } = SysDbType.String;
    public override ParameterDirection Direction { get; set; } = ParameterDirection.Input;
    public override bool IsNullable { get; set; }

    [AllowNull]
    public override string ParameterName { get; set; } = "";

    public override int Size { get; set; }

    [AllowNull]
    public override string SourceColumn { get; set; } = "";

    public override bool SourceColumnNullMapping { get; set; }
    public override object? Value { get; set; }

    public CSharpDbParameter() { }

    public CSharpDbParameter(string name, object? value)
    {
        ParameterName = name;
        Value = value;
    }

    public override void ResetDbType() => DbType = SysDbType.String;
}
