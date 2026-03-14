using System.Data.Common;
using CSharpDB.Primitives;

namespace CSharpDB.Data;

public sealed class CSharpDbDataException : DbException
{
    public new ErrorCode ErrorCode { get; }

    internal CSharpDbDataException(CSharpDbException inner)
        : base(inner.Message, inner)
    {
        ErrorCode = inner.Code;
    }
}
