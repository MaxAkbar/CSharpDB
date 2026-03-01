using System.Data.Common;
using CSharpDB.Core;

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
