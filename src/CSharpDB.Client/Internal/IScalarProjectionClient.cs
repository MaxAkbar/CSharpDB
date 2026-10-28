using CSharpDB.Client.Models;

namespace CSharpDB.Client.Internal;

/// <summary>A private result projection over the existing client session and rental.</summary>
internal interface IScalarProjectionClient
{
    bool SupportsScalarProjection { get; }
    /// <summary>Each returned ValueTask must be consumed exactly once.</summary>
    ValueTask<ScalarProjectionResult> ExecuteScalarProjectionAsync(
        string sql, string? transactionId, CancellationToken cancellationToken);
}

// Only immutable primitives cross this boundary; canonical SDK metadata remains private.
internal sealed record class ScalarTypeSnapshot
{
    public ScalarTypeSnapshot(
        SqlTypeKind Kind, int? Length, int? Precision, int? Scale, int? FractionalSecondsPrecision)
    {
        this.Kind = Kind;
        this.Length = Length;
        this.Precision = Precision;
        this.Scale = Scale;
        this.FractionalSecondsPrecision = FractionalSecondsPrecision;
    }

    public SqlTypeKind Kind { get; }
    public int? Length { get; }
    public int? Precision { get; }
    public int? Scale { get; }
    public int? FractionalSecondsPrecision { get; }
}

internal readonly struct ScalarProjectionResult
{
    private ScalarProjectionResult(
        bool hasCompactInt64, long value, ScalarTypeSnapshot? declaredType, SqlExecutionResult? fallback)
    {
        HasCompactInt64 = hasCompactInt64;
        Int64 = value;
        DeclaredType = declaredType;
        Fallback = fallback;
    }

    internal bool HasCompactInt64 { get; }
    internal long Int64 { get; }
    internal ScalarTypeSnapshot? DeclaredType { get; }
    internal SqlExecutionResult? Fallback { get; }

    internal static ScalarProjectionResult FromCompactInt64(long value, ScalarTypeSnapshot? declaredType)
        => new(true, value, declaredType, null);

    internal static ScalarProjectionResult FromFallback(SqlExecutionResult result)
        => new(false, default, null, result ?? throw new ArgumentNullException(nameof(result)));
}
