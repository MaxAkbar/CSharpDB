namespace CSharpDB.Client;

/// <summary>Selects how SQL and transaction commands travel over native gRPC.</summary>
public enum CSharpDbGrpcCallMode
{
    Unary = 0,

    /// <summary>Experimental persistent command stream; requires GrpcMode.Native.</summary>
    Streaming = 1,
}
