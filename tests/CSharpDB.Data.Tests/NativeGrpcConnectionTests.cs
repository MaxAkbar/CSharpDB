using CSharpDB.Testing;

namespace CSharpDB.Data.Tests;

public sealed class NativeGrpcConnectionTests
{
    [Fact]
    public async Task NativeMode_ConnectionStringUsesRealHttp2Socket()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var host = new GrpcSocketDaemonFixture();
        await using var conn = new CSharpDbConnection(
            $"Transport=Grpc;Grpc Mode=Native;Endpoint={host.NativeEndpoint}");
        await conn.OpenAsync(ct);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT @value";
        cmd.Parameters.AddWithValue("@value", 42);
        Assert.Equal(42, Assert.IsType<int>(await cmd.ExecuteScalarAsync(ct)));
        Assert.NotEmpty(host.Snapshot());
        Assert.All(host.Snapshot(), pair => Assert.Equal("HTTP/2", pair.Key.Protocol));
    }

    [Theory]
    [InlineData("Data Source=:memory:;Grpc Mode=Native")]
    [InlineData("Data Source=:memory:;Pooling=true;Grpc Mode=Native")]
    [InlineData("Transport=Http;Endpoint=http://localhost;Grpc Mode=Native")]
    public async Task NativeMode_RejectsNonGrpcConnections(string connectionString)
    {
        await using var conn = new CSharpDbConnection(connectionString);
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => conn.OpenAsync(TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData("unsupported")]
    [InlineData("42")]
    public void GrpcMode_RejectsUnknownConnectionStringValues(string value)
        => Assert.Throws<FormatException>(() => new CSharpDbConnectionStringBuilder($"Grpc Mode={value}").GrpcMode);
}
