using CSharpDB.Client;
using CSharpDB.Testing;

namespace CSharpDB.Data.Tests;

[Collection("ConnectionPoolState")]
public sealed class StreamingGrpcConnectionTests
{
    [Fact]
    public async Task SdkAndAdo_RentTheSamePersistentSession_AndResetTransactions()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var host = new GrpcSocketDaemonFixture();
        var options = new CSharpDbClientOptions
        {
            Transport = CSharpDbTransport.Grpc, Endpoint = host.NativeEndpoint.AbsoluteUri,
            GrpcMode = CSharpDbGrpcMode.Native, GrpcCallMode = CSharpDbGrpcCallMode.Streaming,
        };
        string cs = $"Transport=Grpc;Endpoint={host.NativeEndpoint};Grpc Mode=Native;Grpc Call Mode=Streaming;Pooling=true;Max Pool Size=1";
        try
        {
            await using (var sdk = await CSharpDbClient.RentAsync(options, 1, ct))
                Assert.Null((await sdk.ExecuteSqlAsync("CREATE TABLE ado_stream (id INTEGER PRIMARY KEY, value DECIMAL(10,2)); INSERT INTO ado_stream VALUES (1, 12.34)", ct)).Error);
            await using (var connection = new CSharpDbConnection(cs))
            {
                await connection.OpenAsync(ct);
                using var command = connection.CreateCommand();
                command.CommandText = "SELECT value FROM ado_stream WHERE id = @id";
                command.Parameters.AddWithValue("@id", 1);
                command.Prepare();
                Assert.Equal(12.34m, await command.ExecuteScalarAsync(ct));
                await using var transaction = await connection.BeginTransactionAsync(ct);
                command.Transaction = transaction;
                command.CommandText = "INSERT INTO ado_stream VALUES (2, 99.50)";
                Assert.Equal(1, await command.ExecuteNonQueryAsync(ct));
                // Returning this connection abandons the uncommitted insert.
            }
            await using (var sdk = await CSharpDbClient.RentAsync(options, 1, ct))
                Assert.Equal(1L, (await sdk.ExecuteSqlAsync("SELECT COUNT(*) FROM ado_stream", ct)).Rows![0][0]);
            Assert.Equal(1, host.Snapshot().Where(p => p.Key.Method.EndsWith("/CommandSession")).Sum(p => p.Value));
            Assert.All(host.Snapshot(), p => { Assert.Equal("HTTP/2", p.Key.Protocol); Assert.Equal("application/grpc", p.Key.ContentType); });
        }
        finally { await CSharpDbClient.ClearPoolAsync(host.NativeEndpoint.AbsoluteUri); }
    }

    [Theory]
    [InlineData("Data Source=:memory:;Grpc Call Mode=Streaming")]
    [InlineData("Transport=Grpc;Endpoint=http://localhost;Grpc Call Mode=Streaming")]
    [InlineData("Transport=Http;Endpoint=http://localhost;Grpc Mode=Native;Grpc Call Mode=Streaming")]
    public async Task Streaming_RejectsOtherTransportsAndImplicitHttpVersion(string connectionString)
    {
        await using var connection = new CSharpDbConnection(connectionString);
        await Assert.ThrowsAsync<InvalidOperationException>(() => connection.OpenAsync(TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData("42")]
    [InlineData("unsupported")]
    public void UnknownCallMode_IsRejected(string value)
        => Assert.Throws<FormatException>(() => new CSharpDbConnectionStringBuilder($"Grpc Call Mode={value}").GrpcCallMode);
}
