using CSharpDB.Migration;
using CSharpDB.Migration.SqlServer;

namespace CSharpDB.Migration.SqlServer.Tests;

public sealed class SqlServerMigrationSourceInspectorTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task InspectUsesImmutableReaderSeam()
    {
        var reader = new RecordingReader(SqlServerTestSnapshot.Create());
        var inspector = new SqlServerMigrationSourceInspector(
            reader,
            SqlServerInspectionLimits.Default);

        MigrationCatalog catalog = await inspector.InspectAsync(Request(), Ct);

        Assert.True(reader.Called);
        Assert.Equal(MigrationSourceKind.SqlServer, inspector.SourceKind);
        Assert.Equal(MigrationSourceKind.SqlServer, catalog.Source.Kind);
    }

    [Fact]
    public async Task UnsupportedProfileRequestPerformsNoSourceAccess()
    {
        var reader = new RecordingReader(SqlServerTestSnapshot.Create());
        var inspector = new SqlServerMigrationSourceInspector(
            reader,
            SqlServerInspectionLimits.Default);

        await Assert.ThrowsAsync<NotSupportedException>(
            async () => await inspector.InspectAsync(
                Request() with { IncludeProfile = true },
                Ct));

        Assert.False(reader.Called);
    }

    [Fact]
    public async Task ProviderShapeFailuresHaveGenericPublicMessage()
    {
        const string secret = "Password=DoNotEchoInspectorSecret";
        var inspector = new SqlServerMigrationSourceInspector(
            new ThrowingReader(new FormatException(secret)),
            SqlServerInspectionLimits.Default);

        SqlServerMigrationException error =
            await Assert.ThrowsAsync<SqlServerMigrationException>(
                async () => await inspector.InspectAsync(Request(), Ct));

        Assert.Equal(
            "The SQL Server schema could not be inspected safely.",
            error.Message);
        Assert.DoesNotContain(secret, error.Message, StringComparison.Ordinal);
        Assert.Null(error.InnerException);
        Assert.DoesNotContain(secret, error.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task PreCanceledInspectionPerformsNoSourceAccess()
    {
        var reader = new RecordingReader(SqlServerTestSnapshot.Create());
        var inspector = new SqlServerMigrationSourceInspector(
            reader,
            SqlServerInspectionLimits.Default);
        using var canceled = new CancellationTokenSource();
        canceled.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            async () => await inspector.InspectAsync(Request(), canceled.Token));

        Assert.False(reader.Called);
    }

    private static MigrationInspectionRequest Request() =>
        new()
        {
            TargetCSharpDbVersion =
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
        };

    private sealed class RecordingReader : ISqlServerCatalogReader
    {
        private readonly SqlServerCatalogSnapshot snapshot;

        public RecordingReader(SqlServerCatalogSnapshot snapshot)
        {
            this.snapshot = snapshot;
        }

        public bool Called { get; private set; }

        public ValueTask<SqlServerCatalogSnapshot> ReadAsync(
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken)
        {
            Called = true;
            return ValueTask.FromResult(snapshot);
        }
    }

    private sealed class ThrowingReader : ISqlServerCatalogReader
    {
        private readonly Exception exception;

        public ThrowingReader(Exception exception)
        {
            this.exception = exception;
        }

        public ValueTask<SqlServerCatalogSnapshot> ReadAsync(
            SqlServerInspectionLimits limits,
            CancellationToken cancellationToken) =>
            ValueTask.FromException<SqlServerCatalogSnapshot>(exception);
    }
}
