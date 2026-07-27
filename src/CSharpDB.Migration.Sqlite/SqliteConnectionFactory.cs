using Microsoft.Data.Sqlite;

namespace CSharpDB.Migration.Sqlite;

internal static class SqliteConnectionFactory
{
    public static SqliteConnection CreateReadOnly(string path) => new(
        new SqliteConnectionStringBuilder
        {
            DataSource = path,
            Mode = SqliteOpenMode.ReadOnly,
            Cache = SqliteCacheMode.Private,
            Pooling = false,
        }.ToString());

    public static SqliteConnection CreateDestination(string path) => new(
        new SqliteConnectionStringBuilder
        {
            DataSource = path,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Private,
            Pooling = false,
        }.ToString());

    public static async ValueTask ConfigureReadOnlyAsync(
        SqliteConnection connection,
        CancellationToken cancellationToken)
    {
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText = "PRAGMA query_only = ON;";
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        command.CommandText = "PRAGMA query_only;";
        object? result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        if (Convert.ToInt32(result, System.Globalization.CultureInfo.InvariantCulture) != 1)
            throw new SqliteMigrationException("SQLite read-only enforcement could not be established.");
    }
}
